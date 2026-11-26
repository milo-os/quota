package frontproxy

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apiserver/pkg/apis/apiserver"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/authenticatorfactory"
	"k8s.io/apiserver/pkg/authentication/request/headerrequest"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	clocktesting "k8s.io/utils/clock/testing"
)

const proxyCN = "front-proxy-client"

var (
	keyOnce sync.Once
	testKey *rsa.PrivateKey
)

// key returns one shared RSA key for leaf certificates; generating a key per
// leaf makes the tests slow without testing anything extra. CAs get their own
// keys so that a CA with the same name but a different key is a real imposter.
func key() *rsa.PrivateKey {
	keyOnce.Do(func() {
		k, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			panic(err)
		}
		testKey = k
	})
	return testKey
}

var serial atomic.Int64

type certSpec struct {
	cn        string
	isCA      bool
	notBefore time.Time
	notAfter  time.Time
}

type issued struct {
	cert *x509.Certificate
	key  *rsa.PrivateKey
	pem  []byte
}

func issue(t testing.TB, spec certSpec, parent *issued) *issued {
	t.Helper()
	k := key()
	if spec.isCA {
		var err error
		if k, err = rsa.GenerateKey(rand.Reader, 2048); err != nil {
			t.Fatalf("generate CA key: %v", err)
		}
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(serial.Add(1)),
		Subject:               pkix.Name{CommonName: spec.cn, Organization: []string{"org-" + spec.cn}},
		NotBefore:             spec.notBefore,
		NotAfter:              spec.notAfter,
		BasicConstraintsValid: true,
		IsCA:                  spec.isCA,
	}
	if spec.isCA {
		tmpl.KeyUsage = x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature
	} else {
		tmpl.KeyUsage = x509.KeyUsageDigitalSignature
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	parentCert, signer := tmpl, k
	if parent != nil {
		parentCert, signer = parent.cert, parent.key
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parentCert, &k.PublicKey, signer)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	c, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	return &issued{cert: c, key: k, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// fakeCA is a CAContentProvider whose bundle can be swapped, like the
// ConfigMap-backed provider when extension-apiserver-authentication changes.
type fakeCA struct {
	cur atomic.Pointer[caState]
}

type caState struct {
	bundle []byte
	opts   x509.VerifyOptions
}

func newFakeCA(t testing.TB, cas ...*issued) *fakeCA {
	f := &fakeCA{}
	f.set(t, cas...)
	return f
}

// set installs a new pool, as a CA rotation does, even for the same CAs.
func (f *fakeCA) set(t testing.TB, cas ...*issued) {
	t.Helper()
	pool := x509.NewCertPool()
	var bundle []byte
	for _, ca := range cas {
		pool.AddCert(ca.cert)
		bundle = append(bundle, ca.pem...)
	}
	f.cur.Store(&caState{bundle: bundle, opts: x509.VerifyOptions{
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}})
}

func (f *fakeCA) Name() string                              { return "fake" }
func (f *fakeCA) CurrentCABundleContent() []byte            { return f.cur.Load().bundle }
func (f *fakeCA) VerifyOptions() (x509.VerifyOptions, bool) { return f.cur.Load().opts, true }
func (f *fakeCA) AddListener(dynamiccertificates.Listener)  {}

var _ dynamiccertificates.CAContentProvider = (*fakeCA)(nil)

// names is a swappable allowed-names provider.
type names struct{ v atomic.Pointer[[]string] }

func newNames(n ...string) *names { x := &names{}; x.set(n...); return x }
func (n *names) set(v ...string)  { n.v.Store(&v) }
func (n *names) Value() []string  { return *n.v.Load() }

func requestHeaderConfig(ca dynamiccertificates.CAContentProvider, allowed headerrequest.StringSliceProvider) *authenticatorfactory.RequestHeaderConfig {
	return &authenticatorfactory.RequestHeaderConfig{
		UsernameHeaders:     headerrequest.StaticStringSlice{"X-Remote-User"},
		UIDHeaders:          headerrequest.StaticStringSlice{"X-Remote-Uid"},
		GroupHeaders:        headerrequest.StaticStringSlice{"X-Remote-Group"},
		ExtraHeaderPrefixes: headerrequest.StaticStringSlice{"X-Remote-Extra-"},
		CAContentProvider:   ca,
		AllowedClientNames:  allowed,
	}
}

// upstreamAuthenticator builds the authenticator exactly as
// DelegatingAuthenticationOptions.ApplyTo does when only a front proxy is
// configured.
func upstreamAuthenticator(t testing.TB, cfg *authenticatorfactory.RequestHeaderConfig) authenticator.Request {
	t.Helper()
	a, _, err := authenticatorfactory.DelegatingAuthenticatorConfig{
		Anonymous:           &apiserver.AnonymousAuthConfig{Enabled: true},
		RequestHeaderConfig: cfg,
	}.New()
	if err != nil {
		t.Fatalf("build upstream authenticator: %v", err)
	}
	return a
}

func proxiedRequest(peers []*x509.Certificate, withUser bool) *http.Request {
	req, _ := http.NewRequest(http.MethodGet, "https://quota.example/apis", nil)
	if peers != nil {
		req.TLS = &tls.ConnectionState{PeerCertificates: peers}
	}
	if withUser {
		req.Header.Set("X-Remote-User", "alice")
		req.Header.Set("X-Remote-Uid", "uid-1")
		req.Header.Add("X-Remote-Group", "devs")
		req.Header.Set("X-Remote-Extra-Scope", "s1")
	}
	return req
}

type result struct {
	resp   *authenticator.Response
	ok     bool
	hasErr bool
	header http.Header
}

func run(a authenticator.Request, req *http.Request) result {
	resp, ok, err := a.AuthenticateRequest(req)
	return result{resp: resp, ok: ok, hasErr: err != nil, header: req.Header}
}

// TestWrapMatchesUpstream checks that for each kind of request the wrapped
// authenticator gives the answer the upstream authenticator gives, on the
// first call (cache miss) and on repeat calls (cache hit).
func TestWrapMatchesUpstream(t *testing.T) {
	now := time.Now()
	valid := certSpec{notBefore: now.Add(-time.Hour), notAfter: now.Add(time.Hour)}
	ca := issue(t, certSpec{cn: "proxy-ca", isCA: true, notBefore: valid.notBefore, notAfter: valid.notAfter}, nil)
	otherCA := issue(t, certSpec{cn: "other-ca", isCA: true, notBefore: valid.notBefore, notAfter: valid.notAfter}, nil)
	inter := issue(t, certSpec{cn: "proxy-intermediate", isCA: true, notBefore: valid.notBefore, notAfter: valid.notAfter}, ca)
	// Same subject as the trusted CA, different key.
	imposter := issue(t, certSpec{cn: "proxy-ca", isCA: true, notBefore: valid.notBefore, notAfter: valid.notAfter}, nil)

	leaf := func(cn string, parent *issued) *x509.Certificate {
		s := valid
		s.cn = cn
		return issue(t, s, parent).cert
	}
	proxyLeaf := leaf(proxyCN, ca)
	viaInter := leaf(proxyCN, inter)

	tests := []struct {
		name     string
		peers    []*x509.Certificate
		withUser bool
		allowed  []string
		wantOK   bool
	}{
		{name: "proxy cert with user headers", peers: []*x509.Certificate{proxyLeaf}, withUser: true, allowed: []string{proxyCN}, wantOK: true},
		{name: "proxy cert, any name allowed", peers: []*x509.Certificate{proxyLeaf}, withUser: true, wantOK: true},
		{name: "proxy cert via intermediate", peers: []*x509.Certificate{viaInter, inter.cert}, withUser: true, allowed: []string{proxyCN}, wantOK: true},
		{name: "intermediate not sent", peers: []*x509.Certificate{viaInter}, withUser: true, allowed: []string{proxyCN}},
		{name: "no TLS", withUser: true, allowed: []string{proxyCN}, wantOK: true},
		{name: "proxy cert without user header", peers: []*x509.Certificate{proxyLeaf}, allowed: []string{proxyCN}, wantOK: true},
		{name: "cert from another CA", peers: []*x509.Certificate{leaf(proxyCN, otherCA)}, withUser: true, allowed: []string{proxyCN}},
		{name: "cert from an imposter CA with the trusted CA's name", peers: []*x509.Certificate{leaf(proxyCN, imposter)}, withUser: true, allowed: []string{proxyCN}},
		{name: "imposter CA sent as intermediate", peers: []*x509.Certificate{leaf(proxyCN, imposter), imposter.cert}, withUser: true, allowed: []string{proxyCN}},
		{name: "common name not allowed", peers: []*x509.Certificate{leaf("someone-else", ca)}, withUser: true, allowed: []string{proxyCN}},
		{name: "self-signed leaf", peers: []*x509.Certificate{leaf(proxyCN, nil)}, withUser: true, allowed: []string{proxyCN}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := requestHeaderConfig(newFakeCA(t, ca), headerrequest.StaticStringSlice(tt.allowed))
			upstream := upstreamAuthenticator(t, cfg)
			wrapped := Wrap(upstreamAuthenticator(t, cfg), cfg, Options{TTL: time.Minute})

			for i := range 3 {
				want := run(upstream, proxiedRequest(tt.peers, tt.withUser))
				got := run(wrapped, proxiedRequest(tt.peers, tt.withUser))
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("call %d: wrapped = %+v, upstream = %+v", i, got, want)
				}
				if got.ok != tt.wantOK {
					t.Fatalf("call %d: ok = %v, want %v (resp %+v)", i, got.ok, tt.wantOK, got.resp)
				}
			}
		})
	}
}

func TestWrapDisabledOrUnconfigured(t *testing.T) {
	upstream := upstreamAuthenticator(t, nil)
	ca := newFakeCA(t, issue(t, certSpec{cn: "ca", isCA: true, notBefore: time.Now().Add(-time.Hour), notAfter: time.Now().Add(time.Hour)}, nil))
	for name, tc := range map[string]struct {
		cfg  *authenticatorfactory.RequestHeaderConfig
		opts Options
	}{
		"ttl zero":          {cfg: requestHeaderConfig(ca, newNames()), opts: Options{}},
		"no requestheader":  {cfg: nil, opts: Options{TTL: time.Minute}},
		"nil allowed names": {cfg: requestHeaderConfig(ca, nil), opts: Options{TTL: time.Minute}},
	} {
		t.Run(name, func(t *testing.T) {
			if got := Wrap(upstream, tc.cfg, tc.opts); got != upstream {
				t.Fatalf("Wrap returned %T, want upstream unchanged", got)
			}
		})
	}
}

// harness drives a cachingVerifier directly with a fake clock and counts
// real verifications.
type harness struct {
	v        *cachingVerifier
	clock    *clocktesting.FakeClock
	ca       *fakeCA
	allowed  *names
	verifies atomic.Int64
}

func newHarness(t testing.TB, start time.Time, ttl time.Duration, roots ...*issued) *harness {
	h := &harness{
		clock:   clocktesting.NewFakeClock(start),
		ca:      newFakeCA(t, roots...),
		allowed: newNames(proxyCN),
	}
	h.v = newCachingVerifier(h.ca.VerifyOptions, h.allowed, Options{TTL: ttl, MaxEntries: 8, Clock: h.clock})
	h.v.delegate = headerrequest.NewDynamic(
		headerrequest.StaticStringSlice{"X-Remote-User"},
		headerrequest.StaticStringSlice{"X-Remote-Uid"},
		headerrequest.StaticStringSlice{"X-Remote-Group"},
		headerrequest.StaticStringSlice{"X-Remote-Extra-"},
	)
	h.v.verify = func(leaf *x509.Certificate, opts x509.VerifyOptions) ([][]*x509.Certificate, error) {
		h.verifies.Add(1)
		return leaf.Verify(opts)
	}
	return h
}

func (h *harness) auth(peers ...*x509.Certificate) error {
	_, ok, err := h.v.AuthenticateRequest(proxiedRequest(peers, true))
	if err == nil && !ok {
		panic("verifier returned no opinion for a request with user headers")
	}
	return err
}

func TestCachingVerifier(t *testing.T) {
	start := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	window := certSpec{notBefore: start.Add(-time.Hour), notAfter: start.Add(24 * time.Hour)}
	ca := issue(t, certSpec{cn: "proxy-ca", isCA: true, notBefore: start.Add(-48 * time.Hour), notAfter: start.Add(48 * time.Hour)}, nil)
	newCA := issue(t, certSpec{cn: "proxy-ca-2", isCA: true, notBefore: start.Add(-48 * time.Hour), notAfter: start.Add(48 * time.Hour)}, nil)
	// The intermediate expires before the leaf it signs.
	inter := issue(t, certSpec{cn: "proxy-intermediate", isCA: true, notBefore: start.Add(-time.Hour), notAfter: start.Add(2 * time.Hour)}, ca)

	leafOf := func(cn string, parent *issued) *x509.Certificate {
		s := window
		s.cn = cn
		return issue(t, s, parent).cert
	}
	leaf := leafOf(proxyCN, ca)
	leaf2 := leafOf(proxyCN, ca)
	interLeaf := leafOf(proxyCN, inter)
	foreign := leafOf(proxyCN, newCA)

	type step struct {
		do           func(h *harness)
		peers        []*x509.Certificate
		wantErr      bool
		wantVerifies int64
	}
	at := func(d time.Duration) func(h *harness) { return func(h *harness) { h.clock.SetTime(start.Add(d)) } }

	tests := []struct {
		name string
		// ttl defaults to longer than any certificate in the test, so a
		// rejection after time moves comes from the validity check and
		// not from the entry expiring.
		ttl   time.Duration
		steps []step
	}{
		{
			name: "second request is served from the cache",
			steps: []step{
				{peers: []*x509.Certificate{leaf}, wantVerifies: 1},
				{peers: []*x509.Certificate{leaf}, wantVerifies: 1},
				{do: at(time.Minute), peers: []*x509.Certificate{leaf}, wantVerifies: 1},
			},
		},
		{
			name: "a different leaf from the same CA is verified on its own",
			steps: []step{
				{peers: []*x509.Certificate{leaf}, wantVerifies: 1},
				{peers: []*x509.Certificate{leaf2}, wantVerifies: 2},
				{peers: []*x509.Certificate{leaf2}, wantVerifies: 2},
			},
		},
		{
			name: "CA rotation that drops the issuing CA rejects a cached leaf",
			steps: []step{
				{peers: []*x509.Certificate{leaf}, wantVerifies: 1},
				{do: func(h *harness) { h.ca.set(t, newCA) }, peers: []*x509.Certificate{leaf}, wantErr: true, wantVerifies: 2},
				{peers: []*x509.Certificate{foreign}, wantVerifies: 3},
			},
		},
		{
			name: "CA reload with the same CA re-verifies once",
			steps: []step{
				{peers: []*x509.Certificate{leaf}, wantVerifies: 1},
				{do: func(h *harness) { h.ca.set(t, ca, newCA) }, peers: []*x509.Certificate{leaf}, wantVerifies: 2},
				{peers: []*x509.Certificate{leaf}, wantVerifies: 2},
			},
		},
		{
			name: "expired leaf is rejected even though it is cached",
			steps: []step{
				{peers: []*x509.Certificate{leaf}, wantVerifies: 1},
				{do: at(24*time.Hour + time.Second), peers: []*x509.Certificate{leaf}, wantErr: true, wantVerifies: 2},
			},
		},
		{
			name: "not-yet-valid leaf is rejected even though it is cached",
			steps: []step{
				{peers: []*x509.Certificate{leaf}, wantVerifies: 1},
				{do: at(-2 * time.Hour), peers: []*x509.Certificate{leaf}, wantErr: true, wantVerifies: 2},
			},
		},
		{
			name: "expired intermediate rejects a cached chain",
			steps: []step{
				{peers: []*x509.Certificate{interLeaf, inter.cert}, wantVerifies: 1},
				{peers: []*x509.Certificate{interLeaf, inter.cert}, wantVerifies: 1},
				{do: at(3 * time.Hour), peers: []*x509.Certificate{interLeaf, inter.cert}, wantErr: true, wantVerifies: 2},
			},
		},
		{
			name: "leaf from the wrong CA is rejected and never cached",
			steps: []step{
				{peers: []*x509.Certificate{foreign}, wantErr: true, wantVerifies: 1},
				{peers: []*x509.Certificate{foreign}, wantErr: true, wantVerifies: 2},
			},
		},
		{
			name: "allowed names are enforced on cache hits",
			steps: []step{
				{peers: []*x509.Certificate{leaf}, wantVerifies: 1},
				{do: func(h *harness) { h.allowed.set("another-proxy") }, peers: []*x509.Certificate{leaf}, wantErr: true, wantVerifies: 1},
				{do: func(h *harness) { h.allowed.set() }, peers: []*x509.Certificate{leaf}, wantVerifies: 1},
			},
		},
		{
			name: "entry expires after the TTL",
			ttl:  5 * time.Minute,
			steps: []step{
				{peers: []*x509.Certificate{leaf}, wantVerifies: 1},
				{do: at(6 * time.Minute), peers: []*x509.Certificate{leaf}, wantVerifies: 2},
				{peers: []*x509.Certificate{leaf}, wantVerifies: 2},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ttl := tt.ttl
			if ttl == 0 {
				ttl = 30 * 24 * time.Hour
			}
			h := newHarness(t, start, ttl, ca)
			for i, s := range tt.steps {
				if s.do != nil {
					s.do(h)
				}
				err := h.auth(s.peers...)
				if (err != nil) != s.wantErr {
					t.Fatalf("step %d: err = %v, wantErr %v", i, err, s.wantErr)
				}
				if got := h.verifies.Load(); got != s.wantVerifies {
					t.Fatalf("step %d: %d verifications, want %d", i, got, s.wantVerifies)
				}
			}
		})
	}
}

func TestCachingVerifierUserInfo(t *testing.T) {
	start := time.Now()
	ca := issue(t, certSpec{cn: "proxy-ca", isCA: true, notBefore: start.Add(-time.Hour), notAfter: start.Add(time.Hour)}, nil)
	leaf := issue(t, certSpec{cn: proxyCN, notBefore: start.Add(-time.Hour), notAfter: start.Add(time.Hour)}, ca).cert
	h := newHarness(t, start, DefaultTTL, ca)
	for range 2 {
		req := proxiedRequest([]*x509.Certificate{leaf}, true)
		resp, ok, err := h.v.AuthenticateRequest(req)
		if err != nil || !ok {
			t.Fatalf("AuthenticateRequest = %v, %v", ok, err)
		}
		u := resp.User
		if u.GetName() != "alice" || u.GetUID() != "uid-1" || !reflect.DeepEqual(u.GetGroups(), []string{"devs"}) ||
			!reflect.DeepEqual(u.GetExtra(), map[string][]string{"scope": {"s1"}}) {
			t.Fatalf("user = %+v", u)
		}
		if req.Header.Get("X-Remote-User") != "" {
			t.Fatalf("auth headers were not cleared")
		}
	}
}

func TestCachingVerifierConcurrent(t *testing.T) {
	start := time.Now()
	ca := issue(t, certSpec{cn: "proxy-ca", isCA: true, notBefore: start.Add(-time.Hour), notAfter: start.Add(time.Hour)}, nil)
	otherCA := issue(t, certSpec{cn: "other-ca", isCA: true, notBefore: start.Add(-time.Hour), notAfter: start.Add(time.Hour)}, nil)
	good := issue(t, certSpec{cn: proxyCN, notBefore: start.Add(-time.Hour), notAfter: start.Add(time.Hour)}, ca).cert
	bad := issue(t, certSpec{cn: proxyCN, notBefore: start.Add(-time.Hour), notAfter: start.Add(time.Hour)}, otherCA).cert
	h := newHarness(t, start, DefaultTTL, ca)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	// Reload the CA (same contents, new pool) while requests run.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				h.ca.set(t, ca)
				time.Sleep(time.Millisecond)
			}
		}
	}()
	var failures atomic.Int64
	var workers sync.WaitGroup
	for w := range 16 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range 200 {
				if w%4 == 0 {
					if h.auth(bad) == nil {
						failures.Add(1)
					}
					continue
				}
				if h.auth(good) != nil {
					failures.Add(1)
				}
			}
		}()
	}
	workers.Wait()
	close(stop)
	wg.Wait()
	if n := failures.Load(); n != 0 {
		t.Fatalf("%d wrong answers under concurrency", n)
	}
}

// BenchmarkFrontProxyAuthentication measures one proxied request through the
// authenticator ApplyTo builds, with and without the cache.
func BenchmarkFrontProxyAuthentication(b *testing.B) {
	now := time.Now()
	ca := issue(b, certSpec{cn: "proxy-ca", isCA: true, notBefore: now.Add(-time.Hour), notAfter: now.Add(time.Hour)}, nil)
	leaf := issue(b, certSpec{cn: proxyCN, notBefore: now.Add(-time.Hour), notAfter: now.Add(time.Hour)}, ca).cert
	cfg := requestHeaderConfig(newFakeCA(b, ca), headerrequest.StaticStringSlice{proxyCN})
	upstream := upstreamAuthenticator(b, cfg)

	for name, a := range map[string]authenticator.Request{
		"upstream": upstream,
		"cached":   Wrap(upstream, cfg, Options{TTL: DefaultTTL}),
	} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, ok, err := a.AuthenticateRequest(proxiedRequest([]*x509.Certificate{leaf}, true)); !ok || err != nil {
					b.Fatalf("AuthenticateRequest = %v, %v", ok, err)
				}
			}
		})
		// The cache is one mutex-guarded LRU; this shows it under
		// contention.
		b.Run(name+"-parallel", func(b *testing.B) {
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if _, ok, err := a.AuthenticateRequest(proxiedRequest([]*x509.Certificate{leaf}, true)); !ok || err != nil {
						b.Errorf("AuthenticateRequest = %v, %v", ok, err)
						return
					}
				}
			})
		})
	}
}
