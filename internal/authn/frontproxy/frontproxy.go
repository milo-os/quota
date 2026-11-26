// Package frontproxy adds a verification cache in front of the delegated
// authenticator's front-proxy (requestheader) path.
//
// Upstream k8s.io/apiserver builds the front-proxy authenticator with
// headerrequest.NewDynamicVerifyOptionsSecure, which wraps the header
// extractor in an x509.Verifier. That verifier runs
// (*x509.Certificate).Verify against the requestheader client CA on every
// request, so every request proxied by kube-apiserver pays for an RSA
// signature check, even though HTTP/2 reuses one TLS connection, and so one
// peer certificate, for many requests.
//
// Wrap puts a fast path in front of the upstream authenticator. The fast path
// does exactly what the upstream front-proxy authenticator does (same verify
// options, same intermediates handling, same allowed-names check, the upstream
// header extractor and the upstream authenticated-group adder), except that a
// successful chain verification is remembered. When the fast path does not
// authenticate a request, for any reason, the request goes to the unchanged
// upstream authenticator, so failures, client-certificate users, bearer
// tokens and anonymous requests behave exactly as before.
package frontproxy

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"net/http"
	"slices"
	"time"

	utilcache "k8s.io/apimachinery/pkg/util/cache"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/authenticatorfactory"
	"k8s.io/apiserver/pkg/authentication/group"
	"k8s.io/apiserver/pkg/authentication/request/headerrequest"
	x509request "k8s.io/apiserver/pkg/authentication/request/x509"
	"k8s.io/utils/clock"
)

const (
	// DefaultTTL bounds how long a successful verification is reused. Cert
	// validity and CA identity are checked on every request regardless, so
	// the TTL only bounds memory held for certificates no longer presented.
	DefaultTTL = 10 * time.Minute
	// DefaultMaxEntries bounds the cache. Each kube-apiserver replica presents
	// one proxy client certificate, so a handful of entries covers rotation.
	DefaultMaxEntries = 256
)

// Options configures the verification cache.
type Options struct {
	// TTL is how long a successful verification may be reused. Zero or
	// negative disables the cache and Wrap returns the upstream authenticator.
	TTL time.Duration
	// MaxEntries bounds the number of cached verifications. Zero uses
	// DefaultMaxEntries.
	MaxEntries int
	// Clock is used for cache expiry and certificate validity. Nil uses the
	// real clock.
	Clock clock.PassiveClock
}

// Wrap returns an authenticator that tries a cached front-proxy
// authentication first and otherwise delegates to upstream unchanged.
//
// upstream must be the authenticator built by
// DelegatingAuthenticationOptions.ApplyTo from the same cfg. If cfg is nil
// (no front-proxy configured) or the cache is disabled, upstream is returned
// as is.
func Wrap(upstream authenticator.Request, cfg *authenticatorfactory.RequestHeaderConfig, opts Options) authenticator.Request {
	if upstream == nil || cfg == nil || opts.TTL <= 0 {
		return upstream
	}
	// Upstream would fail on a nil provider; leave such a config entirely to
	// upstream rather than guess at a default.
	if cfg.CAContentProvider == nil || cfg.AllowedClientNames == nil || cfg.UsernameHeaders == nil ||
		cfg.UIDHeaders == nil || cfg.GroupHeaders == nil || cfg.ExtraHeaderPrefixes == nil {
		return upstream
	}
	v := newCachingVerifier(cfg.CAContentProvider.VerifyOptions, cfg.AllowedClientNames, opts)
	v.delegate = headerrequest.NewDynamic(cfg.UsernameHeaders, cfg.UIDHeaders, cfg.GroupHeaders, cfg.ExtraHeaderPrefixes)
	return &authn{
		// DelegatingAuthenticatorConfig.New wraps its union in the
		// authenticated-group adder; the fast path needs the same so a
		// cached success yields the same user.Info.
		fast:     group.NewAuthenticatedGroupAdder(v),
		upstream: upstream,
	}
}

type authn struct {
	fast     authenticator.Request
	upstream authenticator.Request
}

// AuthenticateRequest implements authenticator.Request.
func (a *authn) AuthenticateRequest(req *http.Request) (*authenticator.Response, bool, error) {
	// The front-proxy authenticator is first in the upstream union, so when
	// it succeeds upstream returns its answer without consulting anything
	// else. The fast path returning the same answer is therefore equivalent.
	// The header extractor only strips auth headers on success, so a fast
	// path miss leaves the request untouched for upstream.
	if resp, ok, err := a.fast.AuthenticateRequest(req); err == nil && ok {
		return resp, true, nil
	}
	return a.upstream.AuthenticateRequest(req)
}

// verifyFunc is (*x509.Certificate).Verify, replaceable in tests.
type verifyFunc func(leaf *x509.Certificate, opts x509.VerifyOptions) ([][]*x509.Certificate, error)

func defaultVerify(leaf *x509.Certificate, opts x509.VerifyOptions) ([][]*x509.Certificate, error) {
	return leaf.Verify(opts)
}

// cachingVerifier mirrors x509request.Verifier with a cache of successful
// chain verifications.
type cachingVerifier struct {
	verifyOptionsFn    x509request.VerifyOptionFunc
	allowedCommonNames headerrequest.StringSliceProvider
	delegate           authenticator.Request

	cache *utilcache.LRUExpireCache
	ttl   time.Duration
	clock clock.PassiveClock
	// verify is (*x509.Certificate).Verify outside tests.
	verify verifyFunc
}

func newCachingVerifier(fn x509request.VerifyOptionFunc, allowed headerrequest.StringSliceProvider, opts Options) *cachingVerifier {
	c := opts.Clock
	if c == nil {
		c = clock.RealClock{}
	}
	maxEntries := opts.MaxEntries
	if maxEntries <= 0 {
		maxEntries = DefaultMaxEntries
	}
	return &cachingVerifier{
		verifyOptionsFn:    fn,
		allowedCommonNames: allowed,
		cache:              utilcache.NewLRUExpireCacheWithClock(maxEntries, c),
		ttl:                opts.TTL,
		clock:              c,
		verify:             defaultVerify,
	}
}

// cacheKey is the SHA-256 of the presented chain: the leaf followed by the
// intermediates the client sent, each length-prefixed.
type cacheKey [sha256.Size]byte

// validityWindow is the span in which every certificate in one verified
// chain is valid, inclusive at both ends as in crypto/x509.
type validityWindow struct {
	notBefore time.Time
	notAfter  time.Time
}

// cacheEntry records one successful verification and the verify options it
// was made with. An entry only counts when the options in use now are the
// same: a CA rotation produces a new Roots pool, so the entry stops matching.
type cacheEntry struct {
	// roots and intermediates are held by pointer. Holding the pointer keeps
	// the pool alive, so its address cannot be reused by a later pool while
	// the entry exists.
	roots          *x509.CertPool
	intermediates  *x509.CertPool
	keyUsages      []x509.ExtKeyUsage
	dnsName        string
	maxConstraints int
	// windows has one entry per chain Verify returned.
	windows []validityWindow
}

func (e *cacheEntry) matches(opts *x509.VerifyOptions) bool {
	return e.roots == opts.Roots &&
		e.intermediates == opts.Intermediates &&
		e.dnsName == opts.DNSName &&
		e.maxConstraints == opts.MaxConstraintComparisions &&
		slices.Equal(e.keyUsages, opts.KeyUsages)
}

// validAt reports whether at least one verified chain is entirely within its
// validity period at now. That is the condition crypto/x509 checks on each
// certificate while building chains.
func (e *cacheEntry) validAt(now time.Time) bool {
	for _, w := range e.windows {
		if !now.Before(w.notBefore) && !now.After(w.notAfter) {
			return true
		}
	}
	return false
}

func keyFor(certs []*x509.Certificate) cacheKey {
	h := sha256.New()
	var n [8]byte
	for _, c := range certs {
		binary.BigEndian.PutUint64(n[:], uint64(len(c.Raw)))
		h.Write(n[:])
		h.Write(c.Raw)
	}
	var k cacheKey
	h.Sum(k[:0])
	return k
}

// AuthenticateRequest follows x509request.Verifier.AuthenticateRequest step
// for step; only the Verify call goes through the cache.
func (v *cachingVerifier) AuthenticateRequest(req *http.Request) (*authenticator.Response, bool, error) {
	if req.TLS == nil || len(req.TLS.PeerCertificates) == 0 {
		return nil, false, nil
	}

	optsCopy, ok := v.verifyOptionsFn()
	if !ok {
		return nil, false, nil
	}
	if err := v.verifyChain(req.TLS.PeerCertificates, optsCopy); err != nil {
		return nil, false, err
	}
	if err := v.verifySubject(req.TLS.PeerCertificates[0].Subject.CommonName); err != nil {
		return nil, false, err
	}
	return v.delegate.AuthenticateRequest(req)
}

func (v *cachingVerifier) verifyChain(peers []*x509.Certificate, opts x509.VerifyOptions) error {
	now := opts.CurrentTime
	if now.IsZero() {
		now = v.clock.Now()
	}

	// A nil Roots means the system pool, which can change without the
	// pointer changing. Do not cache in that case.
	cacheable := opts.Roots != nil
	var key cacheKey
	if cacheable {
		key = keyFor(peers)
		if obj, found := v.cache.Get(key); found {
			if e := obj.(*cacheEntry); e.matches(&opts) && e.validAt(now) {
				return nil
			}
		}
	}

	// Record the options before intermediates from the request are added so
	// the entry compares against what the provider returns next time.
	entry := &cacheEntry{
		roots:          opts.Roots,
		intermediates:  opts.Intermediates,
		keyUsages:      opts.KeyUsages,
		dnsName:        opts.DNSName,
		maxConstraints: opts.MaxConstraintComparisions,
	}

	if opts.Intermediates == nil && len(peers) > 1 {
		opts.Intermediates = x509.NewCertPool()
		for _, intermediate := range peers[1:] {
			opts.Intermediates.AddCert(intermediate)
		}
	}
	// Pin the time Verify uses to the one the cache checks against. With
	// the real clock this is what Verify would use anyway.
	opts.CurrentTime = now

	chains, err := v.verify(peers[0], opts)
	if err != nil {
		return err
	}
	if !cacheable || len(chains) == 0 {
		return nil
	}
	for _, chain := range chains {
		entry.windows = append(entry.windows, windowOf(chain))
	}
	v.cache.Add(key, entry, v.ttl)
	return nil
}

func windowOf(chain []*x509.Certificate) validityWindow {
	w := validityWindow{notBefore: chain[0].NotBefore, notAfter: chain[0].NotAfter}
	for _, c := range chain[1:] {
		if c.NotBefore.After(w.notBefore) {
			w.notBefore = c.NotBefore
		}
		if c.NotAfter.Before(w.notAfter) {
			w.notAfter = c.NotAfter
		}
	}
	return w
}

// verifySubject is x509request.Verifier.verifySubject. It runs on every
// request, cached or not, against the current allowed names.
func (v *cachingVerifier) verifySubject(commonName string) error {
	allowed := v.allowedCommonNames.Value()
	if len(allowed) == 0 {
		return nil
	}
	for _, name := range allowed {
		if name == commonName {
			return nil
		}
	}
	return fmt.Errorf("x509: subject with cn=%s is not in the allowed list", commonName)
}
