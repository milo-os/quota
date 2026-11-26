// Command pgsql runs SQL against Postgres for test/load/ci/linux-sweep.sh on
// machines without psql, such as the self-hosted CI runners.
//
//	pgsql <dsn> -f <file>   run the file as one multi-statement script
//	pgsql <dsn> -c <sql>    run the SQL and print each row, tab-separated
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func main() {
	if len(os.Args) != 4 || (os.Args[2] != "-f" && os.Args[2] != "-c") {
		fmt.Fprintln(os.Stderr, "usage: pgsql <dsn> -f <file> | -c <sql>")
		os.Exit(2)
	}
	if err := run(os.Args[1], os.Args[2], os.Args[3]); err != nil {
		fmt.Fprintln(os.Stderr, "pgsql:", err)
		os.Exit(1)
	}
}

func run(dsn, mode, arg string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	conn, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()

	sql := arg
	if mode == "-f" {
		b, err := os.ReadFile(arg)
		if err != nil {
			return err
		}
		sql = string(b)
	}
	// The simple query protocol runs several statements in one call.
	results, err := conn.Exec(ctx, sql).ReadAll()
	if err != nil {
		return err
	}
	if mode == "-c" {
		for _, r := range results {
			for _, row := range r.Rows {
				cols := make([]string, len(row))
				for i, v := range row {
					cols[i] = string(v)
				}
				fmt.Println(strings.Join(cols, "\t"))
			}
		}
	}
	return nil
}
