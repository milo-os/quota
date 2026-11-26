package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"k8s.io/component-base/cli"

	"go.miloapis.com/quota/internal/version"
)

func main() {
	command := NewQuotaServerCommand()
	code := cli.Run(command)
	os.Exit(code)
}

// NewQuotaServerCommand creates the root command with subcommands for the quota server.
func NewQuotaServerCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "quota",
		Short: "Quota service apiserver",
		Long: `Quota is a standalone Kubernetes-native quota enforcement service.

It provides transactional quota allocation through QuotaDefinition, QuotaGrant,
QuotaClaim, and QuotaBucket resources exposed as an aggregated API server.`,
	}

	cmd.AddCommand(NewServeCommand())
	cmd.AddCommand(NewControllerManagerCommand())
	cmd.AddCommand(NewVersionCommand())

	return cmd
}

// NewVersionCommand creates the version subcommand to display build information.
func NewVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Show version information",
		Long:  `Show the version, git commit, and build details.`,
		Run: func(cmd *cobra.Command, args []string) {
			info := version.Get()
			fmt.Printf("Quota Server\n")
			fmt.Printf("  Version:       %s\n", info.Version)
			fmt.Printf("  Git Commit:    %s\n", info.GitCommit)
			fmt.Printf("  Git Tree:      %s\n", info.GitTreeState)
			fmt.Printf("  Build Date:    %s\n", info.BuildDate)
			fmt.Printf("  Go Version:    %s\n", info.GoVersion)
			fmt.Printf("  Go Compiler:   %s\n", info.Compiler)
			fmt.Printf("  Platform:      %s\n", info.Platform)
		},
	}
}
