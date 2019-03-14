package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"strings"
)

func main() {
	var copyShardCmd = &cobra.Command{
		Use:   "copy-shard <data-node-source-TCP-address> <data-node-destination-TCP-address> <shard-id>",
		Short: "copy shard",
		Long:  `copies a shard from a source data node to a destination data node.`,
		Args:  cobra.MinimumNArgs(3),
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Print: " + strings.Join(args, " "))
		},
	}

	var delay int
	var truncateCmd = &cobra.Command{
		Use:   "truncate-shards [-delay <seconds>]",
		Short: "truncates hot shards",
		Long: `Truncates hot shards, that is, shards that cover the time range that includes the current time (now()).
		The truncate-shards command creates a new shard and the system writes all new points to that shard.`,
		Args: cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Print: "+strings.Join(args, " "), delay)
		},
	}
	truncateCmd.Flags().IntVarP(&delay, "delay", "d", 1, "delay to truncate shards")

	var copyShardStatusCmd = &cobra.Command{
		Use:   "copy-shard-status",
		Short: "Displaying all in-progress copy-shard operations",
		Long: `Shows all in-progress copy shard operations, including the shard’s source node,
		destination node, database, retention policy, shard ID, total size,
		current size, and the operation’s start time.`,
		Args: cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Print: "+strings.Join(args, " "), delay)
		},
	}

	var rootCmd = &cobra.Command{Use: "influxd-ctl"}
	rootCmd.AddCommand(copyShardCmd, truncateCmd, copyShardStatusCmd)
	rootCmd.Execute()
}
