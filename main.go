package main

import (
	"log"
	"os"

	"bsc_stats/collecttop"
)

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)

	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	switch cmd {
	case "collect-top":
		collecttop.Run(args)
	default:
		log.Printf("unknown subcommand %q", cmd)
		usage()
		os.Exit(2)
	}
}

func usage() {
	log.Printf("usage: %s <subcommand> [flags]\n\nsubcommands:\n  collect-top  scan a block/date range and report top-N transaction senders",
		os.Args[0])
}
