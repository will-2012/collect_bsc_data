package main

import (
	"log"
	"os"

	"bsc_stats/collecttop"
	"bsc_stats/importmysql"
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
	case "import-mysql":
		importmysql.Run(args)
	default:
		log.Printf("unknown subcommand %q", cmd)
		usage()
		os.Exit(2)
	}
}

func usage() {
	log.Printf("usage: %s <subcommand> [flags]\n\nsubcommands:\n  collect-top   scan a block/date range and report top-N transaction senders\n  import-mysql  import block/tx data into MySQL for time-range gas-share queries",
		os.Args[0])
}
