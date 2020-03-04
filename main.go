package main

import (
	"context"
	"fmt"

	"os"
	"path/filepath"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

func main() {

	//compose the cfg
	cfg := struct {
		ReadStoragePath string
		WriteRemoteURL  string
	}{}

	// compose the options for the commandline
	Opts := kingpin.New(filepath.Base(os.Args[0]), "The prometheus storage migrator server")
	Opts.Flag("write.remote.url", "The remote write endpoint").Default("http://localhost:7201/api/v1/prom/remote/write").StringVar(&cfg.WriteRemoteURL)
	Opts.Flag("read.storage.path", "Base path for metrics storage which metrics read from.").Default("data/").StringVar(&cfg.ReadStoragePath)
	Opts.HelpFlag.Short('h')
	_, err := Opts.Parse(os.Args[1:])

	if err != nil {
		fmt.Printf("Error parsing commandline arguments, %s.\n", err)
		os.Exit(2)
	}
	//create tsdb connection
	var logger log.Logger
	tsdbOpts := &tsdb.Options{}

	tsdbConn, err := tsdb.Open(cfg.ReadStoragePath, logger, prometheus.DefaultRegisterer, tsdbOpts)
	defer tsdbConn.Close()
	if err != nil {
		fmt.Printf("Error when open tsdb connection in readonly mode, %s.\n", err)
		os.Exit(2)
	}

	// retrieve the blocks
	for _, block := range tsdbConn.Blocks() {
		defer block.Close()
		blockMinT := block.MinTime()
		blockMaxT := block.MaxTime()

		// get storage querier from block
		ctx := context.Background()
		storageQuerier, err := tsdbConn.Querier(ctx, blockMinT, blockMaxT)
		if err != nil {
			fmt.Printf("Error when creating storage querier from block %s, %s.\n", block, err)

		}
		// Get seriesSet from storageQuerier
		labelSelectParams := &storage.SelectParams{
			Start: blockMinT,
			End:   blockMaxT,
		}
		labelMatcher, err := labels.NewMatcher(labels.MatchRegexp, "__name__", ".+")

		if err != nil {
			fmt.Printf("warnings when creating label Matcher %s\n", err)
		}

		timeSeriesSet, warnings, err := storageQuerier.Select(labelSelectParams, labelMatcher)

		if warnings != nil {
			fmt.Printf("warnings when getting time series set from storage querier ,%s", warnings)
		}
		if err != nil {
			fmt.Printf("Errors when getting time series set from storage querier,%s", err)
		}
		// get timeseries data from timeSeriesSet
		for timeSeriesSet.Next() {
			series := timeSeriesSet.At()
			labels := series.Labels()
			chunkIterator := series.Iterator()
			for chunkIterator.Next() {
				timeStamp, seriesValue := chunkIterator.At()
				fmt.Printf("{metric: %s,values: [[%d,%f]]}\n", labels, timeStamp, seriesValue)
			}
			if !timeSeriesSet.Next() { // last loop, the value become flase, but for loop will not exit, so manually exit
				os.Exit(0)
			}
		}

	}
}
