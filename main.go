package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"os"
	"path/filepath"

	"github.com/go-kit/kit/log"
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/m3db/prometheus_remote_client_golang/promremote"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
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
		fmt.Errorf("Error parsing commandline arguments, %s", err)
		os.Exit(2)
	}
	//create tsdb connection
	var logger log.Logger

	tsdbConn, err := tsdb.OpenDBReadOnly(cfg.ReadStoragePath, logger)
	if err != nil {
		fmt.Errorf("Error when open tsdb connection in readonly mode %s", err)
		os.Exit(2)
	}
	ctx := context.Background()
	// retrieve the blocks
	blockReaders, err := tsdbConn.Blocks()
	if err != nil {
		fmt.Errorf("Error when getting blocks from tsdb connection, %s", err)

	}

	for _, blockReader := range blockReaders {
		blockMeta := blockReader.Meta()
		blockMinT := blockMeta.MinTime
		blockMaxT := blockMeta.MaxTime

		// get storage querier from block
		storageQuerier, err := tsdbConn.Querier(ctx, blockMinT, blockMaxT)
		if err != nil {
			fmt.Errorf("Error when creating storage querier from block %s, %s", blockReader, err)

		}
		// Get seriesSet from storageQuerier
		labelSelectParams := &storage.SelectParams{
			Start: blockMinT,
			End:   blockMaxT,
		}
		labelMatcher, err := labels.NewMatcher(labels.MatchRegexp, "instance", ".+")

		if err != nil {
			fmt.Errorf("warnings when creating label Matcher %s", err)
		}

		timeSeriesSet, warnings, err := storageQuerier.Select(labelSelectParams, labelMatcher)

		if warnings != nil {
			fmt.Errorf("warnings when getting time series set from storage querier ,%s", warnings)
		}
		if err != nil {
			fmt.Errorf("Errors when getting time series set from storage querier,%s", err)
		}
		// get timeseries data from timeSeriesSet
		for timeSeriesSet.Next() {
			series := timeSeriesSet.At()
			labels := series.Labels()
			var m3TsLabels []promremote.Label
			for _, label := range labels {
				m3TsLabels = append(m3TsLabels, promremote.Label{
					Name:  label.Name,
					Value: label.Value,
				})
			}
			chunkIterator := series.Iterator()
			for chunkIterator.Next() {
				var m3TimeSeries []promremote.TimeSeries
				timeStamp, tsValue := chunkIterator.At()
				m3TimeSeries = append(m3TimeSeries, promremote.TimeSeries{
					Labels: m3TsLabels,
					Datapoint: promremote.Datapoint{
						Timestamp: time.Unix(timeStamp, 0),
						Value:     tsValue},
				})
				err := storeToM3db(cfg.WriteRemoteURL, m3TimeSeries)
				if err != nil {
					fmt.Errorf("Errors when store metrics to m3db ,%s", err)

				}

			}
		}
	}

}

func storeToM3db(url string, ts []promremote.TimeSeries) error {
	cfg := promremote.NewConfig(
		promremote.WriteURLOption(url),
		promremote.HTTPClientTimeoutOption(60*time.Second),
		promremote.UserAgent("Prometheus/2.15.1"),
	)
	ctx := context.Background()
	defer ctx.Done()

	fmt.Printf("Store metric %#v\n", ts)
	var wrOpts = promremote.WriteOptions{}
	client, err := promremote.NewClient(cfg)
	if err != nil {
		fmt.Errorf("unable to construct client: %#v", err)
		return err
	}
	result, err := client.WriteTimeSeries(ctx, ts, wrOpts)
	if err != nil {
		fmt.Errorf("unable to construct client: %#v", err)
		return err
	}

	fmt.Printf("write result: %#v\n", result)

	return nil
}
