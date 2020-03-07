package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"os"
	"path/filepath"
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

	tsdbConn, err := tsdb.OpenDBReadOnly(cfg.ReadStoragePath, logger)
	if err != nil {
		fmt.Printf("Error when open tsdb connection in readonly mode, %s.\n", err)
		os.Exit(2)
	}
	ctx := context.Background()
	// retrieve the blocks
	blockReaders, err := tsdbConn.Blocks()
	if err != nil {
		fmt.Printf("Error when getting blocks from tsdb connection, %s.\n", err)

	}

	for _, blockReader := range blockReaders {
		blockMeta := blockReader.Meta()
		blockMinT := blockMeta.MinTime
		blockMaxT := blockMeta.MaxTime

		// get storage querier from block
		storageQuerier, err := tsdbConn.Querier(ctx, blockMinT, blockMinT+1)
		if err != nil {
			fmt.Printf("Error when creating storage querier from block %s, %s.\n", blockReader, err)

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
			var tsLables []prompb.Label

			for _, label := range labels {
				tsLables = append(tsLables, prompb.Label{
					Name:  label.Name,
					Value: label.Value,
				})
			}

			chunkIterator := series.Iterator()
			for chunkIterator.Next() {
				var tsSamples []prompb.Sample
				timeStamp, tsValue := chunkIterator.At()
				tsSamples = append(tsSamples, prompb.Sample{
					Timestamp: timeStamp,
					Value:     tsValue,
				})

				var ts []prompb.TimeSeries

				ts = append(ts, prompb.TimeSeries{
					Labels:  tsLables,
					Samples: tsSamples})
				err := storeMetrics(cfg.WriteRemoteURL, ts)
				if err != nil {
					fmt.Println(err)
				}
			}

		}

	}
}

func storeMetrics(url string, ts []prompb.TimeSeries) error {
	//var userAgent = fmt.Sprintf("Prometheus/%s", version.Version)
	fmt.Printf("saving metrics %+v\n", ts)
	tsRequest := &prompb.WriteRequest{
		Timeseries: ts,
	}
	tsRequestData, err := proto.Marshal(tsRequest)
	if err != nil {
		fmt.Errorf("unable to marshal protobuf: %v", err)
	}
	tsRequestBody := snappy.Encode(nil, tsRequestData)
	httpRequest, err := http.NewRequest("POST", url, bytes.NewReader(tsRequestBody))

	if err != nil {
		return err
	}
	httpRequest.Header.Set("Content-Type", "application/x-protobuf")
	//httpRequest.Header.Set("User-Agent", userAgent)

	client := &http.Client{
		Timeout: 20 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}
	ctx := context.Background()
	defer ctx.Done()

	fmt.Printf("http request %+v\n", httpRequest)
	httpResponse, err := client.Do(httpRequest.WithContext(ctx))

	if err != nil {
		fmt.Println(err)
		return err
	}
	defer httpResponse.Body.Close()
	compressedBody, err := ioutil.ReadAll(httpResponse.Body)
	if err != nil {
		fmt.Printf("err %s\n", err)
	}

	uncompressedBody, err := snappy.Decode(nil, compressedBody)

	var prompbResp prompb.ReadResponse
	err = proto.Unmarshal(uncompressedBody, &prompbResp)
	if err != nil {
		return errors.Wrap(err, "unable to unmarshal response body")
	}

	fmt.Printf("Http response body %+v\n", prompbResp)
	return nil

}
