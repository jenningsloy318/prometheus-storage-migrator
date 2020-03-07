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
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"go.etcd.io/etcd/version"
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
		storageQuerier, err := tsdbConn.Querier(ctx, blockMinT, blockMinT+1)
		if err != nil {
			fmt.Errorf("Error when creating storage querier from block %s, %s", blockReader, err)

		}
		// Get seriesSet from storageQuerier
		labelSelectParams := &storage.SelectParams{
			Start: blockMinT,
			End:   blockMaxT,
		}
		labelMatcher, err := labels.NewMatcher(labels.MatchRegexp, "__name__", ".+")

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
	var userAgent = fmt.Sprintf("Prometheus/%s", version.Version)
	tsRequest := &prompb.WriteRequest{
		Timeseries: ts,
	}
	tsRequestData, err := proto.Marshal(tsRequest)
	if err != nil {
		fmt.Errorf("unable to marshal protobuf: %v", err)
	}
	tsRequestBody := snappy.Encode(nil, tsRequestData)
	body := bytes.NewReader(tsRequestBody)
	httpRequest, err := http.NewRequest("POST", url, body)

	if err != nil {
		return err
	}

	httpRequest.Header.Set("Content-Type", "application/x-protobuf")
	httpRequest.Header.Set("Content-Encoding", "snappy")
	httpRequest.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	httpRequest.Header.Set("User-Agent", userAgent)

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

	httpResponse, err := client.Do(httpRequest.WithContext(ctx))

	if err != nil {
		fmt.Println(err)
		return err
	}

	statusCode := httpResponse.StatusCode
	if statusCode/100 != 2 {
		fmt.Errorf("expected HTTP 200 status code: actual=%d", statusCode)
	}

	rawResponseBody, err := ioutil.ReadAll(httpResponse.Body)
	if err != nil {
		fmt.Errorf("Error when getting the response body from http response %s", err)
	}

	defer httpResponse.Body.Close()

	ResponseBody, err := snappy.Decode(nil, rawResponseBody)

	var prompbResp prompb.ReadResponse
	err = proto.Unmarshal(ResponseBody, &prompbResp)
	if err != nil {
		return fmt.Errorf("unable to unmarshal response body, %s", err)
	}

	return nil

}
