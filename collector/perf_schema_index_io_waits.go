// Copyright 2018 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Scrape `performance_schema.table_io_waits_summary_by_index_usage`.

package collector

import (
	"context"
	"database/sql"

	"github.com/prometheus/client_golang/prometheus"
)

const perfIndexIOWaitsQuery = `
	SELECT OBJECT_SCHEMA, OBJECT_NAME, ifnull(INDEX_NAME, 'NONE') as INDEX_NAME,
	    COUNT_FETCH, COUNT_INSERT, COUNT_UPDATE, COUNT_DELETE,
	    SUM_TIMER_FETCH, SUM_TIMER_INSERT, SUM_TIMER_UPDATE, SUM_TIMER_DELETE
	  FROM performance_schema.table_io_waits_summary_by_index_usage
	  WHERE OBJECT_SCHEMA NOT IN ('mysql', 'performance_schema')
	`

// Metric descriptors.
var (
	performanceSchemaIndexWaitsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "index_io_waits_total"),
		"The total number of index I/O wait events for each index and operation.",
		[]string{"schema", "name", "index", "operation"}, nil,
	)
	performanceSchemaIndexWaitsTimeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "index_io_waits_seconds_total"),
		"The total time of index I/O wait events for each index and operation.",
		[]string{"schema", "name", "index", "operation"}, nil,
	)
	indexIoWaitsMetrics = [8]MetricsDefinition{
		MetricsDefinition{"fetchCount", prometheus.CounterValue, performanceSchemaIndexWaitsDesc},
		MetricsDefinition{"insertCount", prometheus.CounterValue, performanceSchemaIndexWaitsDesc},
		MetricsDefinition{"updateCount", prometheus.CounterValue, performanceSchemaIndexWaitsDesc},
		MetricsDefinition{"deleteCount", prometheus.CounterValue, performanceSchemaIndexWaitsDesc},
		MetricsDefinition{"fetchTime", prometheus.CounterValue, performanceSchemaIndexWaitsTimeDesc},
		MetricsDefinition{"insertTime", prometheus.CounterValue, performanceSchemaIndexWaitsTimeDesc},
		MetricsDefinition{"updateTime", prometheus.CounterValue, performanceSchemaIndexWaitsTimeDesc},
		MetricsDefinition{"deleteTime", prometheus.CounterValue, performanceSchemaIndexWaitsTimeDesc},
	}
)

// ScrapePerfIndexIOWaits collects for `performance_schema.table_io_waits_summary_by_index_usage`.
type ScrapePerfIndexIOWaits struct{}

// Name of the Scraper. Should be unique.
func (ScrapePerfIndexIOWaits) Name() string {
	return "perf_schema.indexiowaits"
}

// Help describes the role of the Scraper.
func (ScrapePerfIndexIOWaits) Help() string {
	return "Collect metrics from performance_schema.table_io_waits_summary_by_index_usage"
}

// Version of MySQL from which scraper is available.
func (ScrapePerfIndexIOWaits) Version() float64 {
	return 5.6
}

func getIndexIOWaitsRawMetric(perfSchemaIndexWaitsRows *sql.Rows) (TableStats, error) {
	var (
		objectSchema, objectName, indexName               string
		countFetch, countInsert, countUpdate, countDelete uint64
		timeFetch, timeInsert, timeUpdate, timeDelete     uint64
	)

	if err := perfSchemaIndexWaitsRows.Scan(
		&objectSchema, &objectName, &indexName, &countFetch, &countInsert, &countUpdate, &countDelete,
		&timeFetch, &timeInsert, &timeUpdate, &timeDelete,
	); err != nil {
		return TableStats{}, err
	}

	stats := make(map[string]float64)
	labels := make(map[string][]string)

	stats["fetchCount"] = float64(countFetch)
	labels["fetchCount"] = []string{"fetch"}

	stats["updateCount"] = float64(countUpdate)
	labels["updateCount"] = []string{"update"}

	stats["deleteCount"] = float64(countDelete)
	labels["deleteCount"] = []string{"delete"}

	stats["fetchTime"] = float64(timeFetch) / picoSeconds
	labels["fetchTime"] = []string{"fetch"}

	stats["updateTime"] = float64(timeUpdate) / picoSeconds
	labels["updateTime"] = []string{"update"}

	stats["deleteTime"] = float64(timeDelete) / picoSeconds
	labels["deleteTime"] = []string{"delete"}

	if indexName == "NONE" {
		stats["insertCount"] = float64(countInsert)
		labels["insertCount"] = []string{"insert"}

		stats["insertTime"] = float64(timeInsert) / picoSeconds
		labels["insertTime"] = []string{"insert"}
	}

	return TableStats{objectSchema, objectName, []string{indexName}, stats, labels}, nil
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapePerfIndexIOWaits) Scrape(ctx context.Context, db *sql.DB, ch chan<- prometheus.Metric) error {
	perfSchemaIndexWaitsRows, err := db.QueryContext(ctx, perfIndexIOWaitsQuery)
	if err != nil {
		return err
	}
	defer perfSchemaIndexWaitsRows.Close()

	aggregator := TableAggregator{*Regex, *Substitution, func(t TableStats) string {
		return t.name + "." + t.schema + "." + t.commonLabels[0] // TODO: Review this!
	}}
	aggregatedStats := make(map[string]TableStats)

	for perfSchemaIndexWaitsRows.Next() {
		err := aggregator.processRow(getIndexIOWaitsRawMetric, perfSchemaIndexWaitsRows, aggregatedStats)

		if err != nil {
			return err
		}
	}

	aggregator.sendMetrics(ch, aggregatedStats, indexIoWaitsMetrics[0:])

	return nil
}

// check interface
var _ Scraper = ScrapePerfIndexIOWaits{}
