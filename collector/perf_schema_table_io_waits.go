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

// Scrape `performance_schema.table_io_waits_summary_by_table`.

package collector

import (
	"context"
	"database/sql"

	"github.com/prometheus/client_golang/prometheus"
)

const perfTableIOWaitsQuery = `
	SELECT
	    OBJECT_SCHEMA, OBJECT_NAME,
	    COUNT_FETCH, COUNT_INSERT, COUNT_UPDATE, COUNT_DELETE,
	    SUM_TIMER_FETCH, SUM_TIMER_INSERT, SUM_TIMER_UPDATE, SUM_TIMER_DELETE
	  FROM performance_schema.table_io_waits_summary_by_table
	  WHERE OBJECT_SCHEMA NOT IN ('mysql', 'performance_schema')
	`

// Metric descriptors.
var (
	performanceSchemaTableWaitsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "table_io_waits_total"),
		"The total number of table I/O wait events for each table and operation.",
		[]string{"schema", "name", "operation"}, nil,
	)
	performanceSchemaTableWaitsTimeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "table_io_waits_seconds_total"),
		"The total time of table I/O wait events for each table and operation.",
		[]string{"schema", "name", "operation"}, nil,
	)
	ioWaitsMetrics = [8]MetricsDefinition{
		MetricsDefinition{"fetchCount", prometheus.CounterValue, performanceSchemaTableWaitsDesc},
		MetricsDefinition{"insertCount", prometheus.CounterValue, performanceSchemaTableWaitsDesc},
		MetricsDefinition{"updateCount", prometheus.CounterValue, performanceSchemaTableWaitsDesc},
		MetricsDefinition{"deleteCount", prometheus.CounterValue, performanceSchemaTableWaitsDesc},
		MetricsDefinition{"fetchTime", prometheus.CounterValue, performanceSchemaTableWaitsTimeDesc},
		MetricsDefinition{"insertTime", prometheus.CounterValue, performanceSchemaTableWaitsTimeDesc},
		MetricsDefinition{"updateTime", prometheus.CounterValue, performanceSchemaTableWaitsTimeDesc},
		MetricsDefinition{"deleteTime", prometheus.CounterValue, performanceSchemaTableWaitsTimeDesc},
	}
)

// ScrapePerfTableIOWaits collects from `performance_schema.table_io_waits_summary_by_table`.
type ScrapePerfTableIOWaits struct{}

// Name of the Scraper. Should be unique.
func (ScrapePerfTableIOWaits) Name() string {
	return "perf_schema.tableiowaits"
}

// Help describes the role of the Scraper.
func (ScrapePerfTableIOWaits) Help() string {
	return "Collect metrics from performance_schema.table_io_waits_summary_by_table"
}

// Version of MySQL from which scraper is available.
func (ScrapePerfTableIOWaits) Version() float64 {
	return 5.6
}

func getIOWaitsRawMetric(perfSchemaTableWaitsRows *sql.Rows) (TableStats, error) {
	var (
		objectSchema, objectName                          string
		countFetch, countInsert, countUpdate, countDelete uint64
		timeFetch, timeInsert, timeUpdate, timeDelete     uint64
	)

	if err := perfSchemaTableWaitsRows.Scan(
		&objectSchema, &objectName, &countFetch, &countInsert, &countUpdate, &countDelete,
		&timeFetch, &timeInsert, &timeUpdate, &timeDelete,
	); err != nil {
		return TableStats{}, err
	}

	stats := make(map[string]float64)
	labels := make(map[string][]string)

	stats["fetchCount"] = float64(countFetch)
	labels["fetchCount"] = []string{"fetch"}

	stats["insertCount"] = float64(countInsert)
	labels["insertCount"] = []string{"insert"}

	stats["updateCount"] = float64(countUpdate)
	labels["updateCount"] = []string{"update"}

	stats["deleteCount"] = float64(countDelete)
	labels["deleteCount"] = []string{"delete"}

	stats["fetchTime"] = float64(timeFetch) / picoSeconds
	labels["fetchTime"] = []string{"fetch"}

	stats["insertTime"] = float64(timeInsert) / picoSeconds
	labels["insertTime"] = []string{"insert"}

	stats["updateTime"] = float64(timeUpdate) / picoSeconds
	labels["updateTime"] = []string{"update"}

	stats["deleteTime"] = float64(timeDelete) / picoSeconds
	labels["deleteTime"] = []string{"delete"}

	return TableStats{objectSchema, objectName, nil, stats, labels}, nil
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapePerfTableIOWaits) Scrape(ctx context.Context, db *sql.DB, ch chan<- prometheus.Metric) error {
	perfSchemaTableWaitsRows, err := db.QueryContext(ctx, perfTableIOWaitsQuery)
	if err != nil {
		return err
	}
	defer perfSchemaTableWaitsRows.Close()

	aggregator := NewTableAggregator(*Regex, *Substitution)
	aggregatedStats := make(map[string]TableStats)

	for perfSchemaTableWaitsRows.Next() {
		err := aggregator.processRow(getIOWaitsRawMetric, perfSchemaTableWaitsRows, aggregatedStats)

		if err != nil {
			return err
		}
	}

	aggregator.sendMetrics(ch, aggregatedStats, ioWaitsMetrics[0:])

	return nil
}

// check interface
var _ Scraper = ScrapePerfTableIOWaits{}
