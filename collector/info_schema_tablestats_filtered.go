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

// Scrape `information_schema.table_statistics` grouped by regex.

package collector

import (
	"context"
	"database/sql"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
)

const tableStatFilteredQuery = `
		SELECT
		  TABLE_SCHEMA,
		  TABLE_NAME,
		  ROWS_READ,
		  ROWS_CHANGED,
		  ROWS_CHANGED_X_INDEXES
		  FROM information_schema.table_statistics
		`

// Metric descriptors.
var (
	infoSchemaTableStatsFilteredRowsReadDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, informationSchema, "table_statistics_rows_read_total"),
		"The number of rows read from the table.",
		[]string{"schema", "table"}, nil,
	)
	infoSchemaTableStatsFilteredRowsChangedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, informationSchema, "table_statistics_rows_changed_total"),
		"The number of rows changed in the table.",
		[]string{"schema", "table"}, nil,
	)
	infoSchemaTableStatsFilteredRowsChangedXIndexesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, informationSchema, "table_statistics_rows_changed_x_indexes_total"),
		"The number of rows changed in the table, multiplied by the number of indexes changed.",
		[]string{"schema", "table"}, nil,
	)
)

// Configuration
var (
	tableStatsMetrics = [3]MetricsDefinition{
		MetricsDefinition{"rowsRead", prometheus.CounterValue, infoSchemaTableStatsFilteredRowsReadDesc},
		MetricsDefinition{"rowsChanged", prometheus.CounterValue, infoSchemaTableStatsFilteredRowsChangedDesc},
		MetricsDefinition{"rowsChangedXIndexes", prometheus.CounterValue, infoSchemaTableStatsFilteredRowsChangedXIndexesDesc},
	}
)

// ScrapeTableStatFiltered collects from `information_schema.table_statistics`.
type ScrapeTableStatFiltered struct{}

// Name of the Scraper. Should be unique.
func (ScrapeTableStatFiltered) Name() string {
	return "info_schema.tablestatsfiltered"
}

// Help describes the role of the Scraper.
func (ScrapeTableStatFiltered) Help() string {
	return "If running with userstat=1, set to true to collect table statistics"
}

// Version of MySQL from which scraper is available.
func (ScrapeTableStatFiltered) Version() float64 {
	return 5.1
}

type tableStats struct {
	schema  string
	name    string
	metrics map[string]float64
	labels  map[string][]string
}

func getRawMetric(informationSchemaTableStatisticsRows *sql.Rows) (tableStats, error) {
	var (
		tableSchema         string
		tableName           string
		rowsRead            uint64
		rowsChanged         uint64
		rowsChangedXIndexes uint64
	)

	err := informationSchemaTableStatisticsRows.Scan(
		&tableSchema,
		&tableName,
		&rowsRead,
		&rowsChanged,
		&rowsChangedXIndexes,
	)
	if err != nil {
		return tableStats{}, err
	}

	tempStats := make(map[string]float64)
	tempStats["rowsChanged"] = float64(rowsChanged)
	tempStats["rowsChangedXIndexes"] = float64(rowsChangedXIndexes)
	tempStats["rowsRead"] = float64(rowsRead)

	return tableStats{tableSchema, tableName, tempStats, nil}, nil
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeTableStatFiltered) Scrape(ctx context.Context, db *sql.DB, ch chan<- prometheus.Metric) error {
	var varName, varVal string
	err := db.QueryRowContext(ctx, userstatCheckQuery).Scan(&varName, &varVal)
	if err != nil {
		log.Debugln("Detailed table stats are not available.")
		return nil
	}
	if varVal == "OFF" {
		log.Debugf("MySQL @@%s is OFF.", varName)
		return nil
	}

	tableAggregator := TableAggregator{*Regex, *Substitution}

	informationSchemaTableStatisticsRows, err := db.QueryContext(ctx, tableStatFilteredQuery)
	if err != nil {
		return err
	}
	defer informationSchemaTableStatisticsRows.Close()

	var aggregatedStats = make(map[string]tableStats)

	for informationSchemaTableStatisticsRows.Next() {
		tableAggregator.processRow(getRawMetric, informationSchemaTableStatisticsRows, aggregatedStats)
	}

	tableAggregator.sendMetrics(ch, aggregatedStats, tableStatsMetrics[0:])

	return nil
}

// check interface
var _ Scraper = ScrapeTableStatFiltered{}
