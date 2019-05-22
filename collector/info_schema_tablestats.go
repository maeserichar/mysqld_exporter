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

const tableStatQuery = `
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
	infoSchemaTableStatsRowsReadDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, informationSchema, "table_statistics_rows_read_total"),
		"The number of rows read from the table.",
		[]string{"schema", "table"}, nil,
	)
	infoSchemaTableStatsRowsChangedDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, informationSchema, "table_statistics_rows_changed_total"),
		"The number of rows changed in the table.",
		[]string{"schema", "table"}, nil,
	)
	infoSchemaTableStatsRowsChangedXIndexesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, informationSchema, "table_statistics_rows_changed_x_indexes_total"),
		"The number of rows changed in the table, multiplied by the number of indexes changed.",
		[]string{"schema", "table"}, nil,
	)
)

// Configuration
var (
	tableStatsMetrics = [3]MetricsDefinition{
		MetricsDefinition{"rowsRead", prometheus.CounterValue, infoSchemaTableStatsRowsReadDesc},
		MetricsDefinition{"rowsChanged", prometheus.CounterValue, infoSchemaTableStatsRowsChangedDesc},
		MetricsDefinition{"rowsChangedXIndexes", prometheus.CounterValue, infoSchemaTableStatsRowsChangedXIndexesDesc},
	}
)

// ScrapeTableStat collects from `information_schema.table_statistics`.
type ScrapeTableStat struct{}

// Name of the Scraper. Should be unique.
func (ScrapeTableStat) Name() string {
	return "info_schema.tablestats"
}

// Help describes the role of the Scraper.
func (ScrapeTableStat) Help() string {
	return "If running with userstat=1, set to true to collect table statistics"
}

// Version of MySQL from which scraper is available.
func (ScrapeTableStat) Version() float64 {
	return 5.1
}

func getRawMetric(informationSchemaTableStatisticsRows *sql.Rows) (TableStats, error) {
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
		return TableStats{}, err
	}

	tempStats := make(map[string]float64)
	tempStats["rowsChanged"] = float64(rowsChanged)
	tempStats["rowsChangedXIndexes"] = float64(rowsChangedXIndexes)
	tempStats["rowsRead"] = float64(rowsRead)

	return TableStats{tableSchema, tableName, nil, tempStats, nil}, nil
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeTableStat) Scrape(ctx context.Context, db *sql.DB, ch chan<- prometheus.Metric) error {
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

	tableAggregator := NewTableAggregator(*Regex, *Substitution)

	informationSchemaTableStatisticsRows, err := db.QueryContext(ctx, tableStatQuery)
	if err != nil {
		return err
	}
	defer informationSchemaTableStatisticsRows.Close()

	var aggregatedStats = make(map[string]TableStats)

	for informationSchemaTableStatisticsRows.Next() {
		tableAggregator.processRow(getRawMetric, informationSchemaTableStatisticsRows, aggregatedStats)
	}

	tableAggregator.sendMetrics(ch, aggregatedStats, tableStatsMetrics[0:])

	return nil
}

// check interface
var _ Scraper = ScrapeTableStat{}
