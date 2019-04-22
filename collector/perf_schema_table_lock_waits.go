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

// Scrape `performance_schema.table_lock_waits_summary_by_table`.

package collector

import (
	"context"
	"database/sql"

	"github.com/prometheus/client_golang/prometheus"
)

const perfTableLockWaitsQuery = `
	SELECT
	    OBJECT_SCHEMA,
	    OBJECT_NAME,
	    COUNT_READ_NORMAL,
	    COUNT_READ_WITH_SHARED_LOCKS,
	    COUNT_READ_HIGH_PRIORITY,
	    COUNT_READ_NO_INSERT,
	    COUNT_READ_EXTERNAL,
	    COUNT_WRITE_ALLOW_WRITE,
	    COUNT_WRITE_CONCURRENT_INSERT,
	    COUNT_WRITE_LOW_PRIORITY,
	    COUNT_WRITE_NORMAL,
	    COUNT_WRITE_EXTERNAL,
	    SUM_TIMER_READ_NORMAL,
	    SUM_TIMER_READ_WITH_SHARED_LOCKS,
	    SUM_TIMER_READ_HIGH_PRIORITY,
	    SUM_TIMER_READ_NO_INSERT,
	    SUM_TIMER_READ_EXTERNAL,
	    SUM_TIMER_WRITE_ALLOW_WRITE,
	    SUM_TIMER_WRITE_CONCURRENT_INSERT,
	    SUM_TIMER_WRITE_LOW_PRIORITY,
	    SUM_TIMER_WRITE_NORMAL,
	    SUM_TIMER_WRITE_EXTERNAL
	  FROM performance_schema.table_lock_waits_summary_by_table
	  WHERE OBJECT_SCHEMA NOT IN ('mysql', 'performance_schema', 'information_schema')
	`

// Metric descriptors.
var (
	performanceSchemaSQLTableLockWaitsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "sql_lock_waits_total"),
		"The total number of SQL lock wait events for each table and operation.",
		[]string{"schema", "name", "operation"}, nil,
	)
	performanceSchemaExternalTableLockWaitsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "external_lock_waits_total"),
		"The total number of external lock wait events for each table and operation.",
		[]string{"schema", "name", "operation"}, nil,
	)
	performanceSchemaSQLTableLockWaitsTimeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "sql_lock_waits_seconds_total"),
		"The total time of SQL lock wait events for each table and operation.",
		[]string{"schema", "name", "operation"}, nil,
	)
	performanceSchemaExternalTableLockWaitsTimeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "external_lock_waits_seconds_total"),
		"The total time of external lock wait events for each table and operation.",
		[]string{"schema", "name", "operation"}, nil,
	)

	lockWaitsMetrics = [20]MetricsDefinition{
		MetricsDefinition{"readNormalCount", prometheus.CounterValue, performanceSchemaSQLTableLockWaitsDesc},
		MetricsDefinition{"readSharedLocksCount", prometheus.CounterValue, performanceSchemaSQLTableLockWaitsDesc},
		MetricsDefinition{"readHighPriorityCount", prometheus.CounterValue, performanceSchemaSQLTableLockWaitsDesc},
		MetricsDefinition{"readNoInsertCount", prometheus.CounterValue, performanceSchemaSQLTableLockWaitsDesc},
		MetricsDefinition{"writeNormalCount", prometheus.CounterValue, performanceSchemaSQLTableLockWaitsDesc},
		MetricsDefinition{"writeAllowCount", prometheus.CounterValue, performanceSchemaSQLTableLockWaitsDesc},
		MetricsDefinition{"writeConcurrentInsertCount", prometheus.CounterValue, performanceSchemaSQLTableLockWaitsDesc},
		MetricsDefinition{"writeLowPriorityCount", prometheus.CounterValue, performanceSchemaSQLTableLockWaitsDesc},
		MetricsDefinition{"readExternalCount", prometheus.CounterValue, performanceSchemaExternalTableLockWaitsDesc},
		MetricsDefinition{"writeExternalCount", prometheus.CounterValue, performanceSchemaExternalTableLockWaitsDesc},
		MetricsDefinition{"readNormalTime", prometheus.CounterValue, performanceSchemaExternalTableLockWaitsDesc},
		MetricsDefinition{"readSharedLocksTime", prometheus.CounterValue, performanceSchemaSQLTableLockWaitsTimeDesc},
		MetricsDefinition{"readHighPriorityTime", prometheus.CounterValue, performanceSchemaSQLTableLockWaitsTimeDesc},
		MetricsDefinition{"readNoInsertTime", prometheus.CounterValue, performanceSchemaSQLTableLockWaitsTimeDesc},
		MetricsDefinition{"writeNormalTime", prometheus.CounterValue, performanceSchemaSQLTableLockWaitsTimeDesc},
		MetricsDefinition{"writeAllowTime", prometheus.CounterValue, performanceSchemaSQLTableLockWaitsTimeDesc},
		MetricsDefinition{"writeConcurrentInsertTime", prometheus.CounterValue, performanceSchemaSQLTableLockWaitsTimeDesc},
		MetricsDefinition{"writeLowPriorityTime", prometheus.CounterValue, performanceSchemaSQLTableLockWaitsTimeDesc},
		MetricsDefinition{"readExternalTime", prometheus.CounterValue, performanceSchemaExternalTableLockWaitsTimeDesc},
		MetricsDefinition{"writeExternalTime", prometheus.CounterValue, performanceSchemaExternalTableLockWaitsTimeDesc},
	}
)

// ScrapePerfTableLockWaits collects from `performance_schema.table_lock_waits_summary_by_table`.
type ScrapePerfTableLockWaits struct{}

// Name of the Scraper. Should be unique.
func (ScrapePerfTableLockWaits) Name() string {
	return "perf_schema.tablelocks"
}

// Help describes the role of the Scraper.
func (ScrapePerfTableLockWaits) Help() string {
	return "Collect metrics from performance_schema.table_lock_waits_summary_by_table"
}

// Version of MySQL from which scraper is available.
func (ScrapePerfTableLockWaits) Version() float64 {
	return 5.6
}

func getLockWaitsRawMetric(perfSchemaTableLockWaitsRows *sql.Rows) (tableStats, error) {
	var (
		objectSchema               string
		objectName                 string
		countReadNormal            uint64
		countReadWithSharedLocks   uint64
		countReadHighPriority      uint64
		countReadNoInsert          uint64
		countReadExternal          uint64
		countWriteAllowWrite       uint64
		countWriteConcurrentInsert uint64
		countWriteLowPriority      uint64
		countWriteNormal           uint64
		countWriteExternal         uint64
		timeReadNormal             uint64
		timeReadWithSharedLocks    uint64
		timeReadHighPriority       uint64
		timeReadNoInsert           uint64
		timeReadExternal           uint64
		timeWriteAllowWrite        uint64
		timeWriteConcurrentInsert  uint64
		timeWriteLowPriority       uint64
		timeWriteNormal            uint64
		timeWriteExternal          uint64
	)

	if err := perfSchemaTableLockWaitsRows.Scan(
		&objectSchema,
		&objectName,
		&countReadNormal,
		&countReadWithSharedLocks,
		&countReadHighPriority,
		&countReadNoInsert,
		&countReadExternal,
		&countWriteAllowWrite,
		&countWriteConcurrentInsert,
		&countWriteLowPriority,
		&countWriteNormal,
		&countWriteExternal,
		&timeReadNormal,
		&timeReadWithSharedLocks,
		&timeReadHighPriority,
		&timeReadNoInsert,
		&timeReadExternal,
		&timeWriteAllowWrite,
		&timeWriteConcurrentInsert,
		&timeWriteLowPriority,
		&timeWriteNormal,
		&timeWriteExternal,
	); err != nil {
		return tableStats{}, err
	}

	stats := make(map[string]float64)
	labels := make(map[string][]string)

	stats["readNormalCount"] = float64(countReadNormal)
	labels["readNormalCount"] = []string{"read_normal"}

	stats["readSharedLocksCount"] = float64(countReadWithSharedLocks)
	labels["readSharedLocksCount"] = []string{"read_with_shared_locks"}

	stats["readHighPriorityCount"] = float64(countReadHighPriority)
	labels["readHighPriorityCount"] = []string{"read_high_priority"}

	stats["readNoInsertCount"] = float64(countReadNoInsert)
	labels["readNoInsertCount"] = []string{"read_no_insert"}

	stats["writeNormalCount"] = float64(countWriteNormal)
	labels["writeNormalCount"] = []string{"write_normal"}

	stats["writeAllowCount"] = float64(countWriteAllowWrite)
	labels["writeAllowCount"] = []string{"write_allow_write"}

	stats["writeConcurrentInsertCount"] = float64(countWriteConcurrentInsert)
	labels["writeConcurrentInsertCount"] = []string{"write_concurrent_insert"}

	stats["writeLowPriorityCount"] = float64(countWriteLowPriority)
	labels["writeLowPriorityCount"] = []string{"write_low_priority"}

	stats["readExternalCount"] = float64(countReadExternal)
	labels["readExternalCount"] = []string{"read"}

	stats["writeExternalCount"] = float64(countWriteExternal)
	labels["writeExternalCount"] = []string{"write"}

	stats["readNormalTime"] = float64(timeReadNormal) / picoSeconds
	labels["readNormalTime"] = []string{"read_normal"}

	stats["readSharedLocksTime"] = float64(timeReadWithSharedLocks) / picoSeconds
	labels["readSharedLocksTime"] = []string{"read_with_shared_locks"}

	stats["readHighPriorityTime"] = float64(timeReadHighPriority) / picoSeconds
	labels["readHighPriorityTime"] = []string{"read_high_priority"}

	stats["readNoInsertTime"] = float64(timeReadNoInsert) / picoSeconds
	labels["readNoInsertTime"] = []string{"read_no_insert"}

	stats["writeNormalTime"] = float64(timeWriteNormal) / picoSeconds
	labels["writeNormalTime"] = []string{"write_normal"}

	stats["writeAllowTime"] = float64(timeWriteAllowWrite) / picoSeconds
	labels["writeAllowTime"] = []string{"write_allow_write"}

	stats["writeConcurrentInsertTime"] = float64(timeWriteConcurrentInsert) / picoSeconds
	labels["writeConcurrentInsertTime"] = []string{"write_concurrent_insert"}

	stats["writeLowPriorityTime"] = float64(timeWriteLowPriority) / picoSeconds
	labels["writeLowPriorityTime"] = []string{"write_low_priority"}

	stats["readExternalTime"] = float64(timeReadExternal) / picoSeconds
	labels["readExternalTime"] = []string{"read"}

	stats["writeExternalTime"] = float64(timeWriteExternal) / picoSeconds
	labels["writeExternalTime"] = []string{"write"}

	return tableStats{objectSchema, objectName, stats, labels}, nil
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapePerfTableLockWaits) Scrape(ctx context.Context, db *sql.DB, ch chan<- prometheus.Metric) error {
	perfSchemaTableLockWaitsRows, err := db.QueryContext(ctx, perfTableLockWaitsQuery)
	if err != nil {
		return err
	}
	defer perfSchemaTableLockWaitsRows.Close()

	aggregator := TableAggregator{*Regex, *Substitution}
	aggregatedStats := make(map[string]tableStats)

	for perfSchemaTableLockWaitsRows.Next() {
		err := aggregator.processRow(getLockWaitsRawMetric, perfSchemaTableLockWaitsRows, aggregatedStats)

		if err != nil {
			return err
		}
	}

	aggregator.sendMetrics(ch, aggregatedStats, lockWaitsMetrics[0:])

	return nil
}

// check interface
var _ Scraper = ScrapePerfTableLockWaits{}
