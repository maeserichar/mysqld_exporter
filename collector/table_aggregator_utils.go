package collector

import (
	"database/sql"
	"regexp"

	"github.com/prometheus/client_golang/prometheus"
)

// TableAggregator Utility struct that aggregate metrics by table name
type TableAggregator struct {
	regex        string
	substitution string
}

func (ta TableAggregator) processRow(f func(informationSchemaTableStatisticsRows *sql.Rows) (tableStats, error),
	rows *sql.Rows, aggregatedStats map[string]tableStats) error {

	tempStats, err := f(rows)

	if err != nil {
		return err
	}

	tableNameRegex := regexp.MustCompile(ta.regex)
	tableName := tableNameRegex.ReplaceAllString(tempStats.name, ta.substitution)

	stats, found := aggregatedStats[tempStats.schema+"."+tableName]

	if !found {
		stats = tableStats{tempStats.schema, tableName, make(map[string]uint64)}
	}

	for _, metric := range metrics {
		stats.metrics[metric.name] += tempStats.metrics[metric.name]
	}

	aggregatedStats[tempStats.schema+"."+tableName] = stats

	return nil
}

func (TableAggregator) groupMetrics(ch chan<- prometheus.Metric,
	aggregatedStats map[string]tableStats, metrics []MetricsDefinition) error {
	for _, table := range aggregatedStats {

		for _, metric := range metrics {
			ch <- prometheus.MustNewConstMetric(
				metric.metricDescription, metric.metricType, float64(table.metrics[metric.name]),
				table.schema, table.name,
			)
		}
	}

	return nil
}
