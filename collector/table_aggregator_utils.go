package collector

import (
	"database/sql"
	"regexp"

	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	regex = kingpin.Flag(
		"aggregate_table_metrics.regex",
		"Regex with capture groups for renaming the tables",
	).Default("(.*)").String()

	substitution = kingpin.Flag(
		"aggregate_table_metrics.substitution",
		"Substitution string to apply to the table name",
	).Default("$1").String()
)

// MetricsDefinition A struct that contains the definition of a metric
type MetricsDefinition struct {
	name              string
	metricType        prometheus.ValueType
	metricDescription *prometheus.Desc
}

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
		stats = tableStats{tempStats.schema, tableName, make(map[string]float64), make(map[string]string)}
	}

	for k := range tempStats.metrics {
		stats.metrics[k] += tempStats.metrics[k]

		// TODO: View if there is a more idiomatic way (and manage multiple labels)
		stats.labels[k] = tempStats.labels[k]
	}

	aggregatedStats[tempStats.schema+"."+tableName] = stats

	return nil
}

func (TableAggregator) groupMetrics(ch chan<- prometheus.Metric,
	aggregatedStats map[string]tableStats, metrics []MetricsDefinition) error {
	for _, table := range aggregatedStats {

		for _, metric := range metrics {
			// TODO: Review how to handle this
			if table.labels[metric.name] != "" {
				ch <- prometheus.MustNewConstMetric(
					metric.metricDescription, metric.metricType, float64(table.metrics[metric.name]),
					table.schema, table.name, table.labels[metric.name],
				)
			} else {
				ch <- prometheus.MustNewConstMetric(
					metric.metricDescription, metric.metricType, float64(table.metrics[metric.name]),
					table.schema, table.name,
				)
			}
		}
	}

	return nil
}
