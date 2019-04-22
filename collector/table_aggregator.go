package collector

import (
	"database/sql"
	"regexp"

	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/alecthomas/kingpin.v2"

	// TODO: Remove
	"fmt"
)

var (
	Regex = kingpin.Flag(
		"aggregate_table_metrics.regex",
		"Regex with capture groups for renaming the tables",
	).Default("(.*)").String()

	Substitution = kingpin.Flag(
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

	fmt.Print(ta.regex)

	tableNameRegex := regexp.MustCompile(ta.regex)
	tableName := tableNameRegex.ReplaceAllString(tempStats.name, ta.substitution)

	stats, found := aggregatedStats[tempStats.schema+"."+tableName]

	if !found {
		stats = tableStats{tempStats.schema, tableName, make(map[string]float64), make(map[string][]string)}
	}

	for k := range tempStats.metrics {
		stats.metrics[k] += tempStats.metrics[k]

		// Make sense to allow mixing labels? If different labels it is a different metric
		stats.labels[k] = tempStats.labels[k]
	}

	aggregatedStats[tempStats.schema+"."+tableName] = stats

	return nil
}

func (TableAggregator) sendMetrics(ch chan<- prometheus.Metric,
	aggregatedStats map[string]tableStats, metrics []MetricsDefinition) error {
	for _, table := range aggregatedStats {

		for _, metric := range metrics {

			labels := []string{table.schema, table.name}

			if len(table.labels[metric.name]) > 0 {
				labels = append(labels, table.labels[metric.name]...)
			}

			ch <- prometheus.MustNewConstMetric(
				metric.metricDescription, metric.metricType, float64(table.metrics[metric.name]),
				labels...,
			)
		}
	}

	return nil
}
