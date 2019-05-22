package collector

import (
	"database/sql"
	"regexp"

	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	// Regex the regular expression used for renaming the tables.
	Regex = kingpin.Flag(
		"aggregate_table_metrics.regex",
		"Regex with capture groups for renaming the tables",
	).Default("(.*)").String()

	// Substitution The substitution expression used for renaming tables.
	Substitution = kingpin.Flag(
		"aggregate_table_metrics.substitution",
		"Substitution string to apply to the table name",
	).Default("$1").String()

	defaultKeyProvider = func(stats TableStats) string {
		return stats.schema + "." + stats.name
	}
)

// TableStats A struct that contains the set of metrics for a schema.table
type TableStats struct {
	schema       string
	name         string
	commonLabels []string
	metrics      map[string]float64
	labels       map[string][]string
}

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
	keyProvider  func(stats TableStats) string
}

// NewTableAggregator Constructor method that uses the default key provider (schema.table)
func NewTableAggregator(regex string, substitution string) TableAggregator {
	return TableAggregator{regex, substitution, defaultKeyProvider}
}

func (ta TableAggregator) processRow(f func(informationSchemaTableStatisticsRows *sql.Rows) (TableStats, error),
	rows *sql.Rows, aggregatedStats map[string]TableStats) error {

	tempStats, err := f(rows)

	if err != nil {
		return err
	}

	tableNameRegex := regexp.MustCompile(ta.regex)
	tempStats.name = tableNameRegex.ReplaceAllString(tempStats.name, ta.substitution)

	stats, found := aggregatedStats[ta.keyProvider(tempStats)]

	if !found {
		stats = TableStats{tempStats.schema, tempStats.name, tempStats.commonLabels, make(map[string]float64), make(map[string][]string)}
	}

	for k := range tempStats.metrics {
		stats.metrics[k] += tempStats.metrics[k]

		// Make sense to allow mixing labels? If different labels it is a different metric
		stats.labels[k] = tempStats.labels[k]
	}

	aggregatedStats[ta.keyProvider(tempStats)] = stats

	return nil
}

func (TableAggregator) sendMetrics(ch chan<- prometheus.Metric,
	aggregatedStats map[string]TableStats, metrics []MetricsDefinition) error {
	for _, table := range aggregatedStats {

		for _, metric := range metrics {

			if metricValue, ok := table.metrics[metric.name]; ok {

				labels := []string{table.schema, table.name}

				if len(table.commonLabels) > 0 {
					labels = append(labels, table.commonLabels...)
				}

				if len(table.labels[metric.name]) > 0 {
					labels = append(labels, table.labels[metric.name]...)
				}

				ch <- prometheus.MustNewConstMetric(
					metric.metricDescription, metric.metricType, float64(metricValue),
					labels...,
				)
			}
		}
	}

	return nil
}
