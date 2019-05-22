package collector

import (
	"database/sql"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/smartystreets/goconvey/convey"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var (
	testMetricDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, performanceSchema, "testmetric"),
		"The description of the test metric.",
		[]string{"schema", "name", "zoperation"}, nil,
	)
)

func TestSendMetrics(t *testing.T) {
	ch := make(chan prometheus.Metric)
	aggregator := TableAggregator{}
	aggregatedStats := make(map[string]TableStats)
	aggregatedStats["test.table"] = TableStats{
		"test",
		"table",
		[]string{},
		map[string]float64{
			"readNormalCount":       float64(2),
			"readSharedLocksCount":  float64(4),
			"readHighPriorityCount": float64(5),
		},
		map[string][]string{
			"readNormalCount":       []string{"label1"},
			"readSharedLocksCount":  []string{"label2"},
			"readHighPriorityCount": []string{"label3"},
		},
	}
	metrics := [3]MetricsDefinition{
		MetricsDefinition{"readNormalCount", prometheus.CounterValue, testMetricDesc},
		MetricsDefinition{"readSharedLocksCount", prometheus.CounterValue, testMetricDesc},
		MetricsDefinition{"readHighPriorityCount", prometheus.CounterValue, testMetricDesc},
	}

	go func() {
		aggregator.sendMetrics(ch, aggregatedStats, metrics[0:])
		close(ch)
	}()

	expected := []MetricResult{
		{labels: labelMap{"schema": "test", "name": "table", "zoperation": "label1"}, value: 2},
		{labels: labelMap{"schema": "test", "name": "table", "zoperation": "label2"}, value: 4},
		{labels: labelMap{"schema": "test", "name": "table", "zoperation": "label3"}, value: 5},
	}

	convey.Convey("Metrics comparison", t, func() {
		metricsReaded := 0
		for elem := range ch {
			metric := readMetric(elem)
			convey.So(expected, convey.ShouldContain, metric)
			metricsReaded++
		}

		convey.So(len(expected), convey.ShouldResemble, metricsReaded)
	})
}

func rowProviderFunc(perfSchemaTableLockWaitsRows *sql.Rows) (TableStats, error) {
	var (
		tableSchema         string
		tableName           string
		rowsRead            uint64
		rowsChanged         uint64
		rowsChangedXIndexes uint64
	)

	err := perfSchemaTableLockWaitsRows.Scan(
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

	tempLabels := make(map[string][]string)
	tempLabels["rowsChanged"] = []string{"label1"}
	tempLabels["rowsChangedXIndexes"] = []string{"label2"}
	tempLabels["rowsRead"] = []string{"label3"}

	return TableStats{tableSchema, tableName, []string{}, tempStats, tempLabels}, nil
}

func TestProcessRow(t *testing.T) {
	db, mock, _ := sqlmock.New()
	aggregator := NewTableAggregator(*Regex, *Substitution)
	aggregatedStats := make(map[string]TableStats)

	columns := []string{"TABLE_SCHEMA", "TABLE_NAME", "ROWS_READ", "ROWS_CHANGED", "ROWS_CHANGED_X_INDEXES"}
	rows := sqlmock.NewRows(columns).
		AddRow("mysql", "proxies_priv", 99, 1, 0).
		AddRow("mysql", "user", 1064, 2, 5)
	mock.ExpectQuery(sanitizeQuery("select * from test.query")).WillReturnRows(rows)

	mockRows, _ := db.Query("select * from test.query")

	for mockRows.Next() {
		aggregator.processRow(rowProviderFunc, mockRows, aggregatedStats)
	}

	expectedStats := map[string]TableStats{
		"mysql.proxies_priv": TableStats{
			"mysql",
			"proxies_priv",
			[]string{},
			map[string]float64{
				"rowsChanged":         float64(1),
				"rowsChangedXIndexes": float64(0),
				"rowsRead":            float64(99),
			},
			map[string][]string{
				"rowsChanged":         []string{"label1"},
				"rowsChangedXIndexes": []string{"label2"},
				"rowsRead":            []string{"label3"},
			},
		},
		"mysql.user": TableStats{
			"mysql",
			"user",
			[]string{},
			map[string]float64{
				"rowsChanged":         float64(2),
				"rowsChangedXIndexes": float64(5),
				"rowsRead":            float64(1064),
			},
			map[string][]string{
				"rowsChanged":         []string{"label1"},
				"rowsChangedXIndexes": []string{"label2"},
				"rowsRead":            []string{"label3"},
			},
		},
	}

	convey.Convey("Metrics comparison", t, func() {
		metricsReaded := 0
		for key, metric := range aggregatedStats {
			expectedStat := expectedStats[key]
			convey.So(expectedStat, convey.ShouldResemble, metric)
			metricsReaded++
		}

		convey.So(len(expectedStats), convey.ShouldResemble, metricsReaded)
	})
}

func TestProcessRowWithRegex(t *testing.T) {
	db, mock, _ := sqlmock.New()
	aggregator := NewTableAggregator("(.*)\\d", "$1")
	aggregatedStats := make(map[string]TableStats)

	columns := []string{"TABLE_SCHEMA", "TABLE_NAME", "ROWS_READ", "ROWS_CHANGED", "ROWS_CHANGED_X_INDEXES"}
	rows := sqlmock.NewRows(columns).
		AddRow("mysql", "user2", 99, 1, 0).
		AddRow("mysql", "user1", 1064, 2, 5)
	mock.ExpectQuery(sanitizeQuery("select * from test.query")).WillReturnRows(rows)

	mockRows, _ := db.Query("select * from test.query")

	for mockRows.Next() {
		aggregator.processRow(rowProviderFunc, mockRows, aggregatedStats)
	}

	expectedStats := map[string]TableStats{
		"mysql.user": TableStats{
			"mysql",
			"user",
			[]string{},
			map[string]float64{
				"rowsChanged":         float64(3),
				"rowsChangedXIndexes": float64(5),
				"rowsRead":            float64(1163),
			},
			map[string][]string{
				"rowsChanged":         []string{"label1"},
				"rowsChangedXIndexes": []string{"label2"},
				"rowsRead":            []string{"label3"},
			},
		},
	}

	convey.Convey("Metrics comparison", t, func() {
		metricsReaded := 0
		for key, metric := range aggregatedStats {
			expectedStat := expectedStats[key]
			convey.So(expectedStat, convey.ShouldResemble, metric)
			metricsReaded++
		}

		convey.So(len(expectedStats), convey.ShouldResemble, metricsReaded)
	})
}
