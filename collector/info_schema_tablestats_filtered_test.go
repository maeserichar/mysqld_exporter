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

package collector

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/smartystreets/goconvey/convey"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
	"gopkg.in/alecthomas/kingpin.v2"
)

func TestScrapeTableStatFilteredWithDefaultValues(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error opening a stub database connection: %s", err)
	}
	defer db.Close()

	mock.ExpectQuery(sanitizeQuery(userstatCheckQuery)).WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("userstat", "ON"))

	columns := []string{"TABLE_SCHEMA", "TABLE_NAME", "ROWS_READ", "ROWS_CHANGED", "ROWS_CHANGED_X_INDEXES"}
	rows := sqlmock.NewRows(columns).
		AddRow("mysql", "db", 5, 0, 8).
		AddRow("mysql", "db", 5, 0, 8).
		AddRow("mysql", "db", 5, 0, 8).
		AddRow("mysql", "proxies_priv", 99, 1, 0).
		AddRow("mysql", "user", 1064, 2, 5)
	mock.ExpectQuery(sanitizeQuery(tableStatFilteredQuery)).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		if err = (ScrapeTableStatFiltered{}).Scrape(context.Background(), db, ch); err != nil {
			t.Errorf("error calling function on test: %s", err)
		}
		close(ch)
	}()

	expected := []MetricResult{
		{labels: labelMap{"schema": "mysql", "table": "db"}, value: 15},
		{labels: labelMap{"schema": "mysql", "table": "db"}, value: 0},
		{labels: labelMap{"schema": "mysql", "table": "db"}, value: 24},
		{labels: labelMap{"schema": "mysql", "table": "proxies_priv"}, value: 99},
		{labels: labelMap{"schema": "mysql", "table": "proxies_priv"}, value: 1},
		{labels: labelMap{"schema": "mysql", "table": "proxies_priv"}, value: 0},
		{labels: labelMap{"schema": "mysql", "table": "user"}, value: 1064},
		{labels: labelMap{"schema": "mysql", "table": "user"}, value: 2},
		{labels: labelMap{"schema": "mysql", "table": "user"}, value: 5},
	}
	convey.Convey("Metrics comparison", t, func() {
		for _, expect := range expected {
			got := readMetric(<-ch)
			convey.So(expect, convey.ShouldResemble, got)
		}
	})

	// Ensure all SQL queries were executed
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled exceptions: %s", err)
	}
}

func TestScrapeTableStatFilteredWithCustomRegex(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error opening a stub database connection: %s", err)
	}
	defer db.Close()

	_, err2 := kingpin.CommandLine.Parse([]string{"--collect.info_schema_tablestats_filtered.regex", `(.*)_\d`,
		"--collect.info_schema_tablestats_filtered.substitution", "$1"})
	if err2 != nil {
		t.Fatal(err)
	}

	mock.ExpectQuery(sanitizeQuery(userstatCheckQuery)).WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("userstat", "ON"))

	columns := []string{"TABLE_SCHEMA", "TABLE_NAME", "ROWS_READ", "ROWS_CHANGED", "ROWS_CHANGED_X_INDEXES"}
	rows := sqlmock.NewRows(columns).
		AddRow("mysql", "db_1", 5, 0, 8).
		AddRow("mysql", "db_2", 5, 0, 8).
		AddRow("mysql", "db_3", 5, 0, 8).
		AddRow("mysql", "proxies_priv", 99, 1, 0).
		AddRow("mysql", "user", 1064, 2, 5)
	mock.ExpectQuery(sanitizeQuery(tableStatFilteredQuery)).WillReturnRows(rows)

	ch := make(chan prometheus.Metric)
	go func() {
		if err = (ScrapeTableStatFiltered{}).Scrape(context.Background(), db, ch); err != nil {
			t.Errorf("error calling function on test: %s", err)
		}
		close(ch)
	}()

	expected := []MetricResult{
		{labels: labelMap{"schema": "mysql", "table": "db"}, value: 15},
		{labels: labelMap{"schema": "mysql", "table": "db"}, value: 0},
		{labels: labelMap{"schema": "mysql", "table": "db"}, value: 24},
		{labels: labelMap{"schema": "mysql", "table": "proxies_priv"}, value: 99},
		{labels: labelMap{"schema": "mysql", "table": "proxies_priv"}, value: 1},
		{labels: labelMap{"schema": "mysql", "table": "proxies_priv"}, value: 0},
		{labels: labelMap{"schema": "mysql", "table": "user"}, value: 1064},
		{labels: labelMap{"schema": "mysql", "table": "user"}, value: 2},
		{labels: labelMap{"schema": "mysql", "table": "user"}, value: 5},
	}
	convey.Convey("Metrics comparison", t, func() {
		for _, expect := range expected {
			got := readMetric(<-ch)
			convey.So(expect, convey.ShouldResemble, got)
		}
	})

	// Ensure all SQL queries were executed
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled exceptions: %s", err)
	}
}
