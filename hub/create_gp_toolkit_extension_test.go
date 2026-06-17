// Copyright (c) 2017-2023 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package hub_test

import (
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/greenplum-db/gpupgrade/hub"
	"github.com/greenplum-db/gpupgrade/testutils"
)

func TestGetConnectableDatabases(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("couldn't create sqlmock: %v", err)
	}
	defer testutils.FinishMock(mock, t)
	defer db.Close()

	t.Run("returns all connectable databases", func(t *testing.T) {
		expectPgDatabaseToReturn(mock).WillReturnRows(sqlmock.NewRows([]string{"datname"}).
			AddRow("template1").
			AddRow("postgres").
			AddRow("testdb"))

		databases, err := hub.GetConnectableDatabases(db)
		if err != nil {
			t.Errorf("unexpected error: %#v", err)
		}

		expected := []string{"template1", "postgres", "testdb"}
		if !reflect.DeepEqual(databases, expected) {
			t.Errorf("got %v, want %v", databases, expected)
		}
	})

	t.Run("errors when failing to query", func(t *testing.T) {
		expected := os.ErrPermission
		expectPgDatabaseToReturn(mock).WillReturnError(expected)

		databases, err := hub.GetConnectableDatabases(db)
		if !errors.Is(err, expected) {
			t.Errorf("got %v want %v", err, expected)
		}

		if databases != nil {
			t.Error("expected nil databases")
		}
	})

	t.Run("errors when failing to scan", func(t *testing.T) {
		expectPgDatabaseToReturn(mock).WillReturnRows(sqlmock.NewRows([]string{}).
			AddRow()) // return fewer fields than scan expects

		databases, err := hub.GetConnectableDatabases(db)
		if !strings.Contains(err.Error(), "scanning pg_database") {
			t.Errorf(`expected %v to contain "scanning pg_database"`, err)
		}

		if databases != nil {
			t.Error("expected nil databases")
		}
	})

	t.Run("errors when iterating the rows", func(t *testing.T) {
		expected := os.ErrPermission
		expectPgDatabaseToReturn(mock).WillReturnRows(sqlmock.NewRows([]string{"datname"}).
			AddRow("postgres").
			RowError(0, expected))

		databases, err := hub.GetConnectableDatabases(db)
		if !errors.Is(err, expected) {
			t.Errorf("got %v want %v", err, expected)
		}

		if databases != nil {
			t.Error("expected nil databases")
		}
	})
}

func expectPgDatabaseToReturn(mock sqlmock.Sqlmock) *sqlmock.ExpectedQuery {
	return mock.ExpectQuery(`SELECT datname FROM pg_database WHERE datallowconn AND datname != 'template0';`)
}
