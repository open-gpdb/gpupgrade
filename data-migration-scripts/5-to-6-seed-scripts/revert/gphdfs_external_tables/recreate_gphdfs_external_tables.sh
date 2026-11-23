#!/bin/bash
# Copyright (c) 2017-2023 VMware, Inc. or its affiliates
# SPDX-License-Identifier: Apache-2.0

if [ "$#" -ne 3 ]; then
    echo "Illegal number of parameters"
    echo "Usage: $(basename "$0") <GPHOME> <PGPORT> <DBNAME>"
    exit 1
fi

GPHOME=$1
PGPORT=$2
DBNAME=$3

main() {
    local psql="$GPHOME"/bin/psql
    local pg_dump="$GPHOME"/bin/pg_dump

    local table_names table
    local tables=()

    # Find all GPHDFS external tables.
    table_names=$("$psql" -X -v ON_ERROR_STOP=1 -d "$DBNAME" -h localhost -p "$PGPORT" -Atc "
        SELECT d.objid::regclass
        FROM pg_catalog.pg_depend d
               JOIN pg_catalog.pg_exttable x ON ( d.objid = x.reloid )
               JOIN pg_catalog.pg_extprotocol p ON ( p.oid = d.refobjid )
               JOIN pg_catalog.pg_class c ON ( c.oid = d.objid )
        WHERE d.refclassid = 'pg_extprotocol'::regclass
            AND p.ptcname = 'gphdfs';
    ") || return $?

    if [[ -z "$table_names" ]]; then
        # Don't pg_dump if there are no interesting tables; we'll get useless
        # SQL files.
        return 0
    fi

    while IFS= read -r table; do
        tables+=(-t "$table")
    done < <(printf '%s\n' "$table_names") || return $?

    "$pg_dump" -h localhost -p "$PGPORT" --schema-only "${tables[@]}" "$DBNAME"
}

main
