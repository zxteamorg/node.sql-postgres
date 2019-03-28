#!/bin/bash

# Runtime
SCRIPT_DIR=$(dirname "$0")
cd "${SCRIPT_DIR}"
SCRIPT_DIR=$(pwd -LP)
cd - > /dev/null
PROJECT_DIR=`dirname "${SCRIPT_DIR}"`

echo "Working directory is ${PROJECT_DIR}"

SQL_FILE="${PROJECT_DIR}/test/general.test.pgsql"

if [ ! -f "${SQL_FILE}" ]; then
	echo "SQL Bundle file '${SQL_FILE}' was not found" >&2
	exit -12
fi

if [ -n "$TEST_DB_URL" ]; then
	# extract the protocol
	proto="`echo $TEST_DB_URL | grep '://' | sed -e's,^\(.*://\).*,\1,g'`"
	# remove the protocol
	url=`echo $TEST_DB_URL | sed -e s,$proto,,g`

	# extract the user and password (if any)
	PGUSER="`echo $url | grep @ | cut -d@ -f1`"

	# extract the host -- updated
	hostport=`echo $url | sed -e s,$PGUSER@,,g | cut -d/ -f1`
	PGHOST=`echo $hostport | grep : | cut -d: -f1`
	PGPORT=`echo $hostport | grep : | cut -d: -f2`
	# extract the path (if any)
	PGDB="`echo $url | grep / | cut -d/ -f2-`"
else
	PGHOST=localhost
	PGPORT=5432
	PGDB=emptytestdb
	PGUSER=devtest
fi

CONNECTION_OPTS="--host=${PGHOST} --port=${PGPORT}"
POSTGRES_USER="--username=postgres --no-password"
DEVTEST_USER="--username=${PGUSER} --no-password"
echo "Reseting database..."
echo "Connection opts: ${CONNECTION_OPTS}"

echo " * Apply SQL Bundle ${SQL_FILE}"
psql ${CONNECTION_OPTS} ${DEVTEST_USER} --quiet "--dbname=${PGDB}" "--file=${SQL_FILE}" || exit -34

echo
echo "Reset database completed"
