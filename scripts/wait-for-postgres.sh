#!/bin/sh
# wait-for-postgres.sh

timeout=$1

until psql -h "localhost" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c '\q'; do
  if [ $timeout -eq 0 ]; then
    >&2 echo "timeout while checking Postgres"
    exit 1
  fi

  >&2 echo "Postgres is unavailable - $timeout attempts remaining"
  sleep 1
  timeout="$(($timeout-1))"
done

>&2 echo "Postgres is up"
