#!/bin/bash

protoc --plugin="$(which protoc-gen-bq-schema)" --bq-schema_out=./acit/schemas/ -I=. acit/schema.proto
protoc --python_out=. --pyi_out=. -I=. acit/schema.proto acit/bq_table.proto
