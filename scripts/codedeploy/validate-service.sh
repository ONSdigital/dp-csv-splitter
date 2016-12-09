#!/bin/bash

if [[ $(docker inspect --format="{{ .State.Running }}" dp-csv-splitter) == "false" ]]; then
  exit 1;
fi
