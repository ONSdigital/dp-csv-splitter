#!/bin/bash -ux

pushd dp-csv-splitter
  TAG=$(git describe --exact-match HEAD 2>/dev/null)
  REV=$(git rev-parse --short HEAD)
popd

if [[ $TAG =~ ^release/([0-9]+\.[0-9]+\.[0-9]+(\-rc[0-9]+)?$) ]]; then
  echo ${BASH_REMATCH[1]} > artifacts/revision
else
  echo $REV > artifacts/revision
fi

mv bin/dp-csv-splitter artifacts/
