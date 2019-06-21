#!/usr/bin/env sh

PATH_DASHBOARD=/dashboards

curl http://admin:admin@grafana:3000/api/datasources -H "Content-Type: application/json" -X POST -d "{\"name\":\"prometheus\",\"type\":\"prometheus\",\"url\":\"http://prometheus:9090\",\"access\":\"proxy\",\"isDefault\":true}"

for f in ${PATH_DASHBOARD}/*
do
    echo "creation du dashboard $(basename ${f})"
    curl http://admin:admin@grafana:3000/api/dashboards/db -H "Content-Type: application/json" -X POST -d @${PATH_DASHBOARD}/$(basename ${f})
done
