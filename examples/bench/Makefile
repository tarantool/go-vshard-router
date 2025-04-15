start:
	docker-compose up -d --no-deps --build webserver

wrk:
	go-wrk http://0.0.0.0:8080

k6:
	K6_PROMETHEUS_RW_SERVER_URL=http://0.0.0.0:9090/api/v1/write \
	k6 run -o experimental-prometheus-rw k6.js --vus 1000 --duration 60s
