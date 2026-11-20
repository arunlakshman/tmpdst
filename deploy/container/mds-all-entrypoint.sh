#!/bin/sh
# Demo / quickstart only: one container runs the control plane and the broker
# with in-memory stores. Not for production.
set -eu

/mdscontrolplane -store mem -addr :8080 -metrics-addr :9101 &
cp_pid=$!

i=0
while [ "$i" -lt 60 ]; do
	if wget -q -O /dev/null http://127.0.0.1:9101/metrics 2>/dev/null; then
		break
	fi
	i=$((i + 1))
	sleep 0.25
done

# Advertise loopback so a Kafka client on the host (with -p 9092:9092) sees a
# reachable broker address. Override with a custom image or a different entrypoint
# if you publish a different host port.
/mdsbroker -objects mem -cp-url http://127.0.0.1:8080 \
	-listen 0.0.0.0:9092 -advertise 127.0.0.1:9092 -metrics-addr :9100 &
broker_pid=$!

term() {
	kill -TERM "$broker_pid" "$cp_pid" 2>/dev/null || true
	wait "$broker_pid" 2>/dev/null || true
	wait "$cp_pid" 2>/dev/null || true
}
trap term INT TERM HUP

wait "$broker_pid"
status=$?
kill -TERM "$cp_pid" 2>/dev/null || true
wait "$cp_pid" 2>/dev/null || true
exit "$status"
