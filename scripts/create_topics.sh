#!/usr/bin/env sh
# Create every topic in events/topics.yml.
#
# Topics are created explicitly rather than left to auto-creation. An auto-created topic
# gets one partition, default retention and no record of why — and then the first thing
# anyone notices is that ordering guarantees do not hold, three weeks later.
#
# Idempotent: creating a topic that exists is reported and skipped, so this runs on every
# `docker compose up` without complaint.
#
# Deliberately POSIX sh with no yaml parser. The image this runs in is the Kafka image,
# which has a JVM and shell and nothing else. The topic list is duplicated here from
# events/topics.yml, and tests/integration asserts that the running cluster matches that
# file — so the duplication cannot drift silently.

set -eu

BOOTSTRAP="${KAFKA_BROKERS:-kafka:9092}"
PARTITIONS="${TOPIC_PARTITIONS:-6}"
REPLICATION="${TOPIC_REPLICATION:-1}"
RETENTION_MS="${TOPIC_RETENTION_MS:-604800000}"
DLQ_RETENTION_MS="${DLQ_RETENTION_MS:-2592000000}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"

echo "waiting for kafka at ${BOOTSTRAP}"
i=0
until "${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --list >/dev/null 2>&1; do
    i=$((i + 1))
    if [ "${i}" -gt 60 ]; then
        echo "kafka did not become reachable after 60 attempts" >&2
        exit 1
    fi
    sleep 2
done
echo "kafka is up"

create_topic() {
    name="$1"
    partitions="$2"
    retention="$3"

    if "${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" \
            --describe --topic "${name}" >/dev/null 2>&1; then
        echo "  exists   ${name}"
        return 0
    fi

    "${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" \
        --create \
        --topic "${name}" \
        --partitions "${partitions}" \
        --replication-factor "${REPLICATION}" \
        --config "retention.ms=${retention}" \
        --config "cleanup.policy=delete" \
        --config "compression.type=gzip" \
        --config "max.message.bytes=1048576" >/dev/null
    echo "  created  ${name}  (${partitions} partitions, retention ${retention}ms)"
}

echo "creating live topics"
for topic in \
    orders.v1.events \
    payments.v1.events \
    inventory.v1.events
do
    create_topic "${topic}" "${PARTITIONS}" "${RETENTION_MS}"
done

# notifications keeps 30 days: it is the record of what a customer was and was not told.
create_topic "notifications.v1.events" "${PARTITIONS}" "${DLQ_RETENTION_MS}"

# Dead letter topics. One partition each, because DLQ throughput is expected to be near
# zero and total ordering is more useful than parallelism when a human is reading it.
echo "creating dead letter topics"
for topic in \
    orders.v1.events.dlq \
    payments.v1.events.dlq \
    inventory.v1.events.dlq \
    notifications.v1.events.dlq
do
    create_topic "${topic}" 1 "${DLQ_RETENTION_MS}"
done

echo
echo "topics now present:"
"${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --list | sed 's/^/  /'
