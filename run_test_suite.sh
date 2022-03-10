# THROUGHPUT=(100 500 1000 10000)
# SIZE=(100 500 1000 10000)

THROUGHPUT=(100)
SIZE=(100)


function log() {
    local log_message=$1
    
    echo "[$(date -u)] ${log_message}"
}


function cleanUp() {
    log "Cleaning up Docker containers and volumes"
    docker-compose -f master-kafka-single-node-full-stack.yml down --remove-orphans -v

    log "Removing created Zookeeper volumnes"
    rm -rf volumes/zookeeper/log/version-2
    rm -rf volumes/zookeeper/data/version-2
}

function startUp() {
    log "Creating folders for Zookeeper volumnes"

    # Occassionally this error would appear:
    # "Unable to create data directory /var/lib/zookeeper/log/version-2"
    # Although it appears to be fixed (see below Github PR), the latest Confluent Docker image still exhibits this behaviour
    # It is unreliable to produce as it only occassionally occurs
    # Workaround is to create the folders for Zookeeper and then mount the folders
    # https://github.com/apache/zookeeper/pull/1225
    
    mkdir -p volumes/zookeeper/log/version-2
    mkdir -p volumes/zookeeper/data/version-2

    # Create directory to store test data
    mkdir -p test-data

    log "Starting up Docker containers"
    docker-compose -f master-kafka-single-node-full-stack.yml up -d

    while true; do
        # Kafka Connect is exposed on port 8083. 
        # We check every 10 seconds whether the application has finished starting up.
        log "Checking Kafka start up..."
        local CONNECT_START_UP=$(curl "http://0.0.0.0:8083/" | jq -r '.version')

        if [ -n "${CONNECT_START_UP}" ]; then
            log "Kafka Connect is healthy"
            break
        fi

        log "Waiting 10 seconds before checking again..."
        sleep 10
    done

    log "Adding topics and Mongo Sink to Kafka Connect"

    # As we're sending byte strings to Mongo, we need to reformat the data into JSON.
    # We use "transforms" options to do this.

    local MONGO_SINK=$(curl -X POST -H "Content-Type: application/json" --data '
        {"name": "kafka-mongo-sink",
        "config": {
            "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
            "topics": "demo-topic",
            "connection.uri": "mongodb://admin:mongo@mongo:27017",
            "database": "kafka",
            "collection": "perf",
            "errors.log.include.messages": true,
            "errors.deadletterqueue.context.headers.enable": true,
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "org.apache.kafka.connect.storage.StringConverter",
            "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.KafkaMetaDataStrategy",
            "tasks.max": 2,
            "value.converter.schemas.enable":false,
            "transforms": "MakeMap",
            "transforms.MakeMap.type": "org.apache.kafka.connect.transforms.HoistField$Value",
            "transforms.MakeMap.field": "non-json"
        }}' http://localhost:8083/connectors | jq -r '.error_code')

    # Check to see if an error exists
    if [ ! "${MONGO_SINK}" = "null" ]; then
        log "Please remove and clean existing Kafka Connectors"
        exit
        
    else
        log "Kafka Mongo Sink correctly added"
    fi

     log "Start up complete"

}


function runTests() {
    for _THROUGHPUT in ${THROUGHPUT[@]}; do
        for _SIZE in ${SIZE[@]}; do
            log "Starting new test, throughput: ${_THROUGHPUT}, size: ${_SIZE}"

            local EXISTS=$(docker ps -a -f "name=kafka-broker" | wc -l)
            
            if [ "${EXISTS}" -gt 1 ]; then
                # Remove existing containers if it exists
                cleanUp
            fi

            # Start up all containers and mount volumes
            startUp

            # Run tests
            log "Running test..."
            gtimeout 300s docker exec -it kafka-broker kafka-producer-perf-test \
                --throughput "${_THROUGHPUT}" \
                --num-records 10000000 \
                --topic demo-topic \
                --record-size "${_SIZE}" \
                --producer-props bootstrap.servers=kafka-broker:29092


            log "Grathering test data from Prometheus..."
            python scrape_prometheus.py --throughput "${_THROUGHPUT}" --size "${_SIZE}"


            log "Finished running tests for: throughput: ${_THROUGHPUT}, size: ${_SIZE} "
            cleanUp
        done;
    done

    log "Combining test data together"
    python combine_test_data.py 

    log "Test suite has finished running! Congratulations!"
}

runTests