function log() {
    local log_message=$1
    
    echo "[$(date -u)] ${log_message}"
}

function addConnectors() {
    MONGO_SINK=$(curl -X POST -H "Content-Type: application/json" --data '
        {"name": "kafka-mongo-sink1",
        "config": {
            "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
            "topics": "purchases",
            "connection.uri": "mongodb://admin:mongo@mongo:27017",
            "database": "kafka",
            "collection": "purchases",
            "writemodel.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.UpdateOneTimestampsStrategy",
            "errors.log.include.messages": true,
            "errors.deadletterqueue.context.headers.enable": true,
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInValueStrategy",
            "tasks.max": 2,
            "value.converter.schemas.enable":false
        }}' http://localhost:8083/connectors | jq -r '.error_code')

    DATAGEN_SOURCE=$(curl -X POST -H "Content-Type: application/json" --data '
        {
            "name": "datagen-purchases",
            "config": {
                "connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
                "kafka.topic": "purchases",
                "quickstart": "purchases",
                "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "value.converter.decimal.format": "NUMERIC",
                "max.interval": 1000,
                "tasks.max": "1"
            }
            }' http://localhost:8083/connectors | jq -r '.error_code')


    if [ ! "${MONGO_SINK}" = "null" ] && [ ! "${DATAGEN_SOURCE}" = "null" ]; then
        log "Please remove and clean existing Kafka Connectors"
        exit
        
    else
        log "Connectors correctly added"
    fi
}

function startUp() {
    local EXISTS=$(docker ps -a -f "name=broker" | wc -l)
            
    if [ "${EXISTS}" -gt 1 ]; then
        # Remove existing containers if it exists
        docker-compose down --remove-orphans -v
    fi

    # Create keyfile for replica set before starting Docker containers
    openssl rand -base64 741 > keyfile
    chmod 600 keyfile

    log "Starting up Docker containers"
    docker-compose up -d

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

    addConnectors

    log "Start up complete"

}


startUp