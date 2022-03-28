const { MongoClient } = require("mongodb")


const URI =
  "mongodb://admin:mongo@localhost:27017/?authSource=admin&readPreference=primary&directConnection=true"

const DEFAULT_MAT_VIEW_DB_NAME = "materialized-views"

const client = new MongoClient(URI)

const metadataCache = {}
let connect = null

/**
 * Connects to Mongo if an existing connection does not exist.
 */
async function getMongoClient() {
    if (connect === null) {
        connect = await client.connect()
    }
    return connect
}

/**
 * Loads the materialized view metadata into memory.
 */
async function loadMaterializedViewMetadata() {
    try {
        // Get Mongo client object
        const _client = await getMongoClient()
        
        const cursor = await _client.db(DEFAULT_MAT_VIEW_DB_NAME).collection('metadata').find({})
        
        await cursor.forEach((document) => {
            const key = `${document.source.db}-${document.source.coll}`
            if (metadataCache[key] == undefined) {
                metadataCache[key] = []
            }
            metadataCache[key].push(document)
        })

        console.log('Cached metadata', metadataCache)

    } catch (err) {
        console.log(err)
    }
}

/**
 * For each new event, we need to update all materialized views that have the same source collection.
 * 
 * This function parses the aggregation pipeline and udpate the materialised view
 * 
 * NOTE: For demo purposes, as we know what the aggregation pipeline already does, we'll just simulate the parsing.
 * 
 * @param {JSON} newDocument - new event coming from the change stream.
 * @param {string} materializedViewMetadata - materializedView metadata.
 */
async function parseAggregationPipeline(newDocument, materializedViewMetadata) {
    try {
        const quantity = newDocument.quantity
        const sales = newDocument.quantity * newDocument.price_per_unit
        const id = newDocument.item_type

        const _client = await getMongoClient()
        const db = _client.db(materializedViewMetadata.destination.db)
        const collection = db.collection(materializedViewMetadata.destination.coll)

        const res = await collection.updateOne(
            { _id: id },
            { $inc: { 
                totalQuantity: quantity, 
                totalSales: sales  
            } }
        )

        console.log('Success: ', res)
    } catch (err) {
        console.log('Something went wrong: ', err)
    } 
}

/**
 * Listen to the source database and collection for changes.
 * @param {string} sourceDB - database of the source data.
 * @param {string} sourceColl - collection of the source data.
 */
async function listen(sourceDB, sourceColl, metadata) {
    try { 
        // Get Mongo client object
        const _client = await getMongoClient()

        // Watch source collection (a.k.a where your raw data is being sunk)
        const db = _client.db(sourceDB)
        const collection = db.collection(sourceColl)
        const changeStream = collection.watch()
    
        console.log(metadata)
        changeStream.on('change', next => {
            for (const materializedView of metadata) {
                // For each new event, process the pipeline and update the materialised view
                parseAggregationPipeline(next.fullDocument, materializedView)
            }
        })
    } catch (err) {
        console.log(err)
    }
}

async function run() {
    await loadMaterializedViewMetadata()
    listen("kafka", "purchases", metadataCache["kafka-purchases"])
}

run()