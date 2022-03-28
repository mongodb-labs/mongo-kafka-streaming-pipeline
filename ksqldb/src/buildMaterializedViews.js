const { MongoClient } = require("mongodb")


const URI =
    "mongodb://admin:mongo@localhost:27017/?authSource=admin&readPreference=primary&directConnection=true"


const DEFAULT_MAT_VIEW_DB_NAME = "materialized-views"

const client = new MongoClient(URI)
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
 * Builds a materialized view.
 * @param {string} matViewName - the name of the materialized view.
 * @param {string} dbName - database of the source data.
 * @param {string} collection - collection of the source data.
 * @param {string} pipeline - aggregation pipeline to create the materialized view.
 */
async function buildMaterializedView(matViewName, dbName, collection, pipeline) {
    try {
        // Get Mongo client object
        const _client = await getMongoClient()
        const db = _client.db(dbName)
        const coll = db.collection(collection)

        // Add in merge stage
        let _pipeline = [...pipeline]
        _pipeline.push({ $merge: { into: { db: DEFAULT_MAT_VIEW_DB_NAME, coll: matViewName }, whenMatched: "replace" } })

        // https://jira.mongodb.org/browse/NODE-1398
        // https://stackoverflow.com/questions/49835278/mongodb-node-js-out-with-aggregation-only-working-if-calling-toarray
        await coll.aggregate(_pipeline).toArray()

        // Save metadata about this materialized view
        await client.db(DEFAULT_MAT_VIEW_DB_NAME).collection('metadata').replaceOne({
            _id: matViewName,
        }, {
            _id: matViewName,
            pipeline,
            source: {
                db: dbName,
                coll: collection
            },
            destination: {
                db: DEFAULT_MAT_VIEW_DB_NAME,
                coll: matViewName
            }
        }, { upsert: true })
    } catch (err) {
        console.error(`Something went wrong: ${err}`)
    }
}


/**
 * Runs three pipelines and creates materialized views out of them.
 */
async function run() {
    try {
        const db = 'kafka'
        const collection = 'purchases'
        const pipeline = [
            {
                $addFields: {
                    sales:
                        { $multiply: ["$price_per_unit", "$quantity"] }
                }
            },
            {
                $group:
                {
                    _id: "$item_type",
                    totalSales: { $sum: "$sales" },
                    totalQuantity: { $sum: "$quantity" },
                    avgSales: { $avg: "$sales" }
                }
            }
        ]

        await buildMaterializedView('test', db, collection, pipeline).catch(console.dir)
    } catch (err) {
        console.log(err)
    } finally {
        // Ensures that the client will close when you finish/error
        await client.close()
    }
}

run()