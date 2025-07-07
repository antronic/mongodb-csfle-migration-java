// ==================================================================
// START - Configurable options
//
// Define constants
//
// Define the big collection threshold, how many documents a collection must have to be considered "big"
const BIG_COLLECTION_THRESHOLD = 5000000
// Define the count type (actual or estimated)
//
// Available value: 'actual', 'estimated'
const COUNT_TYPE = 'estimated'
//
// END - Configurable options
// ==================================================================
// List of databases to exclude from inventory assessment
const EXCLUDED_DATABASES = ['admin', 'local', 'config']
// ==================================================================
//
/*
[
  { db: 'db_1', collections: { 'coll_1': 0, 'coll_2': 0 }}
]
*/
const targetDatabases = getTargetDatabasesAndCollections()

/**
 * Get the list of target databases for inventory assessment.
 * @returns {Array} - An array of target database names.
 */
function getTargetDatabasesAndCollections() {
  const targetDatabases = []
  const databases = db.adminCommand({ listDatabases: 1 }).databases
  // Iterate through each database and get its collections
  for (const dbInfo of databases) {
    if (!EXCLUDED_DATABASES.includes(dbInfo.name)) {
      const currentDatabase = { db: dbInfo.name, collections: {} }
      // Get the list of collections for the current database
      const collections = db.getSiblingDB(dbInfo.name).getCollectionNames()
      for (const collName of collections) {
        currentDatabase.collections[collName] = 0
      }
      // Add the current database to the target databases list
      targetDatabases.push(currentDatabase)
    }
  }
  return targetDatabases
}
//
/**
 * Get the estimated document sizes for each collection in the target databases.
 * @returns {Array} - An array of objects containing database and collection information.
 */
function getEstimatedCollectionDocsSizes() {
  const data = []
  //
  for (const dbInfo of targetDatabases) {
    const currentDatabase = { db: dbInfo.db, collections: {} }
    for (const [collName] of Object.entries(dbInfo.collections)) {
      const estimatedDocs = db.getSiblingDB(dbInfo.db).getCollection(collName).estimatedDocumentCount()
      currentDatabase.collections[collName] = estimatedDocs
    }
    data.push(currentDatabase)
  }
  return data
}
//
/**
 * Get the actual document sizes for each collection in the target databases.
 * @returns {Array} - An array of objects containing database and collection information.
 */
function getActualCollectionDocsSize() {
  const data = []
  //
  for (const dbInfo of targetDatabases) {
    const currentDatabase = { db: dbInfo.db, collections: {} }
    for (const [collName] of Object.entries(dbInfo.collections)) {
      const actualDocs = db.getSiblingDB(dbInfo.db).getCollection(collName).countDocuments()
      currentDatabase.collections[collName] = actualDocs
    }
    data.push(currentDatabase)
  }
  return data
}

//
/**
 * Generate a CSV representation of the inventory assessment data.
 * @param {*} data - The inventory assessment data.
 * @returns {string} - The CSV representation of the data.
 */
function generateCSV(data) {
  const csvRows = []
  //
  const totalDocColName = COUNT_TYPE === 'actual' ? 'Actual Documents' : 'Estimated Documents'
  // Get the headers
  const headers = ['Database', 'Collection', totalDocColName, 'Size class']
  csvRows.push(headers.join(','))

  // Format the data
  for (const dbInfo of data) {
    for (const [collName, docCount] of Object.entries(dbInfo.collections)) {
      const sizeClass = docCount > BIG_COLLECTION_THRESHOLD ? 'big' : 'small'
      const row = [dbInfo.db, collName, docCount, sizeClass]
      csvRows.push(row.join(','))
    }
  }
  return csvRows.join('\n')
}

// const startMs = Date.now()
if (COUNT_TYPE === 'actual') {
  console.log(generateCSV(getActualCollectionDocsSize()))
} else if (COUNT_TYPE === 'estimated') {
  console.log(generateCSV(getEstimatedCollectionDocsSizes()))
} else {
  throw new Error('[X] Invalid COUNT_TYPE specified.')
}
// const endMs = Date.now()
// print(`[> ] Inventory assessment completed in ${endMs - startMs} ms`)