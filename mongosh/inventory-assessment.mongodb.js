// ==================================================================
//
// Define constants
const BIG_COLLECTION_THRESHOLD = 10000
//
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

function generateCSV(data) {
  const csvRows = []
  // Get the headers
  const headers = ['Database', 'Collection', 'Estimated Documents', 'Size class']
  csvRows.push(headers.join(','))

  // Format the data
  for (const dbInfo of data) {
    for (const [collName, estimatedDocs] of Object.entries(dbInfo.collections)) {
      const sizeClass = estimatedDocs > BIG_COLLECTION_THRESHOLD ? 'big' : 'small'
      const row = [dbInfo.db, collName, estimatedDocs, sizeClass]
      csvRows.push(row.join(','))
    }
  }
  return csvRows.join('\n')
}

console.log(generateCSV(getEstimatedCollectionDocsSizes()))