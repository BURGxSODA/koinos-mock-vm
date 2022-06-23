const { SoMap } = require('somap')
const { koinos } = require('koinos-proto-js')
const { arraysAreEqual, toHexString } = require('./util')

function canonicalizeSpace(space) {
  return {
    id: space.id != 0 ? space.id: null,
    system: space.system ? space.system : null,
    zone: space.zone && space.zone.length != 0 ? space.zone : null }
}

class Database {
  constructor () {
    this.initDb()
  }

  initDb (arr = []) {
    console.log("init")
    this.db = new SoMap(arr, this.comparator)
    this.commitTransaction()
  }

  comparator (a, b) {
    if (a > b) {
      return 1
    } else if (a < b) {
      return -1
    } else {
      return 0
    }
  }

  commitTransaction () {
    console.log("commit")
    this.backupDb = new SoMap(this.db)
  }

  rollbackTransaction () {
    console.log("rollback")
    this.initDb(this.backupDb)
    this.commitTransaction()
  }

  putObject (space, key, obj) {
    const dbKey = koinos.chain.database_key.encode({ space: canonicalizeSpace(space), key }).finish()
    let bytesUsed = 0

    const currentObj = this.db.has(dbKey)

    if (currentObj) {
      bytesUsed -= currentObj.byteLength
    }

    console.log("putObject: " + toHexString(dbKey) + ", " + toHexString(obj));

    this.db.set(dbKey, obj)

    bytesUsed += obj.byteLength

    return bytesUsed
  }

  removeObject (space, key) {
    const dbKey = koinos.chain.database_key.encode({ space: canonicalizeSpace(space), key }).finish()

    console.log("removeObject: " + toHexString(dbKey));

    this.db.delete(dbKey)
  }

  getObject (space, key) {
    const dbKey = koinos.chain.database_key.encode({ space: canonicalizeSpace(space), key }).finish()
    const value = this.db.get(dbKey)

    console.log("getObject: " + toHexString(dbKey))

    if (value) {
      console.log("getObject: " + toHexString(value))
      return koinos.chain.database_object.create({ exists: true, value })
    }

    return null
  }

  getNextObject (space, key) {
    const dbKey = koinos.chain.database_key.encode({ space: canonicalizeSpace(space), key }).finish()
    console.log("getNextObject: " + toHexString(dbKey));
    if (!this.db.get(dbKey)) {
      return null
    }

    const keys = [...this.db.keys()]

    for (let i = 0; i < keys.length; i++) {
      const currKey = keys[i]

      if (arraysAreEqual(currKey, dbKey)) {
        if ((i + 1) < keys.length) {
          const nextKey = keys[i + 1]
          const nextVal = this.db.get(nextKey)

          const decodedNextKey = koinos.chain.database_key.decode(nextKey)

          if (decodedNextKey.space.system === space.system &&
            decodedNextKey.space.id === space.id &&
            arraysAreEqual(decodedNextKey.space.zone, space.zone)) {
            return koinos.chain.database_object.create({ exists: true, value: nextVal, key: decodedNextKey.key })
          }
        }
      }
    }

    return null
  }

  getPrevObject (space, key) {
    const dbKey = koinos.chain.database_key.encode({ space: canonicalizeSpace(space), key }).finish()
    console.log("getPrevObject: " + toHexString(dbKey));

    if (!this.db.get(dbKey)) {
      return null
    }

    const keys = [...this.db.keys()]

    for (let i = keys.length - 1; i >= 0; i--) {
      const currKey = keys[i]

      if (arraysAreEqual(currKey, dbKey)) {
        if ((i - 1) >= 0) {
          const prevKey = keys[i - 1]
          const prevVal = this.db.get(prevKey)

          const decodedPrevKey = koinos.chain.database_key.decode(prevKey)

          if (decodedPrevKey.space.system === space.system &&
            decodedPrevKey.space.id === space.id &&
            arraysAreEqual(decodedPrevKey.space.zone, space.zone)) {
            return koinos.chain.database_object.create({ exists: true, value: prevVal, key: decodedPrevKey.key })
          }
        }
      }
    }

    return null
  }
}

module.exports = {
  Database
}
