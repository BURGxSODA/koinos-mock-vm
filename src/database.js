const { koinos } = require('koinos-proto-js')
const { arraysAreEqual, toHexString, UInt8ArrayToString, StringToUInt8Array } = require('./util')

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
    this.db = new Map(arr)
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
    this.backupDb = new Map(this.db)
  }

  rollbackTransaction () {
    console.log("rollback")
    this.db = new Map(this.backupDb)
    this.commitTransaction()
  }

  putObject (space, key, obj) {
    const dbKey = koinos.chain.database_key.encode({ space: canonicalizeSpace(space), key }).finish()
    console.log("putObject: " + toHexString(dbKey) + ", " + toHexString(obj));

    this.db.set(UInt8ArrayToString(dbKey), UInt8ArrayToString(obj))
    console.log(this.db)
  }

  removeObject (space, key) {
    const dbKey = koinos.chain.database_key.encode({ space: canonicalizeSpace(space), key }).finish()

    console.log("removeObject: " + toHexString(dbKey));

    this.db.delete(UInt8ArrayToString(dbKey))
  }

  getObject (space, key) {
    const dbKey = koinos.chain.database_key.encode({ space: canonicalizeSpace(space), key }).finish()
    const strValue = this.db.get(UInt8ArrayToString(dbKey))

    console.log(this.db)
    console.log("getObject: " + toHexString(dbKey))

    if (strValue !== undefined) {
      const value = StringToUInt8Array(strValue)
      console.log("found! " + toHexString(value))
      return koinos.chain.database_object.create({ exists: true, value })
    }

    return null
  }

  getNextObject (space, key) {
    const dbKey = koinos.chain.database_key.encode({ space: canonicalizeSpace(space), key }).finish()
    console.log("getNextObject: " + toHexString(dbKey));

    const keys = [...this.db.keys()].sort(this.comparator)

    for (let i = 0; i < keys.length; i++) {
      const currKey = keys[i]
      const decodedCurrKey = koinos.chain.database_key.decode(currKey)

      // if the current key belongs to the space
      if (decodedCurrKey.space.system === space.system &&
        decodedCurrKey.space.id === space.id &&
        arraysAreEqual(decodedCurrKey.space.zone, space.zone)) {
        // if it's the key we are looking for, get the next objec if exists
        if (arraysAreEqual(currKey, dbKey) && (i + 1) < keys.length) {
          const nextKey = keys[i + 1]
          const nextVal = this.db.get(nextKey)

          const decodedNextKey = koinos.chain.database_key.decode(nextKey)

          if (decodedNextKey.space.system === space.system &&
            decodedNextKey.space.id === space.id &&
            arraysAreEqual(decodedNextKey.space.zone, space.zone)) {
            return koinos.chain.database_object.create({ exists: true, value: StringToUInt8Array(nextVal), key: decodedNextKey.key })
          }
        } else if (currKey > dbKey) {
          // if the current key is greater than the one we're looking for
          // then, the current key is considered the next key
          const nextVal = this.db.get(currKey)
          return koinos.chain.database_object.create({ exists: true, value: StringToUInt8Array(nextVal), key: decodedCurrKey.key })
        }
      }
    }

    return null
  }

  getPrevObject (space, key) {
    const dbKey = koinos.chain.database_key.encode({ space: canonicalizeSpace(space), key }).finish()
    console.log("getPrevObject: " + toHexString(dbKey));

    const keys = [...this.db.keys()].sort(this.comparator)

    for (let i = keys.length - 1; i >= 0; i--) {
      const currKey = keys[i]
      const decodedCurrKey = koinos.chain.database_key.decode(currKey)

      // if the current key belongs to the space
      if (decodedCurrKey.space.system === space.system &&
        decodedCurrKey.space.id === space.id &&
        arraysAreEqual(decodedCurrKey.space.zone, space.zone)) {
        // if it's the key we are looking for, get the next objec if exists
        if (arraysAreEqual(currKey, dbKey) && (i - 1) >= 0) {
          const prevKey = keys[i - 1]
          const prevVal = this.db.get(prevKey)

          const decodedPrevKey = koinos.chain.database_key.decode(prevKey)

          if (decodedPrevKey.space.system === space.system &&
            decodedPrevKey.space.id === space.id &&
            arraysAreEqual(decodedPrevKey.space.zone, space.zone)) {
            return koinos.chain.database_object.create({ exists: true, value: StringToUInt8Array(prevVal), key: decodedPrevKey.key })
          }
        } else if (currKey < dbKey) {
          // if the current key is lower than the one we're looking for
          // then, the current key is considered the prev key
          const prevVal = this.db.get(currKey)
          return koinos.chain.database_object.create({ exists: true, value: StringToUInt8Array(prevVal), key: decodedCurrKey.key })
        }
      }
    }

    return null
  }
}

module.exports = {
  Database
}
