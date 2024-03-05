'use strict'

/**
 * Simple Graph Node for use with RedisGraphLite Engine. This graph is interacted through graph Nodes and not queries
 * Original inspiration from redis-graph: https://github.com/tblobaum/redis-graph
 * @author Nathaniel Thomas
 */

let clientConn

/**
 * Set the client object to be used for graph connections
 * @param {Object} client - client connection using ioredis or other compatible redis connection
 */
const setClient = (client) => {
  clientConn = client
}

/**
 * Edge Class Instance. For use within Node Class
 * @class
 */
class Edge {
  /**
   * Construct an Edge relationship for a Node
   * @constructor
   * @param {string} nodeName - name of the Node for which to generate Edges
   * @param {string} edgeType - type of edge relationship (e.g. member)
   * @param {string} alternateEdgeType - the alternate edge relationship to set on external Nodes (e.g. membership)
   * @returns {Object} Edge instance
   */
  constructor (nodeName, edgeType, alternateEdgeType) {
    this['nodeName'] = nodeName
    this['clientConn'] = clientConn

    this.edgeType = edgeType
    this.edgePrefix = `${this.edgeType}_`
    this.edgeNodeName = `${this.edgeType}_${this['nodeName']}`

    this.alternateEdgeType = alternateEdgeType
    this.alternateEdgePrefix = `${this.alternateEdgeType}_`
    this.alternateEdgeNodeName = `${this.alternateEdgeType}_${this['nodeName']}`
  }

  /**
   * Add an Edge from this Node to one or more Nodes by name
   * @param {string|string[]} value - string name or Array of names of Node(s) to add Edge relationships
   */
  async add (value) {
    value = Array.isArray(value) ? value : [ value ]

    let promiseResultArray = []

    value.forEach((gid, i) => {
      promiseResultArray.push(new Promise((resolve, reject) => {
        this['clientConn'].sadd(this.edgeNodeName, gid, (err) => {
          if (err) {
            reject(err)
          }

          this['clientConn'].sadd(`${this.alternateEdgePrefix}${gid}`, this['nodeName'], (err) => {
            if (err) {
              reject(err)
            }

            resolve(true)
          })
        })
      }))
    })

    await Promise.all(promiseResultArray)
    return true
  }

  /**
   * Remove an Edge from this Node to one or more Nodes by name
   * @param {string|string[]} value - string name or Array of names of Node(s) to remove Edge relationships
   */
  async remove (value) {
    value = Array.isArray(value) ? value : [ value ]

    let promiseResultArray = []

    value.forEach((gid, i) => {
      promiseResultArray.push(new Promise((resolve, reject) => {
        this['clientConn'].srem(this.edgeNodeName, gid, (err) => {
          if (err) {
            reject(err)
          }

          this['clientConn'].srem(`${this.alternateEdgePrefix}${gid}`, this['nodeName'], (err) => {
            if (err) {
              reject(err)
            }

            resolve(true)
          })
        })
      }))
    })

    await Promise.all(promiseResultArray)
    return true
  }

  /**
   * Retrieve all Edge relationships for this Node
   * @returns {Array} Array of Edge relationships that for this type in this Node
   */
  async all () {
    let result = await new Promise((resolve, reject) => {
      this['clientConn'].smembers(this.edgeNodeName, (err, data) => {
        if (err) {
          reject(err)
        }

        resolve(data)
      })
    })

    return result
  }

  /**
   * Delete all Edge relationships for this Node
   * @returns {Array} Array of relationships that were removed
   */
  async delete () {
    // Load the items that "has" this item
    let result = await this.all()

    if (result) {
      let promiseResultArray = []

      result.forEach((gid, i) => {
        promiseResultArray.push(new Promise((resolve, reject) => {
          this['clientConn'].srem(`${this.edgePrefix}${gid}`, this['nodeName'], (err) => {
            if (err) {
              reject(err)
            }

            this['clientConn'].srem(`${this.alternateEdgePrefix}${gid}`, this['nodeName'], (err) => {
              if (err) {
                reject(err)
              }

              resolve(true)
            })
          })
        }))
      })

      await Promise.all(promiseResultArray)

      await new Promise((resolve, reject) => {
        this['clientConn'].del(this.edgeNodeName, (err) => {
          if (err) {
            reject(err)
          }

          resolve(true)
        })
      })
    }

    return result
  }
}

/**
 * Attribute Class Instance. For use within Node Class
 * @class
 */
class Attribute {
  /**
   * Attribute management for a Node
   * @constructor
   * @param {string} nodeName - name of the Node to receive attributes
   * @returns {Object} Attribute instance
   */
  constructor (nodeName) {
    this['nodeName'] = nodeName
    this['clientConn'] = clientConn

    this.KVSeparator = '|'

    this.nodeAttributesName = `_${this['nodeName']}`
  }

  /**
   * Retrieve all attributes set on the Node
   * @returns {Object} Object containing key/value pairs
   */
  async all () {
    let result = await new Promise((resolve, reject) => {
      // Retrieve the keys set for a node's attributes
      this['clientConn'].smembers(this.nodeAttributesName, (err, keys) => {
        if (err) {
          reject(err)
        }

        if (keys) {
          this['clientConn'].mget(keys.map(k => `${this.nodeAttributesName}${this.KVSeparator}${k}`), (err, data) => {
            if (err) {
              reject(err)
            }

            let result = {}

            if (data) {
              keys.forEach((key, i) => {
                result[key] = data[i]
              })
            }

            resolve(result)
          })
        } else {
          resolve({})
        }
      })
    })

    return result
  }

  /**
   * Retrieve a single attribute value for a single key on the Node
   * @param {string} key
   * @returns {string}
   */
  async get (key) {
    let value = await new Promise((resolve, reject) => {
      this['clientConn'].get(`${this.nodeAttributesName}${this.KVSeparator}${key}`, (err, data) => {
        if (err) {
          reject(err)
        }

        resolve(data)
      })
    })

    return value
  }

  /**
   * Set a key/value pair attribute on the Node
   * @param {string} key
   * @param {string} value
   */
  async set (key, value) {
    // Manage the attributes
    await new Promise((resolve, reject) => {
      // Add the new attribute to the node's member set
      this['clientConn'].sadd(this.nodeAttributesName, key, (err) => {
        if (err) {
          reject(err)
        }

        // Set the value
        this['clientConn'].set(`${this.nodeAttributesName}${this.KVSeparator}${key}`, value, (err) => {
          if (err) {
            reject(err)
          }

          resolve(true)
        })
      })
    })

    return true
  }

  /**
   * Delete a key/value attribute pair on the Node
   * @param {string} key
   */
  async delete (key) {
    // Remove an object's attribute
    await new Promise((resolve, reject) => {
      // Add the new attribute to the node's member set
      this['clientConn'].srem(this.nodeAttributesName, key, (err) => {
        if (err) {
          reject(err)
        }

        // Set the value
        this['clientConn'].del(`${this.nodeAttributesName}${this.KVSeparator}${key}`, (err) => {
          if (err) {
            reject(err)
          }

          resolve(true)
        })
      })
    })

    return true
  }
}

/**
 * Node Class Instance. For use with interacting with the PBID Graph Lite
 * @class
 */
class Node {
  /**
   * Create a new Node in memory for interacting with the graph
   * @constructor
   * @param {string} nodeName - name of Node
   * @param {Object} [options={}] - Object containing options
   * @returns {Object} Node instance
   */
  constructor (nodeName, options = {}) {
    this['nodeName'] = nodeName
    this['innerMethod'] = options.inner || 'membership'
    this['outerMethod'] = options.outer || 'members'
    this['inner'] = options.inner || 'in'
    this['outer'] = options.outer || 'has'
    this['clientConn'] = clientConn

    this[this['innerMethod']] = new Edge(nodeName, this['inner'], this['outer'])
    this[this['outerMethod']] = new Edge(nodeName, this['outer'], this['inner'])

    this.attribute = new Attribute(nodeName)
  }

  delete () {}

  /**
   * Get the name of this Node
   * @returns {string}
   */
  get name () {
    return this['nodeName']
  }
}

exports.setClient = setClient
exports.Node = Node
