var EventEmitter = require('events').EventEmitter
  , url = require('url')
  , cassandra = require('cassandra-orm')
  , createQueryCtor = require('./query')
  , util = require('./util')
  , fixId = util.fixId
  , solr = require('solr-client')
  , lookup

  , DISCONNECTED  = 1
  , CONNECTING    = 2
  , CONNECTED     = 3
  , DISCONNECTING = 4;

exports = module.exports = plugin;

var intenseLogging = false;

function plugin (racer) {
  lookup = racer['protected'].path.lookup
  Cassandra.prototype.Query = createQueryCtor(racer);
  racer.registerAdapter('db', 'Cassandra', Cassandra);
}

// Make this a racer plugin. This tells `derby.use(...)` to use the plugin on
// racer, not derby.
exports.decorate = 'racer';

exports.useWith = { server: true, browser: false };

// Examples:
// new Cassandra({uri: 'mongodb://localhost:port/database'});
// new Cassandra({
//     host: 'localhost'
//   , port: 27017
//   , database: 'example'
// });
function Cassandra (options) {
  EventEmitter.call(this);
  this.options = options;

  if (this.options.solr)
    this.solr = solr.createClient(this.options.solr);

  var color = require('ansi-color').set
    , bold = function(value) { return color(value, 'bold'); }
    , black = function(value) { return color(value, 'black'); }
    , red = function(value) { return color(value, 'red'); }
    , green = function(value) { return color(value, 'green'); }
    , yellow = function(value) { return color(value, 'yellow'); }
    , blue = function(value) { return color(value, 'blue'); }
    , magenta = function(value) { return color(value, 'magenta'); }
    , cyan = function(value) { return color(value, 'cyan'); }
    , white = function(value) { return color(value, 'white'); }

  if (options.log) {
    this.logger = {};
    this.logger.log = function (cql) {
      console.log.apply(null, [white(new Date), white('(' + process.pid + ')'), yellow('CQL'), bold(cyan('↪'))].concat(cql));
    };
    this.solrLogger = {};
    this.solrLogger.log = function (command, doc) {
      if (doc) {
        console.log.apply(null, [white(new Date), white('(' + process.pid + ')'), yellow('SOLR ' + command), bold(cyan('↪'))].concat(green(JSON.stringify(doc))));
      } else {
        console.log.apply(null, [white(new Date), white('(' + process.pid + ')'), yellow('SOLR ' + command), bold(cyan('↪'))]);
      }
    };
  }


  // TODO Make version scale beyond 1 db
  //      by sharding and with a vector
  //      clock with each member the
  //      version of a shard
  // TODO Initialize the version properly upon web server restart
  //      so it's synced with the STM server (i.e., Redis) version
  this.version = undefined;
}


Cassandra.prototype.__proto__ = EventEmitter.prototype;

Cassandra.prototype.connect = function connect (options, callback) {
  if (typeof options === 'function') {
    callback = options;
    options = null;
  }
  options = options || this.options || {};

  // TODO: Review the options parsing here
  var self = this
    , uri = options.uri || options.host + ':' + options.port + '/' + options.database + '?' +
        'auto_reconnect=' + ('auto_reconnect' in options ? options.auto_reconnect : true)
    , info = url.parse(uri)
    , keyspace = (options.keyspace || (info.pathname && info.pathname.substring(1)) || "Keyspace1").toLowerCase()
  // Allowed values of [true | false | {j:true} | {w:n, wtimeout:n} | {fsync:true}]
  // The default value is false which means the driver receives does not
  // return the information of the success/error of the insert/update/remove
  if (!('safe' in options)) options.safe = false;
  var login = {host:info.hostname, logger: options.logger || this.logger, cqlVersion : '3.0.0', port:info.port || 9160, keyspace:keyspace, user:options.user, pass:options.password};
  this.keyspace = keyspace;
  this.models = {};
  if (callback) this.once('open', callback);
  var self = this;
  cassandra.connect(login, function (err, driver) {
    if (err) console.log(err);
    self.driver = driver;
    self.emit('open', err);
  });
}

Cassandra.prototype.formatIndexDoc = function (collection, doc, id) {
  var o = {};
  for (var i in doc) {
    if (i === '_id' || i === 'id')
      continue;
    var v = doc[i];
    if (this.options.solr && !this.options.solr.schema[i]) {
      if (typeof v === 'string')
        i = i + '_s';
      else if (typeof v === 'number')
        i = i + '_d';
      else if (typeof v === 'date')
        i = i + '_dt';
    }
    o[i] = v;
  }
  o.id = id;
  o.collection_s = collection;
  return o;
}

Cassandra.prototype.indexDoc = function indexDoc(collection, doc, id, where) {
  if (intenseLogging) console.log('indexDoc', where, collection, id, o);
  if (this.solr) {
    function scheduleSolrCommit() {
      clearTimeout(self.commitTimer);
      self.commitTimer = setTimeout(function () {
        if (this.solrLogger) {
          this.solrLogger.log('COMMIT');
        }
        self.solr.commit();
      }, 1000);
    }
    var self = this;
    if (doc) {
      var o = this.formatIndexDoc(collection, doc, id);
      if (this.logger) {
        this.solrLogger.log('ADD', o);
      }
      this.solr.add(o, function(err, obj) {
        if (err) console.log(err);
        scheduleSolrCommit();
      });
    } else {
      this.solr.delete('id', id, function (err, obj) {
        if (err) console.log(err);
        scheduleSolrCommit();
      });
    }
  }
}

Cassandra.prototype.disconnect = function disconnect (callback) {
  this.driver.close(callback);
};

Cassandra.prototype.flush = function flush (cb) {
  this.driver.dropKeyspace(this.keyspace, cb);
};

// Mutator methods called via CustomDataSource.prototype.applyOps
// Cassandra.prototype.update = function update (collection, conds, op, opts, cb) {
//   this.driver.collection(collection).update(conds, op, opts, cb);
// };

Cassandra.prototype.getModel = function getModel (collection, callback) {
  var model = this.models[collection];
  var self = this;
  if (!model) {
    model = this.models[collection] = cassandra.define(collection, { fields: { }, primaryKeys: ['id'] }, null, true);
    model.useConnection(self.driver);
    return model.prepare(function (err) {
      callback(err, model);
    });
  }
  callback(null, model);
}

function _flatten (out, prefix, val, isRoot) {
  if (typeof val === 'string') {
    out[prefix] = val;
  } else if (Array.isArray(val)) {
    for (var j in val) {
      var name = (prefix ? prefix + '.' : '') + j;
      var v = val[j];
      _flatten(out, name, v);
    }
  } else if ((typeof val) === 'object') {
    for (var j in val) {
      if (isRoot) {
        if (j === '_id') {
          val['id'] = val[j];
          delete val[j];
          j = 'id';
        }
      }
      if (j === '$set') {
        _flatten (out, prefix, val[j]);
        continue;
      } else if (j === '$all') {
        out[prefix] = val[j];
        continue;
      }
      var name = (prefix ? prefix + '.' : '') + j;
      var v = val[j];
      _flatten(out, name, v);
    }
  } else
    out[prefix] = val;
}

Cassandra.prototype.flatten = function flatten(val) {
  var obj = {};
  _flatten(obj, null, val, true);
  return obj;
}

Cassandra.prototype.unflatten = function unflatten(val) {
  try {
    if (typeof val === 'string')
      return val;
    var out = undefined;
    var type = 0;
    for (var i in val) {
      var v = val[i];
      var ptr = null;
      var ind = null;
      var items = i.split('.');
      var index = 0;
      while (index < items.length) {
        var j = items[index++];
        type = /[0-9]+/.test(j);
        if (index === 1) {
          if (out === undefined) {
            if (type)
              out = [];
            else
              out = {};
          }
          ptr = out;
        }
        if (index === items.length) {
          ptr[j] = v;
        } else {
          if (ptr[j] === undefined) {
            if (type)
              ptr[j] = [];
            else
              ptr[j] = {};
          }
          ptr = ptr[j]
        }
      }
    }
  } catch (e) {
    console.log('unflatten', e, val);
  }
  return out;
}

Cassandra.prototype.convertQuery = function (doc) {
  if (typeof doc === 'string')
    return doc;
  for (var i in doc) {
    var parts = i.split('.');
    if (parts.length > 1) {
      var val = doc[i];
      delete doc[i];
      var curr = doc;
      for (var i = 0, l = parts.length; i < l; i++) {
        if (i === (l - 1))
          curr[parts[i]] = val;
        else {
          if (!curr[parts[i]]) {
            if (/[0-9]+/.test(parts[i])) {
              curr[parts[i]] = [];
            } else {
              curr[parts[i]] = {};
            }
          }
          curr = curr[parts[i]];
        }
      }
    } else if (i === '_id') {
      doc['id'] = doc[i];
      delete doc[i];
    } 
  }
  return this.flatten(doc);
}

Cassandra.prototype.cleanupId = function (doc, doFlatten) {
  if (typeof doc === 'string')
    return doc;
  var out = {};
  for (var i in doc) {
    if (i === '_id') {
      if (!doFlatten)
        out['id'] = doc[i];
      delete out[i];
    } else if (i === 'id' && doFlatten) {
      delete out['id'];
    } else {
      var j = i;
      if (!doFlatten) {
        j = i.replace(/\./g, '_');
      }
      out[j] = doc[i];
    }
  }
  if (doFlatten)
    return this.flatten(out);
  return out;
}

Cassandra.prototype.convertSort = function (sort) {
  if (sort) {
    console.log("warning: Order by not supported without partition keys");
    return null;
    
    var out = [];
    sort.forEach(function (item) {
      var s = item[0];
      if (item.length > 1 && ((item[1] === 'desc') || (item[1] < 0)))
        s += " DESC";
      else
        s += " ASC";
      out.push(s);
    });
    return out.join(', ');
  }
  return null;
}

Cassandra.prototype.findAndModify = function findAndModify (collection, conds, sort, doc, options, callback) {
  if (intenseLogging) console.log('findAndModify', collection, conds, doc);
  var args = Array.prototype.slice.call(arguments, 1);
  var self = this;
  this.getModel(collection, function (err, model) {
    if (err) {
      return callback(err);
    }
    conds = self.convertQuery(conds);
    id = conds.id && conds.id.split('.');
    doc = self.cleanupId(doc, true);
    if (id && (id.length > 1)) {
      conds.id = id.splice(0, 1)[0];
      id = id.join('.') + '.';
      for (var i in doc) {
        doc[id + i] = doc[i];
        delete doc[i];
      }
    } else {
      id = id.join('.');
    }
    model.one(conds, function (err, oldDoc) {
      if (oldDoc) {
        Object.keys(oldDoc.data).forEach(function (name) {
          if (oldDoc[name] && !doc[name])
            doc[name] = null;
        });
      } else
        oldDoc = {};
      var same = true;
      Object.keys(doc).forEach(function (name) {
        if (oldDoc[name] !== doc[name])
          same = false;
      });
      if (!same) {
        var needMigrate = false;
        for (var i in doc) {
          if (!model.schema.info.fields[i]) {
            model.schema.info.fields[i] = {name:i, type:'Object'};
            needMigrate = true;
          }
        }
        if (needMigrate)
          model.automigrate(update);
        else
          update(null);
        function update(err) {
          if (err) return callback(err);
          if (options.remove) {
            model.destroyById(id, function (err) {
              callback(err);
              self.indexDoc(collection, null, id, 'findAndModifyRemove');
            });
          } else {
            model.update(doc, conds, function (err, info) {
              self.indexDoc(collection, doc, id, 'findAndModify');
              doc._id = doc.id;
              delete doc.id;
              callback(err, self.unflatten(doc));
            }, options);
          }
        }
      } else {
        callback(null, self.unflatten(doc));
      }
    });
  });
}

Cassandra.prototype.rawUpdate = function update (collection, conds, sort, doc, options, callback) {
  if (intenseLogging) console.log('rawUpdate', collection, conds, doc);
  var args = Array.prototype.slice.call(arguments, 1);
  var self = this;
  this.getModel(collection, function (err, model) {
    if (err) return callback(err);
    conds = self.convertQuery(conds);
    model.update(doc, conds, function (err, info) {
      self.indexDoc(collection, doc, conds.id, 'rawUpdate');
      doc._id = doc.id;
      delete doc.id;
      callback(err, doc);
    }, options);
  });
}


Cassandra.prototype.insert = function insert (collection, json, opts, cb) {
  if (intenseLogging) console.log('insert', collection, json);
  // TODO Leverage pkey flag; it may not be _id
  var toInsert = Object.create(json);
  var self = this;
  this.getModel(collection, function (err, model) {
    if (err) return cb(err);
    conds = self.convertQuery(conds);
    if (toInsert._id) {
      json = self.cleanupId(toInsert, true);
      return model.update(json, {where:{id:toInsert._id}}, function (err, info) {
        if (callback) callback(err, info);
      });
    }
    json = self.cleanupId(toInsert, true);
    json.id = new cassandra.UUID();
    model.insert(json, opts, function insertCb (err, doc) {
      self.indexDoc(collection, json, json.id, 'insert');
      if (err) return cb(err);
      cb(null, {_id: doc.id});
    });
  });
};

Cassandra.prototype.remove = function remove (collection, conds, cb) {
  var self = this;
  this.getModel(collection, function (err, model) {
    if (err) return cb(err);
    conds = self.convertQuery(conds);
    model.remove(conds, cb);
  });
};

Cassandra.prototype.findOne = function findOne (collection, conds, cb) {
  if (intenseLogging) console.log('findOne', collection, conds);
  var args = Array.prototype.slice.call(arguments, 1);
  var self = this;
  this.getModel(collection, function (err, model) {
    if (err) return cb(err);
    //selector, fields, options, callback?
    conds = self.convertQuery(conds);
    var indexes = model.schema.getIndexes();
    var needMigrate = false;
    Object.keys(conds).forEach(function (i) {
      if (indexes.indexOf(i) === -1) {
        if (!model.schema.info.fields[i])
          model.schema.info.fields[i] = { name:i, index:true, type: 'Object' }
        else
          model.schema.info.fields[i].index = true;
        needMigrate = true;
      }
    });
    if (needMigrate) {
      model.automigrate(execute);
    } else
      execute(null);
    function execute(err) {
      if (err) return cb(err, null);
      var oldCb = cb;
      cb = function (err, item) {
        if (item) {
          item = self.unflatten(item.data);
          if (oldCb) oldCb(err, item);
        } else {
          if (oldCb) oldCb(err, null);
        }
      }
      //conds, order, offset, callback, consistency
      model.one(conds, cb);
    }
  });
};

Cassandra.prototype.find = function find (collection, conds, opts, cb) {
  if (intenseLogging) console.log('find', collection, conds);
  var self = this;
  this.getModel(collection, function (err, model) {
    if (err) return cb(err);
    if (conds.$solr) {
      if (!self.solr)
        return cb(new Error('Solr not enabled for searching'));
      if (conds.$solr.$reindex || (self.options.solr.reindex && !self.options.solr.reindex[collection])) {
        self.options.solr.reindex[collection] = true;
        model.find({where:{}}, function findCb (err, rows) {
          if (err) return cb(err);
          function addDoc() {
            var row = rows.splice(0, 1)[0];
            if (!row) {
              if (self.solrLogger) {
                self.solrLogger.log('COMMIT');
              }
              self.solr.commit(function (err) {
                if (err) return cb(err);
                solrFind();
              });
            } else {
              var item = self.unflatten(row.data);
              var o = self.formatIndexDoc(collection, item, item.id);
              if (self.solrLogger) {
                self.solrLogger.log('ADD', o);
              }
              self.solr.add(o, function (err) {
                if (err) return cb(err);
                addDoc();
              });
            }
          }
          addDoc();
        });
      } else
        solrFind();
      function solrFind() {
        delete conds.$solr.$reindex;
        var q = self.solr.createQuery().q(conds.$solr).start(opts.offset || 0).rows(opts.limit || 100);
        if (self.solrLogger) {
          self.solrLogger.log('SEARCH', q);
        }
        self.solr.search(q, function (err, docs) {
          if (err) {
            console.log(err);
            return cb(err);
          }
          if (docs) {
            var newConds = {id: {$all:[]}};
            docs.response.docs.forEach(function (doc) {
              newConds.id.$all.push(doc.id);
            });
            if (intenseLogging) console.log('solr docs', docs, newConds);
            finishFind(newConds);
          } else {
            return cb(null, []);
          }
        });
      }
    } else
      finishFind(conds);
    function finishFind(conds) {
      var filter = {
        where: self.convertQuery(conds),
        order: self.convertSort(opts.sort),
        limit: opts.limit,
        offset: opts.offset,
        allowFiltering: true
      };
      var indexes = model.schema.getIndexes();
      var needMigrate = false;
      Object.keys(filter.where).forEach(function (i) {
        if (indexes.indexOf(i) === -1) {
          if (!model.schema.info.fields[i])
            model.schema.info.fields[i] = { name:i, index:true, type: 'Object' }
          else
            model.schema.info.fields[i].index = true;
          needMigrate = true;
        }
      });
      if (needMigrate) {
        model.automigrate(execute);
      } else
        execute(null);
      function execute(err) {
        if (err) return cb(err, null);
      
        model.find(filter, function findCb (err, rows) {
          if (err) return cb(err);
          var docs = [];
          rows.forEach(function (row) {
            if (row.data) {
              var item = self.unflatten(row.data);
              item._id = row.data.id;
              delete item.id;
              docs.push(item);
            }
          });
          return cb(null, docs);
        });
      }
    }
  });
};

Cassandra.prototype.count = function count (collection, conds, opts, cb) {
  var self = this;
  this.getModel(collection, function (err, model) {
    if (err) return cb(err);
    conds = self.convertQuery(conds);
    model.count(conds, opts.limit, opts.offset, function findCb (err, count) {
      if (err) return cb(err);
      return cb(null, count);
    });
  });
};

Cassandra.prototype.setVersion = function setVersion (ver) {
  this.version = Math.max(this.version, ver);
};

Cassandra.prototype.setupRoutes = function setupRoutes (store) {
  var adapter = this;

  store.route('get', '*.*.*', -1000, function (collection, id, relPath, done, next) {
    var fields = { _id: 0 };
    if (relPath === 'id') relPath = '_id';
    fields[relPath] = 1;
    adapter.findOne(collection, {_id: id}, fields, function findOneCb (err, doc) {
      if (err) return done(err);
      if (!doc) return done(null, undefined, adapter.version);
      fixId(doc);
      var curr = doc;
      var parts = relPath.split('.');
      for (var i = 0, l = parts.length; i < l; i++) {
        curr = curr[parts[i]];
      }
      done(null, curr, adapter.version);
    });
  });

  store.route('get', '*.*', -1000, function (collection, id, done, next) {
    adapter.findOne(collection, {_id: id}, function findOneCb (err, doc) {
      if (err) return done(err);
      if (!doc) return done(null, undefined, adapter.version);
      fixId(doc);
      done(null, doc, adapter.version);
    });
  });

  store.route('get', '*', -1000, function (collection, done, next) {
    adapter.find(collection, {}, {}, function findCb (err, docList) {
      if (err) return done(err);
      var docs = {}, doc;
      for (var i = docList.length; i--; ) {
        doc = docList[i];
        fixId(doc);
        docs[doc.id] = doc;
      }
      return done(null, docs, adapter.version);
    });
  });

  function createCb (ver, done) {
    return function findAndModifyCb (err, origDoc) {
      if (err) return done(err);
      adapter.setVersion(ver);
      if (origDoc) fixId(origDoc);
      done(null, origDoc);
    };
  }

  function setCb (collection, id, relPath, val, ver, done, next) {
    if (intenseLogging) console.log('setCb', id, relPath, val, ver);
    if (relPath === 'id') relPath = '_id';
    adapter.findOne(collection, {_id:id}, function (err, oldDoc) {
      var parts = relPath.split('.');
      var data;
      if (oldDoc) {
        curr = oldDoc;
        for (var i = 0, l = parts.length; i < l; i++) {
          if (i === (l - 1))
            curr[parts[i]] = val;
          else {
            if (!curr[parts[i]]) {
              if (/[0-9]+/.test(parts[i])) {
                curr[parts[i]] = [];
              } else {
                curr[parts[i]] = {};
              }
            }
            curr = curr[parts[i]];
          }
        }
        data = oldDoc;
      } else
        data = {};
      data = adapter.cleanupId(data, true);
      adapter.rawUpdate(collection, {_id:id}, [], data, {upsert:true}, next);
    });
  }
  store.route('set', '*.*.*', -1000, setCb);

  store.route('set', '*.*', -1000, function (collection, id, doc, ver, done, next) {
    if (intenseLogging) console.log('set', collection, id, doc);
    var findAndModifyCb = createCb(ver, done);

    if (!id) {
      return adapter.insert(collection, doc, cb);
    }

    // Don't use `delete doc.id` so we avoid side-effects in tests
    var parts = id.split('.');
    var docCopy;
    if (typeof doc !== 'string' && ((typeof doc === 'object') || Array.isArray(doc))) {
      docCopy = {};
      for (var k in doc) {
        if (k === 'id') docCopy._id = id
        else docCopy[k] = doc[k];
      }
    } else
      docCopy = doc;
    if (parts.length > 1) {
      id = parts.splice(0, 1)[0];
      var realDoc = {};
      var curr = realDoc;
      for (var i = 0, l = parts.length; i < l; i++) {
        if (i === (l - 1))
          curr[parts[i]] = docCopy;
        else {
          if (!curr[parts[i]]) {
            if (/[0-9]+/.test(parts[i])) {
              curr[parts[i]] = [];
            } else {
              curr[parts[i]] = {};
            }
          }
          curr = curr[parts[i]];
        }
      }
      docCopy = realDoc;
    }
    adapter.findAndModify(collection, {_id: id}, [], docCopy, {upsert: true}, createCb(ver, done));
  });

  store.route('del', '*.*.*', -1000, function delCb (collection, id, relPath, ver, done, next) {
    if (relPath === 'id') {
      throw new Error('Cannot delete an id');
    }

    var unsetConf = {};
    unsetConf[relPath] = 1;
    var op = { $unset: unsetConf };
    var findAndModifyCb = createCb(ver, done);
    adapter.findAndModify(collection, {_id: id}, [], op, findAndModifyCb);
  });

  store.route('del', '*.*', -1000, function delCb (collection, id, ver, done, next) {
    adapter.findAndModify(collection, {_id: id}, [], {}, {remove: true}, createCb(ver, done));
  });

  function createPushPopFindAndModifyCb (ver, done) {
    return function findAndModifyCb (err, origDoc) {
      if (err) {
        if (/non-array/.test(err.message)) {
          err = new Error('Not an Array');
        }
        if (err) return done(err);
      }
      createCb(ver, done)(err, origDoc);
    }
  }

  // pushCb(collection, id, relPath, vals..., ver, done, next);
  store.route('push', '*.*.*', -1000, function pushCb (collection, id, relPath) {
    var arglen = arguments.length;
    var vals = Array.prototype.slice.call(arguments, 3, arglen-3);
    var ver  = arguments[arglen-3]
    var done = arguments[arglen-2];
    var next = arguments[arglen-1];
    var op = {};
    if (vals.length === 1) (op.$push = {})[relPath] = vals[0];
    else (op.$pushAll = {})[relPath] = vals;

    adapter.findAndModify(collection, {_id: id}, [], op, {upsert: true}, createPushPopFindAndModifyCb(ver, done));
  });

  store.route('pop', '*.*.*', -1000, function popCb (collection, id, relPath, ver, done, next) {
    var popConf = {};
    popConf[relPath] = 1;
    var op = { $pop: popConf };
    adapter.findAndModify(collection, {_id: id}, [], op, {upsert: true}, createPushPopFindAndModifyCb(ver, done));
  });

  function createFindOneCb (collection, id, relPath, ver, done, extra, genNewArray) {
    if (arguments.length === createFindOneCb.length-1) {
      genNewArray = extra;
      extra = null;
    }
    return function cb (err, found) {
      if (err) return done(err);
//      if (!found) {
//        return done(null);
//      }
      var arr = found && found[relPath];
      if (!arr) arr = [];
      if (! Array.isArray(arr)) {
        return done(new Error('Not an Array'));
      }

      arr = genNewArray(arr, extra);

      var setTo = {};
      setTo[relPath] = arr;

      var op = { $set: setTo };

      adapter.findAndModify(collection, {_id: id}, [], op, {upsert: true}, createCb(ver, done));
    };
  }

  // unshiftCb(collection, id, relPath, vals..., ver, done, next);
  store.route('unshift', '*.*.*', -1000, function unshiftCb (collection, id, relPath) {
    var arglen = arguments.length;
    var vals = Array.prototype.slice.call(arguments, 3, arglen-3);
    var ver = arguments[arglen-3];
    var done = arguments[arglen-2];
    var next = arguments[arglen-1];

    var fields = {_id: 0};
    fields[relPath] = 1;
    var cb = createFindOneCb(collection, id, relPath, ver, done, {vals: vals}, function (arr, extra) {
      return extra.vals.concat(arr.slice());
    });
    adapter.findOne(collection, {_id: id}, fields, cb);
  });

  store.route('insert', '*.*.*', -1000, function insertCb (collection, id, relPath, index) {
    var arglen = arguments.length;
    var vals = Array.prototype.slice.call(arguments, 4, arglen-3);
    var ver = arguments[arglen-3];
    var done = arguments[arglen-2];
    var next = arguments[arglen-1];

    var fields = {_id: 0};
    fields[relPath] = 1;
    var cb = createFindOneCb(collection, id, relPath, ver, done, {vals: vals, index: index}, function (arr, extra) {
      var index = extra.index
        , vals = extra.vals;
      return arr.slice(0, index).concat(vals).concat(arr.slice(index));
    });
    adapter.findOne(collection, {_id: id}, fields, cb);
  });

  store.route('shift', '*.*.*', -1000, function shiftCb (collection, id, relPath, ver, done, next) {
    var fields = { _id: 0 };
    fields[relPath] = 1;
    adapter.findOne(collection, {_id: id}, fields, createFindOneCb(collection, id, relPath, ver, done, function (arr) {
      var copy = arr.slice();
      copy.shift();
      return copy;
    }));
  });

  store.route('remove', '*.*.*', -1000, function removeCb (collection, id, relPath, index, count, ver, done, next) {
    var fields = { _id: 0 };
    fields[relPath] = 1;
    adapter.findOne(collection, {_id: id}, fields
      , createFindOneCb(collection, id, relPath, ver, done, {index: index, count: count}, function (arr, extra) {
          var copy = arr.slice();
          var index = extra.index;
          var count = extra.count;
          copy.splice(index, count);
          return copy;
        })
    );
  });

  store.route('move', '*.*.*', -1000, function moveCb (collection, id, relPath, from, to, count, ver, done, next) {
    var fields = { _id: 0 };
    fields[relPath] = 1;
    adapter.findOne(collection, {_id: id}, fields
      , createFindOneCb(collection, id, relPath, ver, done, {to: to, from: from, count: count}, function (arr, extra) {
          var copy = arr.slice();
          var to = extra.to
            , from = extra.from
            , count = extra.count;
          if (to < 0) to += copy.length;
          var values = arr.splice(from, count);
          var args = [to, 0].concat(values);
          arr.splice.apply(arr, args);
          return arr;
        })
    );
  });
};
