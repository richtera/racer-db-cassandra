var EventEmitter = require('events').EventEmitter
  , url = require('url')
  , cassandra = require('cassandra-orm')
  , createQueryCtor = require('./query')
  , util = require('./util')
  , fixId = util.fixId
  , lookup

  , DISCONNECTED  = 1
  , CONNECTING    = 2
  , CONNECTED     = 3
  , DISCONNECTING = 4;

exports = module.exports = plugin;

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

  var login = {host:info.hostname, port:info.port || 9160, keyspace:keyspace, user:options.user, pass:options.password};
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
    model = this.models[collection] = cassandra.define(collection, { fields: { }, primaryKeys: ['id'] });
    model.useConnection(self.driver);
    return model.prepare(function (err) {
      callback(err, model);
    });
  }
  callback(null, model);
}

function _flatten (out, prefix, val) {
  if (Array.isArray(val)) {
    for (var j in val) {
      var name = prefix + '__' + j;
      var v = val[j];
      _flatten(out, name, v);
    }
  } else if (typeof val === 'object') {
    for (var j in val) {
      if (j === '$set') {
        _flatten (out, prefix, val[j]);
        continue;
      }
      var name = prefix + '_' + j;
      var v = val[j];
      _flatten(out, name, v);
    }
  } else
    out[prefix] = val;
}

function flatten(val) {
  var obj = {};
  _flatten(obj, 'c', val);
  return obj;
}

function unflatten(val) {
  var out = undefined;
  var type = 0;
  for (var i in val) {
    var v = val[i];
    var ptr = null;
    var ind = null;
    var items = i.split('_');
    var index = 1;
    while (index < items.length) {
      var j = items[index++];
      if (j === '') {
        j = items[index++];
        type = 1;
      }
      if (index === 2) {
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
  return out;
}

Cassandra.prototype.cleanupId = function (doc, doFlatten) {
  var out = {};
  for (var i in doc) {
    if (i === '_id') {
      if (!doFlatten)
        out['id'] = doc[i];
    } else {
      var j = i;
      if (!doFlatten) {
        j = i.replace(/\./g, '_');
        console.log('rename', i, j);
      }
      out[j] = doc[i];
    }
  }
  console.log(out);
  if (doFlatten)
    return flatten(out);
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
  var args = Array.prototype.slice.call(arguments, 1);
  var self = this;
  this.getModel(collection, function (err, model) {
    if (err) return callback(err);
    console.log("findAndModify", collection, conds, sort, doc, options);
    conds = self.cleanupId(conds);
    doc = self.cleanupId(doc, true);
    console.log('update conds', conds);
    console.log('update doc', doc);
    model.update(doc, conds, function (err, info) {
      console.log('update', err, info);
      doc._id = doc.id;
      delete doc.id;
      callback(err, doc);
    }, options);
  });
}

Cassandra.prototype.insert = function insert (collection, json, opts, cb) {
  // TODO Leverage pkey flag; it may not be _id
  var toInsert = Object.create(json);
  var self = this;
  this.getModel(collection, function (err, model) {
    if (err) return cb(err);
    conds = self.cleanupId(conds);
    if (toInsert._id) {
      json = self.cleanupId(toInsert, true);
      return model.update(json, {where:{id:toInsert._id}}, function (err, info) {
        console.log('insert(as update)', err, info);
        if (callback) callback(err, info);
      });
    }
    json = self.cleanupId(toInsert, true);
    model.insert(json, opts, function insertCb (err) {
      console.log('insert', err);
      if (err) return cb(err);
      cb(null, {_id: toInsert._id});
    });
  });
};

Cassandra.prototype.remove = function remove (collection, conds, cb) {
  var self = this;
  this.getModel(collection, function (err, model) {
    if (err) return cb(err);
    conds = self.cleanupId(conds);
    model.remove(conds, cb);
  });
};

Cassandra.prototype.findOne = function findOne (collection, conds, cb) {
  var args = Array.prototype.slice.call(arguments, 1);
  var self = this;
  this.getModel(collection, function (err, model) {
    if (err) return cb(err);
    //selector, fields, options, callback?
    conds = self.cleanupId(conds);
    var oldCb = cb;
    cb = function (err, item) {
      item = unflatten(item);
      console.log('findone', err, item);
      if (oldCb) oldCb(err, item);
    }
    //conds, order, offset, callback, consistency
    model.one(conds, cb);
  });
};

Cassandra.prototype.find = function find (collection, conds, opts, cb) {
  var self = this;
  this.getModel(collection, function (err, model) {
    if (err) return cb(err);
    var filter = {
      where: self.cleanupId(conds),
      order: self.convertSort(opts.sort),
      limit: opts.limit,
      offset: opts.offset
    };
    console.log('finding');
    model.find(filter, function findCb (err, rows) {
      console.log('find', err, rows);
      if (err) return cb(err);
      var docs = [];
      rows.forEach(function (row) {
        if (row.data) {
          var item = unflatten(row.data);
          item._id = row.data.id;
          delete item.id;
          docs.push(item);
        }
      });
      return cb(null, docs);
    });
  });
};

Cassandra.prototype.count = function count (collection, conds, opts, cb) {
  var self = this;
  this.getModel(collection, function (err, model) {
    if (err) return cb(err);
    conds = self.cleanupId(conds);
    model.count(self.cleanupId(conds), opts.limit, opts.offset, function findCb (err, count) {
      console.log('count', err, count);
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
    if (relPath === 'id') relPath = '_id';
    var setTo = {};
    setTo[relPath] = val;
    var op = { $set: setTo };

    // Patch-up implicit creation of arrays in a path, since Mongo
    // will create an object if not already an array
    var cb = /\.[0-9]+(?=\.|$)/.test(relPath) ?
      function (err, origDoc) {
        if (err) return done(err);
        var re = /\.[0-9]+(?=\.|$)/g
          , root;
        while (match = re.exec(relPath)) {
          root = relPath.slice(0, match.index);
          if (lookup(root, origDoc) == null) {
            setCb(collection, id, root, [], ver, function() {
              setCb(collection, id, relPath, val, ver, done, next);
            });
            return;
          }
        }
        createCb(ver, done)(err, origDoc);
      } : createCb(ver, done);

    adapter.findAndModify(collection
      , {_id: id}     // Conditions
      , []             // Empty sort
      , op             // Modification
      , {upsert: true} // If not found, insert the object represented by op
      , cb
    );
  }
  store.route('set', '*.*.*', -1000, setCb);

  store.route('set', '*.*', -1000, function (collection, id, doc, ver, done, next) {
    var findAndModifyCb = createCb(ver, done);

    if (!id) {
      return adapter.insert(collection, doc, cb);
    }

    // Don't use `delete doc.id` so we avoid side-effects in tests
    var docCopy = {};
    for (var k in doc) {
      if (k === 'id') docCopy._id = id
      else docCopy[k] = doc[k];
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
