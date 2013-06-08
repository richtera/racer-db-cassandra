var EventEmitter = require('events').EventEmitter
  , url = require('url')
  , cassandra = require('cassandra-orm')
  , createQueryCtor = require('./query')
  , util = require('./util')
  , fixId = util.fixId
  , solr = require('solr-client')
  , solrConfig = require('./solrConfig')
  , async = require('async')
  , lookup
  , zk = null
  , mime = require('mime')

  , DISCONNECTED  = 1
  , CONNECTING    = 2
  , CONNECTED     = 3
  , DISCONNECTING = 4;

exports = module.exports = plugin;

exports.ConsistencyLevel = cassandra.ConsistencyLevel;

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
  this.gettingModel = {};
  exports.rootStore = this;
  
  if (this.options.solr) {
    this.options.solr.enableDse = this.options.enableDse;
    this.options.shardCount = (this.options.solr.shardArgs && this.options.solr.shardArgs.numShards) || 1;

    this.solr = solr.createClient(this.options.solr);
    if (this.options.solr.auth) {
      var u = this.options.solr.auth.split(':');
      this.solr.basicAuth(u[0], u[1]);
    }
  }

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
    , uri = options.uri || "cassandra://" + options.host + ':' + options.port + '/' + options.database + '?' +
        'auto_reconnect=' + ('auto_reconnect' in options ? options.auto_reconnect : true)
    , info
    , keyspace;
    
    var uris = uri.split(',');
    if (uris.length === 1) {
      info = url.parse(uri);
      keyspace = (options.keyspace || (info.pathname && info.pathname.substring(1)) || "Keyspace1").toLowerCase()
    } else {
      info = {hosts: []};
      uris.forEach(function (u) {
        var i = url.parse(u);
        info.hosts.push(i.hostname + ":" + (i.port || 9160));
        keyspace = keyspace || i.pathname && i.pathname.substring(1);
        info.auth = info.auth || i.auth;
        info.query = info.query || i.query;
      });
      keyspace = (keyspace || "Keyspace1").toLowerCase();
    }

  // Allowed values of [true | false | {j:true} | {w:n, wtimeout:n} | {fsync:true}]
  // The default value is false which means the driver receives does not
  // return the information of the success/error of the insert/update/remove
  if (!('safe' in options)) options.safe = false;
  if (info.auth) {
    var i = info.auth.split(':');
    options.user = i[0];
    options.password = i[1];
  }
  if (info.query) {
    var q = info.query.split('&');
    q.forEach(function (item) {
      var ii = item.split('=');
      if (ii.length > 1) {
        options[ii[0]] = ii[1];
      }
    });
  }
  var login = {
    hosts: info.hosts,
    host:info.hostname,
    logger: options.logger || this.logger,
    cqlVersion : '3.0.0',
    enableDse: options.enableDse,
    port:info.port || 9160,
    keyspace:keyspace,
    user:options.user,
    password:options.password,
    timeout:options.timeout,
    consistencylevel:options.consistencylevel || 1,
    strategy:options.strategy,
    keyspaceOptions:options.keyspaceOptions && (typeof options.keyspaceOptions === 'object' ? options.keyspaceOptions : JSON.parse(options.keyspaceOptions))
  };
  this.keyspace = keyspace;
  this.models = {};
  if (callback) this.once('open', callback);
  var self = this;
  this.coreInited = {};
  this.coreIniting = {};
  if (this.solr) {
    this.schemaSync = cassandra.schemaSync = function (model, callback) {
      var table = model.schema.info.tableName;
      var timeout;
      if (!self.coreIniting[table]) self.coreIniting[table] = [];
      if (self.coreIniting[table].length == 0) {
        self.coreIniting[table].push(callback);
      } else {
        self.coreIniting[table].push(callback);
        return;
      }
      callback = function (err) {
        while (true) {
          var cb = self.coreIniting[table].splice(0, 1)[0];
          if (cb)
            cb(err);
          else
            break;
        }
      }
      model.schema.enableDse = options.enableDse;
      var schema = solrConfig.solrSchema(model.schema);
      if (schema) {
        var config = solrConfig.solrConfig(model.schema);
        var synonyms = solrConfig.synonyms(model.schema);
        var stopwords = solrConfig.stopwords(model.schema);
        var core = model.connection.keyspace.name + '.' + table;
        var prefix = options.enableDse ? '/resource/' : '/configs/';
        var items = [
          { path: prefix + core + '/synonyms.txt',   contentType: 'text/plain',  content: synonyms,  info: 'synonyms.txt' },
          { path: prefix + core + '/stopwords.txt',  contentType: 'text/plain',  content: stopwords, info: 'stopwords.txt' },
          { path: prefix + core + '/solrconfig.xml', contentType: 'text/xml',    content: config,    info: 'solrconfig.xml' },
          { path: prefix + core + '/schema.xml',     contentType: 'text/xml',    content: schema,    info: 'Schema.xml' }
        ];
        if (options.enableDse) {
          self.solr.sendToSolr({ path: '/resource/' + core + '/schema.xml', method: 'GET'}, function (err, result) {
            var a = schema.replace(/\n|\r/g, '');
            var b = (result || '').replace(/\n|\r/g, '');
            if (a === b) {
              self.coreInited[table] = true;
              if (self.solrLogger) self.solrLogger.log('SCHEMA match ' + core);
              return callback(null);
            }
            if (self.solrLogger)
              self.solrLogger.log('CREATING/RELOADING solr core ' + core);
            function sendItem(err) {
              if (err) {
                self.coreInited[table] = true;
                return callback(err);
              }
              var item = items.splice(0, 1)[0];
              if (item) {
                return self.solr.sendToSolr(item, function (err, result) {
                  if (result)
                    result = '\n' + result;
                  else
                    result = '';
                  if (err) {
                    if (self.solrLogger)
                      self.solrLogger.log('SENDING failed ' + item.info + ' => ' + err.message + result);
                  }
                  sendItem(err);
                });
              } else {
                if (err) {
                  self.coreInited[table] = true;
                  return callback(err);
                }
                solrCreate();
              }
            }
            sendItem(null);
          });
        } else {
          if (!zk) {
            var ZK = require('zkjs');
            zk = new ZK({
              hosts: options.solr.zookeepers,
              root: '/'
            });
            if (self.solrLogger)
              self.solrLogger.log('Starting Zookeeper Session.');
            zk.start(function (err) {
              if (err) {
                if (self.solrLogger)
                  self.solrLogger.log('Unable to connect to zookeeper');
                zk = null;
                self.coreInited[table] = true;
                return callback(err);
              }
              checkZookeeperConfig();
            });
            zk.on('expired', function () {
              if (self.solrLogger)
                self.solrLogger.log('Lost Zookeeper Session.');
              zk.start();
            });
          } else
            checkZookeeperConfig();
          function checkZookeeperConfig() {
            zk.get(
              '/configs/' + core + '/schema.xml',
              function (err, value, zstat) {
                if (err) {
                  return createZookeeperConfigDir();
                }
                if (value) {
                  value = value.toString();
                  if (value === schema) {
                    if (!self.aliveCores) {
                      self.solr.sendToSolr({
                        path: '/admin/cores?action=STATUS&wt=json'
                      }, function (err, result) {
                        result = JSON.parse(result);
                        var exp = new RegExp('^' + self.keyspace + '\\..*');
                        self.aliveCores = {};
                        Object.keys(result.status).filter(function (core) {
                          if (Object.keys(result.status[core]).length == 0)
                            return false; // This core is not up there are no properties
                          return exp.test(core);
                        }).map(function (core) {
                          return core.replace(/_shard[0-9]*_replica[0-9]*/, '');
                        }).forEach(function (core) {
                          self.aliveCores[core] = true;
                        });
                        isCoreAlive();
                      });
                    } else
                      isCoreAlive();
                    function isCoreAlive() {
                      if (!self.aliveCores[core]) {
                        if (self.solrLogger)
                          self.solrLogger.log('SCHEMA matches but core doesn\'t exist ' + core);
                        return createZookeeperConfigDir();
                      }
                      if (self.solrLogger)
                        self.solrLogger.log('SCHEMA matches ' + core);
                      self.coreInited[table] = true;
                      return callback(null);
                    }
                  } else
                    createZookeeperConfigDir();
                }
              }
            );
          }
          function createZookeeperConfigDir() {
            zk.get('/configs/' + core, function (err, value, zstat) {
              if (err)
                return zk.create('/configs/' + core, '', zk.create.NONE, writeZookeeperConfig);
              writeZookeeperConfig();
            });
          }
          function writeZookeeperConfig(err, path) {
            if (err) {
              if (self.solrLogger)
                self.solrLogger.log('Unable to upload file to zookeeper ' + err + (path && (': ' + path)));
              self.coreInited[table] = true;
              return callback(err);
            }
            var item = items.splice(0, 1)[0];
            if (item) {
//              if (self.solrLogger)
//                self.solrLogger.log('Checking ' + item.path);
              return zk.get(item.path, function (err, value, zstat) {
                if (err) {
                  if (self.solrLogger)
                    self.solrLogger.log('Creating ' + item.path);
                  return zk.create(item.path, item.content, zk.create.NONE, writeZookeeperConfig);
                }
//                if (self.solrLogger)
//                  self.solrLogger.log('Updating ' + item.path);
                zk.set(item.path, item.content, zstat.version, function (err, zstat) {
                  if (self.solrLogger)
                    self.solrLogger.log('Updated ' + item.path + ' ' + zstat.version);
                  writeZookeeperConfig(err, item.path);
                });
              });
            }
            solrCreate();
          }
        }
        var createError = null;
        function solrCreate() {
          if (self.solrLogger)
            self.solrLogger.log('Sending RELOAD ' + core);
          var shardArgs = [];
          Object.keys(options.solr.shardArgs || {}).forEach(function (s) {
            shardArgs.push(s + '=' + encodeURIComponent(options.solr.shardArgs[s]));
          });
          if (shardArgs.length) {
            shardArgs = '&' + shardArgs.join('&');
          }
          var p = '/admin/' + (options.enableDse ? 'cores' : 'collections') + '?action=RELOAD&wt=json&name=' + core + shardArgs;
          if (intenseLogging) console.log('Executing ' + p);
          self.solr.sendToSolr({
            path: p
          }, function (err, result) {
            if (!err && !/Could not find collection/.test(result)) {
              if (self.solrLogger)
                self.solrLogger.log('RELOAD ' + core + ' Succeeded');
              self.coreInited[table] = true;
              return callback(err);
            }
            if (self.solrLogger) {
              if (result)
                result = '\n' + result;
              else
                result = '';
              createError = ((err && err.message) || '') + result;
            }
            var p = '/admin/' + (options.enableDse ? 'cores' : 'collections') + '?wt=json&action=CREATE&name=' + core + shardArgs;
            if (self.solrLogger)
              self.solrLogger.log('Sending CREATE ' + core);
            if (intenseLogging) console.log('Executing ' + p);
            self.solr.sendToSolr({
              path: p
            }, function (err, result) {
              if (err) {
                if (self.solrLogger) {
                  if (result)
                    result = '\n' + result + '\n';
                  else
                    result = '\n';
                  self.solrLogger.log('CREATE and RELOAD failed ' + core + ' => ' + err.message + result + createError);
                }
              } else if (self.aliveCores)
                self.aliveCores[core] = true;
              self.coreInited[table] = true;
              callback(err);
            });
          });
        }
      } else {
        self.coreInited[table] = true;
        callback(null);
      }
    };
  }
  cassandra.connect(login, function (err, driver) {
    if (err) console.log(err);
    self.driver = driver;
    self.emit('open', err);
  });
}

Cassandra.prototype.formatIndexDoc = function (collection, doc, id, model) {
  var o = {};
  for (var i in doc) {
    // No property with "." id or _id should be sent to solr for now.
    if (/\.|^_id$|^id$/.test(i))
      continue;
    var v = doc[i];
    var field = model && model.schema && model.schema.info && model.schema.info.fields && model.schema.info.fields[i];
    if (field && !field.solrIndex) {
//      console.log('excluded', i, field);
      if (intenseLogging) console.log('excluding ' + i + ' from solr index for ' + model.schema.info.tableName);
      continue;
    }
    var type = field && field.type && field.type.name;
    if ((type === 'blob' && field.solr_extract) || (i === 'doc_filecontent')) {
      if (!o._extract) {
        o._extract = {};
      }
      o._extract[i] = true;
      continue;
    }
    if (type === 'timestamp') {
      v = new Date(Date.parse(v));
    }
    if (this.options.enableDse) {
      if (/^_.*|.*\..*/.test(i))
        continue;
    }
    o[i] = v;
  }
  o._core = collection;
  o.id = id;
  if (model.schema.info.primaryKeys.length != 1) {
    var q = {id:id};
    q = this.convertQuery(model, q);
    Object.keys(q).forEach(function (e) {
      o[e] = q[e];
    });
  }
  return o;
}

Cassandra.prototype.indexDoc = function indexDoc(collection, doc, id, where, model, callback) {
//  if (this.options.enableDse)
//    return;
  var autoCommit = false;
  if (!callback)
    autoCommit = true;
  if (intenseLogging) console.log('indexDoc', where, collection, id, doc);
  if (this.solr) {
    function scheduleSolrCommit() {
      if (self.commitTimer) clearTimeout(self.commitTimer);
      self.commitTimer = setTimeout(function () {
        if (this.solrLogger) {
          this.solrLogger.log('COMMIT ' + collection);
        }
        self.solr.commit({core:self.keyspace + '.' + collection,waitSearcher:true});
      }, 1000);
    }
    var self = this;
    if (!this.coreInited[collection]) {
      this.schemaSync(model, function (err) {
        if (err) return callback && callback(err);
        sendDocument();
      });
    } else
      sendDocument();
    function sendDocument() {
      if (doc) {
        var o = self.formatIndexDoc(collection, doc, id, model);
        var options = {};
        options.autocommit = true;
        options.core = self.keyspace + '.' + collection;
        if (!o._extract) {
          if (self.logger) {
            self.solrLogger.log('ADD ' + collection, o);
          }
          options.update = 'update';
          options.autocommit = true;
        } else {
          // turn o into a MIME doc
          // concat params from object
          var oo = {};
          var params = Object.keys(o).filter(function (e) {
            if (o._extract[e]) return false;
            var t = /^([^_]*)_(mimetype|filename)/.exec(e);
            if (t && t[1] && o._extract[t[1]]) return false;
            return true;
          }).map(function (e) {
            oo[e] = o[e];
            if (o[e] instanceof Date)
              return encodeURIComponent('literal.' + e) + '=' + encodeURIComponent(((date && !isNaN(date.getTime())) ? date.toISOString() : 'null'));
            return encodeURIComponent('literal.' + e) + '=' + encodeURIComponent(o[e]);
          }).join('&'); 
          var destField = '_body';
          // go through o._extract and make a MIME enclosure for each
          var p = [];
          var sep = 'WebKitFormBoundaryebFz3Q3NHxk7g4qYkalsahlkh';
          Object.keys(o._extract).forEach(function (e) {
            var f = o[e + '_filename'] || 'file';
            var m = mime.lookup(f) || 'application/octet-stream';
            if (e === 'doc_filecontent') {
              f = o['doc_filename'];
              m = o['doc_filetype'];
            }
            var b = new Buffer(doc[e], 'base64');
            p.push(new Buffer(
              '------' + sep + "\r\n" +
              'Content-Disposition: form-data; name="' + e + '"; filename="' + f + '"' + "\r\n" +
              'Content-Type: ' + m + "\r\n" +
              'Content-Length: ' + b.length + "\r\n" +
              '\r\n'));
            p.push(b);
            p.push(new Buffer('\r\n'));
          });
          p.push(new Buffer('------' + sep + '--'));
          try {
            p = Buffer.concat(p);
          }
          catch (e) {
            console.log(e);
          }
          options.update = 'update/extract?commit=' + autoCommit + '&fmap.content=' + destField + '&' + params;
          options.extract = true;
          options.extraHeaders = {
            "content-type": "multipart/form-data; boundary=----" + sep,
            "content-length": p.length
          };
          if (self.logger) {
            oo._extract = o._extract;
            self.solrLogger.log('ADD EXTRACT ' + collection, oo);
          }
          delete o._extract;
          o = p;
        }
        self.solr.add(o, options, function(err, obj) {
          if (err) {
            if (self.solrLogger) {
              self.solrLogger.log('ADD FAILED ' + collection, err.json || err.message)
            }
          }
          return callback && callback(err, obj);
          //if (!self.options.enableDse)
          //  scheduleSolrCommit();
        });
      } else if (!self.options.enableDse) {
        if (self.solrLogger) {
          self.solrLogger.log('DELETE ' + collection, {id:id});
        }
        var data = {delete:{id:id}};
        var options = {core:self.keyspace + '.' + collection};
        self.solr.update(data, options, function (err, obj) {
          if (err) {
            console.log(err, obj);
          }
          //scheduleSolrCommit();
          return callback && callback(err, obj);
        });
      }
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
  var enableDse = this.options.enableDse;
  var model = this.models[collection];
  var self = this;
  if (!model) {
    var timeout;
    if (!this.gettingModel[collection]) this.gettingModel[collection] = [];
    if (this.gettingModel[collection].length == 0) {
      this.gettingModel[collection].push(callback);
      getTheModel();
    } else
      this.gettingModel[collection].push(callback);    
    function getTheModel() {
      if (self.getModelFields) {
        self.getModelFields(collection, done);
      } else {
        done(null, {});
      }
      function done(err, fields) {
        if (err) {
          return callback(err);
        }
        var keys = [];
        Object.keys(fields).forEach(function (e) {
          var key = fields[e];
          key.name = e;
          if (key.primary_key)
            keys.push(key);
        });
        if (keys.length === 0)
          keys.push('id');
        var partitionKeys = 0;
        keys.forEach(function (k) {
          if (k.primary_key < 10)
            partitionKeys++;
        });
        if ((keys.length == partitionKeys) && (keys.length !== 1))
          partitionKeys--;
        else if (partitionKeys < 1)
          partitionKeys = 1;
        var keys = keys.sort(function (a, b) {
          return a.primary_key - b.primary_key;
        }).map(function (e) {
          return e.name;
        });
        model = self.models[collection] = cassandra.define(collection, { fields: fields, primaryKeys: keys, partitionKeys: partitionKeys }, null, true);
        model.useConnection(self.driver);
        return model.prepare(function (err) {
          model.schema.allowFiltering = !enableDse;
          while (true) {
            var cb = self.gettingModel[collection].splice(0, 1)[0];
            if (cb)
              cb(err, model);
            else
              break;
          }
        });
      }
    }
    return;
  }
  if (!model.connection) {
		return model.on('ready', function (connection) {
      model.connection = connection;
      callback(null, model);
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
    if (val && (val.constructor === Date)) {
      out[prefix] = val;
      return;
    }
    for (var j in val) {
      if (isRoot) {
        if (/^_.*/.test(j))
          continue;
        if (j === '_id') {
          val['id'] = val[j];
          delete val[j];
          j = 'id';
        }
      }
      if (j === '$solr' || j === 'solr_query') {
        continue;
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

Cassandra.prototype.flatten = function flatten(model, val) {
  var obj = {};
  _flatten(obj, null, val, true);
  if ((model.schema.info.primaryKeys.length !== 1) && obj.id && (obj.id[0] === ':')) {
    //console.log(model.schema.info.primaryKeys, obj.id);
    try {
      var ids = JSON.parse(new Buffer(obj.id.substring(1), 'base64'));
      var index = 0;
      model.schema.info.primaryKeys.forEach(function (name) {
        obj[name] = ids[index++];
      });
    }
    catch (e) {
      console.log(e, model.schema.info.tableName, obj.id);
    }
  }
  return obj;
}

Cassandra.prototype.unflatten = function unflatten(model, val) {
  try {
    if (typeof val === 'string')
      return val;
    var out = undefined;
    var type = 0;
    for (var i in val) {
      if (/^_.*/.test(i))
        continue;
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
  if (out) {
    var id = [];
    model.schema.info.primaryKeys.forEach(function (name) {
      try {
        id.push(out[name]);
      }
      catch (e) {
        console.log(e, name, out);
      }
    });
    if (id.length == 1)
      out.id = id[0];
    else
      out.id = ':' + new Buffer(JSON.stringify(id)).toString('base64');
  }
  return out;
}

Cassandra.prototype.convertQuery = function (model, doc) {
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
  var doc = this.flatten(model, doc);
  if (model.schema.info.primaryKeys.length > 1)
    delete doc.id;
  return doc;
}

Cassandra.prototype.cleanupId = function (model, doc, doFlatten) {
  if (typeof doc === 'string')
    return doc;
  var out = {};
  for (var i in doc) {
    if (i === '_id') {
      if (!doFlatten)
        out['id'] = doc[i];
      delete out[i];
    } else if (/^id$|^solr_query$|^$solr$/.test(i) && doFlatten) {
      delete out[i];
    } else {
      var j = i;
      if (!doFlatten) {
        j = i.replace(/\./g, '_');
      }
      out[j] = doc[i];
    }
  }
  if (doFlatten)
    return this.flatten(model, out);
  return out;
}

Cassandra.prototype.convertSort = function (sort, isSolr) {
  if (sort) {
//    if (!isSolr)
//      console.log("warning: Order by not supported without partition keys");
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
    conds.id = conds._id || conds.id;
    delete conds._id;
    id = conds.id && conds.id.split('.');
    conds = self.convertQuery(model, conds);
    doc = self.cleanupId(model, doc, true);
    if (id) {
      if (id.length > 1) {
        conds.id = id.splice(0, 1)[0];
        id = id.join('.') + '.';
        for (var i in doc) {
          doc[id + i] = doc[i];
          delete doc[i];
        }
      } else if (id.length == 1) {
        id = id[0];
      }
    }
    model.one(conds, function (err, oldDoc) {
      var same;
      if (!options.remove) {
        if (oldDoc) {
          Object.keys(oldDoc.data).forEach(function (name) {
            if (oldDoc[name] && !doc[name])
              doc[name] = null;
          });
        } else
          oldDoc = {};
        same = true;
        try {
          Object.keys(doc || {}).forEach(function (name) {
            if (oldDoc[name] !== doc[name])
              same = false;
          });
        }
        catch (e) {
          console.log(e, doc);
        }
      } else {
        if (!oldDoc) {
          return callback(new Error('record not found'));
        }
        same = false;
      }
      //console.log(oldDoc, doc);
      if (!same) {
        var needMigrate = false;
        if (!options.remove) {
          for (var i in doc) {
            if (i === 'solr_query' || /^_.*/.test(i))
              continue;
            if (!model.schema.info.fields[i] && !/^_.*/.test(i)) {
              model.schema.info.fields[i] = {name:i, type:'Object'};
              needMigrate = true;
            }
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
              self.indexDoc(collection, null, id, 'findAndModifyRemove', model);
            });
          } else {
            doc.id = id;
            model.update(doc, conds, function (err, info) {
              var o = self.flatten(model, oldDoc.data || {});
              doc._id = doc.id || id;
              delete doc.id;
              Object.keys(doc).forEach(function (n) {
                o[n] = doc[n];
              });
              self.indexDoc(collection, o, id, 'findAndModify', model);
              callback(err, self.unflatten(model, doc));
            }, options);
          }
        }
      } else {
        callback(null, self.unflatten(model, doc));
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
    var id = conds.id || conds._id;
    conds = self.convertQuery(model, conds);
    model.update(doc, conds, function (err, info) {
      self.indexDoc(collection, doc, id, 'rawUpdate', model);
      doc._id = id;
      delete doc.id;
      callback(err, doc);
    }, options);
  });
}


Cassandra.prototype.insert = function insert (collection, json, opts, cb) {
  if (intenseLogging) console.log('insert', collection, json);
  // TODO Leverage pkey flag; it may not be _id
  var self = this;
  this.getModel(collection, function (err, model) {
    if (err) return cb(err);
    var id = json.id = json._id || json.id;
    if (id) {
      var cond = self.convertQuery(model, {id:id});
      json = self.cleanupId(model, json, true);
      return model.update(json, cond, function (err, info) {
        self.indexDoc(collection, json, id, 'insert', model);
        if (cb) cb(err, info);
      }, opts);
    }
    model.provideId(json, function (err, id) {
      if (err) return cb(err);
      json.id = id;
      json = self.cleanupId(model, json, true);
      var conds = {};
      model.schema.info.primaryKeys.forEach(function (e) {
        conds[e] = json[e];
        delete json[e];
      });
      model.update(json, conds, function insertCb (err, doc) {
        self.indexDoc(collection, json, id, 'insert', model);
        if (err) return cb(err);
        cb(null, {_id: id});
      }, opts);
    });
  });
};

Cassandra.prototype.remove = function remove (collection, conds, opts, cb) {
  var self = this;
  this.getModel(collection, function (err, model) {
    if (err) return cb(err);
    conds = self.convertQuery(model, conds);
    model.remove(conds, cb, opts);
  });
};

Cassandra.prototype.findOne = function findOne (collection, conds, opts, cb) {
  if (intenseLogging) console.log('findOne', collection, conds);
  var args = Array.prototype.slice.call(arguments, 1);
  var self = this;
  if (typeof opts === 'function') {
    cb = opts;
    opts = undefined;
  }
  if (!opts) {
    opts = {limit:1};
  } else {
    opts.limit = 1;
  }
  return this.find(collection, conds, opts, function (err, rows) {
    if (err) return cb && cb(err);
    return cb && cb(err, rows[0]);
  });

/*  this.getModel(collection, function (err, model) {
    if (err) return cb(err);
    //selector, fields, options, callback?
    conds = self.convertQuery(model, conds);
    var indexes = model.schema.getIndexes();
    var needMigrate = false;
    var solrProperty = conds.solr_query;
    Object.keys(conds).forEach(function (i) {
      if (i === 'solr_query' || /^_.*|^id$/.test(i))
        return;
      if (indexes.indexOf(i) === -1) {
        if (self.logger)
          self.logger.log('must add index for ' + i + ' to table ' + collection, conds);
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
          item = self.unflatten(item.data) || {};
          if (solrProperty)
            item.solr_query = solrProperty;
          if (intenseLogging) console.log('findOne', item);
          if (oldCb) oldCb(err, item);
        } else {
          if (oldCb) oldCb(err, null);
        }
      }
      //conds, order, offset, callback, consistency
      model.one(conds, cb);
    }
  });
*/
};

Cassandra.prototype.find = function find (collection, conds, opts, cb) {
  if (intenseLogging) console.log('find', collection, conds);
  var self = this;
  this.getModel(collection, function (err, model) {
    if (err) return cb(err);
    var solrProperty = null;
    var docs = [];
    var unsort = {};
    var isFinal = false;
    if (conds.$solr || conds.solr_query) {
      var solrQuery = conds.$solr || conds.solr_query;
      if (!self.solr)
        return cb(new Error('Solr not enabled for searching'));
      if (solrQuery && (solrQuery.reindex || (self.options.solr.reindex && !self.options.solr.reindex[collection]))) {
        var first = true;
        var chunkSize = 1000;
        var lastId = null;
        var queryState = model.schema.info.primaryKeys.length + 1 - model.schema.info.partitionKeys;
        function indexDocs(err) {
          self.options.solr.reindex[collection] = true;
          // Offset is not really supported in CQL 3+
          var idName = model.schema.info.primaryKeys[0];
          var where = {};
          if (lastId) {
            queryState--;
            var tokNames = [];
            var tokVals = [];
            var oldName = null;
            var qs = queryState;
            model.schema.info.primaryKeys.forEach(function (e, i) {
              var f = model.schema.info.fields[e];
              var q;
              if (i < model.schema.info.partitionKeys) {
                var l = qs == 0;
                var v = model.schema.toDatabase(lastId[i], e, model.schema.info.fields);
                tokNames.push(e);
                tokVals.push(v);
                if (oldName) {
                  q = where[oldName];
                  delete where[oldName];
                  oldName = "token(" + tokNames.join(', ') + ")";
                  where[oldName] = q;
                } else {
                  oldName = "token(" + tokNames.join(', ') + ")";
                  q = where[oldName] = {};
                }
                if (l) {
                  q[f.reversed ? 'lt' : 'gt'] = 'token('+ tokVals.join(', ') +')';
                } else {
                  where[oldName] = 'token('+ tokVals.join(', ') +')';
                }
              } else if (qs > 0) {
                qs--;
                l = qs == 0;
                if (l) {
                  q = where[e] = {};
                  q[f.reversed ? 'lt' : 'gt'] = lastId[i];
                } else {
                  where[e] = lastId[i];
                }
              }
            });
          }
          model.find({where:where, limit:chunkSize}, function findCb (err, rows) {
            if (err) {
              console.log(err);
              return cb(err);
            }
            if (first) {
              var isFirst = first;
              first = false;
              var data = {delete:{query: '*:*'}};
              var options = {core:self.keyspace + '.' + model.schema.info.tableName};
              return self.solr.update(data, options, function (err, data) {
                if (err) {
                  console.log(err, data);
                  return cb(err);
                }
                if (isFirst && rows.length === 0) {
                  return solrFind();
                }
                insertRows(rows);
              });
            }
            insertRows(rows);
          });
        }
        function insertRows(rows) {
          // Offset is not really supported in CQL 3+
          if (rows.length === 0) {
            if (queryState === 0) {
              if (self.solrLogger) {
                self.solrLogger.log('COMMIT ' + collection);
              }
              return self.solr.commit({core:self.keyspace + '.' + collection,waitSearcher:true}, function (err) {
                if (err) return cb(err);
                solrFind();
              });
            } else
              return indexDocs(null);
          } else
            queryState = model.schema.info.primaryKeys.length + 1 - model.schema.info.partitionKeys;
          if (self.solrLogger) {
            self.solrLogger.log('INDEX ' + collection + ' count ' + rows.length);
          }
          function addDoc(err) {
            if (err) {
              console.log(err);
              return cb(err);
            }
            var row = rows.splice(0, 1)[0];
            if (!row) {
              return indexDocs(null);
            } else {
              lastId = [];
              model.schema.info.primaryKeys.forEach(function (e, i) {
                lastId.push(row.data[e]);
              });
              var doc = self.unflatten(model, row.data) || {};
              self.indexDoc(collection, doc, doc.id, 'reindex', model, addDoc);
            }
          }
          addDoc(null);
        }
        indexDocs(null, 0);
      } else
        solrFind();
      function solrFind() {
        var q = solrQuery;
        solrProperty = q;
        var hl = false;
        if (typeof solrQuery === 'object') {
          q = solrQuery.q;
          hl = solrQuery.hl;
        }
        var q = self.solr.createQuery().q(q).start(solrQuery.start || opts.offset || 0).rows(solrQuery.rows || opts.limit || 50);
        var fl = solrQuery.fl;
        if (Array.isArray(solrQuery.multicore)) {
          q.multicore(solrQuery.multicore.map(function (core) {
            return self.keyspace + '.' + core;
          }));
          if (fl && fl !== '*') {
            fl += ",_core";
          }
        }
        if (!fl && !(solrQuery.multicore || solrQuery.solronly)) {
          fl = 'id';
        }
        if (hl) {
          q.set('hl=true');
          q.set('hl.fl=' + (solrQuery['hl.fl'] || '*'));
          q.set('hl.simple.pre=' + (solrQuery['hl.simple.pre'] || '<strong>'));
          q.set('hl.simple.post=' + (solrQuery['hl.simple.post'] || '</strong>'));
        }
        q.shards(self.options.shardCount > 1);
        if (typeof solrQuery === 'object') {
          Object.keys(solrQuery).forEach(function (k) {
            if (/^hl$|^hl.fl$|^hl.simple.pre$|^hl.simple.post$|^raw$|^multicore$|^sort$|^reindex$|^solronly$|^start$|^rows$|^q$/.test(k))
              return;
            q.set(k + '=' + encodeURIComponent(solrQuery[k]));
          });
        }
        if (opts.sort) {
          var ss = [];
          opts.sort.forEach(function (s) {
            ss.push(s[0] + "%20" + ((s.length > 0 && s[1] === 'asc' || s[1] !== 'desc') ? 'asc' : 'desc'));
          });
          q.set('sort=' + ss.join(','));
        } else if (solrQuery.sort) {
          q.set('sort=' + encodeURIComponent(solrQuery.sort));
        }
        q.core(self.keyspace + '.' + model.schema.info.tableName);
        if (self.solrLogger) {
          self.solrLogger.log('SEARCH ' + model.schema.info.tableName, q);
        }
        self.solr.search(q, function (err, docs) {
          if (err) {
            if (self.solrLogger) {
              var info = err.json || {
                message: err.message
              };
              info.table = self.keyspace + '.' + model.schema.info.tableName;
              self.solrLogger.log('SEARCH FAILED ' + model.schema.info.tableName, info);
            }
            return cb(null, []);
          }
          if (docs) {
            if (opts.count) {
              return cb(null, docs.response.numFound);
            }
            if (solrQuery.raw) {
              if (solrProperty)
                docs.solr_query = solrProperty;
              return cb(null, docs);
            }
            var allConds;
            var allIds = [];
            var newConds = {id: {$all:[]}};
            if (model.schema.info.primaryKeys.length === 1) {
              docs.response.docs.forEach(function (doc) {
                if (doc.id) {
                  newConds.id.$all.push(doc.id);
                  allIds.push(doc.id);
                }
              });
              allConds = [newConds];
            } else {
              allConds = [];
              condsObj = {};
              var keys = [];
              for (var i = 0; i < model.schema.info.primaryKeys.length - 1; i++) {
                keys.push(model.schema.info.primaryKeys[i]);
              }
              var last = model.schema.info.primaryKeys[model.schema.info.primaryKeys.length - 1];
              docs.response.docs.forEach(function (doc) {
                var hasKeys = true;
                var k = keys.map(function (e) {
                  if (doc[e] === undefined)
                    hasKeys = false;
                  return doc[e];
                }).join('|||');
                if (!hasKeys)
                  return;
                var c = condsObj[k];
                if (!c) {
                  c = condsObj[k] = {};
                  c[last] = { $all:[] };
                  keys.forEach(function (ee) {
                    c[ee] = doc[ee];
                  });
                  allConds.push(c);
                }
                c[last].$all.push(doc[last]);
                allIds.push(doc.id);
              });
            }
            if (intenseLogging) console.log('solr docs', docs, newConds);
            if (allConds.length === 0) {
              return cb(null, []);
            }
            if (solrQuery.multicore || solrQuery.solronly) {
              var items = [];
              docs.response.docs.forEach(function (item) {
                item._id = item.id;
                if (solrProperty)
                  item.solr_query = solrProperty;
                var h;
                if (docs.highlighting && (h = docs.highlighting[item.id])) {
                  item._highlight = h;
                }
                items.push(item);
                delete item.id;
              });
              return cb(null, items);
            } else {
              return finishFind({all:allConds,ids:allIds}, true, docs.highlighting);
            }
          } else {
            return cb(null, []);
          }
        });
      }
    } else {
      return finishFind(conds, false);
    }
    function finishFind(conds, isSolr, highlighting) {
      var filter;
      if (conds.all) {
        var cond = conds.all.splice(0, 1)[0];
        filter = {
          where: self.convertQuery(model, cond),
          order: self.convertSort(opts.sort, isSolr),
          limit: opts.limit,
          offset: opts.offset
        };
        isFinal = conds.all.length === 0;
      } else {
        filter = {
          where: self.convertQuery(model, conds),
          order: self.convertSort(opts.sort, isSolr),
          limit: opts.limit,
          offset: opts.offset
        };
        isFinal = true;
      }
      var needMigrate = false;
      if (!isSolr) {
        var indexes = model.schema.getIndexes();
        Object.keys(filter.where).forEach(function (i) {
          if (i === 'solr_query' || /^_.*$/.test(i))
            return;
          if (model.schema.info.primaryKeys.indexOf(i) != -1)
            return;
          if (indexes.indexOf(i) === -1) {
            if (self.logger)
              self.logger.log('must add index for ' + i + ' to table ' + collection, filter.where);
            if (!model.schema.info.fields[i])
              model.schema.info.fields[i] = { name:i, index:true, type: 'Object' }
            else {
              model.schema.info.fields[i].index = true;
            }
            needMigrate = true;
          }
        });
      }
      if (needMigrate) {
        model.automigrate(execute);
      } else
        execute(null);
      function execute(err) {
        if (err) return cb(err, null);
        if (opts.count) {
          return model.count(filter.where, filter.limit, filter.offset, findCb, opts && opts.consistencylevel);
        } else {
          return model.find(filter, findCb, opts && opts.consistencylevel);
        }
        function findCb (err, rows) {
          if (err) {
            if (self.logger) {
              self.logger.log('unable to find data ' + collection + ': ' + err.message);
            }
            return cb(err);
          }
          if (opts.count) {
            return cb(err, rows);
          }
          rows.forEach(function (row) {
            if (row.data) {
              var item = self.unflatten(model, row.data) || {};
              item._id = item.id;
              if (solrProperty)
                item.solr_query = solrProperty;
              var h;
              if (highlighting && (h = highlighting[item.id])) {
                item._highlight = h;
              }
              delete item.id;
              if (isSolr)
                unsort[item._id] = item;
              else
                docs.push(item);
            }
          });
          if (isFinal) {
            if (isSolr) {
              if (conds.ids) {
                conds.ids.forEach(function (id) {
                  var doc = unsort[id];
                  if (doc)
                    docs.push(doc);
                });
              }
            } else if (opts.sort) {
              docs = docs.sort(function (a, b) {
                for (var i = 0; i < opts.sort.length; i++) {
                  var v = opts.sort[i];
                  var aa = a[v[0]];
                  var bb = b[v[0]];
                  if (aa === bb)
                    continue;
                  var aanull = (aa === undefined) || (aa === null);
                  var bbnull = (bb === undefined) || (bb === null);
                  if (aanull && bbnull)
                    continue;
                  if (v[1].toLowerCase() === 'asc') {
                    if (aanull && !bbnull)
                      return -1;
                    if (!aanull && bbnull)
                      return 1;
                    if (aa < bb)
                      return -1;
                    else if (aa > bb)
                      return 1;
                  } else {
                    if (aanull && !bbnull)
                      return 1;
                    if (!aanull && bbnull)
                      return -1;
                    if (aa > bb)
                      return -1;
                    else if (aa < bb)
                      return 1;
                  }
                }
                return 0;
              });
            }
            return cb(null, docs);
          } else
            finishFind(conds, isSolr, highlighting);
        }
      }
    }
  });
};

Cassandra.prototype.count = function count (collection, conds, opts, cb) {
  var self = this;
  this.getModel(collection, function (err, model) {
    if (err) return cb(err);
    conds = self.convertQuery(model, conds);
    opts.count = true;
    self.find(collection, conds, opts, cb);
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
    console.log(collection, id, relPath, val, ver);
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
      adapter.getModel(collection, function (err, model) {
        if (err) return next(err);
        data = adapter.cleanupId(model, data, true);
        adapter.rawUpdate(collection, {_id:id}, [], data, {upsert:true}, next);
      });
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

exports.Cassandra = Cassandra;
