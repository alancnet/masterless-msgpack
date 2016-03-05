var net = require('net');
var url = require('url');
var util = require('util');
var debug = require('debug')('masterless-msgpack');
var heartbug = require('debug')('masterless-msgpack:heartbeat');
var msgpack = require('msgpack');

//var Chunker = require('chunk-by');
var Linked = require('./linked');

var Emitter = require('events').EventEmitter;

var slice = Array.prototype.slice;
var last = null;

// directional connection states
var NOT_CONNECTED = 0;
var CONNECTING = 1;
var CONNECTED = 2;

var INCOMING = 0;
var OUTGOING = 1;

function noop() {}

function Socket(conn, port, host) {
  // create a chunker transform stream, which parses the json and splits the
  // stream by CRLF
  //   var stream = new Chunker(JSON.parse, {matcher: '\r\n'});
  var stream = new msgpack.Stream(conn);
  stream.write = function(data) {
      stream.send(data);
  }
//  stream.conn = conn;
  stream.end = function() {
      conn.end();
  }
  this.conn = conn;
  this.stream = stream;

  // pipe the raw bytes into the chunker stream
  //conn.pipe(stream);

  // contains the port and host for the connection for reconnection purposes
  this.info = {
    port: port,
    host: host
  };

  this.direction = INCOMING;

  // whether the connection is being intentionally closed from the local end,
  // reduces unnecessary logging in the onend/onclose handlers
  this.closing = false;
}

// encode and send a packet on the connection
Socket.prototype.send = function(packet) {
  //this.conn.write(JSON.stringify(packet) + '\r\n');
  this.stream.send(packet);
};

function Node(id) {
  this.id = id;

  // queued packets yet to be sent
  this.queue = [];

  // holds the Socket instance which contains the connection. null until a
  // handshake completes and a stable connection has been formed
  this.stream = null;

  // a tuple of seconds, nanoseconds at which the last send occurred
  this.lastSend = [];

  // the state of one and only one outgoing connection to the represented node
  // can be one of NOT_CONNECTED (0), CONNECTING (1) or CONNECTED (2). the
  // localPriority field will be non-zero in either of the last two cases
  this.localState = NOT_CONNECTED;
  this.localPriority = 0;

  // the state of one and only one incoming connection to the represented node
  // can be one of NOT_CONNECTED (0), CONNECTING (1) or CONNECTED (2). the
  // remotePriority field will be non-zero in either of the last two cases
  this.remoteState = NOT_CONNECTED;
  this.remotePriority = 0;

  // whether the remote node wants to keep the connection
  this.keeping = true;

  // whether the node is actively reconnecting
  this.reconnecting = false;
}

// just a convenience method that sets the stream field and flushes the queue
Node.prototype.to = function(stream) {
  this.stream = stream;

  var queue = this.queue;
  this.queue = [];

  // send all the packets that have been queued in the send method
  for (var i = 0, n = queue.length; i < n; i++) {
    stream.send(queue[i]);
  }
};

// send and optionally encode a packet, queueing the packet if there isn't yet
// a stable connection to the node
Node.prototype.send = function(packet, isRaw) {
  if (!isRaw) {
      //packet = JSON.stringify(packet) + '\r\n';
      // Don't encode the packet at all because the stream will do that.
  }

  // if there is a stream, there's a stable connection
  if (this.stream) {
    // update the last send tuple
    this.lastSend = process.hrtime();

    // and write the data
    // this.stream.conn.write(packet);
    this.stream.send(packet);
  } else {
    // queue the packet for future transmission
    this.queue.push(packet);
  }
};

function Server(id, options) {
  if (!(this instanceof Server)) {
    return new Server(id, options);
  }

  Emitter.call(this);

  options || (options = {});

  this.id = id;

  // the busy list holds all of the connections that are handshaking, so that
  // the server can close them
  this._busy = new Linked();

  // the keep object maps node ids to booleans of whether the connection should
  // be kept
  this._keep = Object.create(null);

  // the nodes object maps node ids to node objects. these contain all of the
  // currently stable connections to other masterless instances
  this._nodes = Object.create(null);

  // allow the user to specify a custom debugging function, in case more
  // powerful debugging tools are necessary
  this._debug = options.debug || debug;

  // create a tcp server which calls onremoteconn for every incoming connection
  this._server = net.createServer(onremoteconn.bind(this));

  // bind the server to the provided port and host. this does not bind to all
  // interfaces by default because it can be useful to only bind to localhost,
  // especially in unit tests and on continuous integration machines
  this._server.listen(options.port, options.host, onlisten.bind(this));

  // start the heartbeat interval
  this._interval = setInterval(heartbeat.bind(this), 1000);
}

util.inherits(Server, Emitter);

// send all the heartbeats to all the nodes
function heartbeat() {
  for (var key in this._nodes) {
    var node = this._nodes[key];

    // ensure more` than a second has elapsed since the last send. this works
    // because this use of hrtime returns a tuple of [seconds, nanoseconds] and
    // if the seconds field is non-zero it will evaluate to true. also ensure
    // there's actually a node and that the node is connected
    if (process.hrtime(node.lastSend)[0] && node && node.stream) {
      heartbug(this.id, 'hearbeat to', key);
      // node.stream.conn.write('true\r\n');

      node.stream.send({ heartbeat: true });
    }
  }
}

// get the specified node, or create it if it doesn't exist
function getNode(server, id) {
  var node = server._nodes[id];

  if (!node) {
    node = new Node(id);
    server._nodes[id] = node;
  }

  return node;
}

function connect(server, port, host, callback) {
  // ensure the callback is a function or falsy, for onlocalconn
  if (callback && typeof callback !== 'function') {
    callback = null;
  }

  // connect via the specific port and host
  var conn = net.connect(port, host);

  // emit a timeout error when if the connection times out
  conn.on('timeout', function() {
    conn.emit('error', new Error('connection timed out'));
  });

  // the connection will emit a timeout event if the node does not send any data
  // for five seconds
  conn.setTimeout(5000);

  // wrap the connection in a socket object, which holds on to the host/port for
  // reconnection purposes
  var sock = new Socket(conn, port, host);

  sock.direction = OUTGOING;

  // start the local handshake process
  onlocalconn.call(server, sock, callback);

  return sock;
}

// reconnect to a node
function reconnect(server, node, conn) {
  // otherwise, if not a relevant node, not our job to reconnect
  if (node.reconnecting || !server._keep[node.id]) {
    // only log if reconnecting
    if (node.reconnecting) {
      server._debug(server.id, 'connection to', node.id, 'failed but already reconnecting');
    }

    // disconnect from the node
    return server.disconnect(node.id);
  }

  server._debug(server.id, 'reconnecting to', node.id);

  var info = conn.info;

  // open a new connection
  conn = connect(server, info.port, info.host, function(err, id) {
    // if the connection failed, or if the id of the node at that ip and port
    // changed, disconnect from the node id. it is conceivable that a node could
    // die, and a new one could start and take the old node's port on the same
    // machine
    if (err || id !== node.id) {
      server.disconnect(node.id);
    }
  });

  // set the reconnecting flag so we don't try to reconnect too much. the flag
  // is reset in onhandshake so we can reconnect provided we've made it past the
  // handshake
  node.reconnecting = true;
}

// parse an ip and port from a 'tcp://<ip>:<port>' string
function _parse(info) {
  if (typeof info !== 'string') {
    throw new TypeError('expecting string transport uri');
  }

  var uri = url.parse(info);

  if (!(uri.port > 0)) {
    throw new Error('expected valid transport uri');
  }

  return {
    host: uri.hostname,
    port: uri.port
  };
}

// format an ip and port into a 'tcp://<ip>:<port>' string
function _format(host, port) {
  return url.format({
    slashes: true,
    protocol: 'tcp',
    hostname: host,
    port: port
  });
}

Server.prototype.send = function(target, packet) {
  var node = null;

  // throw an error if the packet isn't a regular object
  if (!packet || typeof packet !== 'object' || Array.isArray(packet)) {
    throw new TypeError('expected object packet');
  }

  // if the target is a string, send to the node and return whether the send
  // was successful
  if (typeof target === 'string') {
    node = this._nodes[target];
    if (!node)
      return false;
    node.send(packet);
    return true;
  }

  // ensure the target is an array if it isn't a string
  if (!Array.isArray(target)) {
    throw new TypeError('expected string or array target');
  }

  // pre-encode the packet so we don't duplicate the work of encoding the packet
  // for every target, and count the number of nodes we're able to route to
  // var raw = JSON.stringify(packet) + '\r\n', count = 0;
  var count = 0;

  for (var i = 0, n = target.length; i < n; i++) {
    // get the node representation
    node = this._nodes[target[i]];

    // can't send if we don't have a connection ;)
    if (node) {
      // send the pre-encoded packet, specifying true to tell the send method
      // not to re-encode the packet
//      node.send(raw, true);
      node.send(packet);
      count++;
    }
  }

  // return the number of nodes the packet will actually get to
  return count;
};

Server.prototype.keep = function(id, keep) {
  keep = keep === undefined ? true : !!keep;
  if (keep) {
    this._keep[id] = true;
  } else {
    delete this._keep[id];
  }
  var node = this._nodes[id];
  if (node && node.stream) {
      //node.send([keep]);
      node.send({ $masterless: { keep: keep }});
  }
};

Server.prototype.isKept = function(id) {
  return !!this._keep[id];
};

Server.prototype.connections = function() {
  return Object.keys(this._nodes);
};

// TODO: how to prevent connecting to self?
Server.prototype.connect = function(info, callback) {
  var uri = _parse(info);
  connect(this, uri.port, uri.host, callback);
};

// immediately disconnect from the specified id
Server.prototype.disconnect = function(id) {
  // stop keeping the id
  delete this._keep[id];

  var node = this._nodes[id];

  if (node) {
    // remove the node from the nodes map
    delete this._nodes[id];

    // end the stream. the onhandshake handler will then remove event listeners
    // and add the noop error handlers and
    if (node.stream) {
      node.stream.closing = true;
      node.stream.conn.end();
    }

    this._debug(this.id, 'disconnected from', id);

    this.emit('disconnect', id);
  }
};

// get the connection info
Server.prototype.info = function() {
  // get the address and port the server has bound to, especially important if
  // port was zero, which tells the net module to get a random open port
  var address = this._server.address();

  // if the address is falsy, the server must not yet be listening, or must be
  // closed
  if (!address)
    return null;

  // format the ip and port as a connection string
  return _format(address.address, address.port);
};

Server.prototype.close = function(callback) {
  // empty the map of nodes to keep, they no longer matter
  this._keep = Object.create(null);

  this._debug(this.id, 'has', this._server._connections, 'incoming connections');

  var incoming = 0, outgoing = 0, misc = [];

  // end all of the connections, because server.close only stops accepting
  // connections
  for (var key in this._nodes) {
    var node = this._nodes[key];
    if (node) {
      if (node.localState !== NOT_CONNECTED) {
        outgoing++;
      } else if (node.remoteState !== NOT_CONNECTED) {
        incoming++;
      } else {
        misc.push(node);
      }
      node.stream && node.stream.conn.end();
    }
  }

  this._debug(this.id, 'closing', incoming, 'stable incoming connections');
  this._debug(this.id, 'closing', outgoing, 'stable outgoing connections');

  misc.length && this._debug(this.id, 'has bad stable connections', misc);

  // empty the map of node identifiers to node objects
  this._nodes = Object.create(null);

  incoming = 0;
  outgoing = 0;

  this._debug(this.id, 'closing', this._busy.length, 'handshake connections');

  // end all of the handshaking connections, and empty the list
  this._busy.each(function(conn) {
    if (conn.direction === INCOMING) {
      incoming++;
    } else {
      outgoing++;
    }
    conn.conn.end();
  }).empty();

  this._debug(this.id, 'closing', incoming, 'incoming handshake connections');
  this._debug(this.id, 'closing', outgoing, 'outgoing handshake connections');

  this._debug(this.id, 'not accepting new connections');

  // stop accepting new connections
  this._server.close(callback);

  // clear the heartbeat interval
  clearInterval(this._interval);
  this._interval = null;
};

// handle the tcp server listening event
function onlisten() {
  // get the formatted connection info string
  var info = this.info();

  // the listening event still fires even if the server has been closed as per
  // joyent/node#7834
  if (!info) {
    return this._debug(this.id, 'server closed before listening event');
  }

  // emit the listening event with the connection info
  this.emit('listening', info);
}

// initiates handshake on an outgoing (locally opened) connection
function onlocalconn(conn, callback) {
  var self = this, node = null;

  this._debug(this.id, 'outgoing connection');

  // add the connection to the busy linked list, so that we clean it up while
  // closing the server if but the connection doesn't finish the handshake
  var busy = this._busy.push(conn);

  // unbind all event listeners. add noop error handlers if ignoreErrors is
  // specified
  function unbind(ignoreErrors) {
    // remove all of the listeners bound below
    conn.conn.removeListener('error', onerror);
    conn.stream.removeListener('error', onerror);
    conn.stream.removeListener('msg', ondata);
    conn.stream.removeListener('end', onend);
    conn.conn.removeListener('close', onend);

    // don't assign the noop error handlers if somebody else cares about the
    // errors
    if (ignoreErrors) {
      // ignore errors on the connection or stream because it no longer matters
      conn.conn.on('error', noop);
      conn.stream.on('error', noop);
    }

    // remove the connection from the busy linked list, so that it won't be
    // cleaned up. this happens if the connection is killed, or the handshake
    // completes and the connection is used for application-level data
    self._busy.remove(busy);
  }

  // bind all of the event listeners necessary, reversed by unbind
  //console.log(conn.stream)
  conn.conn.on('error', onerror);
  conn.stream.on('error', onerror);
  conn.stream.addListener('msg', ondata);
  conn.stream.on('end', onend);
  conn.conn.on('close', onend);



  // send our id, including the transport info because the remote node does not
  // have the info reconnect
  conn.send({id: this.id, info: this.info()});

  // an actual network or protocol error: kill the connection, maybe reconnect
  function onerror(err) {
    // if we have a node, we've made the initial handshake, so we have state to
    // deal with
    if (node) {
      // reconnect if the outgoing connection hasn't made any progress otherwise
      // just discard the connection: we're either implicitly reconnecting via
      // handshake or have an established connection
      if (node.remoteState === NOT_CONNECTED) {
        reconnect(self, node, conn);
      }

      // clear the state
      node.localState = NOT_CONNECTED;
      node.localPriority = 0;
    } // else no details on what went wrong or how to fix it, just abort

    self._debug(err, self.id, 'on outgoing connection' + (node ? ' to ' + node.id : ''));

    // unbind event listeners and add noop error handlers
    unbind(true);

    // necessary because the error might be connection-related and implicitly
    // kill the connection, or it could be protocol related in which case the
    // connection is still open
    conn.conn.end();

    // if connection initiator supplied a callback, call back with the error
    // and default to a generic "connection error" object
    callback && callback(err || new Error('connection error'));
  }

  // handle a parsed packet
  function ondata(data) {
    // if there isn't a node and the data includes an id, this is the initial
    // handshake packet with the node's unique identifier and transport info
    if (!node && data && data.id) {
      var id = data.id;

      // connection was initiated by the local node to itself, just abort
      if (id === self.id) {
        self._debug(self.id, 'closing outgoing self-connection');

        // unbind first, making sure to add the noop error handlers, in case
        // ending the connection causes synchronous error events
        unbind(true);

        // end the connection
        return conn.conn.end();
      }

      // get the node for the given id, creating the node if it doesn't exist
      node = getNode(self, id);

      self._debug(self.id, 'initial packet on outgoing connection to', id);

      // continue even if the connection is unwanted, and wait until after
      // emitting connect event in the onhandshake function to abort, so the
      // remote end has time to decide whether the connection is useful

      // if we have an outgoing connection for that node, and it's either making
      // the handshake or fully connected, abort
      if (node.localState !== NOT_CONNECTED) {
        self._debug(self.id, 'closing duplicate outgoing connection to', id);

        // unbind first, making sure to add the noop error handlers, in case
        // sending the priority 0 (abort) packet or ending the connection causes
        // synchronous error events
        unbind(true);

        // send the abort packet
        conn.send({priority: 0});

        // end the connection
        conn.conn.end();

        // if connection initiator supplied a callback, call back with the error
        // that indicates there were multiple outgoing connections
        return callback && callback(new Error('duplicate outgoing connection'));
      }

      // if we have a stable incoming connection for that node, abort
      if (node.remoteState === CONNECTED) {
        self._debug(self.id, 'closing duplicate connection to', id);

        // clear the state
        node.localState = NOT_CONNECTED;
        node.localPriority = 0;

        // unbind first, making sure to add the noop error handlers, in case
        // sending the priority 0 (abort) packet or ending the connection causes
        // synchronous error events
        unbind(true);

        // send the abort packet
        conn.send({priority: 0});

        // end the connection
        conn.conn.end();

        // if connection initiator supplied a callback, call back with the error
        // that indicates there were multiple connections
        return callback && callback(new Error('duplicate connection'));
      }

      // we're now in the handshake, set the state to "connecting"
      node.localState = CONNECTING;

      // if the incoming state is also set to "connecting", generate a priority.
      // in all other cases, set the priority to 1, forcing the use of this
      // connection
      if (node.remoteState === CONNECTING) {
        // have remote connection, need to decide which to use
        node.localPriority = Math.random();
      } else { // remoteState === NOT_CONNECTED
        // no remote connection, force use
        node.localPriority = 1;
      }

      // send the priority packet
      conn.send({priority: node.localPriority});
    } else if (node && data && data.confirm) {
      self._debug(self.id, 'confirm packet on outgoing connection to', node.id);
      self._debug(self.id, 'outgoing connection to', node.id, 'locked');

      // remote has chosen this connection

      // TODO: manually abort incoming connection?

      // set the outgoing state and priority
      node.localState = CONNECTED;
      node.localPriority = 1;

      // unbind the event listeners and don't add the noop error handlers,
      // because onhandshake will define its own
      unbind(false);

      // pass control to onhandshake, which handles the connection after the
      // handshake until the server closes, an error occurs, or the connection
      // becomes unwanted by both parties
      onhandshake.call(self, node, conn);

      // if connection initiator supplied a callback, call back with the id of
      // the node that has completed the handshake
      callback && callback(null, node.id);
    } else if (data !== true) {
      // protocol error, anything other than first packet and confirm packet
      // should just be the tcp FIN packet
      self._debug(self.id, 'unexpected packet on outgoing connection to', node.id + ':', data);

      // don't reconnect because this is an application-level error (tcp would
      // take care of reliable delivery), so just end the connection

      // might be before the initial handshake, so be careful
      if (node) {
        node.localState = NOT_CONNECTED;
        node.localPriority = 0;
      }

      // unbind first, making sure to add the noop error handlers, in case
      // ending the connection causes synchronous error events
      unbind(true);

      // end the connection
      conn.conn.end();

      // if connection initiator supplied a callback, call back with the error
      // indicating that we encountered an unexpected packet
      callback && callback(new Error('unexpected packet'));
    }
  }

  // handle the end of the connection, expected or not
  function onend() {
    self._debug(self.id, 'outgoing connection closed' + (node ? ' to ' + node.id : ''));

    // if we have a node, we've made the initial handshake, so we have state to
    // deal with
    if (node) {
      node.localState = NOT_CONNECTED;
      node.localPriority = 0;
    }

    // unbind event listeners and add noop error handlers
    unbind(true);

    // if connection initiator supplied a callback, call back with the error
    // that the connection has closed
    callback && callback(new Error('connection closed'));
  }
}

// handshakes an incoming connection
function onremoteconn(rconn) {
  var self = this, node = null;

  this._debug(this.id, 'incoming connection');

  // don't actually know the transport info yet
  var conn = new Socket(rconn, null, null);

  // add the connection to the busy linked list, so that we clean it up while
  // closing the server if but the connection doesn't finish the handshake
  var busy = this._busy.push(conn);

  // unbind all event listeners. add noop error handlers if ignoreErrors is
  // specified
  function unbind(ignoreErrors) {
    // remove all of the listeners bound below
    conn.conn.removeListener('error', onerror);
    conn.stream.removeListener('error', onerror);
    conn.stream.removeListener('msg', ondata);
    conn.stream.removeListener('end', onend);
    conn.conn.removeListener('close', onend);

    // don't assign the noop error handlers if somebody else cares about the
    // errors
    if (ignoreErrors) {
      // ignore errors on the connection or stream because it no longer matters
      conn.conn.on('error', noop);
      conn.stream.on('error', noop);
    }

    // remove the connection from the busy linked list, so that it won't be
    // cleaned up. this happens if the connection is killed, or the handshake
    // completes and the connection is used for application-level data
    self._busy.remove(busy);
  }

  // bind all of the event listeners necessary, reversed by unbind
  conn.conn.on('error', onerror);
  conn.stream.on('error', onerror);
  conn.stream.on('msg', ondata);
  conn.stream.on('end', onend);
  conn.conn.on('close', onend);

  // an actual network or protocol error: kill the connection, maybe reconnect
  function onerror(err) {
    // if we have a node, we've made the initial handshake, so we have state to
    // deal with
    if (node) {
      // reconnect if the outgoing connection hasn't made any progress otherwise
      // just discard the connection: we're either implicitly reconnecting via
      // handshake or have an established connection
      if (node.localState === NOT_CONNECTED) {
        reconnect(self, node, conn);
      }

      // clear the state
      node.remoteState = NOT_CONNECTED;
      node.remotePriority = 0;
    } // else no details on what went wrong or how to fix it, just abort

    self._debug(err, self.id, 'on incoming connection' + (node ? ' to ' + node.id : ''));

    // unbind event listeners and add noop error handlers
    unbind(true);

    // necessary because the error might be connection-related and implicitly
    // kill the connection, or it could be protocol related in which case the
    // connection is still open
    conn.conn.end();
  }

  // handle a parsed packet for a NEW connection
  function ondata(data) {
    // if there isn't a node and the data includes an id, this is the initial
    // handshake packet with the node's unique identifier and transport info
    if (!node && data && data.id) {
      var id = data.id;

      // connection was initiated by the local node to itself, just abort
      if (id === self.id) {
        self._debug(self.id, 'closing incoming self-connection');

        // unbind first, making sure to add the noop error handlers, in case
        // ending the connection causes synchronous error events
        unbind(true);

        // end the connection
        return conn.conn.end();
      }

      self._debug(self.id, 'getting info', data.info, 'from', id);

      // try to parse the provided info and store it in the connection object
      try {
        var uri = _parse(data.info);
        conn.info.host = uri.host;
        conn.info.port = uri.port;
      } catch (err) {
        // info might not have been a string, or might not have been a valid
        // url. regardless, we  cannot reconnect without valid transport info,
        // so we'll just abort

        self._debug(self.id, 'bad info on incoming connection', data.info);

        // unbind first, making sure to add the noop error handlers, in case
        // ending the connection causes synchronous error events
        unbind(true);

        // end the connection
        return conn.conn.end();
      }

      // get the node for the given id, creating the node if it doesn't exist
      node = getNode(self, id);

      self._debug(self.id, 'initial packet on incoming connection to', id);

      // continue even if the connection is unwanted, and wait until after
      // emitting connect event in the onhandshake function to abort, so the
      // remote end has time to decide whether the connection is useful

      // if we have an incoming connection for that node, and it's either making
      // the handshake or fully connected, abort. this handles the case where
      // the remote end makes multiple connections to the same node
      if (node.remoteState !== NOT_CONNECTED) {
        self._debug(self.id, 'closing duplicate incoming connection to', id);

        // unbind first, making sure to add the noop error handlers, in case
        // ending the connection causes synchronous error events
        unbind(true);

        // end the connection
        return conn.conn.end();
      }

      // if we have an outgoing connection for that node, and it's either making
      // the handshake or fully connected, abort
      if (node.localState !== NOT_CONNECTED) {
        self._debug(self.id, 'closing duplicate connection to', id);

        // clear the state
        node.remoteState = NOT_CONNECTED;
        node.remotePriority = 0;

        // unbind first, making sure to add the noop error handlers, in case
        // ending the connection causes synchronous error events
        unbind(true);

        // end the connection
        return conn.conn.end();
      }

      // we're now in the handshake, set the state to "connecting"
      node.remoteState = CONNECTING;

      // send our id, not including the transport info because the remote node
      // must have the info to have made the connection in the first place
      conn.send({id: self.id});
    } else if (node && data && data.priority === +data.priority) {
      self._debug(self.id, 'priority', data.priority, 'packet on incoming connection to', node.id);

      // if there's already a stable outgoing connection to the same node, use
      // that connection and close the incoming connection
      if (node.localState === CONNECTED) {
        // clear the state for this connection
        node.remoteState = NOT_CONNECTED;
        node.remotePriority = 0;

        // unbind first, making sure to add the noop error handlers, in case
        // ending the connection causes synchronous error events
        unbind(true);

        // end the connection
        return conn.conn.end();
      }

      // store the remote priority
      node.remotePriority = data.priority;

      // if there's an outgoing connection in the middle of the handshake, it'll
      // have an associated priority we can use to select one of the connections
      if (node.localState === CONNECTING) {
        // if our priority is smaller, abort this connection
        if (node.remotePriority < node.localPriority) {
          // clear the state for this connection
          node.remoteState = NOT_CONNECTED;
          node.remotePriority = 0;

          // unbind first, making sure to add the noop error handlers, in case
          // ending the connection causes synchronous error events
          unbind(true);

          // end the connection
          return conn.conn.end();
        }

        // if the probabilities are the same, then all Math.random must be
        // poorly implemented, or the extremely unlikely has occurred
        if (node.remotePriority === node.localPriority) {
          // TODO: retry priorities when they collide
          self._debug('HOLY PROBABILITY, BATMAN');
        }
      }

      self._debug(self.id, 'incoming connection to', node.id, 'locked');

      // implied if node.localState === NOT_CONNECTED

      // TODO: manually abort outgoing connection?

      // set the incoming state and priority
      node.remoteState = CONNECTED;
      node.remotePriority = 1;

      // send a confirm packet so the other end knows to use this connection
      conn.send({confirm: true});

      // unbind the event listeners and don't add the noop error handlers,
      // because onhandshake will define its own
      unbind(false);

      // pass control to onhandshake, which handles the connection after the
      // handshake until the server closes, an error occurs, or the connection
      // becomes unwanted by both parties
      onhandshake.call(self, node, conn);
    } else if (data !== true) {
      // protocol error, anything other than first packet and confirm packet
      // should just be the tcp FIN packet
      self._debug(self.id, 'unexpected packet on outgoing connection to', node.id + ':', data);

      // don't reconnect because this is an application-level error (tcp would
      // take care of reliable delivery), so just end the connection

      // might be before the initial handshake, so be careful
      if (node) {
        node.remoteState = NOT_CONNECTED;
        node.remotePriority = 0;
      }

      // unbind first, making sure to add the noop error handlers, in case
      // ending the connection causes synchronous error events
      unbind(true);

      // finally, end the connection
      conn.conn.end();
    }
  }

  // handle the end of the connection, expected or not
  function onend() {
    self._debug(self.id, 'incoming connection' + (node ? ' to ' + node.id : '') + ' closed');

    // if we have a node, we've made the initial handshake, so we have state to
    // deal with
    if (node) {
      node.remoteState = NOT_CONNECTED;
      node.remotePriority = 0;
    }

    // unbind event listeners and add noop error handlers
    unbind(true);
  }
}

// handles data and errors after the handshake
function onhandshake(node, conn) {
  var self = this;

  // unbind all event listeners, and add noop error handlers
  function unbind() {
    // remove all of the listeners bound below
    conn.conn.removeListener('error', onerror);
    conn.stream.removeListener('error', onerror);
    conn.stream.removeListener('msg', ondata);
    conn.stream.removeListener('end', onend);
    conn.conn.removeListener('close', onend);

    // ignore errors on the connection or stream because it no longer matters
    conn.conn.on('error', noop);
    conn.stream.on('error', noop);
  }

  // bind all of the event listeners necessary, reversed by unbind
  conn.conn.on('error', onerror);
  conn.stream.on('error', onerror);
  conn.stream.on('msg', ondata);
  conn.stream.on('end', onend);
  conn.conn.on('close', onend);

  // set node.stream = conn, write all the queued packets on the connection
  node.to(conn);

  // if the handshake was part of a reconnect, don't emit the connect event
  if (node.reconnecting) {
    node.reconnecting = false;
  } else {
    // emit the connect event with the node's id and the connection info
    this.emit('connect', node.id, _format(conn.info.host, conn.info.port));
  }

  // tell the other end whether or not this connection should be kept. an array
  // is not really the ideal capsule, but it'll work
  //conn.send([!!this._keep[node.id]]);
  conn.send({ $masterless: { keep: !!this._keep[node.id] }});

  // an actual network or protocol error: kill the connection, and reconnect
  function onerror(err) {
    self._debug(err, self.id, 'after handshake with', node.id);

    // set the node's stream property to null because the stream should only
    // exist if there exists a stable connection to that node
    node.stream = null;

    // clear the state for the appropriate direction
    if (node.remoteState === 2) {
      node.remoteState = 0;
      node.remotePriority = 0;
    } else {
      node.localState = 0;
      node.localPriority = 0;
    }

    // unbind and add the noop error handlers first, in case ending the
    // connection causes synchronous error events
    unbind();

    // necessary because the error might be connection-related and implicitly
    // kill the connection, or it could be protocol related in which case the
    // connection is still open
    conn.conn.end();

    // reconnect because there's a reasonable chance we'll succeed: we've
    // already shaken hands with the node once, it may still exist
    reconnect(self, node, conn);
  }

  // handle a parsed packet for an ESTABLISHED connection
  function ondata(packet) {
    // ignore keep alive packets
    if (packet === true) return;

    // TODO: use a better mechanism. this designates the keeping packet
    // if (Array.isArray(packet)) {
    if (packet && packet.$masterless && packet.$masterless.hasOwnProperty('keep')) {
        // node.keeping = packet[0];
        node.keeping = packet.$masterless.keep;

      // if neither end wants to keep the connection, kill it to save resources
      if (!self._keep[node.id] && !node.keeping) {
        self._debug(self.id, 'discarding unwanted connection to', node.id);

        // don't need to clean up the node state because it will be garbage
        // collected

        // remove the node entry, as we will completely disconnect
        delete self._nodes[node.id];

        // unbind event listeners, and add noop error handlers so any issues
        // with the socket don't poison the event loop
        unbind();

        // end the connection
        conn.conn.end();

        // emit the disconnect event
        self.emit('disconnect', node.id);
      }
    } else {
      self._debug(self.id, 'new message from', node.id);

      // emit a message event with the node that sent the packet and the
      // application data object
      self.emit('message', node.id, packet);
    }
  }

  // handle the end of the connection, expected or not
  function onend() {
    // if the disconnect method was called, then it's expected, so don't bother
    // logging that the connection ended
    if (!conn.closing) {
      self._debug(self.id, 'connection to', node.id, 'closed');
    }

    // if we care about the connection, reconnect
    if (self._keep[node.id]) {
      // set the node's stream property to null because the stream should only
      // exist if there exists a stable connection to that node
      node.stream = null;

      // clear the state for the appropriate direction
      if (node.remoteState === 2) {
        node.remoteState = 0;
        node.remotePriority = 0;
      } else {
        node.localState = 0;
        node.localPriority = 0;
      }

      // unbind event listeners, and add noop error handlers so any issues
      // with the socket don't poison the event loop
      unbind();

      // reconnect because there's a reasonable chance we'll succeed: we've
      // already shaken hands with the node once, it may still exist
      reconnect(self, node, conn);
    } else {
      // remove the node entry, as we will completely disconnect
      delete self._nodes[node.id];

      // unbind event listeners, and add noop error handlers so any issues
      // with the socket don't poison the event loop
      unbind();

      // emit the disconnect event
      self.emit('disconnect', node.id);
    }
  }
}

module.exports = Server;
