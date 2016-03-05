masterless
==========

[![Build Status](https://travis-ci.org/globesherpa/masterless.png)](https://travis-ci.org/globesherpa/masterless)

A fancy tcp client and server that acts as neither, and abstracts out connection deduplication between identical nodes.

Install
=======

```sh
$ npm install masterless
```

Test
====

Run any of the following:

```sh
$ mocha
$ npm test
$ make test
```

_Note:_ remember to `npm install` to get those yummy dev dependencies!

API
===

```js
var Server = require('masterless');

var left = new Server('e4663c9c3f9a89112afc48389a951e09');
var right = new Server('b073a8ee09e1daca57e9d54a5efe5684');

left.on('listening', function(info) {
  right.connect(info);
});

right.on('connect', function(id) {
  right.send(id, {type: 'counter', total: 0});
});

left.on('message', onmessage);
right.on('message', onmessage);

function onmessage(sender, packet) {
  if (packet.type === 'counter') {
    packet.total % 100 === 0 && console.log('counter at', packet.total);
    this.send(sender, {type: 'counter', total: packet.total + 1});
  } else {
    console.log('unknown packet', packet);
  }
}
```

For more examples, see [examples][].

Server(id, [options])
---------------------

Construct a new server object, with an id unique to context in which the server is used. Optionally specify a `port` to listen on.

```js
var server = new Server('67d2cb3db178bc5ca7d9f7157e3119aa');
var serverWithPort = new Server('81358297b83f2d4d6cfa58ba5aff6180', {port: 23412});
```

server.connect(transport)
-----------------------------

Connect to a remote node via a transport uri. Automatically sets the node as a keep node.

```js
server.connect('tcp://192.168.1.64:2435');
server.connect(serverWithPort.info());
```

server.disconnect(id)
---------------------

Disconnect from a remote via a node id.

```js
server.disconnect('81358297b83f2d4d6cfa58ba5aff6180');
```

server.keep(id, [true|false])
-----------------------------

Reconnects to the specified node if the connection dies.

```js
server.keep('81358297b83f2d4d6cfa58ba5aff6180');
```

server.info()
-------------

Returns a formatted transport uri. The result of this function is included in the `listening` event. The `info` method returns `null` if the server has yet to initialize.

```js
server.info(); // => 'tcp://192.168.1.64:35649'
```

server.close([callback])
------------------------

Ends all connections and closes the server.

```js
server.close(function() {
  // server has been closed
});
```

server.send(id, packet)
-----------------------

Sends a packet to one or more nodes. Returns false if unable to send for a single destination.

```js
server.send('81358297b83f2d4d6cfa58ba5aff6180', {some: 'data'});
server.send([left.id, right.id], {some: 'other', random: 'data'});

server.send('not-a-valid-node', {not: 'sent'}); // returns false
```

Features
========

Connection Deduplication
------------------------

The following example will open two connections, then close exactly one as the handshake completes.

```js
var left = new Server(...);
var right = new Server(...);

left.connect(right.info());
right.connect(left.info());
```

Multiple Dispatch
-----------------

Send messages to multiple nodes with low overhead (doesn't reencode the packet).

```js
server.send([left.id, right.id], {multiple: 'dispatch'});
```

Limited Reconnection
--------------------

Use the `keep` method to specify a connection is locally important, and the server will attempt to reconnect once between disconnecting.

```js
var left = new Server(...);
var right = new Server(...);

// both nodes will unilaterally try to reconnect
right.keep(left.id);
left.connect(right.info());

left.once('connect', function() {
  // causes a reconnect
  right.disconnect(left.id);
});
```

Caveats
=======

- The wire protocol is not yet stable--it is a simple protocol, but could be far more lightweight.

License
=======

> The MIT License (MIT)

> Copyright &copy; 2014 GlobeSherpa

> Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

> The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

> THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

[examples]: https://github.com/globesherpa/masterless/tree/master/examples "Examples"
