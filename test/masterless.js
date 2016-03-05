var Server = require('..');
var crypto = require('crypto');

var Emitter = require('events').EventEmitter;

function uid() {
  return crypto.randomBytes(16).toString('hex');
}

function after(count, skip, next) {
  if (typeof skip === 'function') {
    next = skip;
    skip = false;
  }
  return function(err) {
    (!skip && err) ? next(err) : (--count || next(null));
  };
}

function afterAll(name) {
  var i = 1, a = arguments, n = a.length, next = after(--n - 1, true, a[n]);
  while (i < n) a[i++].once(name, next);
}

function bounce(count, left, right, callback) {
  var nextLeft = 0, nextRight = 0;
  var lastLeft = -1, lastRight = -1;

  var emitter = new Emitter();

  expect(left.send(right.id, {clock: nextLeft++, test: true})).to.equal(true);

  left.on('message', leftBouncer);
  right.on('message', rightBouncer);

  function leftBouncer(sender, packet) {
    expect(sender).to.equal(right.id);
    expect(packet.beep).to.equal(true);

    emitter.emit('left');

    if (lastLeft < packet.clock) {
      expect(++lastLeft).to.equal(packet.clock);
      if (nextLeft < count) {
        this.send(right.id, {clock: nextLeft++, test: true});
      } else {
        cleanup();
      }
    }
  }

  function rightBouncer(sender, packet) {
    expect(sender).to.equal(left.id);
    expect(packet.test).to.equal(true);

    emitter.emit('right');

    if (lastRight < packet.clock) {
      expect(++lastRight).to.equal(packet.clock);
      if (nextRight < count) {
        this.send(left.id, {clock: nextRight++, beep: true});
      } else {
        cleanup();
      }
    }
  }

  function cleanup() {
    left.removeListener('message', leftBouncer);
    right.removeListener('message', rightBouncer);

    callback();
  }

  return emitter;
}

describe('server', function() {
  it('should support duplex data', function(done) {
    var a = uid(), b = uid();

    var left = new Server(a, {host: 'localhost'});
    var right = new Server(b, {host: 'localhost'});

    afterAll('listening', left, right, function() {
      right.connect(left.info());
      right.keep(left.id);
    });

    right.on('connect', function() {
      right.send(a, {beep: true});
    });

    left.on('message', function(sender, packet) {
      expect(sender).to.equal(b);
      expect(packet).to.eql({beep: true});
      packet.boop = false;
      left.send(b, packet);
    });

    right.on('message', function(sender, packet) {
      expect(sender).to.equal(a);
      expect(packet).to.eql({beep: true, boop: false});
      left.close();
      right.close();
      done();
    });
  });

  it('should support multicast data', function(done) {
    var a = uid(), b = uid(), c = uid();

    var top = new Server(a, {host: 'localhost'});
    var mid = new Server(b, {host: 'localhost'});
    var bot = new Server(c, {host: 'localhost'});

    afterAll('listening', top, mid, bot, function() {
      mid.connect(top.info());
      bot.connect(top.info());
      mid.keep(a);
      bot.keep(a);
    });

    var nodes = Object.create(null);
    nodes[b] = true;
    nodes[c] = true;
    top.on('connect', function onconnect(id) {
      if (!nodes[id])
        return done(new Error('unexpected node'));
      delete nodes[id];
      // if still waiting, return
      for (var key in nodes)
        return;
      top.removeListener('connect', onconnect);
      top.send([b, c], {beep: true});
    });

    mid.once('message', function(sender, packet) {
      expect(sender).to.equal(a);
      expect(packet).to.eql({beep: true});
    });

    bot.once('message', function(sender, packet) {
      expect(sender).to.equal(a);
      expect(packet).to.eql({beep: true});
    });

    afterAll('message', mid, bot, done);
  });

  it('should support renegotiation', function(done) {
    var a = uid(), b = uid();

    var left = new Server(a, {host: 'localhost'});
    var right = new Server(b, {host: 'localhost'});

    var CYCLES = 10;

    afterAll('listening', left, right, function() {
      right.connect(left.info());
      right.keep(left.id);
    });

    left.on('connect', function(id) {
      expect(id).to.equal(b);
      left.connect(right.info());
      bounce(CYCLES, left, right, finish);
    });

    function finish(err) {
      left.close();
      right.close();
      done(err);
    }
  });

  it('should deduplicate many connections', function(done) {
    var a = uid(), b = uid();

    var left = new Server(a, {host: 'localhost'});
    var right = new Server(b, {host: 'localhost'});

    var CYCLES = 10;

    afterAll('listening', left, right, function() {
      right.connect(left.info());
      right.connect(left.info());
      right.connect(left.info());
      left.connect(right.info());
      left.connect(right.info());
      left.connect(right.info());
      right.keep(left.id);
      left.keep(right.id);
    });

    left.on('connect', onconnect);
    right.on('connect', onconnect);

    function onconnect(id) {
      left.removeListener('connect', onconnect);
      right.removeListener('connect', onconnect);

      expect(id).to.not.equal(this.id);

      bounce(CYCLES, left, right, finish);
    }

    function finish(err) {
      left.close();
      right.close();
      done(err);
    }
  });

  it('should automatically reconnect', function(done) {
    var a = uid(), b = uid();

    var left = new Server(a, {host: 'localhost'});
    var right = new Server(b, {host: 'localhost'});

    afterAll('listening', left, right, function() {
      right.connect(left.info());
      right.keep(left.id);
    });

    var initial = true;

    left.on('connect', function(id) {
      expect(id).to.equal(b);
      if (initial) {
        initial = false;
        left.disconnect(b);
      } else {
        left.send(b, {hello: 'world'});
      }
    });

    var firstRight = true;
    right.on('connect', function(id) {
      expect(id).to.equal(left.id);
      expect(firstRight).to.be.ok;
      firstRight = false;
    });

    right.on('message', function(sender, packet) {
      expect(sender).to.equal(a);
      expect(packet).to.eql({hello: 'world'});
      left.close();
      right.close();
      done();
    });
  });

  it('should reliably reconnect', function(done) {
    var a = uid(), b = uid();

    var left = new Server(a, {host: 'localhost'});
    var right = new Server(b, {host: 'localhost'});

    var connectStart = Date.now();

    afterAll('listening', left, right, function() {
      right.connect(left.info());
      right.keep(left.id);
    });

    afterAll('connect', left, right, function() {
      var index = 0;
      bounce(100, left, right, function(err) {
        left.close();
        right.close();
        done(err);
      }).on('left', function on() {
        if (++index === 50) {
          //console.log(right.id, 'reconnecting to', left.id);
          left.connect(right.info());
          this.removeListener('left', on);
        }
      });
    });
  });

  it('should discard unwanted connections', function(done) {
    var a = uid(), b = uid();

    var left = new Server(a, {host: 'localhost'});
    var right = new Server(b, {host: 'localhost'});

    var discard = false;

    afterAll('listening', left, right, function() {
      right.connect(left.info());
      right.keep(left.id);
    });

    left.on('connect', function() {
      setTimeout(function() {
        discard = true;
        right.keep(left.id, false);
      }, 10);
    });

    var next = after(2, done);

    left.on('disconnect', function() {
      expect(discard).to.be.ok;
      next();
    });

    right.on('disconnect', function() {
      expect(discard).to.be.ok;
      next();
    });
  });

  it('should detect disconnected nodes', function(done) {
    var a = uid(), b = uid();

    var left = new Server(a, {host: 'localhost'});
    var right = new Server(b, {host: 'localhost'});

    afterAll('listening', left, right, function() {
      right.connect(left.info());
      left.keep(right.id);
    });

    var timeout = null, finished = false;
    afterAll('connect', left, right, function() {
      timeout = setTimeout(function() {
        timeout = null;
        if (finished) return;
        finished = true;
        done(new Error('unable to detect disconnected node'));
      }, 50);

      right.close();
    });

    left.once('disconnect', function(id) {
      if (finished) return;
      expect(timeout).to.be.ok;
      expect(id).to.equal(right.id);
      clearTimeout(timeout);
      timeout = null;
      finished = true;
      done();
    });
  });
});
