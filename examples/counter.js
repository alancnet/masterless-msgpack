var Server = require('..');

var left = new Server('e4663c9c3f9a89112afc48389a951e09');
var right = new Server('b073a8ee09e1daca57e9d54a5efe5684');

var start = 0;

left.on('listening', function(info) {
  right.connect(this.id, info);
});

right.on('connect', function(id) {
  start = Date.now();
  right.send(id, {type: 'counter', total: 0});
});

left.on('message', onmessage);
right.on('message', onmessage);

function onmessage(sender, packet) {
  if (packet.type === 'counter') {
    if (packet.total % 1000 === 0) {
       var diff = Date.now() - start;
       var rate = (packet.total / diff).toFixed(2);
       console.log('counter at', packet.total, 'in', diff + 'ms at', rate, 'inc/ms');
     }
    this.send(sender, {type: 'counter', total: packet.total + 1});
  } else {
    console.log('unknown packet', packet);
  }
}
