// bidirectional linked list optimized for push, each, and remove pushed

function Entry(parent, prev, next, value) {
  this.parent = parent;
  this.v = parent._v;

  this.prev = prev;
  this.next = next;
  this.value = value;
}

function Linked() {
  this._v = 0;
  this._head = null;
  this.length = 0;
}

Linked.prototype.push = function(value) {
  var entry = new Entry(this, null, this._head, value);

  if (this._head) {
    this._head.prev = entry;
  }

  this._head = entry;

  this.length++;

  return entry;
};

Linked.prototype.remove = function(entry) {
  if (entry.parent === this && entry.v === this._v) {
    if (entry.next) entry.next.prev = entry.prev;
    if (this._head === entry) this._head = entry.next;
    else entry.prev.next = entry.next;
    entry.v = null;

    this.length--;

    return true;
  }

  return false;
};

Linked.prototype.each = function(fn, me) {
  for (var entry = this._head; entry; entry = entry.next) {
    fn.call(me || this, entry.value);
  }
  return this;
};

Linked.prototype.empty = function() {
  this._head = null;
  this._v++;
  this.length = 0;
  return this;
};

module.exports = Linked;
