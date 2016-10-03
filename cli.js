"use strict";
const nano = require('nanomsg');
const msgpack = require('msgpack-lite');
let req = nano.socket('req');
let addr = 'tcp://0.0.0.0:4040';
req.connect(addr);
let params = {
    ctx: { domain: 'mobile', ip: 'localhost', uid: 'I0000000000000000250000000000041' },
    fun: 'setDriverInfo',
    args: []
};
req.send(msgpack.encode(params));
req.on('data', function (msg) {
    console.log(msgpack.decode(msg));
    req.shutdown(addr);
});
