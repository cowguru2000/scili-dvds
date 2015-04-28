var express = require('express');
var app = express();

app.get('/', function(req, res) {
  res.send('Hello world!');
});

var server = app.listen(8000, function() {
  var addr = server.address();
  console.log("Listening at %s:%s", addr.address, addr.port);
});
