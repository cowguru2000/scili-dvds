var express = require('express');
var path = require('path');
var pg = require('pg');
var app = express();

app.use(express.static('static'));

app.get('/', function(req, res) {
  res.sendFile(path.join(__dirname, './views', 'index.html'));
});

app.get('/search', function(req, res) {
  pg.connect(process.env.DATABASE_URL, function(err, client) {
    console.log(err);
    var query = client.query('SELECT * FROM movies WHERE title ILIKE $1 LIMIT 10;', ['%' + req.query.q + '%'], function(err, result) {
      res.send({'results': result.rows});
    });
  });
});

var server = app.listen(process.env.PORT || 3000, function() {
  var addr = server.address();
  console.log("Listening at %s:%s", addr.address, addr.port);
});
