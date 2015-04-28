var express = require('express');
var path = require('path');
var pg = require('pg');
var app = express();

app.use(express.static('static'));

app.get('/', function(req, res) {
  res.sendFile(path.join(__dirname, './views', 'index.html'));
});

// load genre map
var genre_map = {}
pg.connect(process.env.DATABASE_URL, function(err, client) {
  client.query('SELECT id, title FROM genres;', function(err, res) {
    res.rows.forEach(function(item) {
      genre_map[item['id']] = item['title'];
    });
  });
});

app.get('/search', function(req, res) {
  pg.connect(process.env.DATABASE_URL, function(err, client, done) {
    if (err) {
      res.status(500).end();
      console.error(err);
    } else {
      var query = client.query('SELECT * FROM movies WHERE title ILIKE $1 LIMIT 10;', ['%' + req.query.q + '%'], function(err, result) {
        if (err) {
          res.status(500).end();
        } else if (result.rows.length == 0) {
          res.send({});
        } else {
          // load genres
          var movies_to_genres = {};
          var params = result.rows.map(function(val, idx) { return '$' + (idx + 1); });
          
          var q2 = client.query('SELECT * FROM movies_genres WHERE movie_id IN('+(params.join(','))+');', result.rows.map(function(x) { return x['id']; }), function(err, res2) {
            if (err) {
              console.log(err);
              return;
            }
            res2.rows.forEach(function(pair) {
              if (!(pair['movie_id'] in movies_to_genres)) {
                movies_to_genres[pair['movie_id']] = [];
              }
              movies_to_genres[pair['movie_id']].push(genre_map[pair['genre_id']]);
            });
          
            // stick genres into movie rows
            result.rows.forEach(function(row) { row['genres'] = movies_to_genres[row['id']]; });
            if (err) {
              res.status(500).end();
              console.error(err);
            } else {
              res.send({'results': result.rows});
            }
          });
        }
      });
    }
    done();
  });
});

var server = app.listen(process.env.PORT || 3000, function() {
  var addr = server.address();
  console.log("Listening at %s:%s", addr.address, addr.port);
});
