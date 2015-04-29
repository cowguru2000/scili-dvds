var express = require('express');
var path = require('path');
var pg = require('pg');
var request = require('request');
var app = express();

var CACHE_AVAIL_SEC = 60 * 30; // availability cache expires after half hour

app.use(express.static('static'));

app.get('/', function(req, res) {
  res.sendFile(path.join(__dirname, './views', 'index.html'));
});

// load genre map
var genre_map = {}
pg.connect(process.env.DATABASE_URL, function(err, client, done) {
  client.query('SELECT id, title FROM genres;', function(err, res) {
    res.rows.forEach(function(item) {
      genre_map[item['id']] = item['title'];
    });
    done();
  });
});

function gen_params(arr) {
  return arr.map(function(_, idx) { return '$' + (idx + 1); });
}

app.get('/avail', function(req, res) {
  var recursive_avail_check = function(callnos, avail_dict, to_update) {
    if (callnos.length == 0) {
      res.send(avail_dict);
      pg.connect(process.env.DATABASE_URL, function(err, client, done) {
        if (err) {
          console.log(err);
          done();
          return;
        }
        var not_avail_cns = [];
        var avail_cns = [];
        to_update.forEach(function(cn) {
          var stat = avail_dict[cn];
          if (stat) {
            avail_cns.push(cn);
          } else {
            not_avail_cns.push(cn);
          }
        });
        if (avail_cns.length > 0) {
          client.query('UPDATE movies SET available = true, last_check = \'now\' WHERE josiah_callno IN('+gen_params(avail_cns).join(',')+');', avail_cns, function(err) { err && console.log('cache_avail', err); });
        }
        if (not_avail_cns.length > 0) {
          client.query('UPDATE movies SET available = false, last_check = \'now\' WHERE josiah_callno IN('+gen_params(not_avail_cns).join(',')+');', not_avail_cns, function(err) { err && console.log('cache_unavail', err); });
        }
        done();
      });
      return;
    }
    var first = callnos.pop();
    if (first in avail_dict) { // if the callno has a cached availability result already
      recursive_avail_check(callnos, avail_dict, to_update);
    } else { // the callno does not have a cached availabiity; poll it
      request('http://josiah.brown.edu/record=' + first, function(err, resp, body) {
        avail_dict[first] = body.match('AVAILABLE') != null || body.match('RECENTLY RETURNED') != null;
        to_update.push(first);
        recursive_avail_check(callnos, avail_dict, to_update);
      });
    }
  }
  // limit recursion to 40 (slice!)
  var callnos = req.query.callnos.filter(function(cn) { return !cn.match(/[^A-Za-z0-9]/); }).slice(0, 40);
  if (callnos.length > 0) {
    var now = new Date().getTime() / 1000;
    pg.connect(process.env.DATABASE_URL, function(err, client, done) {
      var master_dict = {};
      client.query('SELECT josiah_callno, extract(epoch from last_check) last_ts, available FROM movies WHERE josiah_callno IN(' + gen_params(callnos).join(',') + ');', callnos, function(err, res) {
        if (err) { console.log('avail', callnos, err); return; }
        res.rows.forEach(function(row) {
          if (row['last_ts'] > (now - CACHE_AVAIL_SEC)) {
            master_dict[row['josiah_callno']] = row['available'];
          }
        }); 
      });
      done();
      recursive_avail_check(callnos.slice(), master_dict, []);
    });
  }
});

app.get('/search', function(req, res) {
  pg.connect(process.env.DATABASE_URL, function(err, client, done) {
    if (err) {
      res.status(500).end();
      console.error(err);
    } else {
      var bindings = ['%' + req.query.q + '%'];
      if (!req.query.q.trim()) {
        var qstr = 'SELECT * FROM movies WHERE title ILIKE $1 AND rating IS NOT NULL AND available = true AND rating > 80 ORDER BY random() LIMIT 20;';
      } else {
        var qstr = 'SELECT movies.* FROM movies WHERE movies.id IN (SELECT movies.id FROM movies ' +
          'LEFT JOIN movies_genres ON movies_genres.movie_id = movies.id ' +
          'LEFT JOIN genres ON movies_genres.genre_id = genres.id ' +
          'WHERE movies.title ILIKE $1 OR LOWER(genres.title) = LOWER($2)) LIMIT 10;';
        bindings.push(req.query.q);
      }
      var limit = (req.query.q == '' ? 20 : 10);
      var query = client.query(qstr, bindings, function(err, result) {
        if (err) {
          res.status(500).end();
          console.log('query', err);
        } else if (result.rows.length == 0) {
          res.send({});
        } else {
          // load genres
          var movies_to_genres = {};
          
          var q2 = client.query('SELECT * FROM movies_genres WHERE movie_id IN('+(gen_params(result.rows).join(','))+');', result.rows.map(function(x) { return x['id']; }), function(err, res2, done2) {
            if (err) {
              console.log('q2', err);
              done2();
              return;
            }
            res2.rows.forEach(function(pair) {
              if (!(pair['movie_id'] in movies_to_genres)) {
                movies_to_genres[pair['movie_id']] = [];
              }
              movies_to_genres[pair['movie_id']].push(genre_map[pair['genre_id']]);
            });
          
            // stick genres into movie rows
            result.rows.forEach(function(row) { 
              row['genres'] = movies_to_genres[row['id']];
              delete row.available; // availability is provided in another endpoint
            });
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
