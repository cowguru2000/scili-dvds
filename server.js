// vim: set ts=2 sw=2 sts=2:
var express = require('express');
var path = require('path');
const { Client } = require('pg');
var request = require('request');
var app = express();

const client = new Client({
  connectionString: process.env.DATABASE_URL 
});
const CACHE_AVAIL_SEC = 60 * 30; // availability cache expires after half hour

app.use(express.static('static'));

app.get('/', function(req, res) {
  res.sendFile(path.join(__dirname, './views', 'index.html'));
});

console.log('Preloading data...');

/* Open a persistent connection */
client.connect()
  .catch(err => console.error("connection error", err.stack))

/* Load a map of genre IDs to genre titles. This is useful for translating
 * genre IDs to text without having to poll the DB. */
var genre_map = {}
client.query('SELECT id, title FROM genres;')
  .then(res => res.rows.forEach(item => {
    genre_map[item['id']] = item['title'];
  }))
  .then(() => {
    console.log('Genre map loaded.')
  })
  .catch(e => console.error(e.stack))

function gen_params(arr) {
  return arr.map(function(_, idx) { return '$' + (idx + 1); })
}

app.get('/avail', function(req, res) {
  if (!req.query.callnos || !req.query.callnos.length) {
    res.send({})
    return
  }

  /* We can't check the availability of several callnos iteratively
   * because HTTP requests for getting availability data off Josiah
   * are async and await doesn't exist until Node 8... :/ */
  var recursive_avail_check = function(callnos, avail_dict, to_update) {
    if (callnos.length == 0) {
      // Send the availability data to the client immediately.
      res.send(avail_dict)
      
      // Compile queries to update the DB cache with any availability
      // data we just got from Josiah.
      var not_avail_cns = [];
      var avail_cns = [];
      to_update.forEach(cn => {
        var stat = avail_dict[cn]
        if (stat) {
          avail_cns.push(cn)
        } else {
          not_avail_cns.push(cn)
        }
      })

      if (avail_cns.length > 0) {
        client.query('UPDATE movies SET available = true, last_check = \'now\' WHERE josiah_callno IN('+gen_params(avail_cns).join(',')+');', avail_cns)
         .catch(err => console.log('cache_avail', err))
      }
      if (not_avail_cns.length > 0) {
        client.query('UPDATE movies SET available = false, last_check = \'now\' WHERE josiah_callno IN('+gen_params(not_avail_cns).join(',')+');', not_avail_cns)
         .catch(err => console.log('cache_unavail', err));
      }
      return
    }
    
    /* Pop the first call number off the list and, if necessary, go poll
     * its availability from Josiah. */
    var first = callnos.pop();
    if (first in avail_dict) { // if the callno has a cached availability result already
      recursive_avail_check(callnos, avail_dict, to_update);
    } else { // the callno does not have a cached availabiity; poll it
      request('http://josiah.brown.edu/record=' + first, (err, resp, body) => {
        avail_dict[first] = body.match('AVAILABLE') != null || body.match('RECENTLY RETURNED') != null;
        to_update.push(first)
        recursive_avail_check(callnos, avail_dict, to_update)
      });
    }
  }

  // limit recursion to 40 (slice!)
  var callnos = req.query.callnos
    .filter(cn => !cn.match(/[^A-Za-z0-9]/))
    .slice(0, 40)
  
  if (callnos.length > 0) {
    var now = new Date().getTime() / 1000;
    var avail_map = {}
    client.query('SELECT josiah_callno, extract(epoch from last_check) last_ts, available FROM movies WHERE josiah_callno IN(' + gen_params(callnos).join(',') + ');', callnos)
      .then(res => {
        res.rows.forEach(function(row) {
          /* If the cached data for an item isn't expired, use that */
          if (row['last_ts'] > (now - CACHE_AVAIL_SEC)) {
            avail_map[row['josiah_callno']] = row['available']
          }
        })
        recursive_avail_check(callnos.slice(), avail_map, [])
      })
      .catch(e => console.error(e.stack))
  }
});

app.get('/search', function(req, res) {
  var now = new Date().getTime() / 1000;
  var bindings = ['%' + req.query.q + '%'];
  if (!req.query.q.trim()) {
    var qstr = 'SELECT * FROM movies WHERE title ILIKE $1 AND rating IS NOT NULL AND available = true AND rating > 80 ORDER BY random() LIMIT 20;';
  } else {
    var qstr = 'SELECT movies.runtime, movies.director, movies.available, movies.josiah_callno, movies.plot_short, movies.rating, movies.title, movies.id, extract(epoch from last_check) last_ts FROM movies WHERE movies.id IN (SELECT movies.id FROM movies ' +
      'LEFT JOIN movies_genres ON movies_genres.movie_id = movies.id ' +
      'LEFT JOIN genres ON movies_genres.genre_id = genres.id ' +
      'WHERE movies.title ILIKE $1 OR LOWER(genres.title) = LOWER($2) OR movies.director ILIKE $1) LIMIT 10;';
    bindings.push(req.query.q);
  }
  var limit = (req.query.q == '' ? 20 : 10);
  client.query(qstr, bindings)
    .then(result => {
      if (result.rows.length == 0) {
        res.send({})
        return
      }

      // Load the genres of the requested movies
      var movies_to_genres = {};
      
      client.query('SELECT * FROM movies_genres WHERE movie_id IN('+(gen_params(result.rows).join(','))+');', result.rows.map(function(x) { return x['id']; }))
        .then(res2 => {
          res2.rows.forEach(pair => {
            if (!(pair['movie_id'] in movies_to_genres)) {
              movies_to_genres[pair['movie_id']] = [];
            }
            movies_to_genres[pair['movie_id']].push(genre_map[pair['genre_id']]);
          });
        })
        .catch(e => console.error('error finding genres of movies', e.stack))
        .then(() => {
          // stick genres into movies in the response
          result.rows.forEach(row => { 
            row['genres'] = movies_to_genres[row['id']];
            if (now > row.last_ts + CACHE_AVAIL_SEC) {
              delete row.available; // cached avail. invalid, availability is provided in another endpoint
            }
          });
          res.send({'results': result.rows});
        })
    })
    .catch(e => { /* err in DB search for movies */
      res.status(500).end();
      console.error('error searching for movies', e.stack)
    })
})

var server = app.listen(process.env.PORT || 3000, function() {
  var addr = server.address();
  console.log("Listening at %s:%s", addr.address, addr.port);
});
