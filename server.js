var express = require('express');
var path = require('path');
var app = express();

app.use(express.static('static'));

app.get('/', function(req, res) {
  res.sendFile(path.join(__dirname, './views', 'index.html'));
});

app.get('/search', function(req, res) {
  console.log(req.query);
  res.send({'results': [
    {'title': 'A Thing', 'rating': 80, 'available': true, 'plot': 'asdf', 'genres': ['Comedy', 'Action', 'Romance']},
    {'title': 'Not Available', 'rating': 34, 'available': false, 'genres': ['Adventure', 'Thriller']}
  ]});
});

var server = app.listen(process.env.PORT || 3000, function() {
  var addr = server.address();
  console.log("Listening at %s:%s", addr.address, addr.port);
});
