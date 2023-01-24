var express = require('express');
var app = express();
app.get('/', function (req, res) {
  res.send('Hello World!');
});

app.get('/stat/vehicle', function (req, res) {
  res.send('Hello World!');
});

app.get('/stat/sensor', function (req, res) {
  res.send('Hello World!');
});

app.get('/stat/day', function (req, res) {
  res.send('Hello World!');
});

app.get('/stat/hour', function (req, res) {
  res.send('Hello World!');
});

app.listen(3000, function () {
  console.log('Example app listening on port 3000!');
});