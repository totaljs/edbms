# Node ElasticSearch Database Management System

[![Professional Support](https://www.totaljs.com/img/badge-support.svg)](https://www.totaljs.com/support/) [![Chat with contributors](https://www.totaljs.com/img/badge-chat.svg)](https://messenger.totaljs.com)

- installation `$ npm install edbms`

## Initialization

```javascript
const EDBMS = require('edbms');
EDBMS.url('http://localhost:9200');
```

## Examples

```javascript
var db = EDB();

// Listing - Performs index/_search
var builder = db.list('index');
builder.scope('query.bool.must[]');
builder.push('match', { title: 'my_search_phrase' });
builder.callback(function(err, response) {
    // ...
});

// Read single document
db.read('index', 'type', '_id').callback(function(err, response) {
    // ...
});

// Create a new document and manual refresh
db.insert('index', 'type', model).refresh().callback(function(err, response) {
    // ...
});

// Update document
db.update('index', 'type', '_id', model).callback(function(err, response) {
    // ...
});

// Partial update
db.modify('index', '_id', model).callback(function(err, response) {
    // ...
});

// Delete document
db.delete('index', 'type', 'id').callback(function(err, response) {
    // ...
});

// Delete by query
var builder = db.delete('index');
builder.scope('query.bool.must[]');
builder.push('term', { userid: 5 });
builder.callback(function(err, response) {
    // ...
});

// Custom query
// Available methods: POST (default), GET, PUT, DELETE
var builder = db.exec('GET /YOUR-INDEX/TYPE/_search');
builder.scope('query.bool.must[]');
builder.push('term', { userid: 5 });
builder.callback(function(err, response) {
    // ...
});

// Refresh index
db.refresh('index').callback(function(err, response) {
    console.log(err, response);
});

// Count of documents
var builder = db.count('index');
builder.scope('query.bool.must[]');
builder.push('term', { userid: 5 });
builder.callback(function(err, response) {
    // ...
});
```
