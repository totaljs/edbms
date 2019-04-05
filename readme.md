# Node ElasticSearch Database Management System

[![Professional Support](https://www.totaljs.com/img/badge-support.svg)](https://www.totaljs.com/support/) [![Chat with contributors](https://www.totaljs.com/img/badge-chat.svg)](https://messenger.totaljs.com) [![NPM version][npm-version-image]][npm-url] [![NPM downloads][npm-downloads-image]][npm-url] [![MIT License][license-image]][license-url]

- installation `$ npm install edbms`

## Initialization

```javascript
const ElasticDB = require('elasticdb');
ElasticDB.url('http://localhost:9200');
```

## Examples

```
var db = EDB();

// Available methods: POST (default), GET, PUT, DELETE
var builder = db.exec('GET /YOUR-INDEX/TYPE/_search');

builder.add('"query":{"term":{"id":$id}}', { id: 5 });
builder.callback(function(err, response) {
	// ...
});
```
