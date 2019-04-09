require('total.js');

var DB = {};
var pending = 0;

function ElasticDB(url, eb) {

	var t = this;
	t.$remap = true;
	// t.$raw = false;
	t.$errors = eb ? eb : new ErrorBuilder();
	t.$commands = [];

	// t.$timeout;
	// t.$output;
	// t.$outputlast;
	// t.output;
	// t.$callback;

	t.url = DB[url || 'default'] || '';
	t.response = {};
	t.tmp = '';

	t.$request = function() {
		t.$timeout = null;
		t.$exec();
	};
}

ElasticDB.clear = function() {
	DB = {};
};

ElasticDB.use = function() {
	// Total.js framework
	if (global.F) {
		global.F.database = function(err) {
			if (typeof(err) === 'function') {
				var db = new ElasticDB();
				err.call(db, db);
			} else
				return new ElasticDB(err);
		};
	}
	return ElasticDB;
};

ElasticDB.url = function(name, url) {

	if (url == null) {
		url = name;
		name = 'default';
	}

	if (url[url.length - 1] === '/')
		url = url.substring(0, url.length - 1);

	DB[name] = url;
	return ElasticDB;
};

ElasticDB.index = function(name, indexname, callback) {

	if (indexname == null || typeof(indexname) === 'function') {
		callback = indexname;
		indexname = name;
		name = 'default';
	}

	pending++;

	var url = DB[name];
	RESTBuilder.HEAD(url + '/' + indexname).exec(function(err, response) {

		pending--;

		if (err)
			throw err;

		if (!response) {
			pending++;
			callback(function(model, callback) {
				pending--;
				RESTBuilder.PUT(url + '/' + indexname, model).exec(callback || ERROR('Create index "' + url + '/' + indexname + '"'));
			});
		}
	});
};

const ED = ElasticDB.prototype;
const TMP = {};

TMP.replace = function(text) {
	return JSON.stringify(TMP.value[text.substring(1)]);
};

ED.callback = function(fn) {
	this.$callback = fn;
	return this;
};

ED.must = function(err, reverse) {
	var self = this;
	self.$commands.push({ type: 'must', value: err || 'unhandled exception', reverse: reverse });
	return self;
};

ED.exec = function(name, index, data) {

	if (typeof(index) === 'object') {
		data = index;
		index = null;
	}

	if (index == null)
		index = name;

	var self = this;
	var builder = new ElasticQuery();
	var beg = index.indexOf(' ');

	if (beg !== -1) {
		var method = index.substring(0, beg);
		builder.options.method = method;
		index = index.substring(beg + 1).trim();
	}

	if (data)
		builder.options.body = data;

	builder.$commandindex = self.$commands.push({ name: name, index: index, builder: builder }) - 1;
	self.$timeout && clearImmediate(self.$timeout);
	self.$timeout = setImmediate(self.$request);
	return builder;
};

ED.output = function(val) {
	this.$output = val;
	return this;
};

ED.$validate = function(cmd) {
	var type = typeof(cmd.value);
	var stop = false;
	switch (type) {
		case 'function':
			var val = cmd.value(self.output, self.$output);
			if (typeof(val) === 'string') {
				stop = true;
				self.$errors.push(val);
			}
			break;
		case 'string':
			if (self.output instanceof Array) {
				if (cmd.reverse) {
					if (self.output.length) {
						self.$errors.push(cmd.value);
						stop = true;
					}
				} else {
					if (!self.output.length) {
						self.$errors.push(cmd.value);
						stop = true;
					}
				}
			} else {
				if (cmd.reverse) {
					if (self.output) {
						self.$errors.push(cmd.value);
						stop = true;
					}
				} else {
					if (!self.output) {
						self.$errors.push(cmd.value);
						stop = true;
					}
				}
			}
			break;
	}

	if (stop) {
		self.$commands = [];
		self.$callback && self.$callback(self.$errors.length ? self.error : null, self.output);
	} else {
		self.$timeout && clearImmediate(self.$timeout);
		self.$timeout = setImmediate(self.$request);
	}
};

ED.$exec = function() {

	var self = this;

	// Pending for indexes...
	if (pending > 0) {
		setTimeout(self.$request, 500);
		return self;
	}

	var cmd = self.$commands.shift();

	if (cmd == null) {
		// end
		// callback
		self.$callback && self.$callback(self.$errors.length ? self.error : null, self.output);
		return self;
	}

	var c = cmd.index[0];

	if (c === '/')
		cmd.index = cmd.index.substring(1);
	else if (c === '[') {
		var beg = cmd.index.indexOf(']');
		cmd.index = DB[cmd.index.substring(1, beg)] + cmd.index.substring(beg + 1);
		self.url = '';
	}

	var builder = cmd.builder;
	var rb = RESTBuilder.url((self.url ? (self.url + '/') : '') + cmd.index);

	// @TODO: it is needed?
	if (builder.options.method !== 'GET') {
		var q = builder.create();
		rb.json(q);
	}

	rb.$method = builder.options.method;
	rb.$keepalive = true;
	rb.exec(function(err, response) {

		if (self.$raw) {
			self.output = self.response[cmd.name] = response;
			builder.options.data && builder.options.data(err, self.response[cmd.name]);
			builder.options.callback && builder.options.callback(err, self.response[cmd.name]);
			self.$timeout && clearImmediate(self.$timeout);
			self.$timeout = setImmediate(self.$request);
			return;
		}

		if (response.error) {
			var err = response.error.type ? (response.error.type + ': ' + response.error.reason) : response.error;
			self.$errors.push(err);
			builder.options.fail && builder.options.fail(err);
			builder.options.callback && builder.options.callback(err);
			self.$timeout && clearImmediate(self.$timeout);
			self.$timeout = setImmediate(self.$request);
			return;
		}

		if (response.result) {
			self.output = self.response[cmd.name] = { id: response._id, status: response.result };
			builder.options.data && builder.options.data(err, self.response[cmd.name]);
			builder.options.callback && builder.options.callback(err, self.response[cmd.name]);
			self.$timeout && clearImmediate(self.$timeout);
			self.$timeout = setImmediate(self.$request);
			return;
		}

		response = response.hits;

		var item;
		var opt = builder.options;
		if (opt.first) {
			if (response.total) {
				item = response.hits[0];
				if (self.$remap) {
					item._source.id = item._id;
					item = item._source;
				}
				self.output = self.response[cmd.name] = item;
			} else
				self.output = self.response[cmd.name] = null;
		} else {
			var output = {};
			output.score = response.max_score;
			output.count = response.total;
			output.pages = output.count ? Math.ceil(output.count / opt.take) : 0;
			output.page = opt.skip ? Math.ceil(opt.skip / opt.take) : 1;
			output.items = response.hits;
			if (self.$remap) {
				for (var i = 0; i < output.items.length; i++) {
					item = output.items[i];
					item._source.id = item._id;
					output.items[i] = item._source;
				}
			}
			self.output = self.response[cmd.name] = output;
		}

		builder.options.data && builder.options.data(err, self.response[cmd.name]);
		builder.options.callback && builder.options.callback(err, self.response[cmd.name]);
		self.$timeout && clearImmediate(self.$timeout);
		self.$timeout = setImmediate(self.$request);
	});
};

function ElasticQuery() {
	this.mapper = {};
	this.items = [];
	this.options = { method: 'post' };
	this.tmp = '';
}

const EP = ElasticQuery.prototype;

EP.fields = function(fields) {

	var self = this;

	if (!self.options.fields)
		self.options.fields = [];

	var arr = arguments;

	if (arr.length === 1 && fields.indexOf(',') !== -1)
		arr = fields.split(',');

	for (var i = 0; i < arr.length; i++)
		self.options.fields.push((arr[i][0] === ' ' ? arr[i].trim() : arr[i]));

	return self;
};

EP.scope = function(path) {
	var self = this;
	self.$scope = path || '';
	return self;
};

EP.push = function(path, value) {

	var self = this;

	if (self.$scope)
		path = self.$scope + (path ? '.' : '') + path;

	if (value === undefined)
		value = NOOP;

	if (self.mapper[path])
		self.mapper[path].push(value);
	else
		self.mapper[path] = [value];

	return self;
};

EP.sort = function(name, type) {
	var self = this;
	var opt = self.options;
	var item;

	if (type) {
		item = {};
		item[name] = type;
	} else
		item = name;

	if (opt.sort)
		opt.sort.push(item);
	else
		opt.sort = [item];

	return self;
};

EP.skip = function(value) {
	var self = this;
	self.options.skip = value;
	return self;
};

EP.take = function(value) {
	var self = this;
	self.options.take = value;
	return self;
};

EP.page = function(page, limit) {
	var self = this;
	if (limit)
		self.options.take = limit;
	self.options.skip = page * self.options.take;
	return self;
};

EP.paginate = function(page, limit, maxlimit) {

	var self = this;
	var limit2 = +(limit || 0);
	var page2 = (+(page || 0)) - 1;

	if (page2 < 0)
		page2 = 0;

	if (maxlimit && limit2 > maxlimit)
		limit2 = maxlimit;

	if (!limit2)
		limit2 = maxlimit;

	self.options.skip = page2 * limit2;
	self.options.take = limit2;
	return self;
};

EP.first = function() {
	var self = this;
	self.options.first = true;
	self.options.take = 1;
	return self;
};

EP.create = function() {
	var self = this;
	var opt = self.options;

	if (opt.body)
		return opt.body;

	var obj = {};
	var keys = Object.keys(self.mapper);
	var tmp;

	if (opt.sort)
		obj.sort = opt.sort;

	opt.take && (obj.size = opt.take);
	opt.skip && (obj.from = opt.skip);
	opt.fields && opt.fields.length && (obj._source = opt.fields);

	for (var i = 0; i < keys.length; i++) {

		var key = keys[i];
		var cur, arr, p, isarr, isend;

		arr = key.split('.');
		cur = obj;

		for (var j = 0; j < arr.length; j++) {

			p = arr[j];
			isarr = p[p.length - 1] === ']';
			isend = j === arr.length - 1;

			if (isarr)
				p = p.substring(0, p.length - 2);

			if (cur instanceof Array) {
				// must be ended
				if (!isend)
					throw new Error('Not allowed path for "' + key + '".');

			} else if (cur[p] === undefined)
				cur[p] = isarr ? [] : {};

			if (!isend)
				cur = cur[p];
		}

		var items = self.mapper[key];
		for (var j = 0; j < items.length; j++) {
			var item = items[j];
			if (item != NOOP) {
				if (cur[p] instanceof Array) {
					cur[p].push(item);
				} else {
					if (cur instanceof Array) {
						tmp = {};
						tmp[p] = item;
						cur.push(tmp);
					} else
						cur[p] = item;
				}
			}
		}
	}

	var body = JSON.stringify(obj);

	if (opt.debug)
		console.log('--->', body);

	return body;
};

EP.debug = function() {
	this.options.debug = true;
	return this;
};

EP.callback = function(callback) {
	this.options.callback = callback;
	return this;
};

EP.data = function(callback) {
	this.options.data = callback;
	return this;
};

EP.fail = function(callback) {
	this.options.fail = callback;
	return this;
};

exports.ElasticDB = ElasticDB;
exports.url = ElasticDB.url;
exports.clear = ElasticDB.clear;
exports.use = ElasticDB.use;
exports.index = ElasticDB.index;

global.EDB = function(name, err) {
	if (name && typeof(name) === 'object') {
		err = name;
		name = null;
	}
	return new ElasticDB(name, err);
};