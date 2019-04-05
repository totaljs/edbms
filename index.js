var DB = {};

function ElasticDB(url, eb) {

	var t = this;
	t.$remap = true;
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

	DB[name] = url;
	return ElasticDB;
};

const REG_PARAM = /\$[a-z.-]+/i;
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
		if (method !== 'GET')
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

ED.$exec = function(builder) {

	var self = this;
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
	var q = builder.create();
	rb.json(q);
	rb.$method = builder.options.method;
	rb.$keepalive = true;
	rb.exec(function(err, response) {

		if (response.error) {
			var err = response.error.type + ': ' + response.error.reason;
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
			output.pages = Math.ceil(output.count / opt.take);
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
	this.items = [];
	this.options = { method: 'post' };
	this.tmp;
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
		self.options.fields.push('"' + (arr[i][0] === ' ' ? arr[i].trim() : arr[i]) + '"');

	return self;
};

EP.add = function(command, arg) {
	var self = this;

	if (arg) {
		TMP.value = arg;
		command = command.replace(REG_PARAM, TMP.replace);
		self.items.push(command);
	} else
		self.items.push(command);
	return self;
};

EP.push = function(command, arg, end) {

	var self = this;

	if (command) {
		TMP.value = arg;
		self.tmp += (self.tmp ? ',' : '') + (arg && arg !== true ? command.replace(REG_PARAM, TMP.replace) : command);
	}

	if (command == null || arg === true || end) {
		self.tmp && self.items.push(self.tmp);
		self.tmp = '';
	}

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

	var builder = [];
	opt.take && builder.push('"size":' + opt.take);
	opt.skip && builder.push('"from":' + opt.skip);
	opt.fields && opt.fields.length && builder.push('"_source":[' + opt.fields.join(',') + ']');
	self.items.length && builder.push(self.items.join(','));
	return '{' + builder.join(',') + '}';
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

global.EDB = function(name, err) {
	if (name && typeof(name) === 'object') {
		err = name;
		name = null;
	}
	return new ElasticDB(name, err);
};