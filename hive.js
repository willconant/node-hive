// hive.js
// (c) 2010 Will Conant
// will.conant@gmail.com

var sys = require('sys');
var querystring = require('querystring');

exports.createHive = function(cmd, cmd_args) {
	return new Hive(cmd, cmd_args);
}

var Hive = function(cmd, cmd_args) {
	this._cmd = cmd;
	this._cmd_args = cmd_args;
	this._drones = [];
	this._queue = [];
	this._timeout_ms = 30000;
};

Hive.prototype.addDrone = function(more_args) {
	this._drones.push( new Drone(this._cmd, this._cmd_args.concat(more_args || [])) );
	return this;
}

Hive.prototype.addDrones = function(count, more_args) {
	for (var i = 0; i < count; i++) {
		this.addDrone(more_args);
	}
	return this;
}

Hive.prototype.setTimeoutDuration = function(timeout_ms) {
	this._timeout_ms = timeout_ms;
	return this;
}

Hive.prototype.request = function(req_data, callback) {
	if (this._drones.length == 0) {
		throw new Error("Hive has no drones. Try Hive.addDrone");
	}
	
	this._queue.push({req_data: req_data, callback: callback});
	
	this._flushQueue();
	
	return this;
}

Hive.prototype._flushQueue = function() {
	while (this._queue.length > 0) {
		var found_one = false;
		
		for (var i = 0; i < this._drones.length; i++) {
			var drone = this._drones[i];
			
			if (drone._restart_me) {
				// THIS WORKER QUIT FOR WHATEVER REASON RESTART HIM
				sys.puts("drone " + drone._proc.pid + " exit. Restarting...");
				drone = this._drones[i] = new Drone(drone._cmd, drone._cmd_args);
			}
			
			if (drone._callback == null) {
				// WE HAVE A WORKER THAT CAN HANDLE THIS
				var queue_item = this._queue.shift();
				drone.request(queue_item.req_data, queue_item.callback, this._timeout_ms);
				found_one = true;
				break;
			}
		}
		
		if (!found_one) {
			// THE WORKERS ARE ALL BACKLOGGED, WAIT 100 ms AND TRY AGAIN
			if (this._queue.length > (this._drones.length * 2)) {
				throw new Error("too many requests backlogged");
			}
			setTimeout(_bind(this._flushQueue, this), 100);
			break;
		}
	}
}

var Drone = function(cmd, cmd_args) {
	this._cmd = cmd;
	this._cmd_args = cmd_args;
	this._proc = process.createChildProcess(cmd, cmd_args);
	this._out_buf = '';
	this._err_buf = '';
	this._callback = null;
	this._timeout = null;
	this._restart_me = false;
	
	var output_cb = function(data) {
		if (data != null) {
			this._out_buf += data;
			
			var idx = this._out_buf.indexOf("\n");
			
			if (idx > -1) {			
				// GRAB THE RESPONSE STRING
				var res_str = this._out_buf.substr(0, idx);
				this._out_buf = ''; // just ignore anything else in the buffer
								
				// GRAB THE CALLBACK
				var callback = this._callback;
				this._callback = null;
				
				// CLEAR THE TIMEOUT
				clearTimeout(this._timeout);
				this._timeout = null;
				
				// PARSE THE RESPONSE
				var res_parsed = _decodeData(res_str);
				
				// HANDLE CONTROLS
				if (res_parsed.CTRL && res_parsed.CTRL.RESTART_ME) {
					this._proc.kill();
					this._restart_me = true;
				}
				
				// AND CALL IT BACK!
				callback(null, res_parsed.RESP);
			}
		}
	}
	
	var error_cb = function(data) {
		// JUST IGNORE STANDARD ERROR
	}
	
	var exit_cb = function(code) {
		if (this._callback != null) {
			clearTimeout(this._timeout);
			this._callback(new Error("drone exited with code " + code));
		}
		
		this._restart_me = true;
	}
	
	this._proc.addListener('output', _bind(output_cb, this));
	this._proc.addListener('error', _bind(error_cb, this));
	this._proc.addListener('exit', _bind(exit_cb, this));
}

Drone.prototype.request = function(req_data, callback, timeout_ms) {
	// MAKE SURE WE AREN'T IN THE MIDDLE OF ONE
	if (this._callback != null) {
		throw new Error("drone is already handling a request");
	}

	// SEND THE REQUEST
	this._proc.write(_encodeData(req_data) + "\n");
	
	// ADD TO THE CALLBACK QUEUE
	this._callback = callback;
	
	// SET A TIMEOUT ON THIS REQUEST
	this._timeout = setTimeout(_bind(function() {
		this._callback(new Error("request timed out"));
		this._proc.kill();
		this._restart_me = true;
	}, this), timeout_ms);
	
	return this;
}

exports.becomeDrone = function(handler) {
	var in_buf = '';

	process.stdio.addListener('data', function(data) {
		
		in_buf += data;
		
		var idx = in_buf.indexOf("\n");
		
		if (idx > -1) {
			var req_str = in_buf.substr(0, idx);
			in_buf = ''; // just ignore anything else in the buffer

			handler(_decodeData(req_str), function(response_data, ctrl_data) {
				process.stdio.write(_encodeData({RESP: response_data, CTRL: ctrl_data}) + "\n");
			});
		}
	
	});
	
	process.stdio.open();
};

var _encodeData = function(data) {
	return querystring.stringify({d: JSON.stringify(data)});
}

var _decodeData = function(encoded) {
	return JSON.parse(querystring.parse(encoded).d);
}

var _bind = function(func, obj) {
	return function() {
		func.apply(obj || root, Array.prototype.slice.call(arguments))
	}
}
