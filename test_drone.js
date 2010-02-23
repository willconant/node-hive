var sys = require('sys');
var hive = require('./hive');

var count = 0;

hive.becomeDrone(function(req_data, respond) {
	setTimeout(function() {
		respond({mypid: process.pid, count: count++}, { RESTART_ME: count > 15 });
	}, 300);
});
