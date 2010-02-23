var sys = require('sys');
var hive = require('./hive');

var my_hive = hive.createHive('node', ['test_drone.js']).addDrones(3);

setInterval(function() {
	my_hive.request({me: 'foo'}, function(err, res_data) {
		if (err) {
			sys.puts(err)
		}
		else {
			sys.puts(JSON.stringify(res_data));
		}
	});
}, 150);
