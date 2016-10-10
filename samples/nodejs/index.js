var winston = require('winston');
var moment = require('moment');
//
// Logging setup.
//
winston.remove(winston.transports.Console);
winston.add(winston.transports.Console, {
    stderrLevels: [],
    handleExceptions: true,
    humanReadableUnhandledException: true,
    timestamp: function() { return moment().format('MMMM Do YYYY, h:mm:ss a'); }
});

winston.level  = 'debug';


//
// Setup Kafka consumer callback.url & dead.callback.url
//
var request = require('request');
var http = require('http');
http.createServer(function(request, response) {
    var body = [];
    var url = request.url;
    request.on('error', function(err) {
        winston.error(err);
    }).on('data', function(chunk) {
        body.push(chunk);
    }).on('end', function() {
        body = Buffer.concat(body).toString();

        response.on('error', function(err) {
            winston.error(err);
        });

        if (url.indexOf('kafka-consumption-callback') > -1) {
            winston.debug("kafka-consumption-callback: " + body);
            // Lets fail every message to test the retries and dead callback.
            response.statusCode = 500;
            response.write('messages failed!');
            response.end();
        } else if (url.indexOf('dead-callback') > -1) {
            winston.debug("dead-callback: " + body);
            // Lets send this to another kafka topic to test the producer.
            var now = new Date();
            var options = {
                url: process.env.KAFKA_PUBLISH_URL,
                method: 'POST',
                json: true,
                body: {
                    topic: process.env.KAFKA_TOPIC_TEST,
                    key: ''+now.getTime(),
                    message: body
                }
            };
            request(options, function(error, response, body) {
                if (error) winston.error(error);
                response.statusCode = 200;
                response.write('dead messages received!');
                response.end();
            });
        }
    });
}).listen(3000);

//
// Post a test message.
//
setTimeout(function() {
    var now = new Date();
    var options = {
        url: process.env.KAFKA_PUBLISH_URL,
        method: 'POST',
        json: true,
        body: {
            topic: process.env.KAFKA_TOPIC,
            key: '' + now.getTime(),
            message: 'test_' + now.getTime()
        }
    };
    request(options, function (error, response, body) {
        if (error) winston.error(error);
        winston.info('test message sent');
    });
}, 15000);