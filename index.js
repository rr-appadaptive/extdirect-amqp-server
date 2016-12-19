var api = require('./lib/api'),
    router = require('./lib/router'),
    extdirect = module.exports;

extdirect.initApi = function(config){
    return new api(config);
};

extdirect.initRouter = function(config){
	var amqpRouter = new router(config);
	
	amqpRouter.initAmqpConsumer(amqpRouter);
	return amqpRouter;
};

exports.broker = require('./lib/broker');

