var api = require('./lib/api'),
    router = require('./lib/router'),
    extdirect = module.exports;

extdirect.initApi = function(config){
    return new api(config);
};

extdirect.initRouter = function(config){
	var serverRouter = new router(config);

	// start the server side Message-Broker, -channel and -queue
	serverRouter.connectAmqp();
    return serverRouter;
};
