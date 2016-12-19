/**
 * http://usejsdoc.org/
 */
var vPath = require('path'),
    platform = require('os').platform(),
    uuid = require('node-uuid'),
    amqp = require('amqplib/callback_api'),
    JSONfn = require('jsonfn')

var infoLogDefault = function (logMsg) {
	console.log('INFO: ' + logMsg);
};

var errorLogDefault = function (logMsg, conn) {
	if (conn) conn = null;
	if (logMsg !== undefined) {
		console.log('ERROR-1: ' + logMsg);
		process.exit(1);
	} 
};


var AmqpConnector = {
		
	handleError: function(logMsg, conn) {
		this.errorLog(err);
		if (conn) {
			this.connection = null;
		}
	},
	
	uri: function() {
		if (typeof this.url === 'object') {
			return this.url;
		} else {
			if (this.url.match(/amqps?:\/\//)) {
				return this.url;
		    }
		    return ['amqp://', this.url].join('');
		}
	},
		
		
	init: function (config) {
	    options = config || {};
	    this.url = options.broker || 'localhost';
	    this.socketOptions = options.socketOptions || {};
	    this.infoLog = options.infoLog || infoLogDefault;
	    this.errorLog = options.errorLog || errorLogDefault;

	    this.connectService();
	    return this;
	},
	
	getConnection: function() {
		if (!global.amqpConnection) {
			this.infoLog('Connecting ' + this.uri() + ' to RabbitMQ-Server');
			this.connectService()
		} 
		return global.amqpConnection;
	},
	
	connectService: function() {
		var uri = this.uri();
		amqp.connect(uri, this.socketOptions, function(err, conn) {
			if (err !== null) cb(err, null);
			amqpConnection = conn;
		});
	}
	
}


var AmqpConsumer = {
	
	handleError: function(logMsg, conn) {
		this.errorLog(logMsg);
		if (global.amqpConnection) {
			amqpConnection.close();
		}
	},
	
	uri: function() {
		if (typeof this.url === 'object') {
			return this.url;
		} else {
			if (this.url.match(/amqps?:\/\//)) {
				return this.url;
		    }
		    return ['amqp://', this.url].join('');
		}
	},
	
	getConnection: function(cb) {
		if (!global.amqpConnection) {
			this.infoLog('Connecting ' + this.uri() + ' to RabbitMQ-Server');
			this.connectService(cb)
		} else {
			cb(null, amqpConnection);
		}
		
	},
	
	connectService: function(cb) {
		var uri = this.uri();
		amqp.connect(uri, this.socketOptions, function(err, conn) {
			if (err !== null) cb(err, null);
			amqpConnection = conn;
			cb(null, conn);
		});
	},
	
	init: function (config) {
	    options = config || {};
	    this.replyTimeOutInterval = options.replyTimeOutInterval || 3000;
	    this.url = options.broker || {protocol: 'amqp', user: 'guest', password: 'guest'};
	    this.socketOptions = options.socketOptions || {heartbeat: 10, host: '192.168.6.43'};
	    this.queue = options.queue || 'extdirect-default-queue';
	    this.queueOptions = options.queueOptions || {exclusive: true, autoDelete: true};
	    this.debugLevel = options.debugLevel || 0;
	    this.standalone = options.standalone || false;
	    this.infoLog = options.infoLog || infoLogDefault;
	    this.errorLog = options.errorLog || errorLogDefault;

	    return this;
	},
	
	startConsume: function(config, router) {
		var me = this;
		
		console.log(config.enableBroker + ' / ' + ('directServices' in config));
	    if (config.enableBroker && ('directServices' in config)) {
	    	
    		this.getConnection(function(err, conn) {
    			if (err !== null) {
    				me.handleError(err, conn);
    				//return null;
    			} else if (conn !== undefined && conn !== null) {
			        process.once('SIGINT', function() { conn.close(); });
		
			        var ex = 'extdirect.topic',
			        	exOk = null;
		
			        conn.createChannel(function(err, ch) {
		            	if (err !== null) this.handleError(err, conn);
		                exOk = ch.assertExchange(ex, 'topic', {durable: false}, function(err, exOk) {
		                	if (err !== null) this.handleError(err, conn);
		                    for (var dxModule in config.directServices) {
		                    	var qModule = dxModule;
		                    	for (var dxAction in config.directServices[dxModule]) {
		                    		var q = qModule + ':' + config.directServices[dxModule][dxAction];
		                            ch.assertQueue(q, {durable: false}, function(err, ok) {
		                            	if (err !== null) this.handleError(err, conn);
		                        		topic = ok.queue.split(':')[0];
			                            ch.bindQueue(ok.queue, exOk.exchange, topic, {}, function(err) {
			                            	if (err !== null) this.handleError(err, conn);
		                            		console.log(' [x] Awaiting RPC requests for queue: ' + ok.queue);
				                            ch.prefetch(1, false);
				                            ch.consume(ok.queue, reply, {noAck:false}, function(err) {
				                                if (err !== null) this.handleError(err, conn);
				                            });
			                            });
		                            });
		                    	}
		                    }
		                    function reply(msg) {
		                        var ctnt = [],
		                        	emptyResponse = {};
		                        
		                        if (msg === null) AmqpConsumer.startConsume(config, router);
		                        
		                        try {
			                        ctnt = JSON.parse(msg.content);
			                        console.log(" [.] Running ExtDirect Server-Request for queue: " + msg.properties.replyTo);
			                        router.processAmqpRequest(ctnt[0], ctnt[1], function(err, response) {
			                        	console.log(' [.] Sending ExtDirect-Response to queue: ' + msg.properties.replyTo);
			                            if (response !== undefined && response !== null) {
			                            	ch.sendToQueue(msg.properties.replyTo, new Buffer(JSONfn.stringify(response)), {correlationId: msg.properties.correlationId});
			                            } else {
			                            	ch.sendToQueue(msg.properties.replyTo, new Buffer('EMPTY'), {correlationId: msg.properties.correlationId});
			                            }
			                        });
		                        } catch (ex) {
		                        	console.log(" [!] Not a valid Ext-Direct RPC Server-Request:");
		                        	return null
		                        }
			                    ch.ack(msg);
		                    }
		                });
		            });
    			}
    		});
        };
	},
	
};
	
var AmqpRpc = {
		
	handleError: function(err, conn) {
		this.errorLog(err);
		if (conn) {
			conn.close();
		}
	},
	
	uri: function() {
		if (typeof this.url === 'object') {
			return this.url;
		} else {
			if (this.url.match(/amqps?:\/\//)) {
				return this.url;
		    }
		    return ['amqp://', this.url].join('');
		}
	},
	
	getConnection: function(cb) {
		if (!global.amqpConnection) {
			this.infoLog('Connecting ' + this.uri() + ' to RabbitMQ-Server');
			this.connectService(cb)
		} else {
			cb(null, amqpConnection);
		}
		
	},
	
	connectService: function(cb) {
		var uri = this.uri();
		amqp.connect(uri, this.socketOptions, function(err, conn) {
			if (err !== null) cb(err, null);
			amqpConnection = conn;
			cb(null, conn);
		});
	},
		
	init: function (config) {
	    options = config || {};
	    this.url = options.broker || 'localhost';
	    this.socketOptions = options.socketOptions || {};
	    this.infoLog = options.infoLog || infoLogDefault;
	    this.errorLog = options.errorLog || errorLogDefault;

	    return this;
	},
	
	publishRpc: function(params, dxQueue, callback) {
	    var me = this,
	    	_params = params instanceof Array ? params : [params],
	    	errors = [],
	    	edMessage = [];
	  
	
		/*
		 * Client part of the extdirect-amqp module. Whenever an ExtDirect api-call is issued, 
		 * it consumes from a direct-reply queue (amq.rabbitmq.reply-to) on the message broker.
		 * For a RPC request, the Client sends a message with two properties: 
		 * - reply_to -, which is set to the direct-reply (amq.rabbitmq.reply-to) queue and 
		 * - correlation_id -, which is set to a unique value for every request.
		 * The request is sent to a queue named according to the requested Extdirect Module
		 * e.g. "DXEmployees" and the method executed eg."read" - queueName: DXEmployees:read
		 * The client waits for data on the direct-reply queue. When a message appears, 
		 * it checks the correlation_id property. If it matches the value from the 
		 * request it returns the response to the application and closes channel and queue.
		 */
		
		// prepare the ExtDirect Request for the message broker
	    
	    edMessage.push(_params);
	    edMessage.push(dxQueue);	    
	    
	    
        me.getConnection(function(err, conn) {
        	if (err !== null) {
				me.handleError(err, conn);
        	} else if (conn !== undefined && conn !== null) {			
				
				process.once('SIGINT', function() { conn.close(); });
        
		        conn.createChannel(function(err, ch) {
		        	if (err !== null) me.handleError(err, conn);
		        	var cId = uuid();
	
		        	function maybeAnswer(msg) {
		                if (msg.properties.correlationId === cId) {
		                    console.log(' [.] Got Answer for Id: ' +  msg.properties.correlationId);
		                    if (msg.content && msg.content !== undefined ) {
		                    	if (msg.content.toString() !== 'EMPTY') {
		                    		console.log(' [.] Returning RPC-Response for Id: ' + msg.properties.correlationId);
		                    		edResponse = JSONfn.parse(msg.content);
		                    		callback(null, edResponse);
		                    		ch.close();
		                    	} else {
		                    		console.log(' [.] Returning Empty RPC-Response for Id: ' + msg.properties.correlationId);
		                    		callback(null);
		                    		ch.close();
		                    	}
		                    } else {
		                        console.log(' [.] Got no RPC-Response for Id: ' + msg.properties.correlationId);
		                        errors.push(' [.] Got no RPC-Response for Id: ' + msg.properties.correlationId)
		                        callback(errors, null);
		                        ch.close();
		                    }	                    
		                } else {
		                	errors.push(' [.] Unexpected Error for Id: ' + msg.properties.correlationId)
		                	callback(errors, null);
		                	ch.close();
		                }
		                
		            };
		        	console.log(' [x] Requesting Direct-Reqply for ExtDirect api-call on queue: ' + 'amq.rabbitmq.reply-to' + ' / '+  cId);
		        	ch.consume('amq.rabbitmq.reply-to', maybeAnswer, {noAck:true}, function(err, cOk) {
		            	if (err !== null) return me.handleError(err, conn);
		        	});
		            console.log(' [x] Sending Direct-Reply Request for ExtDirect api-call to queue: ' + dxQueue + ' with Id: '+  cId);
		            ch.sendToQueue(dxQueue, new Buffer(JSONfn.stringify(edMessage)), { correlationId: cId, replyTo: 'amq.rabbitmq.reply-to' });
		        });
        	}
        });
	}
};
	
var AmqpPublish = {
			
	handleError: function(err, conn) {
		this.errorLog(err);
		if (conn) {
			conn.close();
		}
	},
	
	uri: function() {
		if (typeof this.url === 'object') {
			return this.url;
		} else {
			if (this.url.match(/amqps?:\/\//)) {
				return this.url;
		    }
		    return ['amqp://', this.url].join('');
		}
	},
	
	init: function (config) {
	    options = config || {};
	    this.url = options.broker || 'localhost';
	    this.socketOptions = options.socketOptions || {};
	    this.infoLog = options.infoLog || infoLogDefault;
	    this.errorLog = options.errorLog || errorLogDefault;

	    return this;
	},
	
	getConnection: function(cb) {
		if (!global.amqpConnection) {
			this.infoLog('Connecting ' + this.uri() + ' to RabbitMQ-Server');
			this.connectService(cb)
		} else {
			cb(null, amqpConnection);
		}
		
	},
	
	connectService: function(cb) {
		var uri = this.uri();
		amqp.connect(uri, this.socketOptions, function(err, conn) {
			if (err !== null) cb(err, null);
			amqpConnection = conn;
			cb(null, conn);
		});
	},
		
	publish: function(pMsg, routingKey) {
	    var me = this;	  
		    
        me.getConnection(function(err, conn) {
        	if (err !== null) {
				me.handleError(err, conn);
        	} else if (conn !== undefined && conn !== null) {	        
		        conn.createChannel(function(err, ch) {
		        	if (err !== null) me.handleError(err, conn);
		        	if (routingKey.indexOf('.String') === -1) {
		        		ch.publish('amq.topic', routingKey, new Buffer(JSONfn.stringify(pMsg)));
		        	} else {
		        		ch.publish('amq.topic', routingKey, new Buffer(pMsg));
		        	}
		        	ch.close();
		        });
        	}
        });
	}	

};


exports.RpcConsumer = function (config) {
	return Object.create(AmqpConsumer).init(config);
};

exports.AmqpConnector = function(config) {
	return Object.create(AmqpConnector).init(config);
};

exports.RpcPublisher = function(config) {
	return Object.create(AmqpRpc).init(config);
};

exports.AmqpPublisher = function(config) {
	return Object.create(AmqpPublish).init(config);
}

