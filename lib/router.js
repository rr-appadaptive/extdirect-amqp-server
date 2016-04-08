var vPath = require('path'),
    platform = require('os').platform(),
    amqp = require('amqplib/callback_api'),
    JSONfn = require ('jsonfn'),
    uuid = require('node-uuid');

function Router(config) {
    this.processRoute = function(req, res){
        var me = this;

        if(config.enableProcessors) {

            var root = __dirname.replace(vPath.normalize('/node_modules/extdirect-amqp-gateway/lib'), ''),
                fileName = vPath.join(root, config.classPath, '.scripts', 'RouterProcessor.js'),
                processor = require(fileName);

            if (!config.cacheAPI) {
                //invalidate node module cache
                delete require.cache[fileName];
            }

            processor.beforeTransaction(req, res, function(err){
                if(err){

                } else {
                    me.runTransaction(req, res);
                }
            });
        } else {
            me.runTransaction(req, res);
        }
    };

    this.afterTransaction = function(req, res, upload, batch){
        var str;

        if(config.enableProcessors) {

            var root = __dirname.replace(vPath.normalize('/node_modules/extdirect-amqp-gateway/lib'), ''),
                fileName = vPath.join(root, config.classPath, '.scripts', 'RouterProcessor.js'),
                processor = require(fileName);

            if (!config.cacheAPI) {
                //invalidate node module cache
                delete require.cache[fileName];
            }

            processor.afterTransaction(req, res, batch, function(err, batch){
                if(err){

                } else {
                    str = JSON.stringify(batch, 'utf8');
                    if (upload) {
                        res.writeHead(200, {'Content-type': 'text/html'});
                        res.end('<html><body><textarea>' + str + '</textarea></body></html>');
                    } else {
                        res.writeHead(200, {'Content-type': 'application/json'});
                        res.end(str);
                    }
                }
            });
        } else {
            str = JSON.stringify(batch, 'utf8');
            if (upload) {
                res.writeHead(200, {'Content-type': 'text/html'});
                res.end('<html><body><textarea>' + str + '</textarea></body></html>');
            } else {
                res.writeHead(200, {'Content-type': 'application/json'});
                res.end(str);
            }
        }
    };


    this.runTransaction = function(req, res) {
        var me = this,
            root = __dirname.replace(vPath.normalize('/node_modules/extdirect-amqp-gateway/lib'), ''),
            data = req.body,
            slash = platform.substr(0,3) === 'win' ? '\\' : '/', //untested
            callback,
            currentPacket,
            upload = false,
            d = [],
            reduce,
            rLen,
            clonedPacket,
            batch = [],
            api,
            apiPath,
            i;

        // Finish up
        var finalCallback = function(err, batch) {
            if(reduce === 0) {
                me.afterTransaction(req, res, upload, batch);
            }
        };

        if(data instanceof Array) {
            d = data;
        } else {
            d.push(data);
        }

        reduce = d.length;

        for(i = 0, rLen = d.length; i < rLen; i++) {
            currentPacket = d[i];

            if (currentPacket.extAction) {
                clonedPacket = this.copyObject(currentPacket);
                currentPacket.action = currentPacket.extAction;
                currentPacket.method = currentPacket.extMethod;
                currentPacket.tid = currentPacket.extTID;
                currentPacket.type = currentPacket.extType;
                currentPacket.isUpload = upload = currentPacket.extUpload === 'true';

                clonedPacket.extAction = null;
                clonedPacket.extType = null;
                clonedPacket.extMethod = null;
                clonedPacket.extTID = null;
                clonedPacket.extUpload = null;

            }
            
            apiPath = vPath.join(root, config.classPath, currentPacket.action.replace('.', slash)) + '.js';
            //api = require(apiPath);

            if(!config.cacheAPI){
                //invalidate node module cache
                delete require.cache[apiPath];
            }

            if(!currentPacket.data) {
                currentPacket.data = [];
            }

            if(clonedPacket) {
                currentPacket.data.push(clonedPacket);
            }

            //mix in metadata
            if(currentPacket.metadata) {
                currentPacket.data[0].metadata = currentPacket.metadata;
            }

            // 2nd parameter callback
            // currentPacket.data.push(callback);

            // keep backwards capability on sessions as 3rd parameter
            currentPacket.data.push(req.sessionID ? req.sessionID : null);
            
            // add 4th hand 5th parameter - request object, configurable
            if(config.appendRequestResponseObjects) {
                currentPacket.data.push(req);
                currentPacket.data.push(res);
                // as the amqp-gateway request is split up into a client and a server part,
            	// the server has to know the api-path and the -method
            	currentPacket.data.push(apiPath);
            }

            if(config.logging) {
                config.logging(currentPacket.action, currentPacket.method, clonedPacket);
            }
            
            // error handler for the amqp gateway
            function bail(err, conn) {
  				console.error(err);
  				if (conn) conn.close(function() { process.exit(1); });
			}
			console.log(' [x] trying Api-Execution over Message-Broker: ' + currentPacket.data[4] + ':' + currentPacket.method);
			
			    /*
                 * Client part of the amqp-gateway. Whenever an ExtDirect api-call is issued, 
                 * it creates an anonymous exclusive callback queue on the message broker.
                 * For a RPC request, the Client sends a message with two properties: 
                 * - reply_to -, which is set to the callback queue and 
                 * - correlation_id -, which is set to a unique value for every request.
                 * The request is sent to a queue named "rpc_queue".
                 * The client waits for data on the callback queue. When a message appears, 
                 * it checks the correlation_id property. If it matches the value from the 
                 * request it returns the response to the application and closes channel and queue.
                 */
                function on_connect(err, conn) {
                    conn.createChannel(function(err, ch) {
                    	if (err !== null) return bail(err, conn);
                    	var cId = uuid();
                                                    
                        function maybeAnswer(msg) {
      						if (msg.properties.correlationId === cId) {
        						console.log(' [.] Got Answer for ' + msg.properties.correlationId);
        						if (msg.content && msg.content !== undefined ) {
        							console.log(' [.] Returning RPC-Response for ' + msg.properties.correlationId);
        							batch.push(JSONfn.parse(msg.content));
        							reduce--;
        							finalCallback(null, batch);
        							//res.writeHead(200, {'Content-type': 'application/json'});
                					//res.end(msg.content);
                				} else {
                					reduce--;
                					console.log(' [.] Got no RPC-Response for ' + msg.properties.correlationId);
                				}
      						} else return bail(new Error('Unexpected message'), conn);
      						ch.close(function() { conn.close(); });
    					}
    					
						ch.assertQueue('', {exclusive: true}, function(err, ok) {
							if (err !== null) return bail(err, conn);
      						var queue = ok.queue;
                            console.log(' [x] Requesting ExtDirect api-call: ' + currentPacket.data[4] + ':' + currentPacket.method + ' ' +  cId);

                            ch.consume(queue, maybeAnswer, {noAck:true});
                            
                            ch.sendToQueue('rpc_queue', new Buffer(JSONfn.stringify(currentPacket)), { correlationId: cId, replyTo: queue });
                        });
                    });
                }
			/*
            try {
                /*
                 * Client part of the amqp-gateway. Whenever an ExtDirect api-call is issued, 
                 * it creates an anonymous exclusive callback queue on the message broker.
                 * For a RPC request, the Client sends a message with two properties: 
                 * - reply_to -, which is set to the callback queue and 
                 * - correlation_id -, which is set to a unique value for every request.
                 * The request is sent to a queue named "rpc_queue".
                 * The client waits for data on the callback queue. When a message appears, 
                 * it checks the correlation_id property. If it matches the value from the 
                 * request it returns the response to the application and closes channel and queue.
                amqp.connect('amqp://localhost', function(err, conn) {
                    conn.createChannel(function(err, ch) {
                    	if (err !== null) return bail(err, conn);
                    	var cId = uuid();
                                                    
                        function maybeAnswer(msg) {
      						if (msg.properties.correlationId === cId) {
        						console.log(' [.] Got Answer for ' + msg.properties.correlationId);
        						if (msg.content && msg.content !== undefined ) {
        							console.log(' [.] Returning RPC-Response for ' + msg.properties.correlationId);
        							batch.push(JSONfn.parse(msg.content));
        							reduce--;
        							finalCallback(null, batch);
        							//res.writeHead(200, {'Content-type': 'application/json'});
                					//res.end(msg.content);
                				} else {
                					reduce--;
                					console.log(' [.] Got no RPC-Response for ' + msg.properties.correlationId);
                				}
      						} else return bail(new Error('Unexpected message'), conn);
      						ch.close(function() { conn.close(); });
    					}
    					
						ch.assertQueue('', {exclusive: true}, function(err, ok) {
							if (err !== null) return bail(err, conn);
      						var queue = ok.queue;
                            console.log(' [x] Requesting ExtDirect api-call: ' + currentPacket.data[4] + ':' + currentPacket.method + ' ' +  cId);

                            ch.consume(queue, maybeAnswer, {noAck:true});
                            
                            ch.sendToQueue('rpc_queue', new Buffer(JSONfn.stringify(currentPacket)), { correlationId: cId, replyTo: queue });
                        });
                    });
                });
                //api[currentPacket.method].apply({}, currentPacket.data); <!-- original api-call -->
            } catch(e) {
                if(process.env.NODE_ENV !== 'production') {
                    batch.push({
                        type: 'exception',
                        tid: currentPacket.tid,
                        action: currentPacket.action,
                        method: currentPacket.method,
                        message: e.message,
                        data: e
                    });
                } else {
                    batch.push({
                        type: 'rpc',
                        tid: currentPacket.tid,
                        action: currentPacket.action,
                        method: currentPacket.method,
                        data: null
                    });
                }
                reduce--;
                //finalCallback(null, batch);
            }
            */
        }
    };

    this.copyObject = function(obj) {
        var newObj = {};
        for (var key in obj) {
            newObj[key] = obj[key];
        }
        return newObj;
    }
}

module.exports = Router;
