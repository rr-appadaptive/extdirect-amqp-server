var vPath = require('path'),
    platform = require('os').platform();

function Router(config) {

    this.connectBroker = function() {
        if (config.enableBroker && config.directServices.length > 0) {
            amqp.connect('amqp://localhost', function(err, conn) {
                if (err !== null) return bail(err);

                process.once('SIGINT', function() { conn.close(); });

                var ex = 'extdirect',
                    ok = null,
                    q = null;

                conn.createChannel(function(err, ch) {
                    ok = ch.assertExchange(ex, 'topic', {durable: false});

                    for (var dxModule in config.directServices) {
                    	for (var dxAction in dxModule) {
                    		q = dxModule + ':' + dxAction;
                            ch.assertQueue(q, {durable: false});
                            ch.prefetch(1);
                            ch.consume(q, reply, {noAck:false}, function(err) {
                                if (err !== null) return bail(err, conn);
                                console.log(' [x] Awaiting RPC requests for queue: ' + q);
                            });
                    	}
                    }
                    function reply(msg) {
                        var ctnt = [],
                            finalResponse = function(err, response) {
                                console.log(' [.] Sending ExtDirect-Response to queue: ' + msg.properties.replyTo);
                                ch.sendToQueue(msg.properties.replyTo, new Buffer(JSONfn.stringify(response)), {correlationId: msg.properties.correlationId});
                            };

                        ctnt = JSON.parse(msg.content);
                        this.processRoute(ctnt[1], ctnt[1])

                        console.log(" [.] Running ExtDirect Server-Request for queue: " + msg.properties.replyTo);

                        ch.ack(msg);
                    }
                });
            });
        }
    };

    this.processRoute = function(req, res){
        var me = this;

        if(config.enableProcessors) {

            var root = __dirname.replace(vPath.normalize('/node_modules/extdirect-amqp-server/lib'), ''),
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
        var amqpResponse = [],
            directModule = req.body.action,
            directMethod = req.body.method;
        
        /*
         * according to the configuration the request is either sent directly back to the client-browser
         * or to the message-broker queue the request came from.
         */
        
        // local response
        if (directModule in config.localServices && config.localServices[directModule].indexOf(directMethod) !== -1 ||
        		directModule in config.clientServices && config.clientServices[directModule].indexOf(directMethod) !== -1) {
        	if(config.enableProcessors) {

                var root = __dirname.replace(vPath.normalize('/node_modules/extdirect/lib'), ''),
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
        } else if (directModule in config.directServices && config.directServices[directModule].indexOf(directMethod) !== -1) {
        	if(config.enableProcessors) {

                var root = __dirname.replace(vPath.normalize('/node_modules/extdirect-amqp-server/lib'), ''),
                    fileName = vPath.join(root, config.classPath, '.scripts', 'RouterProcessor.js'),
                    processor = require(fileName);

                if (!config.cacheAPI) {
                    //invalidate node module cache
                    delete require.cache[fileName];
                }

                processor.afterTransaction(req, res, batch, function(err, batch){
                    if(err){

                    } else {

                        if (upload) {
                            amqpResponse.push(batch);
                            amqpResponse.push(true);
                            finalResponse(null, amqpResponse);
                        } else {
                            amqpResponse.push(batch);
                            amqpResponse.push(true);
                            finalResponse(null, amqpResponse);
                        }
                    }
                });
            } else {
                if (upload) {
                    amqpResponse.push(batch);
                    amqpResponse.push(true);
                    finalResponse(null, amqpResponse);
                } else {
                    amqpResponse.push(batch);
                    amqpResponse.push(true);
                    finalResponse(null, amqpResponse);
                }
            }
        }
        
    };


    this.runTransaction = function(req, res) {
        var me = this,
            root = __dirname.replace(vPath.normalize('/node_modules/extdirect-amqp-server/lib'), ''),
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
            i,
            edMessage = [],
            directModule = req.body.action,
            directMethod = req.body.method;
        
        /*
         * according to the configuration the transaction is either run locally or is being sent
         * to the message-broker 
         */
        // local transaction
        if (directModule in config.localServices && config.localServices[directModule].indexOf(directMethod) !== -1 ||
        		directModule in config.directServices && config.directServices[directModule].indexOf(directMethod) !== -1) {
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

                callback = function(packet) {

                    return function(error, result) {
                        var newPacket;
                        result = result !== undefined ? result : {};

                        if(error) {
                            newPacket = {
                                type: 'exception',
                                tid: packet.tid,
                                action: packet.action,
                                method: packet.method,
                                message: 'Exception',
                                data: null
                            };

                            if(process.env.NODE_ENV !== 'production') {
                                newPacket.message = error.message ? error.message : 'Exception';
                                newPacket.data = error;
                            }
                        } else {
                            newPacket = {
                                type: result.event ? 'event' : 'rpc',
                                tid: packet.tid,
                                action: packet.action,
                                method: packet.method,
                                result: result
                            };

                            if(result.event) {
                                newPacket.data = event;
                            }

                            if(config.responseHelper){
                                //We set success to true if responseHelper is TRUE and callback is called without any arguments or with true as result or result object is present, but success is missing
                                if(!result || result === true) {
                                    newPacket.result.success = true;
                                }

                                //Result is there, but success property is not mentioned. Let's fix it
                                if(result && (result.success === undefined || result.success === null)) {
                                    newPacket.result.success = true;
                                }

                                //We set success to false if autoResponse is TRUE and callback is called with result set to false
                                if(result && result === false) {
                                    newPacket.result.success = false;
                                }
                            }
                        }

                        batch.push(newPacket);
                        reduce--;
                        finalCallback(null, batch);
                    }
                }.apply(null, [currentPacket]);

                apiPath = vPath.join(root, config.classPath, currentPacket.action.replace('.', slash)) + '.js';
                api = require(apiPath);

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
                currentPacket.data.push(callback);

                // keep backwards capability on sessions as 3rd parameter
                currentPacket.data.push(req.sessionID ? req.sessionID : null);

                // add 4th hand 5th parameter - request object, configurable
                if(config.appendRequestResponseObjects) {
                    currentPacket.data.push(req);
                    currentPacket.data.push(res);
                }

                if(config.logging) {
                    config.logging(currentPacket.action, currentPacket.method, clonedPacket);
                }

                try {
                    api[currentPacket.method].apply({}, currentPacket.data);
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
                    finalCallback(null, batch);
                }
            }
        //remote transaction
        } else if (directModule in config.clientServices && config.clientServices[directModule].indexOf(directMethod) !== -1) {
            /*
             * Client part of the extdirect-amqp module. Whenever an ExtDirect api-call is issued, 
             * it creates an anonymous exclusive callback queue on the message broker.
             * For a RPC request, the Client sends a message with two properties: 
             * - reply_to -, which is set to the callback queue and 
             * - correlation_id -, which is set to a unique value for every request.
             * The request is sent to a queue named "rpc_queue".
             * The client waits for data on the callback queue. When a message appears, 
             * it checks the correlation_id property. If it matches the value from the 
             * request it returns the response to the application and closes channel and queue.
             */
            
            // prepare the ExtDirect Request for the message broker
            edMessage.push(req);
            
            amqp.connect('amqp://localhost', function(err, conn) {
                conn.createChannel(function(err, ch) {
                    if (err !== null) return bail(err, conn);
                    var cId = uuid(),
                    	dxQueue = directModule + ':' + directAction;

                    function maybeAnswer(msg) {
                        if (msg.properties.correlationId === cId) {
                            console.log(' [.] Got Answer for ' + msg.properties.replyTo + ' / ' + msg.properties.correlationId);
                            if (msg.content && msg.content !== undefined ) {
                                console.log(' [.] Returning RPC-Response for ' + msg.properties.replyTo + ' / ' + msg.properties.correlationId);
                                edResponse = JSONfn.parse(msg.content);
                                me.afterTransaction(req, res, edResponse[1], edResponse[0]);
                            } else {
                                console.log(' [.] Got no RPC-Response for ' + msg.properties.replyTo + ' / ' + msg.properties.correlationId);
                            }
                        } else return bail(new Error('Unexpected message'), conn);
                        ch.close(function() { conn.close(); });
                    }

                    ch.assertQueue(dxQueue, {exclusive: true}, function(err, ok) {
                        if (err !== null) return bail(err, conn);
                        var queue = ok.queue;
                        console.log(' [x] Requesting ExtDirect api-call: ' + dxQueue + ' / '+  cId);

                        ch.consume(queue, maybeAnswer, {noAck:true});

                        ch.sendToQueue(queue, new Buffer(JSONfn.stringify(edMessage)), { correlationId: cId, replyTo: queue });
                    });
                });
            });
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
