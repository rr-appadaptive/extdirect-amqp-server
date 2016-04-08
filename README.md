# extdirect-amqp-server

ExtDirect amqp server using ExtDirect ((c) Sencha Corp.) as communication means with web-clients. All ExtDirect client-requests are sent to an amqp-message-broker (we're using rabbitMQ 3.6.1), using the amqp-protocol (Version 0-9-1) and the message-broker response is then sent as ExtDirect response back the client.

Thus it is possible to keep ExtDirect-proxies in all ExtJs applications while having at the same time the adavantages of a message-broker (security, scalability, distributability). 

The main ExtDirect package is a fork of https://github.com/jurisv/nodejs.extdirect 

If this package is used instead of the original nodejs.extdirect package, all examples of the example git-repository mentioned in the original packgage should work as well.

Developed and tested with rabbitMQ 3.6.1
