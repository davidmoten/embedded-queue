# embedded-queue
Features
* designed for embedded use (include the library in a webapp that uses file system for storage)
* exposes HTTP API 
* supports backpressure
* HTTP GET for consumer (long poll or short poll)
* HTTP POST for request, cancel, put  
* writes (calls to `put`) must be serialized
* supports multiple concurrent consumers
* uses Kafka-like reads (give me all records since time t)
* 
