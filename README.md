# embedded-queue
Features
* designed for embedded use (include the library in a webapp that uses file system for storage)
* exposes HTTP API 
* supports backpressure
* HTTP api OR duplex TCP
* HTTP GET for consumer (long poll or short poll)
* HTTP POST for request, cancel, put  
* writes (calls to`put) must be serialized (?)
* supports multiple concurrent consumers
* uses Kafka-like reads (give me all records since time t/offset)
* maximize use of non-blocking (persistence guarantee means that writes block but reads can use non-blocking
* focus on correctness then speed (while maintaining correctness)
