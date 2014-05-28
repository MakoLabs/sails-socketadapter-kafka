module.exports = {
    'sails-socketadapter-kafka': {
	// REQUIRED
	// The Kafka broker configuration (used by both the producer and the consumer)
	brokers: [{host: 'localhost', port: 9092}]
	
	// REQUIRED
	// The Kafka topic that messages will be sent and received on.
	, topic: "dpic"
	
	// OPTIONAL
	// Sets a nodeId that identifies this specific node
	// , nodeId: "node-1234567",
	
	// OPTIONAL
	// Sets the maximum number of bytes that can be passed via Kafka
	// , maxBytes: 2000000
	
	// OPTIONAL
	// Sets the packet encoding used for messages sent via Kafka
	// By default JSON.stringify and JSON.parse are used. This lets
	// a custom encoding be used. The pack method MUST accept a generic JavaScript value and return a String, and the unpack method MUST accept a String and return a JavaScript value.
	// , pack: JSON.stringify, unpack: JSON.parse
    }
};