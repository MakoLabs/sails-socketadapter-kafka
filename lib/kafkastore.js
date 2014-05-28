/*
 * Copyright(c) 2014 FMR, Inc. <olufemiomojola@fmr.com>
 */

/**
 * Module dependencies.
 */

var crypto = require('crypto')
, Store = require('socket.io/lib/store')
, assert = require('assert')
, uuid = require('uuid')
, utils = require('util');

/**
 * Exports the constructor.
 */

exports = module.exports = Kafka;
Kafka.Client = Client;

/**
 * Kafka store.
 * Options:
 *     - nodeId (fn) gets an id that uniquely identifies this node
 *     - brokers (Array) is the Kafka broker information to use, defaults to [{host: 'localhost', port: 9092}]
 *     - topic (String) is the Kafka topic to listen on, defaults to "sails-topic" if not specified.
 *     - partition (number) is the Kafka partition to use
 *     - pack (fn) custom packing, defaults to JSON
 *     - unpack (fn) custom packing, defaults to JSON
 *
 * @api public
 */
function Kafka (opts) {
    opts = opts || {};

    // node id to uniquely identify this node
    var nodeId = opts.nodeId || function () {
	// by default, we generate a random id 
	return uuid.v4();
    };
    
    this.nodeId = nodeId();
    this.topic = opts.topic || 'sails-topic.'
    this.partition = opts.partition || 0;
    this.subs = {};

    // packing / unpacking mechanism
    if (opts.pack) {
	this.pack = opts.pack;
	this.unpack = opts.unpack;
    } else {
	this.pack = JSON.stringify;
	this.unpack = JSON.parse;
    }
    
    var kafkaesque = require('kafkaesque');
    this.sender = kafkaesque({
	brokers: opts.brokers || [{host: 'localhost', port: 9092}],
	clientId: this.nodeId+"-producer",
	maxBytes: opts.maxBytes || 2000000
    });
    
    var self = this;
    self.sender_ready = false;
    self.pending_publications = [];
    this.sender.tearUp(function(){
	console.log("[sender/tearUp] Ready");
	self.sender_ready = true;	
	if(self.pending_publications.length != 0){
	    var i, publications = self.pending_publications, pubs;
	    for(var i=0;i<publications.length;i++){
		pubs = publications[i];
		self.process_publication(pubs[0], pubs[1]);
	    }
	    self.pending_publications = [];
	}
    });
    
    this.receiver = kafkaesque({
	brokers: opts.brokers || [{host: 'localhost', port: 9092}],
	clientId: this.nodeId+"-consumer",
	group: this.nodeId,
	maxBytes: opts.maxBytes || 2000000
    });
    
    // setup the receiver
    this.receiver_ready = false;
    this.pending_subscriptions = [];
    this.receiver.tearUp(function(){
	console.log("[receiver/tearUp] Ready");
	self.receiver_ready = true;
	
	var handle_subscription = function(err, kafka)
	{
	    if(err){
		if(err.message.indexOf("Leader not elected") === 0){
		    // need to wait: wait 5 seconds
		    setTimeout(function(){
			self.receiver.poll({ topic: self.topic, partition: self.partition }, handle_subscription);
		    }, 5000);
		    return;
		}
		return;
	    }
	    
	    kafka.on('message', function(message, commit){
		// process it
		var msg = self.unpack(message.value);
		
		// don't bother if we sent it
		if(msg.nodeId != self.nodeId && msg.name in self.subs){
		    self.subs[msg.name].apply(null, msg.args);
		}
		
		// then commit it so we don't see it again
		commit();
	    });
	    
	    // report errors
	    kafka.on('error', function(error) {
		console.log(JSON.stringify(error));
	    });
	};
	
	self.receiver.poll({ topic: self.topic, partition: self.partition }, handle_subscription);
    });
    
    this.process_publish = function(name, args)
    {
	self.sender.produce({topic: self.topic, partition: this.partition }, 
			    [ self.pack({ nodeId: this.nodeId, name: name, args: args }) ], function(err, response){
				// TODO: process the error and response bits
				if(err){
				    console.log("[process_publish] Error publishing event:"+err);
				}
			    });
	self.emit.apply(self, ['publish', name].concat(args));	
    };
    
    this.process_subscription = function(name, consumer, fn)
    {
	if (consumer){
	    /*
	    if(name in self.subs){
		console.log("["+self.nodeId+"/"+name+"] REUSE");
		self.subs[name].consumers.push(consumer);
		fn && fn();
		return;
	    }
	    */
	    if(name in this.subs) console.log("[process_subscription] collision "+name);
	    
	    this.subs[name] = consumer;

	    fn && fn();	    
	}
	
	self.emit('subscribe', name, consumer, fn);
    };
    Store.call(this, opts);
};

/**
 * Inherits from Store.
 */

Kafka.prototype.__proto__ = Store.prototype;

/**
 * Publishes a message.
 *
 * @api private
 */

Kafka.prototype.publish = function (name) 
{
    var args = Array.prototype.slice.call(arguments, 1);
    if(this.sender_ready){
	this.process_publish(name, args);
    }else{
	this.pending_publications.push([name, args]);
    }
};

/**
 * Subscribes to a channel
 *
 * @api private
 */
Kafka.prototype.subscribe = function (name, consumer, fn) 
{
    this.process_subscription(name, consumer, fn);
};

/**
 * Unsubscribes
 *
 * @api private
 */

Kafka.prototype.unsubscribe = function (name, fn)
{
    if(name in this.subs){
	delete this.subs[name];
    }
    fn && fn();

    this.emit('unsubscribe', name, fn);
};

/**
 * Destroys the store
 *
 * @api public
 */

Kafka.prototype.destroy = function () {
    Store.prototype.destroy.call(this);
    this.sender.tearDown();
    this.receiver.tearDown();
};

/**
 * Client constructor
 *
 * @api private
 */

function Client (store, id) {
    this.hash = {};
  Store.Client.call(this, store, id);
};

/**
 * Inherits from Store.Client
 */

Client.prototype.__proto__ = Store.Client;

/**
 * Hash get
 *
 * @api private
 */

Client.prototype.get = function (key, fn) {
    console.log("-----------------------------------------------[client_get] "+this.nodeId+"/"+key+"]");
    if(fn){
	if(key in this.hash) fn(null, this.hash[key].value);
	else fn(null, null);
    }
    //this.store.cmd.hget(this.id, key, fn);
  return this;
};

/**
 * Hash set
 *
 * @api private
 */

Client.prototype.set = function (key, value, fn) {
    console.log("-----------------------------------------------[client_set] "+this.nodeId+"/"+key+"/"+value+"]");
    this.hash[key] = { value: value };
    if(fn) fn(null);
  //this.store.cmd.hset(this.id, key, value, fn);
  return this;
};

/**
 * Hash del
 *
 * @api private
 */

Client.prototype.del = function (key, fn) {
    console.log("-----------------------------------------------[client_del] "+this.nodeId+"/"+key+"]");
    if(key in this.hash) delete this.hash[key];
    fn(null);
  //this.store.cmd.hdel(this.id, key, fn);
  return this;
};

/**
 * Hash has
 *
 * @api private
 */

Client.prototype.has = function (key, fn) {
    console.log("-----------------------------------------------[client_has] "+this.nodeId+"/"+key+"]");
    fn(null, key in this.hash);
    /*
  this.store.cmd.hexists(this.id, key, function (err, has) {
    if (err) return fn(err);
    fn(null, !!has);
  });
    */
    
  return this;
};

/**
 * Destroys client
 *
 * @param {Number} number of seconds to expire data
 * @api private
 */

Client.prototype.destroy = function (expiration){
    console.log("-----------------------------------------------[client_destroy]");
    /*
  if ('number' != typeof expiration) {
    this.store.cmd.del(this.id);
  } else {
    this.store.cmd.expire(this.id, expiration);
  }
*/
  return this;
};
