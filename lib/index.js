module.exports = function(config, io){
    var kafkaConfig = config['sails-socketadapter-kafka'];
    var KafkaStore = require("./kafkastore");
    io.set('store', new KafkaStore(kafkaConfig));
};