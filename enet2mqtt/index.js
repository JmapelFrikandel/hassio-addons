#!/usr/bin/env node

const log = require('yalm');
const Mqtt = require('mqtt');
const eNet = require('node-enet-api');
const config = require('./config.js');
const haconfig = require('./config.json');
const pkg = require('./package.json');

// Declare variables
let mqtt;
var mqttConnected = false;
var enetConnected = false;
var enetAddress;
var gw;

// Fill options from optoins.json (using variables passed to config.js)
var mqtt_ip = 'mqtt://test:test@' + config.mqtt_ip
var enet_ip = config.enet_ip
var log_level = config.log_level
var channelArray = config.channelArray

// Start script with info logs
log.setLevel(log_level);
log.info(pkg.name + ' ' + haconfig.version + ' starting');

log.info("enet_ip: " + enet_ip + " mqtt_ip: " + mqtt_ip + " log_level: " + log_level)


// Manually connect to gateway
gw = eNet.gateway({host: enet_ip});
enetAddress = gw.host;
gw.idleTimeout = 600000;

gw.connect();

// Get gateway version
log.info("Requesting gateway version.");
gw.getVersion(function(err, res) {
    if (err) log.error("error: " + err);
    else log.debug("command succeeded: \n" + JSON.stringify(res));
    enetConnected = true;
});

// Get channel info
log.info("Requesting Channel Info");
gw.getChannelInfo(function(err, res) {
    if (err) log.error("error: " + err);
    else {
        //channelArray = []
        log.debug("Channel Info command succeeded: \n" + JSON.stringify(res));

        // TODO: this has to finish before connected can be called.
        // log.info("Setting channelArray");
        // for (i = 0; i < res.length; i++) {
        //     if (res[i] === 1) channelArray += i;
        // }
        // wait(3);
        log.info("channelArray = " + channelArray)
        connected();
    }
});


// Process discovered gateway
function connected()
{
    // Get project listStyleType
    log.info("Requesting Project List");
    gw.getProjectList(function(err, res) {
        if (err) log.error("error: " + err);
        else log.debug("command succeeded: \n" + JSON.stringify(res));
    });

    // Trying to get all data from signed in channels
    gw.client.on('data', function(data) {
        this.data += data;

        log.debug('this.data looks like: ' + this.data);
        var arr = this.data.split("\r\n\r\n");
        log.debug('split data (arr) looks like: ' + arr);

        this.data = arr[arr.length -1];
        log.debug('updated this.data looks like: ' + this.data);

        for (var i = 0; i < arr.length -1; ++i) { //TODO: on first connection to the gateway, an error is thrown
            try{
                var json=JSON.parse(arr[i]);
                //publish dimmer and switch states on mqtt
                if (!(json.VALUES === undefined)){
                    // This creates a lot of errors:
                    log.debug("Value that passed JSON test: " + JSON.stringify(json));
					log.info("Publishing updated info from gateway");
                    for (var i = 0; i < json.VALUES.length; i++){
                        mqttPublish('enet/get/dimmer/'+json.VALUES[i].NUMBER  , json.VALUES[i].VALUE, {retain: config.mqttRetain});
                        mqttPublish('enet/get/switch/'+json.VALUES[i].NUMBER  , json.VALUES[i].STATE, {retain: config.mqttRetain});
                    }
                }
            }
            catch(e){
                log.error(e);
            }
        }
    }.bind(this));

    // Sign in to channels if connection to enet is lost
    gw.client.on('close', function() {
        signIn(channelArray);
    });

    // Sign in every 5 minutes
    (function(){
        signIn(channelArray);
        setTimeout(arguments.callee, 300000);
    })();


    // Connect to mqtt
    log.info('mqtt trying to connect', mqtt_ip);

    mqtt = Mqtt.connect(mqtt_ip, {
        clientId: config.name + '_' + Math.random().toString(16).substr(2, 8),
        will: {topic: config.name + '/connected', payload: '0', retain: (config.mqttRetain)},
        rejectUnauthorized: !config.insecure
    });

	// Log mqtt connection succeeded
    mqtt.on('connect', () => {
        mqttConnected = true;
        log.info('mqtt connected', mqtt_ip);
        mqtt.publish(config.name + '/connected', enetConnected ? '2' : '1', {retain: config.mqttRetain});
        log.info('mqtt subscribe', config.name + '/set/#');
	      mqtt.subscribe(config.name + '/set/#');
    });

	// On mqtt connection closed
    mqtt.on('close', () => {
        if (mqttConnected) {
            mqttConnected = false;
            log.info('mqtt closed ' + mqtt_ip);
        }
    });

	// Log mqtt errors
    mqtt.on('error', err => {
        log.error('mqtt', err.toString());
    });

	// Log mqtt server offline
    mqtt.on('offline', () => {
        log.error('mqtt offline');
    });

	// Log reconnect attempts
    mqtt.on('reconnect', () => {
        log.info('mqtt reconnect');
    });

	// On a new message on the MQTT topic, call value on gateway
    mqtt.on('message', (topic, payload) => {
		log.info("New MQTT message found on " + topic)
        payload = payload.toString();
        log.debug('mqtt <', topic, payload);

        if (payload.indexOf('{') !== -1) {
            try {
                payload = JSON.parse(payload);
            } catch (err) {
                log.error(err.toString());
            }
        } else if (payload === 'false') {
            payload = false;
        } else if (payload === 'true') {
            payload = true;
        } else if (!isNaN(payload)) {
            payload = parseFloat(payload);
        }
        const [, method, type, name, datapoint] = topic.split('/');

        switch (method) {
            case 'set':
                switch (type) {
                    case 'dimmer':
                        setValue(type, name, payload);
                        break;
                    case 'switch':
                        switch(payload) {
                            case 'ON':
                                setValue(type, name, 100);
                                break;
                            case 'OFF':
                                setValue(type, name, 0);
                                break;
                            default:
                            log.error('unknown type', type);
                        }
                        break;
                    default:
                        log.error('unknown type', type);
                }
                break;
            default:
                log.error('unknown method', method);
        }
    });
}

// Function to set a value on the gateway
function setValue(type, name, payload) {
    log.info("Setting " + type + " " + name + " to: " + payload);
    gw.setValueDim(name, payload, function(err, res) {
        if (err) log.error("error: " + err);
        else {
            log.info("Channel command succeeded: \n" + JSON.stringify(res));
        }
    });
};

// Function to sign in to gateway
function signIn(name) {
    gw.signIn(name, function(err, res) {
    if (err) log.error("sign in error: " + err);
    else log.info("sign in succeeded: \n" + JSON.stringify(res));
    });
};

// Function to publish state to mqtt
function mqttPublish(topic, payload, options) {
    if (!payload) {
        payload = '';
    } else if (typeof payload !== 'string') {
        payload = JSON.stringify(payload);
    }
    log.debug('mqtt >', topic, payload);
    mqtt.publish(topic, payload, options);
};
