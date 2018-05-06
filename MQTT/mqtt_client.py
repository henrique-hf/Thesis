import paho.mqtt.client as mqtt
import threading
import os
import logging
import time
import json


class MqttClient(object):
    def __init__(self, client_id, logger, notifier, clean_flag=True):
        self._client = mqtt.Client(client_id, clean_flag)
        self.logger = logger
        self.notifier = notifier    # what's notifier?
        self.__lock = threading.Lock()  # why __ ?

        # Bind callbacks
        self._client.on_connect = self.on_connect
        self._client.on_disconnect = self.on_disconnect
        self._client.on_message = self.on_message
        # self._client.on_log = self.on_log
        self._client.on_publish = self.on_publish
        self._client.on_subscribe = self.on_subscribe
        self._client.on_unsubscribe = self.on_unsubscribe

    def on_connect(self, client, userdata, flags, rc):
        self.logger.info("Connected to broker with result code: " + str(rc))

    def on_disconnect(self, client, userdata, rc):
        self.logger.info("Disconnected from broker with code: " + str(rc))

    def on_message(self, client, userdata, msg):
        self.logger.debug("Message received from: " + msg.topic + " qos: " + str(msg.qos) + " message: " + str(msg.payload))
        self.notifier.notify(msg.topic, msg.payload)

    def on_log(self, client, userdata, level, buf):
        self.logger.debug("Log: " + str(buf))

    def on_subscribe(self, client, userdata, mid, granted_qos):
        self.logger.debug("Subscribed: " + str(mid) + " qos: " + str(granted_qos))

    def on_publish(self, client, userdata, mid):
        self.logger.debug("mid: " + str(mid))

    def on_unsubscribe(self, client, userdata, mid):
        self.logger.debug("Unsubscribed: " + str(mid))

    def start(self, broker_address, port=1883, keepalive=60):
        try:
            self._client.connect(broker_address, port, keepalive)
            self._client.loop_start()    # decide if it's better to use loop_start or loop
        except Exception as e:
            self.logger.error("Error when starting client", e)

    def stop(self):
        try:
            self.logger.info("Disconecting from message broker")
            self._client.loop_stop()
            self._client.disconnect()
        except Exception as e:
            self.logger.error("Error when disconecting from broker", e)

    def publish(self, topic, payload, qos=2, retain=False):
        self.__lock.acquire()
        self._client.publish(topic, payload, qos, retain)
        self.__lock.release()
        self.logger.debug('Publishing to topic: "%s" with msg: %s ' % (topic, str(payload)))

    def subscribe(self, topic, qos=2):
        try:
            self._client.subscribe(topic, qos)
            self.logger.info("Subscribed to: " + topic + " qos: " + str(qos))
        except Exception as e:
            self.logger.error("Error on subscribing to " + topic + " error:", e)

    def unsubscribe(self, topic):
        try:
            self._client.unsubscribe(topic)
            self.logger.info("Unsubscribed to: " + topic)
        except Exception as e:
            self.logger.error("Error on unsubscribing to " + topic + " error:", e)


if __name__ == '__main__':
    broker_address = 'iot.eclipse.org'
    # broker_address = '127.0.0.1'
    topic = "test/mqtt_log/test1"

    test_name = "TestMQTT"
    log_path = "./log/%s.log" % test_name
    log_level = logging.DEBUG
    formatter = logging.Formatter(
        test_name + ": " + "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")

    if not os.path.exists(log_path):
        try:
            os.makedirs(os.path.dirname(log_path))
        except Exception as e:
            pass

    logger = logging.getLogger(test_name)
    logger.setLevel(log_level)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler(log_path, 'w')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    client = MqttClient("client1", logger, None)
    client.start(broker_address)    # connect and start loop
    time.sleep(1)
    # client.subscribe(topic)
    # time.sleep(2)
    # client.publish(topic, '23', 2)
    # time.sleep(2)
    #
    # # test multiple subscriptions to the same topic
    # client.subscribe(topic)
    # time.sleep(2)
    #
    # client.publish(topic, 25, 2)
    # time.sleep(5)
    # client.stop()   # stop loop and disconnect

    # TESTING CONTROL
    topic1 = 'test/mesa/aggregator/aggregator1'
    topic2 = 'test/mesa/aggregator/aggregator2'

    payload1 = {
        "fromAgent": "aggregator1",
        "step": 1,
    }
    payload2 = {
        "fromAgent": "aggregator2",
        "step": 1,
    }

    client.publish(topic1, json.dumps(payload1))
    client.publish(topic2, json.dumps(payload2))

    payload1 = {
        "fromAgent": "aggregator1",
        "step": 2,
    }
    payload2 = {
        "fromAgent": "aggregator2",
        "step": 2,
    }

    client.publish(topic2, json.dumps(payload2))
    client.publish(topic1, json.dumps(payload1))

    time.sleep(10)
    client.stop()