from MQTT.mqtt_client import MqttClient
import json
import logging
import os
import time

BROKER_ADDRESS = "iot.eclipse.org"
LOG_LEVEL = logging.DEBUG


class Control(object):
    def __init__(self):
        self.control_step = 0
        self.dict_aggregators = {}

        log_path = "./log/control.log"
        formatter = logging.Formatter(
            "Control: " + "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")

        if not os.path.exists(log_path):
            try:
                os.makedirs(os.path.dirname(log_path))
            except Exception as e:
                pass

        self.logger = logging.getLogger("control")
        self.logger.setLevel(LOG_LEVEL)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        file_handler = logging.FileHandler(log_path, 'w')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        self.mqtt = MqttClient("control", self.logger, self)
        self.mqtt.start(BROKER_ADDRESS)

    def get_aggr(self):
        pass

    def create_dict(self, list_aggregators):
        for aggr in list_aggregators:
            self.dict_aggregators[aggr] = 0

    def subs_aggr(self):
        for aggr in self.dict_aggregators.keys():
            topic = "test/mesa/aggregator/" + aggr
            self.mqtt.subscribe(topic)
            self.logger.debug("Control subscribed to: %s" % topic)

    def notify(self, topic, payload):
        print("Notified")
        payload_dict = json.loads(payload.decode("utf-8"))
        aggr = payload_dict["fromAgent"]
        step = payload_dict["step"]
        self.logger.debug("Message received from %s: step %d" % (aggr, step))
        self.update_aggr_step(aggr, step)
        self.update_control_step()

    def update_aggr_step(self, aggr, new_step):
        self.dict_aggregators[aggr] = new_step
        self.logger.debug("Updated %s to step %d" % (aggr, new_step))

    def update_control_step(self):
        next_step = True
        for key, value in self.dict_aggregators.items():
            if value <= self.control_step:
                next_step = False
                self.logger.debug("%s is still in step %d" % (key, value))
        if next_step:
            self.control_step += 1
            self.logger.debug("Moving to next step: %d" % self.control_step)
            self.publish_control_step()

    def publish_control_step(self):
        topic = "test/mesa/control"
        payload = json.dumps({"controlStep": self.control_step})
        self.mqtt.publish(topic, payload)

    def stop(self):
        self.mqtt.stop()


if __name__ == '__main__':
    control = Control()
    time.sleep(1)

    # in the future the list of aggregators will come from a webservice (get_aggr)
    list_aggr = ["aggregator1", "aggregator2", "aggregator3"]

    control.create_dict(list_aggr)
    control.subs_aggr()

    for i in range(60):
        time.sleep(5)

    control.stop()
