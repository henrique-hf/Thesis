from mesa import Agent, Model
from mesa.time import RandomActivation
import json
from MQTT.mqtt_client import MqttClient
import time
import logging
import os

BROKER_ADDRESS = "iot.eclipse.org"
LOG_LEVEL = logging.DEBUG


class Aggregator(Agent):

    def __init__(self, unique_id, model):
        super().__init__(unique_id, model)
        self.logger = self.model.logger
        self.mqtt = self.model.mqtt
        self.topic = "test/mesa/aggregator/%s" % self.unique_id
        self.count_step = 1
        self.logger.info("%s created (%s)" % (self.unique_id, self.topic))

    def step(self):
        # The agent's step will go here.
        self.count_step += 1
        self.mqtt.publish(self.topic, self.build_payload())
        self.logger.debug("Round: %d - Agent %s" % (self.count_step, self.unique_id))

    def build_payload(self):
        payload = {
            "fromAgent": self.unique_id,
            "step": self.count_step,
            "topic": self.topic
        }
        result = json.dumps(payload)
        return result


class DistributedModel(Model):
    """A model with some number of agents."""

    def __init__(self, num_aggregators, model_name):
        self.num_aggregators = num_aggregators
        self.model_name = model_name
        self.schedule = RandomActivation(self)
        self.count_step = 0
        self.global_step = 0

        log_path = "./log/%s.log" % self.model_name

        formatter = logging.Formatter(
            self.model_name + ": " + "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")

        if not os.path.exists(log_path):
            try:
                os.makedirs(os.path.dirname(log_path))
            except Exception as e:
                pass

        self.logger = logging.getLogger(self.model_name)
        self.logger.setLevel(LOG_LEVEL)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        file_handler = logging.FileHandler(log_path, 'w')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        self.mqtt = MqttClient(self.model_name, self.logger, self)
        self.mqtt.start(BROKER_ADDRESS)
        time.sleep(2)

        # Create agents
        self.dict_aggregators = {}
        for i in range(self.num_aggregators[0], self.num_aggregators[1]):
            a_name = "aggregator" + str(i)
            a = Aggregator(a_name, self)
            self.schedule.add(a)
            self.dict_aggregators[a_name] = a

        # subscribe to control
        self.mqtt.subscribe("test/mesa/control", 2)

    def step(self):
        """Advance the model by one step."""
        self.count_step += 1
        self.schedule.step()

    def notify(self, topic, payload):
        payload_dict = json.loads(payload.decode("utf-8"))
        self.global_step = payload_dict["controlStep"]

    def stop(self):
        self.mqtt.stop()