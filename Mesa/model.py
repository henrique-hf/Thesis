from mesa import Agent, Model
from mesa.time import RandomActivation
from mesa.space import MultiGrid
from mesa.datacollection import DataCollector
import random
import json
from MQTT.mqtt_client import MqttClient
import time
import logging
import os

BROKER_ADDRESS = "iot.eclipse.org"
LOG_LEVEL = logging.DEBUG


class MoneyAgent(Agent):
    """An agent with fixed initial wealth."""

    def __init__(self, unique_id, model):
        super().__init__(unique_id, model)
        self.wealth = 10
        self.logger = self.model.logger
        self.mqtt = self.model.mqtt
        self.topic = "/test/mesa/%s/%s" % (self.model.model_name, self.unique_id)
        self.logger.info("%s created (%s)" % (self.unique_id, self.topic))

    def step(self):
        # The agent's step will go here.
        self.logger.debug("%s has %d" % (self.unique_id, self.wealth))
        if self.wealth > 0:
            self.mqtt.publish(self.topic, self.build_payload())
        else:
            self.logger.debug("%s has no wealth in step %d" % (self.unique_id, self.model.count_step))

    def give_money(self):
        other = random.choice(self.model.schedule.agents)
        # amount = random.randint(0, self.wealth + 1)
        amount = 1
        self.wealth -= amount
        return other.unique_id, amount

    def build_payload(self):
        other_agent, amount = self.give_money()
        payload = {
            "fromAgent": self.unique_id,
            "toAgent": other_agent,
            "step": self.model.count_step,
            "amount": amount,
            "topic": self.topic
        }
        result = json.dumps(payload)
        self.logger.debug(result)
        return result

    def msg_received(self, topic, payload):
        payload_dict = json.loads(payload.decode("utf-8"))
        self.wealth += payload_dict["amount"]
        self.logger.debug("MESSAGE RECEIVED")


class MoneyModel(Model):
    """A model with some number of agents."""

    def __init__(self, N, model_name):
        self.num_agents = N
        self.model_name = model_name
        self.schedule = RandomActivation(self)
        self.count_step = 0

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
        self.dict_agents = {}
        self.topics_list = []
        for i in range(self.num_agents):
            a = MoneyAgent("agent" + str(i), self)
            self.schedule.add(a)

            self.dict_agents["agent" + str(i)] = a
            self.topics_list.append(a.topic)

        for i in self.topics_list:
            self.mqtt.subscribe(i)

    def step(self):
        """Advance the model by one step."""
        time.sleep(1)
        self.count_step += 1
        self.schedule.step()

    def notify(self, topic, payload):
        payload_dict = json.loads(payload.decode("utf-8"))
        if payload_dict["toAgent"] in self.dict_agents:
            self.dict_agents[payload_dict["toAgent"]].msg_received(topic, payload)
        else:
            self.logger.error("Agent %s not found" % payload_dict["toAgent"])

    def stop(self):
        self.mqtt.stop()
