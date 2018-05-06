from Mesa_distributed.model import DistributedModel
import time

model_name2 = "model2"

Model_2 = DistributedModel([3, 4], model_name2)

for i in range(5):
    while Model_2.count_step > Model_2.global_step:
        time.sleep(0.5)
    time.sleep(2)
    Model_2.step()

time.sleep(5)
Model_2.stop()