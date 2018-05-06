from Mesa_distributed.model import DistributedModel
import time

model_name1 = "model1"

Model_1 = DistributedModel([1, 3], model_name1)

for i in range(5):
    while Model_1.count_step > Model_1.global_step:
        time.sleep(0.5)
    Model_1.step()

time.sleep(5)
Model_1.stop()