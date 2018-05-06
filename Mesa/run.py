from Mesa.model import MoneyModel
import matplotlib.pyplot as plt
import numpy as np
import time

TestModel = MoneyModel(200, "TestModel")
for i in range(5):
    TestModel.step()

time.sleep(5)
TestModel.stop()