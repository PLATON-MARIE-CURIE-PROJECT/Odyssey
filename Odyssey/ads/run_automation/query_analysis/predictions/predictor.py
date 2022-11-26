import matplotlib.pyplot as plt
from numpy import arange
import array
import statistics
import math 
import numpy as np

from scipy.optimize import curve_fit

a = 0.16286445145490303
b = 1.54759136535805
#0.16286445145490303 1.54759136535805 yandex
#0.013854049942186662 1.2547488012097172 sift
#0.0027797186783484577 0.07796044557898549 astro
#0.04543937712383438 -0.7304608299064351
bsf_file = "./bsfs/_new_YANDEX100_initial_bsfs.txt"

pred_file = "./query_execution_time_estimations_yandex100.txt"
f = open(pred_file, 'w')

def linear(x):
    res =  a*x + b
    if(res < 0):
        res = 0.00001 #fix
    return res

with open(bsf_file) as file:
    for line in file:
        bsf_value = float(line)
        prediction = linear(bsf_value)
        f.write(str(prediction) + "\n")



f.close()  
