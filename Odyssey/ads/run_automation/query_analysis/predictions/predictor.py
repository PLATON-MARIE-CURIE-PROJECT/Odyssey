import matplotlib.pyplot as plt
from numpy import arange
import array
import statistics
import math 
import numpy as np

from scipy.optimize import curve_fit

a = 0.0027797186783484577
b = 0.07796044557898549

#0.0027797186783484577 0.07796044557898549
bsf_file = "./bsfs/astro1K_bsfs.txt"

pred_file = "./predicted_execution_times/astro_1K.txt"
f = open(pred_file, 'w')

def linear(x):
    res =  a*x + b
    if(res < 0):
        res = 0.00001
    return res

with open(bsf_file) as file:
    for line in file:
        bsf_value = float(line)
        prediction = linear(bsf_value)
        f.write(str(prediction) + "\n")



f.close()  
