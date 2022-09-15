import matplotlib.pyplot as plt
from numpy import arange
import array
import statistics
import math 
import numpy as np

from scipy.optimize import curve_fit


def plot_data(x,y, filepath, title, color, labelX, labelY):
    plt.scatter(x,y, facecolors='none', edgecolors=color)
    plt.title(title)
    plt.xlabel(labelX)
    plt.ylabel(labelY)
    plt.savefig(filepath)
    plt.close()


def linear(x,a,b):
    return b + a*x

def polynomial_2(x,a, b, c):
    return a * x + b * x**2 + c


def polynomial_5(x, a, b, c, d, e, f):
	return (a * x) + (b * x**2) + (c * x**3) + (d * x**4) + (e * x**5) + f


def sig_parameterized(x, m, M, b, c, d):
    return m + (M-m) / (1 + b * np.exp(-c*(x-d)))

def fit(x_a,y, plot_filepath, plot_title, plot_label_X, plot_label_Y, func):
    x = [x+1 for x in x_a]
    popt, pcov = curve_fit(func, x, y)
    x_line = arange(min(x), max(x), 1)
    y_line = func(x_line, *popt)

    print(*popt)

    plt.scatter(x,y, facecolors='none', edgecolors='blue')
    plt.title(plot_title)
    plt.xlabel(plot_label_X)
    plt.ylabel(plot_label_Y)
    plt.plot(x_line,y_line, '--', color='red', label='linear-regression')
    plt.legend()
    plt.savefig(plot_filepath)
    plt.close()


filename = "./input100astro.txt"
bsfMappings = {}

with open(filename) as file:
    line_counter = 0
    current_query = 0
    current_bsf = 0
    current_exe_time = 0
    for line in file:
        
        if line_counter == 0:
            line_counter += 1
            continue 

        raw = line.rstrip().split()
        line_vals = [float(x) for x in raw]
        
        bsf_id = line_vals[1]
        if bsf_id == 0:
            current_bsf = line_vals[2]

        if bsf_id == 0:
            current_exe_time = line_vals[4]
            bsfMappings[current_bsf] = current_exe_time/1000 #seconds
        
        line_counter += 1

#print(bsfMappings)

#plot_data(list(bsfMappings.keys()), list(bsfMappings.values()), "test.png", "t", "blue","t","t")
fit(list(bsfMappings.keys()), list(bsfMappings.values()), "./plots/astro.png", "Total Execution Time vs initial BSF", "1st BSF of Query", "Total Execution Time (seconds)", linear)


