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


def fit(x_a,y, plot_filepath, plot_title, plot_label_X, plot_label_Y, func):
    x = [x+1 for x in x_a]
    popt, pcov = curve_fit(func, x, y)
    x_line = arange(min(x), max(x), 1)
    y_line = func(x_line, *popt)

    print(*popt)
    plt.rcParams.update({'font.size': 13})
    plt.scatter(x,y, facecolors='none', edgecolors='blue')#, label="data")
    plt.title(plot_title)
    plt.xlabel(plot_label_X)
    plt.ylabel(plot_label_Y)
    plt.plot(x_line,y_line, '--', color='red')#,label = "fit")
    #plt.legend()
    plt.savefig(plot_filepath)
    plt.close()


def sig_parameterized(x, m, M, b, c, d):
    return m + (M-m) / (1 + b * np.exp(-c*(x-d)))


def sigmoid(x, L, x0, k, b0):
    y = L / (1 + np.exp(-k*(x-x0))) + b0
    return y


def polynomial_2(x,a, b, c):
    return a * x + b * x**2 + c


def polynomial_5(x, a, b, c, d, e, f):
	return (a * x) + (b * x**2) + (c * x**3) + (d * x**4) + (e * x**5) + f


def log_par(x, a1, a2):
    return a1*math.log(x) + a2


def fit_log(x,y, plot_savepath):
    arr = np.polyfit(np.log(x), y, 1)
    a1 = arr[0]; a2 = arr[1]
    x_line = arange(min(x), max(x), 1)
    y_line = log_par(x_line, a1, a2)

    plt.scatter(x,y, facecolors='none', edgecolors='blue')
    plt.plot(x_line,y_line, '--', color='red')
    plt.savefig(plot_savepath)


filename = "./inputs/pq_stats_100simple.txt"
bsfMappings = {}

with open(filename) as file:
    line_counter = 0
    current_query = 0
    
    for line in file:
        
        if line_counter == 0:
            line_counter += 1
            continue 

        raw = line.rstrip().split()
        line_vals = [float(x) for x in raw]

        bsfMappings[line_vals[1]] = line_vals[4:]
        line_counter += 1

means = {}
stddevs = {}
avgs = {}
medians = {}

counter = 0
for key in bsfMappings:
    
    if counter == 500:
        break

    currBSF = key
    #if currBSF<15:
    #    continue
    vals = bsfMappings[key]
    
    mean = statistics.mean(vals)
    median = statistics.median(vals)
    avg = 1.0*sum(vals) / len(vals)
    #stddev = statistics.stdev(vals)

    means[currBSF] = mean
    avgs[currBSF] = avg
    #stddevs[currBSF] = stddev
    medians[currBSF] = median

    counter += 1

#plot_data(list(medians.keys()), list(medians.values()), "./plots/random_median.png", "Random - Medians vs BSF", "blue","1st BSF of Query","Median of the sizes of Priority Queues")

#fit(list(medians.keys()), list(medians.values()), "./plots/median.png", "Medians vs BSF", "1st BSF of Query", "Median of the sizes of Priority Queues", sig_parameterized)
#fit(list(avgs.keys()), list(avgs.values()), "./plots/avgs.png", "Averages vs BSF", "1st BSF", "Average size of pq", sig_parameterized)

fit(list(medians.keys()), list(medians.values()), "./plots/seismicMedian_new.png", "Medians vs BSF", "1st Query BSF", "Median sizes of Priority Queues", sig_parameterized)
