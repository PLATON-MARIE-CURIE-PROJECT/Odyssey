import sys
import os
import subprocess

from os import path
from sys import argv


filePath = "./seismic_query5000_execution_times/"
querySize = 5000

files = os.listdir(filePath)
runs = len(files)

print(files)
finalQueryTimes = []

for i in range(querySize):
    finalQueryTimes.append(0)

for file in files:
    currentFile = open(str(filePath+file))
    lines = currentFile.readlines()

    queryNum = 0
    for line in lines:

        if line == '\n':
            continue

        lineVal = float(line)

        finalQueryTimes[queryNum] += float("{:.6f}".format((lineVal/runs)))
        #print(lineVal ,"->", finalQueryTimes[queryNum])
        
        queryNum = queryNum + 1



f = open("official_execution_times/query_execution_time_estimations_seismic2.txt", "a")
for value in finalQueryTimes:
    num = float("{:.6f}".format(value))
    print(num)
    f.write(str(num) + "\n")
f.close()
        
        

            
        