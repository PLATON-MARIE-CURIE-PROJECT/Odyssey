import sys
import os
import subprocess
import statistics
import copy
import getopt
import json

from os import path
from sys import argv
from unittest import result

import numpy
import matplotlib.pyplot as plt


conf = json.load(open("./configuration.json", encoding="utf8"))


def executeCommand(command):
    print(">>>",command)
    out = subprocess.getoutput(command)
    print(out)


def exportVar(name,value):
    os.environ[name] = value


def singleDataPlot(listX,listY,title,path,labelX,labelY):
    plt.rcParams.update({'font.size': 13})

    xvals = listX
    yvals = listY
    plt.plot(yvals, xvals, '-o', color="b")


    plt.xlabel(labelX)
    plt.ylabel(labelY)
    plt.title(title)
  
    plt.savefig(path)
    plt.close()


def doubleBarQAgen(nodes,dict1,dict2,path,title, dict1Lab, dict2Lab):
    labels = nodes
    
    print(labels)
    
    x = numpy.arange(len(labels))  # the label locations
    width = 0.35  # the width of the bars

    fig, ax = plt.subplots()
    rects1 = ax.bar(x - width/2, dict1, width, label=dict1Lab, color = 'moccasin')
    rects2 = ax.bar(x + width/2, dict2, width, label=dict2Lab, color = 'cornflowerblue')

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Seconds')
    ax.set_xlabel('Batches')

    ax.set_title(title)
    plt.xticks(x, labels)
    ax.legend()

    ax.bar_label(rects1, padding=3)
    ax.bar_label(rects2, padding=3)

    fig.tight_layout()
    plt.savefig(path)
    #plt.show()
    plt.close(fig)
    

def doubleBarQA(nodes,swBSF,noswBSF,path,title):
    labels = nodes
    
    print(labels)
    print(swBSF)
    print(noswBSF)

    x = numpy.arange(len(labels))  # the label locations
    width = 0.35  # the width of the bars

    fig, ax = plt.subplots()
    rects1 = ax.bar(x - width/2, swBSF, width, label='SW-BSF', color = 'moccasin')
    rects2 = ax.bar(x + width/2, noswBSF, width, label='Classic', color = 'cornflowerblue')

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Seconds', fontsize=11)
    ax.set_xlabel('Nodes',fontsize=11)

    ax.set_title(title)
    plt.xticks(x, labels)
    plt.rc('font', size=11) 
    ax.legend()

    ax.bar_label(rects1, padding=3)
    ax.bar_label(rects2, padding=3)

    fig.tight_layout()
    plt.savefig(path + "_queryanswering")
    #plt.show()
    plt.close(fig)


def calcAstroResultsMaximumsPerRun():
    runs = 5
    datasets2run = [2]
    threads = [[16,64]];bsfShare = ["bsf-dont-share", "bsf-block-per-query"]
    nodes = [2,4,8,16]
    for threadConf in threads:
        processResults("../experiments/classicExperiments/astroExperiments/",datasets2run,bsfShare,runs,nodes,[threadConf])


def calcDeepResultsMaximumsPerRun():
    runs = 5
    datasets2run = [4]
    threads = [[16,64]];bsfShare = ["bsf-dont-share", "bsf-block-per-query"]
    nodes = [2,4,8,16]
    for threadConf in threads:
        processResults("../experiments/classicExperiments/deepExperiments/",datasets2run,bsfShare,runs,nodes,[threadConf])


def calcSaldResultsMaximumsPerRun():
    runs = 4
    datasets2run = [3]
    threads = [[16,64]];bsfShare = ["bsf-dont-share", "bsf-block-per-query"]
    nodes = [4,8,16]
    for threadConf in threads:
        processResults("../experiments/classicExperiments/saldExperiments/",datasets2run,bsfShare,runs,nodes,[threadConf])


def calcSiftResultsMaximumsPerRun():
    runs = 5
    datasets2run = [1]
    threads = [[16,64]];bsfShare = ["bsf-dont-share", "bsf-block-per-query"]
    nodes = [4,8]
    for threadConf in threads:
        processResults("../experiments/classicExperiments/siftExperiments/",datasets2run,bsfShare,runs,nodes,[threadConf])


def calcDistributedQueriesResults(datasetIndex, nodes, runs, threads, resultsPath):

    doStatic = 1
    doClassic = 1
    doSWBSF = 1

    doDynamic = 1
    doDynamicCoordinator = 1
    doDynamicThread = 1
    
    doGreedySorted = 1
    doGreedyUnSorted = 1
    doDynamicThreadSorted = 1

    doDynamicSorted = 1
    doDynamicSortedCoordinator = 1

    dirPrefix = resultsPath + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/static/"
    distributedQueriesTimes = {}
    for currNodes in nodes:
        if doStatic == 0:
            break
        resPath = dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" 
        res = processDirectory(resPath,runs,currNodes,threads)
        distributedQueriesTimes[str(currNodes)] = decimalTwo(res[conf['indexesCollection']['varIndexes']['queryAnsweringTime']])
    
    dirPrefix =  resultsPath + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/classic/"
    classicQueriesTimes = {}
    for currNodes in nodes:
        if doClassic == 0:
            break
        resPath = dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" 
        res = processDirectory(resPath,runs,currNodes,threads)
        classicQueriesTimes[str(currNodes)] = decimalTwo(res[conf['indexesCollection']['varIndexes']['queryAnsweringTime']])
    
    dirPrefix =  resultsPath + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/swbsf/"
    swBSFQueriesTimes = {}
    for currNodes in nodes:
        if doSWBSF == 0:
            break
        resPath = dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" 
        res = processDirectory(resPath,runs,currNodes,threads)
        swBSFQueriesTimes[str(currNodes)] = decimalTwo(res[conf['indexesCollection']['varIndexes']['queryAnsweringTime']])
       
    dirPrefix =  resultsPath + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic/"
    makespanQueriesTimes = {}
    for currNodes in nodes:
        if doDynamic == 0:
            break
        resPath = dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" 
        res = processDirectory(resPath,runs,currNodes,threads)
        makespanQueriesTimes[str(currNodes)] = decimalTwo(res[conf['indexesCollection']['varIndexes']['queryAnsweringTime']])

    dirPrefix =  resultsPath + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic_coordinator/"
    makespanQueriesTimesCoordinator = {}
    realNodes = [x+1 for x in nodes]
    for currNodes in realNodes:
        if doDynamicCoordinator == 0:
            break

        if currNodes == 2:
            makespanQueriesTimesCoordinator["1"] = 0
            continue
        resPath = dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" 
        res = processDirectory(resPath,runs,currNodes,threads)
        makespanQueriesTimesCoordinator[str(currNodes-1)] = decimalTwo(res[conf['indexesCollection']['varIndexes']['queryAnsweringTime']])

    dirPrefix =  resultsPath + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic_thread/"
    dynamicThread = {}
    for currNodes in nodes:
        if doDynamicThread == 0:
            break
        resPath = dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" 
        res = processDirectory(resPath,runs,currNodes,threads)
        dynamicThread[str(currNodes)] = decimalTwo(res[conf['indexesCollection']['varIndexes']['queryAnsweringTime']])

    dirPrefix =  resultsPath + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/greedy_exectimes_sorted/"
    greedySorted = {}
    for currNodes in nodes:
        if doGreedySorted == 0:
            break
        resPath = dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" 
        res = processDirectory(resPath,runs,currNodes,threads)
        greedySorted[str(currNodes)] = decimalTwo(res[conf['indexesCollection']['varIndexes']['queryAnsweringTime']])

    dirPrefix =  resultsPath + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/greedy_exectimes_unsorted/"
    greedyUnSorted = {}
    for currNodes in nodes:
        if doGreedyUnSorted == 0:
            break
        resPath = dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" 
        res = processDirectory(resPath,runs,currNodes,threads)
        greedyUnSorted[str(currNodes)] = decimalTwo(res[conf['indexesCollection']['varIndexes']['queryAnsweringTime']])

    dirPrefix =  resultsPath + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic_sorted/"
    dynamicSorted = {}
    for currNodes in nodes:
        if doDynamicSorted == 0:
            break
        resPath = dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" 
        res = processDirectory(resPath,runs,currNodes,threads)
        dynamicSorted[str(currNodes)] = decimalTwo(res[conf['indexesCollection']['varIndexes']['queryAnsweringTime']])

    dirPrefix =  resultsPath + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic_sorted_coordinator/"
    dynamicSortedCoordinator = {}
    realNodes = [x+1 for x in nodes]
    for currNodes in realNodes:
        if doDynamicSortedCoordinator == 0:
            break
        
        if currNodes == 2:
            dynamicSortedCoordinator["1"] = 0
            continue

        resPath = dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" 
        res = processDirectory(resPath,runs,currNodes,threads)
        dynamicSortedCoordinator[str(currNodes-1)] = decimalTwo(res[conf['indexesCollection']['varIndexes']['queryAnsweringTime']])
    
    dirPrefix =  resultsPath + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic_sorted_thread/"
    dynamicThreadSorted = {}
    for currNodes in nodes:
        if doDynamicThreadSorted == 0:
            break
        resPath = dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" 
        res = processDirectory(resPath,runs,currNodes,threads)
        dynamicThreadSorted[str(currNodes)] = decimalTwo(res[conf['indexesCollection']['varIndexes']['queryAnsweringTime']])

    dynamicThreadSorted["1"] = dynamicThread["1"] = makespanQueriesTimes["1"] = dynamicSortedCoordinator["1"] = dynamicSorted["1"] = classicQueriesTimes["1"] = greedyUnSorted["1"] =  swBSFQueriesTimes["1"] = makespanQueriesTimesCoordinator["1"] = distributedQueriesTimes["1"] = greedySorted["1"]

    #fourBarFig(list(makespanQueriesTimes.values()), list(dynamicThread.values()), list(dynamicSorted.values()), list(dynamicThreadSorted.values()), list(dynamicSorted.keys()), "Nodes", "Seconds", "Dynamic QA Comparisons", "Module", "Thread", "Module Sorted", "Thread Sorted", resultsPath+"dynamic_comparison")
    doubleBarQAgen(list(greedyUnSorted.keys()), list(greedyUnSorted.values()), list(greedySorted.values()), resultsPath + "comparison_greedy", "Comparison of Greedy Sorted and Unsorted", "Unsorted", "Sorted")
    sevenBarFig(list(distributedQueriesTimes.values()), list(makespanQueriesTimes.values()), list(dynamicThread.values()), list(greedyUnSorted.values()), list(greedySorted.values()), list(dynamicSorted.values()), list(dynamicThreadSorted.values()), list(dynamicSorted.keys()),"Nodes", "Seconds", "Comparison of Distributed Query Answering Algorithms", "Static", "D. Module", "D. Thread", "Greedy Unsorted", "Greedy Sorted", "D. Sorted Module", "D. Sorted Thread", resultsPath + "overall_comparison")
    sixBarFig(list(makespanQueriesTimes.values()), list(dynamicThread.values()), list(makespanQueriesTimesCoordinator.values()), list(dynamicSorted.values()), list(dynamicThreadSorted.values()), list(dynamicSortedCoordinator.values()), list(makespanQueriesTimes.keys()), "Nodes", "Seconds", "Dynamic Distributed Query Answering", "Module", "Thread", "Coordinator", "Sorted Module", "Sorted Thread", "Sorted Coordinator", resultsPath+"dynamic_comparison_with_coordinator")
    fiveBarFig(list(distributedQueriesTimes.values()), list(dynamicThread.values()), list(greedyUnSorted.values()), list(greedySorted.values()), list(dynamicThreadSorted.values()), list(dynamicSorted.keys()),"Nodes", "Seconds", "Comparison of Distributed Query Answering Algorithms", "Static", "Dynamic", "Greedy Unsorted", "Greedy Sorted", "Dynamic Sorted", resultsPath + "overall_comparison_simple")


def calcDynamicQueriesResults(datasetIndex, nodes, runs, threads, resultsPath):
    dirPrefix =  resultsPath + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/makespan/"
    makespanQueriesTimes = {}
    for currNodes in nodes:
        resPath = dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" 
        res = processDirectory(resPath,runs,currNodes,threads)
        makespanQueriesTimes[str(currNodes)] = decimalTwo(res[conf['indexesCollection']['varIndexes']['queryAnsweringTime']])
        #v1 = decimalTwo(res[conf['indexesCollection']['varIndexes']['queryAnsweringTime']])
        #v2 = decimalTwo(res[conf['indexesCollection']['varIndexes']['mpiTime']])
        #v3 = decimalTwo(res[conf['indexesCollection']['varIndexes']['isaxBufferTime']])
        #v4 = decimalTwo(res[conf['indexesCollection']['varIndexes']['indexCreationTime']])
        #y = numpy.array([v1,v2,v3,v4])
        #labs = ["QA", "MPI", "Buffers", "Index"]
        #plt.pie(y, labels = labs, startangle = 90)
        #plt.savefig("./pie" + str(currNodes))
        #plt.close()
        #pieV = decimalTwo(res[conf['indexesCollection']['varIndexes']['queryAnsweringTime']])

    singleBarFig(list(makespanQueriesTimes.values()),list(makespanQueriesTimes.keys()), "Nodes", "Seconds", "Dynamic Queries", "", "./dynamic")


def calcSeismicResultsMaximumsPerRun():
    runs = 5
    datasets2run = [0]
    threads = [[16,64]];bsfShare = ["bsf-dont-share", "bsf-block-per-query"]
    nodes = [1,2,4,8,16]
    for threadConf in threads:
        processResults("../experiments/classicExperiments/seismicExperiments/",datasets2run,bsfShare,runs,nodes,[threadConf])


def calcRandomResultsMaximumsPerRun():
    runs = 5
  
    threads = [[16,64]];bsfShare = ["bsf-dont-share", "bsf-block-per-query"]
    
    for threadConf in threads:
        processResults("../experiments/classicExperiments/randomExperiments/random100Experiments/",[6],bsfShare,runs,[1,2,4,8,16],[threadConf])

    for threadConf in threads:
        processResults("../experiments/classicExperiments/randomExperiments/random200Experiments/",[8],bsfShare,runs,[2,4,8,16],[threadConf])

    for threadConf in threads:
        processResults("../experiments/classicExperiments/randomExperiments/random300Experiments/",[10],bsfShare,runs,[2,4,8,16],[threadConf])

    for threadConf in threads:
        processResults("../experiments/classicExperiments/randomExperiments/random400Experiments/",[12],bsfShare,runs,[2,4,8,16],[threadConf])


def calcSingleNodeResults():
    runs = 5
    threads = [[1,1],[2,2],[4,4],[8,8],[16,16],[32,32],[64,64], [128,128], [256,256]]
    nodes = [1]
    
    processResults("../experiments/singleNode/",[0],["bsf-dont-share"],runs,nodes,threads)
    processResults("../experiments/singleNode/",[6],["bsf-dont-share"],runs,nodes,threads)


def calcScalingResults(resultsPath, datasetIndex, runs, threads, chunkSize, chunks):
    
    dirPrefix = resultsPath + str(conf['datasets'][datasetIndex]['name']) + "/scaling/part"+str(chunkSize)+"/"
    
    currSize = chunkSize
    currNodes = 1
    results = {}
    for currTest in range(chunks):
        
        resultFileFolder = dirPrefix + "part" + str(currSize) + "-nodes" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/"
        res = processDirectory(resultFileFolder,runs,currNodes,threads)
        results[str(currNodes)] = res

        currSize *= 2 
        currNodes *= 2
    
    createNodeFigure(results, "test", "./scaling", "name")


def tripleBarFig(list1,list2,list3,labels, xlabel, ylabel, title, list1Label, list2Label, list3Label, path):
    N = len(labels)
    ind = numpy.arange(N) 
    width = 0.25
  
    xvals = list1
    bar1 = plt.bar(ind, xvals, width, color = 'moccasin')
  
    yvals = list2
    bar2 = plt.bar(ind+width, yvals, width, color='cornflowerblue')
  
    zvals = list3
    bar3 = plt.bar(ind+width*2, zvals, width, color = 'red')

    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
  
    plt.bar_label(bar1, list1)
    plt.bar_label(bar2, list2)
    plt.bar_label(bar3, list3)

    plt.xticks(ind+width,labels)
    plt.legend( (bar1, bar2, bar3), (list1Label,list2Label,list3Label) )
    plt.savefig(path)
    plt.close()


def singleBarFig(list1, labels, xlabel, ylabel, title, list1Label, path):
    N = len(labels)
    ind = numpy.arange(N) 
  
    xvals = list1
    bar1 = plt.bar(ind, xvals, color = 'moccasin')
  
    plt.xlabel(xlabel, fontsize=11)
    plt.ylabel(ylabel, fontsize=11)
    plt.title(title)
  
    plt.bar_label(bar1, list1)
    plt.rc('font', size=11) 
    #plt.xticks(ind+width,labels)
    plt.xticks(ind,labels)
    #plt.legend((bar1),(list1Label))
    plt.savefig(path)
    plt.close()


def fourBarFig(list1,list2,list3, list4, labels, xlabel, ylabel, title, list1Label, list2Label, list3Label, list4Label, path):
    N = len(labels)
    ind = numpy.arange(N) 
    width = 0.20
  
    xvals = list1
    bar1 = plt.bar(ind, xvals, width, color = 'moccasin')
  
    yvals = list2
    bar2 = plt.bar(ind+width, yvals, width, color='cornflowerblue')
  
    zvals = list3
    bar3 = plt.bar(ind+width*2, zvals, width, color = 'lightgreen')

    pvals = list4
    bar4 = plt.bar(ind+width*3, pvals, width, color = 'red')
    plt.rc('font', size=6) 
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
  
    plt.bar_label(bar1, list1)
    plt.bar_label(bar2, list2)
    plt.bar_label(bar3, list3)
    plt.bar_label(bar4, list4)

    plt.xticks(ind+width*1.5,labels)
    plt.legend( (bar1, bar2, bar3, bar4), (list1Label,list2Label,list3Label,list4Label) )
    plt.savefig(path)
    plt.close()


def sixBarFig(list1, list2, list3, list4, list5, list6, labels, xlabel, ylabel, title, list1Label, list2Label, list3Label, list4Label, list5Label, list6Label, path):
    N = len(labels)
    ind = numpy.arange(N) 
    width = 0.10
  
    xvals = list1
    bar1 = plt.bar(ind, xvals, width, color = 'moccasin')
  
    yvals = list2
    bar2 = plt.bar(ind+width, yvals, width, color='cornflowerblue')
  
    zvals = list3
    bar3 = plt.bar(ind+width*2, zvals, width, color = 'lightgreen')

    pvals = list4
    bar4 = plt.bar(ind+width*3, pvals, width, color = 'red')

    kvals = list5
    bar5 = plt.bar(ind+width*4, kvals, width, color = 'orange')

    tvals = list6
    bar6 = plt.bar(ind+width*5, tvals, width, color = 'purple')

    plt.rc('font', size=8) 
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
  
    plt.bar_label(bar1, list1)
    plt.bar_label(bar2, list2)
    plt.bar_label(bar3, list3)
    plt.bar_label(bar4, list4)
    plt.bar_label(bar5, list5)
    plt.bar_label(bar6, list6)
    
    plt.xticks(ind+width*2,labels)
    plt.legend( (bar1, bar2, bar3, bar4, bar5, bar6), (list1Label,list2Label,list3Label,list4Label, list5Label, list6Label) )
    plt.savefig(path)
    plt.close()


def fiveBarFig(list1, list2, list3, list4, list5, labels, xlabel, ylabel, title, list1Label, list2Label, list3Label, list4Label, list5Label, path):
    N = len(labels)
    ind = numpy.arange(N) 
    width = 0.15
  
    xvals = list1
    bar1 = plt.bar(ind, xvals, width, color = 'moccasin')
  
    yvals = list2
    bar2 = plt.bar(ind+width, yvals, width, color='cornflowerblue')
  
    zvals = list3
    bar3 = plt.bar(ind+width*2, zvals, width, color = 'lightgreen')

    pvals = list4
    bar4 = plt.bar(ind+width*3, pvals, width, color = 'red')

    kvals = list5
    bar5 = plt.bar(ind+width*4, kvals, width, color = 'orange')


    plt.rc('font', size=10) 
    plt.xlabel(xlabel, fontsize=10)
    plt.ylabel(ylabel,fontsize=10)
    plt.title(title)
  
    plt.bar_label(bar1, list1)
    plt.bar_label(bar2, list2)
    plt.bar_label(bar3, list3)
    plt.bar_label(bar4, list4)
    plt.bar_label(bar5, list5)
    #plt.bar_label(bar6, list6)
    
    plt.xticks(ind+width*2,labels)
    plt.legend( (bar1, bar2, bar3, bar4, bar5), (list1Label,list2Label,list3Label,list4Label, list5Label) )
    plt.savefig(path)
    plt.close()


def sevenBarFig(list1, list2, list3, list4, list5, list6, list7, labels, xlabel, ylabel, title, list1Label, list2Label, list3Label, list4Label, list5Label, list6Label, list7Label, path):
    N = len(labels)
    ind = numpy.arange(N) 
    width = 0.10
  
    xvals = list1
    bar1 = plt.bar(ind, xvals, width, color = 'moccasin')
  
    yvals = list2
    bar2 = plt.bar(ind+width, yvals, width, color='cornflowerblue')
  
    zvals = list3
    bar3 = plt.bar(ind+width*2, zvals, width, color = 'lightgreen')

    pvals = list4
    bar4 = plt.bar(ind+width*3, pvals, width, color = 'red')

    kvals = list5
    bar5 = plt.bar(ind+width*4, kvals, width, color = 'orange')

    tvals = list6
    bar6 = plt.bar(ind+width*5, tvals, width, color = 'purple')

    rvals = list7
    bar7 = plt.bar(ind+width*6, rvals, width, color = 'gray')

    plt.rc('font', size=8) 
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
  
    #plt.bar_label(bar1, list1)
    #plt.bar_label(bar2, list2)
    #plt.bar_label(bar3, list3)
    #plt.bar_label(bar4, list4)
    #plt.bar_label(bar5, list5)
    #plt.bar_label(bar6, list6)
    #plt.bar_label(bar7, list7)
    
    plt.xticks(ind+width*3,labels)
    plt.legend( (bar1, bar2, bar3, bar4, bar5, bar6, bar7), (list1Label,list2Label,list3Label,list4Label, list5Label, list6Label, list7Label) )
    plt.savefig(path)
    plt.close()


def perNodeQueryModeRes(resultsPath,datasetIndex, runs, threads, nodes):
    
    doStatic = 1

    doDynamic = 1
    doDynamicCoordinator = 1
    doDynamicThread = 1

    doGreedySorted = 1
    doGreedyUnSorted = 1
    
    doDynamicSorted = 1
    doDynamicSortedCoordinator = 1
    doDynamicThreadSorted = 1

    executeCommand("mkdir " + resultsPath + "plots/")
    executeCommand("mkdir " + resultsPath + "plots/" + str(conf['datasets'][datasetIndex]['name']))

    dirPrefix =  resultsPath + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/static/"
    dirPath = resultsPath + "plots/" + str(conf['datasets'][datasetIndex]['name']) + "/static/"
    executeCommand("mkdir " + dirPath)
    staticParallelPerNode = {}
    for currNodes in nodes:
        if doStatic == 0:
            break
        resPath = dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" 
        perNodeRes = processDirectoryForPerNodeAVG(resPath,runs,currNodes,threads)
        for i in range(currNodes):
            staticParallelPerNode[str(i)] = decimalTwo(perNodeRes[i][conf['indexesCollection']['varIndexes']['queryAnsweringTime']])
        filepath = dirPath + "/static_per_node_"+str(conf['datasets'][datasetIndex]['name'])+"_nodes"+str(currNodes)
        singleBarFig(list(staticParallelPerNode.values()), list(staticParallelPerNode.keys()), "Node Rank", "Seconds", str(conf['datasets'][datasetIndex]['name']) + " - Static Parallel Queries: Per Node Times for " + str(currNodes) + " nodes", "", filepath)
    

    dirPrefix =  resultsPath + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic/"
    dirPath = resultsPath + "plots/" + str(conf['datasets'][datasetIndex]['name']) + "/dynamic/"
    executeCommand("mkdir " + dirPath)
    dynamicParallel = {}
    for currNodes in nodes:
        if doDynamic == 0:
            break
        resPath = dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" 
        perNodeRes = processDirectoryForPerNodeAVG(resPath,runs,currNodes,threads)
        for i in range(currNodes):
            dynamicParallel[str(i)] = decimalTwo(perNodeRes[i][conf['indexesCollection']['varIndexes']['queryAnsweringTime']])
            totalQA = perNodeRes[i][conf['indexesCollection']['varIndexes']['queryAnsweringTime']]
            comm = perNodeRes[i][conf['indexesCollection']['varIndexes']['communicationTime']]
            actualQA = totalQA - comm
            #y = numpy.array([comm, actualQA])
            y = [comm, actualQA]
            y = [aSlice/max(y) for aSlice in y]
            labs = ["Send/Recv Time", "Actual Query Answering"]
            print(y)
            plt.pie(y, labels = labs, startangle = 90)
            plt.title("Node " + str(i))
            plt.savefig(dirPath + "dynamic_commpie_"+str(conf['datasets'][datasetIndex]['name'])+"_" + str(currNodes)+"_"+str(i))
            plt.close()
        #singleBarFig(list(dynamicParallel.values()), list(dynamicParallel.keys()), "Node Rank", "Seconds", str(conf['datasets'][datasetIndex]['name']) + " - Dynamic Parallel Queries: Per Node Times for " + str(currNodes) + " nodes", "", dirPath+ "dynamic_per_node_"+str(conf['datasets'][datasetIndex]['name'])+"_nodes"+str(currNodes))


    dirPrefix =  resultsPath + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic_coordinator/"
    dirPath = resultsPath + "plots/" + str(conf['datasets'][datasetIndex]['name']) + "/dynamic_coordinator/"
    executeCommand("mkdir " + dirPath)
    dynamicCoordinator = {}
    realNodes = [x+1 for x in nodes]
    for currNodes in realNodes:
        if doDynamicCoordinator == 0:
            break
        if currNodes == 2:
            currNodes = 1
        resPath = dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" 
        perNodeRes = processDirectoryForPerNodeAVG(resPath,runs,currNodes,threads)
        for i in range(currNodes):
            dynamicCoordinator[str(i)] = decimalTwo(perNodeRes[i][conf['indexesCollection']['varIndexes']['queryAnsweringTime']])
            totalQA = perNodeRes[i][conf['indexesCollection']['varIndexes']['queryAnsweringTime']]
            comm = perNodeRes[i][conf['indexesCollection']['varIndexes']['communicationTime']]
            actualQA = totalQA - comm
            #y = numpy.array([comm, actualQA])
            y = [comm, actualQA]
            y = [aSlice/max(y) for aSlice in y]
            labs = ["Send/Recv Time", "Actual Query Answering"]
            print(y)
            plt.pie(y, labels = labs, startangle = 90)
            plt.title("Node " + str(i))
            plt.savefig(dirPath + "dynamic_coordinator_commpie_"+str(conf['datasets'][datasetIndex]['name'])+"_" + str(currNodes)+"_"+str(i))
            plt.close()
        #singleBarFig(list(dynamicCoordinator.values()), list(dynamicCoordinator.keys()), "Node Rank", "Seconds", str(conf['datasets'][datasetIndex]['name']) + " - Dynamic Parallel Queries Coordinator: Per Node Times for " + str(currNodes) + " nodes", "", dirPath+ "dynamic_coordinator_per_node_"+str(conf['datasets'][datasetIndex]['name'])+"_nodes"+str(currNodes))
        

    dirPrefix =  resultsPath + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/greedy_exectimes_sorted/"
    dirPath = resultsPath + "plots/" + str(conf['datasets'][datasetIndex]['name']) + "/greedy_exectimes_sorted/"
    executeCommand("mkdir " + dirPath)
    greedySorted = {}
    greedySortedEstimations = {}
    for currNodes in nodes:
        if doGreedySorted == 0:
            break
        resPath = dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" 
        perNodeRes = processDirectoryForPerNodeAVG(resPath,runs,currNodes,threads)
        for i in range(currNodes):
            greedySorted[str(i)] = decimalTwo(perNodeRes[i][conf['indexesCollection']['varIndexes']['queryAnsweringTime']])
            greedySortedEstimations[str(i)] = decimalTwo(perNodeRes[i][conf['indexesCollection']['varIndexes']['greedyEstimatedQATime']])
        #singleBarFig(list(greedySorted.values()), list(greedySorted.keys()), "Node Rank", "Seconds", str(conf['datasets'][datasetIndex]['name']) + " - Dynamic Parallel Queries: Per Node Times for " + str(currNodes) + " nodes", "", dirPath+ "greedy_sorted_per_node_"+str(conf['datasets'][datasetIndex]['name'])+"_nodes"+str(currNodes))
        doubleBarQAgen(list(greedySorted.keys()), list(greedySorted.values()), list(greedySortedEstimations.values()), dirPath + "greedy_sorted_with_estimations_per_node_"+str(conf['datasets'][datasetIndex]['name'])+"_nodes"+str(currNodes), "Greedy Sorted Method With Estimations", "Actual QA", "Estimated QA")

    
    dirPrefix =  resultsPath + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/greedy_exectimes_unsorted/"
    dirPath = resultsPath + "plots/" + str(conf['datasets'][datasetIndex]['name']) + "/greedy_exectimes_unsorted/"
    executeCommand("mkdir " + dirPath)
    greedyUnSorted = {}
    greedyUnSortedEstimations = {}
    for currNodes in nodes:
        if doGreedyUnSorted == 0:
            break
        resPath = dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" 
        perNodeRes = processDirectoryForPerNodeAVG(resPath,runs,currNodes,threads)
        for i in range(currNodes):
            greedyUnSorted[str(i)] = decimalTwo(perNodeRes[i][conf['indexesCollection']['varIndexes']['queryAnsweringTime']])
            greedyUnSortedEstimations[str(i)] = decimalTwo(perNodeRes[i][conf['indexesCollection']['varIndexes']['greedyEstimatedQATime']])
        #singleBarFig(list(greedyUnSorted.values()), list(greedyUnSorted.keys()), "Node Rank", "Seconds", str(conf['datasets'][datasetIndex]['name']) + " - Dynamic Parallel Queries: Per Node Times for " + str(currNodes) + " nodes", "", dirPath+ "greedy_unsorted_per_node_"+str(conf['datasets'][datasetIndex]['name'])+"_nodes"+str(currNodes))
        doubleBarQAgen(list(greedyUnSorted.keys()), list(greedyUnSorted.values()), list(greedyUnSortedEstimations.values()), dirPath + "greedy_unsorted_with_estimations_per_node_"+str(conf['datasets'][datasetIndex]['name'])+"_nodes"+str(currNodes), "Greedy Unsorted Method With Estimations", "Actual QA", "Estimated QA")
    
    
    dirPrefix =  resultsPath + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic_sorted/"
    dirPath = resultsPath + "plots/" + str(conf['datasets'][datasetIndex]['name']) + "/dynamic_sorted/"
    executeCommand("mkdir " + dirPath)
    dynamicSorted = {}
    for currNodes in nodes:
        if doDynamicSorted == 0:
            break
        resPath = dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" 
        perNodeRes = processDirectoryForPerNodeAVG(resPath,runs,currNodes,threads)
        for i in range(currNodes):
            dynamicSorted[str(i)] = decimalTwo(perNodeRes[i][conf['indexesCollection']['varIndexes']['queryAnsweringTime']])
        #singleBarFig(list(dynamicSorted.values()), list(dynamicSorted.keys()), "Node Rank", "Seconds", str(conf['datasets'][datasetIndex]['name']) + " - Dynamic Parallel Queries: Per Node Times for " + str(currNodes) + " nodes", "", dirPath+ "dynamic_sorted_per_node_"+str(conf['datasets'][datasetIndex]['name'])+"_nodes"+str(currNodes))
    

    dirPrefix =  resultsPath + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic_sorted_coordinator/"
    dirPath = resultsPath + "plots/" + str(conf['datasets'][datasetIndex]['name']) + "/dynamic_sorted_coordinator/"
    executeCommand("mkdir " + dirPath)
    dynamicSortedCoordinator = {}
    realNodes = [x+1 for x in nodes]
    for currNodes in realNodes:
        if currNodes == 2:
            currNodes = 1
        if doDynamicSortedCoordinator == 0:
            break
        resPath = dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" 
        perNodeRes = processDirectoryForPerNodeAVG(resPath,runs,currNodes,threads)
        for i in range(currNodes):
            dynamicSortedCoordinator[str(i)] = decimalTwo(perNodeRes[i][conf['indexesCollection']['varIndexes']['queryAnsweringTime']])
        #singleBarFig(list(dynamicSortedCoordinator.values()), list(dynamicSortedCoordinator.keys()), "Node Rank", "Seconds", str(conf['datasets'][datasetIndex]['name']) + " - Dynamic Parallel Queries: Per Node Times for " + str(currNodes) + " nodes", "", dirPath+ "dynamic_sorted_coordinator_per_node_"+str(conf['datasets'][datasetIndex]['name'])+"_nodes"+str(currNodes))
    
    dirPrefix =  resultsPath + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic_sorted_thread/"
    dirPath = resultsPath + "plots/" + str(conf['datasets'][datasetIndex]['name']) + "/dynamic_sorted_thread/"
    executeCommand("mkdir " + dirPath)
    dynamicSorted = {}
    for currNodes in nodes:
        if doDynamicThreadSorted == 0:
            break
        resPath = dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" 
        perNodeRes = processDirectoryForPerNodeAVG(resPath,runs,currNodes,threads)
        for i in range(currNodes):
            dynamicSorted[str(i)] = decimalTwo(perNodeRes[i][conf['indexesCollection']['varIndexes']['queryAnsweringTime']])
        #singleBarFig(list(dynamicSorted.values()), list(dynamicSorted.keys()), "Node Rank", "Seconds", str(conf['datasets'][datasetIndex]['name']) + " - Dynamic Parallel Queries: Per Node Times for " + str(currNodes) + " nodes", "", dirPath+ "dynamic_sorted_per_node_"+str(conf['datasets'][datasetIndex]['name'])+"_nodes"+str(currNodes))
        
def calcInitialOutburst(resultsPath,datasetIndex, runs, threads, nodes, bursts):
    dirPrefix = resultsPath + str(conf['datasets'][datasetIndex]['name']) + "/initialBurst/nodes" + str(nodes) + "/"
    
    outburstTimes = {}
    for burst in bursts:
        resultFilPath = dirPrefix + "burst" + str(burst) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/"
        res = processDirectory(resultFilPath,runs,nodes,threads)
        outburstTimes[str(burst)] = decimalTwo(res[conf['indexesCollection']['varIndexes']['queryAnsweringTime']])
    
    singleBarFig(list(outburstTimes.values()), list(outburstTimes.keys()), "Burst Size", "Seconds", str(conf['datasets'][datasetIndex]['name']) + " - Initial burst results for " + str(nodes) + " nodes", "", resultsPath+ "/initial_outburst_"+str(conf['datasets'][datasetIndex]['name'])+"_nodes"+str(nodes))

        
def processResults(resultsPath, datasetsIndex, bsfShare, runs, nodes, threads):

    for currDatasetIndex in datasetsIndex:
        datasetPath = resultsPath + str(conf['datasets'][currDatasetIndex]['name']) + "/"
        
        nodeResultsDict = {}
        bsfShareDict = {}
        bsfNoShareDict = {}
        for currentNodeNum in nodes:
            nodePath = datasetPath + "node" + str(currentNodeNum) + "/"

            for currBsfShare in bsfShare:
                if currentNodeNum == 1:
                    currBSFPath = nodePath
                else:
                    currBSFPath = nodePath + currBsfShare + "/"

                threadResultsDict = {}
                for currThread in threads:
                    threadPath = currBSFPath + "threads" + str(currThread[0]) + "-" + str(currThread[1]) + "/" #current test path
                    
                    avgsOfMaxResults = processDirectory(threadPath, runs, currentNodeNum, currThread)
                    
                    #threadResultsDict[str(currThread[0])+"-"+str(currThread[1])] = avgsOfMaxResults
                    threadResultsDict[str(currThread[0])] = avgsOfMaxResults
                    nodeResultsDict[str(currentNodeNum)] = avgsOfMaxResults #this will work okay only if it runs for single thread conf and single bsf option

                    if currBsfShare == "bsf-dont-share":
                        bsfNoShareDict[str(currentNodeNum)] = decimalTwo(avgsOfMaxResults[conf['indexesCollection']['varIndexes']['queryAnsweringTime']])
                    else:
                        bsfShareDict[str(currentNodeNum)] = decimalTwo(avgsOfMaxResults[conf['indexesCollection']['varIndexes']['queryAnsweringTime']])

                if len(threads) > 1:
                    createThreadFigure(threadResultsDict, conf['datasets'][currDatasetIndex]['name'] + " - Buffer Fill, Index Creation and Query Answering", resultsPath+"threads_"+conf['datasets'][currDatasetIndex]['name'])

                if currentNodeNum == 1 and len(threads) > 1:
                    break

        #create the plots?
        if len(threads) == 1:
            #title = str(conf['datasets'][currDatasetIndex]['name'])  + " - threads:" + str(threads[0][0]) + "," + str(threads[0][1]) + " - " + str(bsfShare[0]) 
            title = str(conf['datasets'][currDatasetIndex]['name'])  + " - " + str(threads[0][0]) + " Threads - Buffer Fill and Index Creation" 
            createNodeFigure(nodeResultsDict, title, resultsPath + "plots/buffers_index",conf['datasets'][currDatasetIndex]['name'] + " - " + str(threads[0][0]) + " Threads - Buffer fill and Index Creation")
            doubleBarQA(list(bsfNoShareDict.keys()), list(bsfShareDict.values()), list(bsfNoShareDict.values()), resultsPath + "plots/"+ conf['datasets'][currDatasetIndex]['name'], conf['datasets'][currDatasetIndex]['name'] + " - " +str(threads[0][1])+ " Threads - Query Answering")


def createNodeFigure(dict,title, path, name):
    fig, (ax1, ax2) = plt.subplots(2)
    fig.suptitle(title) 

    queryAnsweringDict = {}
    indexCreationDict = {}
    bufferFillDict = {}
    
    for key in dict:
        results = dict[key]
        queryAnsweringDict[key] =  decimalTwo(results[conf['indexesCollection']['varIndexes']['queryAnsweringTime']])
        indexCreationDict[key] = decimalTwo(results[conf['indexesCollection']['varIndexes']['indexCreationTime']])
        bufferFillDict[key] = decimalTwo(results[conf['indexesCollection']['varIndexes']['isaxBufferTime']])

    #figure(queryAnsweringDict, title + " - query answering", path+ "qa/"+ name + "_query_answering", "Nodes", "Seconds")

    figure(bufferFillDict, "", "", "Seconds", ax1)
    figure(indexCreationDict, "",  "Nodes", "Seconds", ax2)

    plt.savefig(path)
    plt.close(fig)


def createThreadFigure(dict,title,path):
    
    fig, (ax1, ax2, ax3) = plt.subplots(3)
    fig.suptitle(title) 

    queryAnsweringDict = {}
    indexCreationDict = {}
    bufferFillDict = {}

    for key in dict:
        results = dict[key]

        queryAnsweringDict[key] = decimalTwo(results[conf['indexesCollection']['varIndexes']['queryAnsweringTime']])
        indexCreationDict[key] = decimalTwo(results[conf['indexesCollection']['varIndexes']['indexCreationTime']])
        bufferFillDict[key] = decimalTwo(results[conf['indexesCollection']['varIndexes']['isaxBufferTime']])

    figure(bufferFillDict, "", "", "Seconds", ax1)
    figure(indexCreationDict, "",  "", "Seconds", ax2)
    figure(queryAnsweringDict, "", "Threads", "Seconds", ax3)

    plt.savefig(path)
    plt.close(fig)


def produceChunkResults(resultsPath, datasetIndex, chunks,chunkSize, threads, runs):
    baseDir = resultsPath + str(conf['datasets'][datasetIndex]['name']) + "/chunkloading/chunk"+str(chunkSize) + "/"
    perChunkResults = {}
    for i in range(chunks):
        chunkResultsPath =  baseDir + "chunk" + str(i) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/"
        res = processDirectory(chunkResultsPath, runs, 1, threads)
        perChunkResults[str(i+1)] = res
    createNodeFigure(perChunkResults, "MESSI Instances for every part of " +str(chunkSize) +" data series of Seismic", resultsPath + "chunk"+str(chunkSize), "")


def figure(dict, title, axisX, axisY, ax):

    pl = ax.bar(dict.keys(), dict.values(), color='moccasin')

    ax.set_ylabel(axisY)
    ax.set_xlabel(axisX)
    ax.set_title(title)
    #plt.show()
    #plt.savefig(path)
    #plt.close(fig)
    ax.bar_label(pl, label_type='center')


def processDirectory(dir, runs, nodes, threads): #eg node = 8 results are for the 8 node run
    files = os.listdir(dir)

    if len(files) < runs:
        print("Error: Not enough test files in the directory: ", dir)
        return

    files = files[0:runs] #take only the selected runs
    variables = conf['benchmarkVariables']

    perNodeResults = numpy.zeros((nodes, len(variables)))
    perFileResults = numpy.zeros((0, len(variables)))

    numpy.set_printoptions(suppress=True,formatter={'float_kind': '{:f}'.format})

    for file in files:
        singleFileResults = numpy.zeros((nodes, len(variables)))
        currentFile = open(dir+file)
        lines = currentFile.readlines()

        currentNode = 0
        for line in lines:

            currentLine = line.split(" ")
            
            if currentLine == ['\n']:
                continue

            #print("CurrentLine: ", currentLine)

            for index in range(len(variables)):
                perNodeResults[currentNode][index] += float(currentLine[index])
                singleFileResults[currentNode][index] = float(currentLine[index])
            
            currentNode += 1

        maxOfEachCol = numpy.max(singleFileResults,axis=0)

        #print("maxOfEachCol: ", maxOfEachCol.tolist())
        perFileResults = numpy.vstack([perFileResults, maxOfEachCol])
        
    #print("Directory:", dir)
    #print("perFileResults (max of each col):\n{")
    #print('\n'.join([' '.join(['{:2}'.format(item) for item in row]) for row in perFileResults]))
    #print("}")

    finalResults = numpy.average(perFileResults,axis=0)
    
    #print("===== finalResults:", finalResults.tolist(), "=====")
    
    return finalResults


def processDirectoryForPerNodeAVG(dir, runs,nodes, threads):
    files = os.listdir(dir)

    if len(files) < runs:
        print("Error: Not enough test files in the directory: ", dir)
        return

    files = files[0:runs] #take only the selected runs
    variables = conf['benchmarkVariables']

    perNodeResults = numpy.zeros((nodes, len(variables)))
    perFileResults = numpy.zeros((0, len(variables)))

    numpy.set_printoptions(suppress=True,formatter={'float_kind': '{:f}'.format})

    for file in files:
        singleFileResults = numpy.zeros((nodes, len(variables)))
        currentFile = open(dir+file)
        lines = currentFile.readlines()

        currentNode = 0
        for line in lines:

            currentLine = line.split(" ")
            
            if currentLine == ['\n']:
                continue

            for index in range(len(variables)):
                perNodeResults[currentNode][index] += float(currentLine[index])/float(runs)
                singleFileResults[currentNode][index] = float(currentLine[index])
            
            currentNode += 1

        maxOfEachCol = numpy.max(singleFileResults,axis=0)
        perFileResults = numpy.vstack([perFileResults, maxOfEachCol])
         
    return perNodeResults #avgs


def avgArray2CSV(arr, outputFilePath, flag, inputName):

    output = open(outputFilePath, flag)
    
    pre = "=SPLIT(\""
    output.write(pre)
    s = inputName + ","
    for var in conf['benchmarkVariables']:
        s += var + ","
    output.write(s)

    post = "\";\",\")"
    
    output.write(post)
    output.write("\n")

    
    for i in range(len(arr)):
        pre = "=SPLIT(\""
        output.write(pre)
        
        #if i != len(arr)-2:
            #line = "Node "+str(i+1)+","
        #elif i == len(arr)-1:
            #line = "Average,"
        #else :
            #line = "Max,"
        line = "Avg of Max,"

        for j in range(len(arr[0])):
            line += str(arr[i][j])+","
        
        output.write(line)
        post = "\";\",\")\n"
        output.write(post)
    
    output.write("\n\n")
    output.close()


def perNodeArray2CSV(arr, outputFilePath, flag, inputName):
    output = open(outputFilePath, flag)
    
    pre = "=SPLIT(\""
    output.write(pre)
    s = inputName + ","
    for var in conf['benchmarkVariables']:
        s += var + ","
    output.write(s)

    post = "\";\",\")"
    
    output.write(post)
    output.write("\n")

    
    for i in range(len(arr)):
        pre = "=SPLIT(\""
        output.write(pre)
        
        #if i != len(arr)-2:
            #line = "Node "+str(i+1)+","
        #elif i == len(arr)-1:
            #line = "Average,"
        #else :
            #line = "Max,"
        line = "Node "+str(i+1)+","
        for j in range(len(arr[0])):
            line += str(arr[i][j])+","
        
        output.write(line)
        post = "\";\",\")\n"
        output.write(post)
    
    output.write("\n\n")
    output.close()


def decimalTwo(num):
    num = float(num)
    return float("{:.2f}".format(num))


def calcSeismicDistributedQueriesResults():
    #perNodeQueryModeRes("./seismic_distributedQueriesExperiments_benchmark65/", 0, 10, [16,64], [2,4,8])
    #calcDistributedQueriesResults(0, [1,2,4,8], 10, [16,64], "./seismic_distributedQueriesExperiments_benchmark65/")
    
    #perNodeQueryModeRes("./seismic_distributedQueriesExperiments_benchmark129/", 0, 10, [16,64], [2,4,8])
    #calcDistributedQueriesResults(0, [1,2,4,8], 10, [16,64], "./seismic_distributedQueriesExperiments_benchmark129/")

    perNodeQueryModeRes("../experiments/thesis/fdr_distributed_query_experiments/seismic_distributedQueriesExperiments_benchmark1001/", 0, 10, [16,64], [2,4,8])
    calcDistributedQueriesResults(0, [1,2,4,8], 10, [16,64], "../experiments/fdr_distributed_query_experiments/seismic_distributedQueriesExperiments_benchmark1001/")

    #perNodeQueryModeRes("./seismic_distributedQueriesExperiments_benchmark101/", 0, 10, [16,64], [2,4,8])
    #calcDistributedQueriesResults(0, [1,2,4,8], 10, [16,64], "./seismic_distributedQueriesExperiments_benchmark101/")

    #perNodeQueryModeRes("./seismic_distributedQueriesExperiments_benchmark201/", 0, 10, [16,64], [2,4,8])
    #calcDistributedQueriesResults(0, [1,2,4,8], 10, [16,64], "./seismic_distributedQueriesExperiments_benchmark201/")

    #perNodeQueryModeRes("../experiments/fdr_distributed_query_experiments/seismic_distributedQueriesExperiments_benchmark33/", 0, 10, [16,64], [2,4,8])
    #calcDistributedQueriesResults(0, [1,2,4,8], 10, [16,64], "../experiments/fdr_distributed_query_experiments/seismic_distributedQueriesExperiments_benchmark33/")


def calcTHresults(datasetIndex, nodes, runs, threads, resultsPath, thvals):
    dirPrefix = resultsPath + str(conf['datasets'][datasetIndex]['name']) + "/pq_th_experiments/"
    thtimes = {}
    for th in thvals:
        resPath = dirPrefix + "th" + str(th) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/"
        res = processDirectory(resPath,runs,1,threads)
        thtimes[str(th)] = decimalTwo(res[conf['indexesCollection']['varIndexes']['queryAnsweringTime']])
    
    #singleBarFig(list(thtimes.values()), list(thtimes.keys()), "TH Division Factor", "Seconds", str(conf['datasets'][datasetIndex]['name']) + " - Comparison of TH performance", "", "./th_comparison.png")
    singleDataPlot(list(thtimes.values()), list(thtimes.keys()), str(conf['datasets'][datasetIndex]['name']) + " - Perfomance of workstealing", resultsPath + "th_comparison_line.png", "TH Division Factor", "QA Time(seconds)")


def calcWSresults(datasetIndex, nodes, runs, threads, resultsPath):
    dirPrefix = resultsPath + str(conf['datasets'][datasetIndex]['name']) + "/_chatzakis_workstealing_/"
    wstimes = {}
    nowstimes = {}
   
    resPath = dirPrefix + "ws" + str(nodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/"
    perNodeRes = processDirectoryForPerNodeAVG(resPath,runs,nodes,threads)
    for i in range(nodes):
        wstimes[str(i)] = decimalTwo(perNodeRes[i][conf['indexesCollection']['varIndexes']['queryAnsweringTime']])
        if i == 0:
            print("COMM", decimalTwo(perNodeRes[i][conf['indexesCollection']['varIndexes']['communicationTime']]))
            print("WS",decimalTwo(perNodeRes[i][conf['indexesCollection']['varIndexes']['workstealingQA']]))
            print("PRO",decimalTwo(perNodeRes[i][conf['indexesCollection']['varIndexes']['pqProccessing']]))
            print("PREPRO",decimalTwo(perNodeRes[i][conf['indexesCollection']['varIndexes']['pqPrepro']]))
            print("FILL",decimalTwo(perNodeRes[i][conf['indexesCollection']['varIndexes']['pqFill']]))
    #resPath = dirPrefix + "nows" + str(nodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/"
    #perNodeRes = processDirectoryForPerNodeAVG(resPath,runs,nodes,threads)
    #for i in range(nodes):
    #    nowstimes[str(i)] = decimalTwo(perNodeRes[i][conf['indexesCollection']['varIndexes']['queryAnsweringTime']])
    
    singleBarFig(list(wstimes.values()), list(wstimes.keys()), "Workstealing", "Seconds", str(conf['datasets'][datasetIndex]['name']) + " - Workstealing", "", resultsPath + "ws.png")
    #singleBarFig(list(nowstimes.values()), list(nowstimes.keys()), "Simple", "Seconds", str(conf['datasets'][datasetIndex]['name']) + " - Simple", "", resultsPath + "nows.png")

    #doubleBarQAgen(list(wstimes.keys()), list(nowstimes.values()),list(wstimes.values()), "./ws_comparison_bsf", "Workstealing Comparison", "No Workstealing", "Workstealing")


def calcWSbatchResults(datasetIndex, nodes, runs, threads, resultsPath, batches):
    dirPrefix = resultsPath + str(conf['datasets'][datasetIndex]['name']) + "/_chatzakis_workstealing_/"
    node0_times = {}
    node1_times = {}
    for b in batches:
        resPath = dirPrefix + "wsBatches" + str(nodes) + "/batch" + str(b) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/"
        perNodeRes = processDirectoryForPerNodeAVG(resPath,runs,nodes,threads)
        #for i in range(2): #now only for two
        #    wstimes[b] = decimalTwo(perNodeRes[i][conf['indexesCollection']['varIndexes']['queryAnsweringTime']])
        node0_times[b] = decimalTwo(perNodeRes[0][conf['indexesCollection']['varIndexes']['queryAnsweringTime']])
        node1_times[b] = decimalTwo(perNodeRes[1][conf['indexesCollection']['varIndexes']['queryAnsweringTime']])

    doubleBarQAgen(list(node0_times.keys()), list(node0_times.values()),list(node1_times.values()), "./ws_comparison_bsf_manybatches", "Workstealing Comparison", "Node0", "Node1")




def calc_pdr_results(datasetIndex, runs, threads, node_g_dict, share_bsf, results_path):
    
    nodes = list(node_g_dict.keys())

    dynamic_node_times_ws = {}
    sorted_dynamic_node_times = {}
    #print(sorted_dynamic_node_times)
    dir_prefix = results_path + str(conf['datasets'][datasetIndex]['name']) + "/pdr_dq" + "/dynamic_thread_sorted_ws/"
    share_bsf = 1
    for node in nodes:
        
        group_times_qa = {}
        group_times_index = {}
        group_times_buffers = {}

        path = dir_prefix + "node" + str(node) + "/" 
        node_groups = node_g_dict[node]
        for node_group in node_groups:
            res_path = path + "groups" + str(node_group) + "/bsf_share" + str(share_bsf) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/"
            res = processDirectory(res_path, runs, node, threads)

            group_times_qa[str(node_group)] =  decimalTwo(res[conf['indexesCollection']['varIndexes']['queryAnsweringTime']])
            group_times_index[str(node_group)] =  decimalTwo(res[conf['indexesCollection']['varIndexes']['indexCreationTime']])
            group_times_buffers[str(node_group)] =  decimalTwo(res[conf['indexesCollection']['varIndexes']['isaxBufferTime']])

            #res = processDirectoryForPerNodeAVG(res_path, runs, node, threads)

            #if node_group != 1:
            #    continue

            print("====")
            print("Nodes" , node)
            print("Node Groups" , node_group)
            print("QA: " , decimalTwo(res[conf['indexesCollection']['varIndexes']['queryAnsweringTime']]))
            print("Index: " ,decimalTwo(res[conf['indexesCollection']['varIndexes']['indexCreationTime']]))
            print("Buff: ", decimalTwo(res[conf['indexesCollection']['varIndexes']['isaxBufferTime']]))
            print("total-index:", decimalTwo(res[conf['indexesCollection']['varIndexes']['indexCreationTime']])+decimalTwo(res[conf['indexesCollection']['varIndexes']['isaxBufferTime']]))
            print("total: ", decimalTwo(res[conf['indexesCollection']['varIndexes']['queryAnsweringTime']]) + decimalTwo(res[conf['indexesCollection']['varIndexes']['indexCreationTime']])+decimalTwo(res[conf['indexesCollection']['varIndexes']['isaxBufferTime']]))
            
            #print(res)

            #for i in range(node):
                #print("Node", i)
                #print(decimalTwo(res[i][conf['indexesCollection']['varIndexes']['queryAnsweringTime']]), "\t", end="")
                #print("Index: " ,decimalTwo(res[i][conf['indexesCollection']['varIndexes']['indexCreationTime']]))
                #print("Buff: ", decimalTwo(res[i][conf['indexesCollection']['varIndexes']['isaxBufferTime']]))
  

        #singleBarFig(list(group_times_qa.values()), list(group_times_qa.keys()), "Node Groups", "Query Answering (seconds)", "Per-Group Query Answering, " + str(node) + " nodes, Dynamic Module Algorithm", "", results_path+"dynamic_ws_per_group_times_nodes"+str(node)+"_qa.png")
        #singleBarFig(list(group_times_index.values()), list(group_times_index.keys()), "Node Groups", "Index Creation (seconds)", "Per-Group Index Creation, " + str(node) + " nodes, Dynamic Module Algorithm", "", results_path+"dynamic_ws_per_group_times_nodes"+str(node)+"_index.png")
        #singleBarFig(list(group_times_buffers.values()), list(group_times_buffers.keys()), "Node Groups", "iSAX buffer fill (seconds)", "Per-Group iSAX buffer fill, " + str(node) + " nodes, Dynamic Module Algorithm", "", results_path+"dynamic_ws_per_group_times_nodes"+str(node)+"_buffers.png")

        dynamic_node_times_ws[str(node)] = group_times_qa



def index_scalab_results(datasetIndex, runs, threads,sizes,nodes,results_path):
    dir_prefix = results_path + str(conf['datasets'][datasetIndex]['name']) + "_index_scalability_bench"+ str(conf['datasets'][datasetIndex]['name']) + "/" + str(conf['datasets'][datasetIndex]['name']) + "/index_sc/classic_index_creation/"
    for size in sizes:
        path = dir_prefix + "node" + str(nodes) + "/" + "groups" + str(nodes) + "/size" + str(size) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/"
        res = processDirectory(path, runs, nodes, threads)

        print("====")
        print("Nodes" , nodes)
        print("Size" , size)
        #print("QA: " , decimalTwo(res[conf['indexesCollection']['varIndexes']['queryAnsweringTime']]))
        print("Index: " ,decimalTwo(res[conf['indexesCollection']['varIndexes']['indexCreationTime']]))
        print("Buff: ", decimalTwo(res[conf['indexesCollection']['varIndexes']['isaxBufferTime']]))


def main():
    print("-- Statistics Script Start --")
    
    #calcRandomResultsMaximumsPerRun()
    #calcSeismicResultsMaximumsPerRun()
    #calcAstroResultsMaximumsPerRun()
    #calcSiftResultsMaximumsPerRun()
    #calcDeepResultsMaximumsPerRun()
    #calcSaldResultsMaximumsPerRun()
    #calcSeismicDistributedQueriesResults()

    #calcTHresults(0, 1, 5, [16,64], "../experiments/thesis/temp_experiments/seismic_th_experiment_100/", [1,2,4,8,16,32,64,128,256,512])

    #calcWSresults(0, 2, 10 , [16,64], "seismic_chatzakis_workstealing_2/")

    #calcWSbatchResults(0, 2, 5, [16,64], "./seismic_chatzakis_workstealing_2/",[1,2,3,4,5,6,7,8,9,10])
    #print("-- Statistics Script End --")

    #return

    dd = {}
    #dd[1] = [1]
    #dd[2] = [2]
    #dd[4] = [4]
    dd[8] = [2]
    #dd[16] = [1,2,4,8,16]

    #dd[8] = [1,2,4,8]
    #dd[16] = [1,2,8,16]
    calc_pdr_results(15,4,[16,64], dd, 1 , "./yandex_pdr_distributed_queries_benchmark_pre_400GB_100/")
    
    n = 16
    #print("\nSeismic")hh
    #index_scalab_results(0, 5, [16,64], [25 * 1000000, 50 * 1000000 ,75 * 1000000 ,100 * 1000000], n, "./")

    #print("\nAstro")
    #index_scalab_results(2, 5, [16,64], [67.5 * 1000000, 135 * 1000000 ,202.5 * 1000000 ,270 * 1000000], n, "./")

    #print("\nSIFT")
    #index_scalab_results(1, 5, [16,64], [25 * 10000000, 50 * 10000000 ,75 * 10000000 ,100 * 10000000], n, "./")

    #print("\nDeep")
    #index_scalab_results(4, 5, [16,64], [25 * 10000000, 50 * 10000000 ,75 * 10000000 ,100 * 10000000], n, "./")

    #nn = [2, 4, 8, 16]
    #for nod in nn:
    #    index_scalab_results(2, 5, [16,64], [270 * 1000000], nod, "./")

if __name__ == "__main__":
    main()