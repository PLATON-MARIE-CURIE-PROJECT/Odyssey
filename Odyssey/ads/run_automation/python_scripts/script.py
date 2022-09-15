## Ask botao about the distribution of the data in the buffer
import sys
import os
from os import path
import subprocess
import statistics
import copy
import getopt
from sys import argv

#import numpy
#import matplotlib.pyplot as plt


PARIS_SAVEPATH = "/gpfs/scratch/chatzakis/"

SEISMIC_INDEX = 0
SIFT_INDEX = 1

RANDOM100_INDEX = 6
RANDOM200_INDEX = 8
RANDOM300_INDEX = 10
RANDOM400_INDEX = 12


variableNames = ["Total Input Time",
                 "Total Output Time",
                 "iSAX buffers Time",
                 "Index Creation Time",
                 "Query Answering Time",
                 "MPI Time",
                 "Total Time",
                 "Query result Collection Time",
                 "Min dist","Real Dist",
                 "BSF Changes", 
                 "BSF shares", 
                 "BSF recieves", 
                 "BSF wrong recieves"]

totalVars = len(variableNames)

datasetNames = ["seismic", 
                "sift", 
                "astro", 
                "sald", 
                "deep", 
                "random50", 
                "random100", 
                "random150", 
                "random200", 
                "random250", 
                "random300", 
                "random350", 
                "random400", 
                "random450"]

datasets = ["data_size100M_seismic_len256_znorm.bin", 
            "data_size1B_sift_len128_znorm.bin",
            "data_size270M_astronomy_len256_znorm.bin", 
            "data_size899M_sald_len128_znorm.bin", 
            "data_size1B_deep1b_len96_znorm.bin",
            "randomDatasets/data_size52428800random50GB_len256_znorm.bin", 
            "randomDatasets/data_size104857600random100GB_len256_znorm.bin", 
            "randomDatasets/data_size157286400random150GB_len256_znorm.bin",
            "randomDatasets/data_size209715200random200GB_len256_znorm.bin", 
            "randomDatasets/data_size262144000random250GB_len256_znorm.bin",
            "randomDatasets/data_size314572800random300GB_len256_znorm.bin", 
            "randomDatasets/data_size367001600random350GB_len256_znorm.bin",
            "randomDatasets/data_size419430400random400GB_len256_znorm.bin", 
            "randomDatasets/data_size471859200random450GB_len256_znorm.bin"]

queries = [ "queries_ctrl100_seismic_len256_znorm.bin", 
            "queries_size100_sift_len128_znorm.bin",
            "queries_ctrl100_astronomy_len256_znorm.bin", 
            "queries_ctrl100_sald_len128_znorm.bin", 
            "queries_ctrl100_deep1b_len96_znorm.bin",
            "randomDatasets/queries_ctrl1000_random_len256_znorm.bin", 
            "randomDatasets/queries_ctrl1000_random_len256_znorm.bin", 
            "randomDatasets/queries_ctrl1000_random_len256_znorm.bin", 
            "randomDatasets/queries_ctrl1000_random_len256_znorm.bin",
            "randomDatasets/queries_ctrl1000_random_len256_znorm.bin",
            "randomDatasets/queries_ctrl1000_random_len256_znorm.bin",
            "randomDatasets/queries_ctrl1000_random_len256_znorm.bin",
            "randomDatasets/queries_ctrl1000_random_len256_znorm.bin",
            "randomDatasets/queries_ctrl1000_random_len256_znorm.bin"]

datasetSize = [ [100000000], 
                [1000000000], 
                [270000000], 
                [899000000], 
                [1000000000], 
                [52428800], 
                [104857600], 
                [157286400], 
                [209715200], 
                [262144000], 
                [314572800],
                [367001600],
                [419430400],
                [471859200]]

queriesSize = [ [100], 
                [100], 
                [100], 
                [100], 
                [100], 
                [100], 
                [100], 
                [100], 
                [100], 
                [100], 
                [100], 
                [100],
                [100],
                [100]]

timeSeriesSize = [256, 
                  128, 
                  256, 
                  128, 
                  96, 
                  256, 
                  256, 
                  256, 
                  256, 
                  256, 
                  256, 
                  256,
                  256,
                  256]

INPUT_TIME_INDEX = 0
OUTPUT_TIME_INDEX = 1
ISAX_BUFFER_TIME_INDEX = 2
INDEX_CREATION_TIME_INDEX = 3
QUERY_ANSWERING_TIME_INDEX = 4
MPI_TIME_INDEX = 5
TOTAL_TIME_INDEX = 6
QUERY_COLLECTION_TIME_INDEX = 7
MIN_DIST_INDEX = 8
REAL_DIST_INDEX = 9
BSF_CHANGES_INDEX = 10
BSF_SHARES_INDEX = 11
BSF_RECIEVES_INDEX = 12
BSF_WRONG_RECIEVES_INDEX = 13

INDEX_THREADS_INDEX = 0
QUERY_THREADS_INDEX = 1

def executeCommand(command):
    print(">>>",command)
    out = subprocess.getoutput(command)
    print(out)


def exportVar(name,value):
    os.environ[name] = value


def arrangeJobs2Run_CARV(machines, machines2run, queryTypeTest, totalRuns, resultsPath,datasets2runIndex):
    processes = len(machines2run)
    datasetPrefix = "/spare/chatzakis/datasets/"
    queriesPrefix = "/home1/public/chatzakis/queries/"
    jemallocPath = "/home1/public/chatzakis/jemalloc_install/lib/libjemalloc.so.2"

    for currDatasetIndex in datasets2runIndex:
        for currQuerySize in queriesSize[currDatasetIndex]:
            if len(machines2run) > 1:
                for queryType in queryTypeTest:
                    for currentRunNumber in range(totalRuns):
                        resultFile = resultsPath + datasetNames[currDatasetIndex] + "/" + str(currQuerySize) + "/" + "node" + str(len(machines2run)) +"/" + queryType + "/run" + str(currentRunNumber) + ".txt"
                        hmachines = ""
                        for machine in machines2run:
                            hmachines=hmachines+machines[machine]

                        command = "mpirun -H " + hmachines + " -n " + str(processes) + " -x LD_PRELOAD="+jemallocPath+ " ../bin/ads --simple-work-stealing no --all-nodes-index-all-dataset no"  
                        + " --verbose no --share-bsf " + queryType + " --output-file " + resultFile + " --mpi-already-splitted-dataset no --file-label \"test\" " 
                        + " --dataset " + datasetPrefix+str(datasets[datasets2runIndex]) + "  --leaf-size 2000 --initial-lbl-size 2000 --min-leaf-size 2000 "
                        + " --dataset-size " + str(datasetSize[currDatasetIndex][0]) + " --flush-limit 1000000 --cpu-type 32 --function-type 9990 --in-memory " 
                        + " --queries " + queriesPrefix+str(queries[currDatasetIndex]) + " --queries-size " + str(currQuerySize) + " --read-block 20000 --timeseries-size " + str(datasetSize[currDatasetIndex]) 

                        print(command)
                        #executeCommand(command)
            else:
                for currentRunNumber in range(totalRuns):
                    resultFile = resultsPath + datasetNames[currDatasetIndex] + "/" + str(currQuerySize) + "/" + "node" + str(len(machines2run)) + "/run" + str(currentRunNumber) + ".txt"
                    hmachines = ""
                    for machine in machines2run:
                        hmachines=hmachines+machine

                    command = "mpirun -H " + hmachines + " -n " + str(processes) + " -x LD_PRELOAD="+jemallocPath+ " ../bin/ads --simple-work-stealing no --all-nodes-index-all-dataset no"  
                    + " --verbose no --share-bsf " + queryType + " --output-file " + resultFile + " --mpi-already-splitted-dataset no --file-label \"test\" " 
                    + " --dataset " + datasetPrefix+str(datasets[datasets2runIndex]) + "  --leaf-size 2000 --initial-lbl-size 2000 --min-leaf-size 2000 "
                    + " --dataset-size " + str(datasetSize[currDatasetIndex][0]) + " --flush-limit 1000000 --cpu-type 32 --function-type 9990 --in-memory " 
                    + " --queries " + queriesPrefix+str(queries[currDatasetIndex]) + " --queries-size " + str(currQuerySize) + " --read-block 20000 --timeseries-size " + str(datasetSize[currDatasetIndex]) 

                    print(command)
                    #executeCommand(command)
                    

def arrangeSLURM(resultFile, datasetIndex, currQuerySize, currentNodeNum, scriptPath, queryType, currThread, currentRunNumber):
    exportVar("DRESS_RESULT_FILE", resultFile)
    if queryType == "":
        outFileName = "outputs/"+datasetNames[datasetIndex]+"/dress_"+datasetNames[datasetIndex]+"_querySize"+str(currQuerySize)+"_node"+str(currentNodeNum)+"_threads"+str(currThread[INDEX_THREADS_INDEX])+"-"+str(currThread[QUERY_THREADS_INDEX])+"_run"+str(currentRunNumber)+".txt"
    else:
        outFileName = "outputs/"+datasetNames[datasetIndex]+"/dress_"+datasetNames[datasetIndex]+"_querySize"+str(currQuerySize)+"_node"+str(currentNodeNum)+"_"+str(queryType)+"_threads"+str(currThread[INDEX_THREADS_INDEX])+"-"+str(currThread[QUERY_THREADS_INDEX])+"_run"+str(currentRunNumber)+".txt"

                            
    if(currentNodeNum > 10):
        executeCommand("sbatch --nodes="+ str(currentNodeNum) + " --ntasks="+str(currentNodeNum)+ " --mem=100000MB --partition ncpulargeshort --output " + outFileName + " " + scriptPath)
    else:
        executeCommand("sbatch --nodes="+ str(currentNodeNum) + " --ntasks="+str(currentNodeNum)+ " --mem=100000MB --output " + outFileName + " " + scriptPath)



def arrangeJobs2Run_PARIS(datasets2runIndex, resultsPath, queryTypeTest, totalRuns, totalNodes, totalThreads):
    scriptPath = "../chatzakis_stuff/mcJob.sh"

    datasetPrefix = "/gpfs/scratch/chatzakis/"
    queriesPrefix = "/gpfs/scratch/chatzakis/"

    for datasetIndex in datasets2runIndex:
        
        exportVar("DRESS_DATASET",datasetPrefix+str(datasets[datasetIndex]))
        exportVar("DRESS_DATASET_SIZE",str(datasetSize[datasetIndex][0]))
        exportVar("DRESS_TIMESERIES_SIZE",str(timeSeriesSize[datasetIndex]))
        exportVar("DRESS_QUERIES",queriesPrefix+str(queries[datasetIndex]))

        for currQuerySize in queriesSize[datasetIndex]:
            exportVar("DRESS_QUERIES_SIZE", str(currQuerySize))

            for currentNodeNum in totalNodes:
                exportVar("DRESS_NODES",str(currentNodeNum))

                if currentNodeNum > 1:
                    
                    for queryType in queryTypeTest:
                        exportVar("DRESS_QUERY_TYPE",str(queryType))

                        for currThread in totalThreads:
                            #exportVar("DRESS_THREADS",str(currThread))
                            exportVar("DRESS_INDEX_THREADS",str(currThread[INDEX_THREADS_INDEX]))
                            exportVar("DRESS_QUERY_THREADS",str(currThread[QUERY_THREADS_INDEX]))
                            
                            for currentRunNumber in range(totalRuns):
                                resultFile = resultsPath + datasetNames[datasetIndex] + "/" + str(currQuerySize) + "/" + "node" + str(currentNodeNum) +"/" + queryType +"/threads"+ str(currThread)+"/run" + str(currentRunNumber) + ".txt"
                                #exportVar("DRESS_RESULT_FILE", resultFile)
                                #outFileName = "outputs/"+datasetNames[datasetIndex]+"/dress_"+datasetNames[datasetIndex]+"_querySize"+str(currQuerySize)+"_node"+str(currentNodeNum)+"_"+str(queryType)+"_threads"+str(currThread)+"_run"+str(currentRunNumber)+".txt"
                            
                                #if(currentNodeNum > 10):
                                #    executeCommand("sbatch --nodes="+ str(currentNodeNum) + " --ntasks="+str(currentNodeNum)+ " --mem=100000MB --partition ncpulargeshort --output " + outFileName + " " + scriptPath)
                                #else:
                                #    executeCommand("sbatch --nodes="+ str(currentNodeNum) + " --ntasks="+str(currentNodeNum)+ " --mem=100000MB --output " + outFileName + " " + scriptPath)
                                arrangeSLURM(resultFile, datasetIndex, currQuerySize, currentNodeNum, scriptPath, queryType, currThread, currentRunNumber)


                else:
                    exportVar("DRESS_QUERY_TYPE",str(queryTypeTest[0]))
                    
                    for currThread in totalThreads:
                        #exportVar("DRESS_THREADS",str(currThread))
                        exportVar("DRESS_INDEX_THREADS",str(currThread[INDEX_THREADS_INDEX]))
                        exportVar("DRESS_QUERY_THREADS",str(currThread[QUERY_THREADS_INDEX]))

                        for currentRunNumber in range(totalRuns):
                            resultFile = resultsPath + datasetNames[datasetIndex] + "/" + str(currQuerySize) + "/" + "node" + str(currentNodeNum) +"/threads"+str(currThread)+"/run" + str(currentRunNumber) + ".txt"
                            #outFileName = "outputs/"+datasetNames[datasetIndex]+"/dress_"+datasetNames[datasetIndex]+"_querySize"+str(currQuerySize)+"_threads"+str(currThread)+"_run"+str(currentRunNumber)+".txt"
                            #exportVar("DRESS_RESULT_FILE", resultFile)
                            #executeCommand("sbatch --nodes="+ str(currentNodeNum) + " --ntasks="+str(currentNodeNum)+ " --output " + outFileName + " " + scriptPath)
                            arrangeSLURM(resultFile, datasetIndex, currQuerySize, currentNodeNum, scriptPath, "", currThread, currentRunNumber)


def createAllDirectories(datasets2runIndex, resultsPath, totalNodes, queryTypeTest, totalThreads):

    for d in datasets2runIndex:

        dataPath = resultsPath + datasetNames[d] + "/"
        executeCommand("mkdir " + dataPath)

        for currQuerySize in queriesSize[d]:
            path2create = dataPath + str(currQuerySize) + "/"
            executeCommand("mkdir " + path2create)

            for i in totalNodes:
                nodeDirPath = path2create + "node"+str(i)+"/"
                executeCommand("mkdir " + nodeDirPath)
                for qtype in queryTypeTest:
                    queryTypePath = ""
                    if(i == 1):
                        queryTypePath = nodeDirPath
                        for thread in totalThreads:
                            currThreadPath = queryTypePath + "threads" + str(thread[INDEX_THREADS_INDEX])+"-"+str(thread[QUERY_THREADS_INDEX]) + "/"
                            executeCommand("mkdir " + currThreadPath)
                        break
                    queryTypePath =  nodeDirPath + qtype + "/"
                    executeCommand("mkdir " + queryTypePath)

                    for thread in totalThreads:
                        currThreadPath = queryTypePath + "threads" + str(thread[INDEX_THREADS_INDEX])+"-"+str(thread[QUERY_THREADS_INDEX]) + "/"
                        executeCommand("mkdir " + currThreadPath)
                   


def processFiles(basePath, files, nodes, totalRuns): #eg node = 8 results are for the 8 node run

    nodeResults = numpy.zeros((nodes, totalVars))

    for file in files:

        print("Processing file: ", basePath+file)

        currentFile = open(basePath+file)
        lines = currentFile.readlines()

        currentNode = 0
        for line in lines:

            currentLine = line.split(" ")
            if currentLine == ['\n']:
                continue

            print("CurrentLine: ", currentLine)
            #nodeResults[currentNode][TOTAL_TIME_INDEX] += float(currentLine[TOTAL_TIME_INDEX]) #total time

            for index in range(len(variableNames)):
                nodeResults[currentNode][index] += float(currentLine[index])

            currentNode += 1
        
    #normalize
    for n in range(nodes):
        for var in range(totalVars):
            nodeResults[n][var] = nodeResults[n][var]/totalRuns

        nodeResults[n][MIN_DIST_INDEX] = round(nodeResults[n][MIN_DIST_INDEX])
        nodeResults[n][REAL_DIST_INDEX] = round(nodeResults[n][REAL_DIST_INDEX])
        nodeResults[n][BSF_CHANGES_INDEX] = round(nodeResults[n][BSF_CHANGES_INDEX])
        nodeResults[n][BSF_SHARES_INDEX] = round(nodeResults[n][BSF_SHARES_INDEX])
        nodeResults[n][BSF_RECIEVES_INDEX] = round(nodeResults[n][BSF_RECIEVES_INDEX])
        nodeResults[n][BSF_WRONG_RECIEVES_INDEX] = round(nodeResults[n][BSF_WRONG_RECIEVES_INDEX])

    avgOfEachCol = numpy.average(nodeResults,axis=0)
    maxOfEachCol = numpy.max(nodeResults,axis=0)

    nodeResults = numpy.vstack([nodeResults, avgOfEachCol])
    nodeResults = numpy.vstack([nodeResults, maxOfEachCol])

    return nodeResults


def arr2CSV(arr, outputFilePath, flag, inputName):

    output = open(outputFilePath, flag)
    
    pre = "=SPLIT(\""
    output.write(pre)
    s = inputName + ","
    for var in variableNames:
        s += var + ","
    output.write(s)

    post = "\";\",\")"
    
    output.write(post)
    output.write("\n")

    
    for i in range(len(arr)):
        pre = "=SPLIT(\""
        output.write(pre)
        if i != len(arr)-2:
            line = "Node "+str(i+1)+","
        elif i == len(arr)-1:
            line = "Average,"
        else :
            line = "Max,"    
        for j in range(len(arr[0])):
            line += str(arr[i][j])+","
        
        output.write(line)
        post = "\";\",\")\n"
        output.write(post)
    
    output.write("\n\n")
    output.close()


def processAllFiles(resultsPath, datasets2runIndex, queryTypeTest, totalRuns, totalNodes, totalThreads):
    
    resultCSVfilename = "./results.csv"

    for datasetIndex in datasets2runIndex:
        datasetPath = resultsPath + datasetNames[datasetIndex] + "/"
        for currQuerySize in queriesSize[datasetIndex]:
            queryPath = datasetPath + str(currQuerySize) + "/"

            
            nodeExperimentalMappings = {}
            for currNode in totalNodes:
                nodePath = queryPath + "node" + str(currNode) + "/"
                
                threadPerformanceMappings = {}

                if currNode == 1:
                    
                    for currThread in totalThreads:
                        currTestFilePath = nodePath + "threads"+str(currThread)+"/"
                        resultFiles = os.listdir(currTestFilePath)
                        print("Result files of path",currTestFilePath,"are",resultFiles)
                
                        if len(resultFiles) >= totalRuns:
                            resultArr = processFiles(currTestFilePath,resultFiles[0:totalRuns], currNode, totalRuns)
                            print("Results of path:", currTestFilePath, " are:", resultArr)
                            arr2CSV(resultArr, resultCSVfilename, "a",currTestFilePath)
                            nodeExperimentalMappings[currNode] = resultArr
                            threadPerformanceMappings[currThread] = resultArr
                else:
                    for currQueryTestType in queryTypeTest:
                        currQueryFilePath = nodePath + currQueryTestType + "/"

                        for currThread in totalThreads:
                            testFilePath = currQueryFilePath + "threads"+str(currThread)+"/"
                            
                            resultFiles = os.listdir(testFilePath)
                            print("Result files of path",testFilePath,"are",resultFiles)

                            if len(resultFiles) >= totalRuns:
                                resultArr = processFiles(testFilePath,resultFiles[0:totalRuns], currNode, totalRuns)
                                print("Results of path:", nodePath, " are:", resultArr)
                                arr2CSV(resultArr, resultCSVfilename, "a", testFilePath)
                                nodeExperimentalMappings[currNode] = resultArr


def plotNodeExperimentalMappingsDict(nodeExperimentalMappings):
    
    indexDict = {}
    bufferDict = {}
    queryDict = {}
    realDistsDict = {}
    lbDistsDict = {}

    for key in nodeExperimentalMappings:
        resultArr = nodeExperimentalMappings[key]
        rows, cols = resultArr.shape
        indexDict[key] = resultArr[rows-2][INDEX_CREATION_TIME_INDEX] #rows - 2 = avg row
        bufferDict[key] = resultArr[rows-2][ISAX_BUFFER_TIME_INDEX] #rows - 2 = avg row
        queryDict[key] = resultArr[rows-2][QUERY_ANSWERING_TIME_INDEX] #rows - 2 = avg row
        realDistsDict[key] = resultArr[rows-2][REAL_DIST_INDEX]
        lbDistsDict[key] = resultArr[rows-2][MIN_DIST_INDEX]
        #...

    #Plot Index Creation
    plotDict(indexDict, 'Nodes', 'Seconds', 1)

    #Plot Buffer Creation
    #Plot Query Answering


def createBarFig(dict, title, path, axisX, axisY):

    fig, ax = plt.subplots()

    pl = ax.bar(dict.keys(), dict.values(), color='moccasin')

    ax.set_ylabel(axisY)
    ax.set_xlabel(axisX)
    ax.set_title(title)
    ax.bar_label(pl, label_type='center')
    plt.savefig(path)
    plt.show()



def plotDict(dict, xLabel, yLabel, show):
    plt.hist(dict)
    plt.xlabel(xLabel)
    plt.ylabel(yLabel)
    
    if show :
        plt.show()


def downloadDatasetsAndQueries(saveDirectory, authorizationToken):
    #https://www.quora.com/How-do-I-download-a-very-large-file-from-Google-Drive#:~:text=Downloading%20files%20from%20Google%20drive,with%20sufficient%20space%20for%20file

    datasetSaveName = ["data_size100M_seismic_len256_znorm.bin","data_size270M_astronomy_len256_znorm.bin","data_size1B_deep1b_len96_znorm.bin","data_size899M_sald_len128_znorm.bin","data_size1B_sift_len128_znorm.bin"]
    datasetIDs = ["1XML3uywZdLxChlk_JgglPtVIEEZGs5qD","1bmajMOgdR-QhQXujVpoByZ9gHpprUncV","1ecvWA8i0ql-cmMI4oL63oOUIYzaWRc4z", "1HX2PzYhRrOg381jFMkFNvESUMfRnmv2j", "1kWoKRMyaW2jLmOyD-xkaopAbp3GfHbJy"]
    dataset2downloadIndices = []

    print("Downloading datasets...")

    for i in dataset2downloadIndices:
        command = "curl -H \"Authorization: Bearer "+authorizationToken+"\" https://www.googleapis.com/drive/v3/files/"+datasetIDs[i]+"?alt=media -o "+saveDirectory+datasetSaveName[i] 
        executeCommand(command)

    queriesSaveName = ["queries_ctrl100_seismic_len256_znorm.bin","queries_ctrl100_astronomy_len256_znorm.bin","queries_ctrl100_sald_len128_znorm.bin","queries_ctrl100_deep1b_len96_znorm.bin","queries_size100_sift_len128_znorm.bin"]
    queryIDs = ["14iwjV3JBCxZVBtcbgL3VPzPL8CNa_iGL","1xRZpPOiZSCCkQS6lwBmHyiKPuJ7oXg6A","1rtNcDl1YyJXuZ1VM1uyJr1odooPm0eNX","13n60tns1HaAxspB1xR0ZnOtzR4FkRmc7","1DEYgiPc0u6RWg7ALEIy7TxiJIBNfJMIO"]
    queries2downloadIndices = [0]

    print("Downloading queries...")

    #curl -H "Authorization: Bearer ya29.a0ARrdaM_sUIyADMboRggeB7XyXdakgjOsySYJzdZWFWlOpXIPDEITWxqyeCAJrw1-R5D1w29v8w072rY9eV_8dVR07tLsHNl5fO2g59Wy78ZSM823bdjVpvBktuRJyvPE-8IjlK07OM4k4IW1nP4fPfQ7o1mI" https://www.googleapis.com/drive/v3/files/1kWoKRMyaW2jLmOyD-xkaopAbp3GfHbJy?alt=media -o data_size1B_sift_len128_znorm.bin
    #curl -H "Authorization: Bearer ya29.A0ARrdaM-M0842AujQkNzv5fJPQVoiExbOgQvNJbhLblgtw863rWKoIClQdevCFMOBCS_8jcVEKpvbOMtraciVQQI7Br1igtsJmlAJYKu_5wxvia1qb12ceJjKIQuVuoqq1eVVVS5TE3bH4mARIENo54BShfxl" https://www.googleapis.com/drive/v3/files/1kWoKRMyaW2jLmOyD-xkaopAbp3GfHbJy?alt=media -o /spare/chatzakis/datasets/data_size1B_sift_len128_znorm.bin
    
    for i in queries2downloadIndices:
        command = "curl -H \"Authorization: Bearer "+authorizationToken+"\" https://www.googleapis.com/drive/v3/files/"+queryIDs[i]+"?alt=media -o "+saveDirectory+queriesSaveName[i] 
        executeCommand(command)


def transferFileWithScp(files, hosts, destDir):
    #scp file.txt remote_username@10.10.0.2:/remote/directory
    for file in files:
        for host in hosts:
            command = "scp " + file + " " + host + ":" +destDir
            executeCommand(command)
    

def createRandomDatasets(desiredSizeInGB, savePath):
    len = 256
    size1GB = 1048576
    generatorPath = "../../dataset/dataset/generator"
    #codePath = "../../dataset/dataset/main.c"
    folderPath = savePath + "randomDatasets/"

    executeCommand("mkdir "+ folderPath)
    #executeCommand("make")

    for currSize in desiredSizeInGB:
        size = currSize * size1GB # eg. 50 * 1GB = 50 Gb and more..
        seed = currSize

        exportVar("GSL_RNG_SEED",str(seed))
        datasetName = "data_size"+str(size)+"random"+str(currSize)+"GB_len"+str(len)+"_znorm.bin"

        command = "./" + generatorPath + " --size " + str(size) + " --length " + str(len) + " --z-normalize " + " > " + (folderPath+datasetName)
        executeCommand(command)


def createRandomQueries(desiredSizesInSize, savePath, inputSeed):
    len = 256
    generatorPath = "../../dataset/dataset/generator"
    #codePath = "../../dataset/dataset/main.c"
    folderPath = savePath + "randomDatasets/"
    executeCommand("mkdir "+ folderPath)

    for currSize in desiredSizesInSize:
        seed = currSize*inputSeed
        
        exportVar("GSL_RNG_SEED",str(seed))
        datasetName = "queries_ctrl"+str(currSize)+"_random_len"+str(len)+"_znorm.bin"
        
        command = "./" + generatorPath + " --size " + str(currSize) + " --length " + str(len) + " --z-normalize " + " > " + (folderPath+datasetName)
        executeCommand(command)


def run_simple(nodes, threads, dataset, datasetSize, queries, queriesNumber, bsfYesNo, tsSize):
    resultFile = "./result.txt"
    command = "mpirun -n " + nodes + " ../bin/ads --simple-work-stealing no --all-nodes-index-all-dataset no --verbose no --share-bsf " + bsfYesNo + " --output-file " + resultFile + " --mpi-already-splitted-dataset no --file-label \"test\" --dataset " + dataset +"  --leaf-size 2000 --initial-lbl-size 2000 --min-leaf-size 2000 --dataset-size " + datasetSize + " --flush-limit 1000000  --function-type 9990 --in-memory --queries " + queries + " --queries-size " + queriesNumber + " --read-block 20000 --timeseries-size " + tsSize + " --index-workers 16 --query-workers 16"
    #print(command)
    executeCommand(command)


def main():
    
    # module load intel/21U2/suite
    # module load python/3.9.5
    # squeue -u $USER | awk '{print $1}' | tail -n+2 | xargs scancel

    #downloadDatasetsAndQueries("/spare/chatzakis/datasets/","ya29.A0ARrdaM-M0842AujQkNzv5fJPQVoiExbOgQvNJbhLblgtw863rWKoIClQdevCFMOBCS_8jcVEKpvbOMtraciVQQI7Br1igtsJmlAJYKu_5wxvia1qb12ceJjKIQuVuoqq1eVVVS5TE3bH4mARIENo54BShfxl")
    #createRandomDatasets([5,10,20], PARIS_SAVEPATH)
    #createRandomQueries([1000], PARIS_SAVEPATH, 42)
    
    resultsPath = "./"
    queryTypeTest = ["bsf-dont-share", "bsf-block-per-query"]
    #queryTypeTest = ["bsf-block-per-query"]
    
    totalRuns = 2
    
    #datasets2runIndex = [SEISMIC_INDEX, RANDOM100_INDEX];totalNodes = [1];totalThreads = [1,2,4,8,16,32,64,128,256,512]
    #datasets2runIndex = [SEISMIC_INDEX];totalNodes = [1];totalThreads = [[32,128]]
    #datasets2runIndex = [SIFT_INDEX]; totalNodes = [8,16]; totalThreads=[32]
    #datasets2runIndex = [SEISMIC_INDEX]; totalNodes = [1,2,4,8,16]; totalThreads=[32]
    #datasets2runIndex = [RANDOM100_INDEX, RANDOM200_INDEX,RANDOM300_INDEX,RANDOM400_INDEX]; totalNodes = [1,2,4,8,16]; totalThreads=[32]
    #createAllDirectories(datasets2runIndex, resultsPath, totalNodes, queryTypeTest, totalThreads)
    #arrangeJobs2Run_PARIS(datasets2runIndex, resultsPath, queryTypeTest, totalRuns, totalNodes, totalThreads)

    machines = ["'sith0-roce'","'sith1-roce'","'sith2-roce'","'sith3-roce'","'sith4-roce'","'sith5-roce'","'sith6-roce'","'sith7-roce'","'sith8-roce'"]
    machines2run = [1,6,7,8]
    #createAllDirectories(resultsPath, len(machines2run), queryTypeTest)
    #arrangeJobs2Run_CARV(machines,machines2run,queryTypeTest,totalRuns,resultsPath, datasets2runIndex)

    #totalNodes = [8]
    processAllFiles(resultsPath, datasets2runIndex, queryTypeTest, totalRuns, totalNodes, totalThreads)

    #run_simple("1","32", PARIS_SAVEPATH + "randomDatasets/data_size5242880random5GB_len256_znorm.bin", "5242880", PARIS_SAVEPATH+"randomDatasets/queries_ctrl1000_random_len256_znorm.bin", "5", "bsf-block-per-query", "256")


if __name__ == "__main__":
    main()