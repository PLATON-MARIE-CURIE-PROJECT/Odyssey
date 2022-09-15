import os
import json
from pickle import TRUE
import subprocess
from os import path

# module load intel/21U2/suite
# module load python/3.9.5
# squeue -u $USER | awk '{print $1}' | tail -n+2 | xargs scancel


conf = json.load(open("./configuration.json", encoding="utf8"))


def executeCommand(command):
    print(">>>",command)
    out = subprocess.getoutput(command)
    print(out)


def exportVar(name,value):
    os.environ[name] = value


def scheduleParis(datasetsIndex, bsfShare, runs, nodes, threads):
   
    totalExperiments = 0

    createDirectories(datasetsIndex, nodes, bsfShare, threads, conf['parisData']['resultsPath'])

    exportVar("DRESS_PARALLEL_QUERY_THREADS", "1")
    exportVar("DRESS_QUERY_MODE", "0")
    exportVar("DRESS_WORK_STEALING", "no")

    exportVar("DRESS_CHUNK_TO_LOAD", "0")
    exportVar("DRESS_NODES","1")
    exportVar("DRESS_DISTRIBUTED_QUERIES_INITIAL_BURST", "1")

    exportVar("DRESS_ESTIMATIONS_FILENAME","")

    for currDatasetIndex in datasetsIndex:
        
        exportVar("DRESS_DATASET", str(conf['parisData']['savePath'])+str(conf['datasets'][currDatasetIndex]['filename']))
        exportVar("DRESS_DATASET_SIZE",str(conf['datasets'][currDatasetIndex]['size']))
        exportVar("DRESS_TIMESERIES_SIZE",str(conf['datasets'][currDatasetIndex]['tsSize']))
        exportVar("DRESS_QUERIES",str(conf['parisData']['savePath'])+str(conf['datasets'][currDatasetIndex]['queries']['filename']))
        exportVar("DRESS_QUERIES_SIZE",str(conf['datasets'][currDatasetIndex]['queries']['queriesSize']))

        for currentNodeNum in nodes:
            exportVar("DRESS_NODES",str(currentNodeNum))

            for currBsfShare in bsfShare:
                exportVar("DRESS_QUERY_TYPE",str(currBsfShare))

                for currThread in threads:
                    exportVar("DRESS_INDEX_THREADS",str(currThread[0]))
                    exportVar("DRESS_QUERY_THREADS",str(currThread[1]))

                    for currentRunNumber in range(runs):
                        if currentNodeNum == 1:
                            resultFile = str(conf['parisData']['resultsPath']) + str(conf['datasets'][currDatasetIndex]['name']) + "/" + "node" + str(currentNodeNum) + "/threads" + str(currThread[0]) + "-" + str(currThread[1]) + "/run" + str(currentRunNumber) + ".txt"
                        else:
                            resultFile = str(conf['parisData']['resultsPath']) + str(conf['datasets'][currDatasetIndex]['name']) + "/" + "node" + str(currentNodeNum) + "/" + currBsfShare + "/threads" + str(currThread[0]) + "-" + str(currThread[1]) + "/run" + str(currentRunNumber) + ".txt"
                        
                        exportVar("DRESS_RESULT_FILE", resultFile)

                        if currentNodeNum == 1:
                            outFileName = "outputs/" + str(conf['datasets'][currDatasetIndex]['name']) + "/dress_" + str(conf['datasets'][currDatasetIndex]['name']) + "_querySize" + str(conf['datasets'][currDatasetIndex]['queries']['queriesSize']) + "_node" + str(currentNodeNum) + "_threads" + str(currThread[0]) + "-" + str(currThread[1]) + "_run" + str(currentRunNumber) + ".txt"
                        else:
                            outFileName = "outputs/" + str(conf['datasets'][currDatasetIndex]['name']) + "/dress_" + str(conf['datasets'][currDatasetIndex]['name']) + "_querySize" + str(conf['datasets'][currDatasetIndex]['queries']['queriesSize']) + "_node" + str(currentNodeNum) + "_" + str(currBsfShare) + "_threads" + str(currThread[0]) + "-" + str(currThread[1]) + "_run" + str(currentRunNumber) + ".txt"
  
                        if(currentNodeNum > 10):
                            executeCommand("sbatch --nodes="+ str(currentNodeNum) + " --ntasks="+str(currentNodeNum)+ " --mem=240000MB --partition ncpulargeshort --output " + outFileName + " " + str(conf['parisData']['scriptPath']))
                        else:
                            executeCommand("sbatch --nodes="+ str(currentNodeNum) + " --ntasks="+str(currentNodeNum)+ " --mem=240000MB --output " + outFileName + " " + str(conf['parisData']['scriptPath']))

                        totalExperiments += 1

                if currentNodeNum == 1:
                    break #if we have 1 node, share bsf has no meaning, so we proceed.

    return totalExperiments
                

def scheduleCarv(machines2run, bsfShare, runs, threads, datasetsIndex, nodes):

    createDirectories(datasetsIndex, nodes, bsfShare, threads, conf['carvData']['resultsPath'])

    jemallocPath = conf['jemallocPath']

    for currDatasetIndex in datasetsIndex:
        for currentNodeNum in nodes:
            for currBsfShare in bsfShare:
                for currThread in threads:
                    for currentRunNumber in range(runs):
                        if currentNodeNum == 1:
                            resultFile = str(conf['carvData']['resultsPath']) + "node" + str(currentNodeNum) + "/threads" + str(currThread[0]) + "-" + str(currThread[1]) + "/run" + str(currentRunNumber) + ".txt"
                        else:
                            resultFile = str(conf['carvData']['resultsPath']) + "node" + str(currentNodeNum) + "/" + currBsfShare + "/threads" + str(currThread[0]) + "-" + str(currThread[1]) + "/run" + str(currentRunNumber) + ".txt"
                        
                        
                        hmachines = ""
                        machines2run = conf['carvData']['machines'][0:currentNodeNum] 
                        for machineIndex in range(currentNodeNum):
                            if(machineIndex != currentNodeNum-1):
                                hmachines += machines2run[currentNodeNum]+","
                            else:
                                hmachines += machines2run[currentNodeNum]

                        command = "mpirun -H " + hmachines + " -n " + str(currentNodeNum) + " -x LD_PRELOAD=" + jemallocPath + " ../bin/ads --simple-work-stealing no --all-nodes-index-all-dataset no"  
                        + " --verbose no --share-bsf " + currBsfShare + " --output-file " + resultFile + " --mpi-already-splitted-dataset no --file-label \"test\" " 
                        + " --dataset " + (str(conf['carvData']['savePath'])+str(conf['datasets'][currDatasetIndex]['filename'])) + "  --leaf-size 2000 --initial-lbl-size 2000 --min-leaf-size 2000 "
                        + " --dataset-size " + str(conf['datasets'][currDatasetIndex]['size']) + " --flush-limit 1000000 --cpu-type 32 --function-type 9990 --in-memory " 
                        + " --queries " + (str(conf['carvData']['savePath'])+str(conf['datasets'][currDatasetIndex]['queries']['filename'])) + " --queries-size " + str(conf['datasets'][currDatasetIndex]['queries']['size']) + " --read-block 20000 --timeseries-size " + str(conf['datasets'][currDatasetIndex]['tsSize'])

                        print(command)
                        #executeCommand(command)

                if currentNodeNum == 1:
                    break


def createDirectories(datasetsIndex, nodes, bsfShare, threads, resultsPath):
    for d in datasetsIndex:

        dataPath = resultsPath + str(conf['datasets'][d]['name']) + "/"
        executeCommand("mkdir " + dataPath)

        for i in nodes:
            nodeDirPath = dataPath + "node"+str(i)+"/"
            executeCommand("mkdir " + nodeDirPath)
            
            for qtype in bsfShare:
                    queryTypePath = ""
                    if(i == 1):
                        queryTypePath = nodeDirPath
                        for thread in threads:
                            currThreadPath = queryTypePath + "threads" + str(thread[0])+"-"+str(thread[1]) + "/"
                            executeCommand("mkdir " + currThreadPath)
                        break

                    queryTypePath =  nodeDirPath + qtype + "/"
                    executeCommand("mkdir " + queryTypePath)

                    for thread in threads:
                        currThreadPath = queryTypePath + "threads" + str(thread[0])+"-"+str(thread[1]) + "/"
                        executeCommand("mkdir " + currThreadPath)

        executeCommand("mkdir outputs")
        outputPath = "outputs/" + conf['datasets'][d]['name'] + "/"
        executeCommand("mkdir " + outputPath)


def workStealingExperiments(datasetIndex, dsSize, nodes, runs, threads, doWorkstealing):
    
    executeCommand("mkdir " + str(conf['datasets'][datasetIndex]['name']))
    executeCommand("mkdir " + str(conf['datasets'][datasetIndex]['name']) + "/workstealing")

    exportVar("DRESS_PARALLEL_QUERY_THREADS", "1")
    exportVar("DRESS_QUERY_MODE", "0")
    exportVar("DRESS_CHUNK_TO_LOAD","0")

    exportVar("DRESS_DATASET", str(conf['parisData']['savePath'])+str(conf['datasets'][datasetIndex]['filename']))
    #exportVar("DRESS_DATASET_SIZE",str(conf['datasets'][datasetIndex]['size']))
    exportVar("DRESS_DATASET_SIZE",str(dsSize))
    
    exportVar("DRESS_TIMESERIES_SIZE",str(conf['datasets'][datasetIndex]['tsSize']))
    exportVar("DRESS_QUERIES",str(conf['parisData']['savePath'])+str(conf['datasets'][datasetIndex]['queries']['filename']))
    exportVar("DRESS_QUERIES_SIZE",str(conf['datasets'][datasetIndex]['queries']['queriesSize']))
    exportVar("DRESS_QUERY_TYPE","bsf-dont-share")

    for currNodes in nodes:
        
        exportVar("DRESS_NODES",str(currNodes))
        exportVar("DRESS_INDEX_THREADS",str(threads[0]))
        exportVar("DRESS_QUERY_THREADS",str(threads[1]))

        if currNodes == 1:
            exportVar("DRESS_WORK_STEALING", "no")
        else:
            exportVar("DRESS_WORK_STEALING", "yes")

        if doWorkstealing == 0:
            exportVar("DRESS_WORK_STEALING", "no")

        executeCommand("mkdir " + str(conf['datasets'][datasetIndex]['name']) + "/workstealing/" + "node" + str(currNodes))
        executeCommand("mkdir outputs")
        executeCommand("mkdir outputs/workstealing/")
        executeCommand("mkdir " + "outputs/workstealing/" + str(conf['datasets'][datasetIndex]['name']) + "/")

        for run in range(runs):
            resultFile = str(conf['parisData']['resultsPath']) + str(conf['datasets'][datasetIndex]['name']) + "/workstealing/" + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "_run" + str(run)
            outFileName = "outputs/workstealing/" + str(conf['datasets'][datasetIndex]['name']) + "/dress_" + str(conf['datasets'][datasetIndex]['name']) + "_querySize" + str(conf['datasets'][datasetIndex]['queries']['queriesSize']) + "_node" + str(currNodes) + "_threads" + str(threads[0]) + "-" + str(threads[1]) + "_run" + str(run)

            if doWorkstealing == 1:
                resultFile += "workstealing.txt"
                outFileName += "workstealing.txt"
            else:
                resultFile  += ".txt"
                outFileName += ".txt"

            exportVar("DRESS_RESULT_FILE", resultFile)

            if(currNodes > 10):
                executeCommand("sbatch --nodes="+ str(currNodes) + " --ntasks="+str(currNodes)+ " --mem=200000MB --partition ncpulargeshort --output " + outFileName + " " + str(conf['parisData']['scriptPath']))
            else:
                executeCommand("sbatch --nodes="+ str(currNodes) + " --ntasks="+str(currNodes)+ " --mem=200000MB --output " + outFileName + " " + str(conf['parisData']['scriptPath']))


def experimentsOnPartialDatasets(datasetIndex, runs, threads, chunkSize, chunks):

    exportVar("DRESS_PARALLEL_QUERY_THREADS", "1")
    exportVar("DRESS_QUERY_MODE", "0")
    exportVar("DRESS_DATASET", str(conf['parisData']['savePath'])+str(conf['datasets'][datasetIndex]['filename']))
    exportVar("DRESS_DATASET_SIZE",str(chunkSize))
    exportVar("DRESS_TIMESERIES_SIZE",str(conf['datasets'][datasetIndex]['tsSize']))
    exportVar("DRESS_QUERIES",str(conf['parisData']['savePath'])+str(conf['datasets'][datasetIndex]['queries']['filename']))
    exportVar("DRESS_QUERIES_SIZE",str(conf['datasets'][datasetIndex]['queries']['queriesSize']))
    exportVar("DRESS_WORK_STEALING", "no")
    exportVar("DRESS_NODES","1")
    exportVar("DRESS_QUERY_TYPE","bsf-dont-share")
    exportVar("DRESS_INDEX_THREADS",str(threads[0]))
    exportVar("DRESS_QUERY_THREADS",str(threads[1]))

    executeCommand("mkdir " + str(conf['datasets'][datasetIndex]['name']))
    executeCommand("mkdir " + str(conf['datasets'][datasetIndex]['name']) + "/chunkloading")
    executeCommand("mkdir " + str(conf['datasets'][datasetIndex]['name']) + "/chunkloading/chunk"+str(chunkSize))

    executeCommand("mkdir outputs/")

    executeCommand("mkdir outputs/" + str(conf['datasets'][datasetIndex]['name']))
    executeCommand("mkdir outputs/" + str(conf['datasets'][datasetIndex]['name']) + "/chunkloading/")
    executeCommand("mkdir outputs/" + str(conf['datasets'][datasetIndex]['name']) + "/chunkloading/chunk"+str(chunkSize))

    dirPrefix = str(conf['datasets'][datasetIndex]['name']) + "/chunkloading/chunk"+str(chunkSize)+"/"
    outPrefix = "outputs/" + str(conf['datasets'][datasetIndex]['name']) + "/chunkloading/chunk"+str(chunkSize)+"/"

    for i in range(chunks):
        exportVar("DRESS_CHUNK_TO_LOAD", str(i))

        executeCommand("mkdir " + dirPrefix + "chunk" + str(i))
        executeCommand("mkdir " + dirPrefix + "chunk" + str(i) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")
        
        executeCommand("mkdir " + outPrefix + "chunk" + str(i) + "/")
        executeCommand("mkdir " + outPrefix + "chunk" + str(i) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")

        for run in range(runs):
            resultFile = dirPrefix + "chunk" + str(i) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/"+ "run" + str(run) + ".txt"
            outFileName= outPrefix + "chunk" + str(i) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/"+ "run" + str(run) + ".txt"
            
            exportVar("DRESS_RESULT_FILE", resultFile)
            executeCommand("sbatch --nodes=1 --ntasks=1 --mem=200000MB --output " + outFileName + " " + str(conf['parisData']['scriptPath']))


def scalingExperiment(datasetIndex, runs, threads, chunkSize, chunks):
    exportVar("DRESS_PARALLEL_QUERY_THREADS", "1")
    exportVar("DRESS_QUERY_MODE", "0")
    exportVar("DRESS_DATASET", str(conf['parisData']['savePath'])+str(conf['datasets'][datasetIndex]['filename']))
    exportVar("DRESS_DATASET_SIZE",str(conf['datasets'][datasetIndex]['size']))
    exportVar("DRESS_TIMESERIES_SIZE",str(conf['datasets'][datasetIndex]['tsSize']))
    exportVar("DRESS_QUERIES",str(conf['parisData']['savePath'])+str(conf['datasets'][datasetIndex]['queries']['filename']))
    exportVar("DRESS_QUERIES_SIZE",str(conf['datasets'][datasetIndex]['queries']['queriesSize']))
    exportVar("DRESS_WORK_STEALING", "no")
    exportVar("DRESS_NODES","1")
    exportVar("DRESS_QUERY_TYPE","bsf-dont-share")
    exportVar("DRESS_CHUNK_TO_LOAD", "0")
    
    exportVar("DRESS_INDEX_THREADS",str(threads[0]))
    exportVar("DRESS_QUERY_THREADS",str(threads[1]))

    datasetSize = conf['datasets'][datasetIndex]['size']

    currSize = chunkSize
    currNodes = 1

    executeCommand("mkdir " + str(conf['datasets'][datasetIndex]['name']))
    executeCommand("mkdir " + str(conf['datasets'][datasetIndex]['name']) + "/scaling")
    executeCommand("mkdir " + str(conf['datasets'][datasetIndex]['name']) + "/scaling/part"+str(chunkSize))

    executeCommand("mkdir outputs/")

    executeCommand("mkdir outputs/" + str(conf['datasets'][datasetIndex]['name']))
    executeCommand("mkdir outputs/" + str(conf['datasets'][datasetIndex]['name']) + "/scaling/")
    executeCommand("mkdir outputs/" + str(conf['datasets'][datasetIndex]['name']) + "/scaling/part"+str(chunkSize))

    dirPrefix =              str(conf['datasets'][datasetIndex]['name']) + "/scaling/part"+str(chunkSize)+"/"
    outPrefix = "outputs/" + str(conf['datasets'][datasetIndex]['name']) + "/scaling/part"+str(chunkSize)+"/"

    for currTest in range(chunks):
        exportVar("DRESS_NODES",str(currNodes))
        exportVar("DRESS_DATASET_SIZE",str(currSize))

        executeCommand("mkdir " + dirPrefix + "part" + str(currSize) + "-nodes" + str(currNodes))
        executeCommand("mkdir " + dirPrefix + "part" + str(currSize) + "-nodes" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")
        
        executeCommand("mkdir " + outPrefix + "part" + str(currSize) + "-nodes" + str(currNodes))
        executeCommand("mkdir " + outPrefix + "part" + str(currSize) + "-nodes" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")

        for run in range(runs):
            resultFile = dirPrefix + "part" + str(currSize) + "-nodes" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/"+ "run" + str(run) + ".txt"
            outFileName= outPrefix + "part" + str(currSize) + "-nodes" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/"+ "run" + str(run) + ".txt"
            
            exportVar("DRESS_RESULT_FILE", resultFile)
            
            #executeCommand("sbatch --nodes="+str(currNodes)+" --ntasks="+str(currNodes)+" --mem=200000MB --output " + outFileName + " " + str(conf['parisData']['scriptPath']))
            if(currNodes > 10):
                executeCommand("sbatch --nodes="+ str(currNodes) + " --ntasks="+str(currNodes)+ " --mem=200000MB --partition ncpulargeshort --output " + outFileName + " " + str(conf['parisData']['scriptPath']))
            else:
                executeCommand("sbatch --nodes="+ str(currNodes) + " --ntasks="+str(currNodes)+ " --mem=200000MB --output " + outFileName + " " + str(conf['parisData']['scriptPath']))

        currSize *= 2 
        currNodes *= 2


def distributedQueriesExperiment(datasetIndex, datasetQueries, runs, threads, nodes, estimations_file):
    
    doClassic = 1
    doSWBSF = 1
    
    doDistr = 1

    doMakespan = 1
    doMakespanCoord = 1
    doDynamicThread = 1

    doMakeSortedQueriesStatic = 1 #kako onoma
    doMakeStaticUnsortedGreedy = 1

    doMakeDynamicSortedModule = 1
    doMakeDynamicSortedCoordinator = 1
    doDynamicThreadSorted = 1

    exportVar("DRESS_QUERIES",str(conf['parisData']['savePath'])+str(datasetQueries[0]))#+str(conf['datasets'][datasetIndex]['queries']['filename']))
    exportVar("DRESS_QUERIES_SIZE",str(datasetQueries[1]))#str(conf['datasets'][datasetIndex]['queries']['queriesSize']))

    exportVar("DRESS_PARALLEL_QUERY_THREADS", "1")
    exportVar("DRESS_WORK_STEALING", "no")
    exportVar("DRESS_CHUNK_TO_LOAD", "0")
    exportVar("DRESS_QUERY_TYPE","bsf-dont-share")
    exportVar("DRESS_QUERY_MODE", "2")
    exportVar("DRESS_DISTRIBUTED_QUERIES_INITIAL_BURST", "1")
    exportVar("DRESS_DATASET", str(conf['parisData']['savePath'])+str(conf['datasets'][datasetIndex]['filename']))
    exportVar("DRESS_DATASET_SIZE",str(conf['datasets'][datasetIndex]['size']))
    exportVar("DRESS_TIMESERIES_SIZE",str(conf['datasets'][datasetIndex]['tsSize']))
    
    exportVar("DRESS_INDEX_THREADS",str(threads[0]))
    exportVar("DRESS_QUERY_THREADS",str(threads[1]))
    exportVar("DRESS_NODES","1")

    exportVar("DRESS_ESTIMATIONS_FILENAME",estimations_file)

    folder =        str(conf['datasets'][datasetIndex]['name']) +"_distributedQueriesExperiments_benchmark" + str(datasetQueries[1]) + "/"
    folderOutputs = str(conf['datasets'][datasetIndex]['name']) +"_distributedQueriesExperiments_benchmark" + str(datasetQueries[1]) + "/outputs_benchmark" + str(datasetQueries[1]) +"/"

    executeCommand("mkdir " + folder)
    executeCommand("mkdir " + folderOutputs)
    executeCommand("mkdir " + folder + str(conf['datasets'][datasetIndex]['name']))
    executeCommand("mkdir " + folder + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/")
    executeCommand("mkdir " + folderOutputs + str(conf['datasets'][datasetIndex]['name']))
    executeCommand("mkdir " + folderOutputs + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/")


    # STATIC DISTRIBUTED METHOD
    executeCommand("mkdir " + folder + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/static")
    executeCommand("mkdir " + folderOutputs + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/static")
    dirPrefix = folder +           str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/static/"
    outPrefix = folderOutputs +    str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/static/"
    exportVar("DRESS_QUERY_MODE", "2")
    exportVar("DRESS_QUERY_TYPE","bsf-dont-share")
    for currNodes in nodes:        
        if doDistr == 0:
            break

        exportVar("DRESS_NODES",str(currNodes))

        executeCommand("mkdir " + dirPrefix + "node" + str(currNodes))
        executeCommand("mkdir " + dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")
        executeCommand("mkdir " + outPrefix + "node" + str(currNodes))
        executeCommand("mkdir " + outPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")

        for run in range(runs):
            resultFile = dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
            outFileName= outPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
            
            exportVar("DRESS_RESULT_FILE", resultFile)
            
            if(currNodes > 10):
                executeCommand("sbatch --nodes="+ str(currNodes) + " --ntasks="+str(currNodes)+ " --mem=200000MB --partition ncpulargeshort --output " + outFileName + " " + str(conf['parisData']['scriptPath']))
            else:
                if currNodes == 1:
                    exportVar("DRESS_QUERY_MODE", "0")

                executeCommand("sbatch --nodes="+ str(currNodes) + " --ntasks="+str(currNodes)+ " --mem=200000MB --output " + outFileName + " " + str(conf['parisData']['scriptPath']))
                exportVar("DRESS_QUERY_MODE", "2")


    # CLASSIC SEQUENTIAL
    executeCommand("mkdir " + folder + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/classic")
    executeCommand("mkdir " + folderOutputs + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/classic")
    dirPrefix = folder +             str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/classic/"
    outPrefix = folderOutputs +      str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/classic/"
    exportVar("DRESS_QUERY_MODE", "0")
    exportVar("DRESS_QUERY_TYPE","bsf-dont-share")
    for currNodes in nodes:
        if doClassic == 0:
            break

        exportVar("DRESS_NODES",str(currNodes))

        executeCommand("mkdir " + dirPrefix + "node" + str(currNodes))
        executeCommand("mkdir " + dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")
        
        executeCommand("mkdir " + outPrefix + "node" + str(currNodes))
        executeCommand("mkdir " + outPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")

        for run in range(runs):
            resultFile = dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
            outFileName= outPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
            
            exportVar("DRESS_RESULT_FILE", resultFile)
            
            if(currNodes > 10):
                executeCommand("sbatch --nodes="+ str(currNodes) + " --ntasks="+str(currNodes)+ " --mem=200000MB --partition ncpulargeshort --output " + outFileName + " " + str(conf['parisData']['scriptPath']))
            else:
                executeCommand("sbatch --nodes="+ str(currNodes) + " --ntasks="+str(currNodes)+ " --mem=200000MB --output " + outFileName + " " + str(conf['parisData']['scriptPath']))


    # SWBSF SEQUENTIAL
    executeCommand("mkdir " + folderOutputs + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/swbsf")
    executeCommand("mkdir " + folder + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/swbsf")
    dirPrefix = folder +        str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/swbsf/"
    outPrefix = folderOutputs + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/swbsf/"
    exportVar("DRESS_QUERY_MODE", "0")
    exportVar("DRESS_QUERY_TYPE","bsf-block-per-query")
    for currNodes in nodes:
        if doSWBSF == 0:
            break

        exportVar("DRESS_NODES",str(currNodes))

        executeCommand("mkdir " + dirPrefix + "node" + str(currNodes))
        executeCommand("mkdir " + dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")
        
        executeCommand("mkdir " + outPrefix + "node" + str(currNodes))
        executeCommand("mkdir " + outPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")

        for run in range(runs):
            resultFile = dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
            outFileName= outPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
            
            exportVar("DRESS_RESULT_FILE", resultFile)
            
            if(currNodes > 10):
                executeCommand("sbatch --nodes="+ str(currNodes) + " --ntasks="+str(currNodes)+ " --mem=200000MB --partition ncpulargeshort --output " + outFileName + " " + str(conf['parisData']['scriptPath']))
            else:
                if currNodes == 1:
                    exportVar("DRESS_QUERY_TYPE","bsf-dont-share")
                
                executeCommand("sbatch --nodes="+ str(currNodes) + " --ntasks="+str(currNodes)+ " --mem=200000MB --output " + outFileName + " " + str(conf['parisData']['scriptPath']))
                exportVar("DRESS_QUERY_TYPE","bsf-block-per-query")


    # DYNAMIC DISTRIBUTED MODULE
    executeCommand("mkdir " + folderOutputs + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic")
    executeCommand("mkdir " + folder + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic")
    dirPrefix = folder +             str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic/"
    outPrefix = folderOutputs +      str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic/"
    exportVar("DRESS_QUERY_MODE", "5")
    exportVar("DRESS_QUERY_TYPE","bsf-dont-share")
    for currNodes in nodes:
        
        if doMakespan == 0:
            break

        exportVar("DRESS_NODES",str(currNodes))

        executeCommand("mkdir " + dirPrefix + "node" + str(currNodes))
        executeCommand("mkdir " + dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")
        
        executeCommand("mkdir " + outPrefix + "node" + str(currNodes))
        executeCommand("mkdir " + outPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")

        #if datasetIndex==0:
        #    exportVar("DRESS_DISTRIBUTED_QUERIES_INITIAL_BURST", str(nodeByBurstSeismic[currNodes]))
        #else:
        #    exportVar("DRESS_DISTRIBUTED_QUERIES_INITIAL_BURST", str(nodeByBurstRandom[currNodes]))

        exportVar("DRESS_DISTRIBUTED_QUERIES_INITIAL_BURST", "1")

        for run in range(runs):
            resultFile = dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
            outFileName= outPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
            
            exportVar("DRESS_RESULT_FILE", resultFile)
            
            if(currNodes > 10):
                executeCommand("sbatch --nodes="+ str(currNodes) + " --ntasks="+str(currNodes)+ " --mem=200000MB --partition ncpulargeshort --output " + outFileName + " " + str(conf['parisData']['scriptPath']))
            else:
                if currNodes == 1:
                    exportVar("DRESS_QUERY_MODE", "0")

                executeCommand("sbatch --nodes="+ str(currNodes) + " --ntasks="+str(currNodes)+ " --mem=200000MB --output " + outFileName + " " + str(conf['parisData']['scriptPath']))
                exportVar("DRESS_QUERY_MODE", "5")

    # DYNAMIC DISTRIBUTED THREAD
    executeCommand("mkdir " + folderOutputs + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic_thread")
    executeCommand("mkdir " + folder + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic_thread")
    dirPrefix = folder +             str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic_thread/"
    outPrefix = folderOutputs +      str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic_thread/"
    exportVar("DRESS_QUERY_MODE", "17")
    exportVar("DRESS_QUERY_TYPE","bsf-dont-share")
    for currNodes in nodes:
        
        if doDynamicThread == 0:
            break

        exportVar("DRESS_NODES",str(currNodes))

        executeCommand("mkdir " + dirPrefix + "node" + str(currNodes))
        executeCommand("mkdir " + dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")
        
        executeCommand("mkdir " + outPrefix + "node" + str(currNodes))
        executeCommand("mkdir " + outPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")

        exportVar("DRESS_DISTRIBUTED_QUERIES_INITIAL_BURST", "1")

        for run in range(runs):
            resultFile = dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
            outFileName= outPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
            
            exportVar("DRESS_RESULT_FILE", resultFile)
            
            if(currNodes > 10):
                executeCommand("sbatch --nodes="+ str(currNodes) + " --ntasks="+str(currNodes)+ " --mem=200000MB --partition ncpulargeshort --output " + outFileName + " " + str(conf['parisData']['scriptPath']))
            else:
                if currNodes == 1:
                    exportVar("DRESS_QUERY_MODE", "0")

                executeCommand("sbatch --nodes="+ str(currNodes) + " --ntasks="+str(currNodes)+ " --mem=200000MB --output " + outFileName + " " + str(conf['parisData']['scriptPath']))
                exportVar("DRESS_QUERY_MODE", "17")

    # DYNAMIC DISTRIBUTED COORDINATOR
    executeCommand("mkdir " + folder + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic_coordinator/")
    executeCommand("mkdir " + folderOutputs + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic_coordinator")
    dirPrefix = folder +             str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic_coordinator/"
    outPrefix = folderOutputs +      str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic_coordinator/"
    exportVar("DRESS_QUERY_MODE", "8")
    exportVar("DRESS_QUERY_TYPE","bsf-dont-share")
    realNodes = [x+1 for x in nodes]
    for currNodes in realNodes:
        if doMakespanCoord == 0:
            break

        if currNodes == 2:
            currNodes = 1 

        exportVar("DRESS_NODES",str(currNodes))

        executeCommand("mkdir " + dirPrefix + "node" + str(currNodes))
        executeCommand("mkdir " + dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")
        
        executeCommand("mkdir " + outPrefix + "node" + str(currNodes))
        executeCommand("mkdir " + outPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")
        
        exportVar("DRESS_DISTRIBUTED_QUERIES_INITIAL_BURST", "1")

        for run in range(runs):
            resultFile = dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
            outFileName= outPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
            
            exportVar("DRESS_RESULT_FILE", resultFile)
            
            if(currNodes > 10):
                executeCommand("sbatch --nodes="+ str(currNodes) + " --ntasks="+str(currNodes)+ " --mem=200000MB --partition ncpulargeshort --output " + outFileName + " " + str(conf['parisData']['scriptPath']))
            else:
                if currNodes == 1:
                    exportVar("DRESS_QUERY_MODE", "0")

                executeCommand("sbatch --nodes="+ str(currNodes) + " --ntasks="+str(currNodes)+ " --mem=200000MB --output " + outFileName + " " + str(conf['parisData']['scriptPath']))
                exportVar("DRESS_QUERY_MODE", "8")


    # STATIC SORTED GREEDY
    executeCommand("mkdir " + folder + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/greedy_exectimes_sorted")
    executeCommand("mkdir " + folderOutputs + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/greedy_exectimes_sorted")
    dirPrefix = folder +             str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/greedy_exectimes_sorted/"
    outPrefix = folderOutputs +      str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/greedy_exectimes_sorted/"
    exportVar("DRESS_QUERY_MODE", "12")
    exportVar("DRESS_QUERY_TYPE","bsf-dont-share")
    exportVar("DRESS_DISTRIBUTED_QUERIES_INITIAL_BURST", "1")
    for currNodes in nodes:
        if doMakeSortedQueriesStatic == 0:
            break

        exportVar("DRESS_NODES",str(currNodes))

        executeCommand("mkdir " + dirPrefix + "node" + str(currNodes))
        executeCommand("mkdir " + dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")
        
        executeCommand("mkdir " + outPrefix + "node" + str(currNodes))
        executeCommand("mkdir " + outPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")

        for run in range(runs):
            resultFile = dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
            outFileName= outPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
            
            exportVar("DRESS_RESULT_FILE", resultFile)
            
            if(currNodes > 10):
                executeCommand("sbatch --nodes="+ str(currNodes) + " --ntasks="+str(currNodes)+ " --mem=200000MB --partition ncpulargeshort --output " + outFileName + " " + str(conf['parisData']['scriptPath']))
            else:
                if currNodes == 1:
                    exportVar("DRESS_QUERY_MODE", "0")

                executeCommand("sbatch --nodes="+ str(currNodes) + " --ntasks="+str(currNodes)+ " --mem=200000MB --output " + outFileName + " " + str(conf['parisData']['scriptPath']))
                exportVar("DRESS_QUERY_MODE", "12")


    # STATIC UNSORTED GREEDY
    executeCommand("mkdir " + folder + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/greedy_exectimes_unsorted")
    executeCommand("mkdir " + folderOutputs + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/greedy_exectimes_unsorted")
    dirPrefix = folder +             str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/greedy_exectimes_unsorted/"
    outPrefix = folderOutputs +      str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/greedy_exectimes_unsorted/"
    exportVar("DRESS_QUERY_MODE", "11")
    exportVar("DRESS_QUERY_TYPE","bsf-dont-share")
    exportVar("DRESS_DISTRIBUTED_QUERIES_INITIAL_BURST", "1")
    for currNodes in nodes:
        if doMakeStaticUnsortedGreedy == 0:
            break

        exportVar("DRESS_NODES",str(currNodes))

        executeCommand("mkdir " + dirPrefix + "node" + str(currNodes))
        executeCommand("mkdir " + dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")
        
        executeCommand("mkdir " + outPrefix + "node" + str(currNodes))
        executeCommand("mkdir " + outPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")

        for run in range(runs):
            resultFile = dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
            outFileName= outPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
            
            exportVar("DRESS_RESULT_FILE", resultFile)
            
            if(currNodes > 10):
                executeCommand("sbatch --nodes="+ str(currNodes) + " --ntasks="+str(currNodes)+ " --mem=200000MB --partition ncpulargeshort --output " + outFileName + " " + str(conf['parisData']['scriptPath']))
            else:
                if currNodes == 1:
                    exportVar("DRESS_QUERY_MODE", "0")

                executeCommand("sbatch --nodes="+ str(currNodes) + " --ntasks="+str(currNodes)+ " --mem=200000MB --output " + outFileName + " " + str(conf['parisData']['scriptPath']))
                exportVar("DRESS_QUERY_MODE", "11")


    # DYNAMIC SORTED MODULE
    executeCommand("mkdir " + folder + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic_sorted")
    executeCommand("mkdir " + folderOutputs + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic_sorted")
    dirPrefix = folder +             str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic_sorted/"
    outPrefix = folderOutputs +      str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic_sorted/"
    exportVar("DRESS_QUERY_MODE", "9")
    exportVar("DRESS_QUERY_TYPE","bsf-dont-share")
    exportVar("DRESS_DISTRIBUTED_QUERIES_INITIAL_BURST", "1")
    for currNodes in nodes:
        if doMakeDynamicSortedModule == 0:
            break

        exportVar("DRESS_NODES",str(currNodes))

        executeCommand("mkdir " + dirPrefix + "node" + str(currNodes))
        executeCommand("mkdir " + dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")
        
        executeCommand("mkdir " + outPrefix + "node" + str(currNodes))
        executeCommand("mkdir " + outPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")

        for run in range(runs):
            resultFile = dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
            outFileName= outPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
            
            exportVar("DRESS_RESULT_FILE", resultFile)
            
            if(currNodes > 10):
                executeCommand("sbatch --nodes="+ str(currNodes) + " --ntasks="+str(currNodes)+ " --mem=200000MB --partition ncpulargeshort --output " + outFileName + " " + str(conf['parisData']['scriptPath']))
            else:
                if currNodes == 1:
                    exportVar("DRESS_QUERY_MODE", "0")

                executeCommand("sbatch --nodes="+ str(currNodes) + " --ntasks="+str(currNodes)+ " --mem=200000MB --output " + outFileName + " " + str(conf['parisData']['scriptPath']))
                exportVar("DRESS_QUERY_MODE", "9")

    # DYNAMIC SORTED THREAD
    executeCommand("mkdir " + folder + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic_sorted_thread")
    executeCommand("mkdir " + folderOutputs + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic_sorted_thread")
    dirPrefix = folder +             str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic_sorted_thread/"
    outPrefix = folderOutputs +      str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic_sorted_thread/"
    exportVar("DRESS_QUERY_MODE", "18")
    exportVar("DRESS_QUERY_TYPE","bsf-dont-share")
    exportVar("DRESS_DISTRIBUTED_QUERIES_INITIAL_BURST", "1")
    for currNodes in nodes:
        if doDynamicThreadSorted == 0:
            break

        exportVar("DRESS_NODES",str(currNodes))

        executeCommand("mkdir " + dirPrefix + "node" + str(currNodes))
        executeCommand("mkdir " + dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")
        
        executeCommand("mkdir " + outPrefix + "node" + str(currNodes))
        executeCommand("mkdir " + outPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")

        for run in range(runs):
            resultFile = dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
            outFileName= outPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
            
            exportVar("DRESS_RESULT_FILE", resultFile)
            
            if(currNodes > 10):
                executeCommand("sbatch --nodes="+ str(currNodes) + " --ntasks="+str(currNodes)+ " --mem=200000MB --partition ncpulargeshort --output " + outFileName + " " + str(conf['parisData']['scriptPath']))
            else:
                if currNodes == 1:
                    exportVar("DRESS_QUERY_MODE", "0")

                executeCommand("sbatch --nodes="+ str(currNodes) + " --ntasks="+str(currNodes)+ " --mem=200000MB --output " + outFileName + " " + str(conf['parisData']['scriptPath']))
                exportVar("DRESS_QUERY_MODE", "18")


    # DYNAMIC SORTED COORDINATOR
    executeCommand("mkdir " + folder + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic_sorted_coordinator")
    executeCommand("mkdir " + folderOutputs + str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic_sorted_coordinator")
    dirPrefix = folder +             str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic_sorted_coordinator/"
    outPrefix = folderOutputs +      str(conf['datasets'][datasetIndex]['name']) + "/distributedQueries/dynamic_sorted_coordinator/"
    exportVar("DRESS_QUERY_MODE", "10")
    exportVar("DRESS_QUERY_TYPE","bsf-dont-share")
    exportVar("DRESS_DISTRIBUTED_QUERIES_INITIAL_BURST", "1")
    realNodes = [x+1 for x in nodes]
    for currNodes in realNodes:
        if doMakeDynamicSortedCoordinator == 0:
            break

        if currNodes == 2:
            currNodes = 1

        exportVar("DRESS_NODES",str(currNodes))

        executeCommand("mkdir " + dirPrefix + "node" + str(currNodes))
        executeCommand("mkdir " + dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")
        
        executeCommand("mkdir " + outPrefix + "node" + str(currNodes))
        executeCommand("mkdir " + outPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")

        for run in range(runs):
            resultFile = dirPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
            outFileName= outPrefix + "node" + str(currNodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
            
            exportVar("DRESS_RESULT_FILE", resultFile)
            
            if(currNodes > 10):
                executeCommand("sbatch --nodes="+ str(currNodes) + " --ntasks="+str(currNodes)+ " --mem=200000MB --partition ncpulargeshort --output " + outFileName + " " + str(conf['parisData']['scriptPath']))
            else:
                if currNodes == 1:
                    exportVar("DRESS_QUERY_MODE", "0")

                executeCommand("sbatch --nodes="+ str(currNodes) + " --ntasks="+str(currNodes)+ " --mem=200000MB --output " + outFileName + " " + str(conf['parisData']['scriptPath']))
                exportVar("DRESS_QUERY_MODE", "10")


def distributedQueriesInitialOutburstExperiment(datasetIndex, runs, threads, nodes, bursts):

    exportVar("DRESS_PARALLEL_QUERY_THREADS", "1")
    exportVar("DRESS_WORK_STEALING", "no")
    exportVar("DRESS_CHUNK_TO_LOAD", "0")
    exportVar("DRESS_QUERY_TYPE","bsf-dont-share")
    exportVar("DRESS_QUERY_MODE", "5")
    exportVar("DRESS_NODES",str(nodes))
    exportVar("DRESS_QUERY_TYPE","bsf-dont-share")
    
    exportVar("DRESS_DATASET", str(conf['parisData']['savePath'])+str(conf['datasets'][datasetIndex]['filename']))
    exportVar("DRESS_DATASET_SIZE",str(conf['datasets'][datasetIndex]['size']))
    exportVar("DRESS_TIMESERIES_SIZE",str(conf['datasets'][datasetIndex]['tsSize']))
    exportVar("DRESS_QUERIES",str(conf['parisData']['savePath'])+str(conf['datasets'][datasetIndex]['queries']['filename']))
    exportVar("DRESS_QUERIES_SIZE",str(conf['datasets'][datasetIndex]['queries']['queriesSize']))
    
    exportVar("DRESS_INDEX_THREADS",str(threads[0]))
    exportVar("DRESS_QUERY_THREADS",str(threads[1]))

    executeCommand("mkdir outputs/")
    
    executeCommand("mkdir " + str(conf['datasets'][datasetIndex]['name']))
    executeCommand("mkdir " + str(conf['datasets'][datasetIndex]['name']) + "/initialBurst/")
    executeCommand("mkdir " + str(conf['datasets'][datasetIndex]['name'])+ "/initialBurst/nodes" + str(nodes) + "/")

   
    executeCommand("mkdir outputs/" + str(conf['datasets'][datasetIndex]['name']))
    executeCommand("mkdir outputs/" + str(conf['datasets'][datasetIndex]['name']) + "/initialBurst/")
    executeCommand("mkdir outputs/" + str(conf['datasets'][datasetIndex]['name']) + "/initialBurst/nodes" + str(nodes) + "/")

    dirPrefix =              str(conf['datasets'][datasetIndex]['name']) + "/initialBurst/nodes" + str(nodes) + "/"
    outPrefix = "outputs/" + str(conf['datasets'][datasetIndex]['name']) + "/initialBurst/nodes" + str(nodes) + "/"
    
    for burst in bursts:

        exportVar("DRESS_DISTRIBUTED_QUERIES_INITIAL_BURST", str(burst))

        executeCommand("mkdir " + dirPrefix + "burst" + str(burst))
        executeCommand("mkdir " + dirPrefix + "burst" + str(burst) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")
        
        executeCommand("mkdir " + outPrefix + "burst" + str(burst))
        executeCommand("mkdir " + outPrefix + "burst" + str(burst) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")

        for run in range(runs):
            resultFile = dirPrefix + "burst" + str(burst) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
            outFileName= outPrefix + "burst" + str(burst) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
            
            exportVar("DRESS_RESULT_FILE", resultFile)
            
            if(nodes > 10):
                executeCommand("sbatch --nodes="+ str(nodes) + " --ntasks="+str(nodes)+ " --mem=200000MB --partition ncpulargeshort --output " + outFileName + " " + str(conf['parisData']['scriptPath']))
            else:
                if nodes == 1:
                    exportVar("DRESS_QUERY_MODE", "0")

                executeCommand("sbatch --nodes="+ str(nodes) + " --ntasks="+str(nodes)+ " --mem=200000MB --output " + outFileName + " " + str(conf['parisData']['scriptPath']))
                exportVar("DRESS_QUERY_MODE", "5")
    

def pqTHexperiments(datasetIndex, datasetQueries, runs, threads, nodes, basis_func_file, th_factor_values):
    exportVar("DRESS_QUERIES",str(conf['parisData']['savePath'])+str(datasetQueries[0]))#+str(conf['datasets'][datasetIndex]['queries']['filename']))
    exportVar("DRESS_QUERIES_SIZE",str(datasetQueries[1]))#str(conf['datasets'][datasetIndex]['queries']['queriesSize']))

    exportVar("DRESS_PARALLEL_QUERY_THREADS", "1")
    exportVar("DRESS_WORK_STEALING", "no")
    exportVar("DRESS_CHUNK_TO_LOAD", "0")
    exportVar("DRESS_QUERY_TYPE","bsf-dont-share")
    exportVar("DRESS_QUERY_MODE", "2")
    exportVar("DRESS_DISTRIBUTED_QUERIES_INITIAL_BURST", "1")
    exportVar("DRESS_DATASET", str(conf['parisData']['savePath'])+str(conf['datasets'][datasetIndex]['filename']))
    exportVar("DRESS_DATASET_SIZE",str(conf['datasets'][datasetIndex]['size']))
    exportVar("DRESS_TIMESERIES_SIZE",str(conf['datasets'][datasetIndex]['tsSize']))
    
    exportVar("DRESS_INDEX_THREADS",str(threads[0]))
    exportVar("DRESS_QUERY_THREADS",str(threads[1]))
    exportVar("DRESS_NODES","1")

    exportVar("DRESS_ESTIMATIONS_FILENAME","test.txt")

    exportVar("DRESS_QUERY_MODE", "19")
    exportVar("DRESS_QUERY_TYPE","bsf-dont-share")

    exportVar("DRESS_BASIS_FUNCTION_FILENAME", basis_func_file)

    folder =        str(conf['datasets'][datasetIndex]['name']) +"_th_experiment_" + str(datasetQueries[1]) + "/"
    folderOutputs = str(conf['datasets'][datasetIndex]['name']) +"_th_experiment_" + str(datasetQueries[1])  + "/outputs_th" + str(datasetQueries[1]) +"/"

    executeCommand("mkdir " + folder)
    executeCommand("mkdir " + folderOutputs)
    executeCommand("mkdir " + folder + str(conf['datasets'][datasetIndex]['name']))
    executeCommand("mkdir " + folder + str(conf['datasets'][datasetIndex]['name']) + "/pq_th_experiments/")
    executeCommand("mkdir " + folderOutputs + str(conf['datasets'][datasetIndex]['name']))
    executeCommand("mkdir " + folderOutputs + str(conf['datasets'][datasetIndex]['name']) + "/pq_th_experiments/")

    executeCommand("mkdir " + folder + str(conf['datasets'][datasetIndex]['name']) + "/pq_th_experiments/")
    executeCommand("mkdir " + folderOutputs + str(conf['datasets'][datasetIndex]['name']) + "/pq_th_experiments/")
    dirPrefix = folder +           str(conf['datasets'][datasetIndex]['name']) + "/pq_th_experiments/"
    outPrefix = folderOutputs +    str(conf['datasets'][datasetIndex]['name']) + "/pq_th_experiments/"
    
    for th_val in th_factor_values:

        exportVar("DRESS_TH_DIV_FACTOR", str(th_val))

        executeCommand("mkdir " + dirPrefix + "th" + str(th_val))
        executeCommand("mkdir " + dirPrefix + "th" + str(th_val) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")
        executeCommand("mkdir " + outPrefix + "th" + str(th_val))
        executeCommand("mkdir " + outPrefix + "th" + str(th_val) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")

        for run in range(runs):
            resultFile = dirPrefix + "th" + str(th_val) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
            outFileName= outPrefix + "th" + str(th_val) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
            
            exportVar("DRESS_RESULT_FILE", resultFile)
            executeCommand("sbatch --nodes="+ str(1) + " --ntasks="+str(1)+ " --mem=200000MB --output " + outFileName + " " + str(conf['parisData']['scriptPath']))


def chatzakisWorkstealingExperiment(datasetIndex, datasetQueries, runs, threads, nodes, basis_func_file, th_factor_value):
    exportVar("DRESS_QUERIES",str(conf['parisData']['savePath'])+str(datasetQueries[0]))
    exportVar("DRESS_QUERIES_SIZE",str(datasetQueries[1]))

    exportVar("DRESS_PARALLEL_QUERY_THREADS", "1")
    exportVar("DRESS_WORK_STEALING", "no")
    exportVar("DRESS_CHUNK_TO_LOAD", "0")
    exportVar("DRESS_QUERY_TYPE","bsf-dont-share")
    exportVar("DRESS_QUERY_MODE", "2")
    exportVar("DRESS_DISTRIBUTED_QUERIES_INITIAL_BURST", "1")
    exportVar("DRESS_DATASET", str(conf['parisData']['savePath'])+str(conf['datasets'][datasetIndex]['filename']))
    exportVar("DRESS_DATASET_SIZE",str(conf['datasets'][datasetIndex]['size']))
    exportVar("DRESS_TIMESERIES_SIZE",str(conf['datasets'][datasetIndex]['tsSize']))
    
    exportVar("DRESS_INDEX_THREADS",str(threads[0]))
    exportVar("DRESS_QUERY_THREADS",str(threads[1]))
    exportVar("DRESS_NODES",str(nodes))

    exportVar("DRESS_ESTIMATIONS_FILENAME","test.txt")

    exportVar("DRESS_QUERY_MODE", "19")
    exportVar("DRESS_QUERY_TYPE","bsf-dont-share")
    
    exportVar("DRESS_BASIS_FUNCTION_FILENAME", basis_func_file)
    exportVar("DRESS_TH_DIV_FACTOR", str(th_factor_value))

    folder =        str(conf['datasets'][datasetIndex]['name']) +"_chatzakis_workstealing_" + str(datasetQueries[1]) + "/"
    folderOutputs = str(conf['datasets'][datasetIndex]['name']) +"_chatzakis_workstealing_" + str(datasetQueries[1])  + "/outputs_ws" + str(datasetQueries[1]) +"/"

    executeCommand("mkdir " + folder)
    executeCommand("mkdir " + folderOutputs)
    executeCommand("mkdir " + folder + str(conf['datasets'][datasetIndex]['name']))
    executeCommand("mkdir " + folder + str(conf['datasets'][datasetIndex]['name']) + "/_chatzakis_workstealing_/")
    executeCommand("mkdir " + folderOutputs + str(conf['datasets'][datasetIndex]['name']))
    executeCommand("mkdir " + folderOutputs + str(conf['datasets'][datasetIndex]['name']) + "/_chatzakis_workstealing_/")

    executeCommand("mkdir " + folder + str(conf['datasets'][datasetIndex]['name']) + "/_chatzakis_workstealing_/")
    executeCommand("mkdir " + folderOutputs + str(conf['datasets'][datasetIndex]['name']) + "/_chatzakis_workstealing_/")
    dirPrefix = folder +           str(conf['datasets'][datasetIndex]['name']) + "/_chatzakis_workstealing_/"
    outPrefix = folderOutputs +    str(conf['datasets'][datasetIndex]['name']) + "/_chatzakis_workstealing_/"
    
    
    
    executeCommand("mkdir " + dirPrefix + "ws" + str(nodes) )
    executeCommand("mkdir " + dirPrefix + "ws" + str(nodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")
    executeCommand("mkdir " + outPrefix + "ws" + str(nodes))
    executeCommand("mkdir " + outPrefix + "ws" + str(nodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")

    exportVar("DRESS_CH_WORKSTEALING", str(1))
    for run in range(runs):
        resultFile = dirPrefix + "ws" + str(nodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
        outFileName= outPrefix + "ws" + str(nodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
           
        exportVar("DRESS_RESULT_FILE", resultFile)
        executeCommand("sbatch --nodes="+ str(nodes) + " --ntasks="+str(nodes)+ " --mem=200000MB --output " + outFileName + " " + str(conf['parisData']['scriptPath']))

    executeCommand("mkdir " + dirPrefix + "nows" + str(nodes) )
    executeCommand("mkdir " + dirPrefix + "nows" + str(nodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")
    executeCommand("mkdir " + outPrefix + "nows" + str(nodes))
    executeCommand("mkdir " + outPrefix + "nows" + str(nodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")

    exportVar("DRESS_CH_WORKSTEALING", str(0))
    for run in range(runs):
        resultFile = dirPrefix + "nows" + str(nodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
        outFileName= outPrefix + "nows" + str(nodes) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
           
        exportVar("DRESS_RESULT_FILE", resultFile)
        executeCommand("sbatch --nodes="+ str(nodes) + " --ntasks="+str(nodes)+ " --mem=200000MB --output " + outFileName + " " + str(conf['parisData']['scriptPath']))


def chatzakisWorkstealingExperimentBatches(datasetIndex, datasetQueries, runs, threads, nodes, threshold, batches_to_send ,basis_function):
    exportVar("DRESS_QUERIES",str(conf['parisData']['savePath'])+str(datasetQueries[0]))
    exportVar("DRESS_QUERIES_SIZE",str(datasetQueries[1]))

    exportVar("DRESS_PARALLEL_QUERY_THREADS", "1")
    exportVar("DRESS_WORK_STEALING", "no")
    exportVar("DRESS_CHUNK_TO_LOAD", "0")
    exportVar("DRESS_QUERY_TYPE","bsf-dont-share")
    exportVar("DRESS_QUERY_MODE", "2")
    exportVar("DRESS_DISTRIBUTED_QUERIES_INITIAL_BURST", "1")
    exportVar("DRESS_DATASET", str(conf['parisData']['savePath'])+str(conf['datasets'][datasetIndex]['filename']))
    exportVar("DRESS_DATASET_SIZE",str(conf['datasets'][datasetIndex]['size']))
    exportVar("DRESS_TIMESERIES_SIZE",str(conf['datasets'][datasetIndex]['tsSize']))
    
    exportVar("DRESS_INDEX_THREADS",str(threads[0]))
    exportVar("DRESS_QUERY_THREADS",str(threads[1]))
    exportVar("DRESS_NODES",str(nodes))

    exportVar("DRESS_ESTIMATIONS_FILENAME","test.txt")

    exportVar("DRESS_QUERY_MODE", "19")
    exportVar("DRESS_QUERY_TYPE","bsf-dont-share")
    
    exportVar("DRESS_BASIS_FUNCTION_FILENAME", basis_function)
    exportVar("DRESS_TH_DIV_FACTOR", str(threshold))

    folder =        str(conf['datasets'][datasetIndex]['name']) +"_chatzakis_workstealing_" + str(datasetQueries[1]) + "/"
    folderOutputs = str(conf['datasets'][datasetIndex]['name']) +"_chatzakis_workstealing_" + str(datasetQueries[1])  + "/outputs_wsBatches" + str(datasetQueries[1]) +"/"

    executeCommand("mkdir " + folder)
    executeCommand("mkdir " + folderOutputs)
    executeCommand("mkdir " + folder + str(conf['datasets'][datasetIndex]['name']))
    executeCommand("mkdir " + folder + str(conf['datasets'][datasetIndex]['name']) + "/_chatzakis_workstealing_/")
    executeCommand("mkdir " + folderOutputs + str(conf['datasets'][datasetIndex]['name']))
    executeCommand("mkdir " + folderOutputs + str(conf['datasets'][datasetIndex]['name']) + "/_chatzakis_workstealing_/")

    executeCommand("mkdir " + folder + str(conf['datasets'][datasetIndex]['name']) + "/_chatzakis_workstealing_/")
    executeCommand("mkdir " + folderOutputs + str(conf['datasets'][datasetIndex]['name']) + "/_chatzakis_workstealing_/")
    dirPrefix = folder +           str(conf['datasets'][datasetIndex]['name']) + "/_chatzakis_workstealing_/"
    outPrefix = folderOutputs +    str(conf['datasets'][datasetIndex]['name']) + "/_chatzakis_workstealing_/"
    
    executeCommand("mkdir " + dirPrefix + "wsBatches" + str(nodes) )
    executeCommand("mkdir " + outPrefix + "wsBatches" + str(nodes))
    
    exportVar("DRESS_CH_WORKSTEALING", str(1))
    for batches in batches_to_send:
        for run in range(runs):
            exportVar("DRESS_BATCHES_TO_SEND", str(batches))
            executeCommand("mkdir " + dirPrefix + "wsBatches" + str(nodes)  + "/batch" + str(batches))
            executeCommand("mkdir " + dirPrefix + "wsBatches" + str(nodes)  + "/batch" + str(batches) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")
            executeCommand("mkdir " + outPrefix + "wsBatches" + str(nodes)  + "/batch" + str(batches))
            executeCommand("mkdir " + outPrefix + "wsBatches" + str(nodes)  + "/batch" + str(batches) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")

            resultFile = dirPrefix + "wsBatches" + str(nodes) + "/batch" + str(batches) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
            outFileName= outPrefix + "wsBatches" + str(nodes) + "/batch" + str(batches) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
           
            exportVar("DRESS_RESULT_FILE", resultFile)
            executeCommand("sbatch --nodes="+ str(nodes) + " --ntasks="+str(nodes)+ " --mem=200000MB --output " + outFileName + " " + str(conf['parisData']['scriptPath']))


def schedule_experiment(currNodes, dirPrefix, outPrefix,node_groups, share_bsf, threads, query_mode, default_query_mode, runs):
    exportVar("DRESS_NODES",str(currNodes))
    exportVar("DRESS_QUERY_MODE", str(query_mode))

    executeCommand("mkdir " + dirPrefix + "node" + str(currNodes))
    executeCommand("mkdir " + dirPrefix + "node" + str(currNodes) + "/groups" + str(node_groups))
    executeCommand("mkdir " + dirPrefix + "node" + str(currNodes) + "/groups" + str(node_groups) + "/bsf_share" + str(share_bsf))
    executeCommand("mkdir " + dirPrefix + "node" + str(currNodes) + "/groups" + str(node_groups) + "/bsf_share" + str(share_bsf) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")

    executeCommand("mkdir " + outPrefix + "node" + str(currNodes))
    executeCommand("mkdir " + outPrefix + "node" + str(currNodes) + "/groups" + str(node_groups))
    executeCommand("mkdir " + outPrefix + "node" + str(currNodes) + "/groups" + str(node_groups) + "/bsf_share" + str(share_bsf))
    executeCommand("mkdir " + outPrefix + "node" + str(currNodes) + "/groups" + str(node_groups) + "/bsf_share" + str(share_bsf) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")

    for run in range(runs):
        resultFile = dirPrefix + "node" + str(currNodes) + "/groups" + str(node_groups) + "/bsf_share" + str(share_bsf) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
        outFileName= outPrefix + "node" + str(currNodes) + "/groups" + str(node_groups) + "/bsf_share" + str(share_bsf) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
            
        exportVar("DRESS_RESULT_FILE", resultFile)
            
        if(currNodes > 10):
            executeCommand("sbatch --nodes="+ str(currNodes) + " --ntasks="+str(currNodes)+ " --mem=240000MB --partition ncpulargeshort --output " + outFileName + " " + str(conf['parisData']['scriptPath']))
        else:
            if currNodes == 1:
                exportVar("DRESS_QUERY_MODE", str(default_query_mode))

            executeCommand("sbatch --nodes="+ str(currNodes) + " --ntasks="+str(currNodes)+ " --mem=240000MB --output " + outFileName + " " + str(conf['parisData']['scriptPath']))
            exportVar("DRESS_QUERY_MODE", str(query_mode))
    

def arrange_pdr_experiment(folder, folderOutputs, datasetIndex, fname, version_folder, nodes, flag, node_groups, share_bsf, threads, query_mode,default_query_mode,runs):
    executeCommand("mkdir " + folder + str(conf['datasets'][datasetIndex]['name']) + fname + version_folder)
    executeCommand("mkdir " + folderOutputs + str(conf['datasets'][datasetIndex]['name']) + fname + version_folder)
    dirPrefix = folder +           str(conf['datasets'][datasetIndex]['name']) + fname + version_folder
    outPrefix = folderOutputs +    str(conf['datasets'][datasetIndex]['name']) + fname + version_folder
    for currNodes in nodes:        
        if flag == 0:
            break
        schedule_experiment(currNodes, dirPrefix, outPrefix, node_groups, share_bsf, threads, query_mode,default_query_mode,runs)


def pdr_experiments(datasetIndex, datasetQueries, runs, threads, nodes, node_groups, share_bsf, workstealing, threshold, basis_function, estimations_file):
    
    doMessiExp = 1
    doWSExp = 1
    
    exportVar("DRESS_QUERIES",str(conf['parisData']['savePath'])+str(datasetQueries[0]))
    exportVar("DRESS_QUERIES_SIZE",str(datasetQueries[1]))
    exportVar("DRESS_PARALLEL_QUERY_THREADS", "1")
    exportVar("DRESS_WORK_STEALING", "no")
    exportVar("DRESS_CHUNK_TO_LOAD", "0")
    exportVar("DRESS_QUERY_TYPE","bsf-dont-share")
    exportVar("DRESS_QUERY_MODE", "2")
    exportVar("DRESS_DISTRIBUTED_QUERIES_INITIAL_BURST", "1")
    exportVar("DRESS_DATASET", str(conf['parisData']['savePath'])+str(conf['datasets'][datasetIndex]['filename']))
    exportVar("DRESS_DATASET_SIZE",str(conf['datasets'][datasetIndex]['size']))
    exportVar("DRESS_TIMESERIES_SIZE",str(conf['datasets'][datasetIndex]['tsSize']))
    exportVar("DRESS_INDEX_THREADS",str(threads[0]))
    exportVar("DRESS_QUERY_THREADS",str(threads[1]))
    exportVar("DRESS_NODES","1")
    exportVar("DRESS_ESTIMATIONS_FILENAME",estimations_file)
    exportVar("DRESS_BASIS_FUNCTION_FILENAME", basis_function)
    exportVar("DRESS_TH_DIV_FACTOR", str(threshold))
    exportVar("DRESS_BSF_CHATZAKIS_SHARING", str(share_bsf))
    exportVar("DRESS_QUERY_TYPE","bsf-dont-share")
    exportVar("DRESS_NODE_GROUPS",str(node_groups))
    exportVar("DRESS_CH_WORKSTEALING",str(workstealing))
    exportVar("DRESS_BATCHES_TO_SEND", str(4))
    exportVar("DRESS_PREPROCESS", str(0))

    folder_id = "_pdr_distributed_queries_benchmark_noWS" + str(datasetQueries[1])
    output_folder_id = "outputs_benchmark" + str(datasetQueries[1]) 
    folder =        str(conf['datasets'][datasetIndex]['name']) + folder_id  + "/"
    folderOutputs = str(conf['datasets'][datasetIndex]['name']) + folder_id  + "/"+ output_folder_id +"/"

    fname = "/pdr_dq/"
    executeCommand("mkdir " + folder)
    executeCommand("mkdir " + folderOutputs)
    executeCommand("mkdir " + folder + str(conf['datasets'][datasetIndex]['name']))
    executeCommand("mkdir " + folder + str(conf['datasets'][datasetIndex]['name']) + fname)
    executeCommand("mkdir " + folderOutputs + str(conf['datasets'][datasetIndex]['name']))
    executeCommand("mkdir " + folderOutputs + str(conf['datasets'][datasetIndex]['name']) + fname)

    #######################
    ######## MESSI ########
    #######################
    default_query_mode = 0

    # BASELINE - MESSI
    #version_folder = "baseline_messi/"
    #query_mode = 2
    #arrange_pdr_experiment(folder, folderOutputs, datasetIndex, fname, version_folder, nodes, doMessiExp, node_groups, share_bsf, threads, query_mode,default_query_mode,runs)

    # DYNAMIC COORDINATOR - MESSI
    #version_folder = "dynamic_cooordinator_messi/"
    #query_mode = 8
    #arrange_pdr_experiment(folder, folderOutputs, datasetIndex, fname, version_folder, nodes, doMessiExp, node_groups, share_bsf, threads, query_mode,default_query_mode,runs)

    # DYNAMIC MODULE - MESSI
    #version_folder = "dynamic_module_messi/"
    #query_mode = 5
    #arrange_pdr_experiment(folder, folderOutputs, datasetIndex, fname, version_folder, nodes, doMessiExp, node_groups, share_bsf, threads, query_mode,default_query_mode,runs)

    # DYNAMIC THREAD - MESSI
    #version_folder = "dynamic_thread_messi/"
    #query_mode = 17
    #arrange_pdr_experiment(folder, folderOutputs, datasetIndex, fname, version_folder, nodes, doMessiExp, node_groups, share_bsf, threads, query_mode,default_query_mode,runs)

    # GREEDY SORTED - MESSI
    #version_folder = "greedy_sorted_messi/"
    #query_mode = 12
    #arrange_pdr_experiment(folder, folderOutputs, datasetIndex, fname, version_folder, nodes, doMessiExp, node_groups, share_bsf, threads, query_mode,default_query_mode,runs)

    # GREEDY UNSORTED - MESSI
    #version_folder = "greedy_unsorted_messi/"
    #query_mode = 11
    #arrange_pdr_experiment(folder, folderOutputs, datasetIndex, fname, version_folder, nodes, doMessiExp, node_groups, share_bsf, threads, query_mode,default_query_mode,runs)

    # DYNAMIC SORTED MODULE - MESSI
    #version_folder = "dynamic_module_sorted_messi/"
    #query_mode = 9
    #arrange_pdr_experiment(folder, folderOutputs, datasetIndex, fname, version_folder, nodes, doMessiExp, node_groups, share_bsf, threads, query_mode,default_query_mode,runs)

    # DYNAMIC SORTED COORDINATOR - MESSI
    #version_folder = "dynamic_coordinator_sorted_messi/"
    #query_mode = 10
    #arrange_pdr_experiment(folder, folderOutputs, datasetIndex, fname, version_folder, nodes, doMessiExp, node_groups, share_bsf, threads, query_mode,default_query_mode,runs)

    # DYNAMIC SORTED THREAD - MESSI
    #version_folder = "dynamic_thread_sorted_messi/"
    #query_mode = 18
    #arrange_pdr_experiment(folder, folderOutputs, datasetIndex, fname, version_folder, nodes, doMessiExp, node_groups, share_bsf, threads, query_mode,default_query_mode,runs)

    ##########################
    ######## WS_MESSI ########
    ##########################

    default_query_mode = 19

    # BASELINE - WS
    #version_folder = "baseline_ws/"
    #query_mode = 21
    #arrange_pdr_experiment(folder, folderOutputs, datasetIndex, fname, version_folder, nodes, doWSExp, node_groups, share_bsf, threads, query_mode,default_query_mode,runs)

    # DYNAMIC MODULE - WS
    version_folder = "dynamic_module_ws/"
    query_mode = 22
    arrange_pdr_experiment(folder, folderOutputs, datasetIndex, fname, version_folder, nodes, doWSExp, node_groups, share_bsf, threads, query_mode,default_query_mode,runs)

    # DYNAMIC COORDINATOR - WS
    #version_folder = "dynamic_coordinator_ws/"
    #query_mode = 23
    #arrange_pdr_experiment(folder, folderOutputs, datasetIndex, fname, version_folder, nodes, doWSExp, node_groups, share_bsf, threads, query_mode,default_query_mode,runs)

    # DYNAMIC THREAD - WS
    version_folder = "dynamic_thread_ws/"
    query_mode = 24
    arrange_pdr_experiment(folder, folderOutputs, datasetIndex, fname, version_folder, nodes, doWSExp, node_groups, share_bsf, threads, query_mode,default_query_mode,runs)

    # GREEDY SORTED - WS
    #version_folder = "greedy_sorted_ws/"
    #query_mode = 29
    #arrange_pdr_experiment(folder, folderOutputs, datasetIndex, fname, version_folder, nodes, doWSExp, node_groups, share_bsf, threads, query_mode,default_query_mode,runs)

    # GREEDY UNSORTED - WS
    #version_folder = "greedy_unsorted_ws/"
    #query_mode = 28
    #arrange_pdr_experiment(folder, folderOutputs, datasetIndex, fname, version_folder, nodes, doWSExp, node_groups, share_bsf, threads, query_mode,default_query_mode,runs)

    # DYNAMIC SORTED MODULE - WS
    #version_folder = "dynamic_module_sorted_ws/"
    #query_mode = 25
    #arrange_pdr_experiment(folder, folderOutputs, datasetIndex, fname, version_folder, nodes, doWSExp, node_groups, share_bsf, threads, query_mode,default_query_mode,runs)

    # DYNAMIC SORTED COORDINATOR - WS
    #version_folder = "dynamic_coordinator_sorted_ws/"
    #query_mode = 26
    #arrange_pdr_experiment(folder, folderOutputs, datasetIndex, fname, version_folder, nodes, doWSExp, node_groups, share_bsf, threads, query_mode,default_query_mode,runs)

    # DYNAMIC SORTED TRHEAD - WS
    #version_folder = "dynamic_thread_sorted_ws/"
    #query_mode = 27
    #arrange_pdr_experiment(folder, folderOutputs, datasetIndex, fname, version_folder, nodes, doWSExp, node_groups, share_bsf, threads, query_mode,default_query_mode,runs)


def index_scalab_experiment(datasetIndex, datasetQueries, runs, threads, nodes, node_groups, sizes):
    exportVar("DRESS_QUERIES",str(datasetQueries[0]))
    exportVar("DRESS_QUERIES_SIZE",str(datasetQueries[1]))
    exportVar("DRESS_PARALLEL_QUERY_THREADS", "1")
    exportVar("DRESS_WORK_STEALING", "no")
    exportVar("DRESS_CHUNK_TO_LOAD", "0")
    exportVar("DRESS_QUERY_TYPE","bsf-dont-share")
    exportVar("DRESS_QUERY_MODE", "0")
    exportVar("DRESS_DISTRIBUTED_QUERIES_INITIAL_BURST", "1")
    exportVar("DRESS_DATASET", str(conf['parisData']['savePath'])+str(conf['datasets'][datasetIndex]['filename']))
    
    exportVar("DRESS_TIMESERIES_SIZE",str(conf['datasets'][datasetIndex]['tsSize']))
    exportVar("DRESS_INDEX_THREADS",str(threads[0]))
    exportVar("DRESS_QUERY_THREADS",str(threads[1]))
    exportVar("DRESS_NODES",str(nodes))
    exportVar("DRESS_ESTIMATIONS_FILENAME","text.txt")
    exportVar("DRESS_BASIS_FUNCTION_FILENAME", "text.txt")
    exportVar("DRESS_TH_DIV_FACTOR", str(8))
    exportVar("DRESS_BSF_CHATZAKIS_SHARING", str(0))
    exportVar("DRESS_QUERY_TYPE","bsf-dont-share")
    exportVar("DRESS_NODE_GROUPS",str(node_groups))

    folder_id = "_index_scalability_bench" + str(conf['datasets'][datasetIndex]['name'])
    output_folder_id = "outputs_benchmark" + str(conf['datasets'][datasetIndex]['name'])
    folder =        str(conf['datasets'][datasetIndex]['name']) + folder_id  + "/"
    folderOutputs = str(conf['datasets'][datasetIndex]['name']) + folder_id  + "/"+ output_folder_id +"/"

    fname = "/index_sc/"
    executeCommand("mkdir " + folder)
    executeCommand("mkdir " + folderOutputs)
    executeCommand("mkdir " + folder + str(conf['datasets'][datasetIndex]['name']))
    executeCommand("mkdir " + folder + str(conf['datasets'][datasetIndex]['name']) + fname)
    executeCommand("mkdir " + folderOutputs + str(conf['datasets'][datasetIndex]['name']))
    executeCommand("mkdir " + folderOutputs + str(conf['datasets'][datasetIndex]['name']) + fname)

    version_folder = "classic_index_creation/"
    executeCommand("mkdir " + folder + str(conf['datasets'][datasetIndex]['name']) + fname + version_folder)
    executeCommand("mkdir " + folderOutputs + str(conf['datasets'][datasetIndex]['name']) + fname + version_folder)
    dirPrefix = folder +           str(conf['datasets'][datasetIndex]['name']) + fname + version_folder
    outPrefix = folderOutputs +    str(conf['datasets'][datasetIndex]['name']) + fname + version_folder
    for size in sizes:        
       
        exportVar("DRESS_DATASET_SIZE",str(size))

        executeCommand("mkdir " + dirPrefix + "node" + str(nodes))
        executeCommand("mkdir " + dirPrefix + "node" + str(nodes) + "/groups" + str(node_groups))
        executeCommand("mkdir " + dirPrefix + "node" + str(nodes) + "/groups" + str(node_groups) + "/size" + str(size))
        executeCommand("mkdir " + dirPrefix + "node" + str(nodes) + "/groups" + str(node_groups) + "/size" + str(size) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")

        executeCommand("mkdir " + outPrefix + "node" + str(nodes))
        executeCommand("mkdir " + outPrefix + "node" + str(nodes) + "/groups" + str(node_groups))
        executeCommand("mkdir " + outPrefix + "node" + str(nodes) + "/groups" + str(node_groups) + "/size" + str(size))
        executeCommand("mkdir " + outPrefix + "node" + str(nodes) + "/groups" + str(node_groups) + "/size" + str(size) + "/threads" + str(threads[0]) + "-" + str(threads[1]) +"/")

        for run in range(runs):
            resultFile = dirPrefix + "node" + str(nodes) + "/groups" + str(node_groups) + "/size" + str(size) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
            outFileName= outPrefix + "node" + str(nodes) + "/groups" + str(node_groups) + "/size" + str(size) + "/threads" + str(threads[0]) + "-" + str(threads[1]) + "/" + "run" + str(run) + ".txt"
            
            exportVar("DRESS_RESULT_FILE", resultFile)
            
            if(nodes > 10):
                executeCommand("sbatch --nodes="+ str(nodes) + " --ntasks="+str(nodes)+ " --mem=240000MB --partition ncpulargeshort --output " + outFileName + " " + str(conf['parisData']['scriptPath']))
            else:
                if nodes == 1:
                    exportVar("DRESS_QUERY_MODE", "0")

                executeCommand("sbatch --nodes="+ str(nodes) + " --ntasks="+str(nodes)+ " --mem=240000MB --output " + outFileName + " " + str(conf['parisData']['scriptPath']))
                exportVar("DRESS_QUERY_MODE", "0")


def calc_index_scalab():
    runs = 5
    threads = [16,64]
    nodes =[1,2,4,8,16]
    dataset_indices = [0,1,2,3,4]
   
    #seismic
    for node in nodes:
        sizes = [25 * 1000000, 50 * 1000000 ,75 * 1000000 ,100 * 1000000]
        q = str(conf['parisData']['savePath'])+str(conf['datasets'][0]['queries']['filename'])
        print(q)
        index_scalab_experiment(0, [q,1], runs, threads, node, node, sizes)
    
    #sift
    for node in nodes:
        sizes = [25 * 10000000, 50 * 10000000 ,75 * 10000000 ,100 * 10000000]
        q = str(conf['parisData']['savePath'])+str(conf['datasets'][1]['queries']['filename'])
        print(q)
        index_scalab_experiment(1, [q,1], runs, threads, node, node, sizes)
    
    #astro
    for node in nodes:
        sizes = [67.5 * 1000000, 135 * 1000000 ,202.5 * 1000000 ,270 * 1000000]
        q = str(conf['parisData']['savePath'])+str(conf['datasets'][2]['queries']['filename'])
        print(q)
        index_scalab_experiment(2, [q,1], runs, threads, node, node, sizes)
    
    #deep
    for node in nodes:
        sizes = [25 * 10000000, 50 * 10000000 ,75 * 10000000 ,100 * 10000000]
        q = str(conf['parisData']['savePath'])+str(conf['datasets'][4]['queries']['filename'])
        print(q)
        index_scalab_experiment(4, [q,1], runs, threads, node, node, sizes)
    
    
def calc_pdr_ex_seismic():
    est_f = "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/query_analysis/benchmark_estimations/query_execution_time_estimations_benchmark33.txt"
    
    runs = 5
    th = 16 # or 16?
    
    nodes = [1,2,4,8]
    threads = [16,64]
    
    workstealing = 1
    share_bsf = 1

    # Seismic
    dataset = 0
    est_func = "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/pq_analysis/estimations_parameters/seismic_100classic_sigmoid.txt"
    workload1 = ["queries_ctrl100_seismic_len256_znorm.bin", 100,  "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/query_analysis/official_execution_times/query_execution_time_estimations_seismic1.txt"] #!
    workload2 = ["benchmark101_seismic_len256_znorm.bin",   101, "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/query_analysis/benchmark_estimations/query_execution_time_estimations_benchmark101.txt"]
    workload3 = ["benchmark51_seismic_len256_znorm.bin",   51, "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/query_analysis/benchmark_estimations/query_execution_time_estimations_benchmark51.txt"]
    workload4 = ["benchmark201_seismic_len256_znorm.bin",   201, "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/query_analysis/benchmark_estimations/query_execution_time_estimations_benchmark201.txt"]
    workload5 = ["benchmark301_seismic_len256_znorm.bin",   301, "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/query_analysis/benchmark_estimations/query_execution_time_estimations_benchmark301.txt"]
    workload6 = ["benchmark401_seismic_len256_znorm.bin",   401, "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/query_analysis/benchmark_estimations/query_execution_time_estimations_benchmark401.txt"]

    # Astro
    #dataset = 2 
    #est_func = "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/pq_analysis/estimations_parameters/astro_100classic_sigmoid.txt"
    #workloadA1 = ["astro_bench/queries_ctrl100_astro_len256_znorm.bin", 100, "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/query_analysis/benchmark_estimations/astro100.txt"]
    #workloadA2 = ["astro_bench/queries_ctrl200_astro_len256_znorm.bin", 200, "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/query_analysis/benchmark_estimations/astro200.txt"]
    #workloadA3 = ["astro_bench/queries_ctrl400_astro_len256_znorm.bin", 400, "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/query_analysis/benchmark_estimations/astro400.txt"]
    #workloadA4 = ["astro_bench/queries_ctrl800_astro_len256_znorm.bin", 800, "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/query_analysis/benchmark_estimations/astro800.txt"]

    #Random 100
    #dataset = 6
    #est_func = "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/pq_analysis/estimations_parameters/random_100classic_sigmoid.txt"
    #workload9 = ["randomDatasets/queries_ctrl1000_random_len256_znorm.bin", 100, "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/query_analysis/benchmark_estimations/query_execution_time_estimations_astro_benchmark201.txt"]
    #workload10 = ["randomDatasets/queries_ctrl1000_random_len256_znorm.bin", 200, "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/query_analysis/benchmark_estimations/query_execution_time_estimations_astro_benchmark201.txt"]
    #workload11 = ["randomDatasets/queries_ctrl1000_random_len256_znorm.bin", 400, "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/query_analysis/benchmark_estimations/query_execution_time_estimations_astro_benchmark201.txt"]
    #workload12 = ["randomDatasets/queries_ctrl1000_random_len256_znorm.bin", 800, "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/query_analysis/benchmark_estimations/query_execution_time_estimations_astro_benchmark201.txt"]

    #workload13 = ["benchmark1001_seismic_len256_znorm.bin", 800, "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/query_analysis/benchmark_estimations/query_execution_time_estimations_benchmark1001.txt"]

    #new experiments
    workload1 = ["new-experiments/queries_ctrl1600_seismic_len256_znorm.bin", 100,  "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/query_analysis/official_execution_times/query_execution_time_estimations_seismic1.txt"] #!

    workloads = [workload2]

    for workload in workloads :
        est_f = workload[2]

        nodes = [1,2,4,8]
        node_groups = 1
        pdr_experiments(dataset, workload, runs, threads, nodes, node_groups, share_bsf, workstealing, th, est_func, est_f)
        
        nodes = [2,4,8]
        node_groups = 2
        pdr_experiments(dataset, workload, runs, threads, nodes, node_groups, share_bsf, workstealing, th, est_func, est_f)
        
        nodes = [4,8]
        node_groups = 4
        pdr_experiments(dataset, workload, runs, threads, nodes, node_groups, share_bsf, workstealing, th, est_func, est_f)
        
        nodes = [8]
        node_groups = 8
        pdr_experiments(dataset, workload, runs, threads, nodes, node_groups, share_bsf, workstealing, th, est_func, est_f)
        
        #nodes = [16]
        #node_groups = 16
        #pdr_experiments(0, workload, runs, threads, nodes, node_groups, share_bsf, workstealing, th, est_func, est_f)

    
def main():
    # module load intel/21U2/suite
    # module load python/3.9.5
    #   
    print("-- Scheduler Script Start --")
    
    #scheduleParis([3], ["bsf-dont-share", "bsf-block-per-query"], 5, [2,4,8,16], [[16,64]])
    
    #distributedQueriesExperiment(0, ["benchmark65_seismic_len256_znorm.bin", 65],     10, [16,64], [1,2,4,8],     "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/query_analysis/benchmark_estimations/query_execution_time_estimations_benchmark65.txt")
    #distributedQueriesExperiment(0, ["benchmark129_seismic_len256_znorm.bin", 129],   10, [16,64], [1,2,4,8],     "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/query_analysis/benchmark_estimations/query_execution_time_estimations_benchmark129.txt")
    #distributedQueriesExperiment(0, ["benchmark1001_seismic_len256_znorm.bin", 1001], 10, [16,64], [1,2,4,8],     "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/query_analysis/benchmark_estimations/query_execution_time_estimations_benchmark1001.txt")
    #distributedQueriesExperiment(0, ["benchmark33_seismic_len256_znorm.bin", 33],     10, [16,64], [1,2,4,8],     "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/query_analysis/benchmark_estimations/query_execution_time_estimations_benchmark33.txt")
    #distributedQueriesExperiment(0, ["benchmark101_seismic_len256_znorm.bin", 101],   10, [16,64], [1,2,4,8],     "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/query_analysis/benchmark_estimations/query_execution_time_estimations_benchmark101.txt")
    #distributedQueriesExperiment(0, ["benchmark201_seismic_len256_znorm.bin", 201],   10, [16,64], [1,2,4,8],     "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/query_analysis/benchmark_estimations/query_execution_time_estimations_benchmark201.txt")
    
    #distributedQueriesExperiment(6, ["randomDatasets/queries_ctrl1000_random_len256_znorm.bin", 100], 10, [16,64], [1,2,4,8])

    #pqTHexperiments(0, ["queries_ctrl100_seismic_len256_znorm.bin", 100], 5, [16,64], 1, 
    #"/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/pq_analysis/estimations_parameters/seismic_100classic_sigmoid.txt", [1,2,4,8, 16, 32, 64 ,128, 256, 512])

    #pqTHexperiments(0, ["queries_size10K_seismic_len256_znorm.bin", 200], 5, [16,64], 1, 
    #"/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/pq_analysis/estimations_parameters/seismic_100classic_sigmoid.txt", [1,2,4,8, 16, 32, 64 ,128, 256, 512])

    #pqTHexperiments(0, ["benchmark1001_seismic_len256_znorm.bin", 1001], 5, [16,64], 1, 
    #"/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/pq_analysis/estimations_parameters/seismic_100classic_sigmoid.txt", [1,2,4,8, 16, 32, 64 ,128, 256, 512])

    #chatzakisWorkstealingExperiment(0, ["benchmark2_seismic_len256_znorm.bin", 2], 10, [16,64], 2, "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/pq_analysis/estimations_parameters/seismic_100classic_sigmoid.txt", 16)
    #chatzakisWorkstealingExperimentBatches(0,  ["benchmark2_seismic_len256_znorm.bin", 2], 10, [16,64], 2, 16, [1,2,3,4,5,6,7,8,9,10], "/gpfs/users/chatzakis/Thesis-Manos-Chatzakis/DRESS/ads/run_automation/pq_analysis/estimations_parameters/seismic_100classic_sigmoid.txt")
    
    calc_pdr_ex_seismic()
    #calc_index_scalab()

    print("-- Scheduler Script End --")


if __name__ == "__main__":
    main()

