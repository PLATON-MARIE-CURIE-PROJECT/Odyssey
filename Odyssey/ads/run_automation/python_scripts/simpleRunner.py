import os
import json
import subprocess
from os import path

conf = json.load(open("./configuration.json", encoding="utf8"))


def executeCommand(command):
    print(">>>",command)
    out = subprocess.getoutput(command)
    print(out)


def exportVar(name,value):
    os.environ[name] = value


def run_simple(nodes, threads, dataset, datasetSize, queries, queriesNumber, bsfYesNo, tsSize, mode):
    resultFile = "./result.txt"
    command = "mpirun -n " + str(nodes) + " ../bin/ads --simple-work-stealing no --all-nodes-index-all-dataset no --verbose no --share-bsf " + str(bsfYesNo) + " --output-file " +str(resultFile) + " --mpi-already-splitted-dataset no --file-label \"test\" --dataset " + str(dataset) +"  --leaf-size 2000 --initial-lbl-size 2000 --min-leaf-size 2000 --dataset-size " + str(datasetSize) + " --flush-limit 1000000  --function-type 9990 --in-memory --queries " + str(queries) + " --queries-size " + str(queriesNumber) + " --read-block 20000 --timeseries-size " + str(tsSize) + " --index-workers "+ str(threads[0])+" --query-workers " + str(threads[1]) + " --parallel-query-workers " +  str(threads[2]) + " --query-mode " + str(mode)
    #print(command)
    executeCommand(command)


def main():
    print("-- Runner Script Start --")
    
    id = 0
    run_simple(1,[16,16,2], conf['parisData']['savePath'] + conf['datasets'][id]['filename'], 1000000,  conf['parisData']['savePath'] + "benchmark1001_seismic_len256_znorm.bin", "10", "bsf-dont-share", conf['datasets'][id]['tsSize'], mode=19)
    
    print("-- Runner Script End --")


if __name__ == "__main__":
    main()