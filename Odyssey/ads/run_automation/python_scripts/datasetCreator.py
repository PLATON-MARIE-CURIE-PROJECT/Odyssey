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


def createRandomDatasets(desiredSizeInGB, savePath):
    len = 256
    size1GB = 1048576
    generatorPath = "../../dataset/dataset/generator"
    folderPath = savePath + "randomDatasets/"

    executeCommand("mkdir "+ folderPath)

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
    folderPath = savePath + "randomDatasets/"
    
    executeCommand("mkdir "+ folderPath)

    for currSize in desiredSizesInSize:
        seed = currSize*inputSeed
        
        exportVar("GSL_RNG_SEED",str(seed))
        datasetName = "queries_ctrl"+str(currSize)+"_random_len"+str(len)+"_znorm.bin"
        
        command = "./" + generatorPath + " --size " + str(currSize) + " --length " + str(len) + " --z-normalize " + " > " + (folderPath+datasetName)
        executeCommand(command)


def main():
    print("-- Creator Script Start --")

    createRandomDatasets([5,10,20], conf['parisData']['savePath'])
    createRandomQueries([1000], conf['parisData']['savePath'], 42)

    print("-- Creator Script End --")


if __name__ == "__main__":
    main()