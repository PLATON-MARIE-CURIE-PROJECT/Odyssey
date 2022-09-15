import os
import json
import subprocess
import shutil

from os import path

def executeCommand(command):
    print(">>>",command)
    out = subprocess.getoutput(command)
    print(out)

def copy(fromF, toF):
    shutil.copyfile(fromF, toF)

def exportVar(name,value):
    os.environ[name] = value


def compile():
    executeCommand("make clean")
    copy("run_automation/splitMakes/MakefileS.txt", "Makefile.am")
    executeCommand("make")
    copy("run_automation/splitMakes/MakefileLIBADS.txt", "Makefile.am")
    executeCommand("make")
    copy("run_automation/splitMakes/MakefileComplete.txt", "Makefile")
    executeCommand("make")



def main():
    print("-- Compile Script Start --")
    
    compile()
    
    print("-- Compile Script End --")

    
if __name__ == "__main__":
    main()
