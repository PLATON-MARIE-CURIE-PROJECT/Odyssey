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


def transferFileWithScp(files, hosts, destDir):
    #scp file.txt remote_username@10.10.0.2:/remote/directory
    for file in files:
        for host in hosts:
            command = "scp " + file + " " + host + ":" +destDir
            executeCommand(command)


def main():
    print("-- Transfer Script Start --")

    print("-- Transfer Script End --")


if __name__ == "__main__":
    main()
    