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
    

def downloadDatasetsAndQueries(saveDirectory, authorizationToken):
    #https://www.quora.com/How-do-I-download-a-very-large-file-from-Google-Drive#:~:text=Downloading%20files%20from%20Google%20drive,with%20sufficient%20space%20for%20file

    datasetSaveName = ["data_size100M_seismic_len256_znorm.bin","data_size270M_astronomy_len256_znorm.bin","data_size1B_deep1b_len96_znorm.bin","data_size899M_sald_len128_znorm.bin","data_size1B_sift_len128_znorm.bin"]
    datasetIDs = ["1XML3uywZdLxChlk_JgglPtVIEEZGs5qD","1bmajMOgdR-QhQXujVpoByZ9gHpprUncV","1ecvWA8i0ql-cmMI4oL63oOUIYzaWRc4z", "1HX2PzYhRrOg381jFMkFNvESUMfRnmv2j", "1kWoKRMyaW2jLmOyD-xkaopAbp3GfHbJy"]
    dataset2downloadIndices = [3]

    print("Downloading datasets...")

    for i in dataset2downloadIndices:
        command = "curl -H \"Authorization: Bearer "+authorizationToken+"\" https://www.googleapis.com/drive/v3/files/"+datasetIDs[i]+"?alt=media -o "+saveDirectory+datasetSaveName[i] 
        executeCommand(command)

    queriesSaveName = ["queries_ctrl100_seismic_len256_znorm.bin","queries_ctrl100_astronomy_len256_znorm.bin","queries_ctrl100_sald_len128_znorm.bin","queries_ctrl100_deep1b_len96_znorm.bin","queries_size100_sift_len128_znorm.bin"]
    queryIDs = ["14iwjV3JBCxZVBtcbgL3VPzPL8CNa_iGL","1xRZpPOiZSCCkQS6lwBmHyiKPuJ7oXg6A","1rtNcDl1YyJXuZ1VM1uyJr1odooPm0eNX","13n60tns1HaAxspB1xR0ZnOtzR4FkRmc7","1DEYgiPc0u6RWg7ALEIy7TxiJIBNfJMIO"]
    queries2downloadIndices = []

    print("Downloading queries...")

    #curl -H "Authorization: Bearer ya29.a0ARrdaM_sUIyADMboRggeB7XyXdakgjOsySYJzdZWFWlOpXIPDEITWxqyeCAJrw1-R5D1w29v8w072rY9eV_8dVR07tLsHNl5fO2g59Wy78ZSM823bdjVpvBktuRJyvPE-8IjlK07OM4k4IW1nP4fPfQ7o1mI" https://www.googleapis.com/drive/v3/files/1kWoKRMyaW2jLmOyD-xkaopAbp3GfHbJy?alt=media -o data_size1B_sift_len128_znorm.bin
    #curl -H "Authorization: Bearer ya29.A0ARrdaM-M0842AujQkNzv5fJPQVoiExbOgQvNJbhLblgtw863rWKoIClQdevCFMOBCS_8jcVEKpvbOMtraciVQQI7Br1igtsJmlAJYKu_5wxvia1qb12ceJjKIQuVuoqq1eVVVS5TE3bH4mARIENo54BShfxl" https://www.googleapis.com/drive/v3/files/1kWoKRMyaW2jLmOyD-xkaopAbp3GfHbJy?alt=media -o /spare/chatzakis/datasets/data_size1B_sift_len128_znorm.bin
    
    for i in queries2downloadIndices:
        command = "curl -H \"Authorization: Bearer "+authorizationToken+"\" https://www.googleapis.com/drive/v3/files/"+queryIDs[i]+"?alt=media -o "+saveDirectory+queriesSaveName[i] 
        executeCommand(command)


def main():
    print("-- Downloader Script Start --")
    
    downloadDatasetsAndQueries(conf['parisData']['savePath'],
        "ya29.A0ARrdaM93Kc4Dq7K3EVi4Re2rSXQF9boDax9laJ8dP0rmJzBFX3eai5BsS-cVzsu9QzGEAfJ9shZ-316yFDocXd-T1s1XEpeCozGuemCw3adaQPNvMh0tMwFxsfqNXWbfnCwgKrVmI9RgGPgX5por4LlDCVbq")

    print("-- Downloader Script End --")


if __name__ == "__main__":
    main()