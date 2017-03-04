import json
import os

def readInfoboxConfigFromFile(file):
    outputList = []
    with open(file, "r", encoding="utf-8") as infile:
        for line in infile:
            outputList.append(line)

    return json.loads("".join(outputList))

def readInfoboxConfig():
    infoboxConfig = {}
    directory = "infobox_configs/"
    for file in os.listdir(directory):
        if os.path.isfile(directory + file):
            infoboxConfig.update(readInfoboxConfigFromFile(directory + file))
    return infoboxConfig
