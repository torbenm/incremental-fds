# -*- coding: utf-8 -*-

import json
import copy
import os
import re
import collections

import sys


def calculate_statistics():
    # filename = "wikipedia_infobox_dataset_head_100.json"
    filename = "20120323-en-updates.json"

    articles_count_by_infobox_type = {}

    with open(filename, 'r', encoding="utf-8") as infile:
        line_count = 0
        for line in infile:
            wikipedia_json = json.loads(line)

            infobox_type = wikipedia_json["attribute"][0]["infobox_name"]
            infobox_type = infobox_type.split("\n")[0].lower()

            if not infobox_type in articles_count_by_infobox_type:
                articles_count_by_infobox_type[infobox_type] = 0
            articles_count_by_infobox_type[infobox_type] += 1

            line_count += 1
            if line_count % 1000 == 0:
                print(line_count)

    return articles_count_by_infobox_type


def write_statistics(articles_count_by_infobox_type):
    sorted_article_types = sorted(articles_count_by_infobox_type.keys(),
                                  key=lambda x: articles_count_by_infobox_type[x], reverse=True)

    with open('wikipedia_updates_statistics.txt', 'w', encoding="utf-8") as outfile:
        for key in sorted_article_types:
            write_string = key + " - " + str(articles_count_by_infobox_type[key]) + "\n"
            outfile.write(write_string)


def read_statistics(row_count_threshold):
    with open('wikipedia_updates_statistics.txt', 'r') as infile:
        articles_count_by_infobox_type = {}
        for line in infile:
            split = line.split(" - ")
            if len(split) < 2:
                continue
            ## since datasets with less than at least 100 rows are not of interest, we're skipping them here
            if int(split[1]) < 100:
                continue
            articles_count_by_infobox_type[split[0]] = split[1]
    return articles_count_by_infobox_type


def replace_stuff(filename):
    filename = filename.replace("/", "").replace("Ö", "Oe").replace("Ä", "Ae").replace("Ü", "Ue").replace("ö",
                                                                                                          "oe").replace(
        "ä", "ae").replace("ü", "ue").replace(":", "").replace("\\", "")
    filename = filename.replace("<", "").replace(">", "").replace("|", "").replace("?", "").replace("*", "").replace(
        "\n", "").replace(".", "").replace("'", "").replace(",", "").replace("\t", "")
    return "files_by_infobox_type/" + filename


def dump_data_into_files(articles_by_infobox_type):
    if not os.path.isdir("files_by_infobox_type/"):
        os.makedirs("files_by_infobox_type/")
    for infobox_type in articles_by_infobox_type:
        filename = replace_stuff(infobox_type)
        if len(filename) > 200:
            filename = filename[0:200]
        # print(filename)
        # it looks like under some mysterious circumstances race conditions can occur when opening a file
        # so if a file does not exist we create it by hand first
        if not os.path.exists(filename):
            open(filename, "w", encoding="utf-8").close()
            print("file created")
        with open(filename, 'a+', encoding="utf-8") as outfile:
            for json_dict in articles_by_infobox_type[infobox_type]:
                outfile.write(json.dumps(json_dict))
                outfile.write("\n")


def map_infobox_to_filename(infobox_type):
    # lowercase
    # "infobox_" => "infobox "
    # remove multiple spaces
    # "template:" ??

    infobox_type = infobox_type.lower()
    mult_ws_regex = re.compile(" +")
    infobox_type = re.sub(mult_ws_regex, " ", infobox_type)
    infobox_type = infobox_type.replace("infobox_", "infobox ")
    infobox_type = infobox_type.replace("template:", "")

    return infobox_type


def read_specific_data(target_infobox_types):
    filename = "20120323-en-updates.json"

    articles_by_infobox_type = {}

    with open(filename, 'r', encoding="utf-8") as infile:
        line_count = 0
        for line in infile:
            wikipedia_json = json.loads(line)

            infobox_type = wikipedia_json["attribute"][0]["infobox_name"]
            infobox_type = map_infobox_to_filename(infobox_type)

            if infobox_type in target_infobox_types:
                if not infobox_type in articles_by_infobox_type:
                    articles_by_infobox_type[infobox_type] = []
                articles_by_infobox_type[infobox_type].append(wikipedia_json)

            line_count += 1
            if line_count % 1000 == 0:
                print(line_count)
            if line_count % 5000 == 0:
                print("Dumping...")
                dump_data_into_files(articles_by_infobox_type)
                articles_by_infobox_type = {}

    for infobox_type, articles_count in articles_by_infobox_type.items():
        print("{} - {}".format(infobox_type, len(articles_count)))

    # target_infobox_type = "Infobox Person"
    # target_articles = articles_by_infobox_type[target_infobox_type]


def read_data():
    filename = "20120323-en-updates.json"

    articles_by_infobox_type = {}

    # already_read = 655000
    already_read = 0

    with open(filename, 'r', encoding="utf-8") as infile:
        line_count = 0
        for line in infile:

            line_count += 1
            if line_count % 1000 == 0:
                print(line_count)

            if line_count <= already_read:
                continue

            wikipedia_json = json.loads(line)

            infobox_type = wikipedia_json["attribute"][0]["infobox_name"]
            infobox_type = map_infobox_to_filename(infobox_type)

            if not infobox_type in articles_by_infobox_type:
                articles_by_infobox_type[infobox_type] = []
            articles_by_infobox_type[infobox_type].append(wikipedia_json)

            line_count += 1
            if line_count % 1000 == 0:
                print(line_count)
            if line_count % 5000 == 0:
                print("Dumping...")
                dump_data_into_files(articles_by_infobox_type)
                articles_by_infobox_type = {}

    for infobox_type, articles_count in articles_by_infobox_type.items():
        print("{} - {}".format(infobox_type, len(articles_count)))


def remove_attributes_manually(attributes):
    removable_attributes = set()
    sorted_attributes = sorted(attributes, key=str.lower)
    for attribute in sorted_attributes:
        while True:
            decision = input("Keep attribute \"" + attribute + "\" ? (y/n)\n")
            if decision == "n":
                removable_attributes.add(attribute)
                break
            if decision == "y":
                break
            print("wrong input")
    for attribute in removable_attributes:
        attributes.remove(attribute)


def getAttributes(attributesInput):
    attributes = set()

    detectAttributes = len(attributesInput) == 0
    if not detectAttributes:
        attributes = getAttributesFromInput(attributesInput)

    return attributes


def normalizeAttribute(attribute):
    return attribute.lower().replace(" ", "_")

def getAttributesFromInput(attributesInput):
    attributes = set()

    for attribute in attributesInput:
        attributes.add(normalizeAttribute(attribute))

    return attributes


def addAttributesFromUpdate(attributes, update):
    attributes.add(update["key"].replace("\n", "").replace("\t", "").lower())
    return attributes


def groupUpdatesById(updates, attributes):
    updatesById = {}

    for update in updates["attribute"]:
        updateId = update["id"]

        detectAttributes = len(attributes) == 0
        if detectAttributes:
            attributes = addAttributesFromUpdate(attributes, update)

        if updateId not in updatesById:
            updatesById[updateId] = []
        updatesById[updateId].append(update)

    return updatesById, attributes


def groupDataIntoBaselineAndUpdates(targetInfoboxType, attributes):
    baselineRecords = []
    updateRecords = []

    currentId = 1

    with open("files_by_infobox_type/" + targetInfoboxType, 'r', encoding='utf-8') as infile:
        for line in infile:
            data = json.loads(line)

            updatesById, attributes = groupUpdatesById(data, attributes)

            orderedUpdateIds = sorted(updatesById.keys())
            baselineData = updatesById.pop(orderedUpdateIds[0])
            updateData = updatesById

            articleTitle = data["article_title"]
            print(articleTitle)
            baselineRecords.append(generateBaselineRecord(baselineData, attributes, currentId, articleTitle))

            for entry in updateData.values():
                updateRecord = generateUpdateRecord(entry, attributes, currentId, articleTitle)
                if updateRecord is not None:
                    updateRecords.append(updateRecord)

            currentId += 1

    return baselineRecords, updateRecords


def initBaselineRecord(article, baselineRecordBlueprint, currentId):
    baselineDataEntry = copy.deepcopy(baselineRecordBlueprint)
    baselineDataEntry["id"] = str(currentId)
    baselineDataEntry["article_title"] = article.replace("\"", "\"\"")
    return baselineDataEntry


def addValuesToBaselineRecord(update, attributes, baselineDataEntry):
    key = normalizeAttribute(update["key"])
    if "newvalue" in update and key in attributes:
        baselineDataEntry[key] = update["newvalue"].replace("|", "").replace("\n", "").replace("\"", "\"\"")


def initUpdateRecord(article, updateRecordBlueprint, currentId):
    updateStatement = copy.deepcopy(updateRecordBlueprint)
    updateStatement["::record"] = currentId
    updateStatement["article_title"] = article.replace("\"", "\"\"")
    return updateStatement


def findNewAndOldValues(update, newValues, oldValues, attributes):
    key = normalizeAttribute(update["key"])
    if "newvalue" in update and key in attributes and key not in newValues:
        newValues[key.lower()] = update["newvalue"].replace("\"", "\"\"")
    if "oldvalue" in update and key in attributes and key not in oldValues:
        oldValues[key.lower()] = update["oldvalue"].replace("\"", "\"\"")

    return newValues, oldValues


def addValuesToUpdateStatement(updateStatement, attributes, newValues, oldValues):
    for attribute in attributes:
        if attribute == "article_title":
            continue
        oldValue = ""
        newValue = ""
        compundValue = ""
        if attribute in oldValues:
            oldValue = oldValues[attribute].replace("|", "").replace("\n", "")
        if attribute in newValues:
            newValue = newValues[attribute].replace("|", "").replace("\n", "")
        if oldValue != "" or newValue != "":
            compundValue = oldValue + "|" + newValue
        updateStatement[attribute] = compundValue


def getBaselineRecordBlueprint(attributes):
    baselineRecordBlueprint = collections.OrderedDict()
    baselineRecordBlueprint["id"] = ""
    baselineRecordBlueprint["article_title"] = ""
    for attribute in attributes:
        baselineRecordBlueprint[attribute] = ""

    return baselineRecordBlueprint


def generateBaselineRecord(data, attributes, currentId, articleTitle):
    baselineRecordBlueprint = getBaselineRecordBlueprint(attributes)

    baselineRecord = initBaselineRecord(articleTitle, baselineRecordBlueprint, currentId)

    for entry in data:
        addValuesToBaselineRecord(entry, attributes, baselineRecord)

    # for attribute in baselineRecord:
    #     if baselineRecord[attribute] == None:
    #         baselineRecord[attribute] = ""

    return baselineRecord


def getUpdateRecordBlueprint(attributes):
    updateRecordBlueprint = collections.OrderedDict()
    updateRecordBlueprint["::record"] = ""
    updateRecordBlueprint["article_title"] = ""
    updateRecordBlueprint["::action"] = ""
    for attribute in attributes:
        updateRecordBlueprint[attribute] = ""

    return updateRecordBlueprint


def generateUpdateRecord(data, attributes, currentId, articleTitle):
    updateRecordBlueprint = getUpdateRecordBlueprint(attributes)
    updateRecord = initUpdateRecord(articleTitle, updateRecordBlueprint, currentId)

    newValues = {}
    oldValues = {}
    for entry in data:
        newValues, oldValues = findNewAndOldValues(entry, newValues, oldValues, attributes)

    if len(newValues) == 0:
        updateRecord["::action"] = "delete"
    else:
        updateRecord["::action"] = "update"

    addValuesToUpdateStatement(updateRecord, attributes, newValues, oldValues)

    isValidUpdateStatement = checkValidityOfUpdateStatement(updateRecord, attributes)

    if isValidUpdateStatement:
        return updateRecord
    return None

    for entry in data:
        addValuesTo(entry, attributes, updateRecord)

    return updateRecord

def checkValidityOfUpdateStatement(updateStatement, attributes):
    isValid = False
    for key, value in updateStatement.items():
        if key in ["::action", "::record", "article_title"]:
            continue
        if value != "":
            isValid = True
    return isValid


def generateUpdateStatementFromData(dataByTitle, updateId, article, updateStatementsEntryBlueprint, currentId, attributes):
    updateStatement = initUpdateRecord(article, updateStatementsEntryBlueprint, currentId)

    newValues = {}
    oldValues = {}
    for update in dataByTitle[article]["updates"][updateId]:
        newValues, oldValues = findNewAndOldValues(update, newValues, oldValues, attributes)

    if len(newValues) == 0:
        updateStatement["::action"] = "delete"
    else:
        updateStatement["::action"] = "update"

    addValuesToUpdateStatement(updateStatement, attributes, newValues, oldValues)

    isValidUpdateStatement = checkValidityOfUpdateStatement(updateStatement, attributes)

    if isValidUpdateStatement:
        return updateStatement
    return None


def transformUpdatesIntoStatements(dataByTitle, baselineDataEntryBlueprint, updateStatementsEntryBlueprint, attributes):
    currentId = 1

    baselineData = []
    updateStatements = []

    for article in dataByTitle:
        print(article)

        baselineDataEntry = generateBaselineRecord(dataByTitle, article, baselineDataEntryBlueprint, currentId,
                                                   attributes)
        baselineData.append(baselineDataEntry)

        for updateId in dataByTitle[article]["updates"]:
            updateStatement = generateUpdateStatementFromData(dataByTitle, updateId, article,
                                                              updateStatementsEntryBlueprint, currentId, attributes)
            if updateStatement != None:
                updateStatements.append(updateStatement)
        currentId += 1

    return baselineData, updateStatements


def createTargetDirectoriesIfNecessary():
    if not os.path.exists("data/baseline"):
        os.makedirs("data/baseline/")
    if not os.path.exists("data/updates/"):
        os.makedirs("data/updates/")
    if not os.path.exists("data/inserts/"):
        os.makedirs("data/inserts/")


def splitBaselineDataInHalf(baselineData):
    baselineInserts = baselineData[int(len(baselineData) / 2):len(baselineData)]
    baselineData = baselineData[0:int(len(baselineData) / 2)]

    return baselineData, baselineInserts


def transformBaselineInsertsIntoUpdates(baselineInserts, attributes):
    insertStatements = []
    keys = list(attributes)
    for baselineInsert in baselineInserts:
        insertStatement = copy.deepcopy(getUpdateRecordBlueprint(attributes))
        insertStatement["::record"] = baselineInsert["id"]
        insertStatement["article_title"] = baselineInsert["article_title"]
        insertStatement["::action"] = "insert"
        for i in range(2, len(keys)):
            insertStatement[keys[i]] = baselineInsert[keys[i]]
        insertStatements.append(insertStatement)

    return insertStatements


def mergeInsertAndUpdateStatements(insertStatements, updateStatements):
    combinedUpdateStatements = []
    lastUpdateIndex = 0
    insertedStatementsCount = 0
    for insert in insertStatements:
        lastUpdateIndex = insertInsertStatementIntoUpdateStatements(combinedUpdateStatements, insert, updateStatements,
                                                                    lastUpdateIndex, insertedStatementsCount)
        insertedStatementsCount += 1

    return combinedUpdateStatements


def insertInsertStatementIntoUpdateStatements(combinedUpdateStatements, insert, updateStatements, lastUpdateIndex,
                                              insertedStatementsCount):
    for currentUpdateIndex in range(lastUpdateIndex, len(updateStatements)):
        if int(insert["::record"]) <= int(updateStatements[currentUpdateIndex]["::record"]):
            combinedUpdateStatements.insert(currentUpdateIndex + insertedStatementsCount, insert)
            return currentUpdateIndex
        else:
            combinedUpdateStatements.append(updateStatements[currentUpdateIndex])


def buildTableFromBaselineData(baselineData):
    table = {}
    for record in baselineData:
        table[record["id"]] = record
    return table


def applyInsertStatement(updateStatement, baselineDataTable, attributes):
    newEntry = copy.deepcopy(getBaselineRecordBlueprint(attributes))
    targetId = str(updateStatement["::record"])

    for attribute in updateStatement:
        if attribute in newEntry:
            newEntry[attribute] = updateStatement[attribute]
    baselineDataTable[targetId] = newEntry
    return baselineDataTable


def applyDeleteStatement(updateStatement, baselineDataTable):
    targetId = str(updateStatement["::record"])
    if targetId in baselineDataTable:
        targetEntry = baselineDataTable[targetId]

        for attribute in updateStatement:
            if attribute in targetEntry and updateStatement[attribute] != "" and attribute != "article_title":
                # just to ensure data consistency
                updateStatement[attribute] = targetEntry[attribute]
                targetEntry[attribute] = ""
    else:
        updateStatement["toBeDeleted"] = True
    return baselineDataTable



def checkForTrueDeleteAndCorrect(updateStatement, baselineDataTable):
    targetId = str(updateStatement["::record"])

    targetEntry = baselineDataTable[targetId]

    hasNonEmptyFields = False
    for attribute in targetEntry:
        if attribute in ["id", "article_title"]:
            continue
        if targetEntry[attribute] != "":
            hasNonEmptyFields = True
    if hasNonEmptyFields:
        updateStatement["::action"] = "update"
        # add pipes if it's an update statement
        for attribute in updateStatement:
            if attribute in ["article_title", "::record", "::action"]:
                continue
            elif updateStatement[attribute] != "":
                updateStatement[attribute] += "|"

    else:
        print("Deleting " + baselineDataTable[targetId]["article_title"])
        del baselineDataTable[targetId]
        # remove pipes from delete statement
        for key in updateStatement:
            updateStatement[key] = str(updateStatement[key]).replace("|", "")


    return baselineDataTable


def applyUpdateStatement(updateStatement, baselineDataTable, attributes):
    targetId = str(updateStatement["::record"])

    if targetId in baselineDataTable:
        targetEntry = baselineDataTable[targetId]

        for attribute in updateStatement:
            if attribute in targetEntry and updateStatement[attribute] != "" and attribute != "article_title":
                newValue = updateStatement[attribute].split("|")[1]
                newOldValue = targetEntry[attribute]
                targetEntry[attribute] = newValue
                if newValue != "" or newOldValue != "":
                    updateStatement[attribute] = newOldValue + "|" + newValue

    else:
        updateStatement["::action"] = "insert"
        for attribute, value in updateStatement.items():
            if attribute in ("article_title", "::action", "::record") or value == "":
                continue
            updateStatement[attribute] = value.split("|")[1]
        baselineDataTable = applyInsertStatement(updateStatement, baselineDataTable, attributes)

    return baselineDataTable


def applyUpdateStatementsToBaselineData(updateStatements, baselineDataTable, attributes):
    for updateStatement in updateStatements:
        statementType = updateStatement["::action"]
        if updateStatement["article_title"] == "Haemophilia B":
            print("break")
        if statementType == "insert":
            baselineDataTable = applyInsertStatement(updateStatement, baselineDataTable, attributes)
        elif statementType == "delete":
            baselineDataTable = applyDeleteStatement(updateStatement, baselineDataTable)
            if "toBeDeleted" not in updateStatement:
                baselineDataTable = checkForTrueDeleteAndCorrect(updateStatement, baselineDataTable)
        elif statementType == "update":
            baselineDataTable = applyUpdateStatement(updateStatement, baselineDataTable, attributes)

    updateStatements = [x for x in updateStatements if "toBeDeleted" not in x]
    return updateStatements


def determineFinalUpdateStatementType(updateStatements, baselineData, attributes):
    baselineDataTable = buildTableFromBaselineData(copy.deepcopy(baselineData))
    updateStatements = applyUpdateStatementsToBaselineData(updateStatements, baselineDataTable, attributes)
    return updateStatements


def parseInfoboxUpdatesToCsv(infoboxConfig):
    for targetInfoboxType, attributesInput in infoboxConfig.items():
        attributes = getAttributes(attributesInput)

        print("Now parsing " + targetInfoboxType + "...")

        baselineRecords, updateRecords = groupDataIntoBaselineAndUpdates(targetInfoboxType, attributes)

        print("Grouping data...")

        baselineRecords, baselineInserts = splitBaselineDataInHalf(baselineRecords)
        insertRecords = transformBaselineInsertsIntoUpdates(baselineInserts, attributes)

        updateRecords = mergeInsertAndUpdateStatements(insertRecords, updateRecords)
        updateRecords = determineFinalUpdateStatementType(updateRecords, baselineRecords, attributes)

        writeParsedDataToDisk(targetInfoboxType, baselineRecords, insertRecords, updateRecords, attributes)


def filterUpdatesBySelection(updateStatements, statementTypesToBeParsed):
    if len(statementTypesToBeParsed) == 3:
        return updateStatements
    if len(statementTypesToBeParsed) == 0:
        return []

    filteredUpdateStatements = []
    for updateStatement in updateStatements:
        if updateStatement["::action"] in statementTypesToBeParsed:
            filteredUpdateStatements.append(updateStatement)

    return filteredUpdateStatements


def writeParsedDataToDisk(targetInfoboxType, baselineData, insertRecords, updateStatements, attributes):
    createTargetDirectoriesIfNecessary()

    attributes = list(attributes)
    if ("article_title") in attributes:
        attributes.remove("article_title")
    attributes.insert(0, "article_title")

    print("Writing baseline csv...")
    writeBaselineData(attributes, baselineData, targetInfoboxType)

    print("Writing inserts-only csv...")
    writeInsertOnlyRecords(attributes, insertRecords, targetInfoboxType)

    print("Writing updates csv...")
    writeUpdateStatements(attributes, targetInfoboxType, updateStatements)


def writeUpdateStatements(attributes, targetInfoboxType, updateStatements):
    updateFilename = str("data/updates/" + targetInfoboxType + "_update_statements.csv").replace(" ", "_")
    updateAttributes = arrangeUpdateAttributes(attributes)
    updateRecordsString = transformRecordsToUniqueStringRepresentation(updateAttributes, updateStatements)
    writeAsCsv(updateFilename, updateAttributes, updateRecordsString)


def writeInsertOnlyRecords(attributes, insertRecords, targetInfoboxType):
    insertFilename = str("data/inserts/" + targetInfoboxType + "_insert_statements.csv").replace(" ", "_")
    insertAttributes = arrangeUpdateAttributes(attributes)
    insertRecordsStrings = transformRecordsToUniqueStringRepresentation(insertAttributes, insertRecords)
    writeAsCsv(insertFilename, insertAttributes, insertRecordsStrings)


def writeBaselineData(attributes, baselineData, targetInfoboxType):
    baselineFilename = str("data/baseline/" + targetInfoboxType + "_baseline_data.csv").replace(" ", "_")
    baselineRecordsStrings = transformRecordsToUniqueStringRepresentation(attributes, baselineData)
    writeAsCsv(baselineFilename, attributes, baselineRecordsStrings)


def transformRecordsToUniqueStringRepresentation(attributes, records):
    recordStrings = []
    for record in records:
        recordStrings.append(statementToString(attributes, record))
    return recordStrings

def arrangeUpdateAttributes(attributes):
    updateAttributes = copy.deepcopy(attributes)
    updateAttributes.insert(0, "::action")

    return updateAttributes


def writeAsCsv(filename, attributes, records):
    with open(filename, "w") as outfile:
        header = ""
        count = 0
        for key in attributes:
            if count != 0:
                header += ","
            header += "\"" + key + "\""
            count += 1
        outfile.write(header + "\n")

        for record in records:
            outfile.write(record + "\n")


def statementToString(attributes, entry):
    outputString = ""
    count = 0
    for key in attributes:
        if count != 0:
            outputString += ","
        if entry[key] is not None:
            outputString += "\"" + str(entry[key]) + "\""
        else:
            outputString += "\"\""
        count += 1
    return outputString


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

if __name__ == "__main__":
    # attributes should be provided via infobox_config.json
    infoboxConfig = readInfoboxConfig()

    parseInfoboxUpdatesToCsv(infoboxConfig)
