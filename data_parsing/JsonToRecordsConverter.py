import json

from models.Record import Record
from models.Statement import Statement


def convert(targetInfoboxType, attributes):
    baselineRecords = []
    updateStatements = []

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
                updateStatements.append(generateUpdateStatement(entry, attributes, currentId, articleTitle))

            currentId += 1

    return baselineRecords, updateStatements

def groupUpdatesById(updates, attributes):
    updatesById = {}

    for update in updates["attribute"]:
        updateId = update["id"]

        if updateId not in updatesById:
            updatesById[updateId] = []
        updatesById[updateId].append(update)

    return updatesById, attributes

def generateBaselineRecord(data, attributes, currentId, articleTitle):
    baselineRecord = Record(attributes)
    baselineRecord.id = currentId
    baselineRecord.valueMap["article_title"] = articleTitle
    baselineRecord.addValuesFromJsonData(data)

    return baselineRecord

def generateUpdateStatement(data, attributes, currentId, articleTitle):
    updateStatement = Statement(attributes)
    updateStatement.record = currentId
    updateStatement.valueMap["article_title"] = articleTitle
    updateStatement.oldValueMap["article_title"] = articleTitle
    updateStatement.addValuesFromJsonData(data)

    return updateStatement