import json

import Utils
from models.Record import Record
from models.Statement import Statement


def convert(targetInfoboxType, attributes):
    baselineRecords = []
    updateStatements = []

    currentId = 1

    with open("files_by_infobox_type/" + targetInfoboxType, 'r', encoding='utf-8') as infile:
        for line in infile:
            try:
                data = json.loads(line.replace("\n", "").replace("\r", ""), strict = False)

                updatesById, attributes = groupUpdatesById(data, attributes)

                orderedUpdateIds = sorted(updatesById.keys())
                baselineData = updatesById.pop(orderedUpdateIds[0])
                updateData = updatesById

                articleTitle = data["article_title"]
                print(articleTitle)
                baselineRecords.append(generateBaselineRecord(baselineData, attributes, currentId, articleTitle))

                for entry in updateData.values():
                    updateStatement = generateUpdateStatement(entry, attributes, currentId, articleTitle)
                    if not updateStatement.isIrrelevant():
                        updateStatements.append(updateStatement)

                currentId += 1
            except:
                currentId += 1
                continue

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
    baselineRecord.valueMap["article_title"] = Utils.cleanseValue(articleTitle)
    baselineRecord.addValuesFromJsonData(data)

    return baselineRecord

def generateUpdateStatement(data, attributes, currentId, articleTitle):
    updateStatement = Statement(attributes)
    updateStatement.record = currentId
    updateStatement.valueMap["article_title"] = Utils.cleanseValue(articleTitle)
    updateStatement.oldValueMap["article_title"] = Utils.cleanseValue(articleTitle)
    updateStatement.addValuesFromJsonData(data)

    return updateStatement