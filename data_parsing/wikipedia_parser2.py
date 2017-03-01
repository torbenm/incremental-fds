import ConfigReader
import DataWriter
import JsonToRecordsConverter
import Utils
from models.Statement import Statement
from models.Table import Table


def parseInfoboxUpdatesToCsv(infoboxConfig):
    for targetInfoboxType, attributesInput in infoboxConfig.items():
        attributes = sorted(list(set(attributesInput)))
        for i in range(0, len(attributes)):
            attributes[i] = Utils.normalizeAttribute(attributes[i])
        if "article_title" in attributes:
            attributes.remove("article_title")
        attributes.insert(0, "article_title")

        print("Now parsing " + targetInfoboxType + "...")

        baselineRecords, updateStatements = JsonToRecordsConverter.convert(targetInfoboxType, attributes)

        print("Grouping data...")

        baselineRecords, baselineInserts = splitBaselineDataInHalf(baselineRecords)
        insertStatements = transformBaselineInsertsIntoUpdates(baselineInserts, attributes)

        updateStatements = mergeInsertAndUpdateStatements(insertStatements, updateStatements)
        determineFinalUpdateStatementType(updateStatements, baselineRecords, attributes)

        DataWriter.writeParsedDataToDisk(targetInfoboxType, baselineRecords, insertStatements, updateStatements, attributes)


def splitBaselineDataInHalf(baselineData):
    baselineInserts = baselineData[int(len(baselineData) / 2):len(baselineData)]
    baselineData = baselineData[0:int(len(baselineData) / 2)]

    return baselineData, baselineInserts


def transformBaselineInsertsIntoUpdates(baselineInserts, attributes):
    insertStatements = []

    for baselineInsert in baselineInserts:
        insertStatement = Statement(attributes)
        insertStatement.action = "insert"
        insertStatement.record = baselineInsert.id

        for attribute, value in baselineInsert.valueMap.items():
            insertStatement.valueMap[attribute] = value

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
        if int(insert.record) <= int(updateStatements[currentUpdateIndex].record):
            combinedUpdateStatements.insert(currentUpdateIndex + insertedStatementsCount, insert)
            return currentUpdateIndex
        else:
            combinedUpdateStatements.append(updateStatements[currentUpdateIndex])

def determineFinalUpdateStatementType(updateStatements, baselineData, attributes):
    baselineDataTable = Table(baselineData)
    applyUpdateStatementsToBaselineData(updateStatements, baselineDataTable, attributes)

def applyUpdateStatementsToBaselineData(updateStatements, baselineDataTable, attributes):
    for updateStatement in updateStatements:
        if updateStatement.action == "insert":
            baselineDataTable.insertRecord(updateStatement)
        elif updateStatement.action == "update":
            baselineDataTable.updateRecord(updateStatement)

    updateStatements = [x for x in updateStatements if x.removable]
    return updateStatements


if __name__ == "__main__":
    infoboxConfig = ConfigReader.readInfoboxConfig()
    parseInfoboxUpdatesToCsv(infoboxConfig)