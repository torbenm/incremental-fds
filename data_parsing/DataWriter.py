import os


def writeParsedDataToDisk(targetInfoboxType, baselineRecords, insertStatements, updateStatements, attributes):
    createTargetDirectoriesIfNecessary()

    if "article_title" in attributes:
        attributes.remove("article_title")
    attributes.insert(0, "article_title")

    print("Writing baseline csv...")
    writeBaselineData(attributes, baselineRecords, targetInfoboxType)

    print("Writing inserts-only csv...")
    writeInsertOnlyRecords(attributes, insertStatements, targetInfoboxType)

    print("Writing updates csv...")
    writeUpdateStatements(attributes, targetInfoboxType, updateStatements)


def createTargetDirectoriesIfNecessary():
    if not os.path.exists("data/baseline"):
        os.makedirs("data/baseline/")
    if not os.path.exists("data/updates/"):
        os.makedirs("data/updates/")
    if not os.path.exists("data/inserts/"):
        os.makedirs("data/inserts/")


def writeUpdateStatements(attributes, targetInfoboxType, updateStatements):
    updateFilename = str("data/updates/" + targetInfoboxType + "_update_statements.csv").replace(" ", "_")
    attributes.insert(0, "::action")
    writeAsCsv(updateFilename, attributes, updateStatements)


def writeInsertOnlyRecords(attributes, insertStatements, targetInfoboxType):
    insertFilename = str("data/inserts/" + targetInfoboxType + "_insert_statements.csv").replace(" ", "_")
    attributes.insert(0, "::action")
    writeAsCsv(insertFilename, attributes, insertStatements)


def writeBaselineData(attributes, baselineRecords, targetInfoboxType):
    baselineFilename = str("data/baseline/" + targetInfoboxType + "_baseline_data.csv").replace(" ", "_")
    writeAsCsv(baselineFilename, attributes, baselineRecords)

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
            outfile.write(record.toString() + "\n")