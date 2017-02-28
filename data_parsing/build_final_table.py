import argparse
import copy
import csv

import collections
import os


def initArgparse():
    parser = argparse.ArgumentParser(description="Build the table that would result from applying the update "
                                                 "statements to the baseline data")
    parser.add_argument("--mode", action="append", help="Which statements to be applied (choose from insert, update "
                                                        "and delete")
    parser.add_argument("--infobox", action="store", help="The name of the infobox for which to build the table, "
                                                          "e.g. \"actor\"")
    return parser


def readBaselineData(infobox):
    directory = "data/baseline/"
    filename = "infobox_" + infobox + "_baseline_data.csv"
    return readCsv(directory, filename)


def readUpdateStatements(infobox):
    directory = "data/updates/"
    filename = "infobox_" + infobox + "_update_statements.csv"
    return readCsv(directory, filename)


def readCsv(dir, filename):
    csvReader = csv.DictReader(open(dir + filename, "r", encoding="utf-8"))
    records = []
    for record in csvReader:
        records.append(record)
    return records


def indexBaselineData(records):
    baselineData = collections.OrderedDict()
    for record in records:
        baselineData[record["article_title"]] = record
    return baselineData


def applyInsertStatement(baselineData, updateStatement):
    newRecord = copy.deepcopy(next(iter(baselineData.values())))
    for key, value in updateStatement.items():
        if key == "::action":
            continue
        newRecord[key] = value
    baselineData[newRecord["article_title"]] = newRecord


def applyDeleteStatement(baselineData, updateStatement):
    key = updateStatement["article_title"]
    if key in baselineData:
        # check for mismatches
        targetEntry = baselineData[updateStatement["article_title"]]
        for key, value in updateStatement.items():
            if key in targetEntry and value != targetEntry[key]:
                print("delete::mismatch")
        del baselineData[updateStatement["article_title"]]
    else:
        print("ERROR: already deleted: " + key)


def applyUpdateStatement(baselineData, updateStatement):
    for key, value in updateStatement.items():
        # check for mismatches
        targetEntry = baselineData[updateStatement["article_title"]]
        if key in ("article_title", "::action", "::record"):
            continue
        if value != "":
            if value != targetEntry[key]:
                print("update::mismatch")
            targetEntry[key] = value.split("|")[1]


def applyUpdateStatementsToBaselineData(baselineData, updateStatements, mode):
    for updateStatement in updateStatements:
        action = updateStatement["::action"]
        if action in mode:
            if action == "insert":
                applyInsertStatement(baselineData, updateStatement)
            elif action == "delete":
                applyDeleteStatement(baselineData, updateStatement)
            elif action == "update":
                applyUpdateStatement(baselineData, updateStatement)
    return baselineData


def storeFinalTable(updatedRecords, infobox):
    dir = "data/final/"
    if not os.path.exists(dir):
        os.makedirs(dir)

    fields = next(iter(updatedRecords.values())).keys()
    print(fields)

    filename = "infobox_" + infobox + "_final_table.csv"
    writeAsCsv(dir + filename, fields, updatedRecords)


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

        for articleTitle, record in records.items():
            outputList = []
            for attribute in attributes:
                outputList.append("\"" + record[attribute] + "\"")
            outputString = ",".join(outputList) + "\n"
            outfile.write(outputString)


if __name__ == "__main__":
    parser = initArgparse()
    arguments = parser.parse_args()
    print(arguments.mode)

    baselineData = indexBaselineData(readBaselineData(arguments.infobox))
    updateStatements = readUpdateStatements(arguments.infobox)
    
    updatedRecords = applyUpdateStatementsToBaselineData(baselineData, updateStatements, arguments.mode)
    storeFinalTable(updatedRecords, arguments.infobox)