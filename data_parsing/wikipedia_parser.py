# -*- coding: utf-8 -*-

import json
import copy
import os
import re
import collections

def calculate_statistics():
	# filename = "wikipedia_infobox_dataset_head_100.json"
	filename = "20120323-en-updates.json"

	articles_count_by_infobox_type = {}

	with open(filename, 'r', encoding = "utf-8") as infile:
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
			# if line_count == 10000:
			# 	break

	return articles_count_by_infobox_type

def write_statistics(articles_count_by_infobox_type):
	sorted_article_types = sorted(articles_count_by_infobox_type.keys(), key = lambda x: articles_count_by_infobox_type[x], reverse = True)

	with open('wikipedia_updates_statistics.txt', 'w', encoding = "utf-8") as outfile:
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
	filename = filename.replace("/", "").replace("Ö", "Oe").replace("Ä", "Ae").replace("Ü", "Ue").replace("ö", "oe").replace("ä", "ae").replace("ü", "ue").replace(":", "").replace("\\", "")
	filename = filename.replace("<", "").replace(">", "").replace("|", "").replace("?", "").replace("*", "").replace("\n", "").replace(".", "").replace("'", "").replace(",", "").replace("\t", "")
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
			open(filename, "w", encoding = "utf-8").close()
			print("file created")
		with open(filename, 'a+', encoding = "utf-8") as outfile:
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

	with open(filename, 'r', encoding = "utf-8") as infile:
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

	with open(filename, 'r', encoding = "utf-8") as infile:
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
	sorted_attributes = sorted(attributes, key = str.lower)
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
	detectAttributes = len(attributesInput) == 0

	attributes = set()

	if not detectAttributes:
		 getAttributesFromInput(attributesInput)

	return attributes

def getAttributesFromInput(attributesInput):
	attributes = set()

	for attribute in attributesInput:
		attributes.add(attribute.lower())

	return attributes

def groupDataIntoBaselineAndUpdates(target_infobox_type, attributes, detectAttributes):
	dataByTitle = {}

	with open("files_by_infobox_type/" + target_infobox_type, 'r', encoding = 'utf-8') as infile:
		for line in infile:
			data = json.loads(line)

			article_title = data["article_title"]

			if article_title not in dataByTitle:
				dataByTitle[article_title] = {}
			
			updates_by_id = {}

			for update in data["attribute"]:
				update_id = update["id"]

				if detectAttributes:
					attributes.add(update["key"].replace("\n", "").replace("\t", "").lower())

				if update_id not in updates_by_id:
					updates_by_id[update_id] = []
				updates_by_id[update_id].append(update)

			ordered_update_ids = sorted(updates_by_id.keys())

			dataByTitle[article_title]["baseline"] = updates_by_id.pop(ordered_update_ids[0])
			dataByTitle[article_title]["updates"] = updates_by_id

	return dataByTitle, attributes

def createBaselineAndUpdateDummies(attributes):
	baselineDataEntryBlueprint = collections.OrderedDict()
	baselineDataEntryBlueprint["id"] = None
	baselineDataEntryBlueprint["article_title"] = None
	for attribute in attributes:
		baselineDataEntryBlueprint[attribute] = None
	
	updateStatementsEntryBlueprint = collections.OrderedDict()
	updateStatementsEntryBlueprint["refers_to"] = None
	updateStatementsEntryBlueprint["article_title"] = None
	updateStatementsEntryBlueprint["statement_type"] = None
	for attribute in attributes:
		updateStatementsEntryBlueprint[attribute] = None

	return baselineDataEntryBlueprint, updateStatementsEntryBlueprint

def transformDataIntoRecords(data_by_title, baselineDataEntryBlueprint, updateStatementsEntryBlueprint, attributes):
	## THIS SHOULD WORK IN DEPENDENCE OF THE ATTRIBUTES
	## If we find 'key' and 'Key' and the we will get one match to much
	current_id = 1

	baselineData = []
	updateStatements = []

	for article in data_by_title:
		print(article)
		baselineData_entry = copy.deepcopy(baselineDataEntryBlueprint)
		baselineData_entry["id"] = current_id
		baselineData_entry["article_title"] = article.replace("\"", "\"\"")
		
		for update in data_by_title[article]["baseline"]:
			key = update["key"].lower()
			if "newvalue" in update and key in attributes and key not in baselineData:
				baselineData_entry[key] = update["newvalue"].replace("|", "").replace("\n", "").replace("\"", "\"\"")
			baselineData_entry["id"] = str(current_id)
		if len(baselineData_entry) != len(attributes) + 2:
			print("FEHLER")
		baselineData.append(baselineData_entry)

		for update_id in data_by_title[article]["updates"]:
			new_values = {}
			old_values = {}

			for update_by_id in data_by_title[article]["updates"][update_id]:
				key = update_by_id["key"].lower()
				if "newvalue" in update_by_id and key in attributes and key not in new_values:
					new_values[key.lower()] = update_by_id["newvalue"].replace("\"", "\"\"")
				if "oldvalue" in update_by_id and key in attributes and key not in old_values:
					old_values[key.lower()] = update_by_id["oldvalue"].replace("\"", "\"\"")

			update_statement = copy.deepcopy(updateStatementsEntryBlueprint)
			update_statement["refers_to"] = current_id
			update_statement["article_title"] = article.replace("\"", "\"\"")

			if len(new_values) == 0:
				update_statement["statement_type"] = "delete"
			else:
				update_statement["statement_type"] = "update"

			for attribute in attributes:
				if attribute in new_values and attribute in old_values:
					update_statement[attribute] = old_values[attribute].replace("|", "").replace("\n", "") + "|" + new_values[attribute].replace("|", "").replace("\n", "")
				elif attribute in new_values:
					update_statement[attribute] = new_values[attribute].replace("|", "").replace("\n", "")
				elif attribute in old_values:
					update_statement[attribute] = old_values[attribute].replace("|", "").replace("\n", "")
				else:
					update_statement[attribute] = ""
			if len(update_statement) != len(attributes) + 3:
				print("FEHLER")
			updateStatements.append(update_statement)
		current_id += 1

	return baselineData, updateStatements

def createTargetDirectoriesIfNecessary():
	if not os.path.exists("data/baseline"):
		os.makedirs("data/baseline/")
	if not os.path.exists("data/updates/"):
		os.makedirs("data/updates/")
	if not os.path.exists("data/inserts/"):
		os.makedirs("data/inserts/")

def splitBaselineDataInHalf(baselineData):
	baselineInserts = baselineData[int(len(baselineData)/2):len(baselineData)]
	baselineData = baselineData[0:int(len(baselineData)/2)]

	return baselineData, baselineInserts

def transformBaselineInsertsIntoUpdates(baselineInserts, attributes, updateStatementsEntryBlueprint):
	insertStatements = []
	keys = list(attributes)
	for baselineInsert in baselineInserts:
		insertStatement = copy.deepcopy(updateStatementsEntryBlueprint)
		insertStatement["refers_to"] = baselineInsert["id"]
		insertStatement["article_title"] = baselineInsert["article_title"]
		insertStatement["statement_type"] = "insert"
		for i in range(2, len(keys)):
			insertStatement[keys[i]] = baselineInsert[keys[i]]
		insertStatements.append(insertStatement)

	return insertStatements

def mergeInsertAndUpdateStatements(insertStatements, updateStatements):
	combinedUpdateStatements = []
	lastUpdateIndex = 0
	insertedStatementsCount = 0
	for insert in insertStatements:
		lastUpdateIndex = insertInsertStatementIntoUpdateStatements(combinedUpdateStatements, insert, updateStatements, lastUpdateIndex, insertedStatementsCount)
		insertedStatementsCount += 1

	return combinedUpdateStatements

def insertInsertStatementIntoUpdateStatements(combinedUpdateStatements, insert, updateStatements, lastUpdateIndex, insertedStatementsCount):
	for currentUpdateIndex in range(lastUpdateIndex, len(updateStatements)):
		if int(insert["refers_to"]) <= int(updateStatements[currentUpdateIndex]["refers_to"]):
			combinedUpdateStatements.insert(currentUpdateIndex + insertedStatementsCount, insert)
			return currentUpdateIndex
		else:
			combinedUpdateStatements.append(updateStatements[currentUpdateIndex])

def parseInfoboxUpdatesToCsv(infobox_config, statementTypesToBeParsed):
	for targetInfoboxType, attributesInput in infobox_config.items():
		attributes = getAttributes(attributesInput)
		detectAttributes = len(attributes) != 0
		
		print("Now parsing " + targetInfoboxType + "...")

		dataByTitle, attributes = groupDataIntoBaselineAndUpdates(targetInfoboxType, attributes, detectAttributes)

		baselineDataEntryBlueprint, updateStatementsEntryBlueprint = createBaselineAndUpdateDummies(attributes)
		baselineData, updateStatements = transformDataIntoRecords(dataByTitle, baselineDataEntryBlueprint, updateStatementsEntryBlueprint, attributes)		

		print("Grouping data...")

		baselineData, baselineInserts = splitBaselineDataInHalf(baselineData)
		insertStatements = transformBaselineInsertsIntoUpdates(baselineInserts, attributes, updateStatementsEntryBlueprint)
		updateStatements = mergeInsertAndUpdateStatements(insertStatements, updateStatements)
		
		updateStatements = filterUpdatesBySelection(updateStatements, statementTypesToBeParsed)

		createTargetDirectoriesIfNecessary()
		writeBaselineDataAndUpdateStatementsToDisk(targetInfoboxType, baselineData, updateStatements, attributes)

def filterUpdatesBySelection(updateStatements, statementTypesToBeParsed):
	if len(statementTypesToBeParsed) == 3:
		return updateStatements
	if len(statementTypesToBeParsed) == 0:
		return []

	filteredUpdateStatements = []	
	for updateStatement in updateStatements:
		if updateStatement["statement_type"] in statementTypesToBeParsed:
			filteredUpdateStatements.append(updateStatement)

	return filteredUpdateStatements

def writeBaselineDataAndUpdateStatementsToDisk(targetInfoboxType, baselineData, updateStatements, attributes):
	print("Writing baseline csv...")
	baselineFilename = str("data/baseline/" + targetInfoboxType + "_baselineData.csv").replace(" ", "_")
	baselineAttributes = arrangeBaselineAttributes(attributes)
	writeAsCsv(baselineFilename, baselineFilename, baselineData)

	print("Writing updates csv...")
	updateFilename = str("data/updates/" + targetInfoboxType + "_updateStatements.csv").replace(" ", "_")
	updateAttributes = arrangeUpdateAttributes(attributes)
	writeAsCsv(updateFilename, updateAttributes, updateStatements)

def arrangeBaselineAttributes(attributes):
	baselineAttributes = list(attributes)
	baselineAttributes.insert(0, "article_title")
	baselineAttributes.insert(0, "id")

	return baselineAttributes

def arrangeUpdateAttributes(attributes):
	updateAttributes = list(attributes)
	updateAttributes.insert(0, "article_title")
	updateAttributes.insert(0, "::record")
	updateAttributes.insert(0, "::action")

	return updateAttributes

def writeAsCsv(filename, attributes, statements):
	with open(filename, "w") as outfile:
		header = ""
		count = 0
		for key in attributes:
			if count != 0:
				header += ","
			header += "\"" + key + "\""
			count += 1
		outfile.write(header + "\n")

		for entry in statements:
			outputString = ""
			count = 0
			for value in entry.values():
				if count != 0:
					outputString += ","
				if value != None:
					outputString += "\"" + str(value) + "\""
				else:
					outputString += "\"\""
				count += 1
			outfile.write(outputString + "\n")

if __name__ == "__main__":
	# infobox config is a mapping of infobox name (i.e. name of the file to be parsed) to the attributes in this infobox
	# these attributes have to be manually copied from the corresponding wikipedia page (wikipedia.org/wiki/Template:Infobox_Name)
	# you can also leave the list empty, then attributes will be detected automatically. 
	# THIS WILL SRSLY IMPEDE RUNTIME AND DATA QUALITY, THOUGH! better just don't do it...

	infobox_config = {
		# "infobox disease" : [
			# "Name",
			# "Image",
			# "Caption",
			# "DiseasesDB",
			# "ICD10",
			# "ICD9",
			# "ICDO",
			# "OMIM",
			# "MedlinePlus",
			# "eMedicineSubj",
			# "eMedicineTopic",
			# "MeshID",
		# ],
		"infobox actor" : [
			"honorific_prefix",
			"name",
			"honorific_suffix",
			"image",
			"image_upright",
			"image_size",
			"alt",
			"caption",
			"native_name",
			"native_name_lang",
			"pronunciation",
			"birth_name",
			"birth_date",
			"birth_place",
			"baptised",
			"disappeared_date",
			"disappeared_place",
			"disappeared_status",
			"death_date",
			"death_place",
			"death_cause",
			"body_discovered",
			"resting_place",
			"resting_place_coordinates",
			"burial_place",
			"burial_coordinates",
			"monuments",
			"residence",
			"nationality",
			"other_names",
			"citizenship",
			"education",
			"alma_mater",
			"occupation",
			"years_active",
			"era",
			"employer",
			"organization",
			"agent",
			"known_for",
			"notable_works",
			"style",
			"home_town",
			"salary",
			"net_worth",
			"height",
			"weight",
			"television",
			"title",
			"term",
			"predecessor",
			"successor",
			"party",
			"movement",
			"opponents",
			"boards",
			"religion",
			"denomination",
			"criminal_charge",
			"criminal_penalty",
			"criminal_status",
			"spouse",
			"partner",
			"children",
			"parents",
			"mother",
			"father",
			"relatives",
			"family",
			"callsign",
			"awards",
			"website",
			"module",
			"module2",
			"module3",
			"module4",
			"module5",
			"module6",
			"signature",
			"signature_size",
			"signature_alt",
			"footnotes",
		],
		# "infobox actor" : [],
	}

	# also somehow try to solve delete vs. update statements
	# statement == all changes with the same timestamp
	# so for one statement check whether, in case values are deleted, there are still non empty field left in the record (except id/article name?)

	# select what types of update statements you want
	## ATTENTION: ommiting inserts is probably bad idea as it would result in inconsistent data (i.e. updateStatements targeting nonexisting records)
	statementTypesToBeParsed = ["insert", "delete"]

	parseInfoboxUpdatesToCsv(infobox_config, statementTypesToBeParsed)




