from collections import OrderedDict

class Record:

    def __init__(self, attributes):
        self.id = None
        self.valueMap = OrderedDict()
        for attribute in attributes:
            self.valueMap[attribute] = ""


    def addValuesFromJsonData(self, data):
        for entry in data:
            self.__addValues(entry)

    def __addValues(self, update):
        key = self.__normalizeAttribute(update["key"])
        if "newvalue" in update and key in self.valueMap:
            self.valueMap[key] = update["newvalue"].replace("|", "").replace("\n", "").replace("\"", "\"\"")

    def __normalizeAttribute(self, attribute):
        return attribute.lower().replace(" ", "_")

    def toString(self):
        stringParts = []
        for attribute in self.valueMap:
            stringParts.append("\"" + self.valueMap[attribute] + "\"")
        return ",".join(stringParts)

    def hasNonEmptyValues(self):
        for attribute, value in self.valueMap.items():
            if attribute == "article_title":
                continue
            if value != "":
                return True
        return False



