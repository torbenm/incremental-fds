from collections import OrderedDict

class Statement:

    def __init__(self, attributes):
        self.record = None
        self.action = None
        self.valueMap = OrderedDict()
        self.oldValueMap = OrderedDict()
        for attribute in attributes:
            self.valueMap[attribute] = None
            self.oldValueMap[attribute] = None
        self.removable = False

    def toString(self):
        stringParts = []
        stringParts.append("\"" + self.action + "\"")
        for attribute in self.valueMap:
            oldValue = self.oldValueMap[attribute]
            if oldValue is None:
                oldValue = ""
            newValue = self.valueMap[attribute]
            if newValue is None:
                newValue = ""

            if self.action == "update" and attribute != "article_title":
                stringParts.append("\"" + oldValue + "|" + newValue + "\"")
            else:
                stringParts.append("\"" + newValue + "\"")
        return ",".join(stringParts)


    def addValuesFromJsonData(self, data):
        for entry in data:
            self.__addValues(entry)
        self.action = "update"

    def __addValues(self, update):
        key = self.__normalizeAttribute(update["key"])
        if "newvalue" in update and key in self.valueMap:
            self.valueMap[key] = update["newvalue"].replace("\"", "\"\"")
        if "oldvalue" in update and key in self.oldValueMap:
            self.oldValueMap[key] = update["oldvalue"].replace("\"", "\"\"")

    def __normalizeAttribute(self, attribute):
        return attribute.lower().replace(" ", "_")
