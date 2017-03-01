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
            newValue = self.valueMap[attribute]

            if self.action == "update" and attribute != "article_title":
                if oldValue is None:
                    oldValue = ""
                if newValue is None:
                    newValue = oldValue
                stringParts.append("\"" + oldValue + "|" + newValue + "\"")
            else:
                if newValue is None:
                    newValue = ""
                stringParts.append("\"" + newValue + "\"")
        return ",".join(stringParts)


    def addValuesFromJsonData(self, data):
        for entry in data:
            self.__addValues(entry)
        self.action = "update"

    def isIrrelevant(self):
        if self.action == "update":
            for attribute in self.valueMap:
                if attribute == "article_title":
                    continue
                if self.oldValueMap[attribute] is not None or self.valueMap[attribute] is not None:
                    return False
            return True
        return False

    def __addValues(self, update):
        key = self.__normalizeAttribute(update["key"])
        if "newvalue" in update and key in self.valueMap:
            self.valueMap[key] = update["newvalue"].replace("\"", "\"\"")
        if "oldvalue" in update and key in self.oldValueMap:
            self.oldValueMap[key] = update["oldvalue"].replace("\"", "\"\"")

    def __normalizeAttribute(self, attribute):
        return attribute.lower().replace(" ", "_")
