from collections import OrderedDict
import Utils


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
        # idea: if the old and the new values are identical, the statement does nothing
        if self.action == "update":
            if len([x for x in self.valueMap.keys() if self.valueMap[x] != self.oldValueMap[x]]) == 0:
                return True
        return False

    def __addValues(self, update):
        key = Utils.normalizeAttribute(update["key"])
        if "newvalue" in update and key in self.valueMap:
            self.valueMap[key] = update["newvalue"].replace("\"", "\"\"")
        if "oldvalue" in update and key in self.oldValueMap:
            self.oldValueMap[key] = update["oldvalue"].replace("\"", "\"\"")

