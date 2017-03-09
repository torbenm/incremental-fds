from collections import OrderedDict

import Utils


class Record:

    def __init__(self, attributes):
        self.id = None
        self.valueMap = OrderedDict()
        for attribute in attributes:
            self.valueMap[attribute] = ""

    def addValuesFromValueMap(self, valueMap):
        for attribute, value in valueMap.items():
            if value is None:
                value = ""
            self.valueMap[attribute] = value


    def addValuesFromJsonData(self, data):
        for entry in data:
            self.__addValues(entry)

    def __addValues(self, update):
        key = Utils.normalizeAttribute(update["key"])
        if "newvalue" in update and key in self.valueMap:
            self.valueMap[key] = Utils.cleanseValue(update["newvalue"])

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



