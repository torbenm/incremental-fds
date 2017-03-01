from copy import deepcopy

from models.Record import Record


class Table:

    def __init__(self, records):
        self.data = {}
        for record in records:
            self.data[record.id] = deepcopy(record)

    def get(self, id):
        return self.data[id]

    def updateRecord(self, updateStatement):
        if updateStatement.valueMap["article_title"] == "UltraSPARC T3":
            print("break")
        if updateStatement.record in self.data:
            for attribute, value in updateStatement.valueMap.items():
                oldValue = self.data[updateStatement.record].valueMap[attribute]
                if oldValue == "":
                    updateStatement.oldValueMap[attribute] = None
                else:
                    updateStatement.oldValueMap[attribute] = oldValue
                newValue = updateStatement.valueMap[attribute]
                if newValue is not None:
                    self.data[updateStatement.record].valueMap[attribute] = newValue
            if not self.data[updateStatement.record].hasNonEmptyValues():
                updateStatement.action = "delete"
                self.deleteRecord(updateStatement)
        else:
            updateStatement.action = "insert"
            self.insertRecord(updateStatement)


    def deleteRecord(self, updateStatement):
        if updateStatement.record in self.data:
            del self.data[updateStatement.record]
        else:
            updateStatement.removable = True

    def insertRecord(self, insertStatement):
        record = Record(insertStatement.valueMap.keys())
        record.addValuesFromValueMap(insertStatement.valueMap)
        self.data[insertStatement.record] = record

