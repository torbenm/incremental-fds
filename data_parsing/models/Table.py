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
        for attribute, value in updateStatement.valueMap.items():
            # this should theoretically just copy the record's old value to the old values of the update statement
            updateStatement.oldValueMap[attribute] = self.data[updateStatement.record].valueMap[attribute]
            newValue = updateStatement.valueMap[attribute]
            if newValue is not None:
                self.data[updateStatement.record].valueMap[attribute] = newValue
        if not self.data[updateStatement.record].hasNonEmptyValues():
            updateStatement.action = "delete"


    def deleteRecord(self, id):
        pass

    def insertRecord(self, insertStatement):
        record = Record(insertStatement.valueMap.keys())
        self.data[insertStatement.record] = record
