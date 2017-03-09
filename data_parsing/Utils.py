def normalizeAttribute(attribute):
    return attribute.lower().replace(" ", "_").replace("-", "_")

def cleanseValue(value):
    value = value.replace("|", "").replace("\n", "").replace("\t", "").replace("\r", "").replace("\"", "\"\"")
    if len(value) > 500:
        value = value[0:500]
    return value