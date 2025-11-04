import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pathlib import Path

def load_schema(path="config/schema.json"):
    schema_json = json.loads(Path(path).read_text())
    fields = []
    for f in schema_json["fields"]:
        t = f["type"].lower()
        if t == "string": dt = StringType()
        elif t == "integer": dt = IntegerType()
        elif t == "double": dt = DoubleType()
        fields.append(StructField(f["name"], dt))
    return StructType(fields)
