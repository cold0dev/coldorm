import sqlite3
import os
import json

LOG = False

ormlog = os.getenv("ORMLOG")
if ormlog is not None and ormlog == "1":
    LOG = True

def extract_name_from_model(model):
    return str(model).split("'")[1].split(".")[1]


def extract_fields_from_model(model):
    fields = []
    for entry in dir(model):
        if entry.startswith("__"):
            continue
        if getattr(model, entry) is not None:
            continue
        fields.append(entry)

    return fields


def create_table_from_model(model_name, fields):
    fields = ", ".join(fields)
    return f"CREATE TABLE {model_name}({fields})"


def extract_by_fields(model, fields):
    output = []
    for field in fields:
        output.append(getattr(model, field))
    return output


def str_to_dbstr(s):
    return f"'{s}'"


def convert_value(v):
    if type(v) is str:
        return str_to_dbstr(v)
    else:
        return v

class ModelByteStruct:
    class Endianness:
        BIG = 0
        LITTLE = 1

    fields = []
    endianness = Endianness.BIG

    def __init__(self, endianness = Endianness.BIG) -> None:
        self.endianness = endianness

    def convert_type(self, type):
        types = {
            "u8": "B",
            "u16": "H",
            "u32": "L",
            "u64": "Q",
            "i8": "b",
            "i16": "h",
            "i32": "l",
            "i64": "q",
            "f32": "f",
            "f64": "d",
            "str": "s",
        }

        return types[type]

    def add_field(self, name, type):
        self.fields.append({"name": name, "type": self.convert_type(type)})


# Optional class used for serialization and deserialization 
class Model:
    def to_dict(self):
        fields = {}
        for entry in dir(self):
            if entry.startswith("__"):
                continue
            if str(type(getattr(self, entry))) == "<class 'method'>":
                continue
            fields[entry] = getattr(self, entry)

        return fields
    
    def from_dict(self, data):
        for k, v in data.items():
            setattr(self, k, v)

    def to_json(self):
        return json.dumps(self.to_dict())

    def from_json(self, data):
        obj = json.loads(data)
        for key, value in obj.items():
            if key.startswith("__"):
                continue
            setattr(self, key, value)

    def to_xml(self):
        raise RuntimeError("NOT IMPLEMETED")
    
    def from_xml(self, xml):
        raise RuntimeError("NOT IMPLEMETED")
    
    def to_yaml(self):
        raise RuntimeError("NOT IMPLEMETED")
    
    def from_yaml(self, yaml):
        raise RuntimeError("NOT IMPLEMETED")

class Table:
    def __init__(self, name, parent, model, fields):
        self.name = name
        self.parent = parent
        self.cursor = self.parent.cursor
        self.fields = fields
        self.model = model

    def pack_entry(self, entry):
        e = self.model()
        for name, field in zip(self.fields, entry):
            setattr(e, name, field)
        return e

    def pack_entries(self, entries):
        output = []
        for entry in entries:
            output.append(self.pack_entry(entry))
        return output

    def get_by(self, key, value):
        command = f"SELECT * FROM {self.name} WHERE "

        if type(key) is list and type(value) is list:
            if len(key) != len(value):
                raise RuntimeError(f"Different count of keys({len(key)}) and values({len(value)})")
            for i in range(len(key)):
                k = key[i]
                v = value[i]
                command += f"{k}={convert_value(v)} "
                if i != len(key) - 1:
                    command += "AND "
        else:
            command += f"{key}={convert_value(value)}"

        if LOG:
            print(f"Executing command: {command}")
        res = self.cursor.execute(command)
        res = res.fetchall()
        return self.pack_entries(res)

    def get_all(self):
        command = f"SELECT * FROM {self.name}"
        if LOG:
            print(f"Executing command: {command}")
        res = self.cursor.execute(command)
        res = res.fetchall()
        return self.pack_entries(res)

    def add(self, entry):
        fields = extract_by_fields(entry, self.fields)
        command = f"INSERT INTO {self.name} VALUES ("
        for field in fields:
            if type(field) is str:
                command += f"'{field}',"
            elif type(field) is int:
                command += f"{field},"
            elif type(field) is float:
                command += f"{field},"
            # TODO: add bytes support
            else:
                raise RuntimeError(f"Unsupported type of {field}")
        command = command[:-1] + ")"
        if LOG:
            print(f"Executing command: {command}")
        self.cursor.execute(command)
        self.parent.connection.commit()

    def remove(self, key, value):
        command = f"DELETE FROM {self.name} WHERE {key}={convert_value(value)}"
        if LOG:
            print(f"Executing command: {command}")
        self.cursor.execute(command)
        self.parent.connection.commit()

    def update_by(self, key, entry):
        raise RuntimeError("NOT IMPLEMETED")

    


class ColdORM:
    def __init__(self, name: str, models: list[type], migration: bool = False):
        self.name = name
        self.tables = []

        self.connection = sqlite3.connect(f"{self.name}.db")
        self.cursor = self.connection.cursor()

        for model in models:
            fields = extract_fields_from_model(model)
            model_name = extract_name_from_model(model)
            if migration:
                command = create_table_from_model(model_name, fields)
                self.cursor.execute(command)
            self.tables.append(Table(model_name, self, model, fields))

    def get_table(self, name):
        for table in self.tables:
            if table.name == name:
                return table
        raise RuntimeError(f"Table {self.name} not found")

    def list_tables(self):
        command = "SELECT name FROM sqlite_master WHERE type='table'"
        if LOG:
            print(f"Executing command: {command}")
        res = self.cursor.execute(command)
        res = res.fetchall()
        return [e[0] for e in res]
