import sqlite3
import os
from enum import Enum, auto

LOG = False

ormlog = os.getenv("ORMLOG")
if ormlog is not None and ormlog == "1":
    LOG = True

class FieldType(Enum):
    INTEGER = auto()
    REAL = auto()
    TEXT = auto()
    BLOB = auto()

    @staticmethod
    def type_to_string(t):
        match t:
            case FieldType.INTEGER:
                return "INTEGER"
            case FieldType.REAL:
                return "REAL"
            case FieldType.TEXT:
                return "TEXT"
            case FieldType.BLOB:
                return "BLOB"
            case _:
                raise RuntimeError("Unsupported type")

class Field:
    """
    Represents a field in a database table.
    Contains parameters used for table creation.
    """
    type: FieldType = None
    primary_key: bool = False
    auto_increment: bool = False

    def __init__(self, type: FieldType, primary_key: bool = False, auto_increment: bool = False) -> None:
        self.type = type
        self.primary_key = primary_key
        self.auto_increment = auto_increment


def extract_name_from_model(model):
    try:
      return model.table_name
    except:
      return model.__name__

def extract_fields_from_model(model):
    fields = []
    for entry in dir(model):
        e = getattr(model, entry)
        if type(e) is not Field:
            continue
        fields.append({"name": entry, "type": e})

    return fields


def create_table_from_model(model_name, fields):
    command = f"CREATE TABLE {model_name}("

    for field in fields:
        t: Field = field["type"]
        name = field["name"]
        command += f"{name} {FieldType.type_to_string(t.type)}"
        if t.primary_key:
            command += " PRIMARY KEY"
        if t.auto_increment:
            command += " AUTOINCREMENT"
        command += ","

    return command[:-1] + ")"


def extract_by_fields(model, fields):
    output = []
    fields = [field["name"] for field in fields]
    for field in fields:
        value = getattr(model, field)
        if value is None or type(value) is Field: continue
        output.append({"name": field, "value": value})
    return output

class WhereBuilder:pass
class WhereBuilder:
    def __init__(self, key, value) -> None:
        self.conditions = []
        self.conditions.append({"key": key, "value": value, "op": ""}) 

    
    def AND(self, key, value) -> WhereBuilder:
        self.conditions.append({"key": key, "value": value, "op": "and"}) 
        return self
    
    def OR(self, key, value) -> WhereBuilder:
        self.conditions.append({"key": key, "value": value, "op": "or"}) 
        return self
    
    def get_conditions(self):
        return self.conditions

def get_updated_fields(fields, entry):
    output = []
    fields = [field["name"] for field in fields]
    for field in fields:
        v = getattr(entry, field)
        
        if v is None or type(v) is Field: continue

        output.append({"name": field, "value": v})
    
    return output


class Table:
    def __init__(self, name, parent, model, fields):
        self.name = name
        self.parent = parent
        self.cursor = self.parent.cursor
        self.fields = fields
        self.model = model

    def pack_entry(self, entry, fields):
        out = {}
        if fields[0] == "*":
            fields = [field["name"] for field in self.fields]
        
        for field, value in zip(fields, entry):
            out[field] = value

        return out

    def get(self, where: WhereBuilder, fields=["*"]):
        fields = ",".join(fields)
        command = f"SELECT {fields} FROM {self.name} WHERE "
        for entry in where.get_conditions():
          op = entry["op"]
          key = entry["key"]
          command += f"{op} {key} = ?"

        if LOG:
            print(f"Executing command: `{command}`")
        res = self.cursor.execute(command, [entry["value"] for entry in where.get_conditions()])
        res = res.fetchall()
        res = [self.pack_entry(r, fields) for r in res]
        return res

    # cross_get needs explicit fields
    def cross_get(self, cross_with, where: WhereBuilder, cross: WhereBuilder, fields):
        fields = ",".join(fields)
        command = f"SELECT {fields} FROM {self.name} "
        command += f"CROSS JOIN {cross_with} WHERE "
        
        for entry in where.get_conditions():
            op = entry["op"]
            key = entry["key"]
            command += f"{op} {key} = ?"
        
        command += " AND "

        for entry in cross.get_conditions():
            op = entry["op"]
            key = entry["key"]
            value = entry["value"]
            command += f"{op} {self.name}.{key} = {cross_with}.{value}"

        if LOG:
            print(f"Executing command: `{command}`")
        res = self.cursor.execute(command, [entry["value"] for entry in where.get_conditions()])
        res = res.fetchall()
        res = [self.pack_entry(r, fields) for r in res]
        return res

    def get_all(self, fields=["*"]):
        fields = ",".join(fields)
        command = f"SELECT {fields} FROM {self.name}"
        if LOG:
            print(f"Executing command: `{command}`")
        res = self.cursor.execute(command)
        res = res.fetchall()
        res = [self.pack_entry(r, fields) for r in res]
        return res

    def add(self, entry):
        fields = extract_by_fields(entry, self.fields)
        values = [field["value"] for field in fields]
        command = f"INSERT INTO {self.name} ("

        for field in fields:
            name = field["name"]
            command += f"{name},"

        command = command[:-1] + ") VALUES ("

        for field in fields:
            command += "?,"

        command = command[:-1] + ")"
        if LOG:
            print(f"Executing command: `{command}` with values {values}")
        self.cursor.execute(command, values)

    def add_all(self, entries):
        for entry in entries:
            self.add(entry)

    def remove(self, where: WhereBuilder):
        command = f"DELETE FROM {self.name} WHERE "
        for entry in where.get_conditions():
            op = entry["op"]
            key = entry["key"]
            command += f"{op} {key} = ?"

        if LOG:
            print(f"Executing command: `{command}`")
        self.cursor.execute(command, [entry["value"] for entry in where.get_conditions()])

    def update(self, where: WhereBuilder, entry):
        command = f"UPDATE {self.name} SET "
        updated_fields = get_updated_fields(self.fields, entry)
        values = [field["value"] for field in updated_fields]

        for updated_field in updated_fields:
            name = updated_field["name"]
            command += f"{name} = ?, "

        command = command[:-2] + " WHERE "
        for entry in where.get_conditions():
            op = entry["op"]
            key = entry["key"]
            command += f"{op} {key} = ?"
        
        wheres = [entry["value"] for entry in where.get_conditions()]

        if LOG:
            print(f"Executing command: `{command}` with values {values}")
        self.cursor.execute(command, values + wheres)

class Engine:
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
                if LOG:
                    print(f"Executing command: `{command}`")
                self.cursor.execute(command)
            self.tables.append(Table(model_name, self, model, fields))

    def get_table(self, name) -> Table:
        for table in self.tables:
            if table.name == name:
                return table
        raise RuntimeError(f"Table {name} not found")

    def list_tables(self):
        command = "SELECT name FROM sqlite_master WHERE type='table'"
        if LOG:
            print(f"Executing command: `{command}`")
        res = self.cursor.execute(command)
        res = res.fetchall()
        return [e[0] for e in res]
    
    def commit(self):
        self.connection.commit()
