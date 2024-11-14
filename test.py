from coldorm import FieldType, Field, Engine, WhereBuilder
import os.path, os
import unittest
from dataclasses import dataclass

if os.path.exists("test.db"):
    os.remove("test.db")

@dataclass
class ExampleCrossTable:
    table_name = "named_table"
    id: int = Field(FieldType.INTEGER, primary_key=True, auto_increment=True)
    cross_value: float = Field(FieldType.REAL)


@dataclass
class ExampleTable:
    id: int = Field(FieldType.INTEGER, primary_key=True, auto_increment=True)
    name: str = Field(FieldType.TEXT)
    value: float = Field(FieldType.REAL)

orm = Engine("test", [ExampleCrossTable, ExampleTable], True)

cross_table = orm.get_table("named_table")
cross_table.add(ExampleCrossTable(None, 20.0))
cross_table.add(ExampleCrossTable(None, 30.0))

table = orm.get_table("ExampleTable")

table.add(ExampleTable(None,"NAME1", 1.0))
table.add(ExampleTable(None,"NAME2",2.0))
table.add(ExampleTable(None,"NAME3",1.0))
orm.commit()

for e in table.get_all():
    print(e)

class TestTableMethods(unittest.TestCase):
    def test01_get_all(self):
        res = table.get_all()
        self.assertEqual(len(res), 3)
        self.assertEqual(res[0]["id"], 1)
        self.assertEqual(res[1]["id"], 2)
        self.assertEqual(res[2]["id"], 3)

    def test02_get(self):
        wb = WhereBuilder("name", "NAME2")
        res = table.get(wb)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]["id"], 2)
        wb = WhereBuilder("name", "NAME1").OR("name", "NAME3")
        res = table.get(wb)
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0]["id"], 1)
        self.assertEqual(res[1]["id"], 3)

    def test03_update(self):
        wb = WhereBuilder("name", "NAME2")
        new = ExampleTable(None, None, 4.0)
        table.update(wb, new)
        res = table.get(wb)
        self.assertEqual(res[0]["value"], 4.0)

    def test04_remove(self):
        wb = WhereBuilder("name", "NAME2")
        table.remove(wb)
        res = table.get_all()
        self.assertEqual(res[1]["id"], 3)
    
    def test05_cross_get(self):
        wb = WhereBuilder("name", "NAME1")
        cross = WhereBuilder("id", "id")
        res = table.cross_get("named_table", wb, cross, fields=["name"])
        self.assertEqual(len(res), 1)
    
    def test06_get_fields(self):
        wb = WhereBuilder("name", "NAME1")
        cross = WhereBuilder("id", "id")
        res = table.get(wb, fields=["id", "name"])
        self.assertEqual(len(res[0]), 2)
        
        res = table.get_all(fields=["id", "name"])
        self.assertEqual(len(res[0]), 2)
        
        res = table.cross_get("named_table", wb, cross, fields=["name", "value", "cross_value"])
        self.assertEqual(len(res[0]), 3)

if __name__ == '__main__':
    unittest.main()
