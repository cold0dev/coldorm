from coldorm import FieldType, Field, ColdORM, WhereBuilder
import os.path, os
import unittest
from dataclasses import dataclass

if os.path.exists("test.db"):
    os.remove("test.db")

@dataclass
class ExampleTable:
    id: int = Field(FieldType.INTEGER, True, True)
    name: str = Field(FieldType.TEXT)
    value: float = Field(FieldType.REAL)

orm = ColdORM("test", [ExampleTable], True)

table = orm.get_table("ExampleTable")

table.add(ExampleTable(None,"NAME1", 1.0))
table.add(ExampleTable(None,"NAME2",2.0))
table.add(ExampleTable(None,"NAME3",1.0))

for e in table.get_all():
    print(e.__dict__)

class TestTableMethods(unittest.TestCase):
    def test_get_all(self):
        res = table.get_all()
        self.assertEqual(len(res), 3)
        self.assertEqual(res[0].id, 1)
        self.assertEqual(res[1].id, 2)
        self.assertEqual(res[2].id, 3)

    def test_get_by(self):
        wb = WhereBuilder("name", "NAME2")
        res = table.get_by(wb)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0].id, 2)
        wb = WhereBuilder("name", "NAME1").OR("name", "NAME3")
        res = table.get_by(wb)
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0].id, 1)
        self.assertEqual(res[1].id, 3)

if __name__ == '__main__':
    unittest.main()