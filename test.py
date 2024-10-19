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
orm.commit()

for e in table.get_all():
    print(e.__dict__)

class TestTableMethods(unittest.TestCase):
    def test01_get_all(self):
        res: list[ExampleTable] = table.get_all()
        self.assertEqual(len(res), 3)
        self.assertEqual(res[0].id, 1)
        self.assertEqual(res[1].id, 2)
        self.assertEqual(res[2].id, 3)

    def test02_get(self):
        wb = WhereBuilder("name", "NAME2")
        res: list[ExampleTable] = table.get(wb)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0].id, 2)
        wb = WhereBuilder("name", "NAME1").OR("name", "NAME3")
        res: list[ExampleTable] = table.get(wb)
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0].id, 1)
        self.assertEqual(res[1].id, 3)

    def test03_update(self):
        wb = WhereBuilder("name", "NAME2")
        new = ExampleTable(None, None, 4.0)
        table.update(wb, new)
        res: list[ExampleTable] = table.get(wb)
        self.assertEqual(res[0].value, 4.0)

    def test04_remove(self):
        wb = WhereBuilder("name", "NAME2")
        table.remove(wb)
        res: list[ExampleTable] = table.get_all()
        self.assertEqual(res[1].id, 3)

if __name__ == '__main__':
    unittest.main()