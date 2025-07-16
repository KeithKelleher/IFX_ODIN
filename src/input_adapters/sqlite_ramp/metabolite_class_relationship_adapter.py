from typing import List, Generator
from src.input_adapters.sqlite_ramp.ramp_sqlite_adapter import RaMPSqliteAdapter
from src.models.metabolite import Metabolite
from src.models.metabolite_class import MetaboliteClassRelationship, MetaboliteClass
from src.input_adapters.sqlite_ramp.tables import MetaboliteClass as SqliteMetaboliteClass


class MetaboliteClassRelationshipAdapter(RaMPSqliteAdapter):

    def __init__(self, sqlite_file):
        RaMPSqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all(self) -> Generator[List[MetaboliteClassRelationship], None, None]:
        results = self.get_session().query(
            SqliteMetaboliteClass.ramp_id,
            SqliteMetaboliteClass.class_level_name,
            SqliteMetaboliteClass.class_name,
            SqliteMetaboliteClass.source
        ).all()

        metabolite_class_relationships: List[MetaboliteClassRelationship] = [
            MetaboliteClassRelationship(
                start_node=Metabolite(id=row[0]),
                end_node=MetaboliteClass(
                    id=MetaboliteClass.compiled_name(row[1], row[2]),
                    level=row[1],
                    name=row[2]),
                source=row[3]
            ) for row in results
        ]
        yield metabolite_class_relationships

