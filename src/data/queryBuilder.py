

class QueryBuilder:
    def __init__(self) -> None:
        pass

    def build_query(self, selects, table, joins, constraints):
        return f"{self._build_selects(selects)} {self._build_froms( table, joins)} {self._build_constraints(constraints)};"

    def _build_selects(self, selects=None):
        if selects is not None:
            return f"SELECT {', '.join(selects)}"
        else:
            return "SELECT *"

    def _build_froms(self,  table, joins=None):
        if joins is not None:
            joins_query = " ".join([f"JOIN {f[0]} ON {f[1]}" for f in joins])
            return f"FROM {table} {joins_query}"
        else:
            return f"FROM {table}"

    def _build_constraints(self, constraints=None):
        if constraints is not None:
            return f"WHERE {' AND '.join(constraints)} "
        else:
            return ""
