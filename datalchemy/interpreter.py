'''
interpret a datalog ast into sql query

Yihao Sun
2021 Syracuse
'''

import sys

import networkx as nx
from sqlalchemy import create_engine, Table, MetaData, Column
from sqlalchemy import Integer, Float, String
from sqlalchemy import insert

from datalchemy.ast import *


def metatype_to_columntype(metavar: MetaVar):
    if metavar.dtype == INT_TYPE:
        return Integer
    if metavar.dtype == SYM_TYPE:
        return String(50)
    if metavar.dtype == FLOAT_TYPE:
        return Float
    else:
        return String(255)


class DatalogIntepretor:
    ''' interpretor '''

    def __init__(self):
        self.engine = create_engine("file::memory:?cache=shared")
        self.db_meta = MetaData()
        self.rel_graph = nx.DiGraph()

    def run(self, program: DatalogProgram):
        ''' run a datalog program '''
        for decl in program.rel_decls:
            self.add_declaration(decl)
        self.__create_table()
        for clause in program.clauses:
            self.add_clause(clause)

    def add_fact(self, fact: Fact):
        ''' add a fact into EDB '''
        if not is_facts_valid(fact):
            print(f'arg number mismatch for {fact.rel_decl.name}')
            sys.exit(3)
        col_names = [mv.name for mv in fact.rel_decl.metavars]
        rel_name = fact.rel_decl.name
        edb_table = self.db_meta.tables['rel_name']
        val_dict = {k : v for k in col_names for v in fact.values}
        # find table name in meta data
        stmt = (
            insert(edb_table).
            values(**val_dict)
        )
        conn = self.engine.connect()
        conn.execute(stmt)
        

    def add_declaration(self, decl: Declaration):
        '''
        a declaration will be 2 table in database, 1 for old fact, 1 for generated new facts
        '''
        columns = [Column('id', Integer, primary_key=True, autoincrement=True)]
        for metavar in decl.metavars:
            columns.append(
                Column(metavar.name, metatype_to_columntype(metavar), nullable=False))
        Table(decl.name, self.db_meta, *columns)
        Table(f'{decl.name}_new', self.db_meta, *columns)

    def add_clause(self, clause: HornClause):
        ''' add a horn clause into program, update relation graph '''
        if not is_horn_clause_valid(clause):
            print(f'HornClause {str(clause)} has ungrounded variable')
            sys.exit(3)
        hname = clause.head.name
        for lit in clause.body:
            if self.rel_graph.has_edge(lit.name, hname):
                self.rel_graph[lit.name][hname]['weight'] = self.rel_graph[lit.name][hname]['weight'] + 1
            else:
                self.rel_graph.add_weighted_edges_from([(lit.name, hname, 1)])

    def __create_table(self):
        self.db_meta.create_all()
