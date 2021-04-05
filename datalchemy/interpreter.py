'''
interpret a datalog ast into sql query

Yihao Sun
2021 Syracuse
'''

import sys

import networkx as nx
# import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, Column
from sqlalchemy import Integer, Float, String
from sqlalchemy import insert, func, text, delete, select

from datalchemy.dlast import MetaVar, DatalogProgram, Fact, Declaration, HornClause
from datalchemy.dlast import is_metavar, is_facts_valid, is_horn_clause_valid, remove_unused_metavar, metavar_in_literal, relname_in_caluse
from datalchemy.dlast import INT_TYPE, SYM_TYPE, FLOAT_TYPE


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
        self.engine = create_engine('sqlite://', echo=False)
        self.db_conn = self.engine.connect()
        self.db_meta = MetaData(bind=self.db_conn)
        self.clauses = []
        self.rels = []
        self.output_relnames = []
        self.rel_graph = nx.DiGraph()

    def run(self, program: DatalogProgram):
        ''' run a datalog program '''
        for decl in program.rel_decls:
            self.add_declaration(decl)
        self.__create_table()
        for clause in program.clauses:
            self.add_clause(clause)
        for fact in program.fact:
            self.add_fact(fact)
        self.output_relnames = program.output
        # TODO: compute scc first
        self.compute_fixpoint(program.clauses)
        self.print_output()

    def add_fact(self, fact: Fact):
        ''' add a fact into EDB '''
        if not is_facts_valid(fact):
            print(f'arg number mismatch for {fact.rel_decl.name}')
            sys.exit(3)
        col_names = [mv.name for mv in fact.rel_decl.metavars]
        rel_name = fact.rel_decl.name
        edb_table = self.db_meta.tables[rel_name]
        pk_str = ''
        for v in fact.values:
            pk_str = pk_str + str(v)
        val_dict = {}
        val_dict['pk'] = pk_str
        # val_dict = {k: v for k in col_names for v in fact.values}
        for i, name in enumerate(col_names):
            val_dict[name] = fact.values[i]
        # find table name in meta data
        stmt = (
            insert(edb_table).
            values(**val_dict).
            prefix_with('OR IGNORE')
        )
        # conn = self.engine.connect()
        self.db_conn.execute(stmt)

    def add_declaration(self, decl: Declaration):
        '''
        a declaration will be 2 table in database, 1 for old fact, 1 for generated new facts
        '''
        # pk of a column is a string contain all data in other column
        columns = [Column('pk', String, primary_key=True,
                          nullable=False, unique=True)]
        for metavar in decl.metavars:
            columns.append(
                Column(metavar.name, metatype_to_columntype(metavar), nullable=False))
        columns_new = [
            Column('pk', String, primary_key=True, nullable=False, unique=True)]
        for metavar in decl.metavars:
            columns_new.append(
                Column(metavar.name, metatype_to_columntype(metavar), nullable=False))
        # tmp is just a duplication of delta table
        columns_tmp = [
            Column('pk', String, primary_key=True, nullable=False, unique=True)]
        for metavar in decl.metavars:
            columns_tmp.append(
                Column(metavar.name, metatype_to_columntype(metavar), nullable=False))
        Table(decl.name, self.db_meta, *columns)
        Table(f'{decl.name}_new', self.db_meta, *columns_new)
        Table(f'{decl.name}_tmp', self.db_meta, *columns_tmp)
        self.rels.append(decl)

    def add_clause(self, clause: HornClause):
        ''' add a horn clause into program, update relation graph '''
        if not is_horn_clause_valid(clause):
            print(f'HornClause {str(clause)} has ungrounded variable')
            sys.exit(3)
        clause = remove_unused_metavar(clause)
        hname = clause.head.name
        for lit in clause.body:
            if self.rel_graph.has_edge(lit.name, hname):
                self.rel_graph[lit.name][hname]['weight'] = self.rel_graph[lit.name][hname]['weight'] + 1
            else:
                self.rel_graph.add_weighted_edges_from([(lit.name, hname, 1)])
        self.clauses.append(clause)

    def print_rel(self, rel_name):
        ''' print all facts of a relation '''
        print(f'>>>>>>>>>>>>> {rel_name} >>>>>>>>>>>>>>>')
        tb = self.__get_table(rel_name)
        stmt = select(tb)
        res = self.db_conn.execute(stmt)
        for _r in res:
            print(_r)

    def print_Δ(self, rel_name):
        ''' print all facts of a relation '''
        print(f'>>>>>>>>>>>>> {rel_name}_new >>>>>>>>>>>>>>>')
        tb = self.__get_Δ_table(rel_name)
        stmt = select(tb)
        res = self.db_conn.execute(stmt)
        for _r in res:
            print(_r)

    def print_output(self):
        ''' print the output relation '''
        for output_name in self.output_relnames:
            print(f'>>>>>>>>>>>>> {output_name} >>>>>>>>>>>>>>>')
            tb = self.__get_table(output_name)
            stmt = select(tb)
            res = self.db_conn.execute(stmt)
            for _r in res:
                print(_r)

    def __turncate_Δ(self):
        ''' turncate all Δ table '''
        for _r in self.rels:
            tb_delta = self.__get_Δ_table(_r.name)
            stmt = delete(tb_delta)
            self.db_conn.execution_options(autocommit=False).execute(stmt)
            tb_tmp = self.__get_tmp_table(_r.name)
            self.db_conn.execution_options(
                autocommit=False).execute(delete(tb_tmp))
        self.db_conn.execute(text('COMMIT'))

    def __turncate_tmp(self, rel):
        ''' turncate tmp table '''
        tb_tmp = self.__get_tmp_table(rel.name)
        self.db_conn.execution_options(
            autocommit=False).execute(delete(tb_tmp))
        self.db_conn.execute(text('COMMIT'))

    def __refresh_tmp(self, rel):
        ''' delete all in tmp make it same as Δ '''
        self.__turncate_tmp(rel.name)
        Δ_table = self.__get_Δ_table(rel.name)
        tmp_table = self.__get_tmp_table(rel.name)
        col_names = ['pk'] + [m.name for m in rel.metavars]
        select_stmt = select(*Δ_table.c)
        stmt = (
            insert(tmp_table).
            from_select(col_names, select_stmt)
        )
        self.db_conn.execute(stmt)

    def __create_table(self):
        self.db_meta.create_all()

    def __get_table(self, name):
        ''' get a sql idb table object in meta data by it's name '''
        return self.db_meta.tables[name]

    def __get_Δ_table(self, name):
        ''' get a sql Δ table in meta data by name '''
        return self.db_meta.tables[f'{name}_new']

    def __get_tmp_table(self, name):
        ''' get a sql Δ table in meta data by name '''
        return self.db_meta.tables[f'{name}_tmp']

    def __select_horn_clause(self, clause: HornClause):
        ''' 
        select index for a horn clause, using naive index selection, just select on table
        which the meta variable first appear

        return select sql and index order in selection
        '''
        # select metavar
        select_list = []
        selected_col_names = []
        filter_list = []
        col_mv_map = {}
        for lit in clause.body:
            lit_col_names = [m.name for m in lit.rel_decl.metavars]
            for i, arg in enumerate(lit.args):
                table = self.__get_Δ_table(lit.name)
                col = getattr(table.c, lit_col_names[i])
                if is_metavar(arg):
                    if arg.name not in col_mv_map.keys():
                        # select rule
                        col_mv_map[arg.name] = col
                        select_list.append(col)
                        selected_col_names.append(lit_col_names[i])
                    else:
                        # a join here
                        filter_list.append(col == col_mv_map[arg.name])
                else:
                    # where
                    filter_list.append(col == arg)
        # create query here
        stmt = select(*select_list)
        for fcond in filter_list:
            stmt = stmt.filter(fcond)
        print(stmt)
        res = self.db_conn.execute(stmt)
        val_mv_list = []
        for _r in res:
            rdict = {}
            for i, k in enumerate(selected_col_names):
                rdict[k] = _r[i]
            val_mv_list.append(rdict)
        print(val_mv_list)
        return val_mv_list

    def __drain_Δ(self):
        ''' move all data inside delta table into IDB '''
        for rel in self.rels:
            table = self.__get_table(rel.name)
            Δ_table = self.__get_Δ_table(rel.name)
            col_names = ['pk'] + [m.name for m in rel.metavars]
            select_stmt = select(*Δ_table.c)
            stmt = (
                insert(table).
                prefix_with('OR IGNORE').
                from_select(col_names, select_stmt)
            )
            self.db_conn.execute(stmt)

    def __fullfill_Δ(self):
        ''' copy everything inside IDB into Δ '''
        for rel in self.rels:
            table = self.__get_table(rel.name)
            Δ_table = self.__get_Δ_table(rel.name)
            col_names = ['pk'] + [m.name for m in rel.metavars]
            select_stmt = select(*table.c)
            stmt = (
                insert(Δ_table).
                from_select(col_names, select_stmt)
            )
            self.db_conn.execute(stmt)

    def compute_fixpoint(self, clauses: [HornClause]):
        ''' 
        compute the fixpoint of a set of horn clause using semi-naive evaluation
        '''
        # all relation name needed to be computed
        rel_names = set()
        for c in clauses:
            rel_names = rel_names.union(relname_in_caluse(c))
        # let Δ = original at the begining of algorithm
        self.__fullfill_Δ()
        # only need to keep track of the number of record inside db
        # the result of sqlalchemy will not contain any info when executing multi instert so put this silly code here
        prev_count = 0
        while True:
            Δ_count = 0
            for name in rel_names:
                table = self.__get_Δ_table(name)
                stmt = select(func.count()).select_from(table)
                res = self.db_conn.execute(stmt)
                Δ_count = Δ_count + res.fetchone()[0]
            if Δ_count == prev_count:
                print('reach fixpoint')
                break
            prev_count = Δ_count
            for clause in clauses:
                # used_mv = metavar_in_literal(clause.head)
                target_table = self.__get_Δ_table(clause.head.name)
                target_mvs = [mv.name for mv in clause.head.rel_decl.metavars]
                selected_data = self.__select_horn_clause(clause)
                # create values
                # val_map = {}
                for i, arg in enumerate(clause.head.args):
                    if not is_metavar(arg):
                        selected_data = map(lambda x: x.update(
                            {target_mvs[i]: arg}), selected_data)
                # insert
                if selected_data != []:
                    # selected_data_with_pk = []
                    for d in selected_data:
                        pk_str = ''
                        for arg in d.values():
                            pk_str = pk_str + str(arg)
                        d['pk'] = pk_str
                        # selected_data_with_pk.append()
                    stmt = (
                        insert(target_table).
                        values(selected_data).
                        prefix_with('OR IGNORE')
                    )
                    self.db_conn.execute(stmt)
                self.__refresh_tmp(clause.head.rel_decl)
            self.__drain_Δ()
