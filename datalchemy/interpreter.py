'''
interpret a datalog ast into sql query

Yihao Sun
2021 Syracuse
'''

import sys
import time

import networkx as nx
from sqlalchemy import create_engine, Table, MetaData, Column
from sqlalchemy import Integer, Float, String
from sqlalchemy import insert, func, text, delete, select

from datalchemy.dlast import MetaVar, DatalogProgram, Fact, Declaration, HornClause
from datalchemy.dlast import is_metavar, is_facts_valid, is_horn_clause_valid, metavar_in_literal, relname_in_caluse
from datalchemy.dlast import INT_TYPE, SYM_TYPE, FLOAT_TYPE, UNDESCORE


def metatype_to_columntype(metavar: MetaVar):
    if metavar.dtype == INT_TYPE:
        return Integer
    if metavar.dtype == SYM_TYPE:
        return String(50)
    if metavar.dtype == FLOAT_TYPE:
        return Float
    else:
        return String(255)


def val_to_sql_str(v):
    if type(v) == str:
        return f"'{v}'"
    else:
        return str(v)


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

    def run(self, program: DatalogProgram, silent=False):
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
        while True:
            sccs = list(nx.strongly_connected_components(self.rel_graph))
            if sccs == []:
                break
            computed = []
            for scc in sccs:
                scc_clauses = list(
                    filter(lambda c: c.head.name in scc, self.clauses))
                # print(f'computing relation {scc}')
                if scc_clauses == []:
                    computed = computed + list(scc)
                    continue
                self.compute_fixpoint(scc_clauses)
                computed = computed + list(scc)
            self.rel_graph.remove_nodes_from(computed)
        # self.compute_fixpoint(program.clauses)
        if not silent:
            self.print_output()
        return self.fetch_output()

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
        for i, name in enumerate(col_names):
            val_dict[name] = fact.values[i]
        # find table name in meta data
        stmt = (
            insert(edb_table).
            values(**val_dict).
            prefix_with('OR IGNORE')
        )
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
        Table(decl.name, self.db_meta, *columns)
        Table(f'{decl.name}_new', self.db_meta, *columns_new)
        self.rels.append(decl)

    def add_clause(self, clause: HornClause):
        ''' add a horn clause into program, update relation graph '''
        if not is_horn_clause_valid(clause):
            print(f'HornClause {str(clause)} has ungrounded variable')
            sys.exit(3)
        # clause = remove_unused_metavar(clause)
        hname = clause.head.name
        for lit in clause.body:
            if lit.name == hname:
                continue
            if self.rel_graph.has_edge(lit.name, hname):
                self.rel_graph[lit.name][hname]['weight'] = self.rel_graph[lit.name][hname]['weight'] + 1
            else:
                self.rel_graph.add_weighted_edges_from([(hname, lit.name, 1)])
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
                print(_r[1:])

    def fetch_output(self):
        ''' return output '''
        outs = {}
        for output_name in self.output_relnames:
            tb = self.__get_table(output_name)
            stmt = select(tb)
            res = self.db_conn.execute(stmt)
            outs[output_name] = [_r[1:] for _r in res]
        return outs

    def __turncate_Δ(self, rel: Declaration):
        ''' turncate Δ table of given relation '''
        tb_delta = self.__get_Δ_table(rel.name)
        stmt = delete(tb_delta)
        self.db_conn.execution_options(autocommit=True).execute(stmt)

    def __create_table(self):
        self.db_meta.create_all()

    def __get_table(self, name):
        ''' get a sql idb table object in meta data by it's name '''
        return self.db_meta.tables[name]

    def __get_Δ_table(self, name):
        ''' get a sql Δ table in meta data by name '''
        return self.db_meta.tables[f'{name}_new']

    def __select_horn_clause(self, clause: HornClause):
        ''' 
        select index for a horn clause, using naive index selection, just select on table
        which the meta variable first appear
        this is too complicate in sqlalchemy, so I just assemble sql by hand

        return selected data
        '''
        # select metavar
        select_list = []
        selected_col_names = []
        from_list = []
        where_list = []
        col_mv_map = {}
        rel_counter_map = {}        # how many time a rel is referenced
        for lit in clause.body:
            lit_col_names = [m.name for m in lit.rel_decl.metavars]
            if lit.name not in rel_counter_map.keys():
                rel_counter_map[lit.name] = 1
            else:
                rel_counter_map[lit.name] = rel_counter_map[lit.name] + 1
            table_name = f'{lit.name}_{rel_counter_map[lit.name]}'
            from_list.append(f'{lit.name}_new AS {table_name}')
            for i, arg in enumerate(lit.args):
                col = f'{table_name}.{lit.rel_decl.metavars[i].name}'
                if is_metavar(arg):
                    if arg.name not in col_mv_map.keys():
                        # select rule
                        col_mv_map[arg.name] = col
                        select_list.append(col)
                        selected_col_names.append(lit_col_names[i])
                    else:
                        where_list.append(f'{col_mv_map[arg.name]} = {col}')
                elif arg == UNDESCORE:
                    continue
                else:
                    where_list.append(f'{col} = {val_to_sql_str(arg)}')
        select_sql = f"SELECT {', '.join(select_list)} "
        from_sql = f"FROM {', '.join(from_list)} "
        if where_list == []:
            where_sql = ''
        else:
            where_sql = f"WHERE {' AND '.join(where_list)}"
        stmt = select_sql + from_sql + where_sql + ';'
        res = self.db_conn.execute(text(stmt))
        val_mv_list = []
        for _r in res:
            rdict = {}
            for i, k in enumerate(selected_col_names):
                rdict[k] = _r[i]
            val_mv_list.append(rdict)
        return val_mv_list

    def __fullfill_Δ(self, relations):
        ''' copy everything inside IDB into Δ in a given relation name set '''
        for rel in relations:
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
        rel_in_clauases = []
        for c in clauses:
            rel_names = rel_names.union(relname_in_caluse(c))
        for _r in self.rels:
            if _r.name in rel_names:
                rel_in_clauases.append(_r)
        # let Δ = original at the begining of algorithm
        self.__fullfill_Δ(rel_in_clauases)
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
                print('reach fixpoint!')
                break
            prev_count = Δ_count
            for clause in clauses:
                target_table = self.__get_table(clause.head.name)
                target_Δ_table = self.__get_Δ_table(clause.head.name)
                target_mvs = [mv.name for mv in clause.head.rel_decl.metavars]
                selected_data = self.__select_horn_clause(clause)
                # create values
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
                    # b = b ∪ new_b;
                    stmt = (
                        insert(target_table).
                        values(selected_data).
                        prefix_with('OR IGNORE')
                    )
                    self.db_conn.execute(stmt)
                    # Δb = new_b
                    self.__turncate_Δ(clause.head.rel_decl)
                    stmt = (
                        insert(target_Δ_table).
                        values(selected_data)
                    )
                    self.db_conn.execute(stmt)
