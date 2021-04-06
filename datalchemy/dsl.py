'''
some until function to allow dsl-like code experience

Yihao Sun
2021 Syracuse
'''

import logging
import sys

from datalchemy.dlast import OutputRel, DatalogProgram
from datalchemy.dlast import MetaVar, Declaration, HornClause, Fact, Literal
from datalchemy.interpreter import DatalogIntepretor


class Datalog:
    ''' A Datalog lazy builder wrapper '''

    def __init__(self, name: str):
        self.prog = DatalogProgram(name, [], [], [], [], [])

    def __get_rel_by_name(self, name):
        ''' return a Decal of a relation name, if not found return None '''
        for d in self.prog.rel_decls:
            if d.name == name:
                return d
        return None

    def decl(self, name, *arg):
        ''' 
        declare a relation 
        decl("edge", ('from','int'), ('to', 'int'))
        ⇒
        .decl edge(from: int, to: int)
        '''
        mvs = [MetaVar(*p) for p in arg]
        self.prog.rel_decls.append(Declaration(name, mvs))
        return self

    def fact(self, name, *args):
        ''' 
        add a EDB fact
        fact('edge', 1, 2)
        ⇒
        edge(1, 2).
        '''
        rel_decl = self.__get_rel_by_name(name)
        if rel_decl is None:
            logging.error(
                f'Datalog Error: relation "{name}" must be defined before used!')
            sys.exit(3)
        if len(args) != len(rel_decl.metavars):
            logging.error(
                f'Datalog Error: relation "{name}" has arity mismatch!')
            sys.exit(3)
        self.prog.fact.append(Fact(rel_decl, args))
        return self

    def output(self, name):
        ''' 
        declare a output relation 
        output('path')
        ⇒
        .output path
        '''
        self.prog.output.append(name)
        return self

    def ℍ(self, head, *bodys):
        '''
        add a horn clause to program
        use list to denote a meta variable
        ℍ(('path', (['from'], 1)), ('edge', (['from'], ['to'])))
        ⇒
        path(from, 1) :- edge(from, to)
        '''

        def parse_lit(raw):
            ldecl = self.__get_rel_by_name(raw[0])
            if ldecl is None:
                logging.error(
                    f'Datalog Error: relation "{raw[0]}" must be defined before used!')
                sys.exit(3)
            if len(raw[1]) != len(ldecl.metavars):
                logging.error(
                    f'Datalog Error: relation "{raw[0]}" has arity mismatch!')
                sys.exit(3)
            larg = []
            for i, a in enumerate(raw[1]):
                if type(a) == list:
                    if len(a) != 1:
                        print(
                            "Datalog Error: [] just denote meta variabel please put exactly one str inside ")
                        sys.exit(3)
                    targ = ldecl.metavars[i].dtype
                    larg.append(MetaVar(a[0], targ))
                else:
                    larg.append(a)
            return Literal(raw[0], ldecl, larg)
        hc = HornClause(parse_lit(head), [parse_lit(l) for l in bodys])
        self.prog.clauses.append(hc)
        return self

    def run(self):
        ''' run the datalog program '''
        return DatalogIntepretor().run(self.prog)


def program(name: str) -> Datalog:
    ''' create a Datalog '''
    return Datalog(name)
