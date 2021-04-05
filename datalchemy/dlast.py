'''
datalog ast data type in python

Yihao Sun
2021 Syracuse
'''

from dataclasses import dataclass
from typing import Any

UNDESCORE = '_'

INT_TYPE = 'int'
SYM_TYPE = 'sym'
FLOAT_TYPE = 'float'


@dataclass
class MetaVar:
    ''' meta var in arg position'''
    name: str
    dtype: str = 'int'


@dataclass
class Declaration:
    ''' declare a relation '''
    name: str
    metavars: [MetaVar]


@dataclass
class Literal:
    ''' 
    literal in a datalog clause 
    python do not have sum type, the actually type of arg will be
    args :: [(Int + Str + MetaVar)]
    '''
    name: str
    rel_decl: Declaration
    args: [Any]
    negation: bool = False


@dataclass
class HornClause:
    ''' horn clause '''
    head: Literal
    body: [Literal]


@dataclass
class Fact:
    ''' edb facts in datalog '''
    rel_decl: Declaration
    values: [Any]


@dataclass
class InputRel:
    ''' input relation '''
    name: str
    input_file_path: str
    deliminator: str


@dataclass
class OutputRel:
    ''' output relation '''
    name: str


@dataclass
class DatalogProgram:
    ''' a souffle like datalog program '''
    name: str
    rel_decls: [Declaration] = None
    clauses: [HornClause] = None
    inputs: [InputRel] = None
    output: [str] = None
    fact: [Fact] = None


def is_metavar(arg):
    ''' check if the type of a variable is metavar '''
    if str(type(arg)).find('MetaVar') != -1:
        return True
    else:
        return False


def metavar_in_literal(literal: Literal) -> [MetaVar]:
    ''' get all meta variable inside a literal '''
    mv = []
    for arg in literal.args:
        if str(type(arg)).find('MetaVar') != -1:
            mv.append(mv)
    return mv

def relname_in_caluse(clause: HornClause):
    ''' get all rel name in a horn clause '''
    name_in_head = clause.head.name
    name_in_body = set()
    for l in clause.body:
        name_in_body.add(l.name)
    name_in_body.add(name_in_head)
    return name_in_body

def is_facts_valid(fact: Fact):
    return len(fact.values) == len(fact.rel_decl.metavars)


def remove_unused_metavar(clause: HornClause):
    ''' replace all unused varibale in a horn clause with underscore '''
    vheads = set()
    for arg in clause.head.args:
        # in py10 change to pattern match
        if str(type(arg)).find('MetaVar') != -1:
            vheads.add(arg.name)
    newbody = []
    for lit in clause.body:
        newargs = []
        for arg in lit.args:
            if str(type(arg)).find('MetaVar') != -1:
                if arg.name in vheads:
                    newargs.append(arg)
                else:
                    newargs.append(UNDESCORE)
            else:
                newargs.append(arg)
        newbody.append(Literal(lit.name, lit.rel_decl, newargs, lit.negation))
    newclause = HornClause(clause.head, newbody)
    return newclause


def is_horn_clause_valid(clause: HornClause) -> bool:
    '''
    in a valid horn clause every meta variable should be
    grounded
    TODO: add more check in type
    '''
    if clause.head.negation:
        return False
    vheads = set()
    for arg in clause.head.args:
        # in py10 change to pattern match
        if str(type(arg)).find('MetaVar') != -1:
            vheads.add(arg.name)
    vbody = set()
    for lit in clause.body:
        for arg in lit.args:
            if str(type(arg)).find('MetaVar') != -1:
                vbody.add(arg.name)
    return vheads.issubset(vbody)
