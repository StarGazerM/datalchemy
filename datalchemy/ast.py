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
    dtype: str


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
    negation: bool


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
    rel_decls: [Declaration]
    clauses: [HornClause]
    inputs: [InputRel]
    output: [OutputRel]


def is_facts_valid(fact: Fact):
    return len(fact.values) == len(fact.rel_decl.metavars)


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
    for lit in vbody:
        for arg in lit.args:
            if str(type(arg)).find('MetaVar') != -1:
                vbody.add(arg.name)
    return vheads.issubset(vbody)
