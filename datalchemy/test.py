'''
a test script

Yihao Sun
2021 Syracuse
'''

from datalchemy.dlast import Declaration, DatalogProgram, MetaVar, Literal, HornClause, Fact
from datalchemy.interpreter import DatalogIntepretor

''' 
edge(1,2).
edge(2,3).
edge(3,4).
.decl edge(from, to)
.decl path(from, to)
path(from, to) :- edge(from, to)
path(from, to) :- path(from, mid), path(mid, to)
'''
edge_decl = Declaration('edge', [MetaVar('from', 'int'), MetaVar('to', 'int')])
path_decl = Declaration('path', [MetaVar('from', 'int'), MetaVar('to', 'int')])
program = DatalogProgram(
    'path-g',
    [edge_decl, path_decl],
    [
        HornClause(
            Literal('path', path_decl, [MetaVar('from', 'int'), MetaVar('to', 'int')]),
            [Literal('edge', edge_decl, [MetaVar('from', 'int'), MetaVar('to', 'int')])]
        ),
        HornClause(
            Literal('path', path_decl, [MetaVar('from', 'int'), MetaVar('to', 'int')]),
            [
                Literal('path', path_decl, [MetaVar('from', 'int'), MetaVar('mid', 'int')]),
                Literal('path', path_decl, [MetaVar('mid', 'int'), MetaVar('to', 'int')])
            ]
        )
    ],
    output = [
        'path'
    ],
    fact = [
        Fact(edge_decl, [1, 2]),
        Fact(edge_decl, [2, 3]),
        Fact(edge_decl, [3, 4])
    ]
)

DatalogIntepretor().run(program)
