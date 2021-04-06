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

SELECT path1.from path2.to
FROM path path1, path path2
WHERE path1.to == path2.from
'''
edge_decl = Declaration(
    'edge', [MetaVar('from_', 'int'), MetaVar('to_', 'int')])
path_decl = Declaration(
    'path', [MetaVar('from_', 'int'), MetaVar('to_', 'int')])
program = DatalogProgram(
    'path-g',
    [edge_decl, path_decl],
    [
        HornClause(
            Literal('path', path_decl, [
                    MetaVar('from_', 'int'), MetaVar('to_', 'int')]),
            [Literal('edge', edge_decl, [MetaVar(
                'from_', 'int'), MetaVar('to_', 'int')])]
        ),
        HornClause(
            Literal('path', path_decl, [
                    MetaVar('from_', 'int'), MetaVar('to_', 'int')]),
            [
                Literal('path', path_decl, [
                        MetaVar('from_', 'int'), MetaVar('mid_', 'int')]),
                Literal('path', path_decl, [
                        MetaVar('mid_', 'int'), MetaVar('to_', 'int')])
            ]
        )
    ],
    output=[
        'path'
    ],
    fact=[
        Fact(edge_decl, [1, 2]),
        Fact(edge_decl, [2, 3]),
        Fact(edge_decl, [3, 4])
    ]
)

DatalogIntepretor().run(program)
