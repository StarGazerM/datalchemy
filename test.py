''' a test program '''

from datalchemy.dsl import program

program('path-g'). \
    decl('edge', ('from_', 'int'), ('to_', 'int')). \
    decl('path', ('from_', 'int'), ('to_', 'int')). \
    fact('edge', 1, 2). \
    fact('edge', 2, 3). \
    fact('edge', 3, 4). \
    ℍ(
        ('path', (['from_'], ['to_'])),  # :-
        ('edge', (['from_'], ['to_']))). \
    ℍ(
        ('path', (['from_'], ['to_'])),  # :-
        ('path', (['from_'], ['mid_'])),
        ('path', (['mid_'], ['to_']))). \
    output('path'). \
    run()
