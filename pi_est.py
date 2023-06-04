from random import random
from map_reduce import WeightMapReduce

def is_point_inside_unit_circle(p):
    x, y = random(), random()
    return 1 if x*x + y*y < 1 else

def mapper(p):
    return p, is_point_inside_unit_circle(p)

def reducer(p1, p2):
    return p1[0], p1[1] + p2[1]

n = 100000000
count = WeightMapReduce(mapper=mapper, reducer=reducer, nodes=nodes).execute(range(n))[0]
print(4.0 * count / n)