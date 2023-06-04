from map_reduce import WeightMapReduce, Node

def mapper(words):
    return list(map(lambda word: (word, 1), words))


def reducer(a, b):
    word = a[0]
    sum_of_values = a[1] + b[1]
    return word, sum_of_values


with open('bible.txt') as file:
    words_from_file = file.read().split()

result = WeightMapReduce(mapper=mapper, reducer=reducer, nodes=nodes).execute(words_from_file)
result.sort(key=lambda p: p[1], reverse=True)
print("Top 3 Words: ", result[0], result[1], result[2])
