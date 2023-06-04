from map_reduce import WeightMapReduce, Node


def create_row_and_columns_pairs(a, b):
    pairs = []
    n = len(b)

    for i in range(n):
        row = b[i]
        column = [a[j][i] for j in range(len(a))]
        pairs.append((column, row, i))

    return pairs


def mapper(elements_pairs):
    sum_elements = []

    for elements_pair in elements_pairs:
        column = elements_pair[0]
        row = elements_pair[1]
        for row_index, row_element in enumerate(row):
            for column_index, column_element in enumerate(column):
                coordinates = (column_index, row_index)
                sum_element = row_element * column_element
                sum_elements.append((coordinates, sum_element))

    return sum_elements


def reducer(pair1, pair2):
    coordinates = pair1[0]
    value = pair1[1] + pair2[1]
    return coordinates, value

row_and_column_pairs = create_row_and_columns_pairs(mat1, mat2)
print(len(row_and_column_pairs))
result = WeightMapReduce(mapper=mapper, reducer=reducer, nodes=nodes).execute(row_and_column_pairs)
print(result)