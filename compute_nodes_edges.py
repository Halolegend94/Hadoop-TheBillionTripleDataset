f = open("output/out5/part-r-00000")

total_nodes = 0
total_edges = 0

for line in f:
    if line[0] == 'i':
        split_line = line.split()
        total_nodes += int(split_line[1])
        total_edges += int(split_line[0][1:]) * int(split_line[1])
print(total_nodes, total_edges)