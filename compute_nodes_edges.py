f = open("out/part-r-00000")

total = 0
for line in f:
    if line[0] == 'i':
        total += int(line.split()[1])

print(total)