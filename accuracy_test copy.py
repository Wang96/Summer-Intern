import sys

# two space separated arguments (first : true test set, second : prediction result)

file_1 = sys.argv[1]
file_2 = sys.argv[2]

with open(file_1) as f_1:
    lines_1 = f_1.readlines();

with open(file_2) as f_2:
    lines_2 = f_2.readlines();

n = len(lines_1)
if n != len(lines_2):
    print('not equal length')
    sys.exit()

cnt = 0
correct = 0
for i in range (n):
    temp_1 = int(lines_1[i].strip())
    temp_2 = float(lines_2[i].strip())

    if temp_2 >= 0:
        temp_2 = 1
    else:
        temp_2 = -1

    cnt = cnt + 1
    if temp_1 == temp_2:
        correct = correct + 1

accuracy = float(correct)/cnt

print('accuracy = ' + str(accuracy))
