import sys

origin = sys.argv[1]
pred = sys.argv[2]
num = int(sys.argv[3])

with open(origin) as f_1:
    lines_1 = f_1.readlines();

with open(pred) as f_2:
    lines_2 = f_2.readlines();

n = len(lines_1)
if n != len(lines_2):
    print('not equal length')
    sys.exit()


relevant = 0
selected = 0
correct = 0

for i in range(n):
	truth = int(lines_1[i].strip())
	guess = float(lines_2[i].strip())
	if guess >= 0:
		guess = 1
	else:
		guess = -1
		
	if guess == num:
		selected += 1
	if guess == num and truth == guess:
		correct += 1
		relevant += 1
	elif truth == num and truth != guess:
		relevant += 1

precision = float(correct)/selected
recall = float(correct)/relevant

print('Precision is '+str(precision))
print('Recall is '+str(recall))