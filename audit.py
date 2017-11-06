import sys

file = sys.argv[1]
newfile = open("newaudit","w")

with open(file) as f:
    lines = f.readlines();


#^age:4621649:0.71:-0.217329	 ^ad_position_id:8360363:1:-0.098694	Constant:3261788:1:0.0785716	page_tld^yahoo.com:7298985:1:0.0548565	 ^gender:2126990:1:0.0119801

n = len(lines)
for i in range(n):
	token = lines[i].split()
	if len(token) != 1:
		l = len(token)
		for j in range(l):
			weight = token[j].split(":")[3]
			if weight[0] == '-':
				weight = weight[1:]
			token[j] = token[j]+":"+weight
		newfile.write("\n".join(token))
		newfile.write("\n")

newfile.close()
