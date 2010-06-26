#!/usr/bin/python

import os

n = 100
domain = "debian"

os.system("erl -sname node0 -noshell -s kyeeva_app start > out < /dev/null &")

for i in range(1, n):
    os.system("erl -sname node%s -noshell -kyeeva initnode node0@%s -s kyeeva_app start > out < /dev/null &" % (i, domain))
