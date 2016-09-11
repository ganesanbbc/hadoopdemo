#!/usr/bin/env python

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    elements = line.split(',')
    # increase counters
    print '%s\t%s'%(elements[2],elements[4])
