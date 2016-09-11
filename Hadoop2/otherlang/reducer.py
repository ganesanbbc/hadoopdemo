#!/usr/bin/env python

from operator import itemgetter
import sys

current_drug = None
sum = 0
drug = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    drug, amount = line.split('\t',1)

    # convert amount (currently a string) to int
    try:
        amount = int(amount)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: drug) before it is passed to the reducer
    if current_drug == drug: # Is drug in current key equal to the drug in the previous Key
        sum += amount
    else:
        if current_drug:# is not none -- it executes for end of every key
            # write result to STDOUT
            print '%s\t%s' % (current_drug, sum)
        sum = amount
        current_drug = drug

# do not forget to output the last drug if needed!
if current_drug == drug:
    print '%s\t%s' % (current_drug, sum)

