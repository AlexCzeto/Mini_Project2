#!/bin/bash
# These are the commands that have to be run for phase2:
sort -u reviews.txt | perl break.pl -e >| output.txt
db_load -T -t hash -f output.txt rw.idx
sort -u pterms.txt | perl break.pl -e >| output.txt
db_load -c duplicates=1 -T -t btree -f output.txt pt.idx
sort -u rterms.txt | perl break.pl -e >| output.txt
db_load -c duplicates=1 -T -t btree -f output.txt rt.idx
sort -u scores.txt | perl break.pl -e >| output.txt
