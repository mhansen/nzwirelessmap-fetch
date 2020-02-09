import sys
import csv
import json

json.dump([line for line in csv.DictReader(sys.stdin)], sys.stdout)
