import sys
import csv
import json

# Note all output fields are strings
json.dump([line for line in csv.DictReader(sys.stdin)], sys.stdout)
