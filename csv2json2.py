# Note all output fields are strings
import sys, csv, json
json.dump(list(csv.DictReader(sys.stdin)), sys.stdout)
