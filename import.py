import xlrd
from pymongo import MongoClient
from bson.son import SON

workbook = xlrd.open_workbook('Premier League 2011-12 Match by Match.xls')
sheet = workbook.sheet_by_name('Players')

total_rows = sheet.nrows - 1

# Convert column headings to lowercase_underscore format
headings = [c.replace(" ", "_").lower() for c in sheet.row_values(0)]

mongo = MongoClient('localhost', 27017)
db = mongo.players
collection = db.players_collection

players = []
num_row = 0
insert_batch_size = 100

print "Starting import..."

while num_row < total_rows:
    num_row += 1

    # http://api.mongodb.org/python/current/api/bson/son.html
    son = SON(zip(headings, sheet.row_values(num_row)))
    players.append(son)

    if num_row % insert_batch_size == 0:
        collection.insert(players)
        players = []
        print "Inserted %s of %s records" % (num_row, total_rows + 1)

if len(players):
    collection.insert(players)

# Add any indexes

print "Import completed"
