import unittest
from pymongo import MongoClient
from pymongo import DESCENDING
from bson.code import Code


class QueryTest(unittest.TestCase):
    def setUp(self):
        self.mongo = MongoClient('localhost', 27017)
        self.db = self.mongo.players
        self.collection = self.db.players_collection

    def test_goal_scorers(self):
        # Map reduce to get a list of goal scorers
        mapper = Code("""
                        function () {
                          emit(this.player_surname, {goals: this.goals});
                        }
                      """)

        reducer = Code("""
                         function (key, values) {
                           var sum = 0;
                           values.forEach(function(doc) {
                             sum += doc.goals;
                           });
                           return {goals: sum};
                         }
                       """)

        # Only include results where goals are greater than zero
        self.collection.map_reduce(mapper, reducer, out="scorers",
                                   query={"goals": {"$gt": 0}})

        # Sort in descending order
        result = list(self.db.scorers.find().sort(u'value', DESCENDING))

        self.assertEqual(len(result), 261)
        # Test top scorer
        self.assertEqual(result[0][u'_id'], u'van Persie')
        self.assertEqual(result[0][u'value'], {u'goals': 30.00})

if __name__ == '__main__':
    unittest.main()
