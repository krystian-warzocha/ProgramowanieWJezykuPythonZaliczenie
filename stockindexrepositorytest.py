# -*- coding: utf-8 -*-

import stockindexrepository
import sqlite3
import unittest

db_path = 'stockindex.db'

class StockIndexRepositoryTest(unittest.TestCase):

    def setUp(self):
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        c.execute("DELETE FROM StockIndexQuote")
        c.execute("DELETE FROM StockIndex")
        c.execute("INSERT INTO StockIndex (id, symbol) VALUES(1, 'WIG20')")
        c.execute("INSERT INTO StockIndexQuote (value, value_date, stockindex_id) VALUES(2000.0, '2016-01-04', 1)")
        c.execute("INSERT INTO StockIndexQuote (value, value_date, stockindex_id) VALUES(1900.0, '2016-01-05', 1)")
        conn.commit()
        conn.close()

    def tearDown(self):
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        c.execute("DELETE FROM StockIndexQuote")
        c.execute("DELETE FROM StockIndex")
        conn.commit()
        conn.close()

    def testGetByIdInstance(self):
        index = stockindexrepository.StockIndexRepository().getById(1)
        self.assertIsInstance(index, stockindexrepository.StockIndex, "Objekt nie jest klasy StockIndex")

    def testGetByIdNotFound(self):
        self.assertEqual(stockindexrepository.StockIndexRepository().getById(22),
                None, "Powinno wyjsc None")

    def testGetByIdInvitemsLen(self):
        self.assertEqual(len(stockindexrepository.StockIndexRepository().getById(1).quotes),
                2, "Powinno wyjsc 2")

    def testDeleteNotFound(self):
        self.assertRaises(stockindexrepository.RepositoryException,
                stockindexrepository.StockIndexRepository().delete, 22)

    def testMean(self):
        self.assertTrue(abs(stockindexrepository.StockIndexRepository().getById(1).mean - 1950.0) < 0.0001)

if __name__ == "__main__":
    unittest.main()
