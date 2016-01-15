import sqlite3
import datetime
import time
from scipy import stats

db_path = 'stockindex.db'

class StockIndex():
    """Pojedynczy indeks akcji"""
    def __init__(self, id, symbol = "", quotes = []):
        self.id = id
        self.symbol = symbol
        self.quotes = quotes
        self.update_statistics()

    def calculate_statistics(self):
        """Przelicza statystyki zwiazane ze zbiorem kursow indeksu"""
        values = []
        for quote in self.quotes:
            values.append(quote.value)
        n, (smin, smax), sm, sv, ss, sk = stats.describe(values)
        return (sm, sv, ss, sk)

    def update_statistics(self):
        """Aktualizuje statystyki indeksu jesli indeks ma przypisane kursy"""
        if self.quotes:
            self.mean, self.variance, self.skew, self.kurtosis = self.calculate_statistics()
        else:
            self.mean = 0.0
            self.variance = 0.0
            self.skew = 0.0
            self.kurtosis = 0.0

    def __repr__(self):
        return '{StockIndex: {id: %d, symbol: "%s", mean: %f, variance: %f, skew: %f, kurtosis: %f, quotes: %s}}' % (
            self.id, self.symbol, self.mean, self.variance, self.skew, self.kurtosis, self.quotes)

class StockIndexQuote():
    """Pojedynczy dzienny kurs zamkniecia indeksu"""
    def __init__(self, value, value_date):
        self.value = value
        self.value_date = value_date

    def __repr__(self):
        return '{StockIndexQuote: {value: %f, value_date: %s}}' % (
            self.value, str(self.value_date))

class RepositoryException(Exception):
    """Wyjatek repozytorium indeksow gieldowych"""
    def __init__(self, message, *errors):
        Exception.__init__(self, message)
        self.errors = errors

class Repository():
    """Klasa bazowa repozytorium"""
    def __init__(self):
        try:
            self.conn = self.get_connection()
        except Exception as e:
            raise RepositoryException("Database connection failure", *e.args)
        self._complete = False

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        self.close()

    def complete(self):
        self._complete = True

    def get_connection(self):
        return sqlite3.connect(db_path)

    def close(self):
        if self.conn:
            try:
                if self._complete:
                    self.conn.commit()
                else:
                    self.conn.rollback()
            except Exception as e:
                raise RepositoryException(*e.args)
            finally:
                try:
                    self.conn.close()
                except Exception as e:
                    raise RepositoryException(*e.args)

class StockIndexRepository(Repository):
    """Repozytorium indeksow gieldowych oraz ich kursow"""
    
    def add(self, index):
        """Dodaje do BD indeks gieldowy razem z jego kursami"""
        try:
            c = self.conn.cursor()
            c.execute('INSERT INTO StockIndex (id, symbol, mean, variance, skew, kurtosis) VALUES(?,?,?,?,?,?)',
                      (index.id, str(index.symbol), index.mean, index.variance, index.skew, index.kurtosis))
            if index.quotes:
                for quote in index.quotes:
                    try:
                        c.execute('INSERT INTO StockIndexQuote (value, value_date, stockindex_id) VALUES (?,?,?)',
                                  (quote.value, str(quote.value_date), index.id))
                    except Exception as e:
                        print "error: %f, %s, %d" % (quote.value, quote.value_date.strftime('%Y-%m-%d'), index.id)
                        raise RepositoryException("Error when adding index quote %s to index %s" % (str(quote), str(index)))
        except Exception as e:
            # print e
            raise RepositoryException("Error when adding index %s to repository" % str(index))

    def delete(self, index):
        """Usuwa podany index z bazy danych"""
        try:
            c = self.conn.cursor()
            c.execute('DELETE FROM StockIndexQuote WHERE stockindex_id=?', str(index.id))
            c.execute('DELETE FROM StockIndex WHERE id=?', str(index.id))
        except Exception as e:
            # print e
            raise RepositoryException('Error when deleting the index %s' % str(index))

    def getById(self, id):
        """Returns the Index"""
        try:
            c = self.conn.cursor()
            c.execute("select * from StockIndex where id=?", (str(id),))
            row = c.fetchone()
            index = StockIndex(id)
            if row == None:
                index = None
            else:
                index.symbol = row[1]
                index.mean = row[2]
                index.variance = row[3]
                index.skew = row[4]
                index.kurtosis = row[5]
                c.execute("SELECT * FROM StockIndexQuote WHERE stockindex_id=? ORDER BY value_date ASC", (str(id),))
                quotes_rows = c.fetchall()
                quotes_list = []
                for quote_row in quotes_rows:
                    quote = StockIndexQuote(value=quote_row[0], value_date=quote_row[1])
                    quotes_list.append(quote)
                index.quotes = quotes_list
                index.update_statistics()
        except Exception as e:
            # print e
            raise RepositoryException('Error when getting the stock index by id: %s' % str(id))
        return index

    def update(self, index):
        """Metoda aktualizuje index gieldowy wraz z jego kursami w bazie danych"""
        try:
            idx = self.getById(index.id)
            if idx != None:
                self.delete(idx)
            self.add(index)
        except Exception as e:
            # print e
            raise RepositoryException("Error updating the index %s" % str(index))

if __name__ == '__main__':
    try:
        with StockIndexRepository() as index_repository:
            index_repository.add(
                StockIndex(id = 1, symbol = 'WIG20',
                        quotes = [  StockIndexQuote(value = 2000.0, value_date=datetime.datetime(2016, 1, 4)),
                                    StockIndexQuote(value = 1900.0, value_date=datetime.datetime(2016, 1, 6)),
                                    StockIndexQuote(value = 1800.0, value_date=datetime.datetime(2016, 1, 7))
                                  ])
                )
            index_repository.complete()
    except RepositoryException as e:
        print(e)

    print StockIndexRepository().getById(1)

    try:
        with StockIndexRepository() as index_repository:
            index_repository.update(
                StockIndex(id = 1, symbol = 'WIG30',
                        quotes = [  StockIndexQuote(value = 22000.0, value_date=datetime.datetime(2016, 1, 4)),
                                    StockIndexQuote(value = 21900.0, value_date=datetime.datetime(2016, 1, 6)),
                                    StockIndexQuote(value = 21800.0, value_date=datetime.datetime(2016, 1, 7))
                                  ])
                )
            index_repository.complete()
    except RepositoryException as e:
        print(e)

    print StockIndexRepository().getById(1)
    
    try:
        with StockIndexRepository() as index_repository:
            index_repository.delete( StockIndex(id = 1) )
            index_repository.complete()
    except RepositoryException as e:
        print(e)

