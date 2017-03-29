"""
Class to crawl answers mail.ru
"""
import sqlite3
import requests

from bs4 import BeautifulSoup as bs

class Crawler(object):

    def __init__(self, categories='all', timeline = 'all', verbose=True,
                 schema_name='schema.sql', db_name='q_database.sqlt',
                 bs_features='lxml'):
        """
        init method for Crawler
        :params:
            categories -- (list)              -- categories that should be downloaded
                       -- default val:'all'   -- downloads all questions
            timeline   -- (tuple of timestamp)-- download from timeline[0] to timeline[1]
                       -- default val:'all'   -- downloads all questions
            verbose    -- (bool)              -- if program should output progress
            schema_name-- (str)               -- name of sql file that describes
                                                 structure of database
                       -- default val:'schema.sql'
            db_name    -- (str)               -- name of database
                       -- default val:'q_database.sqlt'
            bs_features-- (str)               -- BeautifulSoup engine to parse html page
                          Look up https://www.crummy.com/software/BeautifulSoup/bs4/doc/ *Installing parser* section
                          It explains things about parsers
                          In short, if something goes wrong, change to 'html.parser'
                       -- deafult val:'lxml'
        """
        self.categories = categories
        self.timeline = timeline
        self.verbose = verbose
        self.schema_name = schema_name
        self.db_name = db_name
        self.bs_features=bs_features

        self.__mail_page = 'https://otvet.mail.ru/'

    def get_db(self):
        """Returns database if exist or creates one and returns it"""
        if not hasattr(self, 'db'):
            self.db = sqlite3.connect(self.db_name)
            self.db.row_factory = sqlite3.Row
        return self.db

    def init_db(self):
        """Initilizes database with sql file"""
        self.get_db()
        with open(self.schema_name, 'r') as f:
            self.db.executescript(f.read())
        self.db.commit()

    def close_db(self):
        """Closes connection to database"""
        if hasattr(self, 'db'):
            self.db.close()

    def get_page(self, params=None):
        """
        Gets page with url self.__mail_page + params.
        params usually would be ['questions', question_id]

        :returns: string of page or None if 404 or something
        """
        if params:
            url = self.__mail_page +  '/'.join(params) + '/'
        else:
            url = self.__mail_page
        r = requests.get(url)
        if r.status_code == 200:
            return r.text
        else:
            return None

    def add_to_database(self, table, items):
        """Add tuples from *items* to *table*"""
        try:
            c = self.db.cursor()
            for item in items:
                item_for_db = ','.join(item)
                print(item_for_db)
                c.execute('INSERT INTO {t} VALUES({i})'.format(t=table, i=item_for_db))
            self.db.commit()
        except:
            raise sqlite3.Error('Unable to insert items into {}'.format(table))

    def get_categories(self, add_to_db=True):
        """
        Downloads parent categories
        :param: add_to_db -- (bool) -- if True will connect to database and add them
                          -- default val:True
        :returns: (list) -- list of tuples with (category_name, name of link)
                            name_of_link: /example/
        """
        # getting main page
        text_page = self.get_page()
        soup = bs(text_page, self.bs_features)
        # searching for categories
        categories = soup.find_all('a', 'medium item item_link')
        # transforming into list of tuples
        # tuple := (name, web_name)
        if self.categories != 'all':
            cats_to_db = [(str(i), '\''+cat.text+'\'', '\''+cat['href']+'\'')
                          for i, cat in enumerate(categories)
                          if cat.text in self.categories]
        else:
            cats_to_db = [(str(i), '\''+cat.text+'\'', '\''+cat['href']+'\'')
                          for i, cat in enumerate(categories)]

        if add_to_db:
            self.add_to_database(table='categories', items=cats_to_db)
        return cats_to_db
