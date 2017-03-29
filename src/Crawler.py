"""
Class to crawl answers mail.ru
"""
import sqlite3
import requests

from bs4 import BeautifulSoup as bs

class Crawler(object):

    def __init__(self, categories='all', timeline = 'all', verbose=True,
                 schema_name='schema.sql', db_name='q_database.sqlt'):
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
        """
        self.categories = categories
        self.timeline = timeline
        self.verbose = verbose
        self.schema_name = schema_name
        self.db_name = db_name

        self.__mail_page = 'https://touch.otvet.mail.ru/'

    def get_db(self):
        """Returns database if exist or creates one and returns it"""
        if not hasattr(self, 'db'):
            self.db = sqlite3.connect(self.db_name)
            self.db.row_factory = sqlite3.Row
        return self.db

    def init_db(self):
        """Initilizes database with sql file"""
        get_db()
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
