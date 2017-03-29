"""
Class to crawl answers mail.ru
"""
import sqlite3

class Crawler(object):

    def __init__(self, categories='all', timeline = 'all', verbose=True):
        """
        init method for Crawler
        :params:
            categories -- (list)              -- categories that should be downloaded
                       -- default val:'all'   -- downloads all questions
            timeline   -- (tuple of timestamp)-- download from timeline[0] to timeline[1]
                       -- default val:'all'   -- downloads all questions
            verbose    -- (bool)              -- if program should output progress
        """
        self.categories = categories
        self.timeline = timeline
        self.verbose = verbose
        self.__mail_page = 'https://touch.otvet.mail.ru/'

    def connect_db(self, schema_name='schema.sql', db_name='q_database.sqlt'):
        """
        connects to db of creates one with name in 'db_name' from 'schema.sql'
        :params:
            schema_name -- (str) -- name of schema file
                        -- default: schema.sql
            db_name     -- (str) --
        """
        try:
            self.db_name = db_name
            self.db = sqlite3.connect(db_name)

            self.db.row_factory = sqlite3.Row
        except:
            print('Unable to connect to database')
            return False
        return True

    def __init_db(self, schema_name, db_name):
        """
        type in .sql file CREATE IF TABLE `NOT EXISTS`
        if table `exists`, this won't overwrite table
        """
        
