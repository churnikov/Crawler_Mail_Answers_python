"""
Class to crawl answers mail.ru
"""
import sqlite3
import requests
import re

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

        self.__mail_page = 'https://otvet.mail.ru'
        self.__exclude = ['Золотой фонд', 'О проектах Mail.Ru', 'Другое']
        self.__reg_q_number = re.compile('[\d]+')

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
            url = self.__mail_page +  ''.join(params)
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
                item_for_db = ', '.join(item)
                print(item_for_db)
                c.execute('INSERT INTO {t} VALUES({i})'.format(t=table, i=item_for_db))
            self.db.commit()
        except:
            raise sqlite3.Error('Unable to insert items into {}'.format(table))

    def get_categories(self, page=None):
        """
        Downloads parent categories
        :param: add_to_db -- (bool) -- if True will connect to database and add them
                          -- default val:True
        :returns: (list) -- list of tuples with (category_name, name of link)
                            name_of_link: /example/
        """
        # getting main page
        text_page = self.get_page(page)
        soup = bs(text_page, self.bs_features)
        # searching for categories
        categories = soup.find_all('a', 'medium item item_link')
        # adding categories to db and return list
        return categories

        if add_to_db:
            self.add_to_database(table=table, items=cats_to_db)

    def __get_cats2sql(self, cats):
        """Stupid (dog) fuction to prepare data for sql"""
        if self.categories != 'all':
            return [(str(j),                    #id; autoincrement
                     '\'' + itm.text + '\'',    #name
                     '\'' + itm['href'] + '\'') #link
                     for j, itm in enumerate(cats)
                         if itm.text in self.categories
                            and itm.text not in self.__exclude]
        else:
            return [(str(j),                    #id; autoincrement
                     '\'' + itm.text + '\'',    #name
                     '\'' + itm['href'] + '\'') #link
                     for j, itm in enumerate(cats)
                        if itm.text not in self.__exclude]

    def __get_subcats2sql(self, cats, i, parent_name, start_id):
        """Stupid (dog) fuction to prepare data for sql
           i -- id of parent category
        """
        if self.categories != 'all':
            return [(str(start_id + j),         #id; autoincrement
                     str(i),                    #parent_id
                     '\'' + itm.text + '\'',    #name
                     '\'' + itm['href'] + '\'') #link
                     for j, itm in enumerate(cats)
                        if itm.text in self.categories
                            and itm.text not in self.__exclude
                            and parent_name not in self.__exclude
                            and itm.text not in self.parent_cats]
        else:
            return [(str(start_id + j),         #id; autoincrement
                     str(i),                    #parent_id
                     '\'' + itm.text + '\'',    #name
                     '\'' + itm['href'] + '\'') #link
                     for j, itm in enumerate(cats)
                        if itm.text not in self.__exclude
                        and parent_name not in self.__exclude
                        and itm.text not in self.parent_cats]

    def add_to_db_categories(self):
        """
        Downloads categories and saves them to database
        """
        categories = self.get_categories()
#       itm looks like this: <a class="medium item item_link" href="/autosport/" name="">Автоспорт</a>,
#       so we are getting text = Автоспорт and 'href' = /autosport/
        cats2sql = self.__get_cats2sql(categories)
        self.add_to_database(table='categories', items=cats2sql)
        self.parent_cats = [cat.text for cat in categories]

        sub2sql = []
        j = 0
        for i, c in enumerate(categories):
            par_name = c.text
            href = c['href']
            sub_categories = self.get_categories(page=href)
            sub2sql.extend(self.__get_subcats2sql(sub_categories, i, par_name, j))
            j += len(sub_categories)
        self.add_to_database(table='sub_categories',
                            items=sub2sql)

    def __fetch_latest_question_id(self):
        """
        Loads main page of `otvet.mail.ru` and gets `id` of latest question.
        Then sets it to `self.latest_question` and returns this values
        """
        page = self.get_page(params=['/open/'])
        soup = bs(page, self.bs_features)

        latest_q = soup.find('a', 'blue item__text')
        self.latest_question = self.__reg_q_number.search(latest_q['href']).group(0)
        return self.latest_question

    def get_latest_question_id(self):
        """Gets latest_question from database. If there is None, fetch one from web."""
        c = self.db.cursor()
        resp = c.execute('SELECT max(`id`) FROM questions')
        latest_q = resp.fetchone()
        self.db.commit()

        if latest_q:
            self.latest_question = latest_q[0]
            return latest_q[0]
        else:
            return self.__fetch_latest_question_id()
