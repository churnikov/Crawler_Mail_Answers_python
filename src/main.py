from Crawler import Crawler

cr = Crawler()
cr.init_db()
cr.add_categories_to_db()
cr.download_all_questions()
