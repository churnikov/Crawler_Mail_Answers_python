CREATE TABLE IF NOT EXISTS questions(
  `id` INTEGER PRIMARY KEY,
  `q_category_id` INTEGER NOT NULL
  `q_sub_category_id` INTEGER
  `q_title` TEXT NOT NULL,
  `q_text` TEXT
  FOREIGN KEY(`q_category_id`) REFERENCES categories(`id`)
  FOREIGN KEY(`q_sub_category_id`) REFERENCES sub_categories(`id`)
);

CREATE TABLE IF NOT EXISTS answers(
  `id` INTEGER PRIMARY KEY AUTOINCREMENT,
  `question_id` INTEGER NOT NULL,
  FOREIGN KEY(`question_id`) REFERENCES questions(`id`)
);

CREATE TABLE IF NOT EXISTS categories(
  `id` INTEGER PRIMARY KEY AUTOINCREMENT,
  `name` TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sub_categories(
  `id` INTEGER PRIMARY KEY AUTOINCREMENT,
  `parent_cat_id` INTEGER,
  `name` TEXT NOT NULL,
  FOREIGN KEY(`parent_cat_id`) REFERENCES categories(`id`)
);
