import pandas as pd
import sqlite3
import os
import sys
from src.exception import CustomException

class SQLiteIO:
    def __init__(self, db_path='reviews.db'):
        try:
            self.conn = sqlite3.connect(db_path)
            self.cursor = self.conn.cursor()
            self.create_table()
        except Exception as e:
            raise CustomException(e, sys)

    def create_table(self):
        try:
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS reviews (
                    product_name TEXT,
                    review_text TEXT,
                    rating INTEGER
                )
            ''')
            self.conn.commit()
        except Exception as e:
            raise CustomException(e, sys)

    def store_reviews(self, product_name: str, reviews: pd.DataFrame):
        try:
            for _, row in reviews.iterrows():
                self.cursor.execute(
                    "INSERT INTO reviews (product_name, review_text, rating) VALUES (?, ?, ?)",
                    (product_name, row.get("review"), row.get("rating"))
                )
            self.conn.commit()
        except Exception as e:
            raise CustomException(e, sys)

    def get_reviews(self, product_name: str):
        try:
            self.cursor.execute(
                "SELECT review_text, rating FROM reviews WHERE product_name = ?", (product_name,)
            )
            rows = self.cursor.fetchall()
            return pd.DataFrame(rows, columns=["review", "rating"])
        except Exception as e:
            raise CustomException(e, sys)
