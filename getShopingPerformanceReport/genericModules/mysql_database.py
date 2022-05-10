import os
import json
import pymysql

class MysqlDatabase:

	_default_charset = 'utf8mb4'
	_default_collation = 'utf8mb4_unicode_ci'

	def __init__(self, db_name:str, db_file_path:str):
		"""Default constructor"""
		self.db_name = db_name
		self.db_file_path = db_file_path
		self._create_database()
		self.connect()

	def _create_database(self):
		db_name = self.db_name
		self.connect(is_exists = False)
		print("Creating database with name {}".format(db_name))
		sql = "CREATE DATABASE IF NOT EXISTS {}".format(db_name)
		self.execute(sql)
		sql= "USE {}".format(db_name)
		self.cursor.execute(sql)
		self.execute(sql)

	def connect(self, is_exists = True):
		db_name = self.db_name
		db_file_path = self.db_file_path
		data = json.load(open(db_file_path))

		credentials = data["credentials"]

		connect_kwargs = {
			'host': credentials['host'],
			'user': credentials['user'],
			'password': credentials['password']
		}

		if is_exists is True:
			connect_kwargs.update({
				'db': db_name,
	            'use_unicode':True,
	            'charset':"utf8",
	            'cursorclass':pymysql.cursors.DictCursor
			})

		conn = pymysql.connect(**connect_kwargs)
		cursor = conn.cursor()
		self.conn = conn
		self.cursor = cursor
		return conn, cursor

	def execute(self, query:str):
		"""
		Execute query

		Args:
		query(str) : query to execute
		"""
		self.cursor.execute(query)
		result = self.cursor.fetchall()
		return result

	def commit(self):
		self.conn.commit()

	def close(self):
		self.conn.close()

	def create_table(self,query:str):
		"""
		Creates a table

		Args:
		query(str) : create query

		"""

		if "PRIMARY KEY" not in query:
			raise ValueError("Define primay key while creating table")

		self.execute(query)

		if (("CHARSET" not in query) and ("COLLATE" not in query)):

			table_name = [i for i in query.split('(')[0].split(' ') if i!=''][-1]
			self.update_charset_collation(table_name)

	def insert_into_table(self,insert_query:str):
		"""
		Inserts data in table

		Args:
		insert_query(str) : insert query

		"""

		self.execute(insert_query)

	def read_table(self,query:str):
		"""
		Returns queried data

		Args:
		query(str) : read query

		"""

		data = self.execute(query)
		return data

	def delete_rows(self,query):
		"""
		Deletes rows

		Args:
		query(str) : delete query

		"""
		self.execute(query)

	def update_charset_collation(self,table_name:str):
		"""
		Updates charset and collation of table to default values.

		Args:
		table_name(str) : table name to update character set and collation for
		"""

		query = "ALTER TABLE {} CONVERT TO CHARACTER SET {} COLLATE {}".format(table_name,self._default_charset,self._default_collation)
		self.execute(query)

	def drop_table(self,table_name:str):
		"""
		Deletes the table structure from the database, along with any data stored in the table.

		Args:
		table_name(str) : table name to drop
		"""
		query = "DROP TABLE IF EXISTS {}".format(table_name)
		self.execute(query)

	def truncate_table(self,table_name:str):
		"""
		Deletes data stored in the table without removing table structure.

		Args:
		table_name(str) : table name to truncate
		"""
		query = "TRUNCATE TABLE {}".format(table_name)
		self.execute(query)

	def drop_database(self):
		"""
		Drops database.
		"""
		query = "DROP DATABASE {}".format(self.db_name)
		self.execute(query)