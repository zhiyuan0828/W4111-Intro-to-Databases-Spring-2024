from typing import Any, Dict, List, Tuple, Union

import pymysql

# Type definitions
# Key-value pairs
KV = Dict[str, Any]
# A Query consists of a string (possibly with placeholders) and a list of values to be put in the placeholders
Query = Tuple[str, List]

class DB:
	def __init__(self, host: str, port: int, user: str, password: str, database: str):
		conn = pymysql.connect(
			host=host,
			port=port,
			user=user,
			password=password,
			database=database,
			cursorclass=pymysql.cursors.DictCursor,
			autocommit=True,
		)
		self.conn = conn

	def get_cursor(self):
		return self.conn.cursor()

	def execute_query(self, query: str, args: List, ret_result: bool) -> Union[List[KV], int]:
		"""Executes a query.

		:param query: A query string, possibly containing %s placeholders
		:param args: A list containing the values for the %s placeholders
		:param ret_result: If True, execute_query returns a list of dicts, each representing a returned
							row from the table. If False, the number of rows affected is returned. Note
							that the length of the list of dicts is not necessarily equal to the number
							of rows affected.
		:returns: a list of dicts or a number, depending on ret_result
		"""
		cur = self.get_cursor()
		count = cur.execute(query, args=args)
		if ret_result:
			return cur.fetchall()
		else:
			return count


	# TODO: all methods below


	@staticmethod
	def build_select_query(table: str, columns: List[str], filters: KV) -> Query:
		"""Builds a query that selects rows. See db_test for examples.

		:param table: The table to be selected from
		:param columns: The attributes to select. If empty, then selects all columns.
		:param filters: Key-value pairs that the rows from table must satisfy
		:returns: A query string and any placeholder arguments
		"""
		# start with the SLECT
		query_str = 'SELECT '

		# add the 'rows' part
		if len(rows) == 0:
			query_str += '* '
		else:
			for i in range(len(rows) - 1):
				query_str += rows[i]
				query_str += ', '
			
			query_str += rows[-1]
			query_str += ' '
		
		# add the FROM and the table
		query_str += 'FROM '
		query_str += table
		query_str += ' '

		# add the WHERE and the filter
		replace_val = []

		if len(filters) == 0:
			query_str = query_str[:-1]
		else:
			query_str += 'WHERE '
			for i in range(len(filters) - 1):
				query_str += list(filters.keys())[i]
				query_str += ' = %s AND '
				replace_val.append(list(filters.values())[i])

			query_str += list(filters.keys())[-1]
			query_str += ' = %s'
			replace_val.append(list(filters.values())[-1])

		return query_str, replace_val

	def select(self, table: str, columns: List[str], filters: KV) -> List[KV]:
		"""Runs a select statement. You should use build_select_query and execute_query.

		:param table: The table to be selected from
		:param columns: The attributes to select. If empty, then selects all columns.
		:param filters: Key-value pairs that the rows to be selected must satisfy
		:returns: The selected rows
		"""
		query_str, replace_val = self.build_select_query(table, rows, filters)

		return self.execute_query(query_str, replace_val, True)

	@staticmethod
	def build_insert_query(table: str, values: KV) -> Query:
		"""Builds a query that inserts a row. See db_test for examples.

		:param table: The table to be inserted into
		:param values: Key-value pairs that represent the values to be inserted
		:returns: A query string and any placeholder arguments
		"""
		# begin with INSERT INTO
		query_str = 'INSERT INTO '

		# add table
		query_str += table
		query_str += ' ('

		# add values key
		keys = list(values.keys())
		vls  = list(values.values())

		for i in range(len(keys) - 1):
			query_str += keys[i]
			query_str += ', '

		query_str += keys[-1]
		query_str += ') VALUES ('

		# add values values
		replace_val = []
		for i in range(len(vls) - 1):
			query_str += '%s, '
			replace_val.append(vls[i])

		query_str += '%s)'
		replace_val.append(vls[-1])

		return query_str, replace_val

	def insert(self, table: str, values: KV) -> int:
		"""Runs an insert statement. You should use build_insert_query and execute_query.

		:param table: The table to be inserted into
		:param values: Key-value pairs that represent the values to be inserted
		:returns: The number of rows affected
		"""
		query_str, replace_val = self.build_insert_query(table, values)

		return self.execute_query(query_str, replace_val, False)

	@staticmethod
	def build_update_query(table: str, values: KV, filters: KV) -> Query:
		"""Builds a query that updates rows. See db_test for examples.

		:param table: The table to be updated
		:param values: Key-value pairs that represent the new values
		:param filters: Key-value pairs that the rows from table must satisfy
		:returns: A query string and any placeholder arguments
		"""
		# start with the SLECT
		query_str = 'UPDATE '

		# add the table and SET
		query_str += table
		query_str += ' SET '

		# add the value
		replace_val = []

		values_key = list(values.keys())
		values_val = list(values.values())
		filter_key = list(filters.keys())
		filter_val = list(filters.values())

		for i in range(len(values_key) - 1):
			query_str += values_key[i]
			query_str += ' = %s, '
			replace_val.append(values_val[i])

		query_str += values_key[-1]
		query_str += ' = %s'
		replace_val.append(values_val[-1])

		# add the filter
		if len(filters) == 0:
			pass
		else:
			query_str += ' WHERE '
			for i in range(len(filter_key) - 1):
				query_str += filter_key[i]
				query_str += ' = %s AND '
				replace_val.append(filter_val[i])

			query_str += filter_key[-1]
			query_str += ' = %s'
			replace_val.append(filter_val[-1])
		
		return query_str, replace_val

	def update(self, table: str, values: KV, filters: KV) -> int:
		"""Runs an update statement. You should use build_update_query and execute_query.

		:param table: The table to be updated
		:param values: Key-value pairs that represent the new values
		:param filters: Key-value pairs that the rows to be updated must satisfy
		:returns: The number of rows affected
		"""
		query_str, replace_val = self.build_update_query(table, values, filters)

		return self.execute_query(query_str, replace_val, False)

	@staticmethod
	def build_delete_query(table: str, filters: KV) -> Query:
		"""Builds a query that deletes rows. See db_test for examples.

		:param table: The table to be deleted from
		:param filters: Key-value pairs that the rows to be deleted must satisfy
		:returns: A query string and any placeholder arguments
		"""
		# start with the DELETE FROM
		query_str = 'DELETE FROM '
	
		# add the table
		query_str += table
		query_str += ' '

		# add the WHERE and the filter
		replace_val = []

		if len(filters) == 0:
			query_str = query_str[:-1]
		else:
			query_str += 'WHERE '
			for i in range(len(filters) - 1):
				query_str += list(filters.keys())[i]
				query_str += ' = %s AND '
				replace_val.append(list(filters.values())[i])

			query_str += list(filters.keys())[-1]
			query_str += ' = %s'
			replace_val.append(list(filters.values())[-1])

		return query_str, replace_val

	def delete(self, table: str, filters: KV) -> int:
		"""Runs a delete statement. You should use build_delete_query and execute_query.

		:param table: The table to be deleted from
		:param filters: Key-value pairs that the rows to be deleted must satisfy
		:returns: The number of rows affected
		"""
		query_str, replace_val = self.build_delete_query(table, filters)

		return self.execute_query(query_str, replace_val, False)
