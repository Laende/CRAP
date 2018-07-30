import mysql.connector as mariadb


class DBError(Exception):
	"""
	Fange exceptions for invalide kommandoer og andre responser som
	ikke er en kode 200 respons.
	"""
	def __init__(self, err):
		pass


class Database(object):
	def __init__(self, db_hostname=None, db_username=None, db_password=None, db_dataname=None, print = False):
		if db_hostname:
			self.db_hostname = str(db_hostname)
		else:
			raise DBError("No hostname given.")
		
		if db_username:
			self.db_username = str(db_username)
		else:
			raise DBError("DB username is needed.")
	
		if db_password:
			self.db_password = str(db_password)
		else:
			raise DBError("DB password is needed.")
		
		if db_dataname:
			self.db_dataname = str(db_dataname)
		else:
			raise DBError("No database name is given.")
			
		self.print = print
		self.db_connection = None
		
	def connect(self):
		try:
			if self.print:
				print("Connecting to database with username: {}".format(self.db_username))
			self.db_connection = mariadb.connect(
				host=self.db_hostname,
				user=self.db_username,
				password=self.db_password,
				database=self.db_dataname)
			if self.print:
				print("Connected!")
		except Exception as e:
			print(str(e))
			exit(0)
			
	def close_connection(self):
		if self.print:
			print("Closing connection..")
		
		if self.db_connection is None:
			pass
		else:
			self.db_connection.close()
			self.db_connection = None
	
	def append_tx_log(self, date, nok_amount, foreign_amount, foreign_currency) -> int:
		out = 0
		
		if self.tx_exists(date, nok_amount, foreign_amount, foreign_currency):
			if self.print:
				print("Transaction already exists, not appending")
		else:
			if self.print:
				print("New transaction found, appending!")
			cursor = self.db_connection.cursor()
			
			try:
				query = "INSERT INTO Tx_Log (Date, NOK_Amount, Foreign_Amount, Foreign_Currency) VALUES(%s, %s, %s, %s)"
				cursor.execute(query, (date, nok_amount, foreign_amount, foreign_currency))
			except Exception as e:
				raise DBError(str(e))
			
			self.db_connection.commit()
			out = cursor.lastrowid
			
		return out
	
	def tx_exists(self, date, nok_amount, foreign_amount, foreign_currency):
		out = True
		cursor = self.db_connection.cursor()
		
		query = "SELECT Tx_ID FROM Tx_Log WHERE Date = %s AND NOK_Amount = %s AND Foreign_Amount = %s AND Foreign_Currency = %s LIMIT 0,1"
		cursor.execute(query, (date, nok_amount, foreign_amount, foreign_currency))
		
		# Check log files for duplicates
		if cursor.fetchone() is None:
			# return false if no match is found
			out = False
		return out
	
	def sale_exists(self, date, cost_base, proceeds, gains, foreign_amount, foreign_currency):
		out = True
		cursor = self.db_connection.cursor()
		
		query = "SELECT Sale_ID FROM Sale_Log WHERE Date = %s AND Cost_Base = %s AND Proceeds = %s AND Gains = %s AND Foreign_Amount = %s AND Foreign_Currency = %s LIMIT 0,1"
		cursor.execute(query, (date, cost_base, proceeds, gains, foreign_amount, foreign_currency))
		# Check db for duplicates
		if cursor.fetchone() is None:
			# return false if no match is found
			out = False
		return out
	
	def append_sales_log(self, date, cost_base, proceeds, gains, foreign_amount, foreign_currency) -> int:
		out = 0
		
		if self.sale_exists(date, cost_base, proceeds, gains, foreign_amount, foreign_currency):
			if self.print:
				print("Sale already exists, not appending")
		else:
			if self.print:
				print("New sale found, appending!")
			cursor = self.db_connection.cursor()
			
			try:
				query = "INSERT INTO Sale_Log (Date, Cost_Base, Proceeds, Gains, Foreign_Amount, Foreign_Currency) VALUES(%s, %s, %s, %s, %s, %s)"
				cursor.execute(query, (date, cost_base, proceeds, gains, foreign_amount, foreign_currency))
			except Exception as e:
				raise DBError(str(e))
			
			self.db_connection.commit()
			out = cursor.lastrowid
		return out
	
	def get_unprocessed_transactions(self):
		if self.print:
			print("Retrieving transactions not sent to Fiken...")
		cursor = self.db_connection.cursor(dictionary=True)
		cursor.execute("SELECT Tx_ID, Date, NOK_Amount, Foreign_Amount, Foreign_Currency FROM Tx_Log WHERE Processed = 0")
		
		return cursor.fetchall()

	def get_unprocessed_sales(self):
		if self.print:
			print("Retrieving sales not sent to Fiken...")
		cursor = self.db_connection.cursor(dictionary=True)
		cursor.execute("SELECT Sale_ID, Date, Cost_Base, Proceeds, Gains, Foreign_Amount, Foreign_Currency FROM Sale_Log WHERE Processed = 0")
		return cursor.fetchall()

	def process_sale(self, sale_id):
		var = 1
		cursor = self.db_connection.cursor()
		try:
			query = "UPDATE Sale_Log SET Processed=%s WHERE Sale_ID = %s"
			cursor.execute(query, (var, sale_id))
		except self.db_connection.Error as error:
			print("Error: {}".format(error))
			raise DBError(str(error))
		self.db_connection.commit()

	def process_transaction(self, tx_id):
		var = 1
		cursor = self.db_connection.cursor()
		try:
			query = "UPDATE Tx_Log SET Processed=%s WHERE Tx_ID=%s"
			cursor.execute(query, (var, tx_id))
		except Exception as e:
			print("Error: {}".format(e))
			raise DBError(str(e))
		self.db_connection.commit()


