import json

from FriPostering import FriPostering
from btctax import BtcTax
from db import Database
from fiken import Fiken

"""
- Mottatt FCT i wallet
	- NOK verdi av når det kom inn
	
- FCT til BTC
	- NOK verdi at BTC du fikk da du solgte - differansen er kapitalinntekt/tap
- BTC til EUR
	- Og NOK verdi av EUR når du solgte
"""

if __name__ == "__main__":
	
	# Initialisations
	###########################################################################################################
	# Load config file
	with open('conf.json') as json_data_file:
		config = json.load(json_data_file)
	
	# Init Bitcoin TAX
	btc_tax = BtcTax(
		username=config["BITOOINTAX_USERNAME"],
		password=config["BITCOINTAX_PASSWORD"],
		api_key=config["BITCOINTAX_API_KEY"],
		api_secret=config["BITCOINTAX_API_SECRET"],
		print=True)
	
	# Init DB
	db = Database(
		db_hostname=config["DB_HOSTNAME"],
		db_username=config["DB_USERNAME"],
		db_password=config["DB_PASSWORD"],
		db_dataname=config["DB_DATA_NAME"],
		print=True)
	
	# Init Fiken
	fiken = Fiken(
		user=config["FIKEN_USERNAME"],
		passwd=config["FIKEN_PASSWORD"],
		company_slug=config["FIKEN_COMPANY_SLUG"])
	###########################################################################################################
	
	# Get the data
	btcTax_data = btc_tax.get_data()

	# Connect to db
	db.connect()
	
	# Push data to DB retrieved from bitcoin.tax
	print("Total transactions: {}".format(len(btcTax_data["income"])))
	for row in btcTax_data["income"]:
		db.append_tx_log(
			row["Date Acquired"],
			row["Cost Basis"],
			row["Volume"],
			row["Symbol"])
	
	# Push data to DB retrieved from bitcoin.tax
	print("Total sales: {}".format(len(btcTax_data["sales"])))
	for row in btcTax_data["sales"]:
		db.append_sales_log(
			row["Date Sold"],
			row["Cost Basis"],
			row["Proceeds"],
			row["Gain"],
			row["Volume"],
			row["Symbol"])
		
	# INCOMING TRANSACTIONS
	###########################################################################################################
	
	# Retrieve unprocessed incoming transaction from database.
	unprocessed_transactions = db.get_unprocessed_transactions()

	# Loop through the transactions and fill the journal entries and lines to fit to fiken
	if unprocessed_transactions:
		# Create a journal entry for fiken
		postering = FriPostering(description="Import fra Bitcoin.tax")
		
		for row in unprocessed_transactions:
			description = "Inntekt - " + str(row["Foreign_Amount"]) + " " + str(row["Foreign_Currency"])
			date = row["Date"]
			entry = postering.addEntry(description, date)
			line = postering.addLine(index=entry,
									 debit_amount=row["NOK_Amount"],
									 debit_account=config["FIKEN_ANNEN_VALUTA"],
									 credit_account=config["FIKEN_FINANSINNTEKTSKONTO"],
									 vat_code="6")
		
		# Retrieve valid json fit to fiken.
		valid_json = postering.toJson()
		
		# Post entries to fiken
		headers = fiken.fri_postering(valid_json)
		
		# If success, then mark these transactions as processed at DB.
		if headers["Location"]:
			for row in unprocessed_transactions:
				db.process_transaction(int(row["Tx_ID"]))
				print("transaction with ID: {} has been processed, updating DB..".format(int(row["Tx_ID"])))
		
		
	else:
		print("No unprocessed transactions found.")
		
	# SALES
	###########################################################################################################
	
	# Retrieve unprocessed sales from database.
	unprocessed_sales = db.get_unprocessed_sales()
	# Loop through the transactions and fill the journal entries and lines to fit to fiken
	if unprocessed_sales:
		g_debit = None
		g_credit = None
		# Create a journal entry for fiken
		postering = FriPostering(description="Import fra Bitcoin.tax")
	
		for row in unprocessed_sales:
			description = "Salg - " + str(row["Foreign_Amount"]) + " " + str(row["Foreign_Currency"])
			date = row["Date"]
			entry = postering.addEntry(description, date)
			postering.addLine(index=entry,
							  debit_amount=row["Cost_Base"],
							  debit_account=config["FIKEN_KUNDEKONTO"],
							  credit_account=config["FIKEN_ANNEN_VALUTA"],
							  vat_code="6")
			
			if row["Gains"] >= 0:
				g_debit = config["FIKEN_KUNDEKONTO"]
				g_credit = config["FIKEN_AGIO_KONTO"]
			elif row["Gains"] < 0:
				g_debit = config["FIKEN_DISAGIO_KONTO"]
				g_credit = config["FIKEN_KUNDEKONTO"]
				
			postering.addLine(index=entry,
							  debit_amount=row["Gains"],
							  debit_account=g_debit,
							  credit_account=g_credit,
							  vat_code="6")
		
		# Retrieve valid json fit to fiken.
		valid_json = postering.toJson()
		
		# Post entries to fiken
		headers = fiken.fri_postering(valid_json)
		
		# If success, then mark these transactions as processed at DB.
		if headers["Location"]:
			for row in unprocessed_sales:
				db.process_sale(int(row["Sale_ID"]))
				print("Sale with ID: {} has been processed, updating DB..".format(int(row["Sale_ID"])))
		db.close_connection()
	
	else:
		print("No unprocessed sales found.")
		db.close_connection()
