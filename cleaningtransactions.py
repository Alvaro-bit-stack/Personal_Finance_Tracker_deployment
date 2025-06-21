import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
import psycopg2.extras
# Load environment variables from .env file
load_dotenv()
#CONFIG
CSV_PATH = os.path.abspath("C:/Users/alvar/OneDrive/Desktop/Personal_FInance_deploy/data/transactions.csv")



DB_CONFIG = {
    "host": os.getenv("host"),
    "database": os.getenv("dbname"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "port": os.getenv("port")
}

#Connecting to the database
conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()



#reading the file
df = pd.read_csv(CSV_PATH)
#Getting rid of columns that are not needed in the data
#axis = 1 means to operate on columns instead of rows
#axis = 0 would operate on rows
df = df.drop(['Check Number','Bank RTN','Transaction Type','Account Number'], axis=1)
#Adding new column that will determine income or expense
df["type"]  = df.apply(lambda row: "income" if pd.notnull(row["Credit"]) else "expense", axis = 1)
#Adding the two columns into one single amount column
#combine_first uses another columns values to fill in the null values of the caller
df["amount"] = df["Credit"].combine_first(df["Debit"])
df = df.drop(["Debit","Credit"],axis=1)


#Cleaning up data in order to match with Postgres table "transactions"
df = df.rename(columns={
    "Date": "date",
    "Account Running Balance": "current_bal",
    "Description": "description"
})
#saveing the changes to a new csv file, couldve done it in place but i want to keep the original and new seperated

CLEANED_CSV_PATH = os.path.abspath("C:/Users/alvar/OneDrive/Desktop/Personal_FInance_deploy/data/cleaned_temp.csv")
df.to_csv(CLEANED_CSV_PATH, index=False)

# Clear the table before inserting
cursor.execute("TRUNCATE transactions RESTART IDENTITY;")

# Insert using execute_values (faster than many INSERT statements)
insert_query = """
    INSERT INTO transactions (date, description, current_bal, type, amount)
    VALUES %s;
"""

# Convert DataFrame rows to list of tuples
records = df[["date", "description", "current_bal", "type", "amount"]].values.tolist()

# Execute batch insert
psycopg2.extras.execute_values(cursor, insert_query, records)
#Assigning categrories to the transactions
cursor.execute("""
    UPDATE transactions
    SET category_id = 1
    WHERE transaction_id in (
	SELECT transaction_id
	FROM transactions
	WHERE description ~* 'GROCERIES|FOOD|EATS|MARKET'AND category_id IS NULL);
""")
cursor.execute("""
    UPDATE transactions
    SET category_id = 2
    WHERE transaction_id in (
	SELECT transaction_id
	FROM transactions
	WHERE description ~* 'ZELLE|SUBSCRIPTION|MICROSOFT|SPOTIFY|EZPASS|AMAZON' AND category_id IS NULL)
""")
cursor.execute("""
    UPDATE transactions
    SET category_id = 4
    WHERE transaction_id in (
	SELECT transaction_id
	FROM transactions
	WHERE description ~* 'NJ|BUS|TRAIN|TICKET|TRANSPORTATION|SUBWAY|PATH|LUKOIL|GAS|PETROL|FUEL|MTA' AND category_id IS NULL);
""")
cursor.execute("""
    UPDATE transactions
    SET category_id = 5
    WHERE transaction_id in (
	SELECT transaction_id
	FROM transactions
	WHERE description ~* 'STEVENS|PAYROLL|DIRECT|DEPOSIT');""")
cursor.execute("""
    UPDATE transactions
    SET category_id = CASE
        WHEN description ~* 'CHILES|IKEA|HOBOKEN|UNIQUE|SHOPPING|CLOTHES|UNIQLO|RESTAURANT|DINING|BAR'  THEN 6
        ELSE 3
    END
	WHERE category_id IS NULL""")
conn.commit()
cursor.close()
conn.close()
print("Import and categorization complete.")
