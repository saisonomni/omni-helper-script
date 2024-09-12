import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime


def connect_to_db(host, port, user, password, dbname):
    try:
        connection = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=dbname
        )
        print("Connected to the database successfully.")
        return connection
    except Exception as error:
        print("Error connecting to the database:", error)
        return None


def insert_data(df, connection):
    # Prepare the SQL query for batch insert
    query = """
    INSERT INTO "omni-lms".repayment_schedule (
        deleted, amount_due, as_of_date, charges_due, closing_principal,
        due_date, from_date, funding_allocation, installment_number,
        interest_days, interest_due, loan_id, opening_principal, partner_id,
        principal_due, product_id, product_partnership_id, seed_rps, timezone,
        principal_adjusted, customer_installment_number, pay_by_date,
        created_at, updated_at
    ) VALUES %s;
    """

    # Preprocess data for batch insert
    current_time_unix = int(datetime.now().timestamp() * 1000)  # Convert current time to Unix timestamp
    values = [
        (
            row['deleted'], row['amount_due'], row['as_of_date'], row['charges_due'],
            row['closing_principal'], row['due_date'], row['from_date'],
            row['funding_allocation'], row['installment_number'], row['interest_days'],
            row['interest_due'], row['loan_id'], row['opening_principal'],
            row['partner_id'], row['principal_due'], row['product_id'],
            row['product_partnership_id'], row['seed_rps'], row['timezone'],
            row['principal_adjusted'], row['customer_installment_number'],
            row['pay_by_date'], current_time_unix, current_time_unix
        )
        for _, row in df.iterrows()
    ]

    # Execute the batch insert
    try:
        with connection.cursor() as cursor:
            execute_values(cursor, query, values)
    except Exception as error:
        print("Error inserting data:", error)


def main():
    # Connection parameters
    host = "localhost"
    port = "6432"
    dbname = "postgres"
    user = ""
    password = ""

    print("Connection config verified?? (Press Enter)")
    _ = input()

    # Load DataFrame from CSV
    csv_file_path = '/Users/ramabhadraiah/Downloads/dev_rps.csv'
    df = pd.read_csv(csv_file_path)

    # Connect to the database
    connection = connect_to_db(host, port, user, password, dbname)

    if connection:
        try:
            # Insert data into the table
            insert_data(df, connection)

            # Commit changes
            print("Press Enter before committing:")
            _ = input()
            connection.commit()
        except Exception as error:
            print("Error during data insertion:", error)
            connection.rollback()  # Rollback in case of error
        finally:
            # Close the connection
            connection.close()
            print("Connection closed.")


if __name__ == "__main__":
    main()
