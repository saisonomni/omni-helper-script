import pandas as pd
import psycopg2


def connect_to_db(host, port, user, password, dbname):
    try:
        # Establish a connection to the PostgreSQL database
        connection = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=dbname
        )

        # Create a cursor object to interact with the database
        cursor = connection.cursor()
        print("Connected to the database successfully.")
        return connection, cursor

    except Exception as error:
        print("Error connecting to the database:", error)
        return None, None


def update_repayment_schedule(df, cursor):
    for _, row in df.iterrows():
        loan_id = str(row['loan_id'])
        installment_number = str(row['customer_installment_number'])

        # Create the SQL query to update the repayment schedule
        query = f"""
        UPDATE \"omni-lms\".repayment_schedule
        SET deleted = 1
        WHERE loan_id = %s
        AND customer_installment_number >= %s;
        """

        # Execute the query with parameters
        cursor.execute(query, (loan_id, installment_number))


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
    df = pd.read_csv('/Users/ramabhadraiah/Downloads/test - Sheet1.csv')

    # Connect to the database
    connection, cursor = connect_to_db(host, port, user, password, dbname)

    if connection and cursor:
        # Update repayment schedule based on DataaFrame
        update_repayment_schedule(df, cursor)

        # Commit changes
        print("Press Enter before committing : ")
        _ = input()
        connection.commit()

        # Close the cursor and connection
        cursor.close()
        connection.close()
        print("Connection closed.")


if __name__ == "__main__":
    main()
