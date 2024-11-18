# import pandas as pd
# from datetime import datetime
# # import custom_config
# import mysql.connector  # For MySQL connection
#
# # SQL Database Configuration
# db_config = {
#     'user': 'root',
#     'password': 'actowiz',
#     'host': 'localhost',
#     'database': 'qcg'
# }
#
#
# # Create a SQL connection
# def create_sql_connection():
#     try:
#         connection = mysql.connector.connect(**db_config)
#         if connection.is_connected():
#             print("Connected to the database")
#         return connection
#     except mysql.connector.Error as e:
#         print(f"Error connecting to MySQL: {e}")
#         return None
#
#
# # Create month-wise table if it does not exist
# def create_table(cursor, table_name):
#     create_table_query = f"""
#     CREATE TABLE IF NOT EXISTS `{table_name}` (
#         `sheet_name` VARCHAR(255),
#         `sheet_count` INT,
#         `last_counted_on` VARCHAR(255)
#     );
#     """
#     try:
#         cursor.execute(create_table_query)
#         print(f"Table '{table_name}' is created or already exists.")
#     except Exception as e:
#         print(f'Error while creating Table {table_name}:', e)
#
#
# # Get last count date from the table
# def get_last_count_date(cursor, table_name, sheet_name):
#     select_query = f"SELECT last_counted_on FROM {table_name} WHERE sheet_name = %s"
#     cursor.execute(select_query, (sheet_name,))
#     last_count_date = cursor.fetchone()
#     return last_count_date[0] if last_count_date else 'N/A'  # Return 0 if no previous entry
#
#
# # Get previous count from the table
# def get_previous_count(cursor, table_name, sheet_name):
#     select_query = f"SELECT sheet_count FROM {table_name} WHERE sheet_name = %s"
#     cursor.execute(select_query, (sheet_name,))
#     result = cursor.fetchone()
#     return result[0] if result else 0  # Return 0 if no previous entry
#
#
# # Insert or update data in the month-wise table
# def insert_or_update_data(cursor, table_name, sheet_name, new_count):
#     current_date = datetime.now().date().strftime(format='%d-%m-%Y')  # DD-MM-YYYY
#     last_count_date = get_last_count_date(cursor, table_name, sheet_name)
#     if last_count_date != current_date:
#         # Check if the record exists
#         print('Counting not done today, hence performing...')
#         previous_count = get_previous_count(cursor, table_name, sheet_name)
#
#         if previous_count > 0:
#             # If the record exists, update the count
#             total_count = previous_count + new_count
#             update_query = f"""UPDATE `{table_name}` SET `sheet_count` = %s, `last_counted_on` = %s WHERE `sheet_name` = %s"""
#             try:
#                 cursor.execute(update_query, (total_count, current_date, sheet_name))
#                 print(f"Updated sheet '{sheet_name}' with total count: {total_count}")
#             except Exception as e:
#                 print('Error while Updating records count', e)
#
#         else:
#             # If no previous record exists, insert a new one
#             insert_query = f"""INSERT INTO {table_name} (`sheet_name`, `sheet_count`, `last_counted_on`) VALUES (%s, %s, %s)"""
#             try:
#                 cursor.execute(insert_query, (sheet_name, new_count, current_date))
#                 print(f"Inserted new sheet '{sheet_name}' with count: {new_count}")
#             except Exception as e:
#                 print('Error while Inserting records count', e)
#     else:
#         print('Counting already done today, hence not performing!!')
#
#
# # Main processing function
# def process_excel_and_store():
#     # delivery_date = custom_config.delivery_date
#     delivery_date = '20241118'
#     # Get current month and create a month-wise table name
#     current_month = datetime.now().strftime("%Y_%m")  # e.g., '2024_10'
#     table_name = f"sheet_data_count_{current_month}"
#
#     # Access database connection
#     connection = create_sql_connection()
#     if connection is None:
#         return  # Exit if database connection fails
#
#     cursor = connection.cursor()
#     try:
#         # Create month-wise table if not exists
#         create_table(cursor, table_name)
#
#         print('Reading Data from Excel file...')
#         # comp_fp = fr'Comp_Raw_Data_{delivery_date}.xlsx'
#         comp_fp = fr"C:\Users\jaimin.gurjar\Downloads\Comp_Raw_Data_{delivery_date}.xlsx"
#
#         # Read all sheets at once
#         df_all_sheets = pd.read_excel(comp_fp, engine='calamine', sheet_name=None)
#         print("Data read from Excel file Done")
#
#         sheets = list(df_all_sheets.keys())  # Getting sheet names
#         # Process each sheet
#         for sheet_name in sheets:
#             df_sheet = df_all_sheets[sheet_name]
#             sheet_count = len(df_sheet)
#             print(f"Rows in {sheet_name}: {sheet_count}")
#
#             # Insert or update data into SQL table
#             insert_or_update_data(cursor=cursor, table_name=table_name, sheet_name=sheet_name, new_count=sheet_count)
#
#         # Commit and close the connection
#         connection.commit()
#         cursor.close()
#         connection.close()
#         print("Data inserted/updated and connection closed.")
#
#     except Exception as e:
#         print(f"Error occurred: {e}")
#
#
# if __name__ == '__main__':
#     # Run the main function
#     process_excel_and_store()


import pymysql
import pandas as pd
from datetime import datetime
# import custom_config

# SQL Database Configuration
db_config = {
    'user': 'root',
    'password': 'actowiz',
    'host': 'localhost',
    'database': 'qcg'
}


# Create a SQL connection
def create_sql_connection():
    try:
        connection = pymysql.connect(user=db_config['user'], password=db_config['password'], host=db_config['host'], database=db_config['database'], autocommit=True)
        print("Connected to the database")
        return connection
    except pymysql.MySQLError as e:
        print(f"Error connecting to MySQL: {e}")
        return None


# Create month-wise table if it does not exist
def create_table(cursor, table_name):
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (`sheet_name` VARCHAR(255), `sheet_count` INT, `last_counted_on` VARCHAR(255));"""
    try:
        cursor.execute(create_table_query)
        print(f"Table '{table_name}' is created or already exists.")
    except Exception as e:
        print(f'Error while creating Table {table_name}:', e)


# Get last count date from the table
def get_last_count_date(cursor, table_name, sheet_name):
    select_query = f"SELECT last_counted_on FROM {table_name} WHERE sheet_name = %s"
    cursor.execute(select_query, (sheet_name,))
    last_count_date = cursor.fetchone()
    return last_count_date[0] if last_count_date else 'N/A'


# Get previous count from the table
def get_previous_count(cursor, table_name, sheet_name):
    select_query = f"SELECT sheet_count FROM {table_name} WHERE sheet_name = %s"
    cursor.execute(select_query, (sheet_name,))
    result = cursor.fetchone()
    return result[0] if result else 0


# Insert or update data in the month-wise table
def insert_or_update_data(cursor, table_name, sheet_name, new_count):
    current_date = datetime.now().date().strftime('%d-%m-%Y')  # DD-MM-YYYY
    last_count_date = get_last_count_date(cursor, table_name, sheet_name)
    # Check if the record already exists for today
    if last_count_date != current_date:
        print('Counting not done today, hence performing...')
        previous_count = get_previous_count(cursor, table_name, sheet_name)

        if previous_count > 0:
            # If the record exists, update the count
            total_count = previous_count + new_count
            update_query = f"""UPDATE `{table_name}` SET `sheet_count` = %s, `last_counted_on` = %s WHERE `sheet_name` = %s"""
            try:
                cursor.execute(update_query, (total_count, current_date, sheet_name))
                print(f"Updated sheet '{sheet_name}' with total count: {total_count}")
            except Exception as e:
                print('Error while Updating records count', e)
        else:
            # If no previous record exists, insert a new one
            insert_query = f"""INSERT INTO `{table_name}` (`sheet_name`, `sheet_count`, `last_counted_on`) VALUES (%s, %s, %s)"""
            try:
                cursor.execute(insert_query, (sheet_name, new_count, current_date))
                print(f"Inserted new sheet '{sheet_name}' with count: {new_count}")
            except Exception as e:
                print('Error while Inserting records count', e)
    else:
        print('Counting already done today, hence not performing!!')


# Main processing function
def process_excel_and_store():
    delivery_date = custom_config.delivery_date
    # Get current month and create a month-wise table name
    current_month = datetime.now().strftime("%Y_%m")  # e.g., '2024_10'
    table_name = f"sheet_data_count_{current_month}"

    connection = create_sql_connection()
    if connection is None:
        return  # Exit if database connection fails
    cursor = connection.cursor()
    try:
        create_table(cursor, table_name)
        print('Reading Data from Excel file...')
        comp_fp = fr'Comp_Raw_Data_{delivery_date}.xlsx'
        # comp_fp = fr"C:\Users\jaimin.gurjar\Downloads\Comp_Raw_Data_{delivery_date}.xlsx"

        # Read all sheets at once by Writing None in sheet_name argument
        df_all_sheets = pd.read_excel(comp_fp, engine='calamine', sheet_name=None)
        print("Data read from Excel file Done")

        sheets = list(df_all_sheets.keys())
        # Process each sheet
        for sheet_name in sheets:
            df_sheet = df_all_sheets[sheet_name]
            sheet_count = len(df_sheet)
            print(f"Rows in {sheet_name}: {sheet_count}")
            # Insert or update data into SQL table
            insert_or_update_data(cursor=cursor, table_name=table_name, sheet_name=sheet_name, new_count=sheet_count)

        # Close the connection
        cursor.close()
        connection.close()
        print("Data inserted/updated and connection closed.")

    except Exception as e:
        print(f"Error occurred: {e}")


if __name__ == '__main__':
    process_excel_and_store()  # Run the main function
