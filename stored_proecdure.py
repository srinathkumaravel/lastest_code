from datetime import datetime
from database import get_database_connection_e_eiis, get_database_engine_e_eiis
import time
import subprocess
import pandas as pd
import pymysql
import os
from sqlalchemy import text
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import traceback


def call_purchase_price_stored_procedure(stored_proc_name, inner_formatted_date):
    conn = get_database_connection_e_eiis()
    try:
        with conn.cursor() as cursor:
            start_time = time.time()  # Record the start time
            print(f'{stored_proc_name} is executing ...')
            # Call the stored procedure
            cursor.callproc(stored_proc_name, (inner_formatted_date,))
            conn.commit()
            end_time = time.time()  # Record the end time
            elapsed_time = end_time - start_time  # Calculate the elapsed time
            elapsed_minutes = elapsed_time / 60  # Convert elapsed time to minutes
            print(f'{stored_proc_name} is Completed in {elapsed_minutes:.2f} minutes')
    finally:
        conn.close()


def call_inventory_stored_procedure(stored_proc_name, inner_formatted_date):
    conn = get_database_connection_e_eiis()
    try:
        with conn.cursor() as cursor:
            start_time = time.time()  # Record the start time
            print(f'{stored_proc_name} is executing ...')
            # Call the stored procedure
            cursor.callproc(stored_proc_name, (inner_formatted_date,))
            conn.commit()
            end_time = time.time()  # Record the end time
            elapsed_time = end_time - start_time  # Calculate the elapsed time
            elapsed_minutes = elapsed_time / 60  # Convert elapsed time to minutes
            print(f'{stored_proc_name} is Completed in {elapsed_minutes:.2f} minutes')
    finally:
        conn.close()


def sql_backup():
    try:
        # Database credentials
        db_host = 'localhost'  # Replace with your database host
        db_user = 'root'  # Replace with your database username
        db_password = 'root'  # Replace with your database password
        db_name = 'e_eiis'  # Replace with your database name

        # Backup directory and file paths
        backup_dir = r'C:\Users\Administrator\Downloads\eiis\SQL BACKUP'
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = f'{backup_dir}\\{db_name}_backup_{timestamp}.sql'

        # Path to mysqldump executable
        mysqldump_path = r'C:\Program Files (x86)\MySQL\MySQL Server 5.0\bin\mysqldump.exe'

        # Backup the complete database (including tables and data)
        db_backup_command = [
            mysqldump_path,
            "--host", db_host,
            "--user", db_user,
            f"--password={db_password}",
            db_name
        ]

        # Backup all stored procedures
        sp_backup_command = [
            mysqldump_path,
            "--host", db_host,
            "--user", db_user,
            f"--password={db_password}",
            db_name,
            "--routines",
            "--no-create-info",
            "--no-data",
            "--no-create-db",
            "--skip-triggers"
        ]

        # Perform database backup
        with open(backup_file, 'w') as output:
            print(f'Backing up database to {backup_file}')
            subprocess.run(db_backup_command, stdout=output, check=True)

            print(f'Backing up stored procedures to {backup_file}')
            # Append stored procedures to the same file
            subprocess.run(sp_backup_command, stdout=output, check=True)

        print(f'Backup successful! All data and stored procedures saved to {backup_file}')
        status = 'success'
    except Exception as error:
        print('The SQL FILE BACKUP ERROR IS BECAUSE OF --->', error)
        status = 'failed'
        backup_file = None

    return backup_file, status


def sql_table_name_row_count():
    # Establish the database connection
    conn = get_database_connection_e_eiis()

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    # Output Excel file path
    output_file = fr'C:\Users\Administrator\Downloads\eiis\EIIS_TABLE_NAME_AND_ROW_COUNT\EIIS_table_row_counts_{timestamp}.xlsx'

    try:
        with conn.cursor() as cursor:
            # Retrieve a list of tables in the database
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()

            # List to store table names and row counts
            table_data = []

            # Get row count for each table
            for table_name in tables:
                table_name = table_name[0]

                # Query to count the number of rows in the table
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                row_count = cursor.fetchone()[0]

                # Append the result to the list
                table_data.append({"Table Name": table_name, "Row Count": row_count})

            # Convert the list to a pandas DataFrame
            df = pd.DataFrame(table_data)

            # Save the DataFrame to an Excel file
            df.to_excel(output_file, index=False)

        print(f"Table names and row counts have been saved to '{output_file}'.")
        status = 'success'
    except pymysql.MySQLError as e:
        print(f"Error while interacting with the database: {e}")
        status = 'failed'

    finally:
        conn.close()

    return status, output_file


def get_previous_day_date():
    with get_database_connection_e_eiis() as conn:
        # Create a cursor object
        cursor = conn.cursor()

        # Define the SQL query to select data from entityeiis table
        sql_query = """
        SELECT STOCK_PERIOD FROM entityeiis;
        """

        # Execute the query
        cursor.execute(sql_query)

        # Fetch all rows from the result set
        stock_periods = cursor.fetchall()

        # Print the fetched data in YYYY-MM-DD format
        for row in stock_periods:
            stock_period = row[0]
            first_formatted_date = (stock_period.strftime('%Y-%m-%d'))
    return first_formatted_date


formatted_date = get_previous_day_date()


def call_stored_procedure_1():
    call_purchase_price_stored_procedure('InsertPurchasePriceAnalysis', formatted_date)
    call_stored_procedure_2()  # Call the next stored procedure


def call_stored_procedure_2():
    call_purchase_price_stored_procedure('Insert_purchase_price_analysis_out_of_catalogue_item', formatted_date)
    call_stored_procedure_3()  # Call the next stored procedure


def call_stored_procedure_3():
    call_purchase_price_stored_procedure('Insert_purchase_price_analysis_cash_purchase', formatted_date)
    call_stored_procedure_4()


def call_stored_procedure_4():
    call_inventory_stored_procedure('update_inventory', formatted_date)
    SQLBackupFunction()


def sql_backup():
    try:
        # Database credentials
        db_host = 'localhost'  # Replace with your database host
        db_user = 'root'  # Replace with your database username
        db_password = 'root'  # Replace with your database password
        db_name = 'e_eiis'  # Replace with your database name

        # Backup directory and file paths
        backup_dir = r'C:\Users\Administrator\Downloads\eiis\SQL BACKUP'
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = f'{backup_dir}\\{db_name}_backup_{timestamp}.sql'

        # Path to mysqldump executable
        mysqldump_path = r'C:\Program Files (x86)\MySQL\MySQL Server 5.0\bin\mysqldump.exe'

        # Backup the complete database (including tables and data)
        db_backup_command = [
            mysqldump_path,
            "--host", db_host,
            "--user", db_user,
            f"--password={db_password}",
            db_name
        ]

        # Backup all stored procedures
        sp_backup_command = [
            mysqldump_path,
            "--host", db_host,
            "--user", db_user,
            f"--password={db_password}",
            db_name,
            "--routines",
            "--no-create-info",
            "--no-data",
            "--no-create-db",
            "--skip-triggers"
        ]

        # Perform database backup
        with open(backup_file, 'w') as output:
            print(f'Backing up database to {backup_file}')
            subprocess.run(db_backup_command, stdout=output, check=True)

            print(f'Backing up stored procedures to {backup_file}')
            # Append stored procedures to the same file
            subprocess.run(sp_backup_command, stdout=output, check=True)

        print(f'Backup successful! All data and stored procedures saved to {backup_file}')
        status = 'success'
    except Exception as error:
        print('The SQL FILE BACKUP ERROR IS BECAUSE OF --->', error)
        status = 'failed'
        backup_file = None

    return backup_file, status


# Function for getting table row counts and saving to Excel
def sql_table_name_row_count():
    conn = get_database_connection_e_eiis()

    timestamp = datetime.now().strftime('%Y_%m_%d_%H%M%S')
    output_file = fr'C:\Users\Administrator\Downloads\eiis\EIIS_TABLE_NAME_AND_ROW_COUNT\EIIS_table_row_counts_{timestamp}.xlsx'

    try:
        with conn.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()

            table_data = []
            for table_name in tables:
                table_name = table_name[0]
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                row_count = cursor.fetchone()[0]
                table_data.append({"Table Name": table_name, "Row Count": row_count})

            df = pd.DataFrame(table_data)
            df.to_excel(output_file, index=False)

        print(f"Table names and row counts have been saved to '{output_file}'.")
        status = 'success'
    except pymysql.MySQLError as e:
        print(f"Error while interacting with the database: {e}")
        status = 'failed'
    finally:
        conn.close()

    return status, output_file


# Function for creating output folder
def create_output_folder(base_folder):
    date_folder = datetime.now().strftime('%Y%m%d')
    output_folder = os.path.join(base_folder, date_folder)
    os.makedirs(output_folder, exist_ok=True)
    return output_folder


# Function for exporting tables to CSV
def export_tables_to_csv(engine, output_folder):
    try:
        with engine.connect() as conn:
            tables = conn.execute(text("SHOW TABLES")).fetchall()

            for table_name in tables:
                table_name = table_name[0]
                output_file = os.path.join(output_folder, f"{table_name}.csv")
                query = f"SELECT * FROM {table_name}"
                df = pd.read_sql(query, conn)
                df.to_csv(output_file, index=False, sep=',')
                print(f"Table '{table_name}' has been saved to '{output_file}'.")

            status = 'success'
    except Exception as error:
        print('The cause of error -->', error)
        status = 'failed'
    return status, output_folder


# Function for CSV backup
def csv_file_backup():
    base_output_folder = r'C:\Users\Administrator\Downloads\eiis\CSV_BACKUP'
    engine = get_database_engine_e_eiis()
    output_folder = create_output_folder(base_output_folder)
    status, output_folder = export_tables_to_csv(engine, output_folder)
    print(f"All tables have been exported as CSV files to the folder '{output_folder}'.")
    return status, output_folder


# Function to send email
def send_email(subject, body, recipient_emails):
    smtp_server = 'smtp.gmail.com'
    smtp_port = 465
    smtp_user = 'srinath.k@esfita.com'
    smtp_password = 'tabbjsqwovtrshse'

    sender_email = smtp_user

    # Create the email
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ', '.join(recipient_emails)
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    server = None  # Initialize server to None

    try:
        server = smtplib.SMTP_SSL(smtp_server, smtp_port)
        server.login(smtp_user, smtp_password)
        server.sendmail(sender_email, recipient_emails, msg.as_string())
        print(f"{subject} email sent successfully to {', '.join(recipient_emails)}!")
    except Exception as e:
        print(f"Failed to send email: {e}")
    finally:
        if server:
            server.quit()


# Main SQL backup function to run all backup processes
def SQLBackupFunction():
    backup_file, backup_status = sql_backup()
    table_count_status, output_file = sql_table_name_row_count()
    csv_backup_status, output_folder = csv_file_backup()

    # Current date for the email header
    current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    email_body = "SQL Backup Summary:\n\n"
    if backup_status == 'success':
        email_body += f"SQL Backup: Success\nBackup File Location: {backup_file}\n\n"
    else:
        email_body += f"SQL Backup: Failed\nError: {backup_file}\n\n"

    if table_count_status == 'success':
        email_body += f"Table Row Count: Success\nOutput File Location: {output_file}\n\n"
    else:
        email_body += f"Table Row Count: Failed\nError: {output_file}\n\n"

    if csv_backup_status == 'success':
        email_body += f"CSV Backup: Success\nOutput Folder Location: {output_folder}\n\n"
    else:
        email_body += f"CSV Backup: Failed\nError: {output_folder}\n\n"

    # Add date to the email subject
    subject = f"SQL Backup Process Summary - {current_date}"

    # List of recipient emails
    recipient_emails = ['srinath.k@esfita.com']

    send_email(subject, email_body, recipient_emails)
