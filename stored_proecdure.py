from datetime import datetime, timedelta
from database import get_database_connection_e_eiis
import time


def call_stored_procedure(stored_proc_name, formatted_date):
    conn = get_database_connection_e_eiis()
    try:
        with conn.cursor() as cursor:
            start_time = time.time()  # Record the start time
            print(f'{stored_proc_name} is executing ...')
            # Call the stored procedure
            cursor.callproc(stored_proc_name, (formatted_date,))
            conn.commit()
            end_time = time.time()  # Record the end time
            elapsed_time = end_time - start_time  # Calculate the elapsed time
            elapsed_minutes = elapsed_time / 60  # Convert elapsed time to minutes
            print(f'{stored_proc_name} is Completed in {elapsed_minutes:.2f} minutes')
    finally:
        conn.close()


def get_previous_day_date():
    # Get today's date
    today = datetime.today()

    # Get the previous day's date
    previous_day_data = today - timedelta(days=1)
    print(previous_day_data)

    # Replace the day with 01
    first_day_of_month = previous_day_data.replace(day=1)

    # Print the new date and return it as a string in the format 'YYYY-MM-DD'
    print(first_day_of_month)
    formatted_date = first_day_of_month.strftime('%Y-%m-%d')
    print(formatted_date)
    return formatted_date


formatted_date = get_previous_day_date()


def call_stored_procedure_1():
    call_stored_procedure('InsertPurchasePriceAnalysis', formatted_date)
    call_stored_procedure_2()  # Call the next stored procedure


def call_stored_procedure_2():
    call_stored_procedure('Insert_purchase_price_analysis_out_of_catalogue_item', formatted_date)
    call_stored_procedure_3()  # Call the next stored procedure


def call_stored_procedure_3():
    call_stored_procedure('Insert_purchase_price_analysis_cash_purchase', formatted_date)
