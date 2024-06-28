from reportlab.lib.pagesizes import letter, landscape
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm
from reportlab.platypus import Table
from reportlab.lib import colors
import os
import pandas as pd
from datetime import datetime
from database import get_database_engine_e_eiis


def fetch_data_for_purchase_price_analysis(from_month, to_month, supplier_id, item_id):
    # Convert the input strings to datetime objects
    start_date = datetime.strptime(from_month, '%Y-%m-%d')
    end_date = datetime.strptime(to_month, '%Y-%m-%d')

    # Initialize the list to store the result
    months_list = []

    # Loop through each month between the start and end date
    current_date = start_date
    while current_date <= end_date:
        months_list.append(current_date.strftime('%Y-%m-%d'))
        # Move to the next month
        next_month = current_date.month % 12 + 1
        next_year = current_date.year + (1 if current_date.month == 12 else 0)
        current_date = datetime(next_year, next_month, 1)

    # Print the result
    print("Months -->", months_list)
    try:
        if len(supplier_id) == 0 and len(item_id) == 0:
            status, combined_df = create_period_based_dataframe(months_list)
        elif len(item_id) == 0:
            status, combined_df = create_period_and_supplier_id_based_dataframe(months_list, supplier_id)
        elif len(supplier_id) == 0:
            status, combined_df = create_period_and_item_id_based_dataframe(months_list, item_id)
        else:
            status, combined_df = create_period_and_supplier_id_and_item_id_dataframe(months_list, supplier_id, item_id)

        if status == "failed":
            pivot_df = pd.DataFrame()
            message = "error"
        elif combined_df.empty:
            print('No data available for the selected filtered')
            print(combined_df)
            message = "No data available"
            pivot_df = pd.DataFrame()
        else:
            # Convert PERIOD to string format for consistency
            combined_df['PERIOD'] = combined_df['PERIOD'].astype(str)
            print(combined_df)
            # Reshape the DataFrame to have separate columns for each period's Qty, Unit_CP, and CessionID
            pivot_df = combined_df.pivot_table(
                index=['ITEM_ID', 'ITEM_NAME', 'PACKAGE_ID'],
                columns='PERIOD',
                values=['Qty', 'Unit_CP'],
                aggfunc='sum',
                fill_value=0  # Ensure missing values are filled with zeros
            )

            # Flatten the multi-level column index
            pivot_df.columns = ['_'.join(col).strip() for col in pivot_df.columns.values]
            pivot_df.reset_index(inplace=True)

            # Display the final DataFrame
            # print(pivot_df)

            # Function to convert column names
            def convert_column_name(column_name):
                if column_name.startswith('Qty'):
                    # Extract the date part after 'Qty_'
                    date_part = column_name.split('_')[-1]
                    # Convert date part to datetime and then to desired format
                    year_month = pd.to_datetime(date_part).strftime('%b-%y')
                    return f"Qty {year_month}"
                elif column_name.startswith('Unit_CP'):
                    # Extract the date part after 'Qty_'
                    date_part = column_name.split('_')[-1]
                    # Convert date part to datetime and then to desired format
                    year_month = pd.to_datetime(date_part).strftime('%b-%y')
                    return f"Unit CP {year_month}"
                else:
                    return column_name  # Return column name as is for non-Qty columns

            # Skip the first three columns and rename the rest
            columns_to_rename = pivot_df.columns[3:]
            pivot_df.rename(columns={col: convert_column_name(col) for col in columns_to_rename}, inplace=True)

            # Define a function to extract month and year from column names
            def extract_month_year(col):
                try:
                    return pd.to_datetime(col.split()[-1], format='%b-%y')
                except ValueError:
                    return pd.Timestamp.max  # Return maximum timestamp for unknown formats

            # Sort columns based on month and year
            sorted_columns = sorted(pivot_df.columns, key=extract_month_year, reverse=True)

            # Reorder columns in DataFrame
            pivot_df = pivot_df[sorted_columns]

            # print(pivot_df)
            # Rename individual columns
            pivot_df.rename(columns={'ITEM_ID': 'Item ID', 'ITEM_NAME': 'Item Name', 'PACKAGE_ID': 'PackageID'},
                            inplace=True)

            # Round off all data from the 4th column onward to 3 decimal places
            pivot_df.iloc[:, 3:] = pivot_df.iloc[:, 3:].round(3)
            # Select all columns after the first three
            columns_to_format = pivot_df.columns[3:]

            # Ensure the relevant columns are of float64 type
            pivot_df[columns_to_format] = pivot_df[columns_to_format].astype(float)

            # Apply the formatting using map
            pivot_df[columns_to_format] = pivot_df[columns_to_format].apply(lambda x: x.map(lambda y: f"{y:.3f}"))

            # Optionally convert the entire DataFrame to strings if needed
            pivot_df = pivot_df.astype(str)
            # Remove rows where the first column has the value '0.0.0'
            pivot_df = pivot_df[pivot_df['Item ID'] != '0.0.0']
            status = "success"
            message = "success"
    except Exception as error:
        print('Error in [fetch_data_for_purchase_price_analysis()] function')
        print('The cause of error -->', error)
        status = "failed"
        message = "No data available"
        pivot_df = pd.DataFrame()

    return status, pivot_df, message


def create_period_based_dataframe(months_list):
    try:
        engine = get_database_engine_e_eiis()
        # Define the query with placeholders for parameters
        df_query = """
                     SELECT 
                        detail.ITEM_ID,
                        i.ITEM_NAME,
                        detail.PACKAGE_ID,
                        head.PERIOD,
                        SUM(detail.QTY) AS Qty,
                        SUM(detail.ACTUAL_INV) AS Unit_CP
                    FROM 
                        suppdeldetail AS detail
                    INNER JOIN 
                        suppdelhead AS head ON head.GRN_ID = detail.GRN_ID
                    INNER JOIN 
                        item AS i ON i.ITEM_ID = detail.ITEM_ID
                    WHERE 
                        head.PERIOD = %s
                    GROUP BY 
                        detail.ITEM_ID, 
                        head.PERIOD;
                """

        # Initialize an empty DataFrame to store the combined results
        combined_df = pd.DataFrame()  # Initialize an empty DataFrame to store the combined results

        for period in months_list:
            period_df = pd.read_sql_query(df_query, engine, params=(period,))

            if period_df.empty:
                # Create a DataFrame with expected columns and a single row of initial data
                initial_data = {'ITEM_ID': ['0.0.0'], 'ITEM_NAME': ['0'], 'PACKAGE_ID': ['0'], 'PERIOD': [period],
                                'Qty': [0.0], 'Unit_CP': [0.0]}
                period_df = pd.DataFrame(initial_data)

            combined_df = pd.concat([combined_df, period_df], ignore_index=True)

        # Remove rows where ITEM_ID is '0.0.0' if it's the only unique ITEM_ID in the DataFrame
        if combined_df['ITEM_ID'].nunique() == 1 and combined_df['ITEM_ID'].iloc[0] == '0.0.0':
            combined_df = combined_df[combined_df['ITEM_ID'] != '0.0.0']

        status = "success"
    except Exception as error:
        print('Error in create_period_based_dataframe() function')
        print('The cause of error -->', error)
        # Create an empty DataFrame
        combined_df = pd.DataFrame()
        status = "failed"
    return status, combined_df


def create_period_and_supplier_id_based_dataframe(months_list, supplier_id):
    try:
        engine = get_database_engine_e_eiis()
        supplier_id_list = [supplier_id] * len(months_list)

        # Define the query with placeholders for parameters
        df_query = """
                     SELECT 
                        detail.ITEM_ID,
                        i.ITEM_NAME,
                        detail.PACKAGE_ID,
                        head.PERIOD,
                        SUM(detail.QTY) AS Qty,
                        SUM(detail.ACTUAL_INV) AS Unit_CP
                    FROM 
                        suppdeldetail AS detail
                    INNER JOIN 
                        suppdelhead AS head ON head.GRN_ID = detail.GRN_ID
                    INNER JOIN 
                        item AS i ON i.ITEM_ID = detail.ITEM_ID
                    WHERE 
                        head.SUPPLIER_ID = %s AND
                        head.PERIOD = %s
                    GROUP BY 
                        detail.ITEM_ID, 
                        head.PERIOD;
                """

        # Initialize an empty DataFrame to store the combined results
        combined_df = pd.DataFrame()

        # Execute the query and retrieve the data as a DataFrame for each period
        for period, supplier_id in zip(months_list, supplier_id_list):
            period_df = pd.read_sql_query(df_query, engine, params=(supplier_id, period))
            if period_df.empty:
                # Create a DataFrame with expected columns and a single row of initial data
                initial_data = {'ITEM_ID': ['0.0.0'], 'ITEM_NAME': ['0'], 'PACKAGE_ID': ['0'], 'PERIOD': [period],
                                'Qty': [0.0], 'Unit_CP': [0.0]}
                period_df = pd.DataFrame(initial_data)

            combined_df = pd.concat([combined_df, period_df], ignore_index=True)

        # Remove rows where ITEM_ID is '0.0.0' if it's the only unique ITEM_ID in the DataFrame
        if combined_df['ITEM_ID'].nunique() == 1 and combined_df['ITEM_ID'].iloc[0] == '0.0.0':
            combined_df = combined_df[combined_df['ITEM_ID'] != '0.0.0']

        status = "success"
    except Exception as error:
        print('Error in create_period_and_supplier_id_based_dataframe() function')
        print('The cause of error -->', error)
        combined_df = pd.DataFrame()
        status = "failed"
    return status, combined_df


def create_period_and_item_id_based_dataframe(months_list, item_id):
    try:
        engine = get_database_engine_e_eiis()
        item_id_list = [item_id] * len(months_list)

        # Define the query with placeholders for parameters
        df_query = """
                     SELECT 
                        detail.ITEM_ID,
                        i.ITEM_NAME,
                        detail.PACKAGE_ID,
                        head.PERIOD,
                        SUM(detail.QTY) AS Qty,
                        SUM(detail.ACTUAL_INV) AS Unit_CP
                    FROM 
                        suppdeldetail AS detail
                    INNER JOIN 
                        suppdelhead AS head ON head.GRN_ID = detail.GRN_ID
                    INNER JOIN 
                        item AS i ON i.ITEM_ID = detail.ITEM_ID
                    WHERE 

                        head.PERIOD = %s AND
                        detail.ITEM_ID = %s
                    GROUP BY 
                        detail.ITEM_ID, 
                        head.PERIOD;
                """

        # Initialize an empty DataFrame to store the combined results
        combined_df = pd.DataFrame()

        # Execute the query and retrieve the data as a DataFrame for each period
        for period, item_id in zip(months_list, item_id_list):
            period_df = pd.read_sql_query(df_query, engine, params=(period, item_id))
            if period_df.empty:
                # Create a DataFrame with expected columns and a single row of initial data
                initial_data = {'ITEM_ID': ['0.0.0'], 'ITEM_NAME': ['0'], 'PACKAGE_ID': ['0'], 'PERIOD': [period],
                                'Qty': [0.0], 'Unit_CP': [0.0]}
                period_df = pd.DataFrame(initial_data)

            combined_df = pd.concat([combined_df, period_df], ignore_index=True)

        # Remove rows where ITEM_ID is '0.0.0' if it's the only unique ITEM_ID in the DataFrame
        if combined_df['ITEM_ID'].nunique() == 1 and combined_df['ITEM_ID'].iloc[0] == '0.0.0':
            combined_df = combined_df[combined_df['ITEM_ID'] != '0.0.0']

        status = "success"
    except Exception as error:
        print('Error in create_period_and_item_id_based_dataframe() function')
        print('The cause of error -->', error)
        combined_df = pd.DataFrame()
        status = "failed"
    return status, combined_df


def create_period_and_supplier_id_and_item_id_dataframe(months_list, supplier_id, item_id):
    try:
        engine = get_database_engine_e_eiis()
        item_id_list = [item_id] * len(months_list)
        supplier_id_list = [supplier_id] * len(months_list)

        # Define the query with placeholders for parameters
        df_query = """
                     SELECT 
                        detail.ITEM_ID,
                        i.ITEM_NAME,
                        detail.PACKAGE_ID,
                        head.PERIOD,
                        SUM(detail.QTY) AS Qty,
                        SUM(detail.ACTUAL_INV) AS Unit_CP
                    FROM 
                        suppdeldetail AS detail
                    INNER JOIN 
                        suppdelhead AS head ON head.GRN_ID = detail.GRN_ID
                    INNER JOIN 
                        item AS i ON i.ITEM_ID = detail.ITEM_ID
                    WHERE 
                        head.SUPPLIER_ID = %s AND
                        head.PERIOD = %s AND
                        detail.ITEM_ID = %s
                    GROUP BY 
                        detail.ITEM_ID, 
                        head.PERIOD;
                """

        # Initialize an empty DataFrame to store the combined results
        combined_df = pd.DataFrame()

        # Execute the query and retrieve the data as a DataFrame for each period
        for period, item_id, supp_id in zip(months_list, item_id_list, supplier_id_list):
            period_df = pd.read_sql_query(df_query, engine, params=(supp_id, period, item_id))
            if period_df.empty:
                # Create a DataFrame with expected columns and a single row of initial data
                initial_data = {'ITEM_ID': ['0.0.0'], 'ITEM_NAME': ['0'], 'PACKAGE_ID': ['0'], 'PERIOD': [period],
                                'Qty': [0.0], 'Unit_CP': [0.0]}
                period_df = pd.DataFrame(initial_data)

            combined_df = pd.concat([combined_df, period_df], ignore_index=True)

        # Remove rows where ITEM_ID is '0.0.0' if it's the only unique ITEM_ID in the DataFrame
        if combined_df['ITEM_ID'].nunique() == 1 and combined_df['ITEM_ID'].iloc[0] == '0.0.0':
            combined_df = combined_df[combined_df['ITEM_ID'] != '0.0.0']

        status = "success"
    except Exception as error:
        print('Error in create_period_and_supplier_id_and_item_id_dataframe() function')
        print('The cause of error in -->', error)
        combined_df = pd.DataFrame()
        status = "failed"
    return status, combined_df


def create_header(c, width, height, person_name):
    left_margin = 18
    right_margin = width - 18
    top_margin = height - 18
    bottom_margin = 18

    c.setLineWidth(1)
    c.rect(left_margin, bottom_margin, right_margin - left_margin, top_margin - bottom_margin, )

    # Rectangle details
    rect_x = left_margin  # Start from left margin
    rect_width = width - 2 * left_margin  # Maintain margins on both sides
    rect_height = 2.3 * cm
    rect_y = height - left_margin - rect_height  # Positioned from the top
    # Draw the rectangle
    c.setLineWidth(1.3)  # Thicker border for the rectangle
    c.rect(rect_x, rect_y, rect_width, rect_height, stroke=1, fill=0)

    # Draw vertical line inside the rectangle
    vertical_line_x = rect_x + 3.5 * cm  # 3.5 cm from the left edge of the rectangle
    vertical_line_start_y = rect_y
    vertical_line_end_y = rect_y + rect_height

    c.setLineWidth(1)  # Line width for the vertical line
    c.line(vertical_line_x, vertical_line_start_y, vertical_line_x, vertical_line_end_y)

    # New image path
    image_path = 'C:\\Users\\Administrator\\Downloads\\eiis\\sodexo.jpg'

    # Image dimensions
    image_width = (vertical_line_x - rect_x) * 0.8  # Reduce the width by 20% for smaller size
    image_height = rect_height * 0.8  # Reduce the height by 20% for smaller size

    # Calculate image position to center it within the left box of the rectangle
    image_x = rect_x + (vertical_line_x - rect_x - image_width) / 2  # Center horizontally
    image_y = rect_y + (rect_height - image_height) / 2  # Center vertically

    # Draw the image inside the left box of the rectangle
    c.drawImage(image_path, image_x, image_y, width=image_width, height=image_height)
    third_elemnt = f"Purchase Price Analysis"
    # List to be placed inside the first rectangle
    list1 = ["SOCAT LLC", "OMAN", third_elemnt]

    # Calculate the width of the text
    text_width = c.stringWidth(third_elemnt, "Helvetica-Bold", 12)

    # Calculate the x position to center the text horizontally in the middle of the rectangle
    text_x = rect_x + (rect_width - text_width) / 2

    # Calculate the y position to center the text vertically in the middle of the rectangle
    text_y = rect_y + (rect_height - len(list1) * c._leading) / 2

    # Move the text slightly upwards
    text_y += (len(list1) - 1) * c._leading / 2  # Adjust this value as needed
    text_y += 0.6 * cm  # Move the text 0.2 cm upwards, adjust as needed

    # Font settings for the text
    c.setFont("Helvetica-Bold", 12)

    # Draw each text element one below the other
    for text in list1:
        text_width = c.stringWidth(text, "Helvetica-Bold", 12)
        text_x = rect_x + (rect_width - text_width) / 2
        c.drawString(text_x, text_y, text)
        text_y -= c._leading  # Move to the next line

    # Add "Generated by: {person_name}" on the left side
    generated_by_text = f"Generated by: {person_name}"
    generated_by_x = left_margin
    generated_by_y = bottom_margin - 0.4 * cm  # Adjust the position as needed
    c.setFont("Helvetica", 8)
    c.drawString(generated_by_x, generated_by_y, generated_by_text)

    # Add current date on the right side
    current_date = datetime.now().strftime("%B %d, %Y")
    current_date_text = f"{current_date}"
    current_date_x = right_margin - c.stringWidth(current_date_text, "Helvetica", 8)
    c.drawString(current_date_x, generated_by_y, current_date_text)

    return rect_x, rect_y, rect_width, rect_height, bottom_margin, left_margin, top_margin, right_margin


def create_purchase_price_analysis_pdf(pivot_df):
    person_name = "administrator"
    path = r'C:\Users\Administrator\Downloads\eiis\Purchase_price_analysis'
    current_time_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"PURCHASE_PRICE_ANALYSIS_{current_time_str}.pdf"
    file_path = os.path.join(path, file_name)
    print(file_path)
    try:
        # Create a canvas object with landscape orientation
        c = canvas.Canvas(file_path, pagesize=landscape(letter))
        width, height = landscape(letter)

        # Call function to create header and get dimensions
        rect_x, rect_y, rect_width, rect_height, bottom_margin, left_margin, top_margin, right_margin = create_header(c,
                                                                                                                      width,
                                                                                                                      height,
                                                                                                                      person_name)

        # Known row height in points
        row_height = 20

        # Calculate available vertical space
        available_space = rect_y - bottom_margin

        # Calculate the number of rows that can fit within the available space
        rows_per_chunk = int(available_space / row_height)

        # Adjust rows_per_chunk to account for not splitting header and data rows unevenly
        # If the header is always included and needs one row by itself:
        rows_per_chunk -= 1  # Adjust for the header if it is included in each chunk
        print('rows_per_chunk', rows_per_chunk)

        # Split DataFrame into chunks based on rows_per_chunk
        chunks = [pivot_df[i:i + rows_per_chunk] for i in range(0, len(pivot_df), rows_per_chunk)]

        for i, chunk in enumerate(chunks):
            table_y = rect_y  # Start the table below the second rectangle

            df_data = chunk.values.tolist()
            df_headers = chunk.columns.tolist()

            # Determine the number of columns in the table
            num_columns = len(df_headers)
            # Calculate the remaining width after margins
            remaining_width = right_margin - left_margin
            print(remaining_width)

            # Create the table
            colWidths = [2.0 * cm, 6.0 * cm, 3.5 * cm]

            # If there are more than 4 columns, calculate the width dynamically for the rest
            if num_columns > 3:
                dynamic_width = (remaining_width - sum(colWidths)) / (num_columns - 3)
                print(dynamic_width)
                colWidths.extend([dynamic_width] * (num_columns - 3))
                print(colWidths)

            df_table = Table([df_headers] + df_data, colWidths=colWidths)

            df_table.setStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.white),  # Header background
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),  # Header text color
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),  # Center align text for all cells
                ('ALIGN', (0, 1), (0, -1), 'CENTER'),  # Center align text in the first column
                ('ALIGN', (2, 1), (2, -1), 'CENTER'),  # Center align text in the third column
                ('ALIGN', (1, 1), (1, -1), 'LEFT'),  # Left align text in the second column
                ('ALIGN', (3, 1), (-1, -1), 'RIGHT'),  # Right align text in all columns after the third column
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),  # Header font
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),  # Body font
                ('FONTSIZE', (0, 0), (-1, -1), 6.0),  # Font size for all cells
                ('BOTTOMPADDING', (0, 0), (-1, -1), 0.1),  # Padding below text
                ('TOPPADDING', (0, 0), (-1, -1), 5),  # Padding above text
                ('GRID', (0, 0), (-1, -1), 1, colors.black)  # Grid lines
            ])

            # Calculate the height of the table
            df_table_height = df_table.wrap(0, 0)[1]

            table_width = width - 2 * left_margin  # Width of the table
            df_table.wrapOn(c, table_width, height)  # Prepare the table for drawing
            df_table.drawOn(c, left_margin, table_y - df_table_height)  # Position and draw the table

            # Move to the next page if there are more chunks
            if i < len(chunks) - 1:
                # Call function to draw header and details for subsequent pages
                c.showPage()
                # Call function to create header and get dimensions
                rect_x, rect_y, rect_width, rect_height, bottom_margin, left_margin, top_margin, right_margin = create_header(
                    c, width, height, person_name)
        c.save()
        status = "success"
    except Exception as error:
        print('Error while generating Purchase price PDF')
        print('The cause of error -->', error)
        status = "failed"
    return status, file_name, file_path
