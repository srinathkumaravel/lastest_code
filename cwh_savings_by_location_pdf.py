from reportlab.lib.pagesizes import letter, landscape
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm
from reportlab.platypus import Table
from reportlab.lib import colors
import os
import pandas as pd
from datetime import datetime
from database import get_database_engine_e_eiis


def fetch_cwh_sav_by_loc(month, year, location_id):
    cost_food_total = None
    cost_cleaning_total = None
    cost_disposal_total = None
    cost_others_total = None
    cost_total_total = None
    issue_food_total = None
    issue_cleaning_total = None
    issue_disposal_total = None
    issue_others_total = None
    issue_total_total = None
    issue_sav_total = None
    concatenated_df = None
    status = "failed"

    try:
        engine = get_database_engine_e_eiis()
        if len(location_id) == 0:
            sql_query = """SELECT 
                            ti.OUR_TRANS_IS,
                            ti.TRAN_LOC_NAME AS LocationName,
                            ti.TRANS_DATE,
                            item.ACCOUNT_NAME,
                            ROUND(SUM(ti.GP), 3) AS CP,
                            ROUND(SUM(ti.IP), 3) AS IP,
                            ROUND(ABS(SUM(ti.SAV)), 3) AS Sav,
                            ROUND(ABS(SUM(ti.SAV) / SUM(ti.IP)) * 100, 3) AS sav_per
                        FROM 
                            TranInter ti
                        INNER JOIN 
                            mst_item_account AS item ON item.ACCOUNT_ID = ti.ACCOUNT_ID 
                        WHERE 
                            ti.ENTITY_ID = 'OM01'
                            AND MONTH(ti.PERIOD) = %s
                            AND YEAR(ti.PERIOD) = %s
                            AND (ti.TRANS_TYPE = 'DD' OR ti.TRANS_TYPE = 'LD')
                        GROUP BY 
                            ti.TRAN_LOC_ID, ti.TRANS_TYPE, ti.OUR_TRANS_IS, ti.REPORT_GROUP, ti.TRANS_DATE, 
                            ti.TRAN_LOC_NAME;
    
                        """
            df = pd.read_sql_query(sql_query, engine, params=(month, year))
        else:
            sql_query = """
                            SELECT 
                                ti.OUR_TRANS_IS,
                                ti.TRAN_LOC_NAME AS LocationName,
                                ti.TRANS_DATE,
                                item.ACCOUNT_NAME,
                                ROUND(COALESCE(SUM(ti.GP), 0), 3) AS CP,
                                ROUND(COALESCE(SUM(ti.IP), 0), 3) AS IP,
                                ROUND(ABS(COALESCE(SUM(ti.SAV), 0)), 3) AS Sav,
                                ROUND(
                                    COALESCE(
                                        ABS(COALESCE(SUM(ti.SAV), 0) / NULLIF(COALESCE(SUM(ti.IP), 0), 0)), 
                                        0
                                    ) * 100, 3
                                ) AS sav_per
                            FROM 
                                TranInter ti
                            INNER JOIN 
                                mst_item_account AS item ON item.ACCOUNT_ID = ti.ACCOUNT_ID 
                            WHERE 
                                ti.ENTITY_ID = 'OM01'
                                AND MONTH(ti.PERIOD) = %s
                                AND YEAR(ti.PERIOD) = %s
                                AND ti.TRAN_LOC_ID = %s
                                AND (
                                    ti.TRAN_LOC_ID != (SELECT CWH FROM entityeiis) 
                                    OR ti.TRANS_TYPE = 'LD'
                                )
                            GROUP BY 
                                ti.TRAN_LOC_ID, ti.TRANS_TYPE, ti.OUR_TRANS_IS, ti.REPORT_GROUP, ti.TRANS_DATE, 
                                ti.TRAN_LOC_NAME;
                        """

            df = pd.read_sql_query(sql_query, engine, params=(month, year, location_id))
        if len(df) == 0:
            print('No data available')
            status = "success"
            message = "No data available"
            return (status, message, cost_food_total, cost_cleaning_total, cost_disposal_total, cost_others_total,
                    cost_total_total,
                    issue_food_total, issue_cleaning_total, issue_disposal_total, issue_others_total,
                    issue_total_total, issue_sav_total, concatenated_df)

        items_list = df['ACCOUNT_NAME'].tolist()
        total_value = df['CP'].tolist()
        total_value_IP = df['IP'].tolist()

        # Remove trailing whitespace from all elements in the list
        items_list = [item.rstrip() for item in items_list]
        num_rows = len(df)
        df.drop(columns=['ACCOUNT_NAME'], inplace=True)

        # Define the columns for the DataFrame
        columns = ['FOOD', 'CLEANING', 'DISPOSABLES', 'Others']

        # Create an empty DataFrame with specified columns
        df_1 = pd.DataFrame(columns=columns)

        # Check if the length of total_value matches the number of rows in the DataFrame
        if len(total_value) != num_rows:
            raise ValueError("Length of total_value must match the number of rows in the DataFrame.")

        # Assign total_value to the DataFrame based on items_list
        for idx, item in enumerate(items_list):
            if item in columns:
                df_1.loc[idx, item] = total_value[idx]
            else:
                # Assign to "Others" column if the item is not in the defined columns
                df_1.loc[idx, "Others"] = total_value[idx]

        # Define a dictionary with old names as keys and new names as values
        rename_dict = {
            'FOOD': 'Food',
            'CLEANING': 'Cleaning',
            'DISPOSABLES': 'Disposal',
            'Others': 'Others',
            'CP': 'Total'
        }

        # Rename the columns using the dictionary
        df_1.rename(columns=rename_dict, inplace=True)

        # Create an empty DataFrame with specified columns
        df_2 = pd.DataFrame(columns=columns)

        # Check if the length of total_value matches the number of rows in the DataFrame
        if len(total_value_IP) != num_rows:
            raise ValueError("Length of total_value must match the number of rows in the DataFrame.")

        # Assign total_value to the DataFrame based on items_list
        for idx, item in enumerate(items_list):
            if item in columns:
                df_2.loc[idx, item] = total_value_IP[idx]
            else:
                # Assign to "Others" column if the item is not in the defined columns
                df_2.loc[idx, "Others"] = total_value_IP[idx]

        # Define a dictionary with old names as keys and new names as values
        rename_dict = {
            'FOOD': 'Food ',
            'CLEANING': 'Cleaning ',
            'DISPOSABLES': 'Disposal ',
            'Others': 'Others ',
            'IP': 'Total '
        }

        # Rename the columns using the dictionary
        df_2.rename(columns=rename_dict, inplace=True)

        # Concatenate the two DataFrames horizontally (along the columns)
        concatenated_df = pd.concat([df, df_1, df_2], axis=1)

        # Define a dictionary with old names as keys and new names as values
        rename_dict = {
            'CP': 'Total',
            'IP': 'Total ',
            'OUR_TRANS_IS': 'Issue No',
            'LocationName': 'Location Name',
            'TRANS_DATE': 'Issue Date',
            'sav_per': 'Sav %'
        }

        # Rename the columns using the dictionary
        concatenated_df.rename(columns=rename_dict, inplace=True)

        # Replace NaN values with 0.00
        concatenated_df = concatenated_df.fillna(0.00).infer_objects()

        # Convert 'Issue Date' column to datetime format
        concatenated_df['Issue Date'] = pd.to_datetime(concatenated_df['Issue Date'])

        # Format 'Issue Date' column to 'dd-mmm-yy' format
        concatenated_df['Issue Date'] = concatenated_df['Issue Date'].dt.strftime('%d-%b-%y')

        # Rearrange columns
        new_order = ['Issue No', 'Location Name', 'Issue Date', 'Food', 'Cleaning', 'Disposal', 'Others', 'Total',
                     'Food ', 'Cleaning ', 'Disposal ', 'Others ', 'Total ', 'Sav', 'Sav %']

        # Reassign the DataFrame with columns in the new order
        concatenated_df = concatenated_df[new_order]

        # Calculate totals
        cost_food_total = round(concatenated_df['Food'].sum(), 2)
        cost_cleaning_total = round(concatenated_df['Cleaning'].sum(), 2)
        cost_disposal_total = round(concatenated_df['Disposal'].sum(), 2)
        cost_others_total = round(concatenated_df['Others'].sum(), 2)
        cost_total_total = round(concatenated_df['Total'].sum(), 2)
        issue_food_total = round(concatenated_df['Food '].sum(), 2)
        issue_cleaning_total = round(concatenated_df['Cleaning '].sum(), 2)
        issue_disposal_total = round(concatenated_df['Disposal '].sum(), 2)
        issue_others_total = round(concatenated_df['Others '].sum(), 2)
        issue_total_total = round(concatenated_df['Total '].sum(), 2)
        issue_sav_total = round(concatenated_df['Sav'].sum(), 2)

        status = "success"
        message = "success"

    except Exception as error:
        print('The cause of error -->', error)
        status = "failed"
        message = "success"

    finally:
        if concatenated_df is None:
            concatenated_df = pd.DataFrame()  # Ensure concatenated_df is not None

        return (status, message, cost_food_total, cost_cleaning_total, cost_disposal_total, cost_others_total, cost_total_total,
                issue_food_total, issue_cleaning_total, issue_disposal_total, issue_others_total,
                issue_total_total, issue_sav_total, concatenated_df)


def draw_header_and_details(c, width, height, formatted_date, person_name):
    left_margin = 18
    right_margin = width - 18
    top_margin = height - 18
    bottom_margin = 18

    c.setLineWidth(1)
    c.rect(left_margin, bottom_margin, right_margin - left_margin, top_margin - bottom_margin)

    # Rectangle details
    rect_x = left_margin  # Start from left margin
    rect_width = width - 2 * left_margin  # Maintain margins on both sides
    rect_height = 2.3 * cm
    rect_y = height - left_margin - rect_height  # Positioned from the top

    # Draw the first rectangle
    c.setLineWidth(1.3)  # Thicker border for the rectangle
    c.rect(rect_x, rect_y, rect_width, rect_height, stroke=1, fill=0)

    # Draw vertical line inside the first rectangle
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

    # List to be placed inside the first rectangle
    third_element = f"Savings By CWH Invoice with Location for the Period of - {formatted_date}"
    list1 = ["SOCAT LLC", "OMAN", third_element]

    # Calculate the width of the text
    text_width = c.stringWidth(third_element, "Helvetica-Bold", 12)

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
        text_width = c.stringWidth(text, "Helvetica-Bold", 10)
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

    # New y position for the second rectangle (below the first one)
    second_rect_y = rect_y - 0.5 * cm  # Positioned below the first rectangle with 0.5 cm spacing
    rect_width = 26.7 * cm
    # Height for the second rectangle
    second_rect_height = 0.5 * cm

    # Draw the second rectangle
    c.rect(rect_x, second_rect_y, rect_width, second_rect_height, stroke=1, fill=0)
    # Draw two vertical lines inside the second rectangle
    # Manually specify positions for the vertical lines
    first_vertical_line_x = rect_x + 8.7 * cm  # Adjust this value as needed
    second_vertical_line_x = rect_x + 16.2 * cm  # Adjust this value as needed
    third_vertical_line_x = rect_x + 23.7 * cm

    # Vertical lines start and end points
    vertical_line_start_y = second_rect_y
    vertical_line_end_y = second_rect_y + second_rect_height

    # Draw the first vertical line
    c.line(first_vertical_line_x, vertical_line_start_y, first_vertical_line_x, vertical_line_end_y)

    # Draw the second vertical line
    c.line(second_vertical_line_x, vertical_line_start_y, second_vertical_line_x, vertical_line_end_y)

    # Draw the Third vertical line
    c.line(third_vertical_line_x, vertical_line_start_y, third_vertical_line_x, vertical_line_end_y)

    c.setFont("Helvetica-Bold", 8)
    cost_text = "Cost"
    issue_text = "Issue"

    # Adjust these values to position the text inside the second rectangle
    cost_text_x = first_vertical_line_x + (second_vertical_line_x - first_vertical_line_x) / 2 - c.stringWidth(
        cost_text) / 2
    cost_text_y = second_rect_y + 0.2 * cm  # Slightly above the bottom of the rectangle
    issue_text_x = second_vertical_line_x + (third_vertical_line_x - second_vertical_line_x) / 2 - c.stringWidth(
        issue_text) / 2
    issue_text_y = second_rect_y + 0.2 * cm  # Slightly above the bottom of the rectangle

    # Draw the text inside the second rectangle
    c.drawString(cost_text_x, cost_text_y, cost_text)
    c.drawString(issue_text_x, issue_text_y, issue_text)

    return second_rect_y, top_margin, left_margin, bottom_margin, right_margin, rect_x, rect_height, rect_width


def create_cwh_sav_by_loc_pdf(concatenated_df, formatted_date, cost_food_total, cost_cleaning_total,
                              cost_disposal_total, cost_others_total, cost_total_total, issue_food_total,
                              issue_cleaning_total, issue_disposal_total, issue_others_total, issue_total_total,
                              issue_sav_total):
    file_name = None
    file_with_path = None
    try:
        person_name = "administrator"
        current_datetime = datetime.now()
        current_date_time_str = current_datetime.strftime("%Y_%m_%d_%H_%M_%S")
        file_name = f'CWH_SAVINGS_BY_LOCATION_{current_date_time_str}.pdf'
        directory_path = r"C:\Users\Administrator\Downloads\eiis\CWH_SAVINGS_BY_LOC"
        file_with_path = os.path.join(directory_path, file_name)

        # Create a canvas object with landscape orientation
        c = canvas.Canvas(file_with_path, pagesize=landscape(letter))
        width, height = landscape(letter)

        # Draw header and details
        (second_rect_y, top_margin, left_margin, bottom_margin, right_margin, rect_x,
         rect_height, rect_width) = draw_header_and_details(
            c, width, height, formatted_date, person_name)
        text_y = second_rect_y - 0.1
        # Known row height in points
        row_height = 18.19685

        # Calculate available vertical space
        available_space = text_y - bottom_margin

        # Calculate the number of rows that can fit within the available space
        rows_per_chunk = int(available_space / row_height)
        # Adjust rows_per_chunk to account for not splitting header and data rows unevenly
        # If the header is always included and needs one row by itself:
        rows_per_chunk -= 1  # Adjust for the header if it is included in each chunk
        print('rows_per_chunk', rows_per_chunk)

        chunks = [concatenated_df[i:i + rows_per_chunk] for i in range(0, len(concatenated_df), rows_per_chunk)]

        for i, chunk in enumerate(chunks):
            table_y = text_y  # Start the table below the second rectangle

            df_data = chunk.values.tolist()
            df_headers = chunk.columns.tolist()

            # Create the table
            colWidths = [2.2 * cm, 5.0 * cm, 1.5 * cm, 1.5 * cm]

            df_table = Table([df_headers] + df_data, colWidths=colWidths)
            # Styling the table

            # Assuming df_table is your TableStyle object
            df_table.setStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.white),  # Header background
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),  # Header text color
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),  # Center align text for all cells
                ('ALIGN', (0, 1), (1, -1), 'LEFT'),  # Left align text in the first and second columns
                ('ALIGN', (2, 1), (14, -1), 'RIGHT'),  # Right align text in the third, fourth, and fifth columns
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),  # Header font
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),  # Body font
                ('FONTSIZE', (0, 0), (-1, -1), 6.0),  # Font size for all cells
                ('BOTTOMPADDING', (0, 0), (-1, -1), 0.1),  # Padding below text
                ('TOPPADDING', (0, 0), (-1, -1), 5),  # Padding above text
                ('GRID', (0, 0), (-1, -1), 1, colors.black),  # Grid lines
                # Remove top border for last two columns (assuming these are column indices 13 and 14)
                ('TOPPADDING', (13, 0), (14, 0), -1),  # Remove top padding for header
                ('TOPPADDING', (13, 1), (14, -1), -1),  # Remove top padding for body
                ('GRID', (13, 0), (14, -1), 1, colors.black),  # Ensure grid for last two columns
            ])

            # Calculate the height of the table
            df_table_height = df_table.wrap(0, 0)[1]
            row_height = df_table.wrap(0, 0)  # Get the height of the header and a single row
            print('row_height---->', row_height)

            table_width = width - 2 * left_margin  # Width of the table
            df_table.wrapOn(c, table_width, height)  # Prepare the table for drawing
            df_table.drawOn(c, left_margin, table_y - df_table_height)  # Position and draw the table

            # Move to the next page if there are more chunks
            if i < len(chunks) - 1:
                # Header and bottom details
                c.showPage()
                (second_rect_y, top_margin, left_margin, bottom_margin, right_margin, rect_x, rect_height,
                 rect_width) = draw_header_and_details(
                    c, width, height, formatted_date, person_name)
                text_y = second_rect_y

            else:
                print('yes')
                # Draw "Closing :" and its value
                text_y -= df_table_height + 20  # Move down below the table

                c.setFont("Helvetica-Bold", 6)
                c.drawString(left_margin + 4 * cm, text_y, "Grand Total :")
                c.drawString(left_margin + 9.10 * cm, text_y, str(cost_food_total))
                c.drawString(left_margin + 10.6 * cm, text_y, str(cost_cleaning_total))
                c.drawString(left_margin + 12.3 * cm, text_y, str(cost_disposal_total))
                c.drawString(left_margin + 13.7 * cm, text_y, str(cost_others_total))
                c.drawString(left_margin + 15.2 * cm, text_y, str(cost_total_total))
                c.drawString(left_margin + 16.5 * cm, text_y, str(issue_food_total))
                c.drawString(left_margin + 18.2 * cm, text_y, str(issue_cleaning_total))
                c.drawString(left_margin + 19.8 * cm, text_y, str(issue_disposal_total))
                c.drawString(left_margin + 21.2 * cm, text_y, str(issue_others_total))
                c.drawString(left_margin + 22.5 * cm, text_y, str(issue_total_total))
                c.drawString(left_margin + 24.4 * cm, text_y, str(issue_sav_total))

                text_y -= 5
                # Draw a horizontal line below the closing value
                line_start_x = left_margin + 4 * cm
                line_end_x = right_margin
                line_y = text_y  # Adjust the vertical position as needed
                c.line(line_start_x, line_y, line_end_x, line_y)
        c.save()
        status = "success"
    except Exception as error:
        print('The Cause of error -->', error)
        status = "failed"

    return status, file_name, file_with_path
