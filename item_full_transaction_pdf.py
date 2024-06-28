from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm
from reportlab.platypus import Table
from reportlab.lib import colors
import os
import pandas as pd
from datetime import datetime
from database import get_database_engine_e_eiis


def fetch_cwh_list(engine):
    """
    Fetches the CWH values from the entityeiis table and returns them as a list.

    Parameters:
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine object for database connection.

    Returns:
        list: List of CWH values from the entityeiis table.
    """
    # Query to fetch data from the entityeiis table
    query = 'SELECT CWH FROM entityeiis;'

    # Execute the query and store results in a DataFrame
    CWH_df = pd.read_sql(query, engine)

    # Convert the 'CWH' column to a list
    cwh_list = CWH_df['CWH'].tolist()

    return cwh_list


def fetch_datas_for_item_full_trans(month, year):
    item_id_list = []
    item_name_list = []
    packing_list = []
    opening_qty_list = []
    opening_cp_list = []
    new_dfs_list = []
    bal_qty_total_list = []
    bal_val_total_list = []
    try:
        engine = get_database_engine_e_eiis()
        df_query = """
                    SELECT 
                        t.SUPP_ID,
                        t.SUPP_NAME,
                        t.TRAN_LOC_ID,
                        t.TRAN_LOC_NAME,
                        t.TRANS_TYPE,
                        t.TRANS_DATE,
                        t.OUR_TRANS_IS AS InvoiceOrReturn,
                        t.QTY AS Qty,
                        CASE 
                            WHEN t.TRANS_TYPE = 'LD' THEN 
                                (SELECT COALESCE(SUM(QTY * STOCK_GP), 0) 
                                 FROM cwhdeldetail 
                                 WHERE ITEM_ID = t.ITEM_ID AND CWH_DEL_ID = t.OUR_TRANS_IS)
                            ELSE t.CP 
                        END AS CP,
                        s.OP_QTY AS OpeningQty,
                        s.OP_CP AS OpeningCP,
                        t.ITEM_ID,
                        t.PACKAGE_ID,
                        t.ITEM_NAME,
                        t.LAST_DATE
                    FROM 
                        traninter t
                    JOIN 
                        stock s ON t.ITEM_ID = s.ITEM_ID AND t.PACKAGE_ID = s.PACKAGE_ID
                    WHERE 
                        MONTH(t.TRANS_DATE) = %s
                        AND YEAR(t.TRANS_DATE) = %s
                    ORDER BY 
                        t.ITEM_ID;
                   """
        df = pd.read_sql_query(df_query, engine, params=(month, year))
        # Renaming columns
        df = df.rename(columns={
            'SUPP_ID': 'Sup ID/Loc ID',
            'SUPP_NAME': 'Sup Name/Loc Name',
            'TRANS_TYPE': 'Transaction',
            'TRANS_DATE': 'Date',
            'InvoiceOrReturn': 'Invoice #',
            'CP': 'Total CP',
            'BAL_QTY': 'Bal Qty',
            'BAL_VAL': 'Bal Val'
        })
        dfs_list = [df_group for _, df_group in df.groupby('ITEM_ID')]
        # Calling fetch_cwh_list() to get the cwh locations

        cwh_list = fetch_cwh_list(engine)
        item_id_list = []
        item_name_list = []
        packing_list = []
        opening_qty_list = []
        opening_cp_list = []
        new_dfs_list = []
        bal_qty_total_list = []
        bal_val_total_list = []

        for df in dfs_list:
            # Initialize the balance lists for the current DataFrame
            bal_qty = []
            bal_val = []
            initial_bal_qty = 0
            initial_bal_val = 0

            # Sort the DataFrame so that "SD" transactions come first
            df['Transaction'] = pd.Categorical(df['Transaction'], categories=['SD', 'LD', 'SR', 'LR'], ordered=True)
            # df = df.sort_values('Transaction').reset_index(drop=True)
            df = df.sort_values('LAST_DATE').reset_index(drop=True)
            # Drop the 'LAST_DATE' column
            df = df.drop(columns=['LAST_DATE'])

            # print(df)
            # Get the transaction details
            tran = df['Transaction'].tolist()
            qty = df['Qty'].tolist()
            tot_cp = df['Total CP'].tolist()
            location_id = df['TRAN_LOC_ID'].tolist()

            for index, (trans, quantity, total_cp, loc_id) in enumerate(zip(tran, qty, tot_cp, location_id)):

                if index == 0:
                    print(df)
                    initial_bal_qty = df.iloc[0, 9]
                    initial_bal_val = df.iloc[0, 10]
                    print(initial_bal_qty)
                if trans == "SD" and loc_id in cwh_list:
                    # print(initial_bal_qty)
                    initial_bal_qty += quantity
                    initial_bal_val += total_cp
                    bal_qty.append(round(initial_bal_qty, 3))
                    bal_val.append(round(initial_bal_val, 3))
                elif trans == "SD" and loc_id not in cwh_list:
                    bal_qty.append(round(initial_bal_qty, 3))
                    bal_val.append(round(initial_bal_val, 3))
                elif trans == "LR":
                    initial_bal_qty += quantity
                    initial_bal_val += total_cp
                    bal_qty.append(round(initial_bal_qty, 3))
                    bal_val.append(round(initial_bal_val, 3))
                elif trans == "LD":
                    initial_bal_qty -= quantity
                    initial_bal_val -= total_cp
                    bal_qty.append(round(initial_bal_qty, 3))
                    bal_val.append(round(initial_bal_val, 3))
                elif trans == "SR":
                    initial_bal_qty -= quantity
                    initial_bal_val -= total_cp
                    bal_qty.append(round(initial_bal_qty, 3))
                    bal_val.append(round(initial_bal_val, 3))
                else:
                    bal_qty.append(round(initial_bal_qty, 3))
                    bal_val.append(round(initial_bal_val, 3))

                # Add the balance columns to the current DataFrame
            df['Bal Qty'] = bal_qty
            df['Bal Val'] = bal_val
            # print(df)

            # Append details to the respective lists
            item_id_list.append(df.iloc[0, 11])
            item_name_list.append(df.iloc[0, 13])
            packing_list.append(df.iloc[0, 12])
            opening_qty_list.append(df.iloc[0, 9])
            opening_cp_list.append(df.iloc[0, 10])

            # Drop unwanted columns
            df.drop(columns=['ITEM_ID', 'PACKAGE_ID', 'ITEM_NAME', 'OpeningQty', 'OpeningCP', 'TRAN_LOC_ID',
                             'TRAN_LOC_NAME'], inplace=True)

            # Convert 'Date' column to datetime format and reformat
            df['Date'] = pd.to_datetime(df['Date'])
            df['Date'] = df['Date'].dt.strftime('%d-%b-%y')

            # Append totals to respective lists
            bal_qty_total_list.append(round(df['Bal Qty'].iloc[-1], 3))
            bal_val_total_list.append(round(df['Bal Val'].iloc[-1], 3))

            # Round the Qty column to 3 decimal places
            df['Qty'] = df['Qty'].round(3)
            df['Total CP'] = df['Total CP'].round(3)

            # Append the modified DataFrame to the new DataFrames list
            new_dfs_list.append(df)

        status = "success"
    except Exception as error:
        print('The cause of error -->', error)
        status = "failed"

    return (status, item_id_list, item_name_list, packing_list, opening_qty_list, opening_cp_list, new_dfs_list,
            bal_qty_total_list, bal_val_total_list)


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

    # List to be placed inside the first rectangle
    third_element = f"Items Full Transaction Details {formatted_date}"
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

    return rect_y, top_margin, left_margin, bottom_margin, right_margin, rect_x, rect_height, rect_width


def create_item_full_trans_pdf(new_dfs_list, item_id_list, item_name_list, packing_list, opening_qty_list,
                               formatted_date,
                               opening_cp_list, bal_qty_total_list, bal_val_total_list):
    file_name = None
    file_with_path = None
    try:
        person_name = "administrator"
        current_datetime = datetime.now()
        current_date_time_str = current_datetime.strftime("%Y_%m_%d_%H_%M_%S")
        file_name = f'ITEM_FULL_TRANS_{current_date_time_str}.pdf'
        directory_path = r"C:\Users\Administrator\Downloads\eiis\item_full_transaction"
        file_with_path = os.path.join(directory_path, file_name)

        # Create a canvas
        c = canvas.Canvas(file_with_path, pagesize=letter)
        width, height = letter  # Size of page (letter size)

        # Draw header and details
        rect_y, top_margin, left_margin, bottom_margin, right_margin, rect_x, rect_height, rect_width = draw_header_and_details(
            c, width, height, formatted_date, person_name)
        text_y = rect_y - 20
        for index, (df, item_id, item_name, package_id, opening_qty, opening_value, bal_qty, bal_val) in enumerate(
                zip(new_dfs_list, item_id_list, item_name_list, packing_list, opening_qty_list, opening_cp_list,
                    bal_qty_total_list, bal_val_total_list)):
            c.setFont("Helvetica-Bold", 8)  # Bold font for labels
            normal_font_size = 7  # Normal font size for values
            # Calculate available vertical space
            available_space = text_y - bottom_margin
            print('available_space', available_space)
            if available_space < 1.5 * cm:
                c.showPage()
                # Draw header and details
                rect_y, top_margin, left_margin, bottom_margin, right_margin, rect_x, rect_height, rect_width = draw_header_and_details(
                    c, width, height, formatted_date, person_name)
                text_y = rect_y - 20
            # Manually set x-coordinates
            x_item_id = left_margin + 2
            x_item_name = left_margin + 90
            x_package_id = left_margin + 255
            x_opening_qty = left_margin + 380

            # Draw labels in bold font
            c.setFont("Helvetica-Bold", 8)
            c.drawString(x_item_id, text_y, "Item ID :")
            c.drawString(x_item_name, text_y, "Item Name :")
            c.drawString(x_package_id, text_y, "Package ID :")
            c.drawString(x_opening_qty, text_y, "Opening Qty :")

            # Draw values in normal font
            c.setFont("Helvetica", normal_font_size)
            c.drawString(x_item_id + 33, text_y, str(item_id))
            c.drawString(x_item_name + 50, text_y, str(item_name))
            c.drawString(x_package_id + 51, text_y, str(package_id))
            c.drawString(x_opening_qty + 90, text_y, str(opening_qty))
            c.drawString(x_opening_qty + 150, text_y, str(opening_value))

            text_y -= 8  # Adjust as necessary based on your content height
            # Known row height in points
            row_height = 20.19685
            # Calculate available vertical space
            available_space = text_y - bottom_margin
            print('available_space', available_space)
            # Calculate the number of rows that can fit within the available space
            rows_per_chunk = int(available_space / row_height)
            rows_per_chunk -= 1  # Adjust for the header if it is included in each chunk
            if available_space < 1.5 * cm:
                c.showPage()
                # Draw header and details
                rect_y, top_margin, left_margin, bottom_margin, right_margin, rect_x, rect_height, rect_width = draw_header_and_details(
                    c, width, height, formatted_date, person_name)
                text_y = rect_y - 20
                available_space = (text_y - 3 * cm) - bottom_margin
                print('available_space', available_space)
                # Calculate the number of rows that can fit within the available space
                rows_per_chunk = int(available_space / row_height)
                rows_per_chunk -= 1  # Adjust for the header if it is included in each chunk

            # Split the DataFrame into the first half and the second half
            first_half = df.iloc[:rows_per_chunk]
            second_half = df.iloc[rows_per_chunk:]
            # print('Len of first DF', len(first_half))
            # print('Len of second DF', len(second_half))
            table_y = text_y
            df_data = first_half.values.tolist()
            df_headers = first_half.columns.tolist()

            # Create the table
            colWidths = [2.3 * cm, 4.6 * cm, 2.1 * cm, 1.7 * cm, 2.0 * cm, 1.9 * cm, 1.9 * cm]

            df_table = Table([df_headers] + df_data, colWidths=colWidths)
            # Styling the table

            # Styling the table
            df_table.setStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.white),  # Header background
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),  # Header text color
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),  # Center align text for all cells
                ('ALIGN', (1, 1), (1, -1), 'LEFT'),  # Left align text in the second column
                ('ALIGN', (2, 1), (2, -1), 'LEFT'),  # Left align text in the third column
                ('ALIGN', (5, 1), (5, -1), 'RIGHT'),  # Right align text in the sixth column (index 5)
                ('ALIGN', (6, 1), (6, -1), 'RIGHT'),  # Right align text in the seventh column (index 6)
                ('ALIGN', (7, 1), (7, -1), 'RIGHT'),
                ('ALIGN', (8, 1), (8, -1), 'RIGHT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),  # Header font
                ('FONTNAME', (1, 1), (-1, -1), 'Helvetica'),  # Body font
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
            if len(second_half) == 0:
                # Draw "Closing :" and its value
                text_y -= df_table_height + 20  # Move down below the table
                # Calculate available vertical space
                available_space = (table_y - df_table_height) - bottom_margin
                print('available_space', available_space)
                if available_space < 0.8 * cm:
                    c.showPage()
                    # Draw header and details
                    rect_y, top_margin, left_margin, bottom_margin, right_margin, rect_x, rect_height, rect_width = draw_header_and_details(
                        c, width, height, formatted_date, person_name)
                    text_y = rect_y - 20

                c.setFont("Helvetica-Bold", 8)
                c.drawString(x_item_id + 14.4 * cm, text_y, "Closing :")

                c.drawString(x_item_id + 16.8 * cm, text_y, str(bal_qty))
                c.drawString(x_item_id + 18.8 * cm, text_y, str(bal_val))
                text_y -= 20
                pass
                # Header and bottom details

            else:
                index += 1
                new_dfs_list.insert(index, second_half)
                item_id_list.insert(index, item_id)
                item_name_list.insert(index, item_name)
                packing_list.insert(index, package_id)
                opening_qty_list.insert(index, opening_qty)
                opening_cp_list.insert(index, opening_value)
                bal_qty_total_list.insert(index, bal_qty)
                bal_val_total_list.insert(index, bal_val)
                c.showPage()
                rect_y, top_margin, left_margin, bottom_margin, right_margin, rect_x, rect_height, rect_width = draw_header_and_details(
                    c, width, height, formatted_date, person_name)
                text_y = rect_y - 20

        c.save()
        status = "success"
    except Exception as Error:
        print('The cause of error -->', Error)
        status = "failed"
    return file_name, file_with_path, status
