from reportlab.lib.pagesizes import letter, landscape
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm
from reportlab.platypus import Table
from reportlab.lib import colors
import os
import pandas as pd
from datetime import datetime
from database import get_database_engine_e_eiis


def fetch_data_for_eom_inv(month, year, family_name):
    family_name_list = []
    new_dfs_list = []
    stock_value_total_list = []
    pur_value_total_list = []
    del_value_total_list = []
    cwh_value_total_list = []
    sav_value_total_list = []
    sav_per_value_total_list = []
    total_value_total_list = []
    try:
        engine = get_database_engine_e_eiis()
        df_query = """SELECT 
                        it.ITEM_ID AS ItemId,
                        it.ITEM_NAME AS ItemName,
                        it.PACKAGE_ID AS PackageId,
                        COALESCE(b.OP_QTY, 0) AS stock_qty,
                        COALESCE(b.OP_GP, 0) AS stock_value,
                        SUM(CASE WHEN ti.TRANS_TYPE = 'SD' THEN COALESCE(ti.QTY, 0) ELSE 0 END)+
                        SUM(CASE WHEN ti.TRANS_TYPE = 'LR' THEN COALESCE(ti.QTY, 0) ELSE 0 END) AS pur_qty,
                        SUM(CASE WHEN ti.TRANS_TYPE = 'SD' THEN COALESCE(ti.GP, 0) ELSE 0 END) +
                        SUM(CASE WHEN ti.TRANS_TYPE = 'LR' THEN COALESCE(ti.GP, 0) ELSE 0 END) AS pur_value,
                        SUM(CASE WHEN ti.TRANS_TYPE = 'LD' THEN COALESCE(ti.QTY, 0) ELSE 0 END) +  
                        SUM(CASE WHEN ti.TRANS_TYPE = 'SD' AND ti.TRAN_LOC_ID != (SELECT CWH FROM entityeiis) THEN COALESCE(ti.QTY, 0) ELSE 0 END) +
                        SUM(CASE WHEN ti.TRANS_TYPE = 'SR' THEN COALESCE(ti.QTY, 0) ELSE 0 END)  AS del_qty,
                        (SELECT COALESCE(SUM(QTY * STOCK_GP), 0)
                         FROM cwhdeldetail
                         WHERE ITEM_ID = ti.ITEM_ID AND CWH_DEL_ID IN 
                           (SELECT CWH_DEL_ID FROM cwhdelhead
                            WHERE MONTH(PERIOD) = MONTH(ti.PERIOD) AND YEAR(PERIOD) = YEAR(ti.PERIOD))
                        ) + 
                        SUM(CASE WHEN ti.TRANS_TYPE = 'SD' AND ti.TRAN_LOC_ID != (SELECT CWH FROM entityeiis) THEN COALESCE(ti.GP, 0) ELSE 0 END)+
                        SUM(CASE WHEN ti.TRANS_TYPE = 'SR' THEN COALESCE(ti.GP, 0) ELSE 0 END) AS del_value,
                        (((SELECT COALESCE(SUM(QTY * STOCK_GP), 0)
                         FROM cwhdeldetail
                         WHERE ITEM_ID = ti.ITEM_ID AND CWH_DEL_ID IN 
                           (SELECT CWH_DEL_ID FROM cwhdelhead
                            WHERE MONTH(PERIOD) = MONTH(ti.PERIOD) AND YEAR(PERIOD) = YEAR(ti.PERIOD))
                        ) + 
                        SUM(CASE WHEN ti.TRANS_TYPE = 'SD' AND ti.TRAN_LOC_ID != (SELECT CWH FROM entityeiis) THEN COALESCE(ti.GP, 0) ELSE 0 END)+
                        SUM(CASE WHEN ti.TRANS_TYPE = 'SR' THEN COALESCE(ti.GP, 0) ELSE 0 END)) / 
                        (SUM(CASE WHEN ti.TRANS_TYPE = 'LD' THEN COALESCE(ti.QTY, 0) ELSE 0 END) +  
                        SUM(CASE WHEN ti.TRANS_TYPE = 'SD' AND ti.TRAN_LOC_ID != (SELECT CWH FROM entityeiis) THEN COALESCE(ti.QTY, 0) ELSE 0 END) +
                        SUM(CASE WHEN ti.TRANS_TYPE = 'SR' THEN COALESCE(ti.QTY, 0) ELSE 0 END))) AS cost_up,
                        it.IP02 AS cwh_up 
                    FROM item it
                    LEFT JOIN Stock b ON b.ITEM_ID = it.ITEM_ID
                    LEFT JOIN traninter ti ON ti.ITEM_ID = it.ITEM_ID
                    WHERE MONTH(ti.PERIOD) = %s AND YEAR(ti.PERIOD) = %s
                    GROUP BY it.ITEM_ID;
                                        """
        df = pd.read_sql_query(df_query, engine, params=(month, year))
        # Multiply two columns and create a new column
        df['cwh_value'] = df['cwh_up'] * df['del_qty']
        df['sav'] = df['cwh_value'] - df['del_value']
        df['sav'] = df['cwh_value'] - df['del_value']
        df['sav_per'] = (df['sav'] / df['cwh_value']) * 100
        df['QTY_IN_HAND'] = df['stock_qty'] + df['pur_qty'] - df['del_qty']
        df['total_value'] = df['stock_value'] + df['pur_value'] - df['del_value']
        # Round all columns except the first three to 2 decimal places
        df.iloc[:, 3:] = df.iloc[:, 3:].round(3)
        # Query to fetch item categories
        df_query = """SELECT ITEM_CAT_PK, NAME FROM mst_item_category;"""
        dict_df = pd.read_sql_query(df_query, engine)

        # Initialize an empty dictionary
        family_names = {}

        # Populate dictionary with item categories
        for index, row in dict_df.iterrows():
            key = str(row['ITEM_CAT_PK'])
            value = row['NAME']
            family_names[key] = value

        print(family_names)

        # Add 'Family ID' column based on the first two characters of 'ITEM_ID'
        df['Family ID'] = df['ItemId'].astype(str).str[:2]
        # Convert column 'A' from int to str
        df['ItemId'] = df['ItemId'].astype(str)
        # Map 'Family ID' to 'Family Name' using the family_names dictionary
        df['Family Name'] = df['Family ID'].map(family_names)
        if len(family_name) != 0:
            # Filter the DataFrame to keep only rows with family_name 'fruits'
            df = df[df['Family Name'] == f'{family_name}']

        dfs_list = [df_group for _, df_group in df.groupby('Family ID')]
        for dfs in dfs_list:
            family_name_list.append(dfs.iloc[0, 17])
            print(dfs)
            dfs = dfs.drop(dfs.columns[17], axis=1)
            dfs = dfs.drop(dfs.columns[16], axis=1)
            stock_value_total_list.append((round(dfs['stock_value'].sum(), 3)))
            pur_value_total_list.append((round(dfs['pur_value'].sum(), 3)))
            del_value_total_list.append((round(dfs['del_value'].sum(), 3)))
            cwh_value_total_list.append((round(dfs['cwh_value'].sum(), 3)))
            sav_value_total_list.append((round(dfs['sav'].sum(), 3)))
            sav_per_value_total_list.append((round(dfs['sav_per'].sum(), 3)))
            total_value_total_list.append((round(dfs['total_value'].sum(), 3)))
            # Renaming columns
            dfs = dfs.rename(columns={
                'ItemId': 'Item ID',
                'ItemName': 'Item Name',
                'PackageId': 'Pack ID',
                'stock_qty': 'Qty',
                'stock_value': 'Val',
                'pur_qty': 'Qty',
                'pur_value': 'Val',
                'del_qty': 'Qty',
                'del_value': 'Cost Val',
                'cost_up': 'Cost UP',
                'cwh_up': 'CWH UP',
                'cwh_value': 'CWH Val',
                'sav': 'Sav',
                'sav_per': 'Sav Per',
                'QTY_IN_HAND': 'Qty in hand',
                'total_value': 'Tot. Val'
            })
            # Replace all NaN values with 0.000
            dfs.fillna(0.000, inplace=True)
            new_dfs_list.append(dfs)
        status = "success"
    except Exception as error:
        print('The cause of error', error)
        status = "failed"

    return (status, new_dfs_list, family_name_list, stock_value_total_list, pur_value_total_list, del_value_total_list,
            cwh_value_total_list, sav_value_total_list, sav_per_value_total_list, total_value_total_list)


def draw_header_and_details(c, width, height, formatted_date, person_name):
    left_margin = 12
    right_margin = width - 12
    top_margin = height - 12
    bottom_margin = 12

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
    third_element = f"EOM INVENTORY FOR THE PERIOD OF - {formatted_date}"
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

    return rect_y, top_margin, left_margin, bottom_margin, right_margin, rect_x, rect_height, rect_width


def create_eom_inv__pdf(new_dfs_list, family_name_list, formatted_date, stock_value_total_list, pur_value_total_list,
                        del_value_total_list,
                        cwh_value_total_list, sav_value_total_list, sav_per_value_total_list, total_value_total_list):
    file_name = None
    file_with_path = None
    try:
        person_name = "administrator"
        current_datetime = datetime.now()
        current_date_time_str = current_datetime.strftime("%Y_%m_%d_%H_%M_%S")
        file_name = f'EOM_INVENTORY_{current_date_time_str}.pdf'
        directory_path = r"C:\Users\Administrator\Downloads\eiis\EOM_INVENTORY"
        file_with_path = os.path.join(directory_path, file_name)

        # Create a canvas object with landscape orientation
        c = canvas.Canvas(file_with_path, pagesize=landscape(letter))
        width, height = landscape(letter)

        # Draw header and details
        rect_y, top_margin, left_margin, bottom_margin, right_margin, rect_x, rect_height, rect_width = draw_header_and_details(
            c, width, height, formatted_date, person_name)

        stock_val_inc = 0
        pur_val_inc = 0
        del_val_inc = 0
        cwh_val_inc = 0
        sav_val_inc = 0
        sav_per_val_inc = 0
        total_val_inc = 0
        # Known row height in points
        row_height = 20
        for index, (
                df, family_name_, stock_val, pur_val, del_val, cwh_val, sav_val, sav_per_val, total_val) in enumerate(
            zip(new_dfs_list,
                family_name_list,
                stock_value_total_list,
                pur_value_total_list,
                del_value_total_list,
                cwh_value_total_list,
                sav_value_total_list,
                sav_per_value_total_list,
                total_value_total_list)):

            # Calculate available vertical space
            available_space = rect_y - bottom_margin
            # Calculate the number of rows that can fit within the available space
            rows_per_chunk = int(available_space / row_height)
            # Adjust rows_per_chunk to account for not splitting header and data rows unevenly
            # If the header is always included and needs one row by itself:
            rows_per_chunk -= 1  # Adjust for the header if it is included in each chunk
            print('rows_per_chunk', rows_per_chunk)
            if available_space < 2.0 * cm:
                c.showPage()
                # Draw header and details
                rect_y, top_margin, left_margin, bottom_margin, right_margin, rect_x, rect_height, rect_width = draw_header_and_details(
                    c, width, height, formatted_date, person_name)
                # Calculate available vertical space
                available_space = rect_y - bottom_margin

                # Calculate the number of rows that can fit within the available space
                rows_per_chunk = int(available_space / row_height)

                # Adjust rows_per_chunk to account for not splitting header and data rows unevenly
                # If the header is always included and needs one row by itself:
                rows_per_chunk -= 1  # Adjust for the header if it is included in each chunk
                print('rows_per_chunk', rows_per_chunk)

            # New y position for the second rectangle (below the first one)
            second_rect_y = rect_y - 0.5 * cm  # Positioned below the first rectangle with 0.5 cm spacing
            rect_width = 27.1 * cm
            # Height for the second rectangle
            second_rect_height = 0.5 * cm

            # Draw the second rectangle
            c.rect(rect_x, second_rect_y, rect_width, second_rect_height, stroke=1, fill=0)
            # Manually specify positions for the vertical lines
            first_vertical_line_x = rect_x + 8.9 * cm  # Adjust this value as needed
            second_vertical_line_x = rect_x + 11.7 * cm  # Adjust this value as needed
            third_vertical_line_x = rect_x + 14.5 * cm
            fourth_vertical_line_x = rect_x + 21.5 * cm

            # Vertical lines start and end points
            vertical_line_start_y = second_rect_y
            vertical_line_end_y = second_rect_y + second_rect_height

            # Draw the first vertical line
            c.line(first_vertical_line_x, vertical_line_start_y, first_vertical_line_x, vertical_line_end_y)

            # Draw the second vertical line
            c.line(second_vertical_line_x, vertical_line_start_y, second_vertical_line_x, vertical_line_end_y)

            # Draw the Third vertical line
            c.line(third_vertical_line_x, vertical_line_start_y, third_vertical_line_x, vertical_line_end_y)

            # Draw the Third vertical line
            c.line(fourth_vertical_line_x, vertical_line_start_y, fourth_vertical_line_x, vertical_line_end_y)

            c.setFont("Helvetica-Bold", 7)
            stock_text = "Stock BOM"
            purchase_text = "Purchase"
            delivery_text = "Delivery"
            family_name = f"Family Name : {family_name_}"
            # Adjust these values to position the text inside the second rectangle
            family_name_text_x = rect_x + (first_vertical_line_x - left_margin) / 2 - c.stringWidth(family_name) / 2
            family_name_text_y = second_rect_y + 0.2 * cm  # Slightly above the bottom of the rectangle
            cost_text_x = first_vertical_line_x + (second_vertical_line_x - first_vertical_line_x) / 2 - c.stringWidth(
                stock_text) / 2
            cost_text_y = second_rect_y + 0.2 * cm  # Slightly above the bottom of the rectangle
            issue_text_x = second_vertical_line_x + (
                    third_vertical_line_x - second_vertical_line_x) / 2 - c.stringWidth(
                purchase_text) / 2
            issue_text_y = second_rect_y + 0.2 * cm  # Slightly above the bottom of the rectangle
            delivery_text_x = third_vertical_line_x + (
                    fourth_vertical_line_x - third_vertical_line_x) / 2 - c.stringWidth(
                delivery_text) / 2
            delivery_text_y = second_rect_y + 0.2 * cm  # Slightly above the bottom of the rectangle
            # Draw the text inside the second rectangle
            c.drawString(family_name_text_x, family_name_text_y, family_name)
            c.drawString(cost_text_x, cost_text_y, stock_text)
            c.drawString(issue_text_x, issue_text_y, purchase_text)
            c.drawString(delivery_text_x, delivery_text_y, delivery_text)

            first_half = df.iloc[:rows_per_chunk]
            second_half = df.iloc[rows_per_chunk:]
            table_y = second_rect_y  # Start the table below the second rectangle
            # print(table_y)

            df_data = first_half.values.tolist()
            df_headers = first_half.columns.tolist()

            # Create the table
            colWidths = [1.4 * cm, 5.1 * cm, 2.4 * cm, 1.4 * cm]

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
                ('FONTSIZE', (0, 0), (-1, -1), 5.5),  # Font size for all cells
                ('BOTTOMPADDING', (0, 0), (-1, -1), 0.1),  # Padding below text
                ('TOPPADDING', (0, 0), (-1, -1), 5),  # Padding above text
                ('GRID', (0, 0), (-1, -1), 1, colors.black),  # Grid lines
                ('TOPPADDING', (13, 0), (14, 0), -1),  # Remove top padding for header
                ('TOPPADDING', (13, 1), (14, -1), -1),  # Remove top padding for body
                ('GRID', (13, 0), (14, -1), 1, colors.black),  # Ensure grid for last two columns
            ])

            # Calculate the height of the table
            df_table_height = df_table.wrap(0, 0)[1]
            # row_height = df_table.wrap(0, 0)  # Get the height of the header and a single row
            print('df_table_height ---->', df_table_height)

            table_width = width - 2 * left_margin  # Width of the table
            df_table.wrapOn(c, table_width, height)  # Prepare the table for drawing
            next_y = table_y - df_table_height
            print('space after table -->', next_y)
            df_table.drawOn(c, left_margin, next_y)  # Position and draw the table

            # Move to the next page if there are more chunks
            if len(second_half) == 0:
                stock_val_inc += round(stock_val, 3)
                pur_val_inc += round(pur_val, 3)
                del_val_inc += round(del_val, 3)
                cwh_val_inc += round(cwh_val, 3)
                sav_val_inc += round(sav_val, 3)
                sav_per_val_inc += round(sav_per_val, 3)
                total_val_inc += round(total_val, 3)

                sav_per_val_per = round((sav_val / cwh_val) * 100, 3)
                # Draw "Closing :" and its value
                table_y = next_y - 0.5 * cm # Move down below the table
                print('df_table_height ------>', df_table_height)
                print('bottom_margin -------->', bottom_margin)
                # Calculate available vertical space
                available_space = df_table_height - bottom_margin

                # Convert available space from points to centimeters
                available_space_cm = available_space * 0.0352778

                print("available_space_cm", available_space_cm)
                if available_space < 1.0 * cm:
                    c.showPage()
                    (rect_y, top_margin, left_margin, bottom_margin, right_margin, rect_x, rect_height,
                     rect_width) = draw_header_and_details(c, width, height, formatted_date, person_name)
                    table_y = rect_y - 0.2 * cm

                c.setFont("Helvetica-Bold", 6)
                c.drawString(left_margin + 4 * cm, table_y, "Sub Total :")
                c.drawString(left_margin + 10.2 * cm, table_y, str(stock_val))
                c.drawString(left_margin + 12.9 * cm, table_y, str(pur_val))
                c.drawString(left_margin + 16.0 * cm, table_y, str(del_val))
                c.drawString(left_margin + 19.9 * cm, table_y, str(cwh_val))
                c.drawString(left_margin + 21.6 * cm, table_y, str(sav_val))
                c.drawString(left_margin + 23.2 * cm, table_y, str(sav_per_val_per))
                c.drawString(left_margin + 25.6 * cm, table_y, str(total_val))
                table_y = table_y - 5
                # Draw a horizontal line below the closing value
                line_start_x = left_margin + 4 * cm
                line_end_x = right_margin
                line_y = table_y  # Adjust the vertical position as needed
                c.line(line_start_x, line_y, line_end_x, line_y)
                rect_y = line_y - 0.2 * cm
                print('rect_y', rect_y)
            else:
                index += 1
                new_dfs_list.insert(index, second_half)
                family_name_list.insert(index, family_name_)
                stock_value_total_list.insert(index, stock_val)
                pur_value_total_list.insert(index, pur_val)
                del_value_total_list.insert(index, del_val)
                cwh_value_total_list.insert(index, cwh_val)
                sav_value_total_list.insert(index, sav_val)
                sav_per_value_total_list.insert(index, sav_per_val)
                total_value_total_list.insert(index, total_val)
                # Header and bottom details
                c.showPage()
                (rect_y, top_margin, left_margin, bottom_margin, right_margin, rect_x, rect_height,
                 rect_width) = draw_header_and_details(c, width, height, formatted_date, person_name)

            # print(rect_y)
        # print('rect_y', rect_y)
        # rect_y = rect_y + 0.5 * cm
        # Draw "Closing :" and its value
        available_space = rect_y - bottom_margin
        if available_space < 1.0 * cm:
            c.showPage()
            (rect_y, top_margin, left_margin, bottom_margin, right_margin, rect_x, rect_height,
             rect_width) = draw_header_and_details(c, width, height, formatted_date, person_name)
        table_y = rect_y - 0.7 * cm  # Move down below the table
        c.setFont("Helvetica-Bold", 6)
        stock_val_inc = round(stock_val_inc, 3)
        pur_val_inc = round(pur_val_inc, 3)
        del_val_inc = round(del_val_inc, 3)
        cwh_val_inc = round(cwh_val_inc, 3)
        sav_val_inc = round(sav_val_inc, 3)
        total_val_inc = round(total_val_inc, 3)
        sav_per_val_inc = round((sav_val_inc / cwh_val_inc) * 100, 3)
        c.drawString(left_margin + 4 * cm, table_y, "Grand Total :")
        c.drawString(left_margin + 10.2 * cm, table_y, str(stock_val_inc))
        c.drawString(left_margin + 12.9 * cm, table_y, str(pur_val_inc))
        c.drawString(left_margin + 16.0 * cm, table_y, str(del_val_inc))
        c.drawString(left_margin + 19.9 * cm, table_y, str(cwh_val_inc))
        c.drawString(left_margin + 21.6 * cm, table_y, str(sav_val_inc))
        c.drawString(left_margin + 23.2 * cm, table_y, str(sav_per_val_inc))
        c.drawString(left_margin + 25.6 * cm, table_y, str(total_val_inc))
        table_y -= 5
        # Draw a horizontal line below the closing value
        line_start_x = left_margin + 4 * cm
        line_end_x = right_margin
        line_y = table_y  # Adjust the vertical position as needed
        c.line(line_start_x, line_y, line_end_x, line_y)
        c.save()
        status = 'success'
        print(f'FILE SAVED IN THIS LOCATION ---> [{file_with_path}]')
    except Exception as error:
        print('The cause of error', error)
        status = 'failed'

    return status, file_name, file_with_path
