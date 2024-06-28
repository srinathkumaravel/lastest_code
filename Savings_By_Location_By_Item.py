from reportlab.lib.pagesizes import letter, landscape
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm
from reportlab.platypus import Table
from reportlab.lib import colors
import os
import pandas as pd
from datetime import datetime
from database import get_database_engine_e_eiis


def fetch_data_for_sav_by_loc_by_item(month, year):
    cession_total_list: list = []
    purchase_total_list: list = []
    sav_total_list: list = []
    sav_per_total_list: list = []
    unique_LocationID = None
    unique_LocationName = None
    dfs = []
    cession_grand_total = 0
    purchase_grand_total = 0
    sav_grand_total = 0
    sav_per_grand_total = 0
    try:
        engine = get_database_engine_e_eiis()
        # Define the query with placeholders for parameters
        df_query = """
                    SELECT
                        ti.TRAN_LOC_ID AS LocationID, 
                        ti.TRAN_LOC_NAME AS LocationName,
                        ti.ITEM_ID,
                        it.Item_Name,
                        ti.PACKAGE_ID,
                        ti.QTY,
                        SUM(ti.IP) AS IP, 
                        SUM(ti.CP) AS CP, 
                        SUM(ti.SAV) AS SAV, 
                        (SUM(ti.SAV) / SUM(ti.IP) * 100) AS SAV_PER
                    FROM 
                        TranInter AS ti
                    INNER JOIN 
                        item AS it ON it.Item_ID = ti.ITEM_ID
                    WHERE 
                        ti.ENTITY_ID = 'OM01' 
                        AND MONTH(ti.PERIOD) = %s 
                        AND YEAR(ti.PERIOD) = %s
                        AND (ti.TRANS_TYPE = 'DD' OR ti.TRANS_TYPE = 'LD' OR ti.TRANS_TYPE = 'LR' OR ti.TRANS_TYPE = 'DR')
                    GROUP BY 
                        ti.ITEM_ID,
                        ti.SUPP_ID, 
                        ti.SUPP_NAME, 
                        ti.TRAN_LOC_ID, 
                        ti.TRAN_LOC_NAME
                    ORDER BY
                        ti.TRAN_LOC_ID;
                     """

        # Execute the query and retrieve the data as a DataFrame
        df = pd.read_sql_query(df_query, engine, params=(month, year))

        if len(df) == 0:
            status = "failed"
            print('status -->', status)
            print(f'No data available for the selected period month --> {month} & year --> {year}')
            return (unique_LocationID, unique_LocationName, dfs, cession_total_list, purchase_total_list,
                    sav_total_list, sav_per_total_list, cession_grand_total, purchase_grand_total, sav_grand_total,
                    sav_per_grand_total, status)

        # Get unique LocationIDs and LocationNames
        unique_LocationID = df['LocationID'].unique()
        unique_LocationName = df['LocationName'].unique()

        # Print unique LocationIDs and LocationNames
        print("unique_LocationID --->>", unique_LocationID)
        print("unique_LocationName --->>", unique_LocationName)

        # Get the full month name
        month_name = datetime(int(year), int(month), 1).strftime('%b')  # 'Jun'

        # Concatenate month and year with a hyphen
        period = f"{month_name}-{year}"  # 'Jun-2024'
        print(period)

        # Sort the DataFrame by 'LocationID'
        df_sorted = df.sort_values(by='LocationID')

        # Group by 'LocationID'
        grouped = df_sorted.groupby('LocationID')

        # List to hold the individual DataFrames
        dfs = []
        # Iterate over each group
        for location_id, group in grouped:
            dfs.append(group.reset_index(drop=True))

        # Print the individual DataFrames
        for df_group in dfs:
            # Sum a single column
            cession_total_list.append(round(df_group['IP'].sum(), 4))
            purchase_total_list.append(round(df_group['CP'].sum(), 4))
            sav_total_list.append(round(df_group['SAV'].sum(), 4))
            sav_per_total_list.append(round(df_group['SAV_PER'].sum(), 4))
            df_group.rename(
                columns={'ITEM_ID': 'Item ID', 'Item_Name': 'Item Name', 'PACKAGE_ID': 'Packing', 'QTY': 'Qantity',
                         'IP': 'Cession Value',
                         'CP': 'Purchase Value', 'SAV': "Savings", 'SAV_PER': "Savings per%"}, inplace=True)
            # Get the names of the first two columns
            columns_to_drop = df_group.columns[:2]

            # Drop the first two columns by their names
            df_group.drop(columns=columns_to_drop, inplace=True)
            print(df_group)
        print(cession_total_list)
        cession_grand_total = round(sum(cession_total_list), 4)
        purchase_grand_total = round(sum(purchase_total_list), 4)
        sav_grand_total = round(sum(sav_total_list), 4)
        sav_per_grand_total = round(sum(sav_per_total_list), 4)
        status = "success"
        print('status -->', status)
    except Exception as error:
        print('The cause of error -->', error)
        status = "failed"
        print('status -->', status)

    return (unique_LocationID, unique_LocationName, dfs, cession_total_list, purchase_total_list,
            sav_total_list, sav_per_total_list, cession_grand_total, purchase_grand_total, sav_grand_total,
            sav_per_grand_total, status)


def fetch_data_for_sav_by_loc_by_item_by_ind_location_id(month, year, location_id):
    cession_total_list: list = []
    purchase_total_list: list = []
    sav_total_list: list = []
    sav_per_total_list: list = []
    unique_LocationID = None
    unique_LocationName = None
    dfs = []
    cession_grand_total = 0
    purchase_grand_total = 0
    sav_grand_total = 0
    sav_per_grand_total = 0
    try:
        engine = get_database_engine_e_eiis()
        # Define the query with placeholders for parameters
        df_query = """
                    SELECT
                        ti.TRAN_LOC_ID AS LocationID, 
                        ti.TRAN_LOC_NAME AS LocationName,
                        ti.ITEM_ID,
                        it.Item_Name,
                        ti.PACKAGE_ID,
                        ti.QTY,
                        SUM(ti.IP) AS IP, 
                        SUM(ti.CP) AS CP, 
                        SUM(ti.SAV) AS SAV, 
                        (SUM(ti.SAV) / SUM(ti.IP) * 100) AS SAV_PER
                    FROM 
                        TranInter AS ti
                    INNER JOIN 
                        item AS it ON it.Item_ID = ti.ITEM_ID
                    WHERE 
                        ti.ENTITY_ID = 'OM01' 
                        AND MONTH(ti.PERIOD) = %s 
                        AND YEAR(ti.PERIOD) = %s
                        AND ti.TRAN_LOC_ID = %s
                        AND (ti.TRANS_TYPE = 'DD' OR ti.TRANS_TYPE = 'LD' OR ti.TRANS_TYPE = 'LR' OR ti.TRANS_TYPE = 'DR')
                    GROUP BY 
                        ti.ITEM_ID,
                        ti.SUPP_ID, 
                        ti.SUPP_NAME, 
                        ti.TRAN_LOC_ID, 
                        ti.TRAN_LOC_NAME
                    ORDER BY
                        ti.TRAN_LOC_ID;
                     """

        # Execute the query and retrieve the data as a DataFrame
        df = pd.read_sql_query(df_query, engine, params=(month, year, location_id))

        if len(df) == 0:
            status = "failed"
            print('status -->', status)
            print(
                f'No data available for the selected period month --> {month} & year --> {year} & location id {location_id}')
            return (unique_LocationID, unique_LocationName, dfs, cession_total_list, purchase_total_list,
                    sav_total_list, sav_per_total_list, cession_grand_total, purchase_grand_total, sav_grand_total,
                    sav_per_grand_total, status)

        # Get unique LocationIDs and LocationNames
        unique_LocationID = df['LocationID'].unique()
        unique_LocationName = df['LocationName'].unique()

        # Print unique LocationIDs and LocationNames
        print("unique_LocationID --->>", unique_LocationID)
        print("unique_LocationName --->>", unique_LocationName)

        # Get the full month name
        month_name = datetime(int(year), int(month), 1).strftime('%b')  # 'Jun'

        # Concatenate month and year with a hyphen
        period = f"{month_name}-{year}"  # 'Jun-2024'
        print(period)

        # Sort the DataFrame by 'LocationID'
        df_sorted = df.sort_values(by='LocationID')

        # Group by 'LocationID'
        grouped = df_sorted.groupby('LocationID')

        # List to hold the individual DataFrames
        dfs = []
        # Iterate over each group
        for location_id, group in grouped:
            dfs.append(group.reset_index(drop=True))

        # Print the individual DataFrames
        for df_group in dfs:
            # Sum a single column
            cession_total_list.append(round(df_group['IP'].sum(), 4))
            purchase_total_list.append(round(df_group['CP'].sum(), 4))
            sav_total_list.append(round(df_group['SAV'].sum(), 4))
            sav_per_total_list.append(round(df_group['SAV_PER'].sum(), 4))
            df_group.rename(
                columns={'ITEM_ID': 'Item ID', 'Item_Name': 'Item Name', 'PACKAGE_ID': 'Packing', 'QTY': 'Qantity',
                         'IP': 'Cession Value',
                         'CP': 'Purchase Value', 'SAV': "Savings", 'SAV_PER': "Savings per%"}, inplace=True)
            # Get the names of the first two columns
            columns_to_drop = df_group.columns[:2]

            # Drop the first two columns by their names
            df_group.drop(columns=columns_to_drop, inplace=True)
            print(df_group)
        print(cession_total_list)
        cession_grand_total = round(sum(cession_total_list), 4)
        purchase_grand_total = round(sum(purchase_total_list), 4)
        sav_grand_total = round(sum(sav_total_list), 4)
        sav_per_grand_total = round(sum(sav_per_total_list), 4)
        status = "success"
        print('status -->', status)
    except Exception as error:
        print('The cause of error -->', error)
        status = "failed"
        print('status -->', status)

    return (unique_LocationID, unique_LocationName, dfs, cession_total_list, purchase_total_list,
            sav_total_list, sav_per_total_list, cession_grand_total, purchase_grand_total, sav_grand_total,
            sav_per_grand_total, status)


def fetch_data_for_sav_by_loc_by_item_by_ind_location_id_and_date(from_date, to_date, location_id):
    cession_total_list: list = []
    purchase_total_list: list = []
    sav_total_list: list = []
    sav_per_total_list: list = []
    unique_LocationID = None
    unique_LocationName = None
    dfs = []
    cession_grand_total = 0
    purchase_grand_total = 0
    sav_grand_total = 0
    sav_per_grand_total = 0
    try:
        engine = get_database_engine_e_eiis()
        # Define the query with placeholders for parameters
        df_query = """
                    SELECT
                        ti.TRAN_LOC_ID AS LocationID, 
                        ti.TRAN_LOC_NAME AS LocationName,
                        ti.ITEM_ID,
                        it.Item_Name,
                        ti.PACKAGE_ID,
                        ti.QTY,
                        SUM(ti.IP) AS IP, 
                        SUM(ti.CP) AS CP, 
                        SUM(ti.SAV) AS SAV, 
                        (SUM(ti.SAV) / SUM(ti.IP) * 100) AS SAV_PER
                    FROM 
                        TranInter AS ti
                    INNER JOIN 
                        item AS it ON it.Item_ID = ti.ITEM_ID
                    WHERE 
                        ti.ENTITY_ID = 'OM01' 
                        AND ti.TRAN_LOC_ID = %s
                        AND ti.TRANS_DATE BETWEEN %s AND %s
                        AND (ti.TRANS_TYPE = 'DD' OR ti.TRANS_TYPE = 'LD' OR ti.TRANS_TYPE = 'LR' OR ti.TRANS_TYPE = 'DR')
                    GROUP BY 
                        ti.ITEM_ID,
                        ti.SUPP_ID, 
                        ti.SUPP_NAME, 
                        ti.TRAN_LOC_ID, 
                        ti.TRAN_LOC_NAME
                    ORDER BY
                        ti.TRAN_LOC_ID;
                     """

        # Execute the query and retrieve the data as a DataFrame
        df = pd.read_sql_query(df_query, engine, params=(location_id, from_date, to_date))

        if len(df) == 0:
            status = "failed"
            print('status -->', status)
            print(
                f'No data available for the selected period from_date --> {from_date} & to_date --> {to_date} & location id {location_id}')
            return (unique_LocationID, unique_LocationName, dfs, cession_total_list, purchase_total_list,
                    sav_total_list, sav_per_total_list, cession_grand_total, purchase_grand_total, sav_grand_total,
                    sav_per_grand_total, status)

        # Get unique LocationIDs and LocationNames
        unique_LocationID = df['LocationID'].unique()
        unique_LocationName = df['LocationName'].unique()

        # Print unique LocationIDs and LocationNames
        print("unique_LocationID --->>", unique_LocationID)
        print("unique_LocationName --->>", unique_LocationName)



        # Sort the DataFrame by 'LocationID'
        df_sorted = df.sort_values(by='LocationID')

        # Group by 'LocationID'
        grouped = df_sorted.groupby('LocationID')

        # List to hold the individual DataFrames
        dfs = []
        # Iterate over each group
        for location_id, group in grouped:
            dfs.append(group.reset_index(drop=True))

        # Print the individual DataFrames
        for df_group in dfs:
            # Sum a single column
            cession_total_list.append(round(df_group['IP'].sum(), 4))
            purchase_total_list.append(round(df_group['CP'].sum(), 4))
            sav_total_list.append(round(df_group['SAV'].sum(), 4))
            sav_per_total_list.append(round(df_group['SAV_PER'].sum(), 4))
            df_group.rename(
                columns={'ITEM_ID': 'Item ID', 'Item_Name': 'Item Name', 'PACKAGE_ID': 'Packing', 'QTY': 'Qantity',
                         'IP': 'Cession Value',
                         'CP': 'Purchase Value', 'SAV': "Savings", 'SAV_PER': "Savings per%"}, inplace=True)
            # Get the names of the first two columns
            columns_to_drop = df_group.columns[:2]

            # Drop the first two columns by their names
            df_group.drop(columns=columns_to_drop, inplace=True)
            print(df_group)
        print(cession_total_list)
        cession_grand_total = round(sum(cession_total_list), 4)
        purchase_grand_total = round(sum(purchase_total_list), 4)
        sav_grand_total = round(sum(sav_total_list), 4)
        sav_per_grand_total = round(sum(sav_per_total_list), 4)
        status = "success"
        print('status -->', status)
    except Exception as error:
        print('The cause of error -->', error)
        status = "failed"
        print('status -->', status)

    return (unique_LocationID, unique_LocationName, dfs, cession_total_list, purchase_total_list,
            sav_total_list, sav_per_total_list, cession_grand_total, purchase_grand_total, sav_grand_total,
            sav_per_grand_total, status)


def create_header(c, period, width, height, person_name):
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
    third_elemnt = f"Savings By Location By Item {period}"
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


def create_save_by_loc_by_item_pdf(unique_LocationID, unique_LocationName, period, dfs, cession_total_list,
                                   purchase_total_list,
                                   sav_total_list, sav_per_total_list, cession_grand_total, purchase_grand_total,
                                   sav_grand_total, sav_per_grand_total):
    # Convert NumPy array to Python list
    unique_LocationID = unique_LocationID.tolist()
    unique_LocationName = unique_LocationName.tolist()

    person_name = "administrator"
    path = r'C:\Users\Administrator\Downloads\eiis\SAVINGS_BY_LOCATION_BY_ITEM'
    current_time_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"SAVINGS_BY_LOCATION_BY_ITEM_{period}_{current_time_str}.pdf"
    file_path = os.path.join(path, file_name)
    print(file_path)
    # Create a canvas object with landscape orientation
    c = canvas.Canvas(file_path, pagesize=landscape(letter))
    width, height = landscape(letter)
    try:
        rect_x, rect_y, rect_width, rect_height, bottom_margin, left_margin, top_margin, right_margin = create_header(c,
                                                                                                                      period,
                                                                                                                      width,
                                                                                                                      height,
                                                                                                                      person_name)

        for index, (loc_id, loc_name, df, cession_total, purchase_total, sav_total, sav_per_total) in enumerate(
                zip(unique_LocationID, unique_LocationName, dfs, cession_total_list, purchase_total_list,
                    sav_total_list,
                    sav_per_total_list)):
            row_height = 20
            # Calculate available vertical space
            available_space = rect_y - bottom_margin
            print('available_space', available_space)
            # Calculate the number of rows that can fit within the available space
            rows_per_chunk = int(available_space / row_height)
            rows_per_chunk -= 1  # Adjust for the header if it is included in each chunk
            if available_space < 1.5 * cm:
                c.showPage()
                # Draw header and details
                rect_x, rect_y, rect_width, rect_height, bottom_margin, left_margin, top_margin, right_margin = create_header(
                    c, period, width, height, person_name)
                available_space = (rect_y) - bottom_margin
                print('available_space', available_space)
                # Calculate the number of rows that can fit within the available space
                rows_per_chunk = int(available_space / row_height)
                rows_per_chunk -= 1  # Adjust for the header if it is included in each chunk
            # Add location ID and its value below the rectangle
            text_x = rect_x + 1.0 * cm
            text_y = rect_y - 20  # Adjust this value to position the text appropriately
            # Font settings for normal text (values)
            c.setFont("Helvetica-Bold", 8)
            c.drawString(text_x, text_y, f"Location ID: {loc_id}")

            # Add location name and its value below the location ID text
            text_x = rect_x + 8 * cm  # Adjust this value to position the text appropriately
            c.drawString(text_x, text_y, f"Location Name: {loc_name}")
            # Horizontal line spanning from left margin to right margin
            c.line(left_margin, text_y - 5, right_margin, text_y - 5)  # Adjust the y-coordinate if needed
            # Split the DataFrame into the first half and the second half
            first_half = df.iloc[:rows_per_chunk]
            second_half = df.iloc[rows_per_chunk:]
            print('Len of first DF', len(first_half))
            print('Len of second DF', len(second_half))
            table_y = text_y - 0.2 * cm
            df_data = first_half.values.tolist()
            df_headers = first_half.columns.tolist()

            # Create the table
            colWidths = [3.0 * cm, 7.86 * cm, 3.3 * cm, 2.5 * cm]

            df_table = Table([df_headers] + df_data, colWidths=colWidths)
            # Styling the table

            # Styling the table
            df_table.setStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.white),  # Header background
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),  # Header text color
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),  # Center align text for all cells
                ('ALIGN', (1, 1), (1, -1), 'LEFT'),  # Left align text in the second column
                ('ALIGN', (2, 1), (2, -1), 'LEFT'),  # Left align text in the third column
                ('ALIGN', (3, 1), (8, -1), 'RIGHT'),  # Right align text in columns 5 to 9
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),  # Header font
                ('FONTNAME', (1, 1), (-1, -1), 'Helvetica'),  # Body font
                ('FONTSIZE', (0, 0), (-1, -1), 7.0),
                ('FONTSIZE', (0, 1), (-1, -1), 6.0),  # Font size for all cells
                ('BOTTOMPADDING', (0, 0), (-1, -1), 0.1),  # Padding below text
                ('TOPPADDING', (0, 0), (-1, -1), 5),  # Padding above text
                ('GRID', (0, 0), (-1, -1), 1, colors.black)  # Grid lines
            ])

            # Calculate the height of the table
            df_table_height = df_table.wrap(0, 0)[1]
            table_width = width - 2 * left_margin  # Width of the table
            df_table.wrapOn(c, table_width, height)  # Prepare the table for drawing
            df_table.drawOn(c, left_margin, table_y - df_table_height)  # Position and draw the table
            rect_y = table_y - df_table_height
            if len(second_half) == 0:
                print('yes')
                rect_y -= 0.5 * cm
                # Add row for totals
                text_y = rect_y
                # Font settings for bold text (labels)
                c.setFont("Helvetica-Bold", 8)
                text_x = left_margin + 12.5 * cm
                c.drawString(text_x, text_y, "Sub-Total:")
                # Calculate the total values
                total_values = [cession_total, purchase_total, sav_total, sav_per_total]
                # Font settings for normal text (values)
                c.setFont("Helvetica-Bold", 8)
                # Manually adjust the x-positions for each total value
                label_x_values = [left_margin + 17.0 * cm, left_margin + 19.6 * cm, left_margin + 22.3 * cm,
                                  left_margin + 25.0 * cm]

                # Font settings for normal text (values)
                c.setFont("Helvetica-Bold", 8)

                # Draw total values next to their respective labels
                for i, total_value in enumerate(total_values):
                    c.drawString(label_x_values[i], text_y, str(total_value))

                # Draw a horizontal blue line
                line_y = text_y - 0.3 * cm  # Adjust the y-coordinate for the line position
                c.setStrokeColor(colors.blue)  # Set the stroke color to blue
                c.line(left_margin + 16.8 * cm, line_y, left_margin + 26.66 * cm, line_y)  # Draw the line
                c.setStrokeColor(colors.black)  # Reset the stroke color to black
                # Horizontal line spanning from left margin to right margin
                c.line(left_margin, line_y - 0.1 * cm, right_margin,
                       line_y - 0.1 * cm)  # Adjust the y-coordinate if needed
                rect_y = line_y - 0.1 * cm

            else:
                index += 1
                dfs.insert(index, second_half)
                cession_total_list.insert(index, cession_total)
                purchase_total_list.insert(index, purchase_total)
                sav_total_list.insert(index, sav_total)
                sav_per_total_list.insert(index, sav_per_total)
                unique_LocationID.insert(index, loc_id)
                unique_LocationName.insert(index, loc_name)
                c.showPage()
                rect_x, rect_y, rect_width, rect_height, bottom_margin, left_margin, top_margin, right_margin = create_header(
                    c, period, width, height, person_name)
        available_space = rect_y - bottom_margin
        if available_space < 1.2 * cm:
            c.showPage()
            # Draw header and details
            rect_x, rect_y, rect_width, rect_height, bottom_margin, left_margin, top_margin, right_margin = create_header(
                c,
                period,
                width,
                height,
                person_name)
            available_space = (rect_y) - bottom_margin
        rect_y -= 0.5 * cm
        # Add row for totals
        text_y = rect_y
        # Font settings for bold text (labels)
        c.setFont("Helvetica-Bold", 8)
        text_x = left_margin + 12.5 * cm
        c.drawString(text_x, text_y, "Grand-Total:")
        # Calculate the total values
        total_values = [cession_grand_total, purchase_grand_total, sav_grand_total, sav_per_grand_total]
        # Font settings for normal text (values)
        c.setFont("Helvetica-Bold", 8)
        # Manually adjust the x-positions for each total value
        label_x_values = [left_margin + 17.0 * cm, left_margin + 19.6 * cm, left_margin + 22.3 * cm,
                          left_margin + 25.0 * cm]

        # Font settings for normal text (values)
        c.setFont("Helvetica-Bold", 8)

        # Draw total values next to their respective labels
        for i, total_value in enumerate(total_values):
            c.drawString(label_x_values[i], text_y, str(total_value))

        # Draw a horizontal blue line
        line_y = text_y - 0.3 * cm  # Adjust the y-coordinate for the line position
        c.setStrokeColor(colors.blue)  # Set the stroke color to blue
        c.line(left_margin + 16.8 * cm, line_y, left_margin + 26.66 * cm, line_y)  # Draw the line
        c.setStrokeColor(colors.black)  # Reset the stroke color to black
        # Horizontal line spanning from left margin to right margin
        c.line(left_margin, line_y - 0.1 * cm, right_margin, line_y - 0.1 * cm)  # Adjust the y-coordinate if needed
        c.save()
        status = "success"
        print('The status -->', status)
    except Exception as error:
        print('The cause of error is in PDF generation')
        print('The reason for error -->', error)
        status = "failed"
        print('The status -->', status)

    return status, file_name, file_path
