from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm
from reportlab.platypus import Table
from reportlab.lib import colors
import os
import pandas as pd
from datetime import datetime
from database import get_database_connection_e_eiis, get_database_engine_e_eiis


def fetch_data_for_cwh_del_by_loc(location_id, month, year):
    try:
        with get_database_connection_e_eiis() as conn:
            cursor = conn.cursor()
            sql_query = """ SELECT head.CWH_DEL_ID, head.DEL_DATE, head.ORD_LOC_ID, 
                            loc.Location_Name FROM cwhdelhead AS head INNER JOIN 
                            location AS loc ON loc.Location_ID = head.ORD_LOC_ID WHERE 
                            head.ORD_LOC_ID = %s AND MONTH(head.PERIOD) = %s AND YEAR(head.PERIOD) = %s;
                        """
            cursor.execute(sql_query, (location_id, month, year))
            records = cursor.fetchall()
            nested_list = [list(item) for item in records]
            cursor.close()

        df_query = """
        SELECT detail.ITEM_ID, item.Item_Name, detail.PACKAGE_ID, ROUND(detail.QTY, 3) AS QTY, 
        ROUND(detail.IP, 3) AS Cession, ROUND(detail.STOCK_GP, 3) AS pur_value, 
        ROUND( detail.IP - detail.STOCK_GP , 3) AS Saving, 
        ROUND(((detail.IP - detail.STOCK_GP) / detail.IP) * 100, 3) AS Sav_Per FROM cwhdeldetail  AS detail
        INNER JOIN item AS item ON item.Item_ID = detail.ITEM_ID WHERE CWH_DEL_ID = %s;
        """
        cession_total_list = []
        pur_value_total_list = []
        saving_total_list = []
        dfs_list = []
        saving_percentage_total = 0
        engine = get_database_engine_e_eiis()
        for sublist in nested_list:
            DEL_ID = str(sublist[0])
            # Execute the query and retrieve the data as a DataFrame
            df = pd.read_sql_query(df_query, engine, params=(DEL_ID,))
            cession_total_list.append(round(df['Cession'].sum(), 3))
            pur_value_total_list.append(round(df['pur_value'].sum(), 3))
            saving_total_list.append(round(df['Saving'].sum(), 3))
            saving_percentage_total += round(df['Sav_Per'].sum(), 3)
            # Define a dictionary with old names as keys and new names as values
            rename_dict = {
                'ITEM_ID': 'Item Code',
                'Item_Name': 'Item Name',
                'PACKAGE_ID': 'Packing',
                'QTY': 'Qty',
                'pur_value': 'Pur.Value',
                'Sav_Per': 'Sav Per%'
            }

            # Rename the columns using the dictionary
            df.rename(columns=rename_dict, inplace=True)
            dfs_list.append(df)
            print(df)

        # Create a datetime object
        date_obj = datetime(int(year), int(month), 1)

        # Format the datetime object
        formatted_date = date_obj.strftime("%B-%Y")

        # Extracting the second element from each sublist
        location_id = [sublist[2] for sublist in nested_list]
        # print("sup_IDs", supplier_id)
        # Initialize an empty dictionary to store the indices
        indices_dict = {loc_id: [] for loc_id in location_id}

        # Iterate over the nested list
        for idx, sublist in enumerate(nested_list):
            for loc_id in sublist:
                if loc_id in location_id:
                    indices_dict[loc_id].append(idx)
        # print(indices_dict)
        index_nested_list = []

        for key, value in indices_dict.items():
            index_nested_list.append(value)

        status = "success"

    except Exception as error:
        cession_total_list = []
        pur_value_total_list = []
        saving_total_list = []
        dfs_list = []
        saving_percentage_total = 0
        status = "failed"
        print('The cause of error -->', error)
    return (cession_total_list, pur_value_total_list, saving_total_list, dfs_list, saving_percentage_total,
            status, formatted_date, nested_list)


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
    third_element = f"CWH Delivery Details By Location {formatted_date}"
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


def create_cwh_del_details_by_loc_pdf(cession_total_list, pur_value_total_list, saving_total_list, dfs_list,
                                      saving_percentage_total,  formatted_date, nested_list, location_id):
    file_name = None
    file_with_path = None
    try:
        person_name = "administrator"
        current_datetime = datetime.now()
        current_date_time_str = current_datetime.strftime("%Y_%m_%d_%H_%M_%S")
        file_name = f'{location_id}_{current_date_time_str}.pdf'

        directory_path = r"C:\Users\Administrator\Downloads\eiis\item_delivered_to_location"
        file_with_path = os.path.join(directory_path, file_name)

        # Create a canvas
        c = canvas.Canvas(file_with_path, pagesize=letter)
        width, height = letter  # Size of page (letter size)

        # Draw header and details
        rect_y, top_margin, left_margin, bottom_margin, right_margin, rect_x, rect_height, rect_width = draw_header_and_details(
            c, width, height, formatted_date, person_name)

        second_rect_y = rect_y - rect_height  # Position of the second rectangle

        page = 1
        cession_grand_total = 0
        purchase_grand_total = 0
        saving_grand_total = 0
        for df, ces_tot, pur_tot, saving_tot, nest_list in zip(dfs_list, cession_total_list, pur_value_total_list,
                                                               saving_total_list, nested_list):
            cession_grand_total += ces_tot
            purchase_grand_total += pur_tot
            saving_grand_total += saving_tot
            loc_code_name = str(nest_list[2]) + " - " + str(nest_list[3])
            cwh_delivery_no = str(nest_list[0])
            del_date_text_str = str(nest_list[1])
            c.rect(rect_x, second_rect_y, rect_width, rect_height, stroke=1, fill=0)

            # Text for the second rectangle
            receiving_text = "Location ID & Name:"
            receiving_value = loc_code_name  # Example value, replace with actual data as needed
            delivery_text = "CWH Delivery No:"
            delivery_value = cwh_delivery_no  # Example value, replace with actual data as needed

            # Delivery Date details
            delivery_date_text = "Delivery Date:"
            # Parse the input date string into a datetime object
            del_date_text = datetime.strptime(del_date_text_str, "%Y-%m-%d")
            # Format the datetime object to the desired format: %B %d, %Y
            delivery_date_value = del_date_text.strftime("%B %d, %Y")

            # Delivery Type details
            delivery_type_text = "Delivery Type:"
            delivery_type_value = "CWH Delivery"  # Example value, replace with actual data as needed

            # Text settings
            text_margin = 0.5 * cm  # Margin from the edge of the rectangle
            text_x = rect_x + text_margin
            manual_adjustment = 1.8 * cm  # Manually adjust this value to position the text vertically within the rectangle
            text_y = second_rect_y + manual_adjustment  # Apply manual adjustment

            # Font settings for bold text (labels)
            c.setFont("Helvetica-Bold", 10)

            # Draw the "Location ID & Name:" label
            c.drawString(text_x, text_y, receiving_text)

            # Calculate the width of the label to position the value next to it
            label_width = c.stringWidth(receiving_text, "Helvetica-Bold", 10)
            value_x = text_x + label_width + 5  # Start the value right after the label with some spacing

            # Font settings for normal text (values)
            c.setFont("Helvetica", 10)
            c.drawString(value_x, text_y, receiving_value)

            # Adjust y-coordinate for the next row
            text_y -= 0.7 * cm  # Adjust the vertical spacing as needed

            # Draw the "CWH Delivery No:" label in bold
            c.setFont("Helvetica-Bold", 10)
            c.drawString(text_x, text_y, delivery_text)
            # Calculate the width of the label to position the value next to it
            label_width = c.stringWidth(delivery_text, "Helvetica-Bold", 10)
            value_x = text_x + label_width + 5  # Start the value right after the label with some spacing

            # Draw the value in normal font
            c.setFont("Helvetica", 10)
            c.drawString(value_x, text_y, delivery_value)

            # Adjust y-coordinate for the next row
            text_y -= 0.7 * cm  # Adjust the vertical spacing as needed

            # Draw the "Delivery Date:" label in bold
            c.setFont("Helvetica-Bold", 10)
            c.drawString(text_x, text_y, delivery_date_text)

            # Calculate the width of the label to position the value next to it
            label_width = c.stringWidth(delivery_date_text, "Helvetica-Bold", 10)
            value_x = text_x + label_width + 5  # Start the value right after the label with some spacing

            # Draw the value in normal font
            c.setFont("Helvetica", 10)
            c.drawString(value_x, text_y, delivery_date_value)

            # Calculate the x position for Delivery Type on the right
            delivery_type_label_width = c.stringWidth(delivery_type_text, "Helvetica-Bold", 10)
            delivery_type_value_width = c.stringWidth(delivery_type_value, "Helvetica", 10)
            total_delivery_type_width = delivery_type_label_width + delivery_type_value_width + 20

            # Set position for Delivery Type
            delivery_type_text_x = right_margin - total_delivery_type_width - text_margin - 5  # Align it from the right margin

            # Adjust y-coordinate for the next row
            text_y += 1.4 * cm

            # Draw Delivery Type
            c.setFont("Helvetica-Bold", 10)
            c.drawString(delivery_type_text_x, text_y, delivery_type_text)
            c.setFont("Helvetica", 10)
            c.drawString(delivery_type_text_x + delivery_type_label_width + 5, text_y, delivery_type_value)

            # Calculate available space for the table
            available_space = second_rect_y - bottom_margin
            if available_space < 1.5 * cm:
                # Not enough space, start a new page
                c.showPage()
                page += 1

                # Redraw header and details for the new page
                rect_y, top_margin, left_margin, bottom_margin, right_margin, rect_x, rect_height, rect_width = draw_header_and_details(
                    c, width, height, formatted_date, person_name)
                second_rect_y = rect_y - rect_height  # Update second_rect_y for the new page
                # Calculate available space for the table
                available_space = second_rect_y - bottom_margin

            # Known row height in points
            row_height = 30.19685

            # Calculate rows per chunk based on available space
            rows_per_chunk = int(available_space / row_height) - 1  # Adjust for header and padding

            # Split DataFrame into chunks
            chunks = [df[i:i + rows_per_chunk] for i in range(0, len(df), rows_per_chunk)]

            for i, chunk in enumerate(chunks):
                table_y = second_rect_y  # Start the table below the second rectangle
                df_data = chunk.values.tolist()
                df_headers = chunk.columns.tolist()

                # Create the table
                colWidths = [2.3 * cm, 6.4 * cm, 3.6 * cm, 1.6 * cm, 1.6 * cm, 1.6 * cm, 1.6 * cm, 1.6 * cm]
                df_table = Table([df_headers] + df_data, colWidths=colWidths)
                df_table.setStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.white),  # Header background
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),  # Header text color
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),  # Default center align text for all cells
                    ('ALIGN', (1, 1), (1, -1), 'LEFT'),  # Left align text in the second column
                    ('ALIGN', (3, 1), (7, -1), 'RIGHT'),  # Right align text in columns 4 to 8
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),  # Header font
                    ('FONTNAME', (1, 1), (-1, -1), 'Helvetica'),  # Body font
                    ('FONTSIZE', (0, 0), (-1, -1), 6.0),  # Font size for all cells
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 3),  # Padding below text
                    ('TOPPADDING', (0, 0), (-1, -1), 12),  # Padding above text
                    ('GRID', (0, 0), (-1, -1), 1, colors.black)  # Grid lines
                ])

                # Calculate the height of the table
                df_table_height = df_table.wrap(0, 0)[1]

                # Check if the table fits on the current page
                if table_y - df_table_height < bottom_margin + 0.5 * cm:
                    # Not enough space, start a new page
                    c.showPage()
                    page += 1

                    # Redraw header and details for the new page
                    rect_y, top_margin, left_margin, bottom_margin, right_margin, rect_x, rect_height, rect_width = draw_header_and_details(
                        c, width, height, formatted_date, person_name)
                    second_rect_y = rect_y - rect_height  # Update second_rect_y for the new page
                    table_y = rect_y

                # Draw the table on the current page
                table_y -= df_table_height
                df_table.drawOn(c, left_margin, table_y)

                # Update second_rect_y for potential new pages
                second_rect_y = table_y

            # After drawing all chunks for the current section, add totals and possibly more content
            text_y = table_y - 0.7 * cm  # Adjust for spacing

            # Draw total values
            c.setFont("Helvetica-Bold", 8)
            text_x = left_margin + 10.5 * cm
            c.drawString(text_x, text_y, "Total:")

            total_values = [ces_tot, pur_tot, saving_tot]

            label_x_values = [left_margin + 13.1 * cm, left_margin + 15.1 * cm, left_margin + 17.0 * cm]

            for i, total_value in enumerate(total_values):
                c.drawString(label_x_values[i], text_y, str(total_value))

            # Draw horizontal blue lines
            line_y = text_y - 0.3 * cm
            c.setStrokeColor(colors.blue)
            c.line(left_margin + 12.8 * cm, line_y, left_margin + 20.2 * cm, line_y)
            line_y = text_y - 0.4 * cm
            c.line(left_margin + 12.8 * cm, line_y, left_margin + 20.2 * cm, line_y)
            c.setStrokeColor(colors.black)

            # Update second_rect_y for the next section
            second_rect_y = line_y - 2.3 * cm

            # Check if a new page is needed after finishing the current section
            if second_rect_y < bottom_margin + 0.5 * cm:
                c.showPage()
                page += 1

                # Redraw header and details for the new page
                rect_y, top_margin, left_margin, bottom_margin, right_margin, rect_x, rect_height, rect_width = draw_header_and_details(
                    c, width, height, formatted_date, person_name)
                second_rect_y = rect_y - rect_height  # Update second_rect_y for the new page

        # After drawing all chunks for the current section, add totals and possibly more content
        text_y = line_y - 0.5 * cm  # Adjust for spacing

        # Draw total values
        c.setFont("Helvetica-Bold", 8)
        text_x = left_margin + 10.5 * cm
        c.drawString(text_x, text_y, "Grand Total:")

        total_values = [round(cession_grand_total, 3), round(purchase_grand_total, 3), round(saving_grand_total, 3),
                        round(saving_percentage_total, 3)]

        label_x_values = [left_margin + 13.1 * cm, left_margin + 15.1 * cm, left_margin + 17.0 * cm,
                          left_margin + 18.7 * cm]

        for i, total_value in enumerate(total_values):
            c.drawString(label_x_values[i], text_y, str(total_value))
        c.save()
        status = "success"
    except Exception as error:
        print('The cause of error -->', error)
        status = "failed"

    return file_name, file_with_path, status
