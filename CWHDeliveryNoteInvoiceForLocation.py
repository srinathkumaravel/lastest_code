from reportlab.lib.pagesizes import letter, landscape
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm
from reportlab.platypus import Table
from reportlab.lib import colors
import os
import pandas as pd
import PyPDF2
from datetime import datetime
from database import get_database_connection_e_eiis, get_database_engine_e_eiis


def fetch_cwh_details_by_loc(month, year):
    df_list: list = []
    food_total: list = []
    total_amount: list = []
    cleaning_total: list = []
    disposal_total: list = []
    other_total: list = []
    formatted_date: None
    delivery_type = "cwh_delivery"
    # Create a datetime object
    date_obj = datetime(int(year), int(month), 1)

    # Format the datetime object
    formatted_date = date_obj.strftime("%B-%Y")
    try:
        with get_database_connection_e_eiis() as conn:
            cursor = conn.cursor()
            sql_query = """ SELECT cwhhead.CWH_DEL_ID, cwhhead.ORD_LOC_ID, loc.Location_Name, cwhhead.STORE_LOC_ID, 
                            cwhhead.DEL_DATE FROM cwhdelhead AS cwhhead INNER JOIN location AS loc ON 
                            loc.Location_ID = cwhhead.ORD_LOC_ID WHERE YEAR(cwhhead.PERIOD) = %s AND 
                            MONTH(cwhhead.PERIOD) = %s;
                        """
            cursor.execute(sql_query, (year, month))
            records = cursor.fetchall()
            nested_list = [list(item) for item in records]
            cursor.close()
            print(nested_list)
            if len(nested_list) == 0:
                print(f'NO DATA AVAILABLE FOR CWH DELIVERY FOR THE SELECTED PERIOD MONTH -> {month} & YEAR -> {year}')
                status = "failed"
                return (status, df_list, other_total, disposal_total, cleaning_total, food_total, total_amount,
                        nested_list, formatted_date, delivery_type)
        engine = get_database_engine_e_eiis()
        df_query = """
                    SELECT cwhdetail.ITEM_ID, item.Item_Name, cwhdetail.PACKAGE_ID, cwhdetail.EXPIRY_DATE, 
                    cwhdetail.BATCH_NO, ROUND(cwhdetail.QTY_UNIT, 3) AS QTY_UNIT, ROUND(cwhdetail.IP_UNIT, 3) AS IP_UNIT, 
                    ROUND((cwhdetail.QTY_UNIT * cwhdetail.IP_UNIT), 4) AS Total_Amount , mstitem.ACCOUNT_NAME 
                    FROM cwhdeldetail AS cwhdetail INNER JOIN item AS item ON item.Item_ID = cwhdetail.ITEM_ID 
                    INNER JOIN mst_item_account AS mstitem ON mstitem.ACCOUNT_ID = item.Account_ID WHERE 
                    cwhdetail.CWH_DEL_ID = %s;
                   """

        for sublist in nested_list:
            DEL_ID = str(sublist[0])
            # Execute the query and retrieve the data as a DataFrame
            df = pd.read_sql_query(df_query, engine, params=(DEL_ID,))
            items_list = df['ACCOUNT_NAME'].tolist()
            total_value = df['Total_Amount'].tolist()
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

            # Concatenate the two DataFrames horizontally (along the columns)
            concatenated_df = pd.concat([df, df_1], axis=1)
            # Replace NaN values with 0.00
            concatenated_df = concatenated_df.fillna(0.00).infer_objects()
            # Define a dictionary with old names as keys and new names as values
            rename_dict = {
                'ITEM_ID': 'Item Code',
                'Item_Name': 'Item Name',
                'PACKAGE_ID': 'Packing',
                'QTY_UNIT': 'Issue Qty',
                'IP_UNIT': 'Unit Price',
                'EXPIRY_DATE': 'Expiry Date',
                'BATCH_NO': 'Batch No',
                'FOOD': 'Food',
                'CLEANING': 'Cleaning',
                'DISPOSABLES': 'Disposal',
                'Total_Amount': 'Total Amount'
            }

            # Rename the columns using the dictionary
            concatenated_df.rename(columns=rename_dict, inplace=True)
            # Assuming "Total Amount" is the column you want to move
            total_amount_column = concatenated_df.pop("Total Amount")

            # Reinsert the "Total Amount" column at the last position
            concatenated_df["Total Amount"] = total_amount_column
            total_amount_sum = round(concatenated_df['Total Amount'].sum(), 3)
            total_amount.append(total_amount_sum)
            food_total_sum = round(concatenated_df['Food'].sum(), 3)
            food_total.append(food_total_sum)
            Cleaning_total_sum = round(concatenated_df['Cleaning'].sum(), 3)
            cleaning_total.append(Cleaning_total_sum)
            Disposal_total_sum = round(concatenated_df['Disposal'].sum(), 3)
            disposal_total.append(Disposal_total_sum)
            Others_total_sum = round(concatenated_df['Others'].sum(), 3)
            other_total.append(Others_total_sum)
            print(concatenated_df)
            df_list.append(concatenated_df)

        status = "success"

    except Exception as error:
        print('The cause of error -->', error)
        status = "failed"

    return (status, df_list, other_total, disposal_total, cleaning_total, food_total, total_amount,
            nested_list, formatted_date, delivery_type)


def merge_pdfs(pdf_files, output_path):
    try:
        # Create a PDF writer object
        pdf_writer = PyPDF2.PdfWriter()

        # Iterate through each PDF file
        for pdf_file in pdf_files:
            # Open the PDF file
            with open(pdf_file, 'rb') as pdf:
                # Create a PDF reader object
                pdf_reader = PyPDF2.PdfReader(pdf)
                # Iterate through each page and add it to the writer
                for page in pdf_reader.pages:
                    pdf_writer.add_page(page)

        # Write the merged PDF to the output file
        with open(output_path, 'wb') as output_pdf:
            pdf_writer.write(output_pdf)
        status = "success"
    except Exception as error:
        print('The Cause of error -->', error)
        status = "failed"
    print(output_path)
    return status, output_path


def draw_header_and_details(c, width, height, formatted_date, person_name, rec_center_no, cwh_delivery_no,
                            store_del_text, del_date_text, delivery_type, direct_delivery_by_text):
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
    third_elemnt = "CWH Delivery Note/Invoice For Location"
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
    # New y position for the second rectangle (below the first one)
    second_rect_y = rect_y - rect_height  # Adding 0.5 cm spacing between rectangles
    # Draw the second rectangle
    c.rect(rect_x, second_rect_y, rect_width, rect_height, stroke=1, fill=0)

    # Text for the second rectangle
    receiving_text = "Receiving Centre :"
    receiving_value = rec_center_no  # Example value, replace with actual data as needed
    delivery_text = "CWH Delivery No :"
    delivery_value = cwh_delivery_no  # Example value, replace with actual data as needed
    direct_del_by_text = "Direct Delivery By :"
    if delivery_type == "cwh_delivery":
        pass
    else:
        direct_del_value = direct_delivery_by_text

    # Store delivery details
    store_delivery_text = "Store Delivery :"
    store_delivery_value = store_del_text  # Example value
    # Delivery Date details
    delivery_date_text = "Delivery Date  :"
    delivery_date_value = del_date_text  # Example value, format as needed

    delivery_type_text = "Delivery Type :"
    if delivery_type == "cwh_delivery":
        # Delivery Type details
        delivery_type_value = "CWH Delivery"  # Example value, replace with actual data as needed
    else:
        delivery_type_value = "Direct Delivery"  # Example value, replace with actual data as needed

    # Text settings
    text_margin = 0.5 * cm  # Margin from the edge of the rectangle
    text_x = rect_x + text_margin
    manual_adjustment = 1.8 * cm  # Manually adjust this value to position the text vertically within the rectangle
    text_y = second_rect_y + manual_adjustment  # Apply manual adjustment

    # Font settings for bold text (labels)
    c.setFont("Helvetica-Bold", 10)

    # Draw the "Receiving Centre :" label
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

    # Draw the "CWH Delivery No:" label in bold
    c.setFont("Helvetica-Bold", 10)
    c.drawString(text_x, text_y, direct_del_by_text)

    # Calculate the width of the label to position the value next to it
    label_width = c.stringWidth(direct_del_by_text, "Helvetica-Bold", 10)

    if delivery_type == "cwh_delivery":
        pass
    else:
        c.setFont("Helvetica", 10)
        c.drawString(text_x + label_width + 5, text_y, direct_del_value)

    # Calculate the x position for Store Delivery on the right
    store_label_width = c.stringWidth(store_delivery_text, "Helvetica-Bold", 10)
    store_value_width = c.stringWidth(store_delivery_value, "Helvetica", 10)
    total_store_width = store_label_width + store_value_width + 35

    # Set position for Store Delivery
    store_text_x = right_margin - total_store_width - text_margin - 20  # Align it from the right margin
    text_y += 1.4 * cm
    # Draw Store Delivery
    c.setFont("Helvetica-Bold", 10)
    c.drawString(store_text_x, text_y, store_delivery_text)
    c.setFont("Helvetica", 10)
    c.drawString(store_text_x + store_label_width + 5, text_y, store_delivery_value)

    # Adjust y-coordinate for the next row
    text_y -= 0.7 * cm  # Adjust the vertical spacing as needed

    # Calculate the width for the delivery date to fit on the right
    delivery_date_label_width = c.stringWidth(delivery_date_text, "Helvetica-Bold", 10)
    delivery_date_value_width = c.stringWidth(delivery_date_value, "Helvetica", 10)
    total_delivery_date_width = delivery_date_label_width + delivery_date_value_width + 7

    # Calculate the x position for Delivery Date on the right
    delivery_date_text_x = right_margin - total_delivery_date_width - text_margin - 5  # Align it from the right margin

    # Draw Delivery Date label and value
    c.setFont("Helvetica-Bold", 10)
    c.drawString(delivery_date_text_x, text_y, delivery_date_text)
    c.setFont("Helvetica", 10)
    c.drawString(delivery_date_text_x + delivery_date_label_width + 5, text_y, delivery_date_value)

    # Adjust y-coordinate for the next row
    text_y -= 0.7 * cm  # Adjust the vertical spacing as needed

    # Calculate the width for the delivery type to fit on the right
    delivery_type_label_width = c.stringWidth(delivery_type_text, "Helvetica-Bold", 10)
    delivery_type_value_width = c.stringWidth(delivery_type_value, "Helvetica", 10)
    total_delivery_type_width = delivery_type_label_width + delivery_type_value_width + 6

    # Calculate the x position for Delivery Type on the right
    delivery_type_text_x = right_margin - total_delivery_type_width - text_margin - 5  # Align it from the right margin

    # Draw Delivery Type label and value
    c.setFont("Helvetica-Bold", 10)
    c.drawString(delivery_type_text_x, text_y, delivery_type_text)
    c.setFont("Helvetica", 10)
    c.drawString(delivery_type_text_x + delivery_type_label_width + 5, text_y, delivery_type_value)

    return second_rect_y, top_margin, left_margin, bottom_margin, right_margin


def calculate_rows_to_fit(table, available_space):
    # Calculate the height of the table without actually drawing it
    table_height = table.wrap(0, 0)[1]
    print(table_height)

    # Calculate the number of rows based on the available space and table height
    rows_to_fit = int(available_space / table_height)

    return rows_to_fit


def create_cwh_pdf_for_loc(df_list, other_total, disposal_total, cleaning_total, food_total, total_amount, nested_list,
                           formatted_date, delivery_type):
    pdf_file_name = []
    page = 0
    person_name = "administrator"
    file_name = None
    output_path = None
    try:
        for df, other_tot, dis_tot, cleaning_tot, food_tot, tot_amount, nest_list in zip(df_list, other_total,
                                                                                         disposal_total, cleaning_total,
                                                                                         food_total, total_amount,
                                                                                         nested_list):
            print(nest_list)
            page += 1
            if delivery_type == "cwh_delivery":
                rec_center_no = str(nest_list[1]) + " " + str(nest_list[2])
                cwh_delivery_no = str(nest_list[0])
                store_del_text = str(nest_list[3])
                del_date_text = str(nest_list[4])
                direct_delivery_by_text = ""
            else:
                rec_center_no = str(nest_list[1]) + " " + str(nest_list[2])
                cwh_delivery_no = str(nest_list[0])
                store_del_text = str(nest_list[6])
                del_date_text = str(nest_list[5])
                direct_delivery_by_text = str(nest_list[3] + " " + str(nest_list[4]))

            current_datetime = datetime.now()
            # Format the current date and time as desired (example: YYYY-MM-DD_HH-MM-SS)
            current_date_time_str = current_datetime.strftime("%Y_%m_%d_%H_%M_%S")
            file_name = f'{page}_{current_date_time_str}.pdf'

            directory_path = r"C:\Users\Administrator\Downloads\eiis\cwh_invoice_for_location\single_pdf"
            file_with_path = directory_path + "\\" + file_name
            pdf_file_name.append(file_with_path)
            # Combine the directory path and file name
            file_path = os.path.join(directory_path, file_name)

            # Create a canvas object with landscape orientation
            c = canvas.Canvas(file_path, pagesize=landscape(letter))
            width, height = landscape(letter)
            # Header and bottom details
            second_rect_y, top_margin, left_margin, bottom_margin, right_margin = draw_header_and_details(c, width,
                                                                                                          height,
                                                                                                          formatted_date,
                                                                                                          person_name,
                                                                                                          rec_center_no,
                                                                                                          cwh_delivery_no,
                                                                                                          store_del_text,
                                                                                                          del_date_text,
                                                                                                          delivery_type,
                                                                                                          direct_delivery_by_text)

            # Split the DataFrame into chunks of 22 rows each
            chunk_size = 14
            chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

            for i, chunk in enumerate(chunks):
                table_y = second_rect_y  # Start the table below the second rectangle

                df_data = chunk.values.tolist()
                df_headers = chunk.columns.tolist()

                # Create the table
                colWidths = [1.6 * cm, 5.5 * cm, 3.0 * cm, 1.4 * cm, 1.2 * cm, 2.0 * cm, 2.0 * cm, 2.0 * cm, 2.0 * cm,
                             2.0 * cm, 2.0 * cm, 2.0 * cm]

                df_table = Table([df_headers] + df_data, colWidths=colWidths)

                df_table.setStyle(([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.white),  # Header background
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),  # Header text color
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),  # Center align text for all cells
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),  # Header font
                    ('FONTNAME', (1, 1), (-1, -1), 'Helvetica'),  # Body font
                    ('FONTSIZE', (0, 0), (-1, -1), 6.5),  # Font size for all cells
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 3),  # Padding below text
                    ('TOPPADDING', (0, 0), (-1, -1), 12),  # Padding above text
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),  # Grid lines
                    ('ALIGN', (1, 1), (1, -1), 'LEFT'),  # Left align the second column
                    ('FONTSIZE', (0, 1), (-1, -1), 6.5)  # Adjust font size for all cells
                ]))
                # Calculate the height of the table
                df_table_height = df_table.wrap(0, 0)[1]

                table_width = width - 2 * left_margin  # Width of the table
                df_table.wrapOn(c, table_width, height)  # Prepare the table for drawing
                df_table.drawOn(c, left_margin, table_y - df_table_height)  # Position and draw the table

                # Move to the next page if there are more chunks
                if i < len(chunks) - 1:
                    # Header and bottom details
                    c.showPage()
                    second_rect_y, top_margin, left_margin, bottom_margin, right_margin = draw_header_and_details(c,
                                                                                                                  width,
                                                                                                                  height,
                                                                                                                  formatted_date,
                                                                                                                  person_name,
                                                                                                                  rec_center_no,
                                                                                                                  cwh_delivery_no,
                                                                                                                  store_del_text,
                                                                                                                  del_date_text,
                                                                                                                  delivery_type,
                                                                                                                  direct_delivery_by_text)
                else:
                    # Add row for totals
                    text_y = table_y - df_table_height - 0.7 * cm
                    # Font settings for bold text (labels)
                    c.setFont("Helvetica-Bold", 8)
                    text_x = left_margin + 10.5 * cm
                    c.drawString(text_x, text_y, "Total:")
                    # Calculate the total values
                    total_values = [food_tot, dis_tot, cleaning_tot, other_tot, tot_amount]
                    # Font settings for normal text (values)
                    c.setFont("Helvetica-Bold", 8)
                    # Manually adjust the x-positions for each total value
                    label_x_values = [left_margin + 17.5 * cm, left_margin + 19.3 * cm, left_margin + 21.4 * cm,
                                      left_margin + 23.3 * cm, left_margin + 25.2 * cm]

                    # Font settings for normal text (values)
                    c.setFont("Helvetica-Bold", 8)

                    # Draw total values next to their respective labels
                    for i, total_value in enumerate(total_values):
                        c.drawString(label_x_values[i], text_y, str(total_value))

                    # Draw a horizontal blue line
                    line_y = text_y - 0.3 * cm  # Adjust the y-coordinate for the line position
                    c.setStrokeColor(colors.blue)  # Set the stroke color to blue
                    c.line(left_margin + 17.0 * cm, line_y, left_margin + 26.8 * cm, line_y)  # Draw the line
                    c.setStrokeColor(colors.black)  # Reset the stroke color to black
                    # Draw a horizontal blue line
                    line_y = text_y - 0.4 * cm  # Adjust the y-coordinate for the line position
                    c.setStrokeColor(colors.blue)  # Set the stroke color to blue
                    c.line(left_margin + 17.0 * cm, line_y, left_margin + 26.8 * cm, line_y)  # Draw the line
                    c.setStrokeColor(colors.black)  # Reset the stroke color to black
                    print(line_y)

            if line_y > 18.425196850393792:
                # Text to be printed below the line
                confirmation_text = "We hereby confirm having received the goods mentioned in the above Delivery Order No / Supplier Invoice in good condition for location use."

                # Calculate the width of the confirmation text
                confirmation_text_width = c.stringWidth(confirmation_text, "Helvetica", 9)

                # Calculate the x position to center the text horizontally between the margins
                confirmation_text_x = left_margin + 0.32 * cm

                # Print the confirmation text
                c.setFont("Helvetica", 9)
                confirmation_text_y = line_y - 0.6 * cm
                c.drawString(confirmation_text_x, confirmation_text_y, confirmation_text)
                print(line_y)
                print(confirmation_text_y)
            else:
                # Header and bottom details
                c.showPage()
                second_rect_y, top_margin, left_margin, bottom_margin, right_margin = draw_header_and_details(c, width,
                                                                                                              height,
                                                                                                              formatted_date,
                                                                                                              person_name,
                                                                                                              rec_center_no,
                                                                                                              cwh_delivery_no,
                                                                                                              store_del_text,
                                                                                                              del_date_text,
                                                                                                              delivery_type,
                                                                                                              direct_delivery_by_text)
                # Text to be printed below the line
                confirmation_text = "We hereby confirm having received the goods mentioned in the above Delivery Order No / Supplier Invoice in good condition for location use."

                # Calculate the width of the confirmation text
                confirmation_text_width = c.stringWidth(confirmation_text, "Helvetica", 9)

                # Calculate the x position to center the text horizontally between the margins
                confirmation_text_x = left_margin + 0.32 * cm

                # Print the confirmation text
                c.setFont("Helvetica", 9)
                confirmation_text_y = second_rect_y - 0.6 * cm
                c.drawString(confirmation_text_x, confirmation_text_y, confirmation_text)

            # Calculate the remaining space between confirmation_text_y and bottom_margin
            remaining_space = confirmation_text_y - bottom_margin
            print(remaining_space)

            # Check if the remaining space is less than the minimum required (5 cm)
            if remaining_space >= 145.41732283464574:

                # Draw two lines below the confirmation text
                line_length = 8.3 * cm  # Adjust the length of the lines as needed

                # Draw the line on the left side
                left_line_x_start = left_margin + 0.32 * cm  # Adjust the x-coordinate as needed
                left_line_y = confirmation_text_y - 0.5 * cm  # Adjust the y-coordinate for the line position
                left_line_x_end = left_line_x_start + line_length  # Adjust the end x-coordinate as needed
                c.line(left_line_x_start, left_line_y, left_line_x_end, left_line_y)

                # Draw the line on the right side
                right_line_x_end = right_margin - 0.32 * cm  # Adjust the x-coordinate as needed
                right_line_y = confirmation_text_y - 0.5 * cm  # Adjust the y-coordinate for the line position
                right_line_x_start = right_line_x_end - line_length  # Adjust the start x-coordinate as needed
                c.line(right_line_x_start, right_line_y, right_line_x_end, right_line_y)

                # Calculate the position for "Delivered By:" text
                delivered_by_text_x = left_margin + 0.32 * cm  # Same x-coordinate as confirmation text
                delivered_by_text_y = left_line_y - 0.7 * cm  # Adjust the y-coordinate as needed

                # Draw "Delivered By:" text
                c.setFont("Helvetica-Bold", 9)
                c.drawString(delivered_by_text_x, delivered_by_text_y, "Delivered By :")

                # Calculate the position for "Received By:" text
                received_by_text_x = right_margin - 6.7 * cm - c.stringWidth("Received By :", "Helvetica-Bold",
                                                                             9)  # Align it from the right margin
                received_by_text_y = right_line_y - 0.7 * cm  # Adjust the y-coordinate as needed

                # Draw "Received By:" text
                c.drawString(received_by_text_x, received_by_text_y, "Received By :")

                # Calculate the position for "Signature:" text
                signature_left_text_x = left_margin + 0.32 * cm  # Same x-coordinate as confirmation text
                signature_left_text_y = left_line_y - 1.4 * cm  # Adjust the y-coordinate as needed

                # Draw "Signature:" text
                c.drawString(signature_left_text_x, signature_left_text_y, "Signature       :")

                # Calculate the position for "Signature:" text
                signature_right_text_x = right_margin - 7.1 * cm - c.stringWidth("Signature :", "Helvetica-Bold",
                                                                                 9)  # Same x-coordinate as confirmation text
                signature_right_text_y = right_line_y - 1.4 * cm  # Adjust the y-coordinate as needed

                # Draw "Signature:" text
                c.drawString(signature_right_text_x, signature_right_text_y, "Signature      :")

                # Calculate the position for "Employee No:" text on the left side
                employee_no_left_text_x = left_margin + 0.32 * cm  # Same x-coordinate as confirmation text
                employee_no_left_text_y = left_line_y - 2.1 * cm  # Adjust the y-coordinate as needed

                # Draw "Employee No:" text on the left side
                c.setFont("Helvetica-Bold", 9)
                c.drawString(employee_no_left_text_x, employee_no_left_text_y, "Employee No :")

                # Calculate the position for "Employee No:" text on the right side
                employee_no_right_text_x = right_margin - 6.5 * cm - c.stringWidth("Employee No  :", "Helvetica-Bold",
                                                                                   9)  # Same x-coordinate as confirmation text
                employee_no_right_text_y = right_line_y - 2.1 * cm  # Adjust the y-coordinate as needed

                # Draw "Employee No:" text on the right side
                c.drawString(employee_no_right_text_x, employee_no_right_text_y, "Employee No :")
            else:
                # Header and bottom details
                c.showPage()
                second_rect_y, top_margin, left_margin, bottom_margin, right_margin = draw_header_and_details(c, width,
                                                                                                              height,
                                                                                                              formatted_date,
                                                                                                              person_name,
                                                                                                              rec_center_no,
                                                                                                              cwh_delivery_no,
                                                                                                              store_del_text,
                                                                                                              del_date_text,
                                                                                                              delivery_type,
                                                                                                              direct_delivery_by_text)
                confirmation_text_y = second_rect_y - 0.6 * cm
                # Draw two lines below the confirmation text
                line_length = 8.3 * cm  # Adjust the length of the lines as needed

                # Draw the line on the left side
                left_line_x_start = left_margin + 0.32 * cm  # Adjust the x-coordinate as needed
                left_line_y = confirmation_text_y - 0.5 * cm  # Adjust the y-coordinate for the line position
                left_line_x_end = left_line_x_start + line_length  # Adjust the end x-coordinate as needed
                c.line(left_line_x_start, left_line_y, left_line_x_end, left_line_y)

                # Draw the line on the right side
                right_line_x_end = right_margin - 0.32 * cm  # Adjust the x-coordinate as needed
                right_line_y = confirmation_text_y - 0.5 * cm  # Adjust the y-coordinate for the line position
                right_line_x_start = right_line_x_end - line_length  # Adjust the start x-coordinate as needed
                c.line(right_line_x_start, right_line_y, right_line_x_end, right_line_y)

                # Calculate the position for "Delivered By:" text
                delivered_by_text_x = left_margin + 0.32 * cm  # Same x-coordinate as confirmation text
                delivered_by_text_y = left_line_y - 0.7 * cm  # Adjust the y-coordinate as needed

                # Draw "Delivered By:" text
                c.setFont("Helvetica-Bold", 9)
                c.drawString(delivered_by_text_x, delivered_by_text_y, "Delivered By :")

                # Calculate the position for "Received By:" text
                received_by_text_x = right_margin - 6.7 * cm - c.stringWidth("Received By :", "Helvetica-Bold",
                                                                             9)  # Align it from the right margin
                received_by_text_y = right_line_y - 0.7 * cm  # Adjust the y-coordinate as needed

                # Draw "Received By:" text
                c.drawString(received_by_text_x, received_by_text_y, "Received By :")

                # Calculate the position for "Signature:" text
                signature_left_text_x = left_margin + 0.32 * cm  # Same x-coordinate as confirmation text
                signature_left_text_y = left_line_y - 1.4 * cm  # Adjust the y-coordinate as needed

                # Draw "Signature:" text
                c.drawString(signature_left_text_x, signature_left_text_y, "Signature       :")

                # Calculate the position for "Signature:" text
                signature_right_text_x = right_margin - 7.1 * cm - c.stringWidth("Signature :", "Helvetica-Bold",
                                                                                 9)  # Same x-coordinate as confirmation text
                signature_right_text_y = right_line_y - 1.4 * cm  # Adjust the y-coordinate as needed

                # Draw "Signature:" text
                c.drawString(signature_right_text_x, signature_right_text_y, "Signature      :")

                # Calculate the position for "Employee No:" text on the left side
                employee_no_left_text_x = left_margin + 0.32 * cm  # Same x-coordinate as confirmation text
                employee_no_left_text_y = left_line_y - 2.1 * cm  # Adjust the y-coordinate as needed

                # Draw "Employee No:" text on the left side
                c.setFont("Helvetica-Bold", 9)
                c.drawString(employee_no_left_text_x, employee_no_left_text_y, "Employee No :")

                # Calculate the position for "Employee No:" text on the right side
                employee_no_right_text_x = right_margin - 6.5 * cm - c.stringWidth("Employee No  :", "Helvetica-Bold",
                                                                                   9)  # Same x-coordinate as confirmation text
                employee_no_right_text_y = right_line_y - 2.1 * cm  # Adjust the y-coordinate as needed

                # Draw "Employee No:" text on the right side
                c.drawString(employee_no_right_text_x, employee_no_right_text_y, "Employee No :")
            c.save()

        # Output folder path for the merged PDF
        output_folder = r'C:\Users\Administrator\Downloads\eiis\cwh_invoice_for_location\merged_pdf'

        # Ensure the output folder exists, create it if not
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        if delivery_type == "cwh_delivery":
            merged_file_name = f"CWH_Delivery_{current_date_time_str}.pdf"
        else:
            merged_file_name = f"Direct_Delivery_{current_date_time_str}.pdf"

        # Output file path for the merged PDF
        output_path = os.path.join(output_folder, merged_file_name)
        # Merge the PDFs
        status, output_path = merge_pdfs(pdf_file_name, output_path)

    except Exception as error:
        print('The cause of error', error)
        status = "failed"

    return status, output_path, merged_file_name


# Direct Delivery code

def fetch_direct_delivery_details_for_loc(month, year):
    df_list: list = []
    food_total: list = []
    total_amount: list = []
    cleaning_total: list = []
    disposal_total: list = []
    other_total: list = []
    formatted_date: None
    delivery_type = "direct_delivery"
    # Create a datetime object
    date_obj = datetime(int(year), int(month), 1)

    # Format the datetime object
    formatted_date = date_obj.strftime("%B-%Y")
    try:
        with get_database_connection_e_eiis() as conn:
            cursor = conn.cursor()
            sql_query = """ SELECT 
                            head.GRN_ID, 
                            head.ORD_LOC_ID, 
                            loc.Location_Name,
                            head.SUPPLIER_ID AS sup_id,
                            sup.Supplier_Name AS sup_name,
                            head.SUPP_DEL_DATE,
                            '' AS STORE_LOC_ID
                        FROM 
                            suppdelhead AS head
                        INNER JOIN 
                            location AS loc ON loc.Location_ID = head.ORD_LOC_ID
                        INNER JOIN
                            suppliers AS sup ON sup.Supplier_ID = head.SUPPLIER_ID
                        LEFT JOIN 
                            entityeiis AS eii ON eii.CWH = head.ORD_LOC_ID
                        WHERE 
                            YEAR(head.PERIOD) = %s 
                            AND MONTH(head.PERIOD) = %s
                            AND eii.CWH IS NULL;
                        """
            cursor.execute(sql_query, (year, month))
            records = cursor.fetchall()
            nested_list = [list(item) for item in records]
            cursor.close()

        if len(nested_list) == 0:
            status = "failed"
            print(f'NO DATA AVAILABLE FOR DIRECT DELIVERY FOR THE SELECTED PERIOD MONTH -> {month} & YEAR -> {year}')
            return (status, df_list, other_total, disposal_total, cleaning_total, food_total, total_amount,
                    nested_list, formatted_date, delivery_type)

        engine = get_database_engine_e_eiis()
        df_query = """
                    SELECT 
                        detail.ITEM_ID, 
                        item.Item_Name, 
                        detail.PACKAGE_ID, 
                        detail.EXPIRY_DATE, 
                        detail.BATCH_NO, 
                        ROUND(detail.QTY_UNIT, 3) AS QTY_UNIT, 
                        ROUND(detail.GP_UNIT, 3) AS GP_UNIT, 
                        ROUND((detail.QTY_UNIT * detail.GP_UNIT), 3) AS Total_Amount, 
                        mstitem.ACCOUNT_NAME 
                    FROM 
                        suppdeldetail AS detail 
                    INNER JOIN 
                        item AS item ON item.Item_ID = detail.ITEM_ID 
                    INNER JOIN 
                        mst_item_account AS mstitem ON mstitem.ACCOUNT_ID = item.Account_ID 
                    WHERE 
                        detail.GRN_ID = %s;  
                   """

        for sublist in nested_list:
            DEL_ID = str(sublist[0])
            # Execute the query and retrieve the data as a DataFrame
            df = pd.read_sql_query(df_query, engine, params=(DEL_ID,))
            items_list = df['ACCOUNT_NAME'].tolist()
            total_value = df['Total_Amount'].tolist()
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

            # Concatenate the two DataFrames horizontally (along the columns)
            concatenated_df = pd.concat([df, df_1], axis=1)
            # Replace NaN values with 0.00
            concatenated_df = concatenated_df.fillna(0.00).infer_objects()
            # Define a dictionary with old names as keys and new names as values
            rename_dict = {
                'ITEM_ID': 'Item Code',
                'Item_Name': 'Item Name',
                'PACKAGE_ID': 'Packing',
                'QTY_UNIT': 'Issue Qty',
                'IP_UNIT': 'Unit Price',
                'EXPIRY_DATE': 'Expiry Date',
                'BATCH_NO': 'Batch No',
                'FOOD': 'Food',
                'CLEANING': 'Cleaning',
                'DISPOSABLES': 'Disposal',
                'Total_Amount': 'Total Amount'
            }

            # Rename the columns using the dictionary
            concatenated_df.rename(columns=rename_dict, inplace=True)
            # Assuming "Total Amount" is the column you want to move
            total_amount_column = concatenated_df.pop("Total Amount")

            # Reinsert the "Total Amount" column at the last position
            concatenated_df["Total Amount"] = total_amount_column
            total_amount_sum = round(concatenated_df['Total Amount'].sum(), 3)
            total_amount.append(total_amount_sum)
            food_total_sum = round(concatenated_df['Food'].sum(), 3)
            food_total.append(food_total_sum)
            Cleaning_total_sum = round(concatenated_df['Cleaning'].sum(), 3)
            cleaning_total.append(Cleaning_total_sum)
            Disposal_total_sum = round(concatenated_df['Disposal'].sum(), 3)
            disposal_total.append(Disposal_total_sum)
            Others_total_sum = round(concatenated_df['Others'].sum(), 3)
            other_total.append(Others_total_sum)
            print(concatenated_df)
            df_list.append(concatenated_df)

        status = "success"

    except Exception as error:
        print('The cause of error -->', error)
        status = "failed"

    return (status, df_list, other_total, disposal_total, cleaning_total, food_total, total_amount,
            nested_list, formatted_date, delivery_type)
