import pymysql
from sqlalchemy import create_engine
from datetime import datetime
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm
from reportlab.platypus import Table, TableStyle, Paragraph
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib import colors
import os
import pandas as pd
from datetime import datetime
from database import get_database_connection_e_eiis, get_database_engine_e_eiis
from collections import Counter


def fetch_del_details(year, month):
    # It's good practice to initialize these at the top to ensure they exist if an error occurs early
    nested_list = []
    arranged_nested_list = []
    arranged_df_list = []
    formatted_date = []
    arranged_total_list = []
    counts_list = []
    status = "success"  # assume success unless something fails

    try:
        with get_database_connection_e_eiis() as conn:
            cursor = conn.cursor()
            query = """
                    SELECT head.GRN_ID, head.SUPPLIER_ID, sup.Supplier_Name, head.SUPP_DEL_DATE, head.SUPP_DEL_NOTE_NO, 
                    head.ORD_LOC_ID, loc.Location_Name FROM suppdelhead AS head 
                    INNER JOIN suppliers AS sup ON sup.Supplier_ID = head.SUPPLIER_ID 
                    INNER JOIN location AS loc ON loc.Location_ID = head.ORD_LOC_ID
                    WHERE YEAR(head.PERIOD) = %s AND MONTH(head.PERIOD) = %s;
                    """
            cursor.execute(query, (year, month))
            records = cursor.fetchall()
            nested_list = [list(item) for item in records]
            cursor.close()

        engine = get_database_engine_e_eiis()
        df_list = []
        total_list = []
        for sublist in nested_list:
            GRN_ID = str(sublist[0])
            df = pd.read_sql_query(f"""
                                   SELECT details.ITEM_ID, item.Item_Name, details.PACKAGE_ID, details.QTY, details.GP, 
                                   details.ACTUAL_INV, details.EXPIRY_DATE FROM suppdeldetail AS details 
                                   INNER JOIN item AS item ON item.Item_ID = details.ITEM_ID 
                                   WHERE details.GRN_ID = %s;
                                   """, engine, params=(GRN_ID,))
            df.rename(columns={
                'ITEM_ID': 'Item Code',
                'Item_Name': 'Item Name',
                'PACKAGE_ID': 'Packing',
                'QTY': 'Qty',
                'ACTUAL_INV': 'Total',
                'EXPIRY_DATE': 'Expiry Date'
            }, inplace=True)
            total_list.append(df['Total'].sum())
            df_list.append(df)

        # Handling dates and rearranging lists based on your existing logic
        date_obj = datetime(int(year), int(month), 1)
        formatted_date = date_obj.strftime("%B-%Y")
        supplier_ids = [sublist[1] for sublist in nested_list]
        indices_dict = {id_: [] for id_ in supplier_ids}
        for idx, sublist in enumerate(nested_list):
            for id_ in sublist:
                if id_ in supplier_ids:
                    indices_dict[id_].append(idx)
        flat_list = [i for sublist in indices_dict.values() for i in sublist]
        arranged_nested_list = [nested_list[i] for i in flat_list]
        arranged_df_list = [df_list[i] for i in flat_list]
        arranged_total_list = [total_list[i] for i in flat_list]
        # Extracting the second element of each sublist
        sup_id_new_list = [sublist[1] for sublist in arranged_nested_list]
        # Dictionary to hold the sum of totals for each sup_id
        sup_id_totals = {}

        # Loop through the sup_id and total lists
        for sid, amt in zip(sup_id_new_list, arranged_total_list):
            if sid in sup_id_totals:
                sup_id_totals[sid] += amt
            else:
                sup_id_totals[sid] = amt

        # Extract the totals into a list
        total_sums_list = list(sup_id_totals.values())
        # Use Counter to count occurrences of each value
        counted_values = Counter(sup_id_new_list)

        # Extract the counts as a list
        counts_list = list(counted_values.values())

    except Exception as error:
        print('The cause of error -->', error)
        status = "failed"

    return (status, arranged_nested_list, arranged_df_list, formatted_date, arranged_total_list,
            total_sums_list, counts_list)


def draw_header_and_details(c, width, height, formatted_date, person_name):
    left_margin = 18
    right_margin = width - 18
    top_margin = height - 18
    bottom_margin = 18

    c.setLineWidth(1)
    c.rect(left_margin, bottom_margin, right_margin - left_margin, top_margin - bottom_margin, )

    # Rectangle details
    rect_x = left_margin  # Start from left margin
    rect_width = width - 2 * left_margin  # Maintain margins on both sides
    rect_height = 2.5 * cm
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
    third_elemnt = f"Supplier Delivery Details By Delivery {formatted_date}"
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

    # Return the required values
    return rect_y, top_margin, left_margin, bottom_margin


def create_supplier_delivery_details_pdf(arranged_nested_list, arranged_df_list, formatted_date, arranged_total_list,
                                         total_sums_list, counts_list):
    try:
        # print(arranged_nested_list)
        pdf_file_name = []
        page = 0
        person_name = "administrator"
        fixed_table_len = 31
        supplier_total = 0
        previous_supplier_ID = None
        current_datetime = datetime.now()
        # Format the current date and time as desired (example: YYYY-MM-DD_HH-MM-SS)
        current_date_time_str = current_datetime.strftime("%Y_%m_%d_%H_%M_%S")
        file_name = f'{page}_{current_date_time_str}.pdf'

        directory_path = r"C:\Users\Administrator\Downloads\eiis\supllier_delivery_details_PDF"
        file_with_path = directory_path + "\\" + file_name
        pdf_file_name.append(file_with_path)
        # Combine the directory path and file name
        file_path = os.path.join(directory_path, file_name)

        # Create a canvas
        c = canvas.Canvas(file_path, pagesize=letter)
        width, height = letter  # Size of page (letter size)

        # Header and bottom details
        rect_y, top_margin, left_margin, bottom_margin = draw_header_and_details(c, width, height, formatted_date,
                                                                                 person_name)
        # Initialize content position
        current_y_position = rect_y - 0.5 * cm
        supplier_total = 0
        grand_tot_index = 0
        count_index = 0
        list_len = (len(arranged_nested_list))
        for index, (nested_list, df, total) in enumerate(zip(arranged_nested_list, arranged_df_list, arranged_total_list)):
            list_to_be_added = []
            # print(list_len)
            grn_id = str(nested_list[0])
            # print(grn_id)
            supplier_id = str(nested_list[1])
            # print(supplier_id)
            supplier_name = str(nested_list[2])
            # print(supplier_name)
            del_date = nested_list[3]
            # print(del_date)
            formatted_del_date = del_date.strftime("%d-%b-%Y")
            sup_note = str(nested_list[4])
            loc_id = str(nested_list[5])
            loc_name = str(nested_list[6])
            df_value = df
            df_len = len(df_value) + 3
            # print(df_len)
            total_value = total
            supplier_total += total_value
            # print(df_value)
            page += 1
            new_table_length = fixed_table_len - df_len
            list_len -= 1

            if previous_supplier_ID == None or previous_supplier_ID == supplier_id:
                pass
            elif list_len == 0:
                print("yes")
                supplier_total = 0

            else:
                supplier_total = 0

            # Known row height in points
            row_height = 20.19685
            # Calculate available vertical space
            available_space = current_y_position - bottom_margin
            print('available_space', available_space)
            # Calculate the number of rows that can fit within the available space
            rows_per_chunk = int(available_space / row_height)
            rows_per_chunk -= 1  # Adjust for the header if it is included in each chunk
            if available_space < 4 * cm:
                c.showPage()
                # Header and bottom details
                rect_y, top_margin, left_margin, bottom_margin = draw_header_and_details(c, width, height, formatted_date,
                                                                                         person_name)
                # Initialize content position
                current_y_position = rect_y - 0.5 * cm
                # Calculate available vertical space
                available_space = current_y_position - bottom_margin
                print('available_space', available_space)
                # Calculate the number of rows that can fit within the available space
                rows_per_chunk = int(available_space / row_height)
                rows_per_chunk -= 1  # Adjust for the header if it is included in each chunk

            c.setFont("Helvetica-Bold", 8.5)
            c.drawString(left_margin + 0.5 * cm, current_y_position, "Our GRN : ")
            c.setFont("Helvetica", 8.5)
            c.drawString(left_margin + 2.2 * cm, current_y_position, grn_id)
            c.setFont("Helvetica-Bold", 8.5)
            c.drawString(left_margin + 6 * cm, current_y_position, "Delivery Date : ")
            c.setFont("Helvetica", 8.5)
            c.drawString(left_margin + 8.5 * cm, current_y_position, formatted_del_date)
            c.setFont("Helvetica-Bold", 9)
            c.drawString(left_margin + 12 * cm, current_y_position, "Supplier Delivery Note No : ")
            c.setFont("Helvetica", 8.5)
            c.drawString(left_margin + 16.8 * cm, current_y_position, sup_note)

            current_position = current_y_position - 0.5 * cm

            c.setFont("Helvetica-Bold", 8.5)
            c.drawString(left_margin + 0.5 * cm, current_position, "Location ID : ")
            c.setFont("Helvetica", 8.5)
            c.drawString(left_margin + 2.5 * cm, current_position, f"{loc_id} - {loc_name}")
            # c.setFont("Helvetica", 9)
            # c.drawString(left_margin + 5.2 * cm, current_position, loc_name)
            c.setFont("Helvetica-Bold", 8.5)
            c.drawString(left_margin + 12 * cm, current_position, "Supplier ID : ")
            c.setFont("Helvetica", 8.5)
            c.drawString(left_margin + 14 * cm, current_position, f"{supplier_id} - {supplier_name}")
            # c.setFont("Helvetica", 9)
            # c.drawString(left_margin + 15.8 * cm, current_position, supplier_name)

            current_position = current_position - 0.5 * cm

            # Split the DataFrame into the first half and the second half
            first_half = df.iloc[:rows_per_chunk]
            print(first_half)
            second_half = df.iloc[rows_per_chunk:]
            sup_id_count = counts_list[count_index]
            counts_list[count_index] = counts_list[count_index] - 1
            # print('Len of first DF', len(first_half))
            # print('Len of second DF', len(second_half))
            table_y = current_position
            df_data = first_half.values.tolist()
            df_headers = first_half.columns.tolist()

            df_table = Table([df_headers] + df_data,
                             colWidths=[1.6 * cm, 5.0 * cm, 3.8 * cm, 2.8 * cm, 2.2 * cm, 2.5 * cm, 2.4 * cm])

            df_table.setStyle(TableStyle([
                ('BACKGROUND', (1, 1), (-1, 1), colors.white),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 0.2),
                ('FONTSIZE', (0, 0), (-1, 0), 6.5),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('ALIGN', (1, 1), (1, -1), 'LEFT'),
                ('ALIGN', (3, 1), (3, -1), 'RIGHT'),  # 4th column
                ('ALIGN', (4, 1), (4, -1), 'RIGHT'),  # 5th column
                ('ALIGN', (5, 1), (5, -1), 'RIGHT'),  # 6th column
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('FONTSIZE', (0, 1), (-1, -1), 6)
            ]))
            # Calculate the height of the table
            df_table_height = df_table.wrap(0, 0)[1]
            current_y_table_position = table_y - df_table_height
            df_table.drawOn(c, left_margin, current_y_table_position)  # Draw the DataFrame table
            grand_tot = total_sums_list[grand_tot_index]
            # print('count_index', count_index)

            print(sup_id_count)
            if len(second_half) == 0:
                # print('yes')
                current_position_after_table = current_y_table_position - 0.5 * cm
                c.setFont("Helvetica-Bold", 9)
                c.drawString(left_margin + 13.9 * cm, current_position_after_table, "Total :")
                c.setFont("Helvetica", 9)
                total_value = round(total_value, 3)
                c.drawString(left_margin + 15.8 * cm, current_position_after_table, str(total_value))
                current_y_position = current_position_after_table - 0.5 * cm

                # print("sup_id_count", sup_id_count)
                if sup_id_count == 1:
                    # print("Not SAME ID")
                    # print(grand_tot_index)
                    # print(grand_tot)
                    current_y_position = current_y_position - 0.2 * cm
                    c.setFont("Helvetica-Bold", 9)
                    c.drawString(left_margin + 12 * cm, current_y_position, "Supplier Total :")
                    c.setFont("Helvetica", 9)
                    grand_tot = round(grand_tot, 3)
                    c.drawString(left_margin + 15.8 * cm, current_y_position, str(grand_tot))
                    current_y_position = current_y_position - 0.5 * cm
                    grand_tot_index += 1
                    count_index += 1


            else:
                index += 1
                list_to_be_added.append(grn_id)
                list_to_be_added.append(supplier_id)
                list_to_be_added.append(supplier_name)
                list_to_be_added.append(del_date)
                list_to_be_added.append(sup_note)
                list_to_be_added.append(loc_id)
                list_to_be_added.append(loc_name)
                arranged_nested_list.insert(index, list_to_be_added)
                arranged_df_list.insert(index, second_half)
                arranged_total_list.insert(index, total)
                total_sums_list.insert(index, grand_tot)
                sup_id_count = sup_id_count + 1
                counts_list.insert(index, sup_id_count)
                c.showPage()
                rect_y, top_margin, left_margin, bottom_margin = draw_header_and_details(c, width, height, formatted_date,
                                                                                         person_name)
                current_y_position = rect_y - 0.5 * cm

        c.save()
        status = "success"
        # print(supplier_total)
    except Exception as error:
        print('The cause of error', error)
        status = "failed"

    return status, file_name, file_with_path