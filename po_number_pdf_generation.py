from datetime import datetime
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm
from reportlab.platypus import Table, TableStyle
from reportlab.lib import colors
import os
import pandas as pd
from database import get_database_connection, get_database_connection_e_eiis


def execute_query(po_num):
    rows_sql_query_pohead = None
    df = None
    total_value = None

    try:
        # Define a context manager for handling the database connection
        with get_database_connection_e_eiis() as conn:
            # Create a cursor object
            cursor = conn.cursor()

            sql_query_pohead = """
            SELECT pohead.Supplier_ID, pohead.Currency_ID, pohead.Currency_Rate, 
                   suppliers.Supplier_Name, suppliers.Address1, suppliers.Fax_No, suppliers.Tel_No, suppliers.No_Days,
                   pohead.Ord_Loc, location.Location_Name, pohead.PO_Date
            FROM pohead
            INNER JOIN suppliers ON pohead.Supplier_ID = suppliers.Supplier_ID 
            INNER JOIN location ON location.Location_ID = pohead.Ord_Loc
            WHERE pohead.Po_Num = %s
            """

            # Execute the query with parameter substitution to avoid SQL injection
            cursor.execute(sql_query_pohead, (po_num,))

            # Fetch all rows from the result set
            rows_sql_query_pohead = cursor.fetchall()

            sql_query_podetail = """
            
            SELECT item.Item_Name, details.PACKAGE_ID, details.QTY, details.ACTUAL_GP
            FROM podetail  as details JOIN item ON item.Item_ID = details.ITEM_ID
            WHERE PO_NUM = %s
            """

            # Execute the query with parameter substitution to avoid SQL injection
            cursor.execute(sql_query_podetail, (po_num,))

            # Fetch all rows from the result set
            rows_sql_query_podetail = cursor.fetchall()

            # Close the cursor (but not the connection, as it will be reused)
            cursor.close()

        # Define the column names
        columns = ['Description', 'Packing', 'Qty.', 'Purch. Price']

        # Convert the result to a DataFrame
        df = pd.DataFrame(rows_sql_query_podetail, columns=columns)

        df['Total Amt.'] = round(df['Qty.'] * df['Purch. Price'], 3)
        df.insert(0, 'Sl No.', range(1, 1 + len(df)))
        total_value = df['Total Amt.'].sum()
        total_value = round(total_value, 3)
        status = "success"

    except Exception as error:
        status = "failed"
        print("The cause of error -->", error)

    return rows_sql_query_pohead, df, total_value, status


def draw_header_details(c, width, height, po_num, currency_rate, address_info, fax_no, tel_no, currency_id, person_name, center_no, location_name, date_of_delivery):
    # Set margins
    left_margin = 11
    right_margin = width - 11
    top_margin = height - 11
    bottom_margin = 11

    # Set the line width for the border
    c.setLineWidth(1)

    # Draw the rectangle for the border
    c.rect(left_margin, bottom_margin, right_margin - left_margin, top_margin - bottom_margin)

    # Rectangle details
    rect_x = left_margin
    rect_width = width - 2 * left_margin
    rect_height = 2.0 * cm
    rect_y = height - left_margin - rect_height

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
    list1 = ["SOCAT LLC", "OMAN", "PURCHASE ORDER"]

    # Calculate the y position to center the text vertically in the middle of the rectangle
    text_y = rect_y + (rect_height - len(list1) * c._leading) / 2

    # Move the text slightly towards the top
    text_y += 1.2 * cm  # Adjust this value as needed

    # Move the text slightly towards the left
    text_x = rect_x + 8 * cm  # Adjust this value as needed

    # Font settings for the text
    c.setFont("Helvetica-Bold", 12)

    # Draw each text element one below the other
    for text in list1:
        c.drawString(text_x, text_y, text)
        text_y -= c._leading  # Move to the next line

    # New box below the existing rectangle
    box_below_x = left_margin  # Start from left margin
    box_below_width = rect_width  # Same width as the above rectangle
    box_below_height = 3.1 * cm  # Increased height of the new box
    box_below_y = rect_y - 3.1 * cm  # Position it directly below the existing rectangle

    # Draw the new box
    c.setLineWidth(1.3)  # Thicker border for the new box
    c.rect(box_below_x, box_below_y, box_below_width, box_below_height, stroke=1, fill=0)

    # Draw vertical line in between the bottom rectangle box
    vertical_line_bottom_x = rect_x + rect_width / 3  # Adjust this value to move the line further left
    vertical_line_bottom_start_y = box_below_y
    vertical_line_bottom_end_y = box_below_y + box_below_height
    c.line(vertical_line_bottom_x, vertical_line_bottom_start_y, vertical_line_bottom_x, vertical_line_bottom_end_y)

    # Font settings for the "PO No:" text
    c.setFont("Helvetica-Bold", 8)  # Set font to Helvetica Bold with size 10 points

    # Add "PO No :" text inside the bottom box
    po_no_text = "PO No       :"
    po_no_text_width = c.stringWidth(po_no_text, "Helvetica-Bold", 8)
    po_no_text_x = box_below_x + 0.5 * cm  # Adjust this value to align the text
    po_no_text_y = box_below_y + box_below_height - 0.7 * cm  # Adjust this value to position the text
    c.drawString(po_no_text_x, po_no_text_y, po_no_text)

    # Add the PO number next to the "PO No :" text
    po_number = str(po_num)
    po_number_width = c.stringWidth(po_number, "Helvetica-Bold", 8)
    po_number_x = po_no_text_x + po_no_text_width + 0.2 * cm  # Adjust this value for spacing
    po_number_y = po_no_text_y
    c.drawString(po_number_x, po_number_y, po_number)

    # Add "Center No :" text inside the bottom box
    center_no_text = "Center No :"
    center_no_text_width = c.stringWidth(center_no_text, "Helvetica-Bold", 8)
    center_no_text_x = box_below_x + 0.5 * cm  # Adjust this value to align the text
    center_no_text_y = box_below_y + box_below_height - 1.25 * cm  # Adjust this value to position the text
    c.drawString(center_no_text_x, center_no_text_y, center_no_text)

    # Font settings for the center number
    c.setFont("Helvetica", 8)  # Set font to Helvetica with size 8 points

    # Add the center number next to the "Center No :" text
    center_number = str(center_no)
    center_number_width = c.stringWidth(center_number, "Helvetica", 8)
    center_number_x = center_no_text_x + center_no_text_width + 0.2 * cm  # Adjust this value for spacing
    center_number_y = center_no_text_y
    c.drawString(center_number_x, center_number_y, center_number)

    c.setFont("Helvetica-Bold", 8)
    # Add "Deliver To:" text inside the bottom box
    Deliver_text = "Deliver To :"
    Deliver_text_width = c.stringWidth(Deliver_text, "Helvetica-Bold", 8)
    Deliver_text_text_x = box_below_x + 0.5 * cm  # Adjust this value to align the text
    Deliver_text_text_y = box_below_y + box_below_height - 1.80 * cm  # Adjust this value to position the text
    c.drawString(Deliver_text_text_x, Deliver_text_text_y, Deliver_text)

    c.setFont("Helvetica", 8)
    # Add the center number next to the "Deliver To:" text
    Deliver_number = str(location_name)
    Deliver_number_width = c.stringWidth(Deliver_number, "Helvetica", 8)
    Deliver_number_x = Deliver_text_text_x + Deliver_text_width + 0.2 * cm  # Adjust this value for spacing
    Deliver_number_y = Deliver_text_text_y
    c.drawString(Deliver_number_x, Deliver_number_y, Deliver_number)

    c.setFont("Helvetica-Bold", 8)
    # Add "Supplier ID:" text inside the bottom box
    supplier_text = "Supplier :"
    supplier_text_width = c.stringWidth(supplier_text, "Helvetica-Bold", 8)
    supplier_text_x = box_below_x + box_below_width - supplier_text_width - 10.5 * cm  # Adjust this value to align
    # the text to the right
    supplier_text_y = box_below_y + box_below_height - 0.5 * cm  # Adjust this value to position the text
    c.drawString(supplier_text_x, supplier_text_y, supplier_text)

    c.setFont("Helvetica", 7)
    # Add the Supplier ID next to the "Supplier ID:" text
    supplier_id = address_info
    supplier_id_lines = supplier_id.split('\n')  # Split the string into lines
    supplier_id_y = supplier_text_y - 0.4 * cm  # Start just below "Supplier :"

    # Draw each line of supplier ID below "Supplier :"
    for line in supplier_id_lines:
        c.drawString(supplier_text_x, supplier_id_y, line)
        supplier_id_y -= 0.4 * cm  # Move to the next line

    c.setFont("Helvetica-Bold", 8)

    # Add "Attn:" text inside the bottom box
    attn_text = "Attn        :"
    attn_text_width = c.stringWidth(attn_text, "Helvetica-Bold", 8)
    attn_text_x = supplier_text_x  # Align with "Supplier :" text
    attn_text_y = supplier_id_y - 0.1 * cm  # Move down from Supplier ID
    c.drawString(attn_text_x, attn_text_y, attn_text)

    c.setFont("Helvetica", 8)
    # Add the Attn name next to the "Attn:" text
    attn_name = "John Doe"
    attn_name_width = c.stringWidth(attn_name, "Helvetica", 8)
    attn_name_x = attn_text_x + attn_text_width + 0.2 * cm  # Adjust this value for spacing
    attn_name_y = attn_text_y
    c.drawString(attn_name_x, attn_name_y, attn_name)

    c.setFont("Helvetica-Bold", 8)
    # Add "Currency :" text inside the bottom box
    currency_text = "Currency :"
    currency_text_width = c.stringWidth(currency_text, "Helvetica-Bold", 8)
    currency_text_x = supplier_text_x  # Adjust this value to align the text
    currency_text_y = supplier_id_y - 0.6 * cm  # Adjust this value to position the text
    c.drawString(currency_text_x, currency_text_y, currency_text)

    c.setFont("Helvetica", 8)
    # Add the Currency next to the "Currency :" text
    currency = currency_id
    currency_width = c.stringWidth(currency, "Helvetica-Bold", 8)
    currency_x = currency_text_x + currency_text_width + 0.2 * cm  # Adjust this value for spacing
    currency_y = currency_text_y
    c.drawString(currency_x, currency_y, currency)

    c.setFont("Helvetica-Bold", 8)
    # Add "Rate:" text inside the bottom box
    rate_text = "Rate:"
    rate_text_width = c.stringWidth(rate_text, "Helvetica-Bold", 8)
    rate_text_x = box_below_x + box_below_width - rate_text_width - 7.5 * cm  # Align with the right side of the
    # bottom rectangle box
    rate_text_y = box_below_y + box_below_height - 2.7 * cm  # Adjust this value to position the text
    c.drawString(rate_text_x, rate_text_y, rate_text)

    c.setFont("Helvetica", 8)
    # Add the rate value next to the "Rate:" text
    rate_value = str(currency_rate)
    rate_value_width = c.stringWidth(rate_value, "Helvetica", 8)
    rate_value_x = rate_text_x + rate_text_width + 0.2 * cm  # Adjust this value for spacing
    rate_value_y = rate_text_y
    c.drawString(rate_value_x, rate_value_y, rate_value)

    c.setFillColorRGB(0, 0, 0.5)  # Dark blue color

    # Add "Tel.:" text inside the bottom box
    tel_text = "Tel. :"
    tel_text_width = c.stringWidth(tel_text, "Helvetica-Bold", 8)
    tel_text_x = box_below_x + box_below_width - tel_text_width - 4 * cm  # Align with the right side of the bottom
    # rectangle box
    tel_text_y = box_below_y + box_below_height - 0.5 * cm  # Adjust this value to position the text
    c.drawString(tel_text_x, tel_text_y, tel_text)

    c.setFillColorRGB(0, 0, 0)  # Reset the fill color to black for other text

    # Add the tel. value next to the "Tel.:" text
    tel_value = str(tel_no)
    tel_value_width = c.stringWidth(tel_value, "Helvetica", 8)
    tel_value_x = tel_text_x + tel_text_width + 0.2 * cm  # Adjust this value for spacing
    tel_value_y = tel_text_y
    c.drawString(tel_value_x, tel_value_y, tel_value)

    c.setFillColorRGB(0, 0, 0.5)  # Dark blue color

    # Add "Fax:" text below the "Tel.:" text
    fax_text = "Fax :"
    fax_text_width = c.stringWidth(fax_text, "Helvetica-Bold", 8)
    fax_text_x = tel_text_x  # Align with "Tel.:" text
    fax_text_y = tel_text_y - 0.5 * cm  # Move down from "Tel.:" text
    c.drawString(fax_text_x, fax_text_y, fax_text)

    c.setFillColorRGB(0, 0, 0)  # Reset the fill color to black for other text

    # Add the fax value next to the "Fax:" text
    fax_value = str(fax_no)
    fax_value_width = c.stringWidth(fax_value, "Helvetica", 8)
    fax_value_x = fax_text_x + fax_text_width + 0.2 * cm  # Adjust this value for spacing
    fax_value_y = fax_text_y
    c.drawString(fax_value_x, fax_value_y, fax_value)

    # Add "Generated by: {person_name}" on the left side
    generated_by_text = f"Generated by: {person_name}"
    generated_by_x = left_margin
    generated_by_y = bottom_margin - 0.2 * cm  # Adjust the position as needed
    c.setFont("Helvetica", 8)
    c.drawString(generated_by_x, generated_by_y, generated_by_text)

    # Add current date on the right side
    current_date = datetime.now().strftime("%B %d, %Y")
    current_date_text = f"{current_date}"
    current_date_x = right_margin - c.stringWidth(current_date_text, "Helvetica", 8)
    c.drawString(current_date_x, generated_by_y, current_date_text)

    return box_below_y, bottom_margin, left_margin, right_margin, top_margin, rect_width, box_below_x, rect_x


def create_pdf(rows_sql_query_pohead_list, total_value, df, po_num):
    file_path = None
    person_name = "Administrator"
    try:
        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = str(po_num) + "_" + current_datetime + ".pdf"
        directory_path = r"C:\Users\Administrator\Downloads\eiis"

        supplier_id = rows_sql_query_pohead_list[0]
        currency_id = rows_sql_query_pohead_list[1]
        currency_rate = rows_sql_query_pohead_list[2]
        supplier_name = rows_sql_query_pohead_list[3]
        address = rows_sql_query_pohead_list[4]
        fax_no = rows_sql_query_pohead_list[5]
        tel_no = rows_sql_query_pohead_list[6]
        Term_of_Payment = rows_sql_query_pohead_list[7]
        center_no = rows_sql_query_pohead_list[8]
        location_name = rows_sql_query_pohead_list[9]
        date_object = rows_sql_query_pohead_list[10]
        # Format the date with abbreviated month name
        date_of_delivery = date_object.strftime("%d-%b-%Y")

        if fax_no is None:
            print(fax_no)
            fax_no = ""
        if tel_no is None:
            tel_no = ""

        address_info = supplier_id + "-" + supplier_name + "\n" + address
        print(address_info)

        # Combine the directory path and file name
        file_path = os.path.join(directory_path, file_name)
        # Create a canvas
        c = canvas.Canvas(file_path, pagesize=letter)
        width, height = letter  # Size of page (letter size)
        box_below_y, bottom_margin, left_margin, right_margin, top_margin, rect_width, box_below_x, rect_x = draw_header_details(
            c, width, height, po_num, currency_rate, address_info, fax_no, tel_no, currency_id, person_name, center_no, location_name, date_of_delivery)

        row_height = 18.19685

        # Calculate available vertical space
        available_space = box_below_y - bottom_margin

        # Calculate the number of rows that can fit within the available space
        rows_per_chunk = int(available_space / row_height)

        # Adjust rows_per_chunk to account for not splitting header and data rows unevenly
        # If the header is always included and needs one row by itself:
        rows_per_chunk -= 1  # Adjust for the header if it is included in each chunk
        print('rows_per_chunk', rows_per_chunk)

        chunks = [df[i:i + rows_per_chunk] for i in range(0, len(df), rows_per_chunk)]

        for i, chunk in enumerate(chunks):
            table_y = box_below_y  # Start the table below the second rectangle

            df_data = chunk.values.tolist()
            df_headers = chunk.columns.tolist()
            col_widths = [35, 250, 100, 59, 75, 70]  # Adjust these values as needed for each column
            df_table = Table([df_headers] + df_data, colWidths=col_widths)

            df_table.setStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.white),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 10),
                ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('ALIGN', (-3, 1), (-1, -1), 'RIGHT'),
                ('ALIGN', (1, 1), (2, -1), 'LEFT'),
                ('VALIGN', (0, 1), (-1, -1), 'MIDDLE'),
                ('LEFTPADDING', (0, 0), (-1, -1), 3),
                ('RIGHTPADDING', (0, 0), (-1, -1), 3),
                ('FONTSIZE', (0, 1), (-1, -1), 7.5),
                ('WORDWRAP', (1, 1), (1, -1), 'LTR'),  # Enforce word wrap in the Description column
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
                box_below_y, bottom_margin, left_margin, right_margin, top_margin, rect_width, box_below_x, rect_x = draw_header_details(
                    c, width, height, po_num, currency_rate, address_info, fax_no, tel_no, currency_id, person_name,
                    center_no, location_name, date_of_delivery)

            else:
                current_y_position = table_y - df_table_height
                available_space = current_y_position - bottom_margin
                print("available_space", available_space)

                if available_space < 2 * cm:
                    c.showPage()
                    # Header and bottom details
                    box_below_y, bottom_margin, left_margin, right_margin, top_margin, rect_width, box_below_x, rect_x = draw_header_details(
                        c, width, height, po_num, currency_rate, address_info, fax_no, tel_no, currency_id, person_name,
                        center_no, location_name, date_of_delivery)

                # Calculate the position for the "Grand Total:" text within the rectangle box
                c.setFont("Helvetica-Bold", 10)
                grand_total_text = "Grand Total: " + currency_id  # Text to display
                grand_total_text_width = c.stringWidth(grand_total_text, "Helvetica", 10)
                grand_total_text_x = left_margin + 20.5 * cm - grand_total_text_width - 0.5 * cm - 3 * cm  # Adjust this value for spacing
                grand_total_text_y = current_y_position - 0.5 * cm  # Adjust this value for spacing

                # Add the "Grand Total:" text to the canvas
                c.drawString(grand_total_text_x, grand_total_text_y, grand_total_text)

                # Calculate the position for entering the amount (with some space)
                amount_text_x = grand_total_text_x + grand_total_text_width + 0.5 * cm  # Adjust this value for spacing
                amount_text_y = grand_total_text_y - 0.5 * cm  # Move up to position above the underline

                # Add some space for entering the amount
                space_for_amount = " " * 10  # Example space for entering the amount
                c.drawString(amount_text_x, amount_text_y, space_for_amount)

                # Draw underline for the space reserved for entering the amount
                underline_y = amount_text_y - 0.001 * cm  # Adjust this value for the position of the underline
                underline_start_x = amount_text_x
                underline_end_x = amount_text_x + 2.7 * cm  # Adjust this value for the width of the underline
                c.line(underline_start_x, underline_y, underline_end_x, underline_y)

                # Calculate the position for the total amount text
                total_amount = total_value  # Example total amount value
                total_amount_text = f"{total_amount:.3f}"  # Format the total amount with two decimal places
                total_amount_text_width = c.stringWidth(total_amount_text, "Helvetica", 8)
                total_amount_text_x = underline_end_x + -1.4 * cm  # Adjust this value for spacing
                total_amount_text_y = underline_y + 0.3 * cm  # Move up to be above the underline

                # Add the total amount text to the canvas
                c.drawString(total_amount_text_x, total_amount_text_y, total_amount_text)

                # Insert "Term of Payment" label in bold
                term_of_payment = str(Term_of_Payment)  # Example term of payment
                terms_label_x = left_margin + 0.7 * cm  # Align with the left margin
                terms_text_y = bottom_margin + 1.5 * cm  # Slightly above the bottom margin
                c.setFont("Helvetica-Bold", 10)  # Bold font for the label
                c.drawString(terms_label_x, terms_text_y, 'Term of Payment        : ')

                # Calculate the width of the "Term of Payment:" label to position the actual terms
                label_width = c.stringWidth('Term of Payment: ', "Helvetica-Bold", 10)

                # Insert the term of payment in normal font
                terms_value_x = terms_label_x + label_width + 24  # Start right after the bold label
                c.setFont("Helvetica", 10)  # Normal font for the term of payment
                c.drawString(terms_value_x, terms_text_y, term_of_payment)

                # Insert "Documents Required" label and value
                documents_required = "Invoice"  # Example documents required
                documents_label_x = rect_x + 0.7 * cm  # Align with the left margin
                documents_text_y = terms_text_y - 25  # Position below "Term of Payment:"
                c.setFont("Helvetica-Bold", 10)  # Bold font for the label
                c.drawString(documents_label_x, documents_text_y, 'Documents Required :')

                # Calculate the width of the "Documents Required:" label to position the actual document names
                documents_label_width = c.stringWidth('Documents Required:', "Helvetica-Bold", 10)

                # Insert the documents required in normal font
                documents_value_x = documents_label_x + documents_label_width + 8  # Start right after the bold label
                c.setFont("Helvetica", 10)  # Normal font for the documents required
                c.drawString(documents_value_x, documents_text_y, documents_required)

                # Add "(PURCHASE DEPARTMENT)" on the right side
                purchase_department_text = "(PURCHASE DEPARTMENT)"
                purchase_department_x = right_margin - c.stringWidth(purchase_department_text, "Helvetica-Bold",
                                                                     10) - 8  # Adjust the position as needed
                c.setFont("Helvetica-Bold", 10)  # Bold font for the purchase department text
                c.drawString(purchase_department_x, documents_text_y, purchase_department_text)

        # Save the PDF
        c.showPage()
        c.save()
        status = "success"
    except Exception as error:
        print("The cause of error -->", error)
        status = "failed"

    return status, file_path, file_name

