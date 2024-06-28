from datetime import datetime
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm
from reportlab.platypus import Table, TableStyle
from reportlab.lib import colors
import os
import pandas as pd
import PyPDF2
from database import get_database_connection


def execute_date_query(start_date, end_date):
    nested_po_heads = None
    df_list = None
    total_value_list = None
    po_number = None

    try:
        with get_database_connection() as conn:
            # Create a cursor object
            cursor = conn.cursor()

            sql_query_pohead = """
            SELECT pohead.PoNum, pohead.SupplierID, pohead.CurrencyId, pohead.CurrencyRate, 
                   suppliers.SupplierName, suppliers.Address1, suppliers.FaxNo, suppliers.TelNo, suppliers.NoDays
            FROM pohead
            INNER JOIN suppliers ON pohead.SupplierID = suppliers.SupplierID
            WHERE pohead.PODate BETWEEN %s AND %s
                """

            # Execute the query with parameter substitution to avoid SQL injection
            cursor.execute(sql_query_pohead, (start_date, end_date))
            # Fetch all rows from the result set
            po_heads = cursor.fetchall()
            nested_po_heads = [list(item) for item in po_heads]

            if len(nested_po_heads) == 0:  # To check whether data available in between dates
                status = "failed"
                message = "No data available for selected period"
                return nested_po_heads, df_list, total_value_list, po_number, status, message

            po_number = [sublist[0] for sublist in nested_po_heads]
            # Process each purchase order to fetch details
            df_list: list = []
            total_value_list: list = []
            for po_head in nested_po_heads:
                po_num = po_head[0]  # Assuming PoNum is the first column in the result
                sql_query_podetail = """
                    SELECT ItemID, PackageId, Qty, ActualGP
                    FROM podetail
                    WHERE PoNum = %s
                    """
                cursor.execute(sql_query_podetail, (po_num,))
                rows_sql_query_podetail = cursor.fetchall()
                # Define the column names
                columns = ['Description', 'Packing', 'Qty.', 'Purch. Price']

                # Convert the result to a DataFrame
                df = pd.DataFrame(rows_sql_query_podetail, columns=columns)

                df['Total Amt.'] = round(df['Qty.'] * df['Purch. Price'], 3)

                df.insert(0, 'Sl No.', range(1, 1 + len(df)))

                total_value = df['Total Amt.'].sum()
                rounded_total_value = round(total_value, 3)
                df_list.append(df)
                total_value_list.append(rounded_total_value)
                status = "success"
                message = "success"
    except Exception as error:
        print('The cause of error -->', error)
        status = "failed"
        message = "Error while fetching data from MY SQL"

    return nested_po_heads, df_list, total_value_list, po_number, status, message


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

    return status, output_path


def create_merged_pdf(nested_po_heads, df_list, total_value_list, po_number, start_date, end_date):
    pdf_file_name = []
    page_num = 0
    total_value_length = len(po_number)
    try:
        for po_num, po_head, df, total_value in zip(po_number, nested_po_heads, df_list, total_value_list):
            print(total_value_length)
            page_num += 1
            print(page_num)
            # Create a list of lists from the DataFrame
            data_list = [df.columns.tolist()] + df.values.tolist()

            # Convert all elements to string and format as required
            formatted_data_list = []
            for row in data_list:
                formatted_row = [str(item) for item in row]
                formatted_data_list.append(formatted_row)

            # Generate filename with date and time
            current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
            directory_path = r"C:\Users\Administrator\Downloads\eiis\single_pdf"
            file_name = str(po_num) + "_" + current_datetime + ".pdf"
            file_with_path = directory_path + "\\" + file_name
            pdf_file_name.append(file_with_path)

            supplier_id = po_head[0]
            currency_id = po_head[1]
            currency_rate = po_head[2]
            supplier_name = po_head[3]
            address = po_head[4]
            fax_no = po_head[5]
            tel_no = po_head[6]
            Term_of_Payment = po_head[7]

            address_info = supplier_id + "- " + supplier_name + "\n" + address

            # Combine the directory path and file name
            file_path = os.path.join(directory_path, file_name)
            # Create a canvas
            c = canvas.Canvas(file_path, pagesize=letter)
            width, height = letter  # Size of page (letter size)

            # Set margins
            margin = 1.5 * cm

            # Rectangle details
            rect_x = margin  # Start from left margin
            rect_width = width - 2 * margin  # Maintain margins on both sides
            rect_height = 2.5 * cm
            rect_y = height - margin - rect_height  # Positioned from the top

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
            box_below_x = margin  # Start from left margin
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
            c.line(vertical_line_bottom_x, vertical_line_bottom_start_y, vertical_line_bottom_x,
                   vertical_line_bottom_end_y)

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
            center_number = "A19008"
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
            Deliver_number = "SOCAT BASE CAMP"
            Deliver_number_width = c.stringWidth(Deliver_number, "Helvetica", 8)
            Deliver_number_x = Deliver_text_text_x + Deliver_text_width + 0.2 * cm  # Adjust this value for spacing
            Deliver_number_y = Deliver_text_text_y
            c.drawString(Deliver_number_x, Deliver_number_y, Deliver_number)

            c.setFont("Helvetica-Bold", 8)
            # Add "Supplier ID:" text inside the bottom box
            supplier_text = "Supplier :"
            supplier_text_width = c.stringWidth(supplier_text, "Helvetica-Bold", 8)
            supplier_text_x = box_below_x + box_below_width - supplier_text_width - 10.5 * cm  # Adjust this value to align the text to the right
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
            rate_text_x = box_below_x + box_below_width - rate_text_width - 7.5 * cm  # Align with the right side of the bottom rectangle box
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
            tel_text_x = box_below_x + box_below_width - tel_text_width - 4 * cm  # Align with the right side of the bottom rectangle box
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

            style = TableStyle([
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

            col_widths = [35, 208, 100, 39, 75, 70]  # Adjust these values as needed for each column

            # Create table object with column widths
            table = Table(formatted_data_list, colWidths=col_widths)

            # Add style to the table
            table.setStyle(style)

            # Get total height of the table
            table.wrapOn(c, width, height)
            table_height = table._height

            # Calculate the position where the table ends
            table_end_y = box_below_y - table_height - 0.01 * cm

            # Calculate the dimensions and position of the rectangle box below the table
            c.setFont("Helvetica-Bold", 10)

            box_below_table_x = margin  # Start from left margin
            box_below_table_width = rect_width  # Same width as the above rectangle
            box_below_table_height = 4 * cm  # Adjust the height of the new box as needed
            box_below_table_y = table_end_y - box_below_table_height  # Position it below the table

            # Draw the rectangle box below the table
            c.setLineWidth(1.3)  # Thicker border for the new box
            c.rect(box_below_table_x, box_below_table_y, box_below_table_width, box_below_table_height, stroke=1,
                   fill=0)

            # Calculate the position for the "Grand Total:" text within the rectangle box
            c.setFont("Helvetica-Bold", 10)
            grand_total_text = "Grand Total: " + currency_id  # Text to display
            grand_total_text_width = c.stringWidth(grand_total_text, "Helvetica", 10)
            grand_total_text_x = box_below_table_x + box_below_table_width - grand_total_text_width - 0.5 * cm - 3 * cm  # Adjust this value for spacing
            grand_total_text_y = box_below_table_y + box_below_table_height - 0.5 * cm  # Adjust this value for spacing

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
            total_amount_text = f"{total_amount:.2f}"  # Format the total amount with two decimal places
            total_amount_text_width = c.stringWidth(total_amount_text, "Helvetica", 8)
            total_amount_text_x = underline_end_x + -1.4 * cm  # Adjust this value for spacing
            total_amount_text_y = underline_y + 0.3 * cm  # Move up to be above the underline

            # Add the total amount text to the canvas
            c.drawString(total_amount_text_x, total_amount_text_y, total_amount_text)

            # Calculate the position for the "Date of Delivery:" text within the rectangle box
            c.setFont("Helvetica", 10)  # Set font to normal weight
            date = "25-Apr-2024"
            delivery_date_label = "Date of Delivery          :"  # Text label to display
            delivery_date_text = f"{delivery_date_label} {date}"  # Combined text
            delivery_date_label_width = c.stringWidth(delivery_date_label, "Helvetica-Bold",
                                                      10)  # Width of the bold label
            delivery_date_text_x = box_below_table_x + 0.5 * cm  # Adjust this value for spacing from the left
            delivery_date_text_y = box_below_table_y + 1.8 * cm  # Adjust this value for spacing from the bottom

            # Add the bold "Date of Delivery:" label to the canvas
            c.setFont("Helvetica-Bold", 10)
            c.drawString(delivery_date_text_x, delivery_date_text_y, delivery_date_label)

            # Add the normal weight date text to the canvas
            c.setFont("Helvetica", 10)
            date_text_x = delivery_date_text_x + delivery_date_label_width + 0.2 * cm  # Adjust this value for spacing
            c.drawString(date_text_x, delivery_date_text_y, date)

            # Calculate the position for the "Term of Payment:" text within the rectangle box
            term_of_payment_label = "Term of Payment        :"  # Text label to display
            term_of_payment = str(Term_of_Payment)  # Value for term of payment
            term_of_payment_label_width = c.stringWidth(term_of_payment_label, "Helvetica-Bold",
                                                        10)  # Width of the bold label
            term_of_payment_text_x = box_below_table_x + 0.5 * cm  # Adjust this value for spacing from the left
            term_of_payment_text_y = delivery_date_text_y - 0.7 * cm  # Adjust this value for spacing from the bottom

            # Add the bold "Term of Payment:" label to the canvas
            c.setFont("Helvetica-Bold", 10)
            c.drawString(term_of_payment_text_x, term_of_payment_text_y, term_of_payment_label)

            # Add the normal weight term of payment text to the canvas
            c.setFont("Helvetica", 10)
            term_of_payment_text_x += term_of_payment_label_width + 0.2 * cm  # Adjust this value for spacing
            c.drawString(term_of_payment_text_x, term_of_payment_text_y, term_of_payment)

            # Calculate the position for the "Documents Required:" text within the rectangle box
            documents_required_label = "Documents Required :"  # Text label to display
            documents_required_value = "Invoice"  # Value for documents required
            documents_required_label_width = c.stringWidth(documents_required_label, "Helvetica-Bold",
                                                           10)  # Width of the bold label
            documents_required_text_x = box_below_table_x + 0.5 * cm  # Adjust this value for spacing from the left
            documents_required_text_y = term_of_payment_text_y - 0.7 * cm  # Adjust this value for spacing from the bottom

            # Add the bold "Documents Required:" label to the canvas
            c.setFont("Helvetica-Bold", 10)
            c.drawString(documents_required_text_x, documents_required_text_y, documents_required_label)

            # Add the normal weight documents required value to the canvas
            c.setFont("Helvetica", 10)
            documents_required_text_x += documents_required_label_width + 0.2 * cm  # Adjust this value for spacing
            c.drawString(documents_required_text_x, documents_required_text_y, documents_required_value)

            # Calculate the position for the "(PURCHASE DEPARTMENT)" text within the rectangle box
            c.setFont("Helvetica-Bold", 10)
            purchase_department_text = "(PURCHASE DEPARTMENT)"  # Text to display
            purchase_department_text_width = c.stringWidth(purchase_department_text, "Helvetica", 10)
            purchase_department_text_x = box_below_table_x + box_below_table_width - purchase_department_text_width - 0.5 * cm  # Adjust this value for spacing from the right
            purchase_department_text_y = box_below_table_y + 0.5 * cm  # Adjust this value for spacing from the bottom

            # Add the "(PURCHASE DEPARTMENT)" text to the canvas
            c.drawString(purchase_department_text_x, purchase_department_text_y, purchase_department_text)
            c.setFont("Helvetica", 8)
            # Calculate the position for "Generated By:" and its value
            generated_by_text = "Generated By:"
            generated_by_text_width = c.stringWidth(generated_by_text, "Helvetica", 10)
            generated_by_value = "Administrator"
            generated_by_value_width = c.stringWidth(generated_by_value, "Helvetica", 10)
            generated_by_text_x = margin  # Adjust this value for spacing from the left
            generated_by_text_y = margin / 2  # Adjust this value for spacing from the bottom

            # Add "Generated By:" text to the canvas
            c.drawString(generated_by_text_x, generated_by_text_y, generated_by_text)

            # Add "(PURCHASE DEPARTMENT)" value to the canvas
            generated_by_value_x = generated_by_text_x + generated_by_text_width + 0.2 * cm  # Adjust this value for spacing
            c.drawString(generated_by_value_x, generated_by_text_y, generated_by_value)

            # Calculate the position for "Page 1 of 2"
            page_text = f"Page {page_num} of {total_value_length}"
            page_text_width = c.stringWidth(page_text, "Helvetica", 10)
            page_text_x = width / 2 - page_text_width / 2  # Center the text horizontally
            c.drawString(page_text_x, generated_by_text_y, page_text)

            # Get the current date
            current_date = datetime.now()

            # Format the date as "Apr 23, 2024"
            formatted_date = current_date.strftime("%b %d, %Y")

            # Calculate the position for "Date : Apr 23, 2024"
            date_text = f"Date : {formatted_date}"
            date_text_width = c.stringWidth(date_text, "Helvetica", 10)
            date_text_x = width - margin - date_text_width  # Adjust this value for spacing from the right
            c.drawString(date_text_x, generated_by_text_y + 0.1 * cm, date_text)

            # Calculate table position to center it within the bottom box
            table_x = box_below_x
            table_y = box_below_y - table_height - 0.0001 * cm

            # Draw the table on the canvas
            table.drawOn(c, table_x, table_y)

            # Save the PDF
            c.showPage()
            c.save()

        # Generate filename with date and time
        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"{start_date}_{end_date}_{current_datetime}.pdf"
        # Output folder path for the merged PDF
        output_folder = r'C:\Users\Administrator\Downloads\eiis\merged_pdf'

        # Ensure the output folder exists, create it if not
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        # Output file path for the merged PDF
        output_path = os.path.join(output_folder, file_name)
        # Merge the PDFs
        status, output_path = merge_pdfs(pdf_file_name, output_path)
        if status == "success":
            print("PDFs merged successfully!")
        else:
            print('Error while Merging PDFs')
    except Exception as error:
        print('The Cause of error -->', error)
        status = "failed"
        pass
    return status, output_path
