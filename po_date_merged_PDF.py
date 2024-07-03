from datetime import datetime
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm
from reportlab.platypus import Table, TableStyle, Paragraph
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
import os
import pandas as pd
from PyPDF2 import PdfWriter, PdfReader
from io import BytesIO
import PyPDF2
from database import get_database_connection_e_eiis


def execute_merged_date_query(start_date, end_date):
    nested_po_heads = None
    po_number = None
    df_list: list = []
    total_value_list: list = []
    location_ids: list = []
    date_of_del: list = []
    location_name: list = []

    try:
        with get_database_connection_e_eiis() as conn:
            # Create a cursor object
            cursor = conn.cursor()

            sql_query_pohead = """
            SELECT pohead.Po_Num, pohead.Supplier_ID, pohead.Currency_ID, pohead.Currency_Rate, 
                   suppliers.Supplier_Name, suppliers.Address1, suppliers.Fax_No, suppliers.Tel_No, 
                   suppliers.No_Days, pohead.Ord_Loc, location.Location_Name, pohead.PO_Date, suppliers.Contact_Person1
            FROM pohead
            INNER JOIN suppliers ON pohead.Supplier_ID = suppliers.Supplier_ID 
            INNER JOIN location ON location.Location_ID = pohead.Ord_Loc
            WHERE pohead.PO_Date BETWEEN %s AND %s
                """

            # Execute the query with parameter substitution to avoid SQL injection
            cursor.execute(sql_query_pohead, (start_date, end_date))
            # Fetch all rows from the result set
            po_heads = cursor.fetchall()
            nested_po_heads = [list(item) for item in po_heads]

            if len(nested_po_heads) == 0:  # To check whether data available in between dates
                status = "failed"
                message = "No data available for selected period"
                print(message)
                return nested_po_heads, df_list, total_value_list, po_number, location_ids, date_of_del, location_name, status

            po_number = [sublist[0] for sublist in nested_po_heads]
            # Process each purchase order to fetch details
            for po_head in nested_po_heads:
                po_num = po_head[0]  # Assuming PoNum is the first column in the result
                location_ids.append(po_head[-4])
                date_of_del.append(po_head[-2])
                location_name.append(po_head[-3])
                sql_query_podetail = """
                       SELECT item.Item_Name, details.PACKAGE_ID, details.QTY, details.ACTUAL_GP
                    FROM podetail  as details JOIN item ON item.Item_ID = details.ITEM_ID
                    WHERE PO_NUM = %s
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

    except Exception as error:
        print('The cause of error -->', error)
        status = "failed"
    print(nested_po_heads, df_list, total_value_list, po_number, location_ids, date_of_del, location_name, status)
    return nested_po_heads, df_list, total_value_list, po_number, location_ids, date_of_del, location_name, status


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


def add_page_numbers(input_pdf, output_file_with_page_number):
    try:
        # Read the existing PDF
        reader = PdfReader(input_pdf)
        writer = PdfWriter()

        # Create a PDF with page numbers
        packet = BytesIO()
        can = canvas.Canvas(packet, pagesize=letter)

        # Add page numbers
        for page_num in range(1, len(reader.pages) + 1):
            can.setFont("Helvetica", 10)
            can.drawString(300, 10, f"Page {page_num}")
            can.showPage()

        can.save()

        # Move to the beginning of the StringIO buffer
        packet.seek(0)
        new_pdf = PdfReader(packet)

        # Merge each page with the new page numbers
        for page_num in range(len(reader.pages)):
            page = reader.pages[page_num]
            page.merge_page(new_pdf.pages[page_num])
            writer.add_page(page)

        # Write the output PDF
        with open(output_file_with_page_number, 'wb') as output_file:
            writer.write(output_file)
        status = "success"
    except Exception as error:
        print('The cause of error -->', error)
        status = "failed"
        output_file_with_page_number = None
    return output_file_with_page_number, status


def draw_header_and_details(c, width, height, address_info_details, attn, currency_id,
                            currency_rate, fax_no, tel_no, person_name, term_of_payment):
    left_margin = 28
    right_margin = width - 28
    top_margin = height - 28
    bottom_margin = 28

    c.setLineWidth(1)
    c.rect(left_margin, bottom_margin, right_margin - left_margin, top_margin - bottom_margin)

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
    box_below_x = rect_x  # Start from left margin
    box_below_width = rect_width  # Same width as the above rectangle
    box_below_height = 2.6 * cm  # Increased height of the new box
    box_below_y = rect_y - 2.6 * cm  # Position it directly below the existing rectangle

    # Draw the new box
    c.setLineWidth(1.3)  # Thicker border for the new box
    c.rect(box_below_x, box_below_y, box_below_width, box_below_height, stroke=1, fill=0)

    # Set font settings for the address info
    c.setFont("Helvetica-Bold", 10)

    # Calculate position to place the address info text
    address_text_x = box_below_x + 0.5 * cm  # Adjust as needed for left margin

    address_text_y = box_below_y + box_below_height - 0.5 * cm  # Adjust as needed for top margin

    # Draw the address info text
    c.drawString(address_text_x, address_text_y, 'Supplier        :')

    c.setFont("Helvetica", 8)
    # Add the Supplier ID next to the "Supplier ID:" text
    supplier_id = address_info_details
    supplier_id_lines = supplier_id.split('\n')  # Split the string into lines
    supplier_id_y = address_text_y - 0.01 * cm  # Start just below "Supplier :"

    # Draw each line of supplier ID below "Supplier :"
    for line in supplier_id_lines:
        c.drawString(115, supplier_id_y, line)
        supplier_id_y -= 0.4 * cm  # Move to the next line

    # Print "Attn:" and its value
    attn_text_y = address_text_y - 35  # Space below the last line of supplier details
    attn_label_x = address_text_x  # Set x position for "Attn:"
    attn_value_x = attn_label_x + 73  # Position for the value of "Attn:"
    c.setFont("Helvetica-Bold", 10)
    c.drawString(attn_label_x, attn_text_y, 'Attn               :')
    c.setFont("Helvetica", 8)
    c.drawString(attn_value_x, attn_text_y, attn)  # Adjust X coordinate to align with value

    # Print "Currency ID:" and its value
    currency_label_x = address_text_x  # Adjust this position based on your layout needs
    currency_value_x = currency_label_x + 73  # Adjust this based on the length of the label
    c.setFont("Helvetica-Bold", 10)
    currency_text_y = address_text_y - 57
    c.drawString(currency_label_x, currency_text_y, 'Currency ID  :')
    c.setFont("Helvetica", 8)
    c.drawString(currency_value_x, currency_text_y, currency_id)  # Print the currency ID value

    # Print "Currency rate:" and its value
    currency_rate_label_x = currency_value_x + 50  # Adjust this position based on your layout needs
    currency_rate_value_x = currency_rate_label_x + 79  # Adjust this based on the length of the label
    c.setFont("Helvetica-Bold", 10)
    currency_rate_text_y = address_text_y - 57
    c.drawString(currency_rate_label_x, currency_rate_text_y, 'Currency Rate :')
    c.setFont("Helvetica", 9)
    c.drawString(currency_rate_value_x, currency_rate_text_y, str(currency_rate))  # Print the currency rate value

    # Print "Tel ph num" and its value
    tel_num_label_x = currency_value_x + 235  # Adjust this position based on your layout needs
    tel_num_value_x = tel_num_label_x + 45  # Adjust this based on the length of the label
    c.setFont("Helvetica-Bold", 10)
    tel_ph_text_y = address_text_y
    c.drawString(tel_num_label_x, tel_ph_text_y, 'Tel no :')
    c.setFont("Helvetica", 9)
    c.drawString(tel_num_value_x, tel_ph_text_y, str(tel_no))  # Print the Tel num value

    # Print "Fax num" and its value
    fax_label_x = currency_value_x + 235  # Adjust this position based on your layout needs
    fac_num_value_x = fax_label_x + 45  # Adjust this based on the length of the label
    c.setFont("Helvetica-Bold", 10)
    fax_text_y = address_text_y - 20
    c.drawString(fax_label_x, fax_text_y, 'Fax no :')
    c.setFont("Helvetica", 9)
    c.drawString(fac_num_value_x, fax_text_y, str(fax_no))  # Print the Fax Num value

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

    return rect_x, top_margin, left_margin, bottom_margin, right_margin


def create_po_merge_pdf(nested_po_heads, df_list, total_value_list, po_number, location_ids, date_of_del,
                        location_name, index_nested_list,
                        address_index_list):
    try:
        pdf_file_name = []
        page = 0
        person_name = "administrator"
        fixed_table_len = 26
        row_height = 20
        page_num_status = 'failed'
        pdf_with_page_num = None
        output_file_with_page_number = None
        for address_index, sub_list in zip(address_index_list, index_nested_list):
            print(address_index)
            address_info = nested_po_heads[address_index]
            page += 1
            current_datetime = datetime.now()
            # Format the current date and time as desired (example: YYYY-MM-DD_HH-MM-SS)
            current_date_time_str = current_datetime.strftime("%Y_%m_%d_%H_%M_%S")
            file_name = f'{page}_{current_date_time_str}.pdf'
            print(file_name)
            directory_path = r"C:\Users\Administrator\Downloads\eiis\po_updated_pdf\single_PDF"
            file_with_path = directory_path + "\\" + file_name
            pdf_file_name.append(file_with_path)

            supplier_id = str(address_info[0])
            currency_id = str(address_info[1])
            currency_rate = str(address_info[2])
            supplier_name = str(address_info[3])
            address = str(address_info[4])
            fax_no = address_info[5]
            tel_no = address_info[6]
            term_of_payment = address_info[-5]
            attn = address_info[-1]
            if attn is None:
                attn = ""

            address_info_details = supplier_id + " " + supplier_name + "\n" + address

            # Combine the directory path and file name
            file_path = os.path.join(directory_path, file_name)
            print(file_path)
            # Create a canvas
            c = canvas.Canvas(file_path, pagesize=letter)
            width, height = letter  # Size of page (letter size)

            # Header and bottom details
            rect_x, top_margin, left_margin, bottom_margin, right_margin = draw_header_and_details(c, width, height,
                                                                                                   address_info_details,
                                                                                                   attn, currency_id,
                                                                                                   currency_rate,
                                                                                                   fax_no, tel_no,
                                                                                                   person_name,
                                                                                                   term_of_payment)

            # Initialize content position
            current_y_position = top_margin - 6 * cm

            # Set the font for this part
            c.setFont("Helvetica", 10)

            # Loop through your indices and data
            for index, idx in enumerate(sub_list):
                df = df_list[idx]
                df_len = len(df) + 1
                total_qty = total_value_list[idx]
                po_num = po_number[idx]
                loc_id = location_ids[idx]
                del_date = date_of_del[idx]
                loc_name = location_name[idx][:18]

                available_space = current_y_position - bottom_margin
                print("available_space", available_space)
                # Calculate the number of rows that can fit within the available space
                rows_per_chunk = int(available_space / row_height)
                rows_per_chunk -= 1  # Adjust for the header if it is included in each chunk
                if available_space < 3 * cm:
                    c.showPage()
                    # Header and bottom details
                    rect_x, top_margin, left_margin, bottom_margin, right_margin = draw_header_and_details(c, width,
                                                                                                           height,
                                                                                                           address_info_details,
                                                                                                           attn,
                                                                                                           currency_id,
                                                                                                           currency_rate,
                                                                                                           fax_no,
                                                                                                           tel_no,
                                                                                                           person_name,
                                                                                                           term_of_payment)

                    # Initialize content position
                    current_y_position = top_margin - 6 * cm

                    # Set the font for this part
                    c.setFont("Helvetica", 9)

                    available_space = current_y_position - bottom_margin
                    print("available_space", available_space)
                    # Calculate the number of rows that can fit within the available space
                    rows_per_chunk = int(available_space / row_height)
                    rows_per_chunk -= 1  # Adjust for the header if it is included in each chunk
                print(current_y_position)
                header_details = f"Del Date : {del_date}   | Loc ID : {loc_id}      | Loc Name : {loc_name}        | Po Num : {po_num}       | Total Value : {total_qty}"
                c.setFont("Helvetica", 8)
                c.drawString(left_margin + 0.5 * cm, current_y_position, header_details)
                current_position = current_y_position - 0.5 * cm

                first_half = df.iloc[:rows_per_chunk]
                second_half = df.iloc[rows_per_chunk:]
                # print('Len of first DF', len(first_half))
                # print('Len of second DF', len(second_half))
                table_y = current_position
                df_data = first_half.values.tolist()
                df_headers = first_half.columns.tolist()

                df_table = Table([df_headers] + df_data,
                                 colWidths=[1.8 * cm, 5.3 * cm, 4.3 * cm, 2.8 * cm, 2.68 * cm, 2.713 * cm])

                df_table.setStyle(TableStyle([('BACKGROUND', (1, 1), (-1, 1), colors.white),
                                              ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                                              ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                                              ('BOTTOMPADDING', (0, 0), (-1, 0), 0.2),
                                              ('FONTSIZE', (0, 0), (-1, 0), 5.5),
                                              ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                                              ('ALIGN', (1, 1), (1, -1), 'LEFT'),
                                              ('GRID', (0, 0), (-1, -1), 1, colors.black),
                                              ('FONTSIZE', (0, 1), (-1, -1), 5.5)]))

                # Calculate the height of the table
                df_table_height = df_table.wrap(0, 0)[1]

                table_width = width - 2 * left_margin  # Width of the table
                df_table.wrapOn(c, table_width, height)  # Prepare the table for drawing
                df_table.drawOn(c, left_margin, table_y - df_table_height)  # Position and draw the table
                current_y_position = table_y - df_table_height - 0.5 * cm

                if len(second_half) == 0:
                    pass
                else:
                    len_of_df_list = (len(df_list))
                    index = index + 1
                    print(index)
                    # Replacing the value at the specified index
                    print(len(df_list))
                    print(len(total_value_list))
                    df_list.append(second_half)
                    print(len(df_list))
                    total_value_list.append(total_qty)
                    po_number.append(po_num)
                    location_ids.append(loc_id)
                    date_of_del.append(del_date)
                    location_name.append(loc_name)
                    sub_list.insert(index, len_of_df_list)
                    print(sub_list)
                    c.showPage()
                    # Header and bottom details
                    rect_x, top_margin, left_margin, bottom_margin, right_margin = draw_header_and_details(c, width,
                                                                                                           height,
                                                                                                           address_info_details,
                                                                                                           attn,
                                                                                                           currency_id,
                                                                                                           currency_rate,
                                                                                                           fax_no,
                                                                                                           tel_no,
                                                                                                           person_name,
                                                                                                           term_of_payment)
                    current_y_position = top_margin - 6 * cm

            available_space = current_y_position - bottom_margin
            print("available_space", available_space)
            if available_space < 2 * cm:
                c.showPage()
                # Header and bottom details
                rect_x, top_margin, left_margin, bottom_margin, right_margin = draw_header_and_details(c, width,
                                                                                                       height,
                                                                                                       address_info_details,
                                                                                                       attn,
                                                                                                       currency_id,
                                                                                                       currency_rate,
                                                                                                       fax_no,
                                                                                                       tel_no,
                                                                                                       person_name,
                                                                                                       term_of_payment)

            # Insert "Term of Payment" label in bold
            term_of_payment = str(term_of_payment)  # Example term of payment
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

            c.save()

        # Output folder path for the merged PDF
        output_folder = r'C:\Users\Administrator\Downloads\eiis\po_updated_pdf\merged_PDF'

        # Ensure the output folder exists, create it if not
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        merged_pdf_file_name = "PURCHASE_ORDER_SUPPLIER" + current_date_time_str + '.pdf'
        # Output file path for the merged PDF
        output_path = os.path.join(output_folder, merged_pdf_file_name)
        # Merge the PDFs
        status, output_path = merge_pdfs(pdf_file_name, output_path)
        if status == "success":
            output_file_path = r'C:\Users\Administrator\Downloads\eiis\po_updated_pdf\merged_PDF'
            pdf_with_page_num = "PURCHASE_ORDER_SUPPLIER" + current_date_time_str + '.pdf'
            # Output file path for the merged PDF
            output_file_with_page_number = os.path.join(output_file_path, pdf_with_page_num)
            output_file_with_page_number, page_num_status = add_page_numbers(output_path, output_file_with_page_number)
            if page_num_status != 'success':
                output_file_with_page_number = None
                pdf_with_page_num = None
        else:
            page_num_status = status

    except Exception as error:
        print('The Cause of error -->', error)
        page_num_status = "failed"

    return page_num_status, pdf_with_page_num, output_file_with_page_number
