from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm
from reportlab.platypus import Table
from reportlab.lib import colors
import os
import pandas as pd
import PyPDF2
from reportlab.lib.utils import simpleSplit
from datetime import datetime
import re
from sqlalchemy.exc import SQLAlchemyError
from database import get_database_connection_e_eiis, get_database_engine_e_eiis


def fetch_con_quotation_req_data(month, year):
    day = "01"
    from_date = day + "-" + month + "-" + year
    formatted_date = month + "_" + year

    sql_query = """SELECT QTN_REQ_NO FROM qtn_req_head WHERE MONTH(PERIOD) = %s AND YEAR(PERIOD) = %s"""
    sup_name_list = []
    fax_no_list = []
    tel_no_list = []
    df_list = []
    status = "failed"
    try:
        with get_database_connection_e_eiis() as conn:
            cursor = conn.cursor()
            cursor.execute(sql_query, (month, year))
            records = cursor.fetchall()
            cursor.close()
        nested_list = [list(item) for item in records]
        # Flatten the list
        req_no_list = [item for sublist in nested_list for item in sublist]
        print(req_no_list)

        with get_database_connection_e_eiis() as conn:
            cursor = conn.cursor()

            # Prepare the SQL query with a placeholder for parameters
            query = """
                        SELECT head.SUPPLIER_ID, sup.Supplier_Name, sup.Fax_No, sup.Tel_No 
                        FROM qtn_req_head AS head 
                        INNER JOIN suppliers AS sup ON sup.Supplier_ID = head.SUPPLIER_ID 
                        WHERE QTN_REQ_NO = %s;
                    """

            # Iterate over the list of request numbers and execute the query for each
            for req_no in req_no_list:
                print(req_no)
                cursor.execute(query, (req_no,))
                supplier_records = cursor.fetchall()

                for supplier_record in supplier_records:
                    supplier_id, supplier_name, fax_no, tel_no = supplier_record
                    sup_name_list.append(supplier_name)

                    if fax_no is None:
                        fax_no = ''
                    fax_no_list.append(fax_no)

                    if tel_no is None:
                        tel_no = ''
                    tel_no_list.append(tel_no)
    except SQLAlchemyError as e:
        print(f"An error occurred: {e}")
        status = "failed"
        return status, sup_name_list, fax_no_list, tel_no_list, df_list, from_date, formatted_date
    print(sup_name_list)
    print(fax_no_list)
    print(tel_no_list)
    try:
        engine = get_database_engine_e_eiis()
        for req_no in req_no_list:
            # Define the query using `%s` as the placeholder for parameters

            sql_query = """
                SELECT detail.ITEM_ID, it.Item_Name, detail.PACKAGE_ID, detail.QTY
                FROM qtn_req_detail AS detail
                INNER JOIN item AS it ON it.Item_ID = detail.ITEM_ID
                WHERE QTN_REQ_NO = %s; """

            # Execute the query and retrieve the data as a DataFrame
            df = pd.read_sql_query(sql_query, engine, params=(req_no,))
            # Adding new columns with default values
            df['price'] = ''  # Initialize price with 0 or suitable default value
            df['brand'] = ''  # Initialize brand with 'Unknown' or suitable default value

            # Insert 'serial_number' column at the first position
            df.insert(0, 'serial_number', range(1, len(df) + 1))
            # Define a dictionary with old names as keys and new names as values
            rename_dict = {
                'serial_number': 'Sl',
                'ITEM_ID': 'Item Code',
                'Item_Name': 'Item Name',
                'PACKAGE_ID': 'Packing',
                'QTY': 'Qty',
                'price': 'Price',
                'brand': 'Brand/Origin'
            }

            # Rename the columns using the dictionary
            df.rename(columns=rename_dict, inplace=True)
            df_list.append(df)
            status = "success"
    except Exception as error:
        print('The cause of error -->', error)
        status = "failed"

    return status, sup_name_list, fax_no_list, tel_no_list, df_list, from_date, formatted_date


def create_header(c, width, height, attention, supplier_name, fax_no, tel_no):
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
    list1 = ["SOCAT LLC", "OMAN", "QUOTATION REQUEST"]

    # Calculate the y position to center the text vertically in the middle of the rectangle
    text_y = rect_y + rect_height - c._leading  # Start from the bottom of the rectangle
    text_y -= 0.5 * cm  # Move the text slightly upwards by adjusting this value

    # Move the text slightly towards the left
    text_x = rect_x + 8 * cm  # Adjust this value as needed

    # Font settings for the text
    c.setFont("Helvetica-Bold", 12)

    # Draw each text element one below the other, centered horizontally
    for text in list1:
        # Calculate the width of the text to center it horizontally
        text_width = c.stringWidth(text)
        text_x = rect_x + (rect_width - text_width) / 2

        c.drawString(text_x, text_y, text)
        text_y -= c._leading  # Move to the next line above

    # New box below the existing rectangle
    box_below_x = left_margin  # Start from left margin
    box_below_width = rect_width  # Same width as the above rectangle
    box_below_height = 2.7 * cm  # Increased height of the new box
    box_below_y = rect_y - 2.7 * cm  # Position it directly below the existing rectangle

    # Draw the new box
    c.setLineWidth(1.3)  # Thicker border for the new box
    c.rect(box_below_x, box_below_y, box_below_width, box_below_height, stroke=1, fill=0)

    # Move the text slightly towards the left for the bottom table
    text_x_bottom = box_below_x + 0.7 * cm  # Adjust this value as needed

    # Move the text slightly towards the top for the bottom table
    text_y_bottom = box_below_y + box_below_height - c._leading  # Start from the bottom of the rectangle

    # Adjust the y-coordinate as needed
    text_y_bottom -= 0.4 * cm  # Move the text up by 0.5 cm

    # Font settings for the bottom table text
    c.setFont("Helvetica-Bold", 10)

    # Draw "Supplier Name: " followed by the supplier name dynamically
    c.drawString(text_x_bottom, text_y_bottom, "Supplier Name : ")
    c.setFont("Helvetica", 10)
    c.drawString(text_x_bottom + 3.0 * cm, text_y_bottom, supplier_name)  # Adjust the x-coordinate as needed

    # Define the y-coordinate for the attention message
    attention_y = text_y_bottom - 0.7 * cm  # Adjust this value as needed
    c.setFont("Helvetica-Bold", 10)
    # Draw "Attention: " followed by the attention message dynamically
    c.drawString(text_x_bottom, attention_y, "Attention          :")
    c.setFont("Helvetica", 10)
    c.drawString(text_x_bottom + 3.0 * cm, attention_y, attention)  # Adjust the x-coordinate as needed

    # Define the y-coordinate for the telephone number
    tel_no_y = attention_y - 0.7 * cm  # Adjust this value as needed
    c.setFont("Helvetica-Bold", 10)
    # Draw "Tel No: " followed by the telephone number dynamically
    c.drawString(text_x_bottom, tel_no_y, "Tel No               : ")
    c.setFont("Helvetica", 10)
    c.drawString(text_x_bottom + 3.0 * cm, tel_no_y, tel_no)  # Adjust the x-coordinate as needed

    # Define the y-coordinate for the fax number (same as the telephone number)
    fax_no_y = tel_no_y
    c.setFont("Helvetica-Bold", 10)
    # Draw "Fax No: " followed by the fax number dynamically
    c.drawString(text_x_bottom + 9.5 * cm, fax_no_y,
                 "Fax No : ")  # Adjust the x-coordinate to align with telephone number
    c.setFont("Helvetica", 10)
    c.drawString(text_x_bottom + 11.1 * cm, fax_no_y, fax_no)  # Adjust the x-coordinate as needed

    return box_below_y, box_below_height, box_below_width, bottom_margin, left_margin


def create_con_quotation_req_pdf(sup_name_list, fax_no_list, tel_no_list, df_list, from_date, formatted_date):
    pdf_file_name: list = []
    page = 0
    attention = ""
    percentage_in_number = "20"
    month_in_numbers = "3"
    person_who_generates = "Administrator"
    file_name = None
    output_path = None
    try:
        for supplier_name, fax_no, tel_no, df in zip(sup_name_list, fax_no_list, tel_no_list, df_list):
            page += 1
            # Remove special characters and replace spaces with underscores
            supplier_name_with_no_spl_ch = re.sub(r'[^A-Za-z0-9]+', '_', supplier_name)
            # Ensure that there are no leading or trailing underscores
            supplier_name_with_no_spl_ch = supplier_name_with_no_spl_ch.strip('_')
            # Generate filename with date and time
            current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
            directory_path = r"C:\Users\Administrator\Downloads\eiis\quotation_req_new_pdf"
            if not os.path.exists(directory_path):
                os.makedirs(directory_path)  # Create directory if it doesn't exist
            file_name = f"{page}_{supplier_name_with_no_spl_ch}_{current_datetime}.pdf"
            file_with_path = os.path.join(directory_path, file_name)
            pdf_file_name.append(file_with_path)
            print(pdf_file_name)

            c = canvas.Canvas(file_with_path, pagesize=letter)
            width, height = letter  # Size of page (letter size)
            # Set margins
            margin = 1.5 * cm

            box_below_y, box_below_height, box_below_width, bottom_margin, left_margin = create_header(c, width, height,
                                                                                                       attention,
                                                                                                       supplier_name,
                                                                                                       fax_no, tel_no)
            # Define the vertical offset for positioning the additional text above the existing box
            text_vertical_offset = -3.4 * cm  # Adjust this value as needed

            # Calculate the y-coordinate to start drawing the additional text
            additional_text_y = box_below_y + box_below_height + text_vertical_offset
            # Define the text with placeholders for dynamic dates
            additional_text = f"Please find hereunder the approximate quantity of food and cleaning items which will be procured by us for the next months with effect from {from_date}. We request you to send your Quotation for our consideration with the following specs package, brand, country of origin."

            # Define the width available for text
            text_width = box_below_width

            # Calculate the remaining height available for text
            remaining_height = additional_text_y - left_margin - 5

            # Calculate the text lines based on available width and height
            text_lines = simpleSplit(additional_text, c._fontname, c._fontsize, text_width)

            # Font settings for the additional text
            c.setFont("Helvetica", 10)

            # Draw the additional text below the existing box
            for line in text_lines:
                c.drawString(left_margin + 5, additional_text_y, line)
                additional_text_y -= c._leading  # Move to the next line above

            # Font settings for the additional content text
            c.setFont("Helvetica", 10)

            # Define the vertical offset for positioning the note text below the existing content
            note_text_vertical_offset = 7.4 * cm  # Adjust this value as needed

            # Calculate the y-coordinate to start drawing the note text
            note_text_y = height - left_margin - note_text_vertical_offset  # Start from the top margin

            # Define the note text with placeholders for dynamic values
            note_text = f"Note:\n1. The Quantity mentioned below is subject to increase or decrease by {percentage_in_number}%\n2. Quotations received after the fixed date / without proper specification shall be rejected.\n3. Shelf life should be above {month_in_numbers} months"

            # Calculate the width available for note text
            note_text_width = width - 2 * left_margin

            # Calculate the note text lines based on available width and height
            note_text_lines = simpleSplit(note_text, c._fontname, c._fontsize, note_text_width)

            # Font settings for the note text
            c.setFont("Helvetica", 10)
            # Define the gap between lines
            line_gap = 0.1 * cm  # Adjust this value as needed

            # Draw the note text below the existing content
            for line in note_text_lines:
                # Draw the line of text at the calculated y-coordinate
                c.drawString(left_margin + 5, note_text_y, line)
                # Move to the next line above for the next iteration
                note_text_y -= c._leading + line_gap
            note_text_y -= 8  # Adjust as necessary based on your content height
            # Known row height in points
            row_height = 22.19685

            # Calculate available vertical space
            available_space = note_text_y - bottom_margin

            # Calculate the number of rows that can fit within the available space
            rows_per_chunk = int(available_space / row_height)
            print()

            # Adjust rows_per_chunk to account for not splitting header and data rows unevenly
            # If the header is always included and needs one row by itself:
            rows_per_chunk -= 1  # Adjust for the header if it is included in each chunk
            print('rows_per_chunk', rows_per_chunk)

            chunks = [df[i:i + rows_per_chunk] for i in range(0, len(df), rows_per_chunk)]

            for i, chunk in enumerate(chunks):
                table_y = note_text_y  # Start the table below the second rectangle

                df_data = chunk.values.tolist()
                df_headers = chunk.columns.tolist()

                col_widths = [35, 65, 210, 100, 39, 50, 76]  # Adjust these values as needed for each column

                df_table = Table([df_headers] + df_data, colWidths=col_widths)
                # Styling the table

                df_table.setStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.white),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 10),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 5),  # Adjust the padding to reduce the height
                    ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                    ('ALIGN', (0, 0), (0, -1), 'RIGHT'),  # Align the first column (index 0) to the right
                    ('ALIGN', (1, 0), (1, -1), 'CENTER'),  # Align the second column (index 1) to the center
                    ('ALIGN', (-3, 1), (-1, -1), 'RIGHT'),  # Align other right-aligned columns as needed
                    ('ALIGN', (2, 0), (3, -1), 'LEFT'),
                    # Align the third and fourth columns (indices 2 and 3) to the left
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
                    box_below_y, box_below_height, box_below_width, bottom_margin, left_margin = create_header(c, width,
                                                                                                               height,
                                                                                                               attention,
                                                                                                               supplier_name,
                                                                                                               fax_no,
                                                                                                               tel_no)
                    note_text_y = box_below_y
                else:
                    # Calculate the y-coordinate where the horizontal lines should be drawn
                    line_y = table_y - df_table_height - 2.0 * cm  # Adjust this value as needed for vertical movement

                    # Define the gap between the two lines
                    line_gap = 0.2 * cm  # Adjust this value as needed

                    # Calculate the x-coordinate for the left line
                    left_line_x = margin
                    # Calculate the x-coordinate for the right line, leaving a gap between the two lines
                    right_line_x = width - margin

                    # Draw the left line
                    c.line(left_line_x, line_y, left_line_x + 160, line_y)  # Adjust the length as needed

                    # Draw the right line, leaving a gap between the two lines
                    c.line(right_line_x, line_y, right_line_x - 160, line_y)  # Adjust the length as needed

                    # Calculate the y-coordinate for the text "PURCHASED BY :"
                    text_x = left_line_x + 0.2 * cm  # Adjust the x-coordinate as needed for horizontal positioning
                    text_y = line_y - 0.7 * cm  # Adjust the y-coordinate as needed for vertical positioning

                    # Draw the text "PURCHASED BY :"
                    c.setFont("Helvetica-Bold", 10)  # Adjust font and size as needed
                    c.drawString(text_x, text_y, "PURCHASED BY :")

                    # Calculate the y-coordinate for the text "SUPPLIER SIGN. & SEAL"
                    text_x = right_line_x - 4.8 * cm  # Adjust the x-coordinate as needed for horizontal positioning
                    text_y = line_y - 0.7 * cm  # Adjust the y-coordinate as needed for vertical positioning

                    # Draw the text "SUPPLIER SIGN. & SEAL"
                    c.setFont("Helvetica-Bold", 10)  # Adjust font and size as needed
                    c.drawString(text_x, text_y, "SUPPLIER SIGN. & SEAL")

                    # Calculate the coordinates for the horizontal line in the bottom margin
                    bottom_line_start_x = left_margin
                    bottom_line_end_x = width - left_margin
                    bottom_line_y = left_margin

                    # Draw the horizontal line in the bottom margin
                    c.line(bottom_line_start_x, bottom_line_y, bottom_line_end_x, bottom_line_y)

                    # Calculate the coordinates for the text "Generated By : {person_who_genrates}"
                    generated_by_text_x = left_margin
                    generated_by_text_y = left_margin - 0.5 * cm  # Adjust the y-coordinate for vertical positioning

                    # Draw the text "Generated By : {person_who_genrates}"
                    c.setFont("Helvetica", 10)  # Adjust font and size as needed
                    c.drawString(generated_by_text_x, generated_by_text_y, f"Generated By : {person_who_generates}")

                    # Get the current date and format it
                    current_date = datetime.now().strftime("%B %d, %Y")

                    # Calculate the width of the formatted current date
                    current_date_width = c.stringWidth(current_date)

                    # Calculate the x-coordinate for the current date to align it to the right side of the page
                    current_date_x = width - left_margin - current_date_width

                    # Draw the current date on the same line as "Generated By :" and "Page 1 of 1"
                    c.drawString(current_date_x, generated_by_text_y, current_date)

            c.save()

        # Generate filename with date and time
        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"Quotation_Request_{formatted_date}_{current_datetime}.pdf"
        # Output folder path for the merged PDF
        output_folder = r'C:\Users\Administrator\Downloads\eiis\quotation_req_new_pdf\merged_pdf'

        # Ensure the output folder exists, create it if not
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        # Output file path for the merged PDF
        output_path = os.path.join(output_folder, file_name)

        # Merge the PDFs
        status, output_path = merge_pdfs(pdf_file_name, output_path)
        if status == "failed":
            output_path = None
    except Exception as error:
        print('The cause of error -->', error)
        status = "failed"

    return status, file_name, output_path


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
        print("PDFs merged successfully!")

    except Exception as error:
        print('Error while merging PDFS pls check merge_pdf() function')
        print('the cause of error -->', error)
        status = "failed"
    return status, output_path
