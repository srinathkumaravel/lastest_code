from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm
from reportlab.platypus import Table, TableStyle
from reportlab.lib import colors
import os
import pandas as pd
import PyPDF2
from datetime import datetime
import re
from database import get_database_engine_e_eiis


def fetch_data(consolidation_id):
    dfs_list = None
    tel_numbers = None
    fax_numbers = None
    status = "failed"
    unique_supplier_names = None
    period = None
    try:
        engine = get_database_engine_e_eiis()

        # Define your SQL query
        sql_query = f""" SELECT  sel_sup.SUPPLIER_ID , sup.Supplier_Name, sel_sup.ITEM_ID, item.Item_Name, sel_sup.PACKAGE_ID,  
                    sel_sup.GP, sel_sup.PERIOD, sup.Fax_No, sup.Tel_No FROM selectedsupplier AS sel_sup JOIN suppliers  
                    AS sup ON sup.Supplier_ID = sel_sup.SUPPLIER_ID JOIN item ON item.Item_ID = sel_sup.ITEM_ID WHERE 
                    CONSOLIDATION_ID =%s; """
        # Execute the query and retrieve the data as a DataFrame
        df = pd.read_sql_query(sql_query, engine, params=(consolidation_id,))
        print(df)

    except Exception as error:
        print('The cause of error -->', error)
        status = 'failed'
        return status, dfs_list, fax_numbers, tel_numbers, unique_supplier_names, period

    try:
        def get_contact_info(df, unique_supplier_ids):
            fax_numbers = []
            tel_numbers = []

            # Convert numpy array to list
            unique_supplier_ids_list = unique_supplier_ids.tolist()

            for index, row in df.iterrows():
                supplier_id = row['SUPPLIER_ID']

                if supplier_id in unique_supplier_ids_list:
                    fax_numbers.append(row['Fax_No'])
                    tel_numbers.append(row['Tel_No'])
                    unique_supplier_ids_list.remove(supplier_id)  # Remove the matched supplier ID from the list
                    if not unique_supplier_ids_list:  # If all unique supplier IDs are found, exit the loop
                        break

            return fax_numbers, tel_numbers

        unique_supplier_ids = df['SUPPLIER_ID'].unique()
        fax_numbers, tel_numbers = get_contact_info(df, unique_supplier_ids)

        # Get the period date from the PERIOD column
        period = str(df.iloc[0, 6])
        # Convert the date string to a datetime object
        date_object = datetime.strptime(period, '%Y-%m-%d')

        # Format the datetime object to 'Month-Year' format
        period = date_object.strftime('%B-%Y')
        print(period)
        # Drop the 'PERIOD' column
        df.drop(columns=['PERIOD', 'Fax_No', 'Tel_No'], inplace=True)
        rename_dict = {'SUPPLIER_ID': 'Supplier ID', 'Supplier_Name': 'Supplier_Name', 'ITEM_ID': 'Item Code',
                       'PACKAGE_ID': 'Packing',
                       'GP': 'Price', 'Item_Name': 'Item Name'}

        # Rename the columns using the dictionary
        df.rename(columns=rename_dict, inplace=True)
        # Get unique supplier IDs
        unique_supplier_ids = df['Supplier ID'].unique()

        # Get unique supplier name
        unique_supplier_names = df['Supplier_Name'].unique()
        print(unique_supplier_names)

        # Create an empty list to store separate DataFrames
        dfs_list = []

        # Iterate over unique supplier IDs
        for supplier_id in unique_supplier_ids:
            # Filter the DataFrame for the current supplier ID
            supplier_df = df[df['Supplier ID'] == supplier_id].copy()

            # Add the index number as a new column 'si' in the filtered DataFrame
            supplier_df.insert(0, 'SI', range(1, len(supplier_df) + 1))
            # Drop 'Supplier_Name' and 'Supplier ID' columns
            supplier_df.drop(columns=['Supplier_Name', 'Supplier ID'], inplace=True)

            # Append the filtered DataFrame to the list
            dfs_list.append(supplier_df)
            status = "success"

    except Exception as error:
        print('The cause of error -->', error)
        status = "failed"

    return status, dfs_list, fax_numbers, tel_numbers, unique_supplier_names, period


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


def create_header(c, width, height, period, consolidation_id, person_who_generates):
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
    c.setLineWidth(1)
    c.rect(rect_x, rect_y, rect_width, rect_height)

    # Draw vertical line inside the rectangle
    vertical_line_x = rect_x + 3.5 * cm  # 3.5 cm from the left edge of the rectangle
    vertical_line_start_y = rect_y  # Bottom Y of rectangle
    vertical_line_end_y = rect_y + rect_height  # Top Y of rectangle

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

    third_element = f"SUPPLIER PRICE CONFIRMATION  {period}"
    # List to be placed inside the first rectangle
    list1 = ["SOCAT LLC", "OMAN", third_element]

    # Calculate the y position to center the text vertically in the middle of the rectangle
    text_y = rect_y + rect_height - c._leading  # Start from the bottom of the rectangle
    text_y -= 0.5 * cm  # Move the text slightly upwards by adjusting this value

    # Move the text slightly towards the left
    text_x = rect_x + 8 * cm  # Adjust this value as needed

    # Font settings for the text
    c.setFont("Helvetica-Bold", 10)

    # Draw each text element one below the other, centered horizontally
    for text in list1:
        # Calculate the width of the text to center it horizontally
        text_width = c.stringWidth(text)
        text_x = rect_x + (rect_width - text_width) / 2

        c.drawString(text_x, text_y, text)
        text_y -= c._leading  # Move to the next line above

    consolidation_id_text = f"Consolidation ID: {consolidation_id}"
    c.setFont("Helvetica-Bold", 8)
    c.setFillColor(colors.blue)
    id_text_width = c.stringWidth(consolidation_id_text)
    id_text_x = rect_x + rect_width - id_text_width - 0.3 * cm
    id_text_y = rect_y + 0.3 * cm
    c.drawString(id_text_x, id_text_y, consolidation_id_text)

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
    c.setFont("Helvetica", 8)  # Adjust font and size as needed
    c.drawString(generated_by_text_x, generated_by_text_y, f"Generated By : {person_who_generates}")

    # Get the current date and format it
    current_date = datetime.now().strftime("%B %d, %Y")

    # Calculate the width of the formatted current date
    current_date_width = c.stringWidth(current_date)

    # Calculate the x-coordinate for the current date to align it to the right side of the page
    current_date_x = width - left_margin - current_date_width

    # Draw the current date on the same line as "Generated By :" and "Page 1 of 1"
    c.drawString(current_date_x, generated_by_text_y, current_date)

    return rect_y, left_margin, bottom_margin, right_margin, top_margin, rect_width


def create_pdf_sup(period, dfs_list, consolidation_id, unique_supplier_names, tel_numbers, fax_numbers):
    pdf_file_name: list = []
    person_who_generates = "Administrator"
    Attn_value = ""
    page_num = 0
    total_page = len(dfs_list)
    try:
        for df, sup_name, tel_no, fax_num in zip(dfs_list, unique_supplier_names, tel_numbers, fax_numbers):
            if tel_no is None:
                tel_no = ""
            if fax_num is None:
                fax_num = ""
            page_num += 1
            current_datetime = datetime.now()
            # Format the current date and time as desired (example: YYYY-MM-DD_HH-MM-SS)
            current_date_time_str = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")
            file_name = f'{consolidation_id}_{sup_name}_{current_date_time_str}.pdf'
            file_name = re.sub(r'[^\w\s.]', '_', file_name)
            directory_path = r"C:\Users\Administrator\Downloads\eiis\supplier_price_confirmation"
            file_with_path = directory_path + "\\" + file_name
            pdf_file_name.append(file_with_path)

            c = canvas.Canvas(file_with_path, pagesize=letter)
            width, height = letter  # Size of page (letter size)

            # Set margins
            margin = 1.5 * cm
            rect_y, left_margin, bottom_margin, right_margin, top_margin, rect_width = create_header(c, width, height,
                                                                                                     period,
                                                                                                     consolidation_id,
                                                                                                     person_who_generates)

            # Add supplier name below the rectangle on the left side
            c.setFillColor(colors.black)
            supplier_text = f"Supplier Name : {sup_name}"
            supplier_x = margin  # Same as rectangle x position
            supplier_y = rect_y - c._leading  # Below the rectangle
            c.drawString(supplier_x, supplier_y, supplier_text)

            # Add telephone number with manual positioning
            c.setFillColor(colors.black)
            tel_number_text = f"Tel No.: {tel_no}"
            tel_number_x = 14.5 * cm  # Adjust this value to position it manually
            tel_number_y = rect_y - c._leading  # Same y position as the supplier name
            c.drawString(tel_number_x, tel_number_y, tel_number_text)

            # Add "Attn:" text below the supplier name
            c.setFillColor(colors.black)
            attn_text = f"Attn                   :{Attn_value}"
            attn_x = margin
            supplier_y = supplier_y - 6.0

            attn_y = supplier_y - c._leading  # Adjust the position below the supplier name
            c.drawString(attn_x, attn_y, attn_text)

            # Add fax number with manual positioning
            c.setFillColor(colors.black)
            fax_number_text = f"Fax No.: {fax_num}"
            fax_number_x = 14.4 * cm  # Adjust this value to position it manually
            fax_number_y = attn_y  # Same y position as the "Attn:" text
            c.drawString(fax_number_x, fax_number_y, fax_number_text)
            c.setFont("Helvetica", 10)
            # Define the content text
            content_text = (
                "We are pleased to confirm our acceptance to purchase the following items for the next three (3) months "
                f"with effect from {period}. Terms and conditions will remain the same as per our agreement letter.")

            # Calculate the width of each line based on the rectangle width
            max_width = rect_width - 0.8 * margin

            # Split the content text into lines that fit within the max_width
            words = content_text.split()
            line = ''
            content_lines = []
            for word in words:
                if c.stringWidth(line + ' ' + word) <= max_width:
                    line += ' ' + word
                else:
                    content_lines.append(line.strip())
                    line = word
            content_lines.append(line.strip())

            # Calculate the starting y-coordinate for the content
            content_y = attn_y - c._leading - 0.2 * cm - len(content_lines) * c._leading  # Adjust y-coordinate

            # Draw each line of the content text
            for line in content_lines:
                if content_y <= margin:  # Check if the text exceeds the page height
                    c.showPage()  # Move to the next page
                    content_y = height - 3 * margin  # Reset y-coordinate for new page

                # Check if the line contains the period, if so, make it bold
                if period in line:
                    # Split the line into two parts: before and after the period
                    parts = line.split(period)
                    # Draw the first part (before period)
                    c.drawString(margin, content_y, parts[0])
                    # Calculate the width of the first part to determine the starting point for the bold part
                    first_part_width = c.stringWidth(parts[0])
                    # Set the font to bold
                    c.setFont("Helvetica-Bold", 10)
                    # Draw the period (in bold)
                    c.drawString(margin + first_part_width, content_y, period)
                    # Set the font back to normal
                    c.setFont("Helvetica", 10)
                    # Draw the second part (after period)
                    c.drawString(margin + first_part_width + c.stringWidth(period), content_y, parts[1])
                else:
                    # Draw the entire line
                    c.drawString(margin, content_y, line)

                content_y -= c._leading  # Move to the next line
            # Known row height in points
            row_height = 20.19685

            # Calculate available vertical space
            available_space = content_y - bottom_margin

            # Calculate the number of rows that can fit within the available space
            rows_per_chunk = int(available_space / row_height)
            print()

            # Adjust rows_per_chunk to account for not splitting header and data rows unevenly
            # If the header is always included and needs one row by itself:
            rows_per_chunk -= 1  # Adjust for the header if it is included in each chunk
            print('rows_per_chunk', rows_per_chunk)

            chunks = [df[i:i + rows_per_chunk] for i in range(0, len(df), rows_per_chunk)]
            for i, chunk in enumerate(chunks):
                table_y = content_y  # Start the table below the second rectangle

                df_data = chunk.values.tolist()
                df_headers = chunk.columns.tolist()

                col_widths = [40, 74, 250, 135, 77]  # Adjust these values as needed for each colum
                # Draw the DataFrame table
                df_table = Table([df_headers] + df_data, colWidths=col_widths)
                df_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.white),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                    ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 7.5),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 5),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                    ('FONTSIZE', (0, 1), (-1, -1), 7.5),
                    ('ALIGN', (2, 1), (2, -1), 'LEFT'),
                    ('ALIGN', (-1, 1), (-1, -1), 'RIGHT')]))

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
                    rect_y, left_margin, bottom_margin, right_margin, top_margin, rect_width = create_header(c, width,
                                                                                                             height,
                                                                                                             period,
                                                                                                             consolidation_id,
                                                                                                             person_who_generates)
                    content_y = rect_y
                else:
                    # Add "Yours faithfully, Purchase Department" below the table
                    c.setFont("Helvetica", 10)
                    c.setFillColor(colors.black)
                    sign_off_text = "Yours faithfully,"
                    sign_off_x = margin
                    # Calculate the y-coordinate where the horizontal lines should be drawn
                    sign_off_y = table_y - df_table_height - 0.7 * cm  # Adjust this value as needed for vertical movement

                    # sign_off_y = table_y - 20
                    c.drawString(sign_off_x, sign_off_y, sign_off_text)

                    # Calculate the width of "Yours faithfully,"
                    sign_off_width = c.stringWidth(sign_off_text)

                    # Add "Purchase Department" based on the width of "Yours faithfully,"
                    purchase_department_text = "Purchase Department"
                    purchase_department_x = sign_off_x + sign_off_width - 2 * cm
                    purchase_department_y = sign_off_y - 0.5 * cm
                    c.drawString(purchase_department_x, purchase_department_y, purchase_department_text)

            # Save the PDF
            c.showPage()
            c.save()

        # Generate filename with date and time
        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"supplier_price_conf_{current_datetime}.pdf"
        print(file_name)
        # Output folder path for the merged PDF
        output_folder = r'C:\Users\Administrator\Downloads\eiis\supplier_price_confirmation\merged_supplier_pricePDF'

        # Ensure the output folder exists, create it if not
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        # Output file path for the merged PDF
        output_path = os.path.join(output_folder, file_name)
        # Merge the PDFs
        pdf_status, output_path = merge_pdfs(pdf_file_name, output_path)

    except Exception as error:
        print('The cause of error -->', error)
        pdf_status = "failed"

    return pdf_status, output_path, file_name
