import pymysql
from sqlalchemy import create_engine
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
from reportlab.lib.utils import simpleSplit
from datetime import datetime
from pprint import pprint
import re
from database import get_database_engine_e_eiis


def fetch_data_for_selected_sup(consolidation_id):
    status = "failed"
    period = None
    dfs_list = []
    try:
        engine = get_database_engine_e_eiis()
        # Define your SQL query
        sql_query = f"""
                    SELECT con.PERIOD, con.ITEM_ID, con.ITEM_NAME, con.PACKAGE_ID, con.GROSS_PRICE, con.GRAND_TOTAL,
                     con.SUP_ID, sup.Supplier_Name FROM consolidation_location_request AS con 
         INNER JOIN suppliers AS sup ON sup.Supplier_ID = con.SUP_ID WHERE CONSOLIDATION_ID = %s;
                    """

        # Execute the query and retrieve the data as a DataFrame
        df = pd.read_sql_query(sql_query, engine, params=(consolidation_id,))
        if len(df) == 0:
            print(f'There\'s no data for selected consolidation_id -->', consolidation_id)
            return status, period, dfs_list
        rename_dict = {'SUP_ID': 'Supplier ID', 'Supplier_Name': 'Supplier Name', 'ITEM_ID': 'Item Code',
                       'PACKAGE_ID': 'Packing',
                       'GROSS_PRICE': 'Gross', 'ITEM_NAME': 'Item Name', 'GRAND_TOTAL': 'Qty'}

        # Rename the columns using the dictionary
        df.rename(columns=rename_dict, inplace=True)

        # Get the period date from the PERIOD column
        period = str(df.iloc[0, 0])
        # Convert the date string to a datetime object
        date_object = datetime.strptime(period, '%Y-%m-%d')

        # Format the datetime object to 'Month-Year' format
        period = date_object.strftime('%B-%Y')
        print(period)
        df.drop(columns=['PERIOD'], inplace=True)
        # List to hold the split DataFrames

        # Splitting the DataFrame into chunks of 3 rows
        n = 36  # Interval of rows per chunk
        for i in range(0, len(df), n):
            dfs_list.append(df[i:i + n])

        status = "success"
    except Exception as error:
        print('The cause of error -->', error)

    return status, period, dfs_list


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


def create_selected_sup_pdf(dfs_list, period, consolidation_id):
    pdf_file_name = []
    person_who_generates = "Administrator"
    page_num = 0
    total_page = len(dfs_list)
    pdf_status = "failed"
    output_path = None
    try:
        for df in dfs_list:
            print(df)
            page_num += 1
            current_datetime = datetime.now()
            # Format the current date and time as desired (example: YYYY-MM-DD_HH-MM-SS)
            current_date_time_str = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")
            file_name = f'{consolidation_id}_{page_num}_{current_date_time_str}.pdf'
            print(file_name)
            file_name = re.sub(r'[^\w\s.]', '_', file_name)
            directory_path = r"C:\Users\Administrator\Downloads\eiis\selected_supplier_pdf"
            file_with_path = directory_path + "\\" + file_name
            pdf_file_name.append(file_with_path)
            print(pdf_file_name)

            # Create a canvas with the specified page size
            c = canvas.Canvas(file_with_path, pagesize=letter)

            # Page size
            width, height = letter

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
            third_element = f"SELECTED SUPPLIER  {period}"
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

            consolidation_id = f"Consolidation ID: {consolidation_id}"
            c.setFont("Helvetica-Bold", 8)
            c.setFillColor(colors.blue)
            id_text_width = c.stringWidth(consolidation_id)
            id_text_x = rect_x + rect_width - id_text_width - 0.3 * cm
            id_text_y = rect_y + 0.3 * cm
            c.drawString(id_text_x, id_text_y, consolidation_id)

            # Now let's handle the DataFrame content below
            table_y = rect_y  # Start 10 points below the first rectangle
            # Convert DataFrame to data that can be used by ReportLab's Table
            data = [df.columns.to_list()] + df.values.tolist()
            col_widths = [48, 150, 78, 50, 50, 60, 120]
            # Create a table and style it
            table = Table(data, colWidths=col_widths)
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.white),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),  # Ensuring all headers are bold
                ('FONTSIZE', (0, 0), (-1, -1), 8),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 2.98),
                ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('ALIGN', (1, 1), (1, -1), 'LEFT'),  # Left alignment for the second column
                ('ALIGN', (-1, 1), (-1, -1), 'LEFT'),  # Left alignment for the last column
                ('ALIGN', (3, 1), (3, -1), 'RIGHT'),  # Right alignment for the fourth column
                ('ALIGN', (4, 1), (4, -1), 'RIGHT'),  # Right alignment for the fifth column
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),  # Data rows font
                ('FONTSIZE', (0, 1), (-1, -1), 6)  # Data rows font size
            ]))

            # Adjust the position of the table according to the available space
            table.wrapOn(c, width, height)
            table.drawOn(c, left_margin, table_y - table._height)

            # Calculate the coordinates for the text "Generated By : {person_who_genrates}"
            generated_by_text_x = bottom_margin
            generated_by_text_y = bottom_margin - 0.4 * cm  # Adjust the y-coordinate for vertical positioning
            # Draw the text "Generated By : {person_who_genrates}"
            c.setFont("Helvetica", 8)  # Adjust font and size as needed
            c.setFillColor(colors.black)
            c.drawString(generated_by_text_x, generated_by_text_y, f"Generated By : {person_who_generates}")

            # Calculate the width of "Page 1 of 1" text
            page_text_width = c.stringWidth(f"Page {page_num} of {total_page}")

            # Calculate the x-coordinate for "Page 1 of 1" to center it horizontally
            page_text_x = (width - page_text_width) / 2

            # Draw "Page 1 of 1" text in the middle of the page
            c.drawString(page_text_x, generated_by_text_y, "Page 1 of 1")

            # Get the current date and format it
            current_date = datetime.now().strftime("%B %d, %Y")

            # Calculate the width of the formatted current date
            current_date_width = c.stringWidth(current_date)
            print(width)

            # Calculate the x-coordinate for the current date to align it to the right side of the page
            current_date_x = 583 - bottom_margin - current_date_width

            # Draw the current date on the same line as "Generated By :" and "Page 1 of 1"
            c.drawString(current_date_x, generated_by_text_y, f"Date : {current_date}")

            # Save the PDF
            c.showPage()
            c.save()

        # Output folder path for the merged PDF
        output_folder = r'C:\Users\Administrator\Downloads\eiis\selected_supplier_pdf\merged_pdf'

        # Ensure the output folder exists, create it if not
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        # Output file path for the merged PDF
        output_path = os.path.join(output_folder, file_name)
        # Merge the PDFs
        status, output_path = merge_pdfs(pdf_file_name, output_path)
        if status == "success":
            pdf_status = status
        else:
            pdf_status = status
    except Exception as error:
        print('The cause of error -->', error)

    return pdf_status, output_path, file_name
