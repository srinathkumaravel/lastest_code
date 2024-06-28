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


def fetch_cwh_saving_data(month, year, loc_id):
    df = None
    Savings = 0
    Discount = 0
    total_sav_disc = 0
    try:
        engine = get_database_engine_e_eiis()
        if len(loc_id) == 0:
            df_query = """ SELECT A.TRAN_LOC_ID AS LocationID ,  A.TRAN_LOC_NAME AS LocationName,
            SUM(A.SAV) AS TotalSavings,((B.OP_DISC+B.IN_DISC)/(B.OP_QTY+B.IN_QTY)) AS UnitDiscount,SUM(A.TRAN_DISC) AS TotalDiscount  
            FROM TranInter A,Stock B WHERE A.ITEM_ID=B.ITEM_ID AND A.PACKAGE_ID=B.PACKAGE_ID AND A.ENTITY_ID=B.ENTITY_ID  
            AND  A.ENTITY_ID= 'OM01' AND A.TRANS_TYPE IN ('LD','DD','LR','DR') AND MONTH(A.PERIOD) = %s AND YEAR(A.PERIOD) = %s
            GROUP BY A.TRAN_LOC_ID,A.ITEM_ID,A.ENTITY_ID,A.TRANS_TYPE """
            grouped_df = pd.read_sql_query(df_query, engine, params=(month, year))
        elif len(month) == 0:
            df_query = """ SELECT A.TRAN_LOC_ID AS LocationID ,  A.TRAN_LOC_NAME AS LocationName,
                        SUM(A.SAV) AS TotalSavings,((B.OP_DISC+B.IN_DISC)/(B.OP_QTY+B.IN_QTY)) AS UnitDiscount,SUM(A.TRAN_DISC) AS TotalDiscount  FROM TranInter A,Stock B WHERE 
                        A.ITEM_ID=B.ITEM_ID AND A.PACKAGE_ID=B.PACKAGE_ID AND A.ENTITY_ID=B.ENTITY_ID  AND  A.ENTITY_ID= 'OM01' AND A.TRANS_TYPE IN ('LD','DD','LR','DR') 
                        AND A.TRAN_LOC_ID = %s GROUP BY A.TRAN_LOC_ID,A.ITEM_ID,A.ENTITY_ID,A.TRANS_TYPE"""
            grouped_df = pd.read_sql_query(df_query, engine, params=(loc_id,))
        else:
            df_query = """ SELECT A.TRAN_LOC_ID AS LocationID ,  A.TRAN_LOC_NAME AS LocationName,
                           SUM(A.SAV) AS TotalSavings,((B.OP_DISC+B.IN_DISC)/(B.OP_QTY+B.IN_QTY)) AS UnitDiscount,
                           SUM(A.TRAN_DISC) AS TotalDiscount  FROM TranInter A,Stock B WHERE 
                           A.ITEM_ID=B.ITEM_ID AND A.PACKAGE_ID=B.PACKAGE_ID AND A.ENTITY_ID=B.ENTITY_ID  AND  
                           A.ENTITY_ID= 'OM01' AND A.TRANS_TYPE IN ('LD','DD','LR','DR') AND MONTH(A.PERIOD) = %s AND YEAR(A.PERIOD) = %s
                           AND A.TRAN_LOC_ID = %s GROUP BY A.TRAN_LOC_ID,A.ITEM_ID,A.ENTITY_ID,A.TRANS_TYPE"""
            grouped_df = pd.read_sql_query(df_query, engine, params=(month, year, loc_id))
        # Group by both 'LocationID' and 'LocationName' and calculate the sum for other columns
        df = grouped_df.groupby(['LocationID', 'LocationName']).sum().reset_index()

        # Renaming columns
        df = df.rename(columns={
            "LocationID": "Location ID",
            "LocationName": "Location Name",
            "TotalSavings": "Savings",
            "UnitDiscount": "Discount",
            "TotalDiscount": "[Sav] + [Disc]"
        })
        Savings = (round(df['Savings'].sum(), 3))
        Discount = (round(df['Discount'].sum(), 3))
        total_sav_disc = (round(df["[Sav] + [Disc]"].sum(), 3))
        status = "success"
    except Exception as error:
        print('The cause of error -->', error)
        status = "failed"

    return status, df, Savings, Discount, total_sav_disc


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
    third_element = f"CWH Savings - {formatted_date}"
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


def create_cwh_savings_pdf(df, formatted_date, Savings, Discount, total_sav_disc):
    file_name = None
    file_with_path = None
    try:
        person_name = "administrator"
        current_datetime = datetime.now()
        current_date_time_str = current_datetime.strftime("%Y_%m_%d_%H_%M_%S")
        file_name = f'CWH_SAVINGS_{current_date_time_str}.pdf'
        directory_path = r"C:\Users\Administrator\Downloads\eiis\CWH_SAVING_PDF"
        file_with_path = os.path.join(directory_path, file_name)

        # Create a canvas
        c = canvas.Canvas(file_with_path, pagesize=letter)
        width, height = letter  # Size of page (letter size)

        # Draw header and details
        rect_y, top_margin, left_margin, bottom_margin, right_margin, rect_x, rect_height, rect_width = draw_header_and_details(
            c, width, height, formatted_date, person_name)
        text_y = rect_y - 0.1
        # Known row height in points
        row_height = 18.19685

        # Calculate available vertical space
        available_space = text_y - bottom_margin

        # Calculate the number of rows that can fit within the available space
        rows_per_chunk = int(available_space / row_height)

        # Adjust rows_per_chunk to account for not splitting header and data rows unevenly
        # If the header is always included and needs one row by itself:
        rows_per_chunk -= 1  # Adjust for the header if it is included in each chunk
        print('rows_per_chunk', rows_per_chunk)

        chunks = [df[i:i + rows_per_chunk] for i in range(0, len(df), rows_per_chunk)]

        for i, chunk in enumerate(chunks):
            table_y = text_y  # Start the table below the second rectangle

            df_data = chunk.values.tolist()
            df_headers = chunk.columns.tolist()

            # Create the table
            colWidths = [3.0 * cm, 6.0 * cm, 3.7 * cm, 3.8 * cm, 3.8 * cm]

            df_table = Table([df_headers] + df_data, colWidths=colWidths)
            # Styling the table

            # Styling the table
            df_table.setStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.white),  # Header background
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),  # Header text color
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),  # Center align text for all cells
                ('ALIGN', (0, 1), (1, -1), 'LEFT'),  # Left align text in the first and second columns
                ('ALIGN', (2, 1), (4, -1), 'RIGHT'),  # Right align text in the third, fourth, and fifth columns
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),  # Header font
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),  # Body font
                ('FONTSIZE', (0, 0), (-1, -1), 6.0),  # Font size for all cells
                ('BOTTOMPADDING', (0, 0), (-1, -1), 0.1),  # Padding below text
                ('TOPPADDING', (0, 0), (-1, -1), 5),  # Padding above text
                ('GRID', (0, 0), (-1, -1), 1, colors.black)  # Grid lines
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
                rect_y, top_margin, left_margin, bottom_margin, right_margin, rect_x, rect_height, rect_width = draw_header_and_details(
                    c, width, height, formatted_date, person_name)
                text_y = rect_y

            else:
                print('yes')
                # Draw "Closing :" and its value
                text_y -= df_table_height + 20  # Move down below the table

                c.setFont("Helvetica-Bold", 8)
                c.drawString(left_margin + 7 * cm, text_y, "Total :")

                # Draw the closing value
                closing_value_1 = str(Savings)  # Replace with your actual closing value
                c.drawString(left_margin + 11.0 * cm, text_y, closing_value_1)

                # Draw the closing value
                closing_value_2 = str(Discount)  # Replace with your actual closing value
                c.drawString(left_margin + 15.3 * cm, text_y, closing_value_2)

                # Draw the closing value
                closing_value_3 = str(total_sav_disc)  # Replace with your actual closing value
                c.drawString(left_margin + 18.8 * cm, text_y, closing_value_3)
                text_y -= 5
                # Draw a horizontal line below the closing value
                line_start_x = left_margin + 7 * cm
                line_end_x = right_margin
                line_y = text_y  # Adjust the vertical position as needed
                c.line(line_start_x, line_y, line_end_x, line_y)
                text_y -= 3
                line_y = text_y  # Adjust the vertical position as needed
                c.line(line_start_x, line_y, line_end_x, line_y)

        c.save()
        status = "success"
    except Exception as error:
        print('The Cause of error -->', error)
        status = "failed"

    return status, file_name, file_with_path
