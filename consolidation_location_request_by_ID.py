import pandas as pd
from datetime import datetime
from database import get_database_engine_e_eiis
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.pdfgen import canvas
from reportlab.platypus import Table, TableStyle


def execute_consolidation_query(consolidation_id):
    table_data = None
    try:
        # Get the database engine
        engine = get_database_engine_e_eiis()
        query = """
        SELECT con.ITEM_ID, con.ITEM_NAME, con.PACKAGE_ID, con.GRAND_TOTAL FROM 
        consolidation_location_request AS con WHERE CONSOLIDATION_ID = %s;
        """

        # Execute the query and retrieve the data as a DataFrame
        df = pd.read_sql_query(query, engine, params=(consolidation_id,))
        if len(df) == 0:
            status = "failed"
            print(f"No data available for selected consolidated ID --> {consolidation_id}")
            return status, table_data

        rename_dict = {
            'ITEM_ID': 'Item ID',
            'ITEM_NAME': 'Item Name',
            'PACKAGE_ID': 'Packing',
            'GRAND_TOTAL': 'Total Qty'
        }

        # Rename the columns using the dictionary
        df.rename(columns=rename_dict, inplace=True)
        # Convert DataFrame to the desired structure
        table_data = df.values.tolist()

        # Adding column names as the first row
        table_data.insert(0, df.columns.tolist())
        status = "success"
    except Exception as error:
        print('The Cause of error --> ', error)
        status = "failed"

    return status, table_data


def draw_header_content(c, width, height, consolidation_id):
    # Set margins
    margin = 1.5 * cm

    # Rectangle details
    rect_x = margin  # Start from left margin
    rect_width = width - 2 * margin  # Maintain margins on both sides
    rect_height = 2.2 * cm
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
    list1 = ["SOCAT LLC", "OMAN", "CONSOLIDATION LOCATION REQUEST"]

    # Calculate the y position to center the text vertically in the middle of the rectangle
    text_y = rect_y + rect_height - c._leading  # Start from the bottom of the rectangle
    text_y -= 0.2 * cm  # Move the text slightly upwards by adjusting this value

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

    # Draw the Consolidation ID
    c.drawString(id_text_x, id_text_y, consolidation_id)


def create_consolidation_pdf(consolidation_id, table_data):
    try:
        # Define the file path where the PDF will be saved
        # Generate filename with date and time
        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = str(consolidation_id) + "_" + current_datetime + ".pdf"
        file_path = rf'C:\Users\Administrator\Downloads\eiis\consolidation_report\{file_name}'

        # Create a canvas object with letter page size
        c = canvas.Canvas(file_path, pagesize=letter)
        width, height = letter  # Size of page (letter size)

        page_number = 1

        # Process the table data in chunks and draw them on pages
        start_row = 0
        while start_row < len(table_data):
            # Draw header content
            try:
                draw_header_content(c, width, height, consolidation_id)
            except Exception as error:
                print('Error occurred in draw_header_content() function')
                print('The cause of error -->', error)
                status = "failed"
                file_path = None
                file_name = None
                return status, file_path, file_name

            # Create table object with column widths for the current chunk
            col_widths = [69, 255, 135, 69]  # Adjust these values as needed for each column
            table_chunk = Table(table_data[start_row:start_row + 35], colWidths=col_widths)

            # Add style to the table
            style = TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.white),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 8),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 7),
                ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('FONTSIZE', (0, 1), (-1, -1), 7.5),
                ('ALIGN', (1, 1), (1, -1), 'LEFT'),  # Left align the second column
                ('ALIGN', (-1, 1), (-1, -1), 'RIGHT')  # Right align the fourth column
            ])
            table_chunk.setStyle(style)

            # Get total height of the current chunk of the table
            table_chunk.wrapOn(c, width, height)
            table_height = table_chunk._height

            # Position the table on the page
            margin = 1.5 * cm
            table_x = margin  # Start from the left margin
            table_y = height - margin - 2.2 * cm - table_height - 0.1 * cm  # Adjust this value as needed

            # Draw the table on the canvas
            table_chunk.drawOn(c, table_x, table_y)

            # Draw page number
            c.setFont("Helvetica", 8)
            page_number_text = f"Page {page_number}"
            page_number_text_width = c.stringWidth(page_number_text)
            page_number_x = margin + (width - 2 * margin - page_number_text_width) / 2
            page_number_y = margin / 2  # 0.75 cm from the bottom margin
            c.drawString(page_number_x, page_number_y, page_number_text)

            # Move to the next page if there are more rows
            start_row += 35
            if start_row < len(table_data):
                c.showPage()
                page_number += 1

        # Save the PDF
        c.save()
        status = "success"
    except Exception as error:
        print('The cause of error -->', error)
        status = "failed"
        file_path = None
        file_name = None

    return status, file_path, file_name
