from datetime import datetime
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm
from reportlab.platypus import Table, TableStyle
from reportlab.lib import colors
import pandas as pd
from reportlab.lib.utils import simpleSplit
from datetime import datetime
from database import get_database_connection_e_eiis, get_database_engine_e_eiis


def get_req_data(req_no):
    with get_database_connection_e_eiis() as conn:
        records = None
        formatted_data_list = None

        try:
            # Create a cursor object
            cursor = conn.cursor()

            # SQL query with placeholders for parameters
            query = """
                    SELECT head.SUPPLIER_ID, sup.SupplierName, sup.FaxNo, sup.TelNo FROM 
                    qtn_req_head AS head INNER JOIN suppliers AS sup ON 
                    sup.SupplierID = head.SUPPLIER_ID WHERE QTN_REQ_NO = %s;
                    """

            # Execute the query with dynamic parameters
            cursor.execute(query, (req_no,))

            # Fetch all records
            records = cursor.fetchall()
            if len(records) != 0:
                pass
            else:
                status = "failed"
                print(f'MESSAGE --> No Records found on Suppliers for given req no --> {req_no}')
                return status, records, formatted_data_list
        except Exception as error:
            print('The Cause of error -->', error)
            status = "failed"
            return status, records, formatted_data_list
        try:
            engine = get_database_engine_e_eiis()
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
            # Create a list of lists from the DataFrame
            data_list = [df.columns.tolist()] + df.values.tolist()

            # Convert all elements to string and format as required
            formatted_data_list = []
            for row in data_list:
                formatted_row = [str(item) for item in row]
                formatted_data_list.append(formatted_row)
            status = "success"

        except Exception as error:
            print('The cause of error -->', error)
            status = "failed"

        return status, records, formatted_data_list


def create_req_pdf(formatted_data_list, records, from_date, before_date, shelf_life, quantity):
    try:
        supplier_name = records[0][1]
        attention = "xxxxxxxx"
        tel_no = records[0][3]
        fax_no = records[0][2]
        percentage_in_number = "0.5"
        month_in_numbers = "8"
        person_who_generates = "Administrator"

        # Get the current date and time
        current_datetime = datetime.now()

        # Format the current date and time as desired (example: YYYY-MM-DD_HH-MM-SS)
        current_date_time_str = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")

        # Construct the file name with the supplier name and current date/time
        file_name = f"{supplier_name}_{current_date_time_str}.pdf"

        # Construct the full file path
        file_path = r'C:\Users\Administrator\Downloads\eiis\quotation request\\' + file_name

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
        box_below_x = margin  # Start from left margin
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

        # Define the vertical offset for positioning the additional text above the existing box
        text_vertical_offset = -3.4 * cm  # Adjust this value as needed

        # Calculate the y-coordinate to start drawing the additional text
        additional_text_y = box_below_y + box_below_height + text_vertical_offset
        # Define the text with placeholders for dynamic dates
        additional_text = f"Please find hereunder the approximate quantity of food and cleaning items which will be procured by us for the next months with effect from {from_date}. We request you to send your Quotation for our consideration on or before {before_date} with the following specs package, brand, country of origin."

        # Define the width available for text
        text_width = box_below_width

        # Calculate the remaining height available for text
        remaining_height = additional_text_y - margin

        # Calculate the text lines based on available width and height
        text_lines = simpleSplit(additional_text, c._fontname, c._fontsize, text_width)

        # Font settings for the additional text
        c.setFont("Helvetica", 10)

        # Draw the additional text below the existing box
        for line in text_lines:
            c.drawString(margin, additional_text_y, line)
            additional_text_y -= c._leading  # Move to the next line above

        # Font settings for the additional content text
        c.setFont("Helvetica", 10)

        # Define the vertical offset for positioning the note text below the existing content
        note_text_vertical_offset = 7.4 * cm  # Adjust this value as needed

        # Calculate the y-coordinate to start drawing the note text
        note_text_y = height - margin - note_text_vertical_offset  # Start from the top margin

        # Define the note text with placeholders for dynamic values
        note_text = f"Note:\n1. The Quantity mentioned below is subject to increase or decrease by {quantity}%\n2. Quotations received after the fixed date / without proper specification shall be rejected.\n3. Shelf life should be above {shelf_life} months"

        # Calculate the width available for note text
        note_text_width = width - 2 * margin

        # Calculate the note text lines based on available width and height
        note_text_lines = simpleSplit(note_text, c._fontname, c._fontsize, note_text_width)

        # Font settings for the note text
        c.setFont("Helvetica", 10)
        # Define the gap between lines
        line_gap = 0.1 * cm  # Adjust this value as needed

        # Draw the note text below the existing content
        for line in note_text_lines:
            # Draw the line of text at the calculated y-coordinate
            c.drawString(margin, note_text_y, line)
            # Move to the next line above for the next iteration
            note_text_y -= c._leading + line_gap

        style = TableStyle([
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
            ('ALIGN', (2, 0), (3, -1), 'LEFT'),  # Align the third and fourth columns (indices 2 and 3) to the left
            ('FONTSIZE', (0, 1), (-1, -1), 7.5),
            ('WORDWRAP', (1, 1), (1, -1), 'LTR'),  # Enforce word wrap in the Description column
        ])

        col_widths = [35, 65, 180, 87, 39, 50, 70]  # Adjust these values as needed for each column

        # Create table object with column widths
        table = Table(formatted_data_list, colWidths=col_widths)

        # Add style to the table
        table.setStyle(style)

        # Get total height of the table
        table.wrapOn(c, width, height)
        table_height = table._height

        # Calculate the y-coordinate to start drawing the table
        table_y = margin + 13 * cm  # Start below the margin, adjusted vertically by 1 centimeter

        # Draw the table below the text
        table.drawOn(c, margin, table_y)

        # Calculate the y-coordinate where the horizontal lines should be drawn
        line_y = table_y - table_height + 1.4 * cm  # Adjust this value as needed for vertical movement

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
        bottom_line_start_x = margin
        bottom_line_end_x = width - margin
        bottom_line_y = margin

        # Draw the horizontal line in the bottom margin
        c.line(bottom_line_start_x, bottom_line_y, bottom_line_end_x, bottom_line_y)

        # Calculate the coordinates for the text "Generated By : {person_who_genrates}"
        generated_by_text_x = margin
        generated_by_text_y = margin - 0.7 * cm  # Adjust the y-coordinate for vertical positioning

        # Draw the text "Generated By : {person_who_genrates}"
        c.setFont("Helvetica", 10)  # Adjust font and size as needed
        c.drawString(generated_by_text_x, generated_by_text_y, f"Generated By : {person_who_generates}")

        # Calculate the width of "Page 1 of 1" text
        page_text_width = c.stringWidth("Page 1 of 1")

        # Calculate the x-coordinate for "Page 1 of 1" to center it horizontally
        page_text_x = (width - page_text_width) / 2

        # Draw "Page 1 of 1" text in the middle of the page
        c.drawString(page_text_x, generated_by_text_y, "Page 1 of 1")

        # Get the current date and format it
        current_date = datetime.now().strftime("%B %d, %Y")

        # Calculate the width of the formatted current date
        current_date_width = c.stringWidth(current_date)

        # Calculate the x-coordinate for the current date to align it to the right side of the page
        current_date_x = width - margin - current_date_width

        # Draw the current date on the same line as "Generated By :" and "Page 1 of 1"
        c.drawString(current_date_x, generated_by_text_y, current_date)

        # Save the PDF
        c.showPage()
        c.save()
        status = "success"
    except Exception as error:
        print('The Cause of error -->', error)
        status = "failed"
        file_path = None

    return status, file_path
