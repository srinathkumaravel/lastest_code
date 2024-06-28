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
import requests
import json


def get_price_comparison_data(period, consolidationId, itemId):
    item_details = None
    dfs = None
    try:
        # Correct the URL with the valid IP address or domain
        url = "http://103.27.234.110:8080/portal_scm/procurementProcessController/showPriceComparison"
        # JSON data to be sent with the POST request
        data = {
            "period": str(period),
            "consolidationId": str(consolidationId),
            "itemId": str(itemId)
        }

        # Headers to indicate that the request body is JSON
        headers = {
            'Content-Type': 'application/json'
        }

        try:
            # Sending the POST request
            response = requests.post(url, headers=headers, data=json.dumps(data))
            response.raise_for_status()  # Raises an HTTPError for bad responses
            # Process the response if it's successful (200)
            if response.status_code == 200:
                print("Response received successfully:")
                return_response = response.json()  # Assuming the response will be JSON
                # pprint(return_response)
            else:
                print(f"Failed to receive a successful HTTP response: Status code {response.status_code}")
        except requests.RequestException as e:
            status = "failed"
            print('Error while calling JAVA SIDE API FOR FETCHING DATA')
            print(f"An error occurred: {e}")
            return status, dfs, item_details

        # Initialize an empty list to store consolidationIds
        consolidation_ids = []

        # Iterate through each dictionary in the responseContents list and extract the consolidationId
        for con_id in return_response['responseContents']:
            consolidation_ids.append(con_id['consolidationId'])
        print("The fetched CONSOLIDATION ID is -->", consolidation_ids)

        # Initialize an empty list to store item details
        item_details = []

        # Iterate through each dictionary in the responseContents list
        for content in return_response['responseContents']:
            # Iterate through each item in the items list
            for item in content['items']:
                item_name = item['itemName']
                item_id = item['itemId']
                package_id = item['packageId']
                item_details.append({'item_name': item_name, 'item_id': item_id, 'package_id': package_id})
        # print(item_details)
        # print(len(item_details))

        dfs = []

        # Iterate through each dictionary in the responseContents list
        for content in return_response['responseContents']:
            # Iterate through each sublist in the items
            for sub_list in content['items']:
                supplier_name = []
                supplier_id = []
                term = []
                disc = []
                gross = []
                net = []
                net_pp = []
                qty = []
                total_cost = []
                stats = []
                remarks = []
                netUp = []
                for sublist in sub_list['subList']:
                    # Extract required information
                    supplier_id.append(sublist['supplierId'])
                    supplier_name.append(sublist['supplierName'])
                    term.append(sublist['term'])
                    disc.append(sublist['disc'])
                    gross.append(sublist['gross'])
                    net.append(round(sublist['net'], 3))
                    net_pp.append(round(sublist['netPp'], 3))
                    qty.append(sublist['qty'])
                    total_cost.append(round(sublist['totalCost'], 3))
                    netUp.append(round(sublist['netUp'], 3))
                    stats.append(sublist['stats'])
                    remarks.append(sublist['remarks'])

                # Create DataFrame for each sublist
                df = pd.DataFrame([supplier_id, supplier_name, term, disc, gross, net, net_pp, qty,
                                   total_cost, netUp, stats, remarks]).T

                rename_dict = {0: 'Supplier ID', 1: 'Supplier Name', 2: 'Term', 3: 'Discount',
                               4: 'Gross', 5: 'Net', 6: 'Net Net P.P', 7: 'Qty', 8: 'Amount',
                               9: 'Net Net U.P', 10: 'Stats', 11: 'Remarks'}

                # Rename the columns using the dictionary
                df.rename(columns=rename_dict, inplace=True)
                # Append DataFrame to list
                dfs.append(df)
                print(df)
        # print(len(dfs))
        print('Datas fetched successfully from the API ....')
        status = "success"

    except Exception as error:
        print('The Cause of error -->', error)
        status = "failed"

    return status, dfs, item_details


def create_header(c, period, consolidationId, width, height):
    left_margin = 18
    right_margin = width - 18
    top_margin = height - 18
    bottom_margin = 18
    # Set full margins and draw rectangle
    c.setPageSize((letter[0], letter[1]))
    c.setLineWidth(1)
    c.rect(left_margin, bottom_margin, right_margin - left_margin, top_margin - bottom_margin)

    # Rectangle details
    rect_x = left_margin
    rect_width = width - 2 * left_margin
    rect_height = 2.5 * cm
    rect_y = height - left_margin - rect_height

    c.setLineWidth(1.3)
    c.rect(rect_x, rect_y, rect_width, rect_height, stroke=1, fill=0)

    # Draw vertical line inside the rectangle
    vertical_line_x = rect_x + 3.5 * cm
    vertical_line_start_y = rect_y
    vertical_line_end_y = rect_y + rect_height

    c.setLineWidth(1)
    c.line(vertical_line_x, vertical_line_start_y, vertical_line_x, vertical_line_end_y)

    # Draw the image inside the left box of the rectangle
    image_path = 'C:\\Users\\Administrator\\Downloads\\eiis\\sodexo.jpg'
    image_width = (vertical_line_x - rect_x) * 0.8
    image_height = rect_height * 0.8
    image_x = rect_x + (vertical_line_x - rect_x - image_width) / 2
    image_y = rect_y + (rect_height - image_height) / 2
    c.drawImage(image_path, image_x, image_y, width=image_width, height=image_height)

    third_element = f"Price Comparison Report for the month of {period}"
    list1 = ["SOCAT LLC", "OMAN", third_element]

    text_y = rect_y + (rect_height - len(list1) * c._leading) / 2
    text_y += 1.2 * cm
    text_x = rect_x + 8 * cm
    c.setFont("Helvetica-Bold", 10)

    # Draw list1 on the right side rectangle
    for text in list1:
        text_width = c.stringWidth(text)
        text_x = rect_x + 8 * cm + (rect_width - 8 * cm - text_width) / 2
        c.drawString(text_x, text_y, text)
        text_y -= c._leading

    consolidation_id = f"Consolidation ID: {consolidationId}"
    c.setFont("Helvetica-Bold", 8)
    c.setFillColor(colors.blue)
    id_text_width = c.stringWidth(consolidation_id)
    id_text_x = rect_x + rect_width - id_text_width - 0.3 * cm
    id_text_y = rect_y + 0.3 * cm
    c.drawString(id_text_x, id_text_y, consolidation_id)

    # Draw "Generated by:" text and dynamic variable with adjusted font size
    generated_by_text = "Generated by: "
    generated_by_value = "Administrator"  # You can replace this with your dynamic variable

    # Set font size
    font_size = 6.5

    c.setFont("Helvetica", font_size)

    # Calculate text width
    generated_by_text_width = c.stringWidth(generated_by_text)
    generated_by_value_width = c.stringWidth(generated_by_value)

    # Draw text
    c.drawString(left_margin, bottom_margin - 15, generated_by_text)
    c.drawString(left_margin + generated_by_text_width, bottom_margin - 15, generated_by_value)

    # Draw "Date: <date>" in the right bottom corner
    date_text = "Date: " + datetime.now().strftime("%d %B %Y")
    date_text_width = c.stringWidth(date_text)
    c.drawRightString(right_margin, bottom_margin - 15, date_text)

    return rect_x, rect_y, rect_width, rect_height, bottom_margin, left_margin, top_margin


def draw_price_comparison_pdf(item_details, dfs, period, consolidationId, itemId):
    file_path = None
    file_name = None
    try:
        file_path = r'C:\Users\Administrator\Downloads\eiis\price_comparison_pdf'
        # Format current datetime as a string suitable for a filename
        current_time_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"{consolidationId}_{itemId}_{period}_{current_time_str}.pdf"
        # Append the filename directly to the base_path
        file_path += f"\\{file_name}"
        print(file_path)
        c = canvas.Canvas(file_path, pagesize=letter)
        width, height = letter  # Size of page (letter size)

        # Initialize total pages count
        total_pages = 0

        # Function to draw content within specified margins
        def draw_content_on_page(page_number):
            nonlocal total_pages  # Ensure we're using the total_pages from the outer scope
            total_pages += 1

        # Draw header content on the first page
        rect_x, rect_y, rect_width, rect_height, bottom_margin, left_margin, top_margin = create_header(c, period,
                                                                                                        consolidationId,
                                                                                                        width, height)

        # Initialize content position
        current_y_position = top_margin - 3 * cm
        # Known row height in points
        row_height = 20.19685
        for index, (item, df) in enumerate(zip(item_details, dfs)):
            print(current_y_position)
            # print(df)
            item_name = item['item_name']
            item_id = item['item_id']
            package_id = item['package_id']
            item_details_text = f"Item Name: {item_name}      |   Item ID: {item_id}      |   Package ID: {package_id}"
            c.setFont("Helvetica", 8)
            c.drawString(left_margin + 1 * cm, current_y_position, item_details_text)
            current_position = current_y_position - 0.5 * cm
            # Calculate available vertical space
            available_space = current_position - 2.0 * cm - bottom_margin
            print("available_space", available_space)
            # Calculate the number of rows that can fit within the available space
            rows_per_chunk = int(available_space / row_height)
            rows_per_chunk -= 1  # Adjust for the header if it is included in each chunk
            if available_space < 1.5 * cm:
                c.showPage()
                rect_x, rect_y, rect_width, rect_height, bottom_margin, left_margin, top_margin = create_header(c,
                                                                                                                period,
                                                                                                                consolidationId,
                                                                                                                width,
                                                                                                                height)
                current_y_position = top_margin - 3 * cm
                current_position = current_y_position - 0.5 * cm
                # Calculate available vertical space
                available_space = current_position - 2.0 * cm - bottom_margin
                print("available_space", available_space)
                # Calculate the number of rows that can fit within the available space
                rows_per_chunk = int(available_space / row_height)
                rows_per_chunk -= 1  # Adjust for the header if it is included in each chunk

                # print("rows_per_chunk -->", rows_per_chunk)

            # Split the DataFrame into the first half and the second half
            first_half = df.iloc[:rows_per_chunk]
            second_half = df.iloc[rows_per_chunk:]
            # print('Len of first DF', len(first_half))
            # print('Len of second DF', len(second_half))
            table_y = current_position
            df_data = first_half.values.tolist()
            df_headers = first_half.columns.tolist()

            df_table = Table([df_headers] + df_data, colWidths=[1.8 * cm, 4.6 * cm, 1 * cm, 1 * cm, 1 * cm, 1 * cm,
                                                                1.5 * cm, 1.5 * cm, 1.8 * cm, 1.8 * cm, 1 * cm,
                                                                2.3 * cm])
            df_table.setStyle(TableStyle([('BACKGROUND', (1, 1), (-1, 1), colors.white),
                                          ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                                          ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                                          ('BOTTOMPADDING', (0, 0), (-1, 0), 0.5),
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
                index += 1
                # Insert the new value at the specified index
                dfs.insert(index, second_half)
                new_dict = {'item_name': item_name, 'item_id': item_id, 'package_id': package_id}
                # Insert the new dictionary at the specified index
                item_details.insert(index, new_dict)
                c.showPage()
                rect_x, rect_y, rect_width, rect_height, bottom_margin, left_margin, top_margin = create_header(c,
                                                                                                                period,
                                                                                                                consolidationId,
                                                                                                                width,
                                                                                                                height)
                current_y_position = top_margin - 3 * cm

        c.save()
        status = "success"
    except Exception as error:
        print('The Cause of error -->', error)
        status = "failed"

    return file_path, status, file_name
