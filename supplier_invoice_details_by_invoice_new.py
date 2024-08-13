from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm
from reportlab.platypus import Table, TableStyle
from reportlab.lib import colors
import os
import pandas as pd
from datetime import datetime
from database import get_database_engine_e_eiis

# Assuming you have an existing function `get_database_engine_e_eiis` to get the SQLAlchemy engine.
engine = get_database_engine_e_eiis()


def build_query_both(month=None, year=None, supplier_id=None, start_date=None, end_date=None, supp_inv_id=None):
    query = """
        SELECT DISTINCT 
            head.SUPPLIER_ID, 
            sup.Supplier_Name, 
            head.SUPP_INV_ID, 
            head.SUPP_INV_DATE, 
            head.ORD_LOC_ID, 
            loc.Location_Name,
            head.GRN_ID, 
            det.ITEM_ID, 
            it.Item_Name, 
            det.PACKAGE_ID, 
            det.QTY, 
            det.GP, 
            det.ACTUAL_INV 
        FROM 
            suppdelhead AS head 
        INNER JOIN 
            suppliers AS sup ON sup.Supplier_ID = head.SUPPLIER_ID 
        INNER JOIN 
            suppdeldetail AS det ON det.GRN_ID = head.GRN_ID 
        INNER JOIN 
            item AS it ON it.Item_ID = det.ITEM_ID
        INNER JOIN 
            location AS loc ON loc.Location_ID = head.ORD_LOC_ID 
    """

    conditions = []
    params = []

    if month is not None:
        conditions.append("MONTH(head.PERIOD) = %s")
        params.append(month)
    if year is not None:
        conditions.append("YEAR(head.PERIOD) = %s")
        params.append(year)
    if supplier_id is not None:
        conditions.append("head.SUPPLIER_ID = %s")
        params.append(supplier_id)
    if start_date is not None and end_date is not None:
        conditions.append("head.SUPP_INV_DATE BETWEEN %s AND %s")
        params.append(start_date)
        params.append(end_date)
    if supp_inv_id is not None:
        conditions.append("head.SUPP_INV_ID = %s")
        params.append(supp_inv_id)
    conditions.append("head.STATUS_FK = '1'")
    if conditions:
        where_clause = " WHERE " + " AND ".join(conditions)
        query += where_clause

    query += """
        ORDER BY 
            head.SUPPLIER_ID, 
            head.SUPP_INV_ID, 
            head.ORD_LOC_ID, 
            head.GRN_ID, 
            det.ITEM_ID;
    """

    return query, params


def build_query_dd(month=None, year=None, supplier_id=None, start_date=None, end_date=None, supp_inv_id=None):
    query = """
        SELECT DISTINCT 
            head.SUPPLIER_ID, 
            sup.Supplier_Name, 
            head.SUPP_INV_ID, 
            head.SUPP_INV_DATE, 
            head.ORD_LOC_ID, 
            loc.Location_Name,
            head.GRN_ID, 
            det.ITEM_ID, 
            it.Item_Name, 
            det.PACKAGE_ID, 
            det.QTY, 
            det.GP, 
            det.ACTUAL_INV 
        FROM 
            suppdelhead AS head 
        INNER JOIN 
            suppliers AS sup ON sup.Supplier_ID = head.SUPPLIER_ID 
        INNER JOIN 
            suppdeldetail AS det ON det.GRN_ID = head.GRN_ID 
        INNER JOIN 
            item AS it ON it.Item_ID = det.ITEM_ID
        INNER JOIN 
            location AS loc ON loc.Location_ID = head.ORD_LOC_ID 
    """

    conditions = []
    params = []

    if month is not None:
        conditions.append("MONTH(head.PERIOD) = %s")
        params.append(month)
    if year is not None:
        conditions.append("YEAR(head.PERIOD) = %s")
        params.append(year)
    if supplier_id is not None:
        conditions.append("head.SUPPLIER_ID = %s")
        params.append(supplier_id)
    if start_date is not None and end_date is not None:
        conditions.append("head.SUPP_INV_DATE BETWEEN %s AND %s")
        params.append(start_date)
        params.append(end_date)
    if supp_inv_id is not None:
        conditions.append("head.SUPP_INV_ID = %s")
        params.append(supp_inv_id)

    conditions.append("head.STATUS_FK = '1'")

    conditions.append("ORD_LOC_ID != (SELECT cwh FROM entityeiis)")

    if conditions:
        where_clause = " WHERE " + " AND ".join(conditions)
        query += where_clause

    query += """
        ORDER BY 
            head.SUPPLIER_ID, 
            head.SUPP_INV_ID, 
            head.ORD_LOC_ID, 
            head.GRN_ID, 
            det.ITEM_ID;
    """

    return query, params


def build_query_cwh(month=None, year=None, supplier_id=None, start_date=None, end_date=None, supp_inv_id=None):
    query = """
        SELECT DISTINCT 
            head.SUPPLIER_ID, 
            sup.Supplier_Name, 
            head.SUPP_INV_ID, 
            head.SUPP_INV_DATE, 
            head.ORD_LOC_ID, 
            loc.Location_Name,
            head.GRN_ID, 
            det.ITEM_ID, 
            it.Item_Name, 
            det.PACKAGE_ID, 
            det.QTY, 
            det.GP, 
            det.ACTUAL_INV 
        FROM 
            suppdelhead AS head 
        INNER JOIN 
            suppliers AS sup ON sup.Supplier_ID = head.SUPPLIER_ID 
        INNER JOIN 
            suppdeldetail AS det ON det.GRN_ID = head.GRN_ID 
        INNER JOIN 
            item AS it ON it.Item_ID = det.ITEM_ID
        INNER JOIN 
            location AS loc ON loc.Location_ID = head.ORD_LOC_ID 
    """

    conditions = []
    params = []

    if month is not None:
        conditions.append("MONTH(head.PERIOD) = %s")
        params.append(month)
    if year is not None:
        conditions.append("YEAR(head.PERIOD) = %s")
        params.append(year)
    if supplier_id is not None:
        conditions.append("head.SUPPLIER_ID = %s")
        params.append(supplier_id)
    if start_date is not None and end_date is not None:
        conditions.append("head.SUPP_INV_DATE BETWEEN %s AND %s")
        params.append(start_date)
        params.append(end_date)
    if supp_inv_id is not None:
        conditions.append("head.SUPP_INV_ID = %s")
        params.append(supp_inv_id)

    conditions.append("head.STATUS_FK = '1'")

    conditions.append("ORD_LOC_ID = (SELECT cwh FROM entityeiis)")

    if conditions:
        where_clause = " WHERE " + " AND ".join(conditions)
        query += where_clause

    query += """
        ORDER BY 
            head.SUPPLIER_ID, 
            head.SUPP_INV_ID, 
            head.ORD_LOC_ID, 
            head.GRN_ID, 
            det.ITEM_ID;
    """

    return query, params


def fetch_data(month, year, supplier_id, start_date, end_date, supp_inv_id, report_type):
    if report_type == 1:
        print('[FETCHING CWH DELIVERY DATA...]')
        df_query, params = build_query_cwh(month, year, supplier_id, start_date, end_date, supp_inv_id)

    elif report_type == 2:
        print('[FETCHING DIRECT DELIVERY DATA ...]')
        df_query, params = build_query_dd(month, year, supplier_id, start_date, end_date, supp_inv_id)

    else:
        print('[FETCHING BOTH CWH AND DIRECT DELIVERY DATA ...]')
        df_query, params = build_query_both(month, year, supplier_id, start_date, end_date, supp_inv_id)
        # Ensure params are passed as a list of tuples
    params_tuple = tuple(params)
    print(params_tuple)
    df = pd.read_sql_query(df_query, engine, params=params_tuple)

    return df


def process_data(df):
    unique_supplier_ids = None
    unique_supplier_names = None
    unique_supplier_inv_ids = None
    dfs = None
    try:
        # Extract unique values
        unique_supplier_ids = df['SUPPLIER_ID'].unique()
        unique_supplier_names = df['Supplier_Name'].unique()
        unique_supplier_inv_ids = df['SUPP_INV_ID'].unique()

        # Sort and group the DataFrame
        df_sorted = df.sort_values(by='SUPPLIER_ID')
        grouped = df_sorted.groupby(['SUPP_INV_ID', 'ORD_LOC_ID'])

        # List to hold the individual DataFrames
        dfs = [group.reset_index(drop=True) for _, group in grouped]
        print(dfs)
        print(len(unique_supplier_ids))
        print(len(unique_supplier_inv_ids))
        df_status = "success"  # You can use this variable as needed
    except Exception as error:
        print('Error occurred in process_data() function')
        print('The cause of error -->', error)
        df_status = "failed"

    return unique_supplier_ids, unique_supplier_names, unique_supplier_inv_ids, dfs, df_status


def fetch_supplier_invoice_data_by_supplier_id_new(month, year, supplier_id, start_date, end_date, supp_inv_id,
                                                   report_type, period):
    dfs = fetch_data(month, year, supplier_id, start_date, end_date, supp_inv_id, report_type)
    unique_supplier_ids, unique_supplier_name, unique_supplier_inv_id, dfs, df_status = process_data(dfs)

    if df_status == "success":
        pdf_status, pdf_file_name, pdf_filepath = create_supplier_invoice_details_pdf(unique_supplier_ids,
                                                                                      unique_supplier_name,
                                                                                      unique_supplier_inv_id, period,
                                                                                      dfs)
        if pdf_status == "success":
            final_status = "success"
    else:
        final_status = "failed"
        pdf_file_name = None
        pdf_filepath = None
    return final_status, pdf_file_name, pdf_filepath


def create_header(c, period, width, height):
    try:
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

        third_element = f" Supplier Invoice Details By Invoice {period}"
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
    except Exception as error:
        print('The Cause of error -->', error)
    return rect_x, rect_y, rect_width, rect_height, bottom_margin, left_margin, top_margin, right_margin


def create_supplier_invoice_details_pdf(unique_supplier_ids, unique_supplier_name, unique_supplier_inv_id, period, dfs):
    filepath = None
    file_name = None
    try:
        # Ensure the directory exists
        path = r'C:\Users\Administrator\Downloads\eiis\sup_inv_del'
        if not os.path.exists(path):
            os.makedirs(path)

        current_time_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"SUP_INV_{period}_{current_time_str}.pdf"
        filepath = os.path.join(path, file_name)
        print(filepath)

        c = canvas.Canvas(filepath, pagesize=letter)
        width, height = letter
        # Ensure create_header function is defined or replace it with appropriate code
        rect_x, rect_y, rect_width, rect_height, bottom_margin, left_margin, top_margin, right_margin = create_header(
            c, period, width, height
        )

        row_height = 20
        current_y_position = top_margin - 3 * cm
        grand_total = 0

        for sup_id, sup_name in zip(unique_supplier_ids, unique_supplier_name):
            sup_details_rect_y_position = current_y_position - 1.5 * cm
            c.setFont("Helvetica-Bold", 10)
            c.drawString(left_margin + 30, current_y_position + 0.2 * cm, f"Supplier ID: {sup_id}")
            c.drawString(left_margin + 240, current_y_position + 0.2 * cm, f"Supplier Name: {sup_name}")
            inv_rect_y_position = current_y_position - 1.5 * cm
            sub_grand_total = 0

            for inv_no in unique_supplier_inv_id:
                for i, original_sup_df in enumerate(dfs):
                    sup_df = original_sup_df.copy()  # Work with a copy of the DataFrame
                    available_space = inv_rect_y_position - bottom_margin
                    # print("available_space", available_space)
                    # Calculate the number of rows that can fit within the available space
                    rows_per_chunk = int(available_space / row_height)
                    rows_per_chunk -= 1  # Adjust for the header if it is included in each chunk
                    if available_space < 3 * cm:
                        c.showPage()
                        rect_x, rect_y, rect_width, rect_height, bottom_margin, left_margin, top_margin, right_margin = create_header(
                            c, period, width, height)
                        current_y_position = top_margin - 3 * cm
                        inv_rect_y_position = current_y_position - 1.5 * cm
                    first_element_supp_inv_id = sup_df['SUPP_INV_ID'].iloc[0]
                    first_elemnt_sup_id = sup_df['SUPPLIER_ID'].iloc[0]

                    if inv_no == first_element_supp_inv_id and sup_id == first_elemnt_sup_id:
                        c.rect(left_margin, inv_rect_y_position, width - 2 * left_margin, 1 * cm)
                        c.setFont("Helvetica-Bold", 9)
                        c.drawString(left_margin + 5, inv_rect_y_position + 0.3 * cm, f"Supp invoice no : {inv_no}")
                        inv_rect_y_position -= 1.5 * cm
                        sup_details_rect_y_position = inv_rect_y_position - 0.5 * cm
                        loc_id = sup_df['ORD_LOC_ID'].iloc[0]
                        loc_name = sup_df['Location_Name'].iloc[0]
                        # print("Location ID - ", loc_id)
                        # print("Location Name - ", loc_name)
                        total_value = ((round(sup_df['ACTUAL_INV'].sum(), 3)))
                        # Remove the 'value' column

                        sup_rect_x_position = left_margin
                        sup_rect_width = width - 2 * left_margin
                        sup_rect_height = 1 * cm  # Adjust the height as needed
                        # Draw the rectangle
                        c.rect(sup_rect_x_position, sup_details_rect_y_position, sup_rect_width, sup_rect_height)
                        # sup_details_rect_y_position -= 0.01 * cm
                        c.setFont("Helvetica", 9)
                        c.drawString(left_margin + 5, sup_details_rect_y_position + 1.3 * cm,
                                     f"Delivery Location ID : {loc_id}")
                        c.drawString(left_margin + 150, sup_details_rect_y_position + 1.3 * cm,
                                     f"Delivery Name : {loc_name}")

                        current_position = sup_details_rect_y_position
                        # Calculate available vertical space
                        available_space = current_position - bottom_margin
                        # Calculate the number of rows that can fit within the available space
                        rows_per_chunk = int(available_space / row_height)
                        rows_per_chunk -= 1  # Adjust for the header if it is included in each chunk
                        if available_space < 3.0 * cm:
                            print("YES AVAILABLE SPACE IS LESS THAN 2.5 CM")
                            c.showPage()
                            rect_x, rect_y, rect_width, rect_height, bottom_margin, left_margin, top_margin, right_margin = create_header(
                                c, period, width, height)
                            current_position = top_margin - 3 * cm
                            inv_rect_y_position = current_position - 0.2 * cm
                            available_space = current_position - bottom_margin
                            # print('available_space', available_space)
                            rows_per_chunk = int(available_space / row_height)
                            rows_per_chunk -= 1
                            sup_details_rect_y_position = current_position - 1.0 * cm
                        # print(current_position)
                        table_y = current_position + 1.0 * cm
                        first_half = sup_df.iloc[:rows_per_chunk]
                        first_half = first_half.drop(
                            columns=['ORD_LOC_ID', 'Location_Name', 'SUPPLIER_ID', 'Supplier_Name', 'SUPP_INV_ID'])
                        rename_dict = {
                            'SUPP_INV_DATE': 'Inv Date',
                            'GRN_ID': 'Del Ref',
                            'ITEM_ID': 'Item Code',
                            'Item_Name': 'Item Name',
                            'PACKAGE_ID': 'Packing',
                            'QTY': 'Qty',
                            'ACTUAL_INV': 'Total',
                            'GP': 'Unit Price'
                        }
                        # Rename the columns using the dictionary
                        first_half.rename(columns=rename_dict, inplace=True)
                        # print("Location ID - ", loc_id)
                        # print("Location Name - ", loc_name)

                        second_half = sup_df.iloc[rows_per_chunk:]

                        df_data = first_half.values.tolist()
                        df_headers = first_half.columns.tolist()

                        data = [df_headers] + df_data

                        table = Table(data,
                                      colWidths=[2.0 * cm, 3.5 * cm, 2 * cm, 4.3 * cm, 3.1 * cm, 1.8 * cm, 1.8 * cm,
                                                 1.8 * cm])

                        table.setStyle(TableStyle([
                            ('BACKGROUND', (0, 0), (-1, 0), colors.white),
                            ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                            ('FONTSIZE', (0, 0), (-1, 0), 8),
                            ('BOTTOMPADDING', (0, 0), (-1, 0), 4),
                            ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                            ('GRID', (0, 0), (-1, -1), 1, colors.black),
                            ('FONTSIZE', (0, 1), (-1, -1), 6.5),
                            ('ALIGN', (1, 1), (1, -1), 'LEFT'),  # 2nd column
                            ('ALIGN', (3, 1), (3, -1), 'LEFT'),  # 4th column
                            ('ALIGN', (4, 1), (4, -1), 'LEFT'),  # 4th column
                            ('ALIGN', (5, 1), (5, -1), 'RIGHT'),  # 6th column
                            ('ALIGN', (6, 1), (6, -1), 'RIGHT'),  # 7th column
                            ('ALIGN', (7, 1), (7, -1), 'RIGHT')  # 8th column
                        ]))

                        table_width, table_height = table.wrapOn(c, width, height)

                        df_table_height = table.wrap(0, 0)[1]
                        table_width = width - 2 * left_margin
                        table.wrapOn(c, table_width, height)
                        table.drawOn(c, left_margin, table_y - df_table_height)
                        # print(sup_details_rect_y_position)
                        sup_details_rect_y_position -= table_height + 0.3 * cm
                        # print(sup_details_rect_y_position)
                        inv_rect_y_position = sup_details_rect_y_position + 0.6 * cm
                        if len(second_half) == 0:
                            sub_grand_total += total_value
                            # Draw the "Total" text in bold font and its value in normal font
                            c.setFont("Helvetica-Bold", 8)
                            c.drawString(left_margin + 1 * cm, inv_rect_y_position - 0.2 * cm, " Inv Sub Total-Disc : ")
                            c.setFont("Helvetica-Bold", 8)
                            c.drawString(left_margin + 4 * cm, inv_rect_y_position - 0.2 * cm, str(total_value))

                            # Draw the "Total" text in bold font and its value in normal font
                            c.setFont("Helvetica-Bold", 8)
                            c.drawString(left_margin + 16 * cm, inv_rect_y_position - 0.2 * cm, "Inv Sub Total : ")
                            c.setFont("Helvetica-Bold", 8)
                            c.drawString(left_margin + 19 * cm, inv_rect_y_position - 0.2 * cm, str(total_value))
                            inv_rect_y_position -= 1.3 * cm
                            pass
                        else:
                            i += 1
                            dfs.insert(i, second_half)
                            c.showPage()
                            rect_x, rect_y, rect_width, rect_height, bottom_margin, left_margin, top_margin, right_margin = create_header(
                                c, period, width, height)
                            current_y_position = top_margin - 3 * cm
                            inv_rect_y_position = current_y_position

            current_y_position = inv_rect_y_position
            c.setFont("Helvetica-Bold", 8)
            c.drawString(left_margin + 1 * cm, inv_rect_y_position + 0.2 * cm, " Sup Sub Total-Disc: ")
            c.setFont("Helvetica-Bold", 8)
            c.drawString(left_margin + 4 * cm, inv_rect_y_position + 0.2 * cm, str(sub_grand_total))

            # Draw the "Total" text in bold font and its value in normal font
            c.setFont("Helvetica-Bold", 8)
            c.drawString(left_margin + 19 * cm, inv_rect_y_position + 0.2 * cm, str(sub_grand_total))
            inv_rect_y_position -= 1.0 * cm
            pass
            c.line(left_margin, current_y_position, right_margin, current_y_position)
            current_y_position = inv_rect_y_position - 0.5 * cm
            grand_total += sub_grand_total
        c.setFont("Helvetica-Bold", 8)
        c.drawString(left_margin + 1 * cm, current_y_position - 0.2 * cm, " Inv Grand Total-Disc: ")
        c.setFont("Helvetica-Bold", 8)
        grand_total = round(grand_total, 3)
        c.drawString(left_margin + 4 * cm, current_y_position - 0.2 * cm, str(grand_total))
        # Draw the "Total" text in bold font and its value in normal font
        c.setFont("Helvetica-Bold", 8)
        c.drawString(left_margin + 17.5 * cm, current_y_position - 0.2 * cm, str(grand_total))

        c.line(left_margin, current_y_position - 0.4 * cm, right_margin, current_y_position - 0.4 * cm)
        # current_y_position = inv_rect_y_position - 0.5 * cm
        c.save()
        print("success")
        status = "success"

    except Exception as error:
        print('The cause of error', error)
        status = "failed"
    return status, file_name, filepath
