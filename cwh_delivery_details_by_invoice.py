from reportlab.lib.pagesizes import letter, landscape
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm
from reportlab.platypus import Table, TableStyle
from reportlab.lib import colors
import os
import pandas as pd
from datetime import datetime
from database import get_database_engine_e_eiis


def fetch_cwh_invoice_details_by_date_and_location(from_date, to_date, location_id):
    # List to store final DataFrames
    final_df = []
    sub_total_list = []
    Grand_total_list = []
    try:
        engine = get_database_engine_e_eiis()
        # SQL query to fetch data
        if len(location_id) == 0:
            df_query_1 = """
                        SELECT
                        CONCAT(ti.TRAN_LOC_ID, ' ', ti.TRAN_LOC_NAME) AS code_name,
                        ti.OUR_TRANS_IS AS issue_no,
                        ti.TRANS_DATE AS issue_date,
                        ROUND(SUM(itemt.IP02 * ti.QTY), 3) AS issue_price,
                        ROUND(SUM(ti.CP), 3) AS cost_price,
                        ti.REPORT_GROUP,
                        ti.TRANS_TYPE,
                        CASE
                            WHEN acc.ACCOUNT_NAME IN ('FOOD', 'CLEANING', 'DISPOSABLES') THEN acc.ACCOUNT_NAME
                            ELSE 'OTHERS'
                        END AS Item_Category_Name
                    FROM
                        traninter AS ti
                    INNER JOIN
                        mst_item_category AS m ON LEFT(ti.ITEM_ID, 2) = m.ITEM_CAT_PK
                    INNER JOIN 
                        mst_item_account AS acc ON acc.ITEM_ACCOUNT_PK = m.ACCOUNT_FK
                    INNER JOIN 
                        item AS itemt ON itemt.ITEM_ID = ti.ITEM_ID
                    WHERE
                        ti.ENTITY_ID = 'OM01'
                        AND ti.TRANS_DATE BETWEEN %s AND %s
                        AND (ti.TRANS_TYPE = 'DD' OR ti.TRANS_TYPE = 'LD' OR ti.TRANS_TYPE = 'SD')
                    GROUP BY
                        ti.TRAN_LOC_ID,
                        ti.TRANS_TYPE,
                        ti.OUR_TRANS_IS, 
                        ti.REPORT_GROUP, 
                        m.ACCOUNT_FK
                    ORDER BY
                        ti.TRAN_LOC_ID,
                        ti.OUR_TRANS_IS,
                        ti.TRANS_DATE;

                        """
            # Fetch data from database into DataFrame
            df = pd.read_sql_query(df_query_1, engine, params=(from_date, to_date))
        else:
            df_query_1 = """
                        SELECT
                    CONCAT(ti.TRAN_LOC_ID, ' ', ti.TRAN_LOC_NAME) AS code_name,
                    ti.OUR_TRANS_IS AS issue_no,
                    ti.TRANS_DATE AS issue_date,
                    ROUND(SUM(itemt.IP02 * ti.QTY), 3) AS issue_price,
                    ROUND(SUM(ti.CP), 3) AS cost_price,
                    ti.REPORT_GROUP,
                    ti.TRANS_TYPE,
                    CASE
                        WHEN acc.ACCOUNT_NAME IN ('FOOD', 'CLEANING', 'DISPOSABLES') THEN acc.ACCOUNT_NAME
                        ELSE 'OTHERS'
                    END AS Item_Category_Name
                FROM
                    traninter AS ti
                INNER JOIN
                    mst_item_category AS m ON LEFT(ti.ITEM_ID, 2) = m.ITEM_CAT_PK
                INNER JOIN 
                    mst_item_account AS acc ON acc.ITEM_ACCOUNT_PK = m.ACCOUNT_FK
                INNER JOIN 
                    item AS itemt ON itemt.ITEM_ID = ti.ITEM_ID
                WHERE
                    ti.ENTITY_ID = 'OM01' AND
                    ti.TRANS_DATE BETWEEN %s AND %s
                    AND ti.TRAN_LOC_ID = %s
                    AND (ti.TRANS_TYPE = 'DD' OR ti.TRANS_TYPE = 'LD' OR ti.TRANS_TYPE = 'SD')
                GROUP BY
                    ti.TRAN_LOC_ID,
                    ti.TRANS_TYPE,
                    ti.OUR_TRANS_IS, 
                    ti.REPORT_GROUP, 
                    m.ACCOUNT_FK
                ORDER BY
                    ti.TRAN_LOC_ID,
                    ti.OUR_TRANS_IS,
                    ti.TRANS_DATE;
                        """
            # Fetch data from database into DataFrame
            df = pd.read_sql_query(df_query_1, engine, params=(from_date, to_date, location_id))
        if len(df) == 0:
            print(f"NO data available for the selected period from_date -- {from_date} and to_date --- {to_date}")
            status = "failed"
            return status, final_df, sub_total_list, Grand_total_list

        # Grouping the DataFrame by 'code_name' and 'issue_no'
        grouped_df = df.groupby(['code_name'])

        # Iterating over the groups and accessing each group as a DataFrame
        for code_name, group_df in grouped_df:
            print(f"Code Name: {code_name}")
            print(group_df)  # This will print each group as a separate DataFrame

            # Pivot the DataFrame for cost price
            df_pivot_cp = group_df.pivot(index='issue_no', columns='Item_Category_Name', values='cost_price')

            # Define all expected categories
            expected_categories = ['FOOD', 'CLEANING ', 'DISPOSAL', 'OTHERS']

            # Ensure all expected categories are present as columns, add them if missing
            for category in expected_categories:
                if category not in df_pivot_cp.columns:
                    df_pivot_cp[category] = 0

            # Reorder the columns as per the expected categories
            df_pivot_cp = df_pivot_cp[expected_categories]

            # Reset index to make sure there's no index column
            df_pivot_cp.reset_index(drop=True, inplace=True)

            # Replace NaN values with 0.000
            df_pivot_cp.fillna(0.000, inplace=True)

            # Add a new column 'Total_cp' that sums up values across each row
            df_pivot_cp['Total_cp'] = df_pivot_cp.sum(axis=1)

            # Pivot the DataFrame for issue price
            df_pivot_ip = group_df.pivot(index='issue_no', columns='Item_Category_Name', values='issue_price')

            # Ensure all expected categories are present as columns, add them if missing
            for category in expected_categories:
                if category not in df_pivot_ip.columns:
                    df_pivot_ip[category] = 0

            # Reorder the columns as per the expected categories
            df_pivot_ip = df_pivot_ip[expected_categories]

            # Reset index to make sure there's no index column
            df_pivot_ip.reset_index(drop=True, inplace=True)

            # Replace NaN values with 0.000
            df_pivot_ip.fillna(0.000, inplace=True)

            # Add a new column 'Total_ip' that sums up values across each row
            df_pivot_ip['Total_ip'] = df_pivot_ip.sum(axis=1)

            # Remove duplicates based on 'code_name' and 'issue_no'
            df_unique = group_df.drop_duplicates(subset=['code_name', 'issue_no'])

            # Keep only the first 3 columns
            df_unique = df_unique.iloc[:, :3]

            # Reset index
            df_unique.reset_index(drop=True, inplace=True)

            # Concatenate horizontally (add new columns)
            concatenated_df = pd.concat([df_unique, df_pivot_cp, df_pivot_ip], axis=1)

            # Calculate savings
            concatenated_df['Savings'] = concatenated_df['Total_ip'] - concatenated_df['Total_cp']

            # Keep only the first value in the first column and replace others with ''
            concatenated_df.loc[1:, 'code_name'] = ''

            concatenated_df = concatenated_df.rename(columns={
                'code_name': 'Code Name',
                'issue_no': 'Issue No.',
                'issue_date': 'Issue Date',
                'FOOD': 'Food',
                'CLEANING': 'Cleaning',
                'DISPOSAL': 'Disposal',
                'OTHERS': 'Others',
                'Total_cp': 'Total',
                'Total_ip': 'Total'
            })
            # Round all columns except the first 3
            concatenated_df.iloc[:, 3:] = concatenated_df.iloc[:, 3:].round(3)
            # Append the final DataFrame to the list
            final_df.append(concatenated_df)
            sub_sub_total_list = []
            Food_issue = round(concatenated_df.iloc[:, 3].sum(), 3)  # Sum of column at index 1
            sub_sub_total_list.append(Food_issue)

            Cleaning_issue = round(concatenated_df.iloc[:, 4].sum(), 3)  # Sum of column at index 2
            sub_sub_total_list.append(Cleaning_issue)

            Disposal_issue = round(concatenated_df.iloc[:, 5].sum(), 3)  # Sum of column at index 2
            sub_sub_total_list.append(Disposal_issue)

            Others_issue = round(concatenated_df.iloc[:, 6].sum(), 3)  # Sum of column at index 2
            sub_sub_total_list.append(Others_issue)

            Total_issue = round(concatenated_df.iloc[:, 7].sum(), 3)
            sub_sub_total_list.append(Total_issue)

            Food_pur = round(concatenated_df.iloc[:, 8].sum(), 3)
            sub_sub_total_list.append(Food_pur)

            Cleaning_pur = round(concatenated_df.iloc[:, 9].sum(), 3)
            sub_sub_total_list.append(Cleaning_pur)

            Disposal_pur = round(concatenated_df.iloc[:, 10].sum(), 3)
            sub_sub_total_list.append(Disposal_pur)

            Others_pur = round(concatenated_df.iloc[:, 11].sum(), 3)
            sub_sub_total_list.append(Others_pur)

            Total_pur = round(concatenated_df.iloc[:, 12].sum(), 3)
            sub_sub_total_list.append(Total_pur)

            savings_pur = round(concatenated_df.iloc[:, 13].sum(), 3)
            sub_sub_total_list.append(savings_pur)

            sub_total_list.append(sub_sub_total_list)
        # Printing final list of DataFrames
        for idx, df in enumerate(final_df):
            print(f"\nFinal DataFrame {idx + 1}:")
            print(df)
        print(sub_total_list)
        # Initialize the list to store the sums
        Grand_total_list = []

        # Calculate the sum for each index position
        for i in range(len(sub_total_list[0])):
            position_sum = sum(sub_total_list[i] for sub_total_list in sub_total_list)
            position_sum_rounded = round(position_sum, 3)
            Grand_total_list.append(position_sum_rounded)

        print("Sum of each element position across sublists (rounded to 3 decimal places):")
        print(Grand_total_list)
        # Optionally, if you want to rename the final DataFrame variable
        # renamed_df = final_df[0].copy()  # Example for renaming the first DataFrame in the list
        status = "success"
    except Exception as error:
        print('The cause of error -->', error)
        status = "failed"

    return status, final_df, sub_total_list, Grand_total_list


def fetch_cwh_invoice_details(month, year, location_id):
    # List to store final DataFrames
    final_df = []
    sub_total_list = []
    Grand_total_list = []
    try:
        engine = get_database_engine_e_eiis()
        # SQL query to fetch data
        if len(location_id) == 0:
            df_query_1 = """
                        SELECT
                        CONCAT(ti.TRAN_LOC_ID, ' ', ti.TRAN_LOC_NAME) AS code_name,
                        ti.OUR_TRANS_IS AS issue_no,
                        ti.TRANS_DATE AS issue_date,
                        ROUND(SUM(itemt.IP02 * ti.QTY), 3) AS issue_price,
                        ROUND(SUM(ti.CP), 3) AS cost_price,
                        ti.REPORT_GROUP,
                        ti.TRANS_TYPE,
                        CASE
                            WHEN acc.ACCOUNT_NAME IN ('FOOD', 'CLEANING', 'DISPOSABLES') THEN acc.ACCOUNT_NAME
                            ELSE 'OTHERS'
                        END AS Item_Category_Name
                    FROM
                        traninter AS ti
                    INNER JOIN
                        mst_item_category AS m ON LEFT(ti.ITEM_ID, 2) = m.ITEM_CAT_PK
                    INNER JOIN 
                        mst_item_account AS acc ON acc.ITEM_ACCOUNT_PK = m.ACCOUNT_FK
                    INNER JOIN 
                    item AS itemt ON itemt.ITEM_ID = ti.ITEM_ID
                    WHERE
                        ti.ENTITY_ID = 'OM01'
                        AND MONTH(ti.PERIOD) = %s
                        AND YEAR(ti.PERIOD) = %s
                        AND (ti.TRANS_TYPE = 'DD' OR ti.TRANS_TYPE = 'LD' OR ti.TRANS_TYPE = 'SD')
                    GROUP BY
                        ti.TRAN_LOC_ID,
                        ti.TRANS_TYPE,
                        ti.OUR_TRANS_IS, 
                        ti.REPORT_GROUP, 
                        m.ACCOUNT_FK
                    ORDER BY
                        ti.TRAN_LOC_ID,
                        ti.OUR_TRANS_IS,
                        ti.TRANS_DATE;
                        """
            # Fetch data from database into DataFrame
            df = pd.read_sql_query(df_query_1, engine, params=(month, year))
        else:
            df_query_1 = """
                        SELECT
                    CONCAT(ti.TRAN_LOC_ID, ' ', ti.TRAN_LOC_NAME) AS code_name,
                    ti.OUR_TRANS_IS AS issue_no,
                    ti.TRANS_DATE AS issue_date,
                    ROUND(SUM(itemt.IP02 * ti.QTY), 3) AS issue_price,
                    ROUND(SUM(ti.CP), 3) AS cost_price,
                    ti.REPORT_GROUP,
                    ti.TRANS_TYPE,
                    CASE
                        WHEN acc.ACCOUNT_NAME IN ('FOOD', 'CLEANING', 'DISPOSABLES') THEN acc.ACCOUNT_NAME
                        ELSE 'OTHERS'
                    END AS Item_Category_Name
                FROM
                    traninter AS ti
                INNER JOIN
                    mst_item_category AS m ON LEFT(ti.ITEM_ID, 2) = m.ITEM_CAT_PK
                INNER JOIN 
                    mst_item_account AS acc ON acc.ITEM_ACCOUNT_PK = m.ACCOUNT_FK
                INNER JOIN 
                    item AS itemt ON itemt.ITEM_ID = ti.ITEM_ID
                WHERE
                    ti.ENTITY_ID = 'OM01'
                    AND MONTH(ti.PERIOD) = %s
                    AND YEAR(ti.PERIOD) = %s
                    AND ti.TRAN_LOC_ID = %s
                    AND (ti.TRANS_TYPE = 'DD' OR ti.TRANS_TYPE = 'LD' OR ti.TRANS_TYPE = 'SD')
                GROUP BY
                    ti.TRAN_LOC_ID,
                    ti.TRANS_TYPE,
                    ti.OUR_TRANS_IS, 
                    ti.REPORT_GROUP, 
                    m.ACCOUNT_FK
                ORDER BY
                    ti.TRAN_LOC_ID,
                    ti.OUR_TRANS_IS,
                    ti.TRANS_DATE;
                        """
            # Fetch data from database into DataFrame
            df = pd.read_sql_query(df_query_1, engine, params=(month, year, location_id))
        if len(df) == 0:
            print(f"NO data available for the selected period month -- {month} and year --- {year}")
            status = "failed"
            return status, final_df, sub_total_list, Grand_total_list

        # Grouping the DataFrame by 'code_name' and 'issue_no'
        grouped_df = df.groupby(['code_name'])

        # Iterating over the groups and accessing each group as a DataFrame
        for code_name, group_df in grouped_df:
            print(f"Code Name: {code_name}")
            print(group_df)  # This will print each group as a separate DataFrame

            # Pivot the DataFrame for cost price
            df_pivot_cp = group_df.pivot(index='issue_no', columns='Item_Category_Name', values='cost_price')

            # Define all expected categories
            expected_categories = ['FOOD', 'CLEANING ', 'DISPOSAL', 'OTHERS']

            # Ensure all expected categories are present as columns, add them if missing
            for category in expected_categories:
                if category not in df_pivot_cp.columns:
                    df_pivot_cp[category] = 0

            # Reorder the columns as per the expected categories
            df_pivot_cp = df_pivot_cp[expected_categories]

            # Reset index to make sure there's no index column
            df_pivot_cp.reset_index(drop=True, inplace=True)

            # Replace NaN values with 0.000
            df_pivot_cp.fillna(0.000, inplace=True)

            # Add a new column 'Total_cp' that sums up values across each row
            df_pivot_cp['Total_cp'] = df_pivot_cp.sum(axis=1)

            # Pivot the DataFrame for issue price
            df_pivot_ip = group_df.pivot(index='issue_no', columns='Item_Category_Name', values='issue_price')

            # Ensure all expected categories are present as columns, add them if missing
            for category in expected_categories:
                if category not in df_pivot_ip.columns:
                    df_pivot_ip[category] = 0

            # Reorder the columns as per the expected categories
            df_pivot_ip = df_pivot_ip[expected_categories]

            # Reset index to make sure there's no index column
            df_pivot_ip.reset_index(drop=True, inplace=True)

            # Replace NaN values with 0.000
            df_pivot_ip.fillna(0.000, inplace=True)

            # Add a new column 'Total_ip' that sums up values across each row
            df_pivot_ip['Total_ip'] = df_pivot_ip.sum(axis=1)

            # Remove duplicates based on 'code_name' and 'issue_no'
            df_unique = group_df.drop_duplicates(subset=['code_name', 'issue_no'])

            # Keep only the first 3 columns
            df_unique = df_unique.iloc[:, :3]

            # Reset index
            df_unique.reset_index(drop=True, inplace=True)

            # Concatenate horizontally (add new columns)
            concatenated_df = pd.concat([df_unique, df_pivot_cp, df_pivot_ip], axis=1)

            # Calculate savings
            concatenated_df['Savings'] = concatenated_df['Total_ip'] - concatenated_df['Total_cp']

            # Keep only the first value in the first column and replace others with ''
            concatenated_df.loc[1:, 'code_name'] = ''

            concatenated_df = concatenated_df.rename(columns={
                'code_name': 'Code Name',
                'issue_no': 'Issue No.',
                'issue_date': 'Issue Date',
                'FOOD': 'Food',
                'CLEANING': 'Cleaning',
                'DISPOSAL': 'Disposal',
                'OTHERS': 'Others',
                'Total_cp': 'Total',
                'Total_ip': 'Total'
            })
            # Round all columns except the first 3
            concatenated_df.iloc[:, 3:] = concatenated_df.iloc[:, 3:].round(3)
            # Append the final DataFrame to the list
            final_df.append(concatenated_df)
            sub_sub_total_list = []
            Food_issue = round(concatenated_df.iloc[:, 3].sum(), 3)  # Sum of column at index 1
            sub_sub_total_list.append(Food_issue)

            Cleaning_issue = round(concatenated_df.iloc[:, 4].sum(), 3)  # Sum of column at index 2
            sub_sub_total_list.append(Cleaning_issue)

            Disposal_issue = round(concatenated_df.iloc[:, 5].sum(), 3)  # Sum of column at index 2
            sub_sub_total_list.append(Disposal_issue)

            Others_issue = round(concatenated_df.iloc[:, 6].sum(), 3)  # Sum of column at index 2
            sub_sub_total_list.append(Others_issue)

            Total_issue = round(concatenated_df.iloc[:, 7].sum(), 3)
            sub_sub_total_list.append(Total_issue)

            Food_pur = round(concatenated_df.iloc[:, 8].sum(), 3)
            sub_sub_total_list.append(Food_pur)

            Cleaning_pur = round(concatenated_df.iloc[:, 9].sum(), 3)
            sub_sub_total_list.append(Cleaning_pur)

            Disposal_pur = round(concatenated_df.iloc[:, 10].sum(), 3)
            sub_sub_total_list.append(Disposal_pur)

            Others_pur = round(concatenated_df.iloc[:, 11].sum(), 3)
            sub_sub_total_list.append(Others_pur)

            Total_pur = round(concatenated_df.iloc[:, 12].sum(), 3)
            sub_sub_total_list.append(Total_pur)

            savings_pur = round(concatenated_df.iloc[:, 13].sum(), 3)
            sub_sub_total_list.append(savings_pur)

            sub_total_list.append(sub_sub_total_list)
        # Printing final list of DataFrames
        for idx, df in enumerate(final_df):
            print(f"\nFinal DataFrame {idx + 1}:")
            print(df)
        print(sub_total_list)
        # Initialize the list to store the sums
        Grand_total_list = []

        # Calculate the sum for each index position
        for i in range(len(sub_total_list[0])):
            position_sum = sum(sub_total_list[i] for sub_total_list in sub_total_list)
            position_sum_rounded = round(position_sum, 3)
            Grand_total_list.append(position_sum_rounded)

        print("Sum of each element position across sublists (rounded to 3 decimal places):")
        print(Grand_total_list)
        # Optionally, if you want to rename the final DataFrame variable
        # renamed_df = final_df[0].copy()  # Example for renaming the first DataFrame in the list
        status = "success"
    except Exception as error:
        print('The cause of error -->', error)
        status = "failed"

    return status, final_df, sub_total_list, Grand_total_list


def create_header(c, period, width, height):
    left_margin = 11
    right_margin = width - 11
    top_margin = height - 11
    bottom_margin = 11

    # Set the line width for the border
    c.setLineWidth(1)

    # Draw the rectangle for the border
    c.rect(left_margin, bottom_margin, right_margin - left_margin, top_margin - bottom_margin)

    # Rectangle details
    rect_x = left_margin
    rect_width = width - 2 * left_margin
    rect_height = 2.0 * cm
    rect_y = height - left_margin - rect_height

    c.setLineWidth(1.3)
    c.rect(rect_x, rect_y, rect_width, rect_height, stroke=1, fill=0)

    # Draw vertical line inside the rectangle
    vertical_line_x = rect_x + 3.5 * cm
    vertical_line_start_y = rect_y
    vertical_line_end_y = rect_y + rect_height

    c.setLineWidth(1)
    c.line(vertical_line_x, vertical_line_start_y, vertical_line_x, vertical_line_end_y)

    # New image path
    image_path = 'C:\\Users\\Administrator\\Downloads\\eiis\\sodexo.jpg'
    image_width = (vertical_line_x - rect_x) * 0.8
    image_height = rect_height * 0.8
    image_x = rect_x + (vertical_line_x - rect_x - image_width) / 2
    image_y = rect_y + (rect_height - image_height) / 2
    c.drawImage(image_path, image_x, image_y, width=image_width, height=image_height)

    third_element = "CWH Delivery Details By Invoice"
    list1 = ["SOCAT LLC", "OMAN", third_element]

    c.setFont("Helvetica-Bold", 10)
    total_text_height = len(list1) * c._leading

    text_y = rect_y + (rect_height - total_text_height) / 2 + total_text_height - c._leading / 2
    for text in list1:
        text_width = c.stringWidth(text)
        text_x = (vertical_line_x - 30) + (rect_width - vertical_line_x - text_width) / 2
        c.drawString(text_x, text_y, text)
        text_y -= c._leading

    # Draw a small box on the right side
    small_box_x = rect_x + rect_width - 5 * cm
    small_box_y = rect_y
    small_box_width = 5 * cm
    small_box_height = rect_height

    c.rect(small_box_x, small_box_y, small_box_width, small_box_height, stroke=1, fill=0)
    report_num = ""
    # Text inside the small box
    report_details = [
        ("Report No :", report_num),
        ("Currency  :", "OMR"),
        ("Rate          :", "IISRATE")
    ]
    c.setFont("Helvetica-Bold", 8)

    text_y = small_box_y + small_box_height - c._leading - 2
    for label, value in report_details:
        c.drawString(small_box_x + 2, text_y, label)
        c.drawString(small_box_x + 2 + c.stringWidth(label + " "), text_y, value)
        text_y -= c._leading + 8  # Add extra space between lines

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
    c.drawString(left_margin, bottom_margin - 8, generated_by_text)
    c.drawString(left_margin + generated_by_text_width, bottom_margin - 8, generated_by_value)

    # Draw "Date: <date>" in the right bottom corner
    date_text = "Date: " + datetime.now().strftime("%d %B %Y")
    date_text_width = c.stringWidth(date_text)
    c.drawRightString(right_margin, bottom_margin - 8, date_text)

    # Draw the second rectangle below the first one
    second_rect_height = 0.7 * cm
    second_rect_y = rect_y - second_rect_height  # add some space between the two rectangles

    c.setLineWidth(1.3)
    c.rect(rect_x, second_rect_y, rect_width, second_rect_height, stroke=1, fill=0)

    # Draw the text in the center of the second rectangle
    period_text = f"For the Period of {period}"
    text_width = c.stringWidth(period_text)
    text_x = rect_x + (rect_width - text_width) / 2
    text_y = second_rect_y - 3 + (second_rect_height - c._leading) / 2 + c._leading / 2
    c.setFont("Helvetica-Bold", 10)
    c.drawString(text_x, text_y, period_text)

    return second_rect_y, bottom_margin, left_margin, top_margin, rect_x, rect_width, right_margin


def create_cwh_invoice_pdf(period, final_df, sub_total_list, Grand_total_list):
    print(final_df)
    row_height = 20
    total_index = 0
    # Define the path and file name
    path = r'C:\Users\Administrator\Downloads\eiis\cwh_del_det_by_inv'
    current_time_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"CWH_DEL_DETAILS_BY_INVOICE_{period}_{current_time_str}.pdf"
    full_path = os.path.join(path, file_name)
    try:
        # Create a canvas object with landscape orientation
        c = canvas.Canvas(full_path, pagesize=landscape(letter))
        width, height = landscape(letter)

        # Call the create_header function
        second_rect_y, bottom_margin, left_margin, top_margin, rect_x, rect_width, right_margin = create_header(c, period,
                                                                                                                width,
                                                                                                                height)
        # Draw the second rectangle below the first one
        header_rect_height = 0.7 * cm
        header_rect_y = second_rect_y - header_rect_height  # add some space between the two rectangles

        c.setLineWidth(1.3)
        c.rect(rect_x, header_rect_y, rect_width, header_rect_height, stroke=1, fill=0)
        # Specify the distances from the left edge of the rectangle
        distance1 = 5.4 * cm  # Distance for the first line from the left edge of the rectangle
        distance2 = 17.2 * cm  # Distance for the second line
        distance3 = 25.4 * cm  # Distance for the third line

        # Calculate the actual X coordinates of the lines
        first_line_x = rect_x + distance1
        second_line_x = rect_x + distance2
        third_line_x = rect_x + distance3

        # Draw vertical lines
        c.line(first_line_x, header_rect_y, first_line_x, header_rect_y + header_rect_height)
        c.line(second_line_x, header_rect_y, second_line_x, header_rect_y + header_rect_height)
        c.line(third_line_x, header_rect_y, third_line_x, header_rect_y + header_rect_height)

        # Set the font and size
        font_name = "Helvetica-Bold"
        font_size = 8
        c.setFont(font_name, font_size)

        # Define text positions (centers of the boxes)
        text_y = header_rect_y + (header_rect_height / 2) - (font_size / 2)  # Adjust for vertical centering

        # Draw text inside the boxes
        # Draw text inside the boxes
        c.drawCentredString((left_margin + first_line_x) / 2, text_y, "Location Details")
        c.drawCentredString((first_line_x + second_line_x) / 2, text_y, "Cost")
        c.drawCentredString((second_line_x + third_line_x) / 2, text_y, "Issue")

        for index, (df, sub_total) in enumerate(zip(final_df, sub_total_list)):
            available_space = header_rect_y - bottom_margin
            print("available_space", available_space)

            # Calculate the number of rows that can fit within the available space
            rows_per_chunk = int(available_space / row_height)
            rows_per_chunk -= 1  # Adjust for the header if it is included in each chunk
            if available_space < 2.5 * cm:
                c.showPage()
                # Call the create_header function
                second_rect_y, bottom_margin, left_margin, top_margin, rect_x, rect_width, right_margin = create_header(c,
                                                                                                                        period,
                                                                                                                        width,
                                                                                                                        height)
                header_rect_height = 0.7 * cm
                header_rect_y = second_rect_y - header_rect_height  # add some space between the two rectangles

                c.setLineWidth(1.3)
                c.rect(rect_x, header_rect_y, rect_width, header_rect_height, stroke=1, fill=0)
                # Draw vertical lines
                c.line(first_line_x, header_rect_y, first_line_x, header_rect_y + header_rect_height)
                c.line(second_line_x, header_rect_y, second_line_x, header_rect_y + header_rect_height)
                c.line(third_line_x, header_rect_y, third_line_x, header_rect_y + header_rect_height)

                # Set the font and size
                font_name = "Helvetica-Bold"
                font_size = 8
                c.setFont(font_name, font_size)

                # Define text positions (centers of the boxes)
                text_y = header_rect_y + (header_rect_height / 2) - (font_size / 2)  # Adjust for vertical centering

                # Draw text inside the boxes
                c.drawCentredString((left_margin + first_line_x) / 2, text_y, "Location Details")
                c.drawCentredString((first_line_x + second_line_x) / 2, text_y, "Cost")
                c.drawCentredString((second_line_x + third_line_x) / 2, text_y, "Issue")

                available_space = header_rect_y - bottom_margin
                print("available_space", available_space)
                # Calculate the number of rows that can fit within the available space
                rows_per_chunk = int(available_space / row_height)
                print('rows_per_chunk', rows_per_chunk)
                rows_per_chunk -= 1  # Adjust for the header if it is included in each chunk

            first_half = df.iloc[:rows_per_chunk]
            second_half = df.iloc[rows_per_chunk:]
            # print('Len of first DF', len(first_half))
            # print('Len of second DF', len(second_half))
            table_y = header_rect_y
            df_data = first_half.values.tolist()
            df_headers = first_half.columns.tolist()

            df_table = Table([df_headers] + df_data, colWidths=[5.4 * cm, 2 * cm, 1.64 * cm])

            df_table.setStyle(TableStyle([
                # Outer boundaries
                ('BOX', (0, 0), (-1, -1), 1, colors.black),
                # Vertical lines between columns
                ('LINEBEFORE', (1, 0), (1, -1), 1, colors.black),
                ('LINEBEFORE', (2, 0), (2, -1), 1, colors.black),
                ('LINEBEFORE', (3, 0), (3, -1), 1, colors.black),
                # Background and text styles for headers
                ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 8),
                ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
                # Cell styles for data rows
                ('ALIGN', (1, 1), (-1, -1), 'CENTER'),  # Center-align all data columns except the first one
                ('ALIGN', (0, 1), (0, -1), 'LEFT'),  # Left-align the first column
                ('FONTSIZE', (0, 1), (-1, -1), 6),
            ]))

            # Calculate the height of the table
            df_table_height = df_table.wrap(0, 0)[1]

            table_width = width - 2 * left_margin  # Width of the table
            df_table.wrapOn(c, table_width, height)  # Prepare the table for drawing
            df_table.drawOn(c, left_margin, table_y - df_table_height)  # Position and draw the table
            header_rect_y = table_y - df_table_height - 0.5 * cm
            if len(second_half) == 0:
                available_space = header_rect_y - bottom_margin
                if available_space < 2.5 * cm:
                    c.showPage()
                    # Call the create_header function
                    second_rect_y, bottom_margin, left_margin, top_margin, rect_x, rect_width, right_margin = create_header(
                        c, period, width, height)
                    header_rect_height = 0.7 * cm
                    header_rect_y = second_rect_y - header_rect_height  # add some space between the two rectangles
                    c.setLineWidth(1.3)
                    c.rect(rect_x, header_rect_y, rect_width, header_rect_height, stroke=1, fill=0)
                    # Draw vertical lines
                    c.line(first_line_x, header_rect_y, first_line_x, header_rect_y + header_rect_height)
                    c.line(second_line_x, header_rect_y, second_line_x, header_rect_y + header_rect_height)
                    c.line(third_line_x, header_rect_y, third_line_x, header_rect_y + header_rect_height)

                    # Set the font and size
                    font_name = "Helvetica-Bold"
                    font_size = 8
                    c.setFont(font_name, font_size)

                    # Define text positions (centers of the boxes)
                    text_y = header_rect_y + (header_rect_height / 2) - (font_size / 2)  # Adjust for vertical centering

                    # Draw text inside the boxes
                    c.drawCentredString((left_margin + first_line_x) / 2, text_y, "Location Details")
                    c.drawCentredString((first_line_x + second_line_x) / 2, text_y, "Cost")
                    c.drawCentredString((second_line_x + third_line_x) / 2, text_y, "Issue")

                sub_total_sub_list = sub_total_list[total_index]
                header_rect_y = header_rect_y - 0.3 * cm
                c.setFont("Helvetica-Bold", 6.0)  # Adjust the font size here
                c.drawCentredString(6 * cm, header_rect_y, "Sub-Total : ")
                # Set the desired font and size

                # Manually specify the positions for each element
                element_positions = [
                    (9.7 * cm, header_rect_y),
                    (11.6 * cm, header_rect_y),
                    (13.1 * cm, header_rect_y),
                    (14.6 * cm, header_rect_y),
                    (16.3 * cm, header_rect_y),
                    (17.9 * cm, header_rect_y),
                    (20.1 * cm, header_rect_y),
                    (21.7 * cm, header_rect_y),
                    (23.1 * cm, header_rect_y),
                    (24.5 * cm, header_rect_y),
                    (26.2 * cm, header_rect_y),

                ]

                # Print the 12 elements in a single row below the table
                for i, (x, y) in enumerate(element_positions):
                    c.drawString(x, y, str(sub_total[i]))
                header_rect_y -= 0.4 * cm
            else:
                index += 1
                final_df.insert(index, second_half)
                sub_total_list.insert(index, sub_total)
                c.showPage()
                # Call the create_header function
                second_rect_y, bottom_margin, left_margin, top_margin, rect_x, rect_width, right_margin = create_header(c,
                                                                                                                        period,
                                                                                                                        width,
                                                                                                                        height)
                header_rect_height = 0.7 * cm
                header_rect_y = second_rect_y - header_rect_height  # add some space between the two rectangles

                c.setLineWidth(1.3)
                c.rect(rect_x, header_rect_y, rect_width, header_rect_height, stroke=1, fill=0)
                # Draw vertical lines
                c.line(first_line_x, header_rect_y, first_line_x, header_rect_y + header_rect_height)
                c.line(second_line_x, header_rect_y, second_line_x, header_rect_y + header_rect_height)
                c.line(third_line_x, header_rect_y, third_line_x, header_rect_y + header_rect_height)

                # Set the font and size
                font_name = "Helvetica-Bold"
                font_size = 8
                c.setFont(font_name, font_size)

                # Define text positions (centers of the boxes)
                text_y = header_rect_y + (header_rect_height / 2) - (font_size / 2)  # Adjust for vertical centering

                # Draw text inside the boxes
                c.drawCentredString((left_margin + first_line_x) / 2, text_y, "Location Details")
                c.drawCentredString((first_line_x + second_line_x) / 2, text_y, "Cost")
                c.drawCentredString((second_line_x + third_line_x) / 2, text_y, "Issue")
                available_space = header_rect_y - bottom_margin

        header_rect_y -= 0.1 * cm
        # Draw the horizontal line
        c.line(left_margin, header_rect_y, right_margin, header_rect_y)
        header_rect_y -= 0.3 * cm
        c.setFont("Helvetica-Bold", 6.0)  # Adjust the font size here
        c.drawCentredString(6 * cm, header_rect_y, "Grand-Total : ")
        # Set the desired font and size
        # Manually specify the positions for each element
        element_positions = [
            (9.7 * cm, header_rect_y),
            (11.6 * cm, header_rect_y),
            (13.1 * cm, header_rect_y),
            (14.6 * cm, header_rect_y),
            (16.3 * cm, header_rect_y),
            (17.9 * cm, header_rect_y),
            (20.1 * cm, header_rect_y),
            (21.7 * cm, header_rect_y),
            (23.1 * cm, header_rect_y),
            (24.5 * cm, header_rect_y),
            (26.2 * cm, header_rect_y),

        ]

        # Print the 12 elements in a single row below the table
        for i, (x, y) in enumerate(element_positions):
            c.drawString(x, y, str(Grand_total_list[i]))
            header_rect_y -= 0.2 * cm
        # Save the PDF
        c.save()
        status = "success"
    except Exception as error:
        print('The cause of error -->', error)
        status = "failed"
    return status, file_name, full_path
