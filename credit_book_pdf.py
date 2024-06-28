from datetime import datetime
from reportlab.lib.pagesizes import letter, landscape
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm
from reportlab.platypus import Table, TableStyle
from reportlab.lib import colors
import os
import pandas as pd
from database import get_database_engine_e_eiis, get_database_connection_e_eiis


def get_sum_of_stock_gp():
    with get_database_connection_e_eiis() as conn:
        cursor = conn.cursor()
        sql_query = """ SELECT 
                            ROUND(COALESCE(SUM(s.OP_GP), 0), 3) AS Total_OP_GP,
                            COALESCE(
                                CASE 
                                    WHEN it_acc.ACCOUNT_NAME IN ('FOOD', 'CLEANING', 'DISPOSABLES') THEN it_acc.ACCOUNT_NAME 
                                    ELSE 'OTHERS' 
                                END,
                                'OTHERS'
                            ) AS Item_Category_Name
                        FROM 
                            mst_item_account AS it_acc
                        LEFT JOIN 
                            mst_item_category AS m ON it_acc.ITEM_ACCOUNT_PK = m.ACCOUNT_FK
                        LEFT JOIN 
                            stock AS s ON LEFT(s.ITEM_ID, 2) = m.ITEM_CAT_PK
                        GROUP BY 
                            CASE 
                                    WHEN it_acc.ACCOUNT_NAME IN ('FOOD', 'CLEANING', 'DISPOSABLES') THEN it_acc.ACCOUNT_NAME 
                                    ELSE 'OTHERS' 
                                END;
                    """
        cursor.execute(sql_query)
        records = cursor.fetchall()
        nested_list = [list(item) for item in records]
        cursor.close()
        print(nested_list)

        # Define the order of categories
        category_order = ['FOOD', 'CLEANING', 'DISPOSABLES', 'OTHERS']

        # Sort the data based on the defined order
        sorted_data = sorted(nested_list, key=lambda x: category_order.index(x[1]))

        # Extract the values in the specified order
        sorted_values = [item[0] for item in sorted_data]

        food_opening_value = sorted_values[0]
        cleaning_opening_value = sorted_values[1]
        disposal_opening_value = sorted_values[2]
        others_opening_value = sorted_values[3]
        # Calculate the sum of the list
        total_opening_sum = round(sum(sorted_values), 3)

        return food_opening_value, cleaning_opening_value, disposal_opening_value, others_opening_value, total_opening_sum


def capitalize_first_letter(s):
    return s.capitalize()


def fetch_credit_book_data(month, year):
    table_sub_total = []
    sub_total = []
    grand_total = []
    cession_in_out_df = []
    cash_pur_df = []
    credit_pur_df = []
    try:
        engine = get_database_engine_e_eiis()
        # CESSION IN
        # SQL query to fetch data
        df_query_0 = """
                        SELECT 
                        CONCAT('Return from ', lhead.LOCATION_H_ID) AS Narration,
                        CASE 
                            WHEN mia.ACCOUNT_NAME IN ('FOOD', 'CLEANING', 'DISPOSABLES') THEN mia.ACCOUNT_NAME
                            ELSE 'OTHERS' 
                        END AS Item_Category_Name,
                        ROUND(SUM(ldet.QTY * ldet.STOCK_GP), 3) AS Total_QTY
                    FROM 
                        locrethead AS lhead
                    INNER JOIN 
                        locretdetail AS ldet 
                        ON ldet.LOC_RET_D_ID = lhead.LOC_RET_H_ID
                    INNER JOIN 
                        mst_item_category AS m
                        ON LEFT(ldet.ITEM_ID, 2) = m.ITEM_CAT_PK
                    LEFT JOIN 
                        mst_item_account AS mia
                        ON m.ACCOUNT_FK = mia.ITEM_ACCOUNT_PK
                    WHERE 
                        MONTH(lhead.PERIOD) = %s
                        AND YEAR(lhead.PERIOD) = %s
                    GROUP BY 
                        lhead.LOCATION_H_ID,
                        CASE 
                            WHEN mia.ACCOUNT_NAME IN ('FOOD', 'CLEANING', 'DISPOSABLES') THEN mia.ACCOUNT_NAME
                            ELSE 'OTHERS' 
                        END;

        """

        # Fetch data from database into DataFrame
        df = pd.read_sql_query(df_query_0, engine, params=(month, year))

        if len(df) == 0:
            print('yes')
            # Define your data as a dictionary
            data = {
                'Narration': ['Return from'],
                'FOOD': [0.000],
                'CLEANING': [0.000],
                'DISPOSAL': [0.000],
                'OTHERS': [0.000]
            }

            # Create the DataFrame
            df = pd.DataFrame(data)

            # Create df2 with default values of 0
            df2 = pd.DataFrame(0.0, index=df.index,
                               columns=[f"{col}" if col != 'Narration' else col for col in df.columns])

            # Create df3 with default values of 0
            df3 = pd.DataFrame(0.0, index=df.index,
                               columns=[f"{col}" if col != 'Narration' else col for col in df.columns])

            # Concatenate df2 and df3 with new_df
            cession_in_df = pd.concat(
                [df[['Narration']], df2.drop(columns=['Narration']), df.drop(columns=['Narration']),
                 df3.drop(columns=['Narration'])], axis=1)

            # Insert S.No as the first column
            cession_in_df.insert(0, 'S.No', range(1, len(cession_in_df) + 1))

        else:
            # Define relevant item categories
            relevant_categories = ['FOOD', 'CLEANING', 'DISPOSABLES', 'OTHERS']

            # Ensure all relevant categories are in the DataFrame, even if initially not present
            dfs_to_concat = []
            existing_narrations = df['Narration'].unique()  # Fetch unique narrations from existing df

            for cat in relevant_categories:
                if cat not in df['Item_Category_Name'].unique():
                    for narration in existing_narrations:
                        new_row = pd.DataFrame(
                            {'Narration': [narration], 'Item_Category_Name': [cat], 'Total_QTY': [0.0]})
                        dfs_to_concat.append(new_row)

            # Concatenate the original DataFrame with the additional rows using pd.concat
            if dfs_to_concat:
                df = pd.concat([df] + dfs_to_concat, ignore_index=True)

            # Pivot the table with specified columns
            new_df = df.pivot_table(index='Narration', columns='Item_Category_Name', values='Total_QTY', aggfunc='sum',
                                    fill_value=0.0)

            # Reorder columns to match specified order
            new_df = new_df.reindex(columns=relevant_categories,
                                    fill_value=0.0)  # Fill missing columns with 0 if they don't exist

            # Reset index to default (remove the current index)
            new_df = new_df.reset_index()

            # Create df2 with default values of 0
            df2 = pd.DataFrame(0.0, index=new_df.index,
                               columns=[f"{col}" if col != 'Narration' else col for col in new_df.columns])

            # Create df3 with default values of 0
            df3 = pd.DataFrame(0.0, index=new_df.index,
                               columns=[f"{col}" if col != 'Narration' else col for col in new_df.columns])

            # Concatenate df2 and df3 with new_df
            cession_in_df = pd.concat(
                [new_df[['Narration']], df2.drop(columns=['Narration']), new_df.drop(columns=['Narration']),
                 df3.drop(columns=['Narration'])], axis=1)

            # Insert S.No as the first column
            cession_in_df.insert(0, 'S.No', range(1, len(cession_in_df) + 1))

        # Rename columns
        cession_in_df = cession_in_df.rename(columns=lambda x: capitalize_first_letter(x))

        # Calculate totals for each column (excluding the first two columns)
        cession_in_df_totals_list = cession_in_df.iloc[:, 2:].astype(float).sum().round(3).tolist()
        # Initialize an empty list to store the sums
        zero_table_sums_list = []

        # Loop through the list in steps of 4
        for i in range(0, len(cession_in_df_totals_list), 4):
            # Sum the current group of 4 elements
            group_sum = sum(cession_in_df_totals_list[i:i + 4])
            # Append the sum to the sums_list
            zero_table_sums_list.append(group_sum)

        # CESSION OUT GOING
        # SQL query to fetch data
        df_query_1 = """
                        SELECT 
                'CESSION OUT GOING' AS Narration,
                CASE 
                    WHEN mia.ACCOUNT_NAME IN ('FOOD', 'CLEANING', 'DISPOSABLES') THEN mia.ACCOUNT_NAME
                    ELSE 'OTHERS' 
                END AS Item_Category_Name,
                ROUND(SUM(cdet.QTY * cdet.STOCK_GP), 3) AS Total_QTY
            FROM 
                cwhdelhead AS chead
            INNER JOIN 
                cwhdeldetail AS cdet 
                ON cdet.CWH_DEL_ID = chead.CWH_DEL_ID
            INNER JOIN 
                mst_item_category AS m
                ON LEFT(cdet.ITEM_ID, 2) = m.ITEM_CAT_PK
            LEFT JOIN 
                mst_item_account AS mia
                ON m.ACCOUNT_FK = mia.ITEM_ACCOUNT_PK
            
            WHERE 
                MONTH(chead.PERIOD) = %s
                AND YEAR(chead.PERIOD) = %s
                
            GROUP BY 
                CASE 
                    WHEN mia.ACCOUNT_NAME IN ('FOOD', 'CLEANING', 'DISPOSABLES') THEN mia.ACCOUNT_NAME
                    ELSE 'OTHERS' 
                END;
        """

        # Fetch data from database into DataFrame
        df = pd.read_sql_query(df_query_1, engine, params=(month, year))

        if len(df) == 0:
            print('yes')
            # Define your data as a dictionary
            data = {
                'Narration': ['CESSION OUT GOING'],
                'FOOD': [0.000],
                'CLEANING': [0.000],
                'DISPOSAL': [0.000],
                'OTHERS': [0.000]
            }

            # Create the DataFrame
            df = pd.DataFrame(data)

            # Create df2 with default values of 0
            df2 = pd.DataFrame(0.0, index=df.index,
                               columns=[f"{col}" if col != 'Narration' else col for col in df.columns])

            # Create df3 with default values of 0
            df3 = pd.DataFrame(0.0, index=df.index,
                               columns=[f"{col}" if col != 'Narration' else col for col in df.columns])

            # Concatenate df2 and df3 with new_df
            cession_out_df = pd.concat(
                [df[['Narration']], df2.drop(columns=['Narration']), df3.drop(columns=['Narration']),
                 df.drop(columns=['Narration'])], axis=1)

            # Insert S.No, as the first column
            cession_out_df.insert(0, 'S.No', range(1, len(cession_out_df) + 1))

        else:
            # Define relevant item categories
            relevant_categories = ['FOOD', 'CLEANING', 'DISPOSABLES', 'OTHERS']

            # Ensure all relevant categories are in the DataFrame, even if initially not present
            dfs_to_concat = []
            existing_narrations = df['Narration'].unique()  # Fetch unique narrations from existing df

            for cat in relevant_categories:
                if cat not in df['Item_Category_Name'].unique():
                    for narration in existing_narrations:
                        new_row = pd.DataFrame(
                            {'Narration': [narration], 'Item_Category_Name': [cat], 'Total_QTY': [0]})
                        dfs_to_concat.append(new_row)

            # Concatenate the original DataFrame with the additional rows using pd.concat
            if dfs_to_concat:
                df = pd.concat([df] + dfs_to_concat, ignore_index=True)

            # Pivot the table with specified columns
            new_df = df.pivot_table(index='Narration', columns='Item_Category_Name', values='Total_QTY', aggfunc='sum',
                                    fill_value=0.0)

            # Reorder columns to match specified order
            new_df = new_df.reindex(columns=relevant_categories,
                                    fill_value=0.0)  # Fill missing columns with 0 if they don't exist

            # Reset index to default (remove the current index)
            new_df = new_df.reset_index()

            # Create df2 with default values of 0
            df2 = pd.DataFrame(0.0, index=new_df.index,
                               columns=[f"{col}" if col != 'Narration' else col for col in new_df.columns])

            # Create df3 with default values of 0
            df3 = pd.DataFrame(0.0, index=new_df.index,
                               columns=[f"{col}" if col != 'Narration' else col for col in new_df.columns])

            # Concatenate df2 and df3 with new_df
            cession_out_df = pd.concat(
                [new_df[['Narration']], df2.drop(columns=['Narration']), df3.drop(columns=['Narration']),
                 new_df.drop(columns=['Narration'])], axis=1)

            # Insert S.No, as the first column
            cession_out_df.insert(0, 'S.No', range(1, len(cession_out_df) + 1))

        # Rename columns
        cession_out_df = cession_out_df.rename(columns=lambda x: capitalize_first_letter(x))

        # Calculate totals for each column (excluding the first two columns)
        cession_out_df_totals_list = cession_out_df.iloc[:, 2:].astype(float).sum().round(3).tolist()
        # Initialize an empty list to store the sums
        first_table_sums_list = []

        # Loop through the list in steps of 4
        for i in range(0, len(cession_out_df_totals_list), 4):
            # Sum the current group of 4 elements
            group_sum = sum(cession_out_df_totals_list[i:i + 4])
            # Append the sum to the sums_list
            first_table_sums_list.append(group_sum)
        temp_table_sub_total_list = []
        temp_sub_total_list = []
        for i in range(len(cession_in_df_totals_list)):
            temp_table_sub_total_list.append(cession_in_df_totals_list[i] + cession_out_df_totals_list[i])
        table_sub_total.append(temp_table_sub_total_list)
        # print(table_sub_total)

        for i in range(len(zero_table_sums_list)):
            temp_sub_total_list.append(zero_table_sums_list[i] + first_table_sums_list[i])
        sub_total.append(temp_sub_total_list)
        # Concatenate along columns
        cession_in_out_df = pd.concat([cession_in_df, cession_out_df], axis=0, ignore_index=True)
        # Get the value of the second last element in the 'S.no' column
        second_last_value = cession_in_out_df['S.no'].iloc[-2]

        # Add 1 to this value
        new_value = second_last_value + 1

        # Update the last element with this new value
        cession_in_out_df.at[cession_in_out_df.index[-1], 'S.no'] = new_value

        # CASH PURCHASE QUERY
        df_query_2 = """
                        SELECT 
                    'S00000 CASH PURCHASE' AS Narration,
                    CASE 
                        WHEN mia.ACCOUNT_NAME IN ('FOOD', 'CLEANING', 'DISPOSABLES') THEN mia.ACCOUNT_NAME
                        ELSE 'OTHERS' 
                    END AS Item_Category_Name,
                    ROUND(SUM(sdet.QTY * sdet.STOCK_GP), 3) AS Total_QTY
                FROM 
                    suppdelhead AS shead
                INNER JOIN 
                    suppliers AS sup 
                    ON sup.Supplier_ID = shead.SUPPLIER_ID
                INNER JOIN 
                    suppdeldetail AS sdet 
                    ON sdet.GRN_ID = shead.GRN_ID
                INNER JOIN 
                    mst_item_category AS m
                    ON LEFT(sdet.ITEM_ID, 2) = m.ITEM_CAT_PK
                LEFT JOIN 
                    mst_item_account AS mia
                    ON m.ACCOUNT_FK = mia.ITEM_ACCOUNT_PK
                WHERE 
                    MONTH(shead.PERIOD) = %s
                    AND YEAR(shead.PERIOD) = %s
                    AND shead.SUPPLIER_ID = 'S00000'
                GROUP BY 
                    CASE 
                        WHEN mia.ACCOUNT_NAME IN ('FOOD', 'CLEANING', 'DISPOSABLES') THEN mia.ACCOUNT_NAME
                        ELSE 'OTHERS' 
                    END;
                    """

        # Fetch data from database into DataFrame
        df = pd.read_sql_query(df_query_2, engine, params=(month, year))

        if len(df) == 0:
            print('yes')
            # Define your data as a dictionary
            data = {
                'Narration': ['S00000 CASH PURCHASE'],
                'FOOD': [0.000],
                'CLEANING': [0.000],
                'DISPOSAL': [0.000],
                'OTHERS': [0.000]
            }

            # Create the DataFrame
            df = pd.DataFrame(data)
            # Create df2 with default values of 0
            df2 = pd.DataFrame(0.0, index=df.index,
                               columns=[f"{col}" if col != 'Narration' else col for col in df.columns])

            # Create df3 with default values of 0
            df3 = pd.DataFrame(0.0, index=df.index,
                               columns=[f"{col}" if col != 'Narration' else col for col in df.columns])

            # Concatenate df2 and df3 with new_df
            cash_pur_df = pd.concat([df[['Narration']], df.drop(columns=['Narration']), df2.drop(columns=['Narration']),
                                     df3.drop(columns=['Narration'])], axis=1)

            # Insert S.No, as the first column
            cash_pur_df.insert(0, 'S.No', range(1, len(cash_pur_df) + 1))

        else:
            # Define relevant item categories
            relevant_categories = ['FOOD', 'CLEANING', 'DISPOSABLES', 'OTHERS']

            # Ensure all relevant categories are in the DataFrame, even if initially not present
            dfs_to_concat = []
            existing_narrations = df['Narration'].unique()  # Fetch unique narrations from existing df

            for cat in relevant_categories:
                if cat not in df['Item_Category_Name'].unique():
                    for narration in existing_narrations:
                        new_row = pd.DataFrame(
                            {'Narration': [narration], 'Item_Category_Name': [cat], 'Total_QTY': [0.0]})
                        dfs_to_concat.append(new_row)

            # Concatenate the original DataFrame with the additional rows using pd.concat
            if dfs_to_concat:
                df = pd.concat([df] + dfs_to_concat, ignore_index=True)

            # Pivot the table with specified columns
            new_df = df.pivot_table(index='Narration', columns='Item_Category_Name', values='Total_QTY', aggfunc='sum',
                                    fill_value=0.0)

            # Reorder columns to match specified order
            new_df = new_df.reindex(columns=relevant_categories,
                                    fill_value=0.0)  # Fill missing columns with 0 if they don't exist

            # Reset index to default (remove the current index)
            new_df = new_df.reset_index()

            # Create df2 with default values of 0
            df2 = pd.DataFrame(0.0, index=new_df.index,
                               columns=[f"{col}" if col != 'Narration' else col for col in new_df.columns])

            # Create df3 with default values of 0
            df3 = pd.DataFrame(0.0, index=new_df.index,
                               columns=[f"{col}" if col != 'Narration' else col for col in new_df.columns])

            # Concatenate df2 and df3 with new_df
            cash_pur_df = pd.concat(
                [new_df[['Narration']], new_df.drop(columns=['Narration']), df2.drop(columns=['Narration']),
                 df3.drop(columns=['Narration'])], axis=1)

            # Insert S.No, as the first column
            cash_pur_df.insert(0, 'S.No', range(1, len(cash_pur_df) + 1))

        # Rename columns
        cash_pur_df = cash_pur_df.rename(columns=lambda x: capitalize_first_letter(x))
        # Calculate totals for each column (excluding the first two columns)
        cash_pur_df_totals_list = cash_pur_df.iloc[:, 2:].astype(float).sum().round(3).tolist()
        # Initialize an empty list to store the sums
        second_table_sums_list = []

        # Loop through the list in steps of 4
        for i in range(0, len(cash_pur_df_totals_list), 4):
            # Sum the current group of 4 elements
            group_sum = sum(cash_pur_df_totals_list[i:i + 4])
            # Append the sum to the sums_list
            second_table_sums_list.append(group_sum)
        table_sub_total.append(cash_pur_df_totals_list)
        # print(table_sub_total)
        sub_total.append(second_table_sums_list)

        # CREDIT PURCHASE
        df_query_3 = """
                        SELECT 
                            CONCAT(shead.SUPPLIER_ID, ' ', sup.Supplier_Name) AS Narration,
                            CASE 
                    WHEN mia.ACCOUNT_NAME IN ('FOOD', 'CLEANING', 'DISPOSABLES') THEN mia.ACCOUNT_NAME
                    ELSE 'OTHERS' 
                END AS Item_Category_Name,
                            ROUND(SUM(sdet.QTY * sdet.STOCK_GP), 3) AS Total_QTY
                        FROM 
                            suppdelhead AS shead
                        INNER JOIN 
                            suppliers AS sup 
                            ON sup.Supplier_ID = shead.SUPPLIER_ID
                        INNER JOIN 
                            suppdeldetail AS sdet 
                            ON sdet.GRN_ID = shead.GRN_ID
                        INNER JOIN 
                            mst_item_category AS m
                            ON LEFT(sdet.ITEM_ID, 2) = m.ITEM_CAT_PK
                        LEFT JOIN 
                mst_item_account AS mia
                ON m.ACCOUNT_FK = mia.ITEM_ACCOUNT_PK
                        WHERE 
                            MONTH(shead.PERIOD) = %s
                            AND YEAR(shead.PERIOD) = %s
                            AND shead.SUPPLIER_ID != 'S00000'
                        GROUP BY 
                            shead.SUPPLIER_ID, 
                            sup.Supplier_Name,
                            CASE 
                    WHEN mia.ACCOUNT_NAME IN ('FOOD', 'CLEANING', 'DISPOSABLES') THEN mia.ACCOUNT_NAME
                    ELSE 'OTHERS' 
                END;
                    """

        # Fetch data from database into DataFrame
        df = pd.read_sql_query(df_query_3, engine, params=(month, year))

        # Define relevant item categories
        relevant_categories = ['FOOD', 'CLEANING', 'DISPOSABLES', 'OTHERS']

        # Ensure all relevant categories are in the DataFrame, even if initially not present
        dfs_to_concat = []
        existing_narrations = df['Narration'].unique()  # Fetch unique narrations from existing df

        for cat in relevant_categories:
            if cat not in df['Item_Category_Name'].unique():
                for narration in existing_narrations:
                    new_row = pd.DataFrame({'Narration': [narration], 'Item_Category_Name': [cat], 'Total_QTY': [0.0]})
                    dfs_to_concat.append(new_row)

        # Concatenate the original DataFrame with the additional rows using pd.concat
        if dfs_to_concat:
            df = pd.concat([df] + dfs_to_concat, ignore_index=True)

        # Pivot the table with specified columns
        new_df = df.pivot_table(index='Narration', columns='Item_Category_Name', values='Total_QTY', aggfunc='sum',
                                fill_value=0.0)

        # Reorder columns to match specified order
        new_df = new_df.reindex(columns=relevant_categories,
                                fill_value=0.0)  # Fill missing columns with 0 if they don't exist

        # Reset index to default (remove the current index)
        new_df = new_df.reset_index()

        # Create df2 with default values of 0
        df2 = pd.DataFrame(0.0, index=new_df.index,
                           columns=[f"{col}" if col != 'Narration' else col for col in new_df.columns])

        # Create df3 with default values of 0
        df3 = pd.DataFrame(0.0, index=new_df.index,
                           columns=[f"{col}" if col != 'Narration' else col for col in new_df.columns])

        # Concatenate df2 and df3 with new_df
        credit_pur_df = pd.concat(
            [new_df[['Narration']], new_df.drop(columns=['Narration']), df2.drop(columns=['Narration']),
             df3.drop(columns=['Narration'])], axis=1)

        # Insert S.No, as the first column
        credit_pur_df.insert(0, 'S.No', range(1, len(credit_pur_df) + 1))

        # Rename columns
        credit_pur_df = credit_pur_df.rename(columns=lambda x: capitalize_first_letter(x))
        # Calculate totals for each column (excluding the first two columns)
        credit_pur_df_totals_list = credit_pur_df.iloc[:, 2:].astype(float).sum().round(3).tolist()
        # Initialize an empty list to store the sums
        third_table_sums_list = []

        # Loop through the list in steps of 4
        for i in range(0, len(credit_pur_df_totals_list), 4):
            # Sum the current group of 4 elements
            group_sum = sum(credit_pur_df_totals_list[i:i + 4])
            # Append the sum to the sums_list
            third_table_sums_list.append(group_sum)
        # Display the resulting DataFrame
        table_sub_total.append(credit_pur_df_totals_list)
        sub_total.append(third_table_sums_list)
        print(table_sub_total)
        print(sub_total)
        # Initialize the grand_total list with zeros

        # Extract and sum the first elements
        first_elements = [sublist[0] for sublist in sub_total]
        result_1 = round(sum(first_elements), 3)

        # Extract and sum the second elements
        second_elements = [sublist[1] for sublist in sub_total]
        result_2 = round(sum(second_elements), 3)

        # Extract and sum the third elements
        third_elements = [sublist[2] for sublist in sub_total]
        result_3 = round(sum(third_elements), 3)
        # Create the grand_total list
        grand_total = [result_1, result_2, result_3]
        print(grand_total)  # Output: [295.0, 0.0, 20.0]
        status = "success"
    except Exception as error:
        status = "failed"
        print('The cause of error -->', error)

    return status, cession_in_out_df, cash_pur_df, credit_pur_df, table_sub_total, sub_total, grand_total


def create_header(c, period, width, height):
    left_margin = 18
    right_margin = width - 18
    top_margin = height - 18
    bottom_margin = 18

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

    # Draw the image inside the left box of the rectangle
    image_path = 'C:\\Users\\Administrator\\Downloads\\eiis\\sodexo.jpg'
    image_width = (vertical_line_x - rect_x) * 0.8
    image_height = rect_height * 0.8
    image_x = rect_x + (vertical_line_x - rect_x - image_width) / 2
    image_y = rect_y + (rect_height - image_height) / 2
    c.drawImage(image_path, image_x, image_y, width=image_width, height=image_height)

    third_element = "Credit Book"
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
    c.drawString(left_margin, bottom_margin - 15, generated_by_text)
    c.drawString(left_margin + generated_by_text_width, bottom_margin - 15, generated_by_value)

    # Draw "Date: <date>" in the right bottom corner
    date_text = "Date: " + datetime.now().strftime("%d %B %Y")
    date_text_width = c.stringWidth(date_text)
    c.drawRightString(right_margin, bottom_margin - 15, date_text)

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


def create_credit_book_pdf(period, cession_in_out_df, cash_pur_df, credit_pur_df, table_sub_total, sub_total,
                           grand_total):

    table_list = [cession_in_out_df, cash_pur_df, credit_pur_df]
    print(table_list)
    row_height = 20
    # Define the path and file name
    path = r'C:\Users\Administrator\Downloads\eiis\credit_book'
    current_time_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"CREDIT_BOOK_{period}_{current_time_str}.pdf"
    full_path = os.path.join(path, file_name)
    try:
        # Create a canvas object with landscape orientation
        c = canvas.Canvas(full_path, pagesize=landscape(letter))
        width, height = landscape(letter)

        # Call the create_header function
        second_rect_y, bottom_margin, left_margin, top_margin, rect_x, rect_width, right_margin = create_header(c,
                                                                                                                period,
                                                                                                                width,
                                                                                                                height)
        # Draw the second rectangle below the first one
        header_rect_height = 0.7 * cm
        header_rect_y = second_rect_y - header_rect_height  # add some space between the two rectangles

        c.setLineWidth(1.3)
        c.rect(rect_x, header_rect_y, rect_width, header_rect_height, stroke=1, fill=0)
        # Specify the distances from the left edge of the rectangle
        distance1 = 7.0 * cm  # Distance for the first line from the left edge of the rectangle
        distance2 = 13.56 * cm  # Distance for the second line
        distance3 = 20.13 * cm  # Distance for the third line

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
        c.drawCentredString((first_line_x + second_line_x) / 2, text_y, "Credit Purchase")
        c.drawCentredString((second_line_x + third_line_x) / 2, text_y, "Cession In")
        c.drawCentredString((third_line_x + (third_line_x + (third_line_x - second_line_x))) / 2, text_y, "Cession Out")

        for index, (df, table_total, sub_total_sub_list) in enumerate(zip(table_list, table_sub_total, sub_total)):
            available_space = header_rect_y - bottom_margin
            print("available_space", available_space)

            # Calculate the number of rows that can fit within the available space
            rows_per_chunk = int(available_space / row_height)
            rows_per_chunk -= 1  # Adjust for the header if it is included in each chunk
            if available_space < 3 * cm:
                c.showPage()
                # Call the create_header function
                second_rect_y, bottom_margin, left_margin, top_margin, rect_x, rect_width, right_margin = create_header(
                    c,
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
                c.drawCentredString((first_line_x + second_line_x) / 2, text_y, "Credit Purchase")
                c.drawCentredString((second_line_x + third_line_x) / 2, text_y, "Cession In")
                c.drawCentredString((third_line_x + (third_line_x + (third_line_x - second_line_x))) / 2, text_y,
                                    "Cession Out")

                available_space = header_rect_y - bottom_margin
                print("available_space", available_space)
                # Calculate the number of rows that can fit within the available space
                rows_per_chunk = int(available_space / row_height)
                rows_per_chunk -= 1  # Adjust for the header if it is included in each chunk

            first_half = df.iloc[:rows_per_chunk]
            second_half = df.iloc[rows_per_chunk:]
            # print('Len of first DF', len(first_half))
            # print('Len of second DF', len(second_half))
            table_y = header_rect_y
            df_data = first_half.values.tolist()
            df_headers = first_half.columns.tolist()

            df_table = Table([df_headers] + df_data, colWidths=[1.4 * cm, 5.6 * cm, 1.64 * cm])

            df_table.setStyle(TableStyle([('BACKGROUND', (1, 1), (-1, 1), colors.white),
                                          ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                                          ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                                          ('BOTTOMPADDING', (0, 0), (-1, 0), 0.2),
                                          ('FONTSIZE', (0, 0), (-1, 0), 6),
                                          ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                                          ('ALIGN', (1, 1), (1, -1), 'LEFT'),
                                          ('ALIGN', (2, 1), (-1, -1), 'RIGHT'),
                                          ('GRID', (0, 0), (-1, -1), 1, colors.black),
                                          ('FONTSIZE', (0, 1), (-1, -1), 5.5)]))

            # Calculate the height of the table
            df_table_height = df_table.wrap(0, 0)[1]

            table_width = width - 2 * left_margin  # Width of the table
            df_table.wrapOn(c, table_width, height)  # Prepare the table for drawing
            df_table.drawOn(c, left_margin, table_y - df_table_height)  # Position and draw the table
            header_rect_y = table_y - df_table_height - 0.5 * cm
            if len(second_half) == 0:
                available_space = header_rect_y - bottom_margin
                if available_space < 1.5 * cm:
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
                    c.drawCentredString((first_line_x + second_line_x) / 2, text_y, "Credit Purchase")
                    c.drawCentredString((second_line_x + third_line_x) / 2, text_y, "Cession In")
                    c.drawCentredString((third_line_x + (third_line_x + (third_line_x - second_line_x))) / 2, text_y,
                                        "Cession Out")
                header_rect_y = header_rect_y - 0.3 * cm
                # Set the desired font and size
                c.setFont("Helvetica-Bold", 6.5)  # Adjust the font size here
                # Manually specify the positions for each element
                element_positions = [
                    (8.0 * cm, header_rect_y),
                    (9.8 * cm, header_rect_y),
                    (11.6 * cm, header_rect_y),
                    (13.3 * cm, header_rect_y),
                    (15.0 * cm, header_rect_y),
                    (16.6 * cm, header_rect_y),
                    (18.1 * cm, header_rect_y),
                    (19.7 * cm, header_rect_y),
                    (21.5 * cm, header_rect_y),
                    (23.1 * cm, header_rect_y),
                    (24.7 * cm, header_rect_y),
                    (26.2 * cm, header_rect_y),
                ]

                # Print the 12 elements in a single row below the table
                for i, (x, y) in enumerate(element_positions):
                    c.drawString(x, y, str(table_total[i]))
                header_rect_y -= 0.6 * cm
                # Draw the static headings and dynamic values
                c.drawString(8 * cm, header_rect_y, "Sub Total Purchase : ")
                c.drawString(10.5 * cm, header_rect_y, str(sub_total_sub_list[0]))

                c.drawString(14 * cm, header_rect_y, "Sub Total Cession In : ")
                c.drawString(16.5 * cm, header_rect_y, str(sub_total_sub_list[1]))

                c.drawString(21.5 * cm, header_rect_y, "Sub Total Cession Out: ")
                c.drawString(24.5 * cm, header_rect_y, str(sub_total_sub_list[2]))
                header_rect_y = header_rect_y - 0.3 * cm
                """
                                if index != len(table_list) - 1:
                    header_rect_y -= 0.3 * cm
                    header_rect_height = 0.7 * cm
                    header_rect_y = header_rect_y - header_rect_height  # add some space between the two rectangles
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
                    c.drawCentredString((first_line_x + second_line_x) / 2, text_y, "Credit Purchase")
                    c.drawCentredString((second_line_x + third_line_x) / 2, text_y, "Cession In")
                    c.drawCentredString((third_line_x + (third_line_x + (third_line_x - second_line_x))) / 2, text_y,
                                        "Cession Out")
                """
            else:
                index += 1
                table_list.insert(index, second_half)
                table_sub_total.insert(index, table_total)
                sub_total.insert(index, sub_total_sub_list)

                c.showPage()
                # Call the create_header function
                second_rect_y, bottom_margin, left_margin, top_margin, rect_x, rect_width, right_margin = create_header(
                    c,
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
                c.drawCentredString((first_line_x + second_line_x) / 2, text_y, "Credit Purchase")
                c.drawCentredString((second_line_x + third_line_x) / 2, text_y, "Cession In")
                c.drawCentredString((third_line_x + (third_line_x + (third_line_x - second_line_x))) / 2, text_y,
                                    "Cession Out")

                available_space = header_rect_y - bottom_margin

        header_rect_y -= 0.5 * cm
        # Draw the static headings and dynamic values
        c.drawString(8 * cm, header_rect_y, "Grand Total Purchase : ")
        c.drawString(10.8 * cm, header_rect_y, str(grand_total[0]))

        c.drawString(14 * cm, header_rect_y, "Grand Total Cession In : ")
        c.drawString(16.8 * cm, header_rect_y, str(grand_total[1]))

        c.drawString(21.5 * cm, header_rect_y, "Grand Total Cession Out: ")
        c.drawString(24.8 * cm, header_rect_y, str(grand_total[2]))
        c.line(left_margin, header_rect_y - 0.2 * cm, right_margin, header_rect_y - 0.2 * cm)

        available_space = (header_rect_y - 0.2 * cm) - bottom_margin

        if available_space < 4.7 * cm:
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
            c.drawCentredString((first_line_x + second_line_x) / 2, text_y, "Credit Purchase")
            c.drawCentredString((second_line_x + third_line_x) / 2, text_y, "Cession In")
            c.drawCentredString((third_line_x + (third_line_x + (third_line_x - second_line_x))) / 2, text_y,
                                "Cession Out")

            available_space = header_rect_y - bottom_margin

        header_rect_y -= 1.0 * cm
        # Draw the rectangle on the left side below the elements
        rect_x = 2 * cm
        rect_y = header_rect_y - 4.6 * cm  # Adjust y-coordinate to leave space for the rectangle
        rect_width = 11.5 * cm
        rect_height = 4.6 * cm

        c.rect(rect_x, rect_y, rect_width, rect_height)
        # Set the font for the text inside the rectangle
        c.setFont("Helvetica-Bold", 10)  # Adjust the font size here

        # Initialize the vertical offset for the internal texts
        inner_text_offset = 0.5 * cm

        # Add "Opening Balance:" text on the left side inside the rectangle
        c.drawString(rect_x + 0.2 * cm, rect_y + rect_height - inner_text_offset, "Opening Balance:")
        c.drawString(rect_x + rect_width - 4.5 * cm, rect_y + rect_height - inner_text_offset, "Closing Balance:")

        # Adjust inner_text_offset for additional entries
        inner_text_offset += 0.8 * cm

        # calling get_sum_of_stock_gp method top get opening balance
        food_opening_value, cleaning_opening_value, disposal_opening_value, others_opening_value, total_opening_sum = get_sum_of_stock_gp()
        food_closing_balance = round(
            table_sub_total[0][4] - table_sub_total[0][8] + table_sub_total[1][0] + table_sub_total[2][0] + food_opening_value, 3)
        cleaning_closing_balance = round(
            table_sub_total[0][5] - table_sub_total[0][9] + table_sub_total[1][1] + table_sub_total[2][1] + cleaning_opening_value, 3)
        disposal_closing_balance = round(
            table_sub_total[0][6] - table_sub_total[0][10] + table_sub_total[1][2] + table_sub_total[2][2] + disposal_opening_value, 3)
        other_closing_balance = round(
            table_sub_total[0][7] - table_sub_total[0][11] + table_sub_total[1][3] + table_sub_total[2][3] + others_opening_value, 3)

        closing_balance_total = round(food_closing_balance + cleaning_closing_balance + disposal_closing_balance + other_closing_balance, 3)
        # List of categories to add
        categories = ["Food:", "Cleaning:", "Disposal:", "Others:", "Total:"]

        # Dummy values for demonstration purposes (replace these with actual data)
        opening_values = [food_opening_value, cleaning_opening_value, disposal_opening_value, others_opening_value, total_opening_sum]  # Example values for Opening Balance
        closing_values = [food_closing_balance, cleaning_closing_balance, disposal_closing_balance, other_closing_balance, closing_balance_total]  # Example values for Closing Balance
        c.setFont("Helvetica", 8)  # Adjust the font size here
        # Add category texts below "Opening Balance" and "Closing Balance"
        for category, open_val, close_val in zip(categories, opening_values, closing_values):
            # Opening Balance
            c.drawString(rect_x + 0.2 * cm, rect_y + rect_height - inner_text_offset, category)
            c.drawString(rect_x + 1.5 * cm, rect_y + rect_height - inner_text_offset, str(open_val))
            # Closing Balance
            c.drawString(rect_x + rect_width - 4.5 * cm, rect_y + rect_height - inner_text_offset, category)
            c.drawString(rect_x + rect_width - 2.8 * cm, rect_y + rect_height - inner_text_offset, str(close_val))
            inner_text_offset += 0.8 * cm  # Move down for the next category

        # Draw first horizontal line to the right of the rectangle
        line_x1 = rect_x + rect_width + 0.5 * cm
        line_x2 = rect_x + rect_width + 4 * cm  # Adjust the length of the first line as needed
        line_y = rect_y + rect_height - 3.5 * cm  # Adjust y-coordinate for the line
        c.line(line_x1, line_y, line_x2, line_y)

        # Draw second horizontal line slightly to the right of the first line
        line_x3 = line_x2 + 5 * cm  # Adjust the x-coordinate for the second line
        c.line(line_x2 + 1.5 * cm, line_y, line_x3, line_y)

        # Add text below each line
        c.setFont("Helvetica", 10)
        text_y = line_y - 0.5 * cm  # Adjust y-coordinate for the text
        c.drawString(line_x1 + 0.7 * cm, text_y, "Purchase Officer")
        c.drawString(line_x3 - 3.3 * cm, text_y, "Purchase Manager")

        # Save the PDF
        c.save()
        print(f"PDF saved as {file_name}")
        status = "success"
    except Exception as error:
        print('The cause of error -->', error)
        status = "failed"
    return status, file_name, full_path
