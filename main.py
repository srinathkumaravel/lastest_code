from flask import Flask, make_response, request, jsonify, send_file
from po_number_pdf_generation import execute_query, create_pdf
from po_date_pdf_generation import execute_date_query, create_merged_pdf
from location_request_by_req_no import location_ReqNo, req_no_excel_generation, fetch_location_ReqNo
from location_request_by_period import location_period, create_merge_excel, fetch_location_period_new_selected
from quotation_request_pdf import get_req_data, create_req_pdf
from consolidation_location_request_by_ID import execute_consolidation_query, create_consolidation_pdf
from price_comparison_by_period_and_Con_ID import get_price_comparison_data, draw_price_comparison_pdf
from suppliy_price_comparison import fetch_data, create_pdf_sup
from selected_supplier_by_con_id import fetch_data_for_selected_sup, create_selected_sup_pdf
from po_date_merged_PDF import execute_merged_date_query, create_po_merge_pdf
from supplier_delivery_details_by_delivery_PDF import fetch_del_details, create_supplier_delivery_details_pdf
from cwhdeliverypdf import fetch_cwh_details, create_cwh_pdf, fetch_direct_delivery_details, merge_pdfs
from Theoritical_Stock_Excel_REport import fetch_theoritical_data, generate_theoritical_excel_report
from cwh_delivery_details_by_location import create_cwh_del_details_by_loc_pdf, fetch_data_for_cwh_del_by_loc
from item_full_transaction_pdf import create_item_full_trans_pdf, fetch_datas_for_item_full_trans
from cwh_savings_pdf import create_cwh_savings_pdf, fetch_cwh_saving_data
from cwh_savings_by_location_pdf import create_cwh_sav_by_loc_pdf, fetch_cwh_sav_by_loc
from eom_inventory_pdf import fetch_data_for_eom_inv, create_eom_inv__pdf
from quotation_request_consolidation_pdf import fetch_con_quotation_req_data, create_con_quotation_req_pdf
from supplier_invoice_details_by_invoice import fetch_supplier_invoice_data, create_supplier_invoice_details_pdf, \
    fetch_supplier_invoice_data_by_supplier_id, fetch_supplier_invoice_data_by_date_and_supplier_id
from credit_book_pdf import fetch_credit_book_data, create_credit_book_pdf
from cwh_delivery_details_by_invoice import fetch_cwh_invoice_details, create_cwh_invoice_pdf, \
    fetch_cwh_invoice_details_by_date_and_location
from CWHDeliveryNoteInvoiceForLocation import fetch_cwh_details_by_loc, fetch_direct_delivery_details_for_loc, \
    create_cwh_pdf_for_loc, merge_pdfs
from cwh_delivery_details_by_ind_location import fetch_cwh_details_by_ind_loc, \
    fetch_direct_delivery_details_for_ind_loc, create_cwh_pdf_for_ind_loc, fetch_cwh_details_old, \
    fetch_direct_delivery_details_old
from Savings_By_Location_By_Item import fetch_data_for_sav_by_loc_by_item, create_save_by_loc_by_item_pdf, \
    fetch_data_for_sav_by_loc_by_item_by_ind_location_id, fetch_data_for_sav_by_loc_by_item_by_ind_location_id_and_date
from cwh_delivery_details_by_location_date_wise import fetch_direct_delivery_details_date_wise_old, \
    fetch_cwh_details_by_loc_date_old, fetch_cwh_details_by_loc_date_unit_price, \
    fetch_direct_delivery_details_for_ind_loc_date_wise, create_cwh_pdf_for_ind_loc_by_date
from purchase_price_analysis import fetch_data_for_purchase_price_analysis, create_purchase_price_analysis_pdf
from cwh_delivery_details_by_invoice_cwh_direct_delivery_from_to_date import fetch_cwh_details_from_to_date, \
    fetch_cwh_details_unit_from_to_date, fetch_direct_delivery_details_unit_from_to_date, \
    fetch_direct_delivery_details_from_to_date, create_cwh_dd_pdf_date_wise, merge_pdfs_date_wise
from purchase_price_analysis_Excel import fetch_data_for_excel_report, create_purchase_price_analysis_excel_report
from cwh_delivery_details_by_ind_location import fetch_cwh_details_by_ind_loc, \
    fetch_direct_delivery_details_for_ind_loc, create_cwh_pdf_for_ind_loc, fetch_cwh_details_old, \
    fetch_direct_delivery_details_old
from waitress import serve
import os
from datetime import datetime
from collections import OrderedDict
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
from stored_proecdure import call_stored_procedure_1, call_stored_procedure_2, call_stored_procedure_3
from purchase_price_analysis_excel_new import create_purchase_price_excel_report, get_data_for_excel
from user_rights_excel_report import fetch_and_create_user_rights_excel

app = Flask(__name__)

IP_ADDRESS = '137.59.55.54'
PORT_NUMBER = '5001'


@app.route('/report_po_number', methods=['POST', 'GET'])
def report_po_number():
    if request.method == 'POST':
        po_num = request.form.get('po_number')
        print("PO NUMBER -->", po_num)
        if po_num:
            rows_sql_query_pohead, df, total_value, status = execute_query(po_num)
            if status == "success":
                rows_sql_query_pohead_list = list(rows_sql_query_pohead[0])

                status, file_path = create_pdf(rows_sql_query_pohead_list, total_value, df, po_num)
            else:
                return jsonify(status="failed", message="error while fetching data from SQL")
            if status == "success" and file_path is not None:
                try:
                    # Read the PDF file data
                    with open(file_path, 'rb') as pdf_file:
                        pdf_data = pdf_file.read()

                    # Create a response with the PDF data and set custom headers
                    response = make_response(pdf_data)
                    response.headers.set('Content-Type', 'application/pdf')

                    # Generate download and preview link
                    download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_pdf?file={file_path}'
                    preview_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/preview_pdf?file={file_path}'
                    return jsonify(pdf=response.data.decode('latin-1'),
                                   preview_link=preview_link,
                                   download_link=download_link,
                                   status="success",
                                   message="success")

                except FileNotFoundError:
                    return jsonify(status="failed", message="PDF not found."), 404
            else:
                return jsonify(status="failed", message="Failed to generate PDF."), 500
        else:
            return jsonify(status="failed", message="PO number not provided."), 400
    else:
        return jsonify(status="failed", message="Method not allowed."), 405


@app.route('/report_po_date', methods=['GET', 'POST'])
def report_po_date():
    if request.method == 'POST':
        start_date = request.form.get('from_date')
        end_date = request.form.get('to_date')
        try:
            print('Start date -->', start_date)
            print('End date -->', end_date)
            datetime.strptime(start_date, '%Y-%m-%d')
            datetime.strptime(end_date, '%Y-%m-%d')
            pass
        except ValueError:
            status = "failed"
            message = "Dates are not in the required format (YYYY-mm-dd)."
            print("Failed because -->", message)
            return jsonify(status=status)
        nested_po_heads, df_list, total_value_list, po_number, status, message = execute_date_query(start_date,
                                                                                                    end_date)
        if status == "success":
            for sublist in nested_po_heads:
                sublist.pop(0)
        else:
            print("Failed because -->", message)
            return jsonify(status=status)

        status, file_path = create_merged_pdf(nested_po_heads, df_list, total_value_list, po_number,
                                              start_date, end_date)

        if status == "success" and file_path is not None:
            try:
                # Read the PDF file data
                with open(file_path, 'rb') as pdf_file:
                    pdf_data = pdf_file.read()

                # Create a response with the PDF data and set custom headers
                response = make_response(pdf_data)
                response.headers.set('Content-Type', 'application/pdf')

                # Generate download and preview link
                download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_pdf?file={file_path}'
                preview_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/preview_pdf?file={file_path}'
                return jsonify(pdf=response.data.decode('latin-1'),
                               preview_link=preview_link,
                               download_link=download_link,
                               status="success")

            except FileNotFoundError:
                return jsonify(status="failed", message="PDF not found."), 404
        else:
            return jsonify(status="failed", message="Failed to generate PDF."), 500
    else:
        return jsonify(status="failed", message="Method not allowed."), 405


@app.route('/report_po_date_merged_PDF', methods=['GET', 'POST'])
def report_po_date_merged_PDF():
    if request.method == 'POST':
        start_date = request.form.get('from_date')
        end_date = request.form.get('to_date')
        try:
            print('Start date -->', start_date)
            print('End date -->', end_date)
            datetime.strptime(start_date, '%Y-%m-%d')
            datetime.strptime(end_date, '%Y-%m-%d')
            pass
        except ValueError:
            status = "failed"
            message = "Dates are not in the required format (YYYY-mm-dd)."
            print("Failed because -->", message)
            return jsonify(status=status)
        nested_po_heads, df_list, total_value_list, po_number, location_ids, date_of_del, location_name, status = execute_merged_date_query(
            start_date,
            end_date)
        if status == "success":
            for sublist in nested_po_heads:
                sublist.pop(0)
            term_of_payment_list = [sublist[-4] for sublist in nested_po_heads]
            # Extracting the second element from each sublist
            supplier_id = [sublist[0] for sublist in nested_po_heads]
            loc_id = [sublist[-3] for sublist in nested_po_heads]
            print("location_id", loc_id)

            print("sup_IDs", supplier_id)

            # Create a set of unique pairs
            unique_pairs = set(zip(supplier_id, loc_id))
            print("Unique Pairs:", unique_pairs)

            # Sort the unique pairs and create an OrderedDict
            sorted_unique_pairs = sorted(unique_pairs)
            sorted_unique_pairs_dict = OrderedDict((pair, []) for pair in sorted_unique_pairs)

            # Create dictionary with sorted pairs
            matching_indices = {pair: [] for pair in sorted_unique_pairs_dict}
            print("Initial matching_indices:", matching_indices)

            # Iterate through the lists and check for matches
            for index, (sup, loc) in enumerate(zip(supplier_id, loc_id)):
                pair = (sup, loc)
                if pair in unique_pairs:
                    matching_indices[pair].append(index)

            print("Populated matching_indices:", matching_indices)

            # Extract indices into nested list and address index list
            index_nested_list = []
            address_index_list = []

            for key, value in matching_indices.items():
                index_nested_list.append(value)
            print("Index Nested List:", index_nested_list)
            print("Length of Index Nested List:", len(index_nested_list))

            for value in index_nested_list:
                if value:  # Check if the list is not empty
                    address_index_list.append(value[0])
            print("Address Index List:", address_index_list)
        else:
            return jsonify(status=status)

        pdf_status, merged_pdf_file_name, output_path = create_po_merge_pdf(nested_po_heads, df_list, total_value_list,
                                                                            po_number, location_ids, date_of_del,
                                                                            location_name, index_nested_list,
                                                                            address_index_list)

        if status == "success" and output_path is not None:
            try:
                # Read the PDF file data
                with open(output_path, 'rb') as pdf_file:
                    pdf_data = pdf_file.read()

                # Create a response with the PDF data and set custom headers
                response = make_response(pdf_data)
                response.headers.set('Content-Type', 'application/pdf')

                # Generate download and preview link
                download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_pdf?file={output_path}'
                preview_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/preview_pdf?file={output_path}'
                return jsonify(pdf=response.data.decode('latin-1'),
                               preview_link=preview_link,
                               download_link=download_link,
                               status="success",
                               fileName=merged_pdf_file_name)

            except FileNotFoundError:
                message = "PDF not found."
                print(message)
                return jsonify(status="failed"), 404
        else:
            message = "Failed to generate PDF."
            print(message)
            return jsonify(status="failed"), 500


@app.route('/location_request_by_req_num', methods=['GET', 'POST'])
def location_ReqNoexcel():
    if request.method == 'POST':
        req_no = request.form.get('req_no')
        location_id = request.form.get('location_id')

        month_variable, location_id_code, pivot_df, status = location_ReqNo(req_no, location_id)

        if status == "success":
            excel_status, excel_location = req_no_excel_generation(month_variable, location_id_code, pivot_df)
        else:
            return jsonify(status=status)

        if excel_status == "success" and excel_location is not None:
            try:
                # Generate download link
                download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_excel?file={excel_location}'
                print('Excel file ready for download')
                return jsonify(file_path=excel_location,
                               download_link=download_link,
                               status=excel_status)

            except FileNotFoundError:
                print("Excel file not found.")
                return jsonify(status=excel_status), 404
        else:
            return jsonify(status=excel_status)


@app.route('/location_req_by_period', methods=['GET', 'POST'])
def locationReqByPeriod():
    if request.method == 'POST':
        year = request.form.get('year')
        month = request.form.get('month')

        status, pivot_df_list, location_id_list, ReqNo_code_list = location_period(year, month)
        if status == "success":
            excel_report_status, excel_file_path = create_merge_excel(location_id_list, pivot_df_list, ReqNo_code_list)
        else:
            return jsonify(status=status)

        if excel_report_status == "success" and excel_file_path is not None:
            try:
                # Generate download link
                download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_excel?file={excel_file_path}'
                print('Excel file ready for download')
                return jsonify(file_path=excel_file_path,
                               download_link=download_link,
                               status=excel_report_status)

            except FileNotFoundError:
                print("Excel file not found.")
                excel_report_status = "failed"
                return jsonify(status=excel_report_status), 404
        else:
            excel_report_status = "failed"
            return jsonify(status=excel_report_status)


@app.route('/location_req_by_period_selected', methods=['GET', 'POST'])
def location_req_by_period_selected():
    print('CALLING[location_req_by_period_selected] API ...')
    if request.method == 'POST':
        year = request.form.get('year')
        month = request.form.get('month')
        sql_query_type = str(request.form.get('type'))

        status, pivot_df_list, location_id_list, ReqNo_code_list, location_name_list = fetch_location_period_new_selected(
            year, month, sql_query_type)
        if status == "success" and len(pivot_df_list) != 0:
            excel_report_status, excel_file_path, file_name = create_merge_excel(location_id_list, pivot_df_list,
                                                                                 ReqNo_code_list, location_name_list)
        elif len(pivot_df_list) == 0:
            print('The dataframe is empty no data available to print in excel')
            status = "failed"
            return jsonify(status=status)
        else:
            return jsonify(status=status)

        if excel_report_status == "success" and excel_file_path is not None:
            # Save the merged PDF file in the specified output path
            try:
                # Generate download link
                download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_excel?file={excel_file_path}'
                print('Excel file ready for download')
                return jsonify(fileName=file_name,
                               download_link=download_link,
                               status=excel_report_status)

            except FileNotFoundError:
                print("Excel file not found.")
                return jsonify(status=excel_report_status), 404
        else:
            return jsonify(status=excel_report_status)


@app.route('/location_request_by_req_num_selected', methods=['GET', 'POST'])
def location_request_by_req_num_selected():
    if request.method == 'POST':
        req_no = request.form.get('req_no')
        location_id = request.form.get('location_id')
        sql_query_type = str(request.form.get('type'))
        month_variable, location_id_code, pivot_df, status, location_name_code = fetch_location_ReqNo(req_no,
                                                                                                      location_id,
                                                                                                      sql_query_type)

        if status == "success" and pivot_df is not None:
            status, file_path, filename = req_no_excel_generation(month_variable, location_id_code, pivot_df,
                                                                  location_name_code)

        elif pivot_df is None:
            print('No data available Empty DataFrame')
            status = "failed"
            return jsonify(status=status)
        else:
            return jsonify(status=status)

        if status == "success" and file_path is not None:
            try:
                if os.path.exists(file_path):
                    # Generate download link
                    download_link = f'http://{request.host}/download_excel?file={file_path}'
                    print('Excel file ready for download')
                    return jsonify(fileName=filename,
                                   download_link=download_link,
                                   status=status)
                else:
                    print("Excel file not found at expected location.")
                    return jsonify(status="failed", message="File not found"), 404

            except Exception as e:
                print(f"An error occurred: {e}")
                return jsonify(status="failed", message=str(e)), 500
        else:
            return jsonify(status=status)


@app.route('/quotation_pdf', methods=['GET', 'POST'])
def quotation_pdf():
    req_no = request.form.get('req_no')
    from_date = request.form.get('from_date')
    before_date = request.form.get('before_date')
    quantity = request.form.get('quantity')
    shelf_life = request.form.get('shelf_life')
    status, records, formatted_data_list = get_req_data(req_no)
    if status == "success":
        pdf_status, pdf_path = create_req_pdf(formatted_data_list, records, from_date, before_date, shelf_life,
                                              quantity)
    else:
        return jsonify(status=status)

    if pdf_status == "success":
        try:
            # Read the PDF file data
            with open(pdf_path, 'rb') as pdf_file:
                pdf_data = pdf_file.read()

            # Create a response with the PDF data and set custom headers
            response = make_response(pdf_data)
            response.headers.set('Content-Type', 'application/pdf')

            # Generate download and preview link
            download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_pdf?file={pdf_path}'
            preview_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/preview_pdf?file={pdf_path}'
            return jsonify(pdf=response.data.decode('latin-1'),
                           preview_link=preview_link,
                           download_link=download_link,
                           status="success")

        except FileNotFoundError:
            return jsonify(status="failed", message="PDF not found."), 404
    else:
        return jsonify(status="failed", message="Failed to generate PDF."), 500


@app.route('/consolidation_location_request', methods=['GET', 'POST'])
def consolidation_location_request():
    consolidation_id = str(request.form.get('consolidation_id'))
    print('The entered Consolidation ID -->', consolidation_id)
    status, table_date = execute_consolidation_query(consolidation_id)
    if status == "success":
        status, file_path, file_name = create_consolidation_pdf(consolidation_id, table_date)
    else:
        return jsonify(status=status)
    if status == "success":
        try:
            # Read the PDF file data
            with open(file_path, 'rb') as pdf_file:
                pdf_data = pdf_file.read()

            # Create a response with the PDF data and set custom headers
            response = make_response(pdf_data)
            response.headers.set('Content-Type', 'application/pdf')

            # Generate download and preview link
            download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_pdf?file={file_path}'
            preview_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/preview_pdf?file={file_path}'
            print('The consolidation location PDF GENERATED successfully and response sent ...')
            return jsonify(pdf=response.data.decode('latin-1'),
                           preview_link=preview_link,
                           download_link=download_link,
                           status="success")

        except FileNotFoundError:
            return jsonify(status="failed", message="PDF not found."), 404
    else:
        return jsonify(status="failed", message="Failed to generate PDF."), 500


@app.route('/price_comparison_by_period', methods=['POST', 'GET'])
def price_comparison():
    period = str(request.form.get('period'))
    consolidationId = str(request.form.get('consolidationId'))
    itemId = str(request.form.get('itemId'))
    print("***********************************************************************************************************")
    print('PERIOD -->', period)
    print('CONSOLIDATION ID -->', consolidationId)
    print('ITEM ID -->', itemId)

    if len(consolidationId) == 0 or len(period) == 0:
        status = "failed"
        print("failed")
        print('Consolidation ID is empty Please pass it')
        return jsonify(status=status)
    status, dfs, item_details = get_price_comparison_data(period, consolidationId, itemId)

    if status == "success":
        file_path, status, file_name = draw_price_comparison_pdf(item_details, dfs, period, consolidationId, itemId)
    else:
        return jsonify(status=status)

    if status == "success":
        print(status)
        try:
            # Read the PDF file data
            with open(file_path, 'rb') as pdf_file:
                pdf_data = pdf_file.read()

            # Create a response with the PDF data and set custom headers
            response = make_response(pdf_data)
            response.headers.set('Content-Type', 'application/pdf')

            # Generate download and preview link
            download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_pdf?file={file_path}'
            preview_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/preview_pdf?file={file_path}'
            print('The consolidation location PDF GENERATED successfully and response sent ...')
            print("***************************************************************************************************")
            return jsonify(pdf=response.data.decode('latin-1'),
                           preview_link=preview_link,
                           download_link=download_link,
                           status="success",
                           fileName=file_name)

        except FileNotFoundError:
            return jsonify(status="failed", message="PDF not found."), 404
    else:
        return jsonify(status="failed", message="Failed to generate PDF."), 500


@app.route('/supplier_price_confirmation', methods=['POST', 'GET'])
def supplier_price_conf():
    consolidation_id = request.form.get('consolidation_id')
    print('The given consolidation_id --> ', consolidation_id)
    if len(consolidation_id) == 0:
        print('No consolidation is passed received empty string')
        status = "failed"
        return jsonify(status=status)

    status, dfs_list, fax_numbers, tel_numbers, unique_supplier_names, period = fetch_data(consolidation_id)

    if status == "success":
        pdf_status, output_path, file_name = create_pdf_sup(period, dfs_list, consolidation_id,
                                                            unique_supplier_names, tel_numbers, fax_numbers)
    else:
        return jsonify(status=status)
    if pdf_status == "success":
        print(pdf_status)
        try:
            # Read the PDF file data
            with open(output_path, 'rb') as pdf_file:
                pdf_data = pdf_file.read()

            # Create a response with the PDF data and set custom headers
            response = make_response(pdf_data)
            response.headers.set('Content-Type', 'application/pdf')

            # Generate download and preview link
            download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_pdf?file={output_path}'
            preview_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/preview_pdf?file={output_path}'
            print('The consolidation location PDF GENERATED successfully and response sent ...')
            print("***************************************************************************************************")
            return jsonify(pdf=response.data.decode('latin-1'),
                           preview_link=preview_link,
                           download_link=download_link,
                           status="success",
                           fileName=file_name)

        except FileNotFoundError:
            return jsonify(status="failed", message="PDF not found."), 404
    else:
        return jsonify(status="failed", message="Failed to generate PDF."), 500


@app.route('/selected_supplier', methods=['POST', 'GET'])
def selected_supplier():
    consolidation_id = request.form.get('consolidation_id')
    print('The given consolidation_id --> ', consolidation_id)
    if len(consolidation_id) == 0:
        print('No consolidation is passed received empty string')
        status = "failed"
        return jsonify(status=status)

    status, period, dfs_list = fetch_data_for_selected_sup(consolidation_id)
    if status == "success":
        pdf_status, output_path, file_name = create_selected_sup_pdf(dfs_list, period, consolidation_id)
    else:
        print('Error while fetching data from data base ...')
        return jsonify(status=status)

    if pdf_status == "success":
        print(pdf_status)
        try:
            # Read the PDF file data
            with open(output_path, 'rb') as pdf_file:
                pdf_data = pdf_file.read()

            # Create a response with the PDF data and set custom headers
            response = make_response(pdf_data)
            response.headers.set('Content-Type', 'application/pdf')

            # Generate download and preview link
            download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_pdf?file={output_path}'
            preview_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/preview_pdf?file={output_path}'
            print('The consolidation location PDF GENERATED successfully and response sent ...')
            print("***************************************************************************************************")
            return jsonify(pdf=response.data.decode('latin-1'),
                           preview_link=preview_link,
                           download_link=download_link,
                           status="success",
                           fileName=file_name)

        except FileNotFoundError:
            return jsonify(status="failed", message="PDF not found."), 404
    else:
        return jsonify(status="failed", message="Failed to generate PDF."), 500


@app.route('/supplier_delivery_details_by_delivery', methods=['POST', 'GET'])
def supplier_delivery_details_by_delivery():
    if request.method == 'POST':
        year = request.form.get('year')
        print('year --> ', year)
        month = request.form.get('month')
        print('Month --> ', month)
        (status, arranged_nested_list, arranged_df_list, formatted_date, arranged_total_list,
         total_sums_list, counts_list) = fetch_del_details(year, month)
        if status == "success":
            status, file_name, file_with_path = create_supplier_delivery_details_pdf(arranged_nested_list,
                                                                                     arranged_df_list, formatted_date,
                                                                                     arranged_total_list,
                                                                                     total_sums_list, counts_list)
        else:
            return jsonify(status=status)

        if status == "success":
            try:
                # Read the PDF file data
                with open(file_with_path, 'rb') as pdf_file:
                    pdf_data = pdf_file.read()

                # Create a response with the PDF data and set custom headers
                response = make_response(pdf_data)
                response.headers.set('Content-Type', 'application/pdf')

                # Generate download and preview link
                download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_pdf?file={file_with_path}'
                preview_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/preview_pdf?file={file_with_path}'
                print('The Supplier delivery details by delivery PDF GENERATED successfully and response sent ...')
                print(
                    "***************************************************************************************************")
                return jsonify(pdf=response.data.decode('latin-1'),
                               preview_link=preview_link,
                               download_link=download_link,
                               status="success",
                               fileName=file_name)

            except FileNotFoundError:
                message = "PDF not found."
                print(message)
                return jsonify(status="failed"), 404
        else:
            return jsonify(status=status)


@app.route('/cwh_delivery_invoice_pdf', methods=['POST', 'GET'])
def cwh_delivery_invoice_pdf():
    if request.method == 'POST':
        year = request.form.get('year')
        month = request.form.get('month')
        print(f'Year: {year}, Month: {month}')

        merged_files_list = []

        status, df_list, other_totals, disposal_totals, cleaning_totals, food_totals, total_amounts, nested_lists, formatted_dates, delivery_types = fetch_cwh_details(
            month, year)
        if status == "success" and len(df_list) != 0:
            print("YES")
            cwh_pdf_status, cwh_output_path, _ = create_cwh_pdf(df_list, other_totals, disposal_totals, cleaning_totals,
                                                                food_totals, total_amounts, nested_lists,
                                                                formatted_dates,
                                                                delivery_types)
            if cwh_pdf_status != "success":
                return jsonify(status=cwh_pdf_status)
            print(cwh_output_path)
            merged_files_list.append(cwh_output_path)
            print(merged_files_list)
        else:
            pass

        status, df_list, other_totals, disposal_totals, cleaning_totals, food_totals, total_amounts, nested_lists, formatted_dates, delivery_types = fetch_direct_delivery_details(
            month, year)
        if status == "success" and len(df_list) != 0:
            direct_delivery_pdf_status, direct_delivery_output_path, _ = create_cwh_pdf(df_list, other_totals,
                                                                                        disposal_totals,
                                                                                        cleaning_totals,
                                                                                        food_totals, total_amounts,
                                                                                        nested_lists, formatted_dates,
                                                                                        delivery_types)
            if direct_delivery_pdf_status != "success":
                return jsonify(status=direct_delivery_pdf_status)

            merged_files_list.append(direct_delivery_output_path)
        else:
            pass
        print("merged_files_list --->", merged_files_list)
        if len(merged_files_list) == 0:
            return jsonify(status="failed")

        output_folder = r'C:\Users\Administrator\Downloads\eiis\CWH\merged_pdf'
        current_datetime = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        merged_file_name = f"CWH_DIRECT_DELIVERY_{current_datetime}.pdf"
        output_path = os.path.join(output_folder, merged_file_name)

        merge_status, merged_output_path = merge_pdfs(merged_files_list, output_path)
        if merge_status != "success":
            return jsonify(status=merge_status)

        file_name = os.path.basename(merged_output_path)
        print(f"File name: {file_name}")

        try:
            with open(merged_output_path, 'rb') as pdf_file:
                pdf_data = pdf_file.read()

            response = make_response(pdf_data)
            response.headers.set('Content-Type', 'application/pdf')

            download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_pdf?file={merged_output_path}'
            preview_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/preview_pdf?file={merged_output_path}'

            print('The Supplier delivery details by delivery PDF GENERATED successfully and response sent ...')
            print("***************************************************************************************************")

            return jsonify(
                pdf=response.data.decode('latin-1'),
                preview_link=preview_link,
                download_link=download_link,
                status="success",
                fileName=file_name
            )
        except FileNotFoundError:
            message = "PDF not found."
            print(message)
            return jsonify(status="failed", message=message), 404

    return jsonify(status="failed")


@app.route('/cwh_delivery_invoice_pdf_new', methods=['POST', 'GET'])
def cwh_delivery_invoice_pdf_new():
    if request.method == 'POST':
        year = request.form.get('year')
        month = request.form.get('month')

        print(f'Year: {year}, Month: {month}')

        merged_files_list = []

        status, df_list, other_totals, disposal_totals, cleaning_totals, food_totals, total_amounts, nested_lists, formatted_dates, delivery_types = fetch_cwh_details_by_loc(
            month, year)
        if status == "success" and len(df_list) != 0:
            print("YES")
            cwh_pdf_status, cwh_output_path, _ = create_cwh_pdf_for_loc(df_list, other_totals, disposal_totals,
                                                                        cleaning_totals,
                                                                        food_totals, total_amounts, nested_lists,
                                                                        formatted_dates,
                                                                        delivery_types)
            if cwh_pdf_status != "success":
                return jsonify(status=cwh_pdf_status)
            print(cwh_output_path)
            merged_files_list.append(cwh_output_path)
            print(merged_files_list)
        else:
            pass

        status, df_list, other_totals, disposal_totals, cleaning_totals, food_totals, total_amounts, nested_lists, formatted_dates, delivery_types = fetch_direct_delivery_details_for_loc(
            month, year)
        if status == "success" and len(df_list) != 0:
            direct_delivery_pdf_status, direct_delivery_output_path, _ = create_cwh_pdf_for_loc(df_list, other_totals,
                                                                                                disposal_totals,
                                                                                                cleaning_totals,
                                                                                                food_totals,
                                                                                                total_amounts,
                                                                                                nested_lists,
                                                                                                formatted_dates,
                                                                                                delivery_types)
            if direct_delivery_pdf_status != "success":
                return jsonify(status=direct_delivery_pdf_status)

            merged_files_list.append(direct_delivery_output_path)
        else:
            pass
        print("merged_files_list --->", merged_files_list)
        if len(merged_files_list) == 0:
            return jsonify(status="failed")

        output_folder = r'C:\Users\Administrator\Downloads\eiis\cwh_invoice_for_location\merged_pdf'
        current_datetime = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        merged_file_name = f"CWH_DIRECT_DELIVERY_INVOICE_FOR_LOCATION_{current_datetime}.pdf"
        output_path = os.path.join(output_folder, merged_file_name)

        merge_status, merged_output_path = merge_pdfs(merged_files_list, output_path)
        if merge_status != "success":
            return jsonify(status=merge_status)

        file_name = os.path.basename(merged_output_path)
        print(f"File name: {file_name}")

        try:
            with open(merged_output_path, 'rb') as pdf_file:
                pdf_data = pdf_file.read()

            response = make_response(pdf_data)
            response.headers.set('Content-Type', 'application/pdf')

            download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_pdf?file={merged_output_path}'
            preview_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/preview_pdf?file={merged_output_path}'

            print('The Supplier delivery details by delivery PDF GENERATED successfully and response sent ...')
            print("***************************************************************************************************")

            return jsonify(
                pdf=response.data.decode('latin-1'),
                preview_link=preview_link,
                download_link=download_link,
                status="success",
                fileName=file_name
            )
        except FileNotFoundError:
            message = "PDF not found."
            print(message)
            return jsonify(status="failed", message=message), 404

    return jsonify(status="failed")


@app.route('/cwh_delivery_invoice_pdf_date_wise', methods=['POST', 'GET'])
def cwh_delivery_invoice_pdf_date_wise():
    if request.method == 'POST':
        from_date = request.form.get('from_date')
        to_date = request.form.get('to_date')
        year = request.form.get('year')
        month = request.form.get('month')
        screen_num = str(request.form.get('screen_num'))
        print(f'from_date: {from_date}, to_date: {to_date}, screen_number: {screen_num}, month: {month}, year: {year}')

        merged_files_list = []
        if screen_num == "1":
            status, df_list, other_totals, disposal_totals, cleaning_totals, food_totals, total_amounts, nested_lists, delivery_types, message = fetch_cwh_details_from_to_date(
                from_date, to_date, month, year)
            if status == "success" and len(df_list) != 0 and message == "success":
                print("YES")
                cwh_pdf_status, cwh_output_path, _ = create_cwh_dd_pdf_date_wise(df_list, other_totals, disposal_totals,
                                                                                 cleaning_totals,
                                                                                 food_totals, total_amounts,
                                                                                 nested_lists,
                                                                                 delivery_types)
                if cwh_pdf_status != "success":
                    message = "failed"
                    return jsonify(status=cwh_pdf_status, message=message)
                print(cwh_output_path)
                merged_files_list.append(cwh_output_path)
                print(merged_files_list)
            else:
                pass

            status, df_list, other_totals, disposal_totals, cleaning_totals, food_totals, total_amounts, nested_lists, delivery_types, message = fetch_direct_delivery_details_from_to_date(
                from_date, to_date, month, year)
            if status == "success" and len(df_list) != 0 and message == "success":
                direct_delivery_pdf_status, direct_delivery_output_path, _ = create_cwh_dd_pdf_date_wise(df_list,
                                                                                                         other_totals,
                                                                                                         disposal_totals,
                                                                                                         cleaning_totals,
                                                                                                         food_totals,
                                                                                                         total_amounts,
                                                                                                         nested_lists,
                                                                                                         delivery_types)
                if direct_delivery_pdf_status != "success":
                    message = "failed"
                    return jsonify(status=direct_delivery_pdf_status, message=message)

                merged_files_list.append(direct_delivery_output_path)
            else:
                pass
            print("merged_files_list --->", merged_files_list)
            if len(merged_files_list) == 0:
                message = "No data available"
                print(message)
                return jsonify(status="success", message=message)
        else:
            status, df_list, other_totals, disposal_totals, cleaning_totals, food_totals, total_amounts, nested_lists, delivery_types, message = fetch_cwh_details_unit_from_to_date(
                from_date, to_date, month, year)
            if status == "success" and len(df_list) != 0 and message == "success":
                print("YES")
                cwh_pdf_status, cwh_output_path, _ = create_cwh_dd_pdf_date_wise(df_list, other_totals, disposal_totals,
                                                                                 cleaning_totals,
                                                                                 food_totals, total_amounts,
                                                                                 nested_lists,
                                                                                 delivery_types)
                if cwh_pdf_status != "success":
                    message = "failed"
                    return jsonify(status=cwh_pdf_status, message=message)
                print(cwh_output_path)
                merged_files_list.append(cwh_output_path)
                print(merged_files_list)
            else:
                pass

            status, df_list, other_totals, disposal_totals, cleaning_totals, food_totals, total_amounts, nested_lists, delivery_types, message = fetch_direct_delivery_details_unit_from_to_date(
                from_date, to_date, month, year)
            if status == "success" and len(df_list) != 0 and message == "success":
                direct_delivery_pdf_status, direct_delivery_output_path, _ = create_cwh_dd_pdf_date_wise(df_list,
                                                                                                         other_totals,
                                                                                                         disposal_totals,
                                                                                                         cleaning_totals,
                                                                                                         food_totals,
                                                                                                         total_amounts,
                                                                                                         nested_lists,
                                                                                                         delivery_types)
                if direct_delivery_pdf_status != "success":
                    message = "failed"
                    return jsonify(status=direct_delivery_pdf_status, message=message)

                merged_files_list.append(direct_delivery_output_path)
            else:
                pass
            print("merged_files_list --->", merged_files_list)
            if len(merged_files_list) == 0:
                message = "No data available"
                print(message)
                return jsonify(status="failed", message=message)
        output_folder = r'C:\Users\Administrator\Downloads\eiis\CWH\merged_pdf'
        current_datetime = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        merged_file_name = f"CWH_DIRECT_DELIVERY_{current_datetime}.pdf"
        output_path = os.path.join(output_folder, merged_file_name)

        merge_status, merged_output_path = merge_pdfs_date_wise(merged_files_list, output_path)
        if merge_status != "success":
            message = "failed"
            return jsonify(status=merge_status, message=message)

        file_name = os.path.basename(merged_output_path)
        print(f"File name: {file_name}")

        try:
            with open(merged_output_path, 'rb') as pdf_file:
                pdf_data = pdf_file.read()

            response = make_response(pdf_data)
            response.headers.set('Content-Type', 'application/pdf')

            download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_pdf?file={merged_output_path}'
            preview_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/preview_pdf?file={merged_output_path}'

            print('The CWH DELIVERY DETAILS BY INVOICE AND LOCATION  PDF GENERATED successfully and response sent ...')
            print("***************************************************************************************************")
            message = "success"
            return jsonify(
                pdf=response.data.decode('latin-1'),
                preview_link=preview_link,
                download_link=download_link,
                status="success",
                fileName=file_name,
                message=message
            )
        except FileNotFoundError:
            message = "PDF not found."
            print(message)
            return jsonify(status="failed", message=message), 404

    return jsonify(status="failed", message="failed")


@app.route('/theoritical_stock_API', methods=['POST', 'GET'])
def theoritical_stock_excel():
    if request.method == 'POST':
        dfs_list, family_id_list, family_name_list, total_qty_list, total_value_list, status = fetch_theoritical_data()
        if status == "success":
            status, file_name, excel_filepath = generate_theoritical_excel_report(dfs_list, family_id_list,
                                                                                  family_name_list, total_qty_list,
                                                                                  total_value_list)
        else:
            return jsonify(status=status)

        if status == "success" and excel_filepath is not None:
            try:
                # Generate download link
                download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_excel?file={excel_filepath}'
                print('Excel file ready for download')
                return jsonify(file_path=excel_filepath,
                               download_link=download_link,
                               status=status,
                               fileName=file_name)

            except FileNotFoundError:
                status = "failed"
                print("Excel file not found.")
                return jsonify(status=status), 404
        else:
            status = "failed"
            return jsonify(status=status)


@app.route('/cwh_delivery_details_by_location_api', methods=['POST', 'GET'])
def cwh_delivery_details_by_location():
    if request.method == 'POST':
        location_id = request.form.get('location_id')
        month = request.form.get('month')
        year = request.form.get('year')
        (cession_total_list, pur_value_total_list, saving_total_list, dfs_list, saving_percentage_total,
         status, formatted_date, nested_list) = fetch_data_for_cwh_del_by_loc(location_id, month, year)
        if status == "success":
            file_name, file_with_path, status = create_cwh_del_details_by_loc_pdf(cession_total_list,
                                                                                  pur_value_total_list,
                                                                                  saving_total_list, dfs_list,
                                                                                  saving_percentage_total,
                                                                                  formatted_date, nested_list,
                                                                                  location_id)
        else:
            return jsonify(status=status)

        if status == "success" and file_with_path is not None:
            try:
                # Read the PDF file data
                with open(file_with_path, 'rb') as pdf_file:
                    pdf_data = pdf_file.read()

                # Create a response with the PDF data and set custom headers
                response = make_response(pdf_data)
                response.headers.set('Content-Type', 'application/pdf')

                # Generate download link
                download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_pdf?file={file_with_path}'
                preview_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/preview_pdf?file={file_with_path}'
                print('PDF file ready for download')
                return jsonify(pdf=response.data.decode('latin-1'),
                               file_path=file_with_path,
                               download_link=download_link,
                               preview_link=preview_link,
                               status=status,
                               fileName=file_name)

            except FileNotFoundError:
                status = "failed"
                print("PDF file not found.")
                return jsonify(status=status), 404
        else:
            status = "failed"
            return jsonify(status=status)


@app.route('/item_full_transaction_api', methods=['POST', 'GET'])
def item_full_transaction():
    print('calling [item_full_transaction_api] api ...')
    if request.method == 'POST':
        month = request.form.get('month')
        year = request.form.get('year')
        # Create a datetime object
        date_obj = datetime(int(year), int(month), 1)

        # Format the datetime object
        formatted_date = date_obj.strftime("%B-%Y")
        (status, item_id_list, item_name_list, packing_list, opening_qty_list, opening_cp_list,
         new_dfs_list, bal_qty_total_list, bal_val_total_list) = fetch_datas_for_item_full_trans(month, year)
        if status == "success":
            file_name, file_with_path, status = create_item_full_trans_pdf(new_dfs_list, item_id_list,
                                                                           item_name_list, packing_list,
                                                                           opening_qty_list,
                                                                           formatted_date, opening_cp_list,
                                                                           bal_qty_total_list, bal_val_total_list)
        else:
            return jsonify(status=status)

        if status == "success" and file_with_path is not None:
            try:
                # Read the PDF file data
                with open(file_with_path, 'rb') as pdf_file:
                    pdf_data = pdf_file.read()

                # Create a response with the PDF data and set custom headers
                response = make_response(pdf_data)
                response.headers.set('Content-Type', 'application/pdf')

                # Generate download link
                download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_pdf?file={file_with_path}'
                preview_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/preview_pdf?file={file_with_path}'
                print('PDF file ready for download')
                return jsonify(pdf=response.data.decode('latin-1'),
                               download_link=download_link,
                               preview_link=preview_link,
                               status=status,
                               fileName=file_name)

            except FileNotFoundError:
                status = "failed"
                print("PDF file not found.")
                return jsonify(status=status), 404
        else:
            status = "failed"
            return jsonify(status=status)


@app.route('/cwh_savings_api', methods=['POST', 'GET'])
def cwh_savings():
    if request.method == 'POST':
        month = request.form.get('month')
        year = request.form.get('year')
        loc_id = request.form.get('location_id')
        print(len(loc_id))
        if len(year) != 0:
            # Create a datetime object
            date_obj = datetime(int(year), int(month), 1)
            # Format the datetime object
            formatted_date = date_obj.strftime("%B-%Y")
        else:
            formatted_date = ""

        (status, df, Savings, Discount, total_sav_disc) = fetch_cwh_saving_data(month, year, loc_id)
        if status == "success":
            status, file_name, file_with_path = create_cwh_savings_pdf(df, formatted_date, Savings, Discount,
                                                                       total_sav_disc)
        else:
            return jsonify(status=status)

        if status == "success" and file_with_path is not None:
            try:
                # Read the PDF file data
                with open(file_with_path, 'rb') as pdf_file:
                    pdf_data = pdf_file.read()

                # Create a response with the PDF data and set custom headers
                response = make_response(pdf_data)
                response.headers.set('Content-Type', 'application/pdf')

                # Generate download link
                download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_pdf?file={file_with_path}'
                preview_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/preview_pdf?file={file_with_path}'
                print('PDF file ready for download')
                return jsonify(pdf=response.data.decode('latin-1'),
                               download_link=download_link,
                               preview_link=preview_link,
                               status=status,
                               fileName=file_name)

            except FileNotFoundError:
                status = "failed"
                print("PDF file not found.")
                return jsonify(status=status), 404
        else:
            status = "failed"
            return jsonify(status=status)


@app.route('/cwh_savings_by_loc_api', methods=['POST', 'GET'])
def cwh_savings_by_loc():
    if request.method == 'POST':
        month = request.form.get('month')
        year = request.form.get('year')
        location_id = request.form.get('location_id')
        # Create a datetime object
        date_obj = datetime(int(year), int(month), 1)

        # Format the datetime object
        formatted_date = date_obj.strftime("%B-%Y")
        (
            status, message, cost_food_total, cost_cleaning_total, cost_disposal_total, cost_others_total,
            cost_total_total,
            issue_food_total, issue_cleaning_total, issue_disposal_total, issue_others_total,
            issue_total_total, issue_sav_total, concatenated_df) = fetch_cwh_sav_by_loc(month, year, location_id)
        if status == "success" and message == "success":
            status, file_name, file_with_path = create_cwh_sav_by_loc_pdf(concatenated_df, formatted_date,
                                                                          cost_food_total, cost_cleaning_total,
                                                                          cost_disposal_total, cost_others_total,
                                                                          cost_total_total, issue_food_total,
                                                                          issue_cleaning_total, issue_disposal_total,
                                                                          issue_others_total, issue_total_total,
                                                                          issue_sav_total)
        else:
            return jsonify(status=status, message=message)

        if status == "success" and file_with_path is not None:
            try:
                # Read the PDF file data
                with open(file_with_path, 'rb') as pdf_file:
                    pdf_data = pdf_file.read()

                # Create a response with the PDF data and set custom headers
                response = make_response(pdf_data)
                response.headers.set('Content-Type', 'application/pdf')

                # Generate download link
                download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_pdf?file={file_with_path}'
                preview_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/preview_pdf?file={file_with_path}'
                print('PDF file ready for download')
                return jsonify(pdf=response.data.decode('latin-1'),
                               download_link=download_link,
                               preview_link=preview_link,
                               status=status,
                               fileName=file_name,
                               message='success')

            except FileNotFoundError:
                status = "failed"
                print("PDF file not found.")
                return jsonify(status=status, message="PDF file not found."), 404
        else:
            status = "failed"
            return jsonify(status=status, message='failed')


@app.route('/eom_inventory_pdf_api', methods=['POST', 'GET'])
def eom_inventory_pdf():
    if request.method == 'POST':
        month = request.form.get('month')
        year = request.form.get('year')
        family_name = request.form.get('family_name')
        # Create a datetime object
        date_obj = datetime(int(year), int(month), 1)

        # Format the datetime object
        formatted_date = date_obj.strftime("%B-%Y")
        (status, new_dfs_list, family_name_list, stock_value_total_list, pur_value_total_list, del_value_total_list,
         cwh_value_total_list, sav_value_total_list, sav_per_value_total_list,
         total_value_total_list) = fetch_data_for_eom_inv(month, year, family_name)
        if status == "success":
            status, file_name, file_with_path = create_eom_inv__pdf(new_dfs_list, family_name_list,
                                                                    formatted_date, stock_value_total_list,
                                                                    pur_value_total_list,
                                                                    del_value_total_list,
                                                                    cwh_value_total_list, sav_value_total_list,
                                                                    sav_per_value_total_list,
                                                                    total_value_total_list)
        else:
            return jsonify(status=status)

        if status == "success" and file_with_path is not None:
            try:
                # Read the PDF file data
                with open(file_with_path, 'rb') as pdf_file:
                    pdf_data = pdf_file.read()

                # Create a response with the PDF data and set custom headers
                response = make_response(pdf_data)
                response.headers.set('Content-Type', 'application/pdf')

                # Generate download link
                download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_pdf?file={file_with_path}'
                preview_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/preview_pdf?file={file_with_path}'
                print('PDF file ready for download')
                return jsonify(pdf=response.data.decode('latin-1'),
                               file_path=file_with_path,
                               download_link=download_link,
                               preview_link=preview_link,
                               status=status,
                               fileName=file_name)

            except FileNotFoundError:
                status = "failed"
                print("PDF file not found.")
                return jsonify(status=status), 404
        else:
            status = "failed"
            return jsonify(status=status)


@app.route('/quotation_pdffull', methods=['POST', 'GET'])
def quotation_pdffull_con():
    if request.method == 'POST':
        month = request.form.get('month')
        year = request.form.get('year')
        (status, sup_name_list, fax_no_list, tel_no_list, df_list, from_date,
         formatted_date) = fetch_con_quotation_req_data(month, year)
        if status == "success":
            status, file_name, file_with_path = create_con_quotation_req_pdf(sup_name_list, fax_no_list, tel_no_list,
                                                                             df_list, from_date, formatted_date)
        else:
            return jsonify(status=status)

        if status == "success" and file_with_path is not None:
            try:
                # Read the PDF file data
                with open(file_with_path, 'rb') as pdf_file:
                    pdf_data = pdf_file.read()

                # Create a response with the PDF data and set custom headers
                response = make_response(pdf_data)
                response.headers.set('Content-Type', 'application/pdf')

                # Generate download link
                download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_pdf?file={file_with_path}'
                preview_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/preview_pdf?file={file_with_path}'
                print('PDF file ready for download')
                return jsonify(pdf=response.data.decode('latin-1'),
                               download_link=download_link,
                               preview_link=preview_link,
                               status=status,
                               fileName=file_name)

            except FileNotFoundError:
                status = "failed"
                print("PDF file not found.")
                return jsonify(status=status), 404
        else:
            status = "failed"
            return jsonify(status=status)


@app.route('/supplier_invoice_details_by_delivery_pdf', methods=['POST', 'GET'])
def supplier_invoice_details_by_delivery_pdf():
    if request.method == 'POST':
        month = request.form.get('month')
        year = request.form.get('year')
        # Get the full month name
        month_name = datetime(int(year), int(month), 1).strftime('%b')  # 'Feb'

        # Concatenate month and year with a hyphen
        period = f"{month_name}-{year}"  # 'Feb-2024'
        (unique_supplier_ids, unique_supplier_name, unique_supplier_inv_id, dfs, status) = fetch_supplier_invoice_data(
            month, year)
        if status == "success":
            status, file_name, filepath = create_supplier_invoice_details_pdf(unique_supplier_ids, unique_supplier_name,
                                                                              unique_supplier_inv_id, period, dfs)
        else:
            return jsonify(status=status)

        if status == "success" and filepath is not None:
            try:
                # Read the PDF file data
                with open(filepath, 'rb') as pdf_file:
                    pdf_data = pdf_file.read()

                # Create a response with the PDF data and set custom headers
                response = make_response(pdf_data)
                response.headers.set('Content-Type', 'application/pdf')

                # Generate download link
                download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_pdf?file={filepath}'
                preview_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/preview_pdf?file={filepath}'
                print('PDF file ready for download')
                return jsonify(pdf=response.data.decode('latin-1'),
                               download_link=download_link,
                               preview_link=preview_link,
                               status=status,
                               fileName=file_name)

            except FileNotFoundError:
                status = "failed"
                print("PDF file not found.")
                return jsonify(status=status), 404
        else:
            status = "failed"
            return jsonify(status=status)


@app.route('/supplier_invoice_details_by_delivery_by_supplier_id_and_date_pdf', methods=['POST', 'GET'])
def supplier_invoice_details_by_delivery_by_supplier_id_and_date_pdf():
    if request.method == 'POST':
        from_date = request.form.get('from_date')
        to_date = request.form.get('to_date')
        supp_id = request.form.get('supplier_id')
        # Concatenate month and year with a hyphen
        period = ""  # 'Feb-2024'
        (unique_supplier_ids, unique_supplier_name, unique_supplier_inv_id, dfs,
         status) = fetch_supplier_invoice_data_by_date_and_supplier_id(
            from_date, to_date, supp_id)
        if status == "success":
            status, file_name, filepath = create_supplier_invoice_details_pdf(unique_supplier_ids, unique_supplier_name,
                                                                              unique_supplier_inv_id, period, dfs)
        else:
            return jsonify(status=status)

        if status == "success" and filepath is not None:
            try:
                # Read the PDF file data
                with open(filepath, 'rb') as pdf_file:
                    pdf_data = pdf_file.read()

                # Create a response with the PDF data and set custom headers
                response = make_response(pdf_data)
                response.headers.set('Content-Type', 'application/pdf')

                # Generate download link
                download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_pdf?file={filepath}'
                preview_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/preview_pdf?file={filepath}'
                print('PDF file ready for download')
                return jsonify(pdf=response.data.decode('latin-1'),
                               download_link=download_link,
                               preview_link=preview_link,
                               status=status,
                               fileName=file_name)

            except FileNotFoundError:
                status = "failed"
                print("PDF file not found.")
                return jsonify(status=status), 404
        else:
            status = "failed"
            return jsonify(status=status)


@app.route('/supplier_invoice_details_by_delivery_by_supplier_id_pdf', methods=['POST', 'GET'])
def supplier_invoice_details_by_delivery_by_supplier_id_pdf():
    if request.method == 'POST':
        month = request.form.get('month')
        year = request.form.get('year')
        sup_id = request.form.get('supplier_id')
        # Get the full month name
        month_name = datetime(int(year), int(month), 1).strftime('%b')  # 'Feb'

        # Concatenate month and year with a hyphen
        period = f"{month_name}-{year}"  # 'Feb-2024'
        (unique_supplier_ids, unique_supplier_name, unique_supplier_inv_id, dfs,
         status) = fetch_supplier_invoice_data_by_supplier_id(
            month, year, sup_id)
        if status == "success":
            status, file_name, filepath = create_supplier_invoice_details_pdf(unique_supplier_ids, unique_supplier_name,
                                                                              unique_supplier_inv_id, period, dfs)
        else:
            return jsonify(status=status)

        if status == "success" and filepath is not None:
            try:
                # Read the PDF file data
                with open(filepath, 'rb') as pdf_file:
                    pdf_data = pdf_file.read()

                # Create a response with the PDF data and set custom headers
                response = make_response(pdf_data)
                response.headers.set('Content-Type', 'application/pdf')

                # Generate download link
                download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_pdf?file={filepath}'
                preview_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/preview_pdf?file={filepath}'
                print('PDF file ready for download')
                return jsonify(pdf=response.data.decode('latin-1'),
                               download_link=download_link,
                               preview_link=preview_link,
                               status=status,
                               fileName=file_name)

            except FileNotFoundError:
                status = "failed"
                print("PDF file not found.")
                return jsonify(status=status), 404
        else:
            status = "failed"
            return jsonify(status=status)


@app.route('/credit_book_api', methods=['POST', 'GET'])
def credit_book_api():
    if request.method == 'POST':
        month = request.form.get('month')
        year = request.form.get('year')
        # Get the full month name
        month_name = datetime(int(year), int(month), 1).strftime('%b')  # 'Feb'

        # Concatenate month and year with a hyphen
        period = f"{month_name}-{year}"  # 'Feb-2024'
        (status, cession_in_out_df, cash_pur_df, credit_pur_df, table_sub_total, sub_total,
         grand_total) = fetch_credit_book_data(month, year)
        if status == "success":
            status, file_name, filepath = create_credit_book_pdf(period, cession_in_out_df, cash_pur_df, credit_pur_df,
                                                                 table_sub_total, sub_total, grand_total)
        else:
            return jsonify(status=status)

        if status == "success" and filepath is not None:
            try:
                # Read the PDF file data
                with open(filepath, 'rb') as pdf_file:
                    pdf_data = pdf_file.read()

                # Create a response with the PDF data and set custom headers
                response = make_response(pdf_data)
                response.headers.set('Content-Type', 'application/pdf')

                # Generate download link
                download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_pdf?file={filepath}'
                preview_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/preview_pdf?file={filepath}'
                print('PDF file ready for download')
                return jsonify(pdf=response.data.decode('latin-1'),
                               download_link=download_link,
                               preview_link=preview_link,
                               status=status,
                               fileName=file_name)

            except FileNotFoundError:
                status = "failed"
                print("PDF file not found.")
                return jsonify(status=status), 404
        else:
            status = "failed"
            return jsonify(status=status)


@app.route('/cwh_delivery_details_by_invoice', methods=['POST', 'GET'])
def cwh_delivery_details_by_invoice():
    if request.method == 'POST':
        print('CALLING [cwh_delivery_details_by_invoice] API ...')
        month = request.form.get('month')
        year = request.form.get('year')
        location_id = request.form.get('location_id')
        from_date = request.form.get('from_date')
        to_date = request.form.get('to_date')
        if len(from_date) == 0 and len(to_date) == 0:
            # Get the full month name
            month_name = datetime(int(year), int(month), 1).strftime('%b')  # 'Feb'
            # Concatenate month and year with a hyphen
            period = f"{month_name}-{year}"  # 'Feb-2024'
            (status, final_df, sub_total_list, Grand_total_list) = fetch_cwh_invoice_details(month, year, location_id)
        else:
            # Concatenate month and year with a hyphen
            period = ""  # 'Feb-2024'
            (status, final_df, sub_total_list, Grand_total_list) = fetch_cwh_invoice_details_by_date_and_location(
                from_date, to_date, location_id)
        if status == "success":
            status, file_name, filepath = create_cwh_invoice_pdf(period, final_df, sub_total_list, Grand_total_list)
        else:
            return jsonify(status=status)

        if status == "success" and filepath is not None:

            try:
                # Read the PDF file data
                with open(filepath, 'rb') as pdf_file:
                    pdf_data = pdf_file.read()

                # Create a response with the PDF data and set custom headers
                response = make_response(pdf_data)
                response.headers.set('Content-Type', 'application/pdf')

                # Generate download link
                download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_pdf?file={filepath}'
                preview_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/preview_pdf?file={filepath}'
                print('PDF file ready for download')
                return jsonify(pdf=response.data.decode('latin-1'),
                               download_link=download_link,
                               preview_link=preview_link,
                               status=status,
                               fileName=file_name)

            except FileNotFoundError:
                status = "failed"
                print("PDF file not found.")
                return jsonify(status=status), 404
        else:
            status = "failed"
            return jsonify(status=status)


"""
@app.route('/cwh_del_details_by_ind_loc', methods=['POST', 'GET'])
def cwh_del_details_by_ind_loc():
    print('CALLING [cwh_del_details_by_ind_loc] API ...')
    if request.method == 'POST':
        year = request.form.get('year')
        month = request.form.get('month')
        loc_id = request.form.get('location_id')
        del_type = request.form.get('del_type')
        screen_num = str(request.form.get('screen_num'))

        print(f'Year: {year}, Month: {month}, location ID: {loc_id}, Delivery Type: '
              f'{del_type}, screen_num: {screen_num}')

        if del_type == "cwh_delivery" and screen_num == '2':
            (status, df_list, other_total, disposal_total, cleaning_total, food_total, total_amount,
             nested_list, formatted_date, delivery_type) = fetch_cwh_details_by_ind_loc(month, year, loc_id)
        elif del_type == "direct_delivery" and screen_num == "2":
            (status, df_list, other_total, disposal_total, cleaning_total, food_total, total_amount,
             nested_list, formatted_date, delivery_type) = fetch_direct_delivery_details_for_ind_loc(month, year,
                                                                                                     loc_id)
        elif del_type == "cwh_delivery" and screen_num == "1":
            (status, df_list, other_total, disposal_total, cleaning_total, food_total, total_amount,
             nested_list, formatted_date, delivery_type) = fetch_cwh_details_old(month, year, loc_id)
        elif del_type == "direct_delivery" and screen_num == "1":
            (status, df_list, other_total, disposal_total, cleaning_total, food_total, total_amount,
             nested_list, formatted_date, delivery_type) = fetch_direct_delivery_details_old(month, year, loc_id)

        if status == "failed":
            return jsonify(status=status)
        else:
            (status, output_path, merged_file_name) = create_cwh_pdf_for_ind_loc(df_list, other_total, disposal_total,
                                                                                 cleaning_total, food_total,
                                                                                 total_amount,
                                                                                 nested_list,
                                                                                 formatted_date, delivery_type)
        if status == "success" and output_path is not None:
            try:
                # Read the PDF file data
                with open(output_path, 'rb') as pdf_file:
                    pdf_data = pdf_file.read()

                # Create a response with the PDF data and set custom headers
                response = make_response(pdf_data)
                response.headers.set('Content-Type', 'application/pdf')

                # Generate download link
                download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_pdf?file={output_path}'
                preview_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/preview_pdf?file={output_path}'
                print('PDF file ready for download')
                return jsonify(pdf=response.data.decode('latin-1'),
                               download_link=download_link,
                               preview_link=preview_link,
                               status=status,
                               fileName=merged_file_name)

            except FileNotFoundError:
                status = "failed"
                print("PDF file not found.")
                return jsonify(status=status), 404
        else:
            status = "failed"
            return jsonify(status=status)

"""


@app.route('/cwh_del_details_by_ind_loc_date_wise', methods=['POST', 'GET'])
def cwh_del_details_by_ind_loc_date_wise():
    print('CALLING [cwh_del_details_by_ind_loc_date_wise] API ...')
    if request.method == 'POST':
        from_date = request.form.get('from_date')
        to_date = request.form.get('to_date')
        loc_id = request.form.get('location_id')
        del_type = request.form.get('del_type')
        screen_num = str(request.form.get('screen_num'))

        print(f'From Date: {from_date}, To Date: {to_date}, location ID: {loc_id}, Delivery Type: '
              f'{del_type}, screen_num: {screen_num}')

        if del_type == "cwh_delivery" and screen_num == '2':
            (status, df_list, other_total, disposal_total, cleaning_total, food_total, total_amount,
             nested_list, delivery_type) = fetch_cwh_details_by_loc_date_unit_price(loc_id, from_date,
                                                                                    to_date)
        elif del_type == "direct_delivery" and screen_num == "2":
            (status, df_list, other_total, disposal_total, cleaning_total, food_total, total_amount,
             nested_list, delivery_type) = fetch_direct_delivery_details_for_ind_loc_date_wise(loc_id,
                                                                                               from_date,
                                                                                               to_date)
        elif del_type == "cwh_delivery" and screen_num == "1":
            (status, df_list, other_total, disposal_total, cleaning_total, food_total, total_amount,
             nested_list, delivery_type) = fetch_cwh_details_by_loc_date_old(loc_id, from_date, to_date)
        elif del_type == "direct_delivery" and screen_num == "1":
            (status, df_list, other_total, disposal_total, cleaning_total, food_total, total_amount,
             nested_list, delivery_type) = fetch_direct_delivery_details_date_wise_old(loc_id,
                                                                                       from_date,
                                                                                       to_date)

        if status == "failed":
            return jsonify(status=status)
        else:
            (status, output_path, merged_file_name) = create_cwh_pdf_for_ind_loc_by_date(df_list, other_total,
                                                                                         disposal_total,
                                                                                         cleaning_total, food_total,
                                                                                         total_amount,
                                                                                         nested_list,
                                                                                         delivery_type)
        if status == "success" and output_path is not None:
            try:
                # Read the PDF file data
                with open(output_path, 'rb') as pdf_file:
                    pdf_data = pdf_file.read()

                # Create a response with the PDF data and set custom headers
                response = make_response(pdf_data)
                response.headers.set('Content-Type', 'application/pdf')

                # Generate download link
                download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_pdf?file={output_path}'
                preview_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/preview_pdf?file={output_path}'
                print('PDF file ready for download')
                return jsonify(pdf=response.data.decode('latin-1'),
                               download_link=download_link,
                               preview_link=preview_link,
                               status=status,
                               fileName=merged_file_name)

            except FileNotFoundError:
                status = "failed"
                print("PDF file not found.")
                return jsonify(status=status), 404
        else:
            status = "failed"
            return jsonify(status=status)


@app.route('/savings_by_loc_by_item', methods=['POST', 'GET'])
def savings_by_loc_by_item():
    if request.method == 'POST':
        print('CALLING [savings_by_loc_by_item] API ...')
        month = request.form.get('month')
        year = request.form.get('year')
        # Get the full month name
        month_name = datetime(int(year), int(month), 1).strftime('%b')  # 'Feb'

        # Concatenate month and year with a hyphen
        period = f"{month_name}-{year}"  # 'Feb-2024'
        (unique_LocationID, unique_LocationName, dfs, cession_total_list, purchase_total_list,
         sav_total_list, sav_per_total_list, cession_grand_total, purchase_grand_total, sav_grand_total,
         sav_per_grand_total, status) = fetch_data_for_sav_by_loc_by_item(month, year)
        if status == "success":
            status, file_name, filepath = create_save_by_loc_by_item_pdf(unique_LocationID, unique_LocationName, period,
                                                                         dfs,
                                                                         cession_total_list,
                                                                         purchase_total_list,
                                                                         sav_total_list, sav_per_total_list,
                                                                         cession_grand_total, purchase_grand_total,
                                                                         sav_grand_total, sav_per_grand_total)
        else:
            return jsonify(status=status)

        if status == "success" and filepath is not None:
            try:
                # Read the PDF file data
                with open(filepath, 'rb') as pdf_file:
                    pdf_data = pdf_file.read()

                # Create a response with the PDF data and set custom headers
                response = make_response(pdf_data)
                response.headers.set('Content-Type', 'application/pdf')

                # Generate download link
                download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_pdf?file={filepath}'
                preview_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/preview_pdf?file={filepath}'
                print('PDF file ready for download')
                return jsonify(pdf=response.data.decode('latin-1'),
                               download_link=download_link,
                               preview_link=preview_link,
                               status=status,
                               fileName=file_name)

            except FileNotFoundError:
                status = "failed"
                print("PDF file not found.")
                return jsonify(status=status), 404
        else:
            status = "failed"
            return jsonify(status=status)


@app.route('/savings_by_loc_by_item_by_ind_location_id', methods=['POST', 'GET'])
def savings_by_loc_by_item_by_ind_location_id():
    if request.method == 'POST':
        print('CALLING [savings_by_loc_by_item_by_ind_location_id] API ...')
        month = request.form.get('month')
        year = request.form.get('year')
        location_id = request.form.get('location_id')
        # Get the full month name
        month_name = datetime(int(year), int(month), 1).strftime('%b')  # 'Feb'

        # Concatenate month and year with a hyphen
        period = f"{month_name}-{year}"  # 'Feb-2024'
        (unique_LocationID, unique_LocationName, dfs, cession_total_list, purchase_total_list,
         sav_total_list, sav_per_total_list, cession_grand_total, purchase_grand_total, sav_grand_total,
         sav_per_grand_total, status) = fetch_data_for_sav_by_loc_by_item_by_ind_location_id(month, year, location_id)
        if status == "success":
            status, file_name, filepath = create_save_by_loc_by_item_pdf(unique_LocationID, unique_LocationName, period,
                                                                         dfs,
                                                                         cession_total_list,
                                                                         purchase_total_list,
                                                                         sav_total_list, sav_per_total_list,
                                                                         cession_grand_total, purchase_grand_total,
                                                                         sav_grand_total, sav_per_grand_total)
        else:
            return jsonify(status=status)

        if status == "success" and filepath is not None:
            try:
                # Read the PDF file data
                with open(filepath, 'rb') as pdf_file:
                    pdf_data = pdf_file.read()

                # Create a response with the PDF data and set custom headers
                response = make_response(pdf_data)
                response.headers.set('Content-Type', 'application/pdf')

                # Generate download link
                download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_pdf?file={filepath}'
                preview_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/preview_pdf?file={filepath}'
                print('PDF file ready for download')
                return jsonify(pdf=response.data.decode('latin-1'),
                               download_link=download_link,
                               preview_link=preview_link,
                               status=status,
                               fileName=file_name)

            except FileNotFoundError:
                status = "failed"
                print("PDF file not found.")
                return jsonify(status=status), 404
        else:
            status = "failed"
            return jsonify(status=status)


@app.route('/savings_by_loc_by_item_by_ind_location_id_and_date', methods=['POST', 'GET'])
def savings_by_loc_by_item_by_ind_location_id_and_date():
    if request.method == 'POST':
        print('CALLING [savings_by_loc_by_item_by_ind_location_id_and_date] API ...')
        from_date = request.form.get('from_date')
        to_date = request.form.get('to_date')
        location_id = request.form.get('location_id')

        # Concatenate month and year with a hyphen
        period = ""  # 'Feb-2024'
        (unique_LocationID, unique_LocationName, dfs, cession_total_list, purchase_total_list,
         sav_total_list, sav_per_total_list, cession_grand_total, purchase_grand_total, sav_grand_total,
         sav_per_grand_total, status) = fetch_data_for_sav_by_loc_by_item_by_ind_location_id_and_date(from_date,
                                                                                                      to_date,
                                                                                                      location_id)
        if status == "success":
            status, file_name, filepath = create_save_by_loc_by_item_pdf(unique_LocationID, unique_LocationName, period,
                                                                         dfs,
                                                                         cession_total_list,
                                                                         purchase_total_list,
                                                                         sav_total_list, sav_per_total_list,
                                                                         cession_grand_total, purchase_grand_total,
                                                                         sav_grand_total, sav_per_grand_total)
        else:
            return jsonify(status=status)

        if status == "success" and filepath is not None:
            try:
                # Read the PDF file data
                with open(filepath, 'rb') as pdf_file:
                    pdf_data = pdf_file.read()

                # Create a response with the PDF data and set custom headers
                response = make_response(pdf_data)
                response.headers.set('Content-Type', 'application/pdf')

                # Generate download link
                download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_pdf?file={filepath}'
                preview_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/preview_pdf?file={filepath}'
                print('PDF file ready for download')
                return jsonify(pdf=response.data.decode('latin-1'),
                               download_link=download_link,
                               preview_link=preview_link,
                               status=status,
                               fileName=file_name)

            except FileNotFoundError:
                status = "failed"
                print("PDF file not found.")
                return jsonify(status=status), 404
        else:
            status = "failed"
            return jsonify(status=status)


@app.route('/purchase_price_analysis_api', methods=['POST', 'GET'])
def purchase_price_analysis_api():
    if request.method == 'POST':
        print('CALLING [purchase_price_analysis_api] API ...')
        from_month = request.form.get('from_month')
        to_month = request.form.get('to_month')
        supplier_id = request.form.get('supplier_id')
        item_id = request.form.get('item_id')
        status, pivot_df, message = fetch_data_for_purchase_price_analysis(from_month, to_month, supplier_id, item_id)
        if status == "failed":
            return jsonify(status=status, message=message)
        elif pivot_df.empty:
            return jsonify(status=status, message=message)
        else:
            status, file_name, file_path = create_purchase_price_analysis_pdf(pivot_df)

        if status == "success" and file_path is not None:
            try:
                # Read the PDF file data
                with open(file_path, 'rb') as pdf_file:
                    pdf_data = pdf_file.read()

                # Create a response with the PDF data and set custom headers
                response = make_response(pdf_data)
                response.headers.set('Content-Type', 'application/pdf')

                # Generate download link
                download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_pdf?file={file_path}'
                preview_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/preview_pdf?file={file_path}'
                print('PDF file ready for download')
                message = "success"
                return jsonify(pdf=response.data.decode('latin-1'),
                               download_link=download_link,
                               preview_link=preview_link,
                               status=status,
                               fileName=file_name,
                               message=message)

            except FileNotFoundError:
                status = "failed"
                print("PDF file not found.")
                message = "failed"
                return jsonify(status=status, message=message), 404
        else:
            status = "failed"
            message = "failed"
            return jsonify(status=status, message=message)


# purchase price analysis excel reports
@app.route('/purchase_price_analysis_excel_report_api', methods=['GET', 'POST'])
def purchase_price_analysis_excel_report():
    if request.method == 'POST':
        print('CALLING [purchase_price_analysis_excel_report_api] API ...')
        period = request.form.get('period')
        excel_type = str(request.form.get('excel_type'))
        print(f'Period : {period}, Excel Type : {excel_type}')
        (df_first_six, df_after_six, length_of_column, from_month, mid_month,
         to_month, QTY_1, QTY_2, QTY_3, Amt_1, Amt_2, Amt_3, AvgUnitPrice_1, AvgUnitPrice_2,
         AvgUnitPrice_3, avg_Amt, avg_QTY, avg_UnitPrice, data_status, data_message) = fetch_data_for_excel_report(
            period,
            excel_type)
        if data_message == "success" and data_status == "success":
            report_status, report_message, file_name, file_path = create_purchase_price_analysis_excel_report(
                df_first_six, df_after_six, length_of_column, from_month, mid_month,
                to_month, QTY_1, QTY_2, QTY_3, Amt_1, Amt_2, Amt_3, AvgUnitPrice_1,
                AvgUnitPrice_2, AvgUnitPrice_3, avg_Amt, avg_QTY, avg_UnitPrice
                , excel_type)
            if report_status == "success":
                try:
                    # Generate download link
                    download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_excel?file={file_path}'
                    print("Excel file ready for download")
                    return jsonify(file_path=file_path,
                                   download_link=download_link,
                                   status=report_status,
                                   fileName=file_name,
                                   message=report_message)

                except FileNotFoundError:
                    status = "failed"
                    print("Excel file not found.")
                    return jsonify(status=status, message='Excel file not found.'), 404
            else:
                status = "failed"
                return jsonify(status=status, message=status)
        elif data_message == "No data available" and data_status == "success":
            return jsonify(status=data_status, message=data_message)
        else:
            return jsonify(status=data_status, message=data_message)
    else:
        return jsonify(status="Method not allowed. Only POST requests are allowed."), 405


@app.route('/cwh_delivery_note_invoice_period_and_loc_id', methods=['POST', 'GET'])
# @app.route('/cwh_del_details_by_ind_loc', methods=['POST', 'GET'])
def cwh_del_details_by_ind_loc():
    print('CALLING [cwh_delivery_note_invoice_period_and_loc_id] API ...')
    # print('CALLING [cwh_del_details_by_ind_loc] API ...')
    if request.method == 'POST':
        year = request.form.get('year')
        month = request.form.get('month')
        loc_id = request.form.get('location_id')
        del_type = request.form.get('del_type')
        screen_num = str(request.form.get('screen_num'))

        print(
            f'Year: {year}, Month: {month}, location ID: {loc_id}, Delivery Type: {del_type}, Screen Num: {screen_num}')

        if del_type == "cwh_delivery" and screen_num == '2':
            (status, df_list, other_total, disposal_total, cleaning_total, food_total, total_amount,
             nested_list, formatted_date, delivery_type, message) = fetch_cwh_details_by_ind_loc(month, year, loc_id)
        elif del_type == "direct_delivery" and screen_num == "2":
            (status, df_list, other_total, disposal_total, cleaning_total, food_total, total_amount,
             nested_list, formatted_date, delivery_type, message) = fetch_direct_delivery_details_for_ind_loc(month,
                                                                                                              year,
                                                                                                              loc_id)
        elif del_type == "cwh_delivery" and screen_num == "1":
            (status, df_list, other_total, disposal_total, cleaning_total, food_total, total_amount,
             nested_list, formatted_date, delivery_type, message) = fetch_cwh_details_old(month, year, loc_id)
        elif del_type == "direct_delivery" and screen_num == "1":
            (status, df_list, other_total, disposal_total, cleaning_total, food_total, total_amount,
             nested_list, formatted_date, delivery_type, message) = fetch_direct_delivery_details_old(month, year,
                                                                                                      loc_id)
        elif del_type == "Both" and screen_num == "1":
            pdf_file_path_list: list = []
            (cwh_data_status, df_list, other_total, disposal_total, cleaning_total, food_total, total_amount,
             nested_list, formatted_date, delivery_type, cwh_data_message) = fetch_cwh_details_old(month, year, loc_id)
            if cwh_data_status == "failed":
                return jsonify(status=cwh_data_status, message=cwh_data_message)
            elif cwh_data_status == "success" and cwh_data_message == 'No data available':
                # return jsonify(status=status, message=message)
                pass
            else:
                (cwh_pdf_status, output_path, merged_file_name) = create_cwh_pdf_for_ind_loc(df_list, other_total,
                                                                                             disposal_total,
                                                                                             cleaning_total, food_total,
                                                                                             total_amount,
                                                                                             nested_list,
                                                                                             formatted_date,
                                                                                             delivery_type)
                if cwh_pdf_status == "success":
                    pdf_file_path_list.append(output_path)
                else:
                    print(cwh_pdf_status)
                    pass
            (
                direct_delivery_data_status, df_list, other_total, disposal_total, cleaning_total, food_total,
                total_amount,
                nested_list, formatted_date, delivery_type,
                direct_delivery_message) = fetch_direct_delivery_details_old(month, year,
                                                                             loc_id)

            if direct_delivery_data_status == "failed":
                return jsonify(status=direct_delivery_data_status, message=direct_delivery_message)
            elif direct_delivery_data_status == "success" and direct_delivery_message == 'No data available':
                # return jsonify(status=status, message=message)
                pass
            elif cwh_data_status == "success" and direct_delivery_data_status == "success" and direct_delivery_message == 'No data available' and cwh_data_message == "No data available":
                status = "success"
                return jsonify(status=status, message=direct_delivery_message)
            else:
                (direct_delivery_pdf_status, output_path, merged_file_name) = create_cwh_pdf_for_ind_loc(df_list,
                                                                                                         other_total,
                                                                                                         disposal_total,
                                                                                                         cleaning_total,
                                                                                                         food_total,
                                                                                                         total_amount,
                                                                                                         nested_list,
                                                                                                         formatted_date,
                                                                                                         delivery_type)
                if direct_delivery_pdf_status == "success":
                    pdf_file_path_list.append(output_path)
                else:
                    print(direct_delivery_pdf_status)
                    pass
            if len(pdf_file_path_list) != 0:
                output_folder = r'C:\Users\Administrator\eiis_pdf\pythonProject\PDFfile\cwh\merged_pdf'
                current_datetime = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
                merged_file_name = f"CWH_DIRECT_DELIVERY_{current_datetime}.pdf"
                output_path = os.path.join(output_folder, merged_file_name)

                merge_status, merged_output_path = merge_pdfs(pdf_file_path_list, output_path)
                if merge_status != "success":
                    message = "failed"
                    return jsonify(status=merge_status, message=message)

                file_name = os.path.basename(merged_output_path)
                print(f"File name: {file_name}")

                try:

                    with open(merged_output_path, 'rb') as pdf_file:
                        pdf_data = pdf_file.read()

                    response = make_response(pdf_data)
                    response.headers.set('Content-Type', 'application/pdf')

                    download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_pdf?file={merged_output_path}'
                    preview_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/preview_pdf?file={merged_output_path}'

                    print('The CWH DIRECT DELIVERY  PDF GENERATED successfully and response sent ...')
                    print(
                        "***************************************************************************************************")

                    return jsonify(
                        pdf=response.data.decode('latin-1'),
                        preview_link=preview_link,
                        download_link=download_link,
                        status="success",
                        fileName=file_name,
                        message="success"
                    )
                except FileNotFoundError:
                    message = "PDF not found."
                    print(message)
                    return jsonify(status="failed", message=message), 404

            return jsonify(status="failed", message="failed")

        elif del_type == "Both" and screen_num == "2":
            pdf_file_path_list: list = []
            (cwh_data_status, df_list, other_total, disposal_total, cleaning_total, food_total, total_amount,
             nested_list, formatted_date, delivery_type, cwh_data_message) = fetch_cwh_details_by_ind_loc(month, year,
                                                                                                          loc_id)
            if cwh_data_status == "failed":
                return jsonify(status=cwh_data_status, message=cwh_data_message)
            elif cwh_data_status == "success" and cwh_data_message == 'No data available':
                # return jsonify(status=status, message=message)
                pass
            else:
                (cwh_pdf_status, output_path, merged_file_name) = create_cwh_pdf_for_ind_loc(df_list, other_total,
                                                                                             disposal_total,
                                                                                             cleaning_total, food_total,
                                                                                             total_amount,
                                                                                             nested_list,
                                                                                             formatted_date,
                                                                                             delivery_type)
                if cwh_pdf_status == "success":
                    pdf_file_path_list.append(output_path)
                else:
                    print(cwh_pdf_status)
                    pass
            (
                direct_delivery_data_status, df_list, other_total, disposal_total, cleaning_total, food_total,
                total_amount,
                nested_list, formatted_date, delivery_type,
                direct_delivery_message) = fetch_direct_delivery_details_for_ind_loc(month, year,
                                                                                     loc_id)

            if direct_delivery_data_status == "failed":
                return jsonify(status=direct_delivery_data_status, message=direct_delivery_message)
            elif direct_delivery_data_status == "success" and direct_delivery_message == 'No data available':
                # return jsonify(status=status, message=message)
                pass
            elif cwh_data_status == "success" and direct_delivery_data_status == "success" and direct_delivery_message == 'No data available' and cwh_data_message == "No data available":
                status = "success"
                return jsonify(status=status, message=direct_delivery_message)
            else:
                (direct_delivery_pdf_status, output_path, merged_file_name) = create_cwh_pdf_for_ind_loc(df_list,
                                                                                                         other_total,
                                                                                                         disposal_total,
                                                                                                         cleaning_total,
                                                                                                         food_total,
                                                                                                         total_amount,
                                                                                                         nested_list,
                                                                                                         formatted_date,
                                                                                                         delivery_type)
                if direct_delivery_pdf_status == "success":
                    pdf_file_path_list.append(output_path)
                else:
                    print(direct_delivery_pdf_status)
                    pass
            if len(pdf_file_path_list) != 0:
                output_folder = r'C:\Users\Administrator\eiis_pdf\pythonProject\PDFfile\cwh\merged_pdf'
                current_datetime = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
                merged_file_name = f"CWH_DIRECT_DELIVERY_{current_datetime}.pdf"
                output_path = os.path.join(output_folder, merged_file_name)

                merge_status, merged_output_path = merge_pdfs(pdf_file_path_list, output_path)
                if merge_status != "success":
                    message = "failed"
                    return jsonify(status=merge_status, message=message)

                file_name = os.path.basename(merged_output_path)
                print(f"File name: {file_name}")

                try:

                    with open(merged_output_path, 'rb') as pdf_file:
                        pdf_data = pdf_file.read()

                    response = make_response(pdf_data)
                    response.headers.set('Content-Type', 'application/pdf')

                    download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_pdf?file={merged_output_path}'
                    preview_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/preview_pdf?file={merged_output_path}'

                    print('The CWH DIRECT DELIVERY  PDF GENERATED successfully and response sent ...')
                    print(
                        "***************************************************************************************************")

                    return jsonify(
                        pdf=response.data.decode('latin-1'),
                        preview_link=preview_link,
                        download_link=download_link,
                        status="success",
                        fileName=file_name,
                        message="success"
                    )
                except FileNotFoundError:
                    message = "PDF not found."
                    print(message)
                    return jsonify(status="failed", message=message), 404

            return jsonify(status="failed", message="failed")

        else:
            return jsonify(status='failed', message='No variables passed')

        if status == "failed":
            return jsonify(status=status, message=message)
        elif status == "success" and message == 'No data available':
            return jsonify(status=status, message=message)
        else:
            (status, output_path, merged_file_name) = create_cwh_pdf_for_ind_loc(df_list, other_total, disposal_total,
                                                                                 cleaning_total, food_total,
                                                                                 total_amount,
                                                                                 nested_list,
                                                                                 formatted_date, delivery_type)
        if status == "success" and output_path is not None:

            try:

                # Read the PDF file data
                with open(output_path, 'rb') as pdf_file:
                    pdf_data = pdf_file.read()

                # Create a response with the PDF data and set custom headers
                response = make_response(pdf_data)
                response.headers.set('Content-Type', 'application/pdf')

                # Generate download link
                download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_pdf?file={output_path}'
                preview_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/preview_pdf?file={output_path}'
                print('PDF file ready for download')
                return jsonify(pdf=response.data.decode('latin-1'),
                               download_link=download_link,
                               preview_link=preview_link,
                               status=status,
                               fileName=merged_file_name,
                               message='success')

            except FileNotFoundError:
                status = "failed"
                print("PDF file not found.")
                return jsonify(status=status, message='PDF file not found.'), 404
        else:
            status = "failed"
            return jsonify(status=status, message='failed')


@app.route('/user_rights_excel_report_api', methods=['GET', 'POST'])
def user_rights_excel_report_api():
    if request.method == 'POST':
        status, message, file_path, file_name = fetch_and_create_user_rights_excel()
        if message == 'No data available' or message == 'failed':
            return jsonify(status=status, message=message)
        else:
            if status == "success" and message == "success":
                try:
                    # Generate download link
                    download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_excel?file={file_path}'
                    print("Excel file ready for download")
                    return jsonify(file_path=file_path,
                                   download_link=download_link,
                                   status=status,
                                   fileName=file_name,
                                   message=message)

                except FileNotFoundError:
                    status = "failed"
                    print("Excel file not found.")
                    return jsonify(status=status, message='Excel file not found.'), 404
    else:
        return jsonify(status="Method not allowed. Only POST requests are allowed."), 405


# method to preview the PDF
@app.route('/preview_pdf', methods=['GET'])
def preview_pdf():
    file_path = request.args.get('file')
    if file_path:
        try:
            with open(file_path, 'rb') as pdf_file:
                pdf_data = pdf_file.read()
            response = make_response(pdf_data)
            response.headers.set('Content-Type', 'application/pdf')
            return response
        except FileNotFoundError:
            return "PDF not found.", 404
    else:
        return "File path not provided.", 400


# Method to download the PDF
@app.route('/download_pdf', methods=['GET'])
def download_pdf():
    file_path = request.args.get('file')
    if file_path:
        try:
            # Extract the original filename from the file path
            original_filename = os.path.basename(file_path)

            with open(file_path, 'rb') as pdf_file:
                pdf_data = pdf_file.read()

            response = make_response(pdf_data)
            response.headers.set('Content-Type', 'application/pdf')

            # Set the original filename in the Content-Disposition header
            response.headers.set('Content-Disposition', f'attachment; filename="{original_filename}"')

            return response
        except FileNotFoundError:
            return "PDF not found.", 404
    else:
        return "File path not provided.", 400


# Method to download the Excel
@app.route('/download_excel')
def download_excel():
    file_path = request.args.get('file')
    if file_path:
        try:
            # Extract the original file name from the path
            original_filename = os.path.basename(file_path)
            # Send the file with the original file name as the download name
            return send_file(file_path, as_attachment=True, download_name=original_filename)
        except Exception as e:
            print(e)
            return jsonify(status="failed"), 404
    else:
        return jsonify(status="failed"), 400


def create_app():
    print('scheduler is running ...')
    # Initialize scheduler
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=call_stored_procedure_1, trigger="cron", hour=1, minute=30)
    scheduler.start()

    # Shut down the scheduler when exiting the app
    atexit.register(lambda: scheduler.shutdown())

    return app


app = create_app()


@app.route('/purchase_price_analysis_excel_report_api_latest', methods=['GET', 'POST'])
def purchase_price_analysis_excel_report_latest():
    if request.method == 'POST':

        print('CALLING [purchase_price_analysis_excel_report_api_latest] API ...')
        from_date = request.form.get('from_date')
        to_date = request.form.get('to_date')
        excel_type = str(request.form.get('excel_type'))
        print(f'From Date : {from_date}, To Date : {to_date}, Excel Type : {excel_type}')

        concatenated_df, desired_order, data_message, data_status = get_data_for_excel(from_date, to_date, excel_type)

        if data_status == "success" and data_message == "success":
            report_status, report_message, file_name, file_path = create_purchase_price_excel_report(concatenated_df,
                                                                                                     desired_order,
                                                                                                     excel_type)
            if report_status == "success":
                try:
                    # Generate download link
                    download_link = f'http://{IP_ADDRESS}:{PORT_NUMBER}/download_excel?file={file_path}'
                    print('Excel file ready for download')
                    return jsonify(file_path=file_path,
                                   download_link=download_link,
                                   status=report_status,
                                   fileName=file_name,
                                   message=report_message)

                except FileNotFoundError:
                    status = "failed"
                    print("Excel file not found.")
                    return jsonify(status=status, message='Excel file not found.'), 404
            else:
                status = "failed"
                return jsonify(status=status, message=status)
        elif data_message == "No data available" and data_status == "success":
            return jsonify(status=data_status, message=data_message)
        else:
            return jsonify(status=data_status, message=data_message)
    else:
        return jsonify(status="Method not allowed. Only POST requests are allowed."), 405


serve(app, host='0.0.0.0', port=5001, threads=1000)  # WAITRESS!
