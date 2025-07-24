import pandas as pd
import os
import re
from datetime import datetime
import random 
import shutil
import numpy as np 
import json # ocr_id_mapping の保存/読み込みに必要

# --- 設定項目（ここだけ、くまちゃんの環境に合わせて修正してね！） ---
# フィルタリング済みファイルが保存されているフォルダ（支払手形用）
INPUT_BASE_DIR = r'G:\共有ドライブ\商工中金\202412_勘定科目明細本番稼働\50_検証\010_反対勘定性能評価\20_テストデータ\作成ワーク\30_支払手形\Import' 
# アプリのルートディレクトリ（例: C:\Users\User26\yoko\dev\csvRead_notesPayable）
APP_ROOT_DIR = r'C:\Users\User26\yoko\dev\csvRead_notesPayable'
# 加工後のCSVファイルを保存するフォルダ
PROCESSED_OUTPUT_BASE_DIR = os.path.join(APP_ROOT_DIR, 'processed_output') 
# マスタデータファイルが保存されているフォルダ
MASTER_DATA_DIR = os.path.join(APP_ROOT_DIR, 'master_data')

# ★★★ 支払手形用のFINAL_POSTGRE_COLUMNS を設計書通りに定義！ ★★★
# お客様から提示された全70個のカラムを正確に反映します。
FINAL_POSTGRE_COLUMNS = [
    "ocr_result_id", "page_no", "id", "jgroupid_string", "cif_number", "settlement_at",
    "registration_number_original", "registration_number", "payee_name_original", "payee_name",
    "payee_com_code", "payee_com_code_status_id", "payee_comcd_relation_source_type_id",
    "payee_exist_comcd_relation_history_id", "issue_date_original", "issue_date",
    "due_date_original", "due_date", "balance_original", "balance",
    "paying_bank_name_original", "paying_bank_name", "paying_bank_name_code",
    "paying_bank_branch_name", "description_original", "description",
    "conf_registration_number", "conf_payee_name", "conf_issue_date", "conf_due_date",
    "conf_balance", "conf_paying_bank_name", "conf_paying_bank_branch_name",
    "conf_description",
    "coord_x_registration_number", "coord_y_registration_number", "coord_h_registration_number", "coord_w_registration_number",
    "coord_x_payee_name", "coord_y_payee_name", "coord_h_payee_name", "coord_w_payee_name",
    "coord_x_issue_date", "coord_y_issue_date", "coord_h_issue_date", "coord_w_issue_date",
    "coord_x_due_date", "coord_y_due_date", "coord_h_due_date", "coord_w_due_date",
    "coord_x_balance", "coord_y_balance", "coord_h_balance", "coord_w_balance",
    "coord_x_paying_bank_name", "coord_y_paying_bank_name", "coord_h_paying_bank_name", "coord_w_paying_bank_name",
    "coord_x_paying_bank_branch_name", "coord_y_paying_bank_branch_name", "coord_h_paying_bank_branch_name", "coord_w_paying_bank_branch_name",
    "coord_x_description", "coord_y_description", "coord_h_description", "coord_w_description",
    "row_no", "insertdatetime", "updatedatetime", "updateuser"
]


# --- 各CSVファイル形式ごとのマッピングルールを定義 ---
# ★★★ 支払手形アプリ (B*080.csv) のマッピングを定義 ★★★
NOTES_PAYABLE_MAPPING_DICT = {
    'payee_name': '支払先', 
    'issue_date': '振出年月日', 
    'due_date': '支払期日', 
    'balance': '金額', 
    'paying_bank_name': '支払銀行名称', 
    'paying_bank_branch_name': '支払銀行支店名', 
    'description': '摘要', 
}

# その他のマッピング辞書は、このアプリで他のファイルタイプを処理する場合に備えて維持
# 現状、支払手形アプリはB*080.csvのみを想定
FINANCIAL_STATEMENT_MAPPING_DICT = {} 
LOAN_DETAILS_MAPPING_DICT = {} 

# ★★★ NO_HEADER_MAPPING_DICT をタブ区切りデータの「デフォルト」として再定義！（支払手形向けに調整） ★★★
# 支払手形のタブ区切りデータの具体的な列順が不明なため、受取手形のNO_HEADER_MAPPING_DICTをベースに、
# FINAL_POSTGRE_COLUMNSの順序と一般的なOCR出力の列順を考慮し、仮のインデックスを設定。
# お客様の実際のB*080.csvデータに合わせて、以下のインデックスを正確に調整する必要があります！
NO_HEADER_MAPPING_DICT = {
    'ocr_result_id': 0, 'page_no': 1, 'id': 2, 'jgroupid_string': 3, 'cif_number': 4, 'settlement_at': 5,
    'payee_name_original': 6, 'payee_name': 7, 'payee_com_code': 8,      
    'issue_date_original': 9, 'issue_date': 10, 'due_date_original': 11, 'due_date': 12,            
    'paying_bank_name_original': 15, 'paying_bank_name': 15,          
    'paying_bank_branch_name_original': 16, # originalなし -> originalを追加して調整
    'paying_bank_branch_name': 16,   
    'description_original': 21, 'description': 22,                 
}


# --- 関数定義 ---
def clean_balance_no_comma(value): 
    try:
        cleaned_value = str(value).replace(',', '').replace('¥', '').replace('￥', '').replace('円', '').strip() 
        if not cleaned_value:
            return '' 
        numeric_value = float(cleaned_value)
        return str(int(numeric_value)) 
    except ValueError:
        return '' 

ocr_id_mapping = {}
_ocr_id_sequence_counter = 0 
_ocr_id_fixed_timestamp_str = "" 

def get_ocr_result_id_for_group(file_group_root_name): 
    global ocr_id_mapping
    global _ocr_id_sequence_counter
    global _ocr_id_fixed_timestamp_str

    if file_group_root_name not in ocr_id_mapping:
        sequence_part_int = _ocr_id_sequence_counter * 10
        if sequence_part_int > 99999: 
            sequence_part_int = sequence_part_int % 100000 
        
        sequence_part_str = str(sequence_part_int).zfill(5) 
        
        new_ocr_id = f"{_ocr_id_fixed_timestamp_str}{sequence_part_str}" 

        ocr_id_mapping[file_group_root_name] = new_ocr_id
        _ocr_id_sequence_counter += 1
    
    return ocr_id_mapping[file_group_root_name]

# payee_com_code 用に調整
payee_name_to_com_code_map = {} 
next_payee_com_code_val = 100 

def get_payee_com_code_for_name(payee_name): 
    """
    payee_nameに基づいて3桁の会社コードを採番・取得し、先頭に '8' を付けて4桁にする。
    同じpayee_nameには同じコードを割り当てる。
    """
    global payee_name_to_com_code_map 
    global next_payee_com_code_val 

    payee_name_str = str(payee_name).strip() 
    
    if not payee_name_str: 
        return "" 

    if payee_name_str in payee_name_to_com_code_map:
        return payee_name_to_com_code_map[payee_name_str]
    else:
        new_code_int = next_payee_com_code_val % 1000 
        if new_code_int < 100: 
            new_code_int = 100 + new_code_int 
        
        new_code_4digit = '8' + str(new_code_int).zfill(3) # ★★★ 頭に '8' を付ける ★★★
        
        payee_name_to_com_code_map[payee_name_str] = new_code_4digit 
        next_payee_com_code_val += 1
        return new_code_4digit

def is_likely_amount_column(series):
    """金額らしい列かどうかを判定する関数"""
    if not pd.api.types.is_string_dtype(series): 
        series = series.astype(str)
    
    cleaned_series = series.dropna().astype(str).str.replace(r'[¥￥,円\s　]', '', regex=True)
    
    if cleaned_series.empty:
        return False 

    patterns = [r'^\d{1,3}(,\d{3})*(\.\d+)?$', r'^\d+円$', r'^[\d,]+$', r'^\d+\.\d{2}$', r'^[+-]?\d+$'] 
    
    match_count = 0
    for val in cleaned_series:
        if any(re.fullmatch(p, val) for p in patterns): 
            match_count += 1
    
    return match_count >= max(1, len(cleaned_series) * 0.5) 

def detect_amount_column_index(df):
    """DataFrameから金額列のインデックスを特定する"""
    potential_amount_cols = []
    for i in range(df.shape[1] -1, -1, -1): 
        col = df.columns[i]
        if is_likely_amount_column(df[col]):
            numeric_values = df[col].astype(str).str.replace(r'[¥￥,円\s　]', '', regex=True).apply(lambda x: pd.to_numeric(x, errors='coerce'))
            if not numeric_values.isnull().all(): 
                potential_amount_cols.append((i, numeric_values.sum())) 
    
    if not potential_amount_cols:
        return -1 

    potential_amount_cols.sort(key=lambda x: x[1], reverse=True)
    return potential_amount_cols[0][0] 


def process_universal_csv(input_filepath, processed_output_base_dir, input_base_dir, 
                        master_df, ocr_id_map_for_groups, current_file_group_root_name, 
                        final_postgre_columns_list, notes_payable_map, no_header_map):
    """
    全てのAIRead出力CSVファイルを読み込み、統一されたPostgreSQL向けカラム形式に変換して出力する関数。
    CSVの種類（ヘッダー内容）を判別し、それぞれに応じたマッピングを適用する。
    """
    df_original = None
    file_type = "不明" 
    
    try:
        encodings_to_try = ['utf-8', 'utf-8-sig', 'shift_jis', 'cp932']
        
        for enc in encodings_to_try:
            try:
                # 1. ヘッダーあり、カンマ区切りで読み込みを試す (支払手形など一般的なCSV形式)
                df_temp_comma_header = pd.read_csv(input_filepath, encoding=enc, header=0, sep=',', quotechar='"', 
                                                    dtype=str, na_values=['〃'], keep_default_na=False)
                df_temp_comma_header.columns = df_temp_comma_header.columns.str.strip() 
                current_headers_comma = df_temp_comma_header.columns.tolist()

                # ★★★ 支払手形ファイルのヘッダー判別条件に調整 ★★★
                is_notes_payable = ('支払先' in current_headers_comma) and ('金額' in current_headers_comma)
                
                if is_notes_payable:
                    df_original = df_temp_comma_header.copy()
                    file_type = "支払手形情報"
                else: 
                    # 2. ヘッダーなし、タブ区切りで読み込みを試す (汎用データ_ヘッダーなしの可能性が高い)
                    try:
                        df_temp_tab_noheader = pd.read_csv(input_filepath, encoding=enc, header=None, sep='\t', quotechar='"', 
                                                        dtype=str, na_values=['〃'], keep_default_na=False)
                        df_temp_tab_noheader.columns = df_temp_tab_noheader.columns.astype(str).str.strip()
                        
                        # 汎用データと判定する基準: タブ区切りで読み込めて、かつある程度の列数があること
                        max_idx_no_header_map = max(no_header_map.values()) if no_header_map else 0
                        
                        if df_temp_tab_noheader.shape[1] > max_idx_no_header_map: 
                            file_type = "汎用データ_ヘッダーなし"
                            df_original = df_temp_tab_noheader.copy()
                        else: # 列数が少ない場合は、カンマ区切りヘッダーなしの再試行へ
                            raise ValueError("タブ区切りデータが期待する列数に満たない") 

                    except Exception as e_tab:
                        # 3. タブ区切りでも失敗したら、カンマ区切り、ヘッダーなしで再試行（最終フォールバック）
                        print(f"  ファイル {os.path.basename(input_filepath)} を {enc} でタブ区切り読み込み失敗。カンマ区切りを試します。エラー: {e_tab}")
                        file_type = "汎用データ_ヘッダーなし" 
                        df_original = pd.read_csv(input_filepath, encoding=enc, header=None, sep=',', quotechar='"', 
                                                dtype=str, na_values=['〃'], keep_default_na=False)
                        df_original.columns = df_original.columns.astype(str).str.strip() 
                
                print(f"  デバッグ: ファイル {os.path.basename(input_filepath)} の判定結果: '{file_type}'")
                print(f"  デバッグ: 読み込んだ df_original のカラム:\n{df_original.columns.tolist()}")
                print(f"  デバッグ: 読み込んだ df_original の最初の3行:\n{df_original.head(3).to_string()}") 
                print(f"  デバッグ: df_original内の欠損値 (NaN) の数:\n{df_original.isnull().sum().to_string()}") 
                    
                break 
            except Exception as e_inner: 
                print(f"  ファイル {os.path.basename(input_filepath)} を {enc} で読み込み失敗。別のエンコーディングを試します。エラー: {e_inner}")
                df_original = None 
                continue 

        if df_original is None or df_original.empty:
            print(f"  警告: ファイル {os.path.basename(input_filepath)} をどのエンコーディングとヘッダー設定でも読み込めませんでした。処理をスキップします。")
            return 
        
        print(f"  ファイル {os.path.basename(input_filepath)} は '{file_type}' として処理します。")

    except Exception as e:
        print(f"❌ エラー発生（{input_filepath}）: CSV読み込みまたはファイルタイプ判別で問題が発生しました。エラー: {e}")
        import traceback
        traceback.print_exc()
        return

    # --- データ加工処理 ---
    df_data_rows = df_original.copy() 

    if df_data_rows.empty:
        print(f"  警告: ファイル {os.path.basename(input_filepath)} に有効なデータ行が見つからなかったため、加工をスキップします。")
        return 

    # 「〃」マークのみをffillで埋め、空文字列はそのまま維持
    df_data_rows = df_data_rows.ffill() 
    df_data_rows = df_data_rows.fillna('') 
    print(f"  ℹ️ 「〃」マークを直上データで埋め、元々ブランクだった箇所は維持しました。")

    # 合計行の削除ロジック
    keywords_to_delete = ["合計", "小計", "計"] 
    
    filter_conditions = []
    keywords_regex = r'|'.join([re.escape(k) for k in keywords_to_delete]) 
    
    # ★★★ 合計行削除の対象カラムを支払手形に合わせて調整 ★★★
    if file_type == "支払手形情報":
        if '支払先' in df_data_rows.columns: 
            filter_conditions.append(df_data_rows['支払先'].str.contains(keywords_regex, regex=True, na=False))
    elif file_type == "汎用データ_ヘッダーなし": 
        # '汎用データ_ヘッダーなし' の場合、 payee_name は NO_HEADER_MAPPING_DICT の payee_name に対応する列からデータを見る
        if 'payee_name' in no_header_map and str(no_header_map['payee_name']) in df_data_rows.columns: 
            filter_conditions.append(df_data_rows[str(no_header_map['payee_name'])].str.contains(keywords_regex, regex=True, na=False))
        elif '0' in df_data_rows.columns: # 最悪0列目全体でチェック
            filter_conditions.append(df_data_rows['0'].str.contains(keywords_regex, regex=True, na=False))

    if filter_conditions:
        combined_filter = pd.concat(filter_conditions, axis=1).any(axis=1)
        rows_deleted_count = combined_filter.sum()
        df_data_rows = df_data_rows[~combined_filter].reset_index(drop=True)
        if rows_deleted_count > 0:
            print(f"  ℹ️ 合計行（キーワードパターン: {keywords_regex}）を {rows_deleted_count} 行削除しました。")
    
    num_rows_to_process = len(df_data_rows) 
    
    # df_processed の初期化
    df_processed = pd.DataFrame('', index=range(num_rows_to_process), columns=final_postgre_columns_list)


    # --- 共通項目 (PostgreSQLのグリーンの表の左側、自動生成項目) を生成 ---
    df_processed['ocr_result_id'] = [get_ocr_result_id_for_group(current_file_group_root_name)] * num_rows_to_process 

    df_processed['page_no'] = [1] * num_rows_to_process 

    df_processed['id'] = range(1, num_rows_to_process + 1)

    df_processed['jgroupid_string'] = ['001'] * num_rows_to_process

    cif_number_val = current_file_group_root_name[1:] 
    df_processed['cif_number'] = [cif_number_val] * num_rows_to_process

    settlement_at_val = datetime.now().strftime('%Y%m') 
    df_processed['settlement_at'] = [settlement_at_val] * num_rows_to_process


    # --- 各ファイルタイプに応じたマッピングルールを適用 ---
    mapping_to_use = {}
    if file_type == "支払手形情報": 
        mapping_to_use = NOTES_PAYABLE_MAPPING_DICT 
    elif file_type == "汎用データ_ヘッダーなし": 
        mapping_to_use = NO_HEADER_MAPPING_DICT 
    # FINANCIAL_STATEMENT_MAPPING_DICT, LOAN_DETAILS_MAPPING_DICT は使用しない

    df_data_rows.columns = df_data_rows.columns.astype(str) # 念のためstrに変換
    
    # マッピング処理：元のCSVデータをPostgreSQLカラムにコピー（「★今のまま」に対応）
    for col_name_in_original_df in df_data_rows.columns: 
        if col_name_in_original_df in final_postgre_columns_list:
            df_processed[col_name_in_original_df] = df_data_rows[col_name_in_original_df].copy()

    # マッピング辞書（NOTES_PAYABLE_MAPPING_DICT または NO_HEADER_MAPPING_DICT）を適用する
    for pg_col_name, src_ref in mapping_to_use.items():
        if df_processed[pg_col_name].isin(['', None, np.nan]).all(): 
            source_data_series = None
            if isinstance(src_ref, str): 
                if src_ref in df_data_rows.columns: 
                    source_data_series = df_data_rows[src_ref]
            elif isinstance(src_ref, int): 
                if str(src_ref) in df_data_rows.columns: 
                    source_data_series = df_data_rows[str(src_ref)]
            
            if source_data_series is not None:
                df_processed[pg_col_name] = source_data_series.astype(str).values 
            else:
                pass 


    # ★★★ 金額カラムの動的検出ロジックを追加！ ★★★
    # 汎用データの場合のみ、金額を自動検出して埋める
    if file_type == "汎用データ_ヘッダーなし" or file_type == "支払手形情報": 
        amount_col_idx = detect_amount_column_index(df_data_rows)
        if amount_col_idx != -1:
            raw_balance_series = df_data_rows.iloc[:, amount_col_idx].astype(str) 
            
            df_processed['balance'] = raw_balance_series.apply(clean_balance_no_comma) 
            df_processed['balance_original'] = df_processed['balance'].copy() 
            print(f"  ℹ️ 金額カラムを列インデックス '{amount_col_idx}' から動的に検出しました。")
        else:
            print("  ⚠️ 警告: 金額カラムを動的に検出できませんでした。balanceカラムはブランクのままです。")


    # --- Excel関数相当のロジックを適用（派生カラムの生成） ---
    # ★★★ 各カラムの生成ロジックを設計書に忠実に再現する！ ★★★
    
    # 登録・法人番号 (ブランク)
    df_processed['registration_number_original'] = '' 
    df_processed['registration_number'] = '' 

    # 支払先 (payee_name) 
    df_processed['payee_name_original'] = df_processed['payee_name'].copy() 
    
    # 支払先企業コード (payee_com_code) は頭に '8' を付ける
    df_processed['payee_com_code'] = df_processed['payee_name'].apply(get_payee_com_code_for_name)
    
    # 支払先企業コードステータスIDなど (固定値)
    df_processed['payee_com_code_status_id'] = '30'
    df_processed['payee_comcd_relation_source_type_id'] = '30'
    df_processed['payee_exist_comcd_relation_history_id'] = '20'

    # 振出年月日 (issue_date), 支払期日 (due_date)
    df_processed['issue_date_original'] = df_processed['issue_date'].copy() 
    df_processed['due_date_original'] = df_processed['due_date'].copy()   

    # 支払銀行名称, 支払銀行支店名
    df_processed['paying_bank_name_original'] = df_processed['paying_bank_name'].copy() 
    df_processed['paying_bank_branch_name'] = df_processed['paying_bank_branch_name'].copy() # originalがないため

    # 支払銀行金融機関コード (paying_bank_name_code) はブランク
    df_processed['paying_bank_name_code'] = '' 
    
    # 摘要 (description)
    df_processed['description_original'] = df_processed['description'].copy() 
    
    # 信頼値 (conf_系) (固定値)
    df_processed['conf_registration_number'] = '100'
    df_processed['conf_payee_name'] = '100' 
    df_processed['conf_issue_date'] = '100'
    df_processed['conf_due_date'] = '100'
    df_processed['conf_balance'] = '100'
    df_processed['conf_paying_bank_name'] = '100'
    df_processed['conf_paying_bank_branch_name'] = '100'
    df_processed['conf_description'] = '100'

    # 座標 (coord_系) (固定値)
    df_processed['coord_x_registration_number'] = '3000'
    df_processed['coord_y_registration_number'] = '3000'
    df_processed['coord_h_registration_number'] = '3000'
    df_processed['coord_w_registration_number'] = '3000'
    df_processed['coord_x_payee_name'] = '3000' 
    df_processed['coord_y_payee_name'] = '3000' 
    df_processed['coord_h_payee_name'] = '3000' 
    df_processed['coord_w_payee_name'] = '3000' 
    df_processed['coord_x_issue_date'] = '3000'
    df_processed['coord_y_issue_date'] = '3000'
    df_processed['coord_h_issue_date'] = '3000'
    df_processed['coord_w_issue_date'] = '3000'
    df_processed['coord_x_due_date'] = '3000'
    df_processed['coord_y_due_date'] = '3000'
    df_processed['coord_h_due_date'] = '3000'
    df_processed['coord_w_due_date'] = '3000'
    df_processed['coord_x_balance'] = '3000'
    df_processed['coord_y_balance'] = '3000'
    df_processed['coord_h_balance'] = '3000'
    df_processed['coord_w_balance'] = '3000'
    df_processed['coord_x_paying_bank_name'] = '3000'
    df_processed['coord_y_paying_bank_name'] = '3000'
    df_processed['coord_h_paying_bank_name'] = '3000'
    df_processed['coord_w_paying_bank_name'] = '3000'
    df_processed['coord_x_paying_bank_branch_name'] = '3000'
    df_processed['coord_y_paying_bank_branch_name'] = '3000'
    df_processed['coord_h_paying_bank_branch_name'] = '3000'
    df_processed['coord_w_paying_bank_branch_name'] = '3000'
    df_processed['coord_x_description'] = '3000'
    df_processed['coord_y_description'] = '3000'
    df_processed['coord_h_description'] = '3000'
    df_processed['coord_w_description'] = '3000'

    df_processed['row_no'] = range(1, num_rows_to_process + 1) 
    df_processed['insertdatetime'] = '' 
    df_processed['updatedatetime'] = '' 
    df_processed['updateuser'] = 'testuser' 
    
    # ★★★ 最終的なカンマ除去処理を、保存直前で確実に実行 ★★★
    # ここで balance_original と balance の両方からカンマなどを除去し、数値形式に整えます。
    NUMERIC_COLUMNS_TO_CLEAN = ["balance_original", "balance"]
    for col in NUMERIC_COLUMNS_TO_CLEAN:
        if col in df_processed.columns:
            df_processed[col] = df_processed[col].astype(str).apply(clean_balance_no_comma)

    # --- 保存処理 ---
    relative_path_to_file = os.path.relpath(input_filepath, input_base_dir)
    relative_dir_to_file = os.path.dirname(relative_path_to_file)
    processed_output_sub_dir = os.path.join(processed_output_base_dir, relative_dir_to_file)
    
    processed_output_filename = os.path.basename(input_filepath).replace('.csv', '_processed.csv')
    processed_output_filepath = os.path.join(processed_output_sub_dir, processed_output_filename) 
    
    os.makedirs(processed_output_sub_dir, exist_ok=True) 
    df_processed.to_csv(processed_output_filepath, index=False, encoding='utf-8-sig')

    print(f"✅ 加工完了: {input_filepath} -> {processed_output_filepath}")

# --- メイン処理 ---
if __name__ == "__main__":
    print(f"--- 処理開始: {datetime.now()} ({APP_ROOT_DIR}) ---") 
    
    _ocr_id_fixed_timestamp_str = datetime.now().strftime('%Y%m%d%H%M')
    print(f"  ℹ️ OCR ID生成の固定時刻: {_ocr_id_fixed_timestamp_str}")

    os.makedirs(PROCESSED_OUTPUT_BASE_DIR, exist_ok=True) 

    MASTER_DATA_DIR = os.path.join(APP_ROOT_DIR, 'master_data') 
    os.makedirs(MASTER_DATA_DIR, exist_ok=True) 

    payee_master_filepath = os.path.join(MASTER_DATA_DIR, 'master.csv') 
    payee_master_df = pd.DataFrame() 
    if os.path.exists(payee_master_filepath):
        try:
            payee_master_df = pd.read_csv(payee_master_filepath, encoding='utf-8')
            print(f"  ℹ️ {payee_master_filepath} を読み込みました (このデータはpayee_com_code生成に利用されます)。")
        except Exception as e:
            print(f"❌ エラー: {payee_master_filepath} の読み込みに失敗しました。エラー: {e}")
            payee_master_df = pd.DataFrame() 
    else:
        print(f"⚠️ 警告: {payee_master_filepath} が見つかりません (payee_com_code生成に影響する可能性があります)。")
        payee_master_df = pd.DataFrame() 


    jgroupid_master_filepath = os.path.join(MASTER_DATA_DIR, 'jgroupid_master.csv')
    jgroupid_values_from_master = [] 
    if os.path.exists(jgroupid_master_filepath): 
        try:
            df_jgroupid_temp = pd.read_csv(jgroupid_master_filepath, encoding='utf-8', header=None)
            
            if not df_jgroupid_temp.empty and df_jgroupid_temp.shape[1] > 0:
                jgroupid_values_from_master = df_jgroupid_temp.iloc[:, 0].astype(str).tolist()
                if not jgroupid_values_from_master:
                    raise ValueError("jgroupid_master.csv からデータを読み込めましたが、リストが空です。")
            else:
                raise ValueError("jgroupid_master.csv が空またはデータがありません。")
            
        except Exception as e:
            print(f"❌ エラー: jgroupid_master.csv の読み込みに失敗しました。エラー: {e}")
            jgroupid_values_from_master = [] 
    else:
        print(f"⚠️ 警告: {jgroupid_master_filepath} が見つかりません。パスを確認してください。")
        jgroupid_values_from_master = [str(i).zfill(3) for i in range(1, 94)] # デフォルト値で初期化しておく

    INPUT_CSV_FILES_DIR = INPUT_BASE_DIR 

    # ocr_result_id のマッピングを事前に生成するロジック
    print("\n--- ocr_result_id マッピング事前生成開始 ---")
    ocr_id_mapping = {}
    _ocr_id_sequence_counter = 0 
    
    all_target_file_groups_root = set() 
    for root, dirs, files in os.walk(INPUT_CSV_FILES_DIR): 
        for filename in files:
            if filename.lower().endswith('.csv') and not filename.lower().endswith('_processed.csv'):
                # ファイル名から「ファイルグループのルート名」を抽出 (BXXXXXX)
                # INPUT_CSV_FILES_DIR には B*080.csv のみが存在すると仮定
                match = re.match(r'^(B\d{6})_.*\.jpg_080\.csv$', filename, re.IGNORECASE) 
                
                if match: # パターンに合致した場合のみ処理
                    all_target_file_groups_root.add(match.group(1)) 
                else:
                    print(f"  ℹ️ ocr_result_id生成対象外のファイル名: {filename} (パターン不一致)。")
                    
    sorted_file_groups_root = sorted(list(all_target_file_groups_root)) 
    
    for group_root_name in sorted_file_groups_root:
        get_ocr_result_id_for_group(group_root_name) 
    
    print("--- ocr_result_id マッピング事前生成完了 ---")
    print(f"生成された ocr_id_mapping (最初の5つ): {list(ocr_id_mapping.items())[:5]}...")

    # 生成した ocr_id_mapping をファイルに保存
    ocr_id_map_filepath = os.path.join(MASTER_DATA_DIR, 'ocr_id_mapping_notesPayable.json') # 支払手形用にファイル名変更
    try:
        with open(ocr_id_map_filepath, 'w', encoding='utf-8') as f:
            json.dump(ocr_id_mapping, f, ensure_ascii=False, indent=4)
        print(f"  ✅ ocr_id_mapping を {ocr_id_map_filepath} に保存しました。")
    except Exception as e:
            print(f"❌ エラー: ocr_id_mapping の保存に失敗しました。エラー: {e}")

    # メインのファイル処理ループ
    for root, dirs, files in os.walk(INPUT_CSV_FILES_DIR): 
        for filename in files:
            if filename.lower().endswith('.csv') and not filename.lower().endswith('_processed.csv'): 
                input_filepath = os.path.join(root, filename)
                print(f"\n--- 処理対象ファイル: {input_filepath} ---")

                current_file_group_root_name = None
                # ファイル名から「ファイルグループのルート名」を抽出 (BXXXXXX)
                # INPUT_CSV_FILES_DIR には B*080.csv のみが存在すると仮定
                match = re.match(r'^(B\d{6})_.*\.jpg_080\.csv$', filename, re.IGNORECASE) 
                
                if match: # パターンに合致した場合のみ処理
                    current_file_group_root_name = match.group(1) 
                
                if current_file_group_root_name is None:
                    print(f"  ⚠️ 警告: ファイル {filename} のファイルグループのルート名を特定できませんでした。このファイルはスキップします。")
                    continue 

                process_universal_csv(input_filepath, PROCESSED_OUTPUT_BASE_DIR, INPUT_CSV_FILES_DIR, 
                                    payee_master_df, ocr_id_mapping, current_file_group_root_name, # maker_master_df -> payee_master_df
                                    FINAL_POSTGRE_COLUMNS, NO_HEADER_MAPPING_DICT, NOTES_PAYABLE_MAPPING_DICT) 

    print(f"\n🎉 全てのファイルの加工処理が完了しました！ ({datetime.now()}) 🎉")
    