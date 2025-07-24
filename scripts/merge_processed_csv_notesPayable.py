import pandas as pd
import os
import re
import shutil 
from datetime import datetime 
import json 

# 設定項目
APP_ROOT_DIR = r'C:\Users\User26\yoko\dev\csvRead_notesPayable'

# 加工済みファイルがあるフォルダ (process_data_notesPayable.pyが出力する場所)
PROCESSED_OUTPUT_BASE_DIR = os.path.join(APP_ROOT_DIR, 'processed_output') 
# マージ済みファイルを保存するフォルダ
MERGED_OUTPUT_BASE_DIR = os.path.join(APP_ROOT_DIR, 'merged_output') 
# マスタデータフォルダ（ocr_id_mapping_notesPayable.json が保存されている場所）
MASTER_DATA_DIR = os.path.join(APP_ROOT_DIR, 'master_data')

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


def merge_processed_csv_files():
    """
    processed_output フォルダ内の加工済みCSVファイルをファイルグループごとに結合し、
    merged_output フォルダに保存する関数。
    """
    print(f"--- ファイルグループごとの結合処理開始 ({datetime.now()}) ---")
    print(f"加工済みファイルフォルダ: {PROCESSED_OUTPUT_BASE_DIR}")
    print(f"結合済みファイル出力フォルダ: {MERGED_OUTPUT_BASE_DIR}")

    # 結合済みファイル出力フォルダが存在しない場合は作成
    os.makedirs(MERGED_OUTPUT_BASE_DIR, exist_ok=True)

    files_to_merge_by_group = {}
    
    # processed_output フォルダ内を再帰的に検索
    for root, dirs, files in os.walk(PROCESSED_OUTPUT_BASE_DIR): 
        for filename in files:
            # '_processed.csv' で終わるファイルのみを対象とする
            if filename.lower().endswith('_processed.csv'):
                # ★★★ 支払手形アプリのファイル名パターン (B*080_processed.csv) で抽出 ★★★
                match = re.match(r'^(B\d{6})_.*\.jpg_080_processed\.csv$', filename, re.IGNORECASE)
                if match:
                    group_root_name = match.group(1) # 例: B000304
                    page_num_str = re.search(r'_(\d+)\.jpg_080_processed\.csv$', filename, re.IGNORECASE)
                    page_num = int(page_num_str.group(1)) if page_num_str else 1 # ページ番号を抽出、なければ1
                    filepath = os.path.join(root, filename)

                    if group_root_name not in files_to_merge_by_group:
                        files_to_merge_by_group[group_root_name] = []
                    files_to_merge_by_group[group_root_name].append((page_num, filepath))
                else:
                    print(f"  ℹ️ マージ対象外のファイル形式 (パターン不一致): {filename}")

    merged_files_count = 0
    # ファイルグループのルート名でソートして、結合順を保証
    sorted_merged_groups = sorted(files_to_merge_by_group.keys())
    
    # ocr_id_mapping_notesPayable.json を読み込む
    ocr_id_map_filepath = os.path.join(MASTER_DATA_DIR, 'ocr_id_mapping_notesPayable.json')
    ocr_id_mapping_from_file = {}
    try:
        if os.path.exists(ocr_id_map_filepath):
            with open(ocr_id_map_filepath, 'r', encoding='utf-8') as f:
                json_content = f.read()
                if json_content: # ファイルが空でないことを確認
                    ocr_id_mapping_from_file = json.loads(json_content)
                else:
                    print(f"  ⚠️ 警告: ocr_id_mapping_notesPayable.json が空です。")
            print(f"  ✅ ocr_id_mapping_notesPayable.json を {ocr_id_map_filepath} から読み込みました。")
        else:
            print(f"  ⚠️ 警告: ocr_id_mapping_notesPayable.json が見つかりません。OCR IDの整合性チェックに影響する可能性があります。")
    except Exception as e:
        print(f"❌ エラー: ocr_id_mapping_notesPayable.json の読み込みに失敗しました。エラー: {e}")

    for group_root_name in sorted_merged_groups: 
        page_files = files_to_merge_by_group[group_root_name]
        if not page_files: 
            continue 
        
        # このグループの ocr_result_id, cif_number, jgroupid_string の「期待値」を設定
        expected_ocr_id_for_group = ocr_id_mapping_from_file.get(group_root_name) 
        expected_cif_number_for_group = group_root_name[1:] 
        expected_jgroupid_string_for_group = '001' 
        
        combined_df = pd.DataFrame(columns=FINAL_POSTGRE_COLUMNS) 
        
        print(f"  → グループ '{group_root_name}' のファイルを結合中 (期待OCR ID: {expected_ocr_id_for_group})...")
        
        global_id_counter = 1 

        # ページ番号でソート
        page_files.sort(key=lambda x: x[0])

        for page_index, (page_num, filepath) in enumerate(page_files):
            try:
                # _processed.csv のヘッダーを読み込む
                df_page = pd.read_csv(filepath, encoding='utf-8-sig', dtype=str, header=0, na_values=['〃'], keep_default_na=False)
                
                if df_page.empty: 
                    print(f"    ℹ️ {os.path.basename(filepath)} は空のためスキップします。")
                    continue

                # カラム順を FINAL_POSTGRE_COLUMNS に合わせる
                if list(df_page.columns) != FINAL_POSTGRE_COLUMNS:
                    df_page = df_page.reindex(columns=FINAL_POSTGRE_COLUMNS).fillna('') 

                # ID情報の強制上書き
                df_page['ocr_result_id'] = expected_ocr_id_for_group
                df_page['cif_number'] = expected_cif_number_for_group
                df_page['jgroupid_string'] = expected_jgroupid_string_for_group

                # 'id' は全体の連番に振り直す
                df_page['id'] = range(global_id_counter, global_id_counter + len(df_page))
                global_id_counter += len(df_page) 

                df_page['page_no'] = 1 # page_no は全て1固定 (支払手形も同じ要件と仮定)
                
                combined_df = pd.concat([combined_df, df_page], ignore_index=True)
                print(f"    - ページ {page_num} ({os.path.basename(filepath)}) を結合しました。")
            except Exception as e:
                print(f"  ❌ エラー: ページ {page_num} のファイル {os.path.basename(filepath)} の読み込み/結合中に問題が発生しました。エラー: {e}")
                import traceback 
                traceback.print_exc() 
                combined_df = pd.concat([combined_df, pd.DataFrame(columns=FINAL_POSTGRE_COLUMNS)], ignore_index=True)


        # 結合されたDataFrameを新しいフォルダに保存
        merged_output_filename = f"{group_root_name}_merged.csv" 
        merged_output_filepath = os.path.join(MERGED_OUTPUT_BASE_DIR, merged_output_filename)
        
        # 古いファイルを削除するロジック（_processed_merged.csv形式）
        old_filename_pattern = f"{group_root_name}_processed_merged.csv" 
        old_filepath = os.path.join(MERGED_OUTPUT_BASE_DIR, old_filename_pattern)
        if os.path.exists(old_filepath):
            try:
                os.remove(old_filepath)
                print(f"  ✅ 古いファイル '{old_filename_pattern}' を削除しました。")
            except Exception as e:
                print(f"  ❌ エラー: 古いファイル '{old_filename_pattern}' の削除中に問題が発生しました。エラー: {e}")

        try:
            if not combined_df.empty: 
                # ヘッダーなしで保存 (PostgreSQL COPYコマンド向け)
                combined_df.to_csv(merged_output_filepath, index=False, encoding='utf-8-sig', header=False) 
                merged_files_count += 1
                print(f"  ✅ グループ '{group_root_name}' の結合ファイルを保存しました: {merged_output_filepath}")
            else:
                print(f"  ⚠️ 警告: グループ '{group_root_name}' に結合対象の有効なデータが見つからなかったため、ファイルは保存されません。")
        except Exception as e:
            print(f"  ❌ エラー: グループ '{group_root_name}' の結合ファイルの保存中に問題が発生しました。エラー: {e}")

    print(f"\n--- ファイルグループごとの結合処理完了 ({datetime.now()}) ---")
    print(f"🎉 結合されたファイルグループ数: {merged_files_count} 🎉")

# --- メイン処理 ---
if __name__ == "__main__":
    print(f"--- 結合処理スクリプト開始: {datetime.now()} ---")
    merge_processed_csv_files()
    print(f"\n🎉 全ての結合処理が完了しました！ ({datetime.now()}) 🎉")
    