import os
import shutil
import re
from datetime import datetime

# 設定項目
# 探すフォルダ（共有ドライブ上の元のCSVファイルがある場所）
INPUT_BASE_DIR = r'G:\共有ドライブ\VLM-OCR\20_教師データ\30_output_csv'

# 保存フォルダ（フィルタリング結果をコピーする場所）
# 受取手形アプリのパスと明確に区別できるように新しいパスを指定
OUTPUT_BASE_DIR = r'G:\共有ドライブ\商工中金\202412_勘定科目明細本番稼働\50_検証\010_反対勘定性能評価\20_テストデータ\作成ワーク\30_支払手形\Import'

# アプリのルートディレクトリ（任意、ログ出力などに使用）
APP_ROOT_DIR = r'C:\Users\User26\yoko\dev\csvRead_notesPayable'

# --- 関数定義 ---
def filter_and_copy_csv_files():
    
    # B*080.csvを抽出
    print(f"--- フィルタリング＆コピー処理開始 ({datetime.now()}) ---")
    print(f"検索元フォルダ: {INPUT_BASE_DIR}")
    print(f"コピー先フォルダ: {OUTPUT_BASE_DIR}")

    # コピー先フォルダが存在しない場合は作成
    os.makedirs(OUTPUT_BASE_DIR, exist_ok=True)

    copied_files_count = 0
    skipped_files_count = 0

    # INPUT_BASE_DIR 以下を再帰的に探索
    for root, dirs, files in os.walk(INPUT_BASE_DIR):
        for filename in files:
            # CSVファイルであり、かつ加工済みファイルではないことを確認
            if filename.lower().endswith('.csv') and not filename.lower().endswith('_processed.csv'):
                input_filepath = os.path.join(root, filename)

                # ★★★ ここがフィルタリング条件！Bから始まって080で終わるファイルだけ！ ★★★
                # 例: B000001_1.jpg_080.csv
                match = re.match(r'^B\d{6}_.*\.jpg_080\.csv$', filename, re.IGNORECASE)

                if match:
                    # コピー先のパスを生成（元のフォルダ構造は引き継がずフラットに保存）
                    output_filepath = os.path.join(OUTPUT_BASE_DIR, filename)
                    
                    try:
                        shutil.copy2(input_filepath, output_filepath)
                        copied_files_count += 1
                        print(f"  ✅ コピー成功: {filename}")
                    except Exception as e:
                        print(f"  ❌ コピー失敗: {filename} -> エラー: {e}")
                else:
                    skipped_files_count += 1
                    print(f"  ℹ️ スキップ: {filename} (条件に一致しません)")

    print(f"\n--- フィルタリング＆コピー処理完了 ({datetime.now()}) ---")
    print(f"🎉 コピーされたファイル数: {copied_files_count} 🎉")
    print(f"スキップされたファイル数: {skipped_files_count}")

# --- メイン処理 ---
if __name__ == "__main__":
    # 新しいプロジェクトのルートフォルダが存在しない場合は作成（ログ出力パスのため）
    os.makedirs(APP_ROOT_DIR, exist_ok=True) 
    filter_and_copy_csv_files()
    