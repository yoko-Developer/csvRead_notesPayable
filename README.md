# csvRead_notesPayable

## **テーブル名: bills_payable(支払手形)**

AIReadで出力したcsvファイルをPostgreSQLに取り込めるように加工するアプリケーションです

## 実行方法

### カレントディレクトリ

`C:\Users\User26\yoko\dev\csvRead_notesPayable`

### 実行コマンド

- 検索結果: **B*080.csv**に絞り込み

    ```
    python filter_and_copy_csvs_notesPayable.py
    ```

- 加工ファイル作成

    ```
    python process_data_notesPayable.py
    ```

- ファイルをマージ

    ```
    python merge_processed_csv_notesPayable.py
    ```

- データベースにcsvを登録

    ```
    python insert_to_postgres.py
    ```

### ファイル格納ディレクトリ

★ 読取フォルダ(元データ)

`G:\共有ドライブ\VLM-OCR\20_教師データ\30_output_csv`

★ 出力先フォルダ

- 絞り込み結果

    `G:\共有ドライブ\商工中金\202412_勘定科目明細本番稼働\50_検証\010_反対勘定性能評価\20_テストデータ\作成ワーク\30_支払手形\Import`

- 加工済み

    `C:\Users\User26\yoko\dev\csvRead_notesPayable\processed_output`

- マージ済み

    `C:\Users\User26\yoko\dev\csvRead_notesPayable\merged_output`

## レイヤー構成
```
ccsv_notesPayable/
├── master_data/
│   ├── master.csv
│   └── jgroupid_master.csv
├── processed_output/                           // _080_processed.csvが生成される
├── merged_output/                              // _merged.csvが生成される
└── scripts/
    ├── filter_and_copy_csvs_notesPayable.py    //  検索＆コピーを担当
    ├── process_data_notesPayable.py            //  加工を担当
    ├── merge_processed_csv_notesPayable.py     //  マージ（結合）を担当
    └── insert_to_postgres.py                   //  DB登録を担当
```


## ルール

### カラム

- **ocr_result_id**: ファイルごとに一意

    最後0で18桁にする `yyyymmddhhmmsssss0`

- **page_no**: 全て`1`で固定

- **id**: ファイルごとに連番

- **jgroupid_string(店番)**: 全て`001`で固定

- **cif_number(顧客番号)**: ファイルごとに一意

    ファイル番号の数字部分6桁(B000050->000050)

- **payee_name_original**,**payee_name(支払先)**: ランダム

- **payee_com_code(TBC)**: 頭に`8`を追加した3桁の自動採番でカウントアップ
    支払先に紐づく(payee_nameが同じならpayee_com_codeも同じ)


## SQL文

```
-- 古いテーブルとインデックスがあれば削除
DROP INDEX IF EXISTS idx_jgroupid_string;
DROP TABLE IF EXISTS bills_payable;

-- テーブル再作成
CREATE TABLE bills_payable (
    ocr_result_id CHAR(18) NOT NULL,
    page_no INTEGER NOT NULL,
    id INTEGER NOT NULL,
    jgroupid_string VARCHAR(3),
    cif_number VARCHAR(7),
    settlement_at VARCHAR(6),
    registration_number_original TEXT,
    registration_number TEXT,
    payee_name_original TEXT,
    payee_name TEXT,
    payee_com_code TEXT,
    payee_com_code_status_id INTEGER,
    payee_comcd_relation_source_type_id INTEGER,
    payee_exist_comcd_relation_history_id INTEGER,
    issue_date_original TEXT,
    issue_date TEXT,
    due_date_original TEXT,
    due_date TEXT,
    balance_original NUMERIC,
    balance NUMERIC,
    paying_bank_name_original TEXT,
    paying_bank_name TEXT,
    paying_bank_name_code TEXT,
    paying_bank_branch_name TEXT,
    description_original TEXT,
    description TEXT,
    conf_registration_number INTEGER,
    conf_payee_name INTEGER,
    conf_issue_date INTEGER,
    conf_due_date INTEGER,
    conf_balance INTEGER,
    conf_paying_bank_name INTEGER,
    conf_paying_bank_branch_name INTEGER,
    conf_description INTEGER,
    coord_x_registration_number NUMERIC,
    coord_y_registration_number NUMERIC,
    coord_h_registration_number NUMERIC,
    coord_w_registration_number NUMERIC,
    coord_x_payee_name NUMERIC,
    coord_y_payee_name NUMERIC,
    coord_h_payee_name NUMERIC,
    coord_w_payee_name NUMERIC,
    coord_x_issue_date NUMERIC,
    coord_y_issue_date NUMERIC,
    coord_h_issue_date NUMERIC,
    coord_w_issue_date NUMERIC,
    coord_x_due_date NUMERIC,
    coord_y_due_date NUMERIC,
    coord_h_due_date NUMERIC,
    coord_w_due_date NUMERIC,
    coord_x_balance NUMERIC,
    coord_y_balance NUMERIC,
    coord_h_balance NUMERIC,
    coord_w_balance NUMERIC,
    coord_x_paying_bank_name NUMERIC,
    coord_y_paying_bank_name NUMERIC,
    coord_h_paying_bank_name NUMERIC,
    coord_w_paying_bank_name NUMERIC,
    coord_x_paying_bank_branch_name NUMERIC,
    coord_y_paying_bank_branch_name NUMERIC,
    coord_h_paying_bank_branch_name NUMERIC,
    coord_w_paying_bank_branch_name NUMERIC,
    coord_x_description NUMERIC,
    coord_y_description NUMERIC,
    coord_h_description NUMERIC,
    coord_w_description NUMERIC,
    row_no SMALLINT,
    insertdatetime TIMESTAMP,
    updatedatetime TIMESTAMP,
    updateuser TEXT,
    PRIMARY KEY (ocr_result_id, page_no, id)
);

-- インデックス再作成
CREATE INDEX idx_jgroupid_string ON bills_payable(jgroupid_string);
```

```
-- インデックス作成
CREATE INDEX idx_bills_payable_jgroupid_string ON bills_payable (jgroupid_string);
CREATE INDEX idx_bills_payable_cif_number ON bills_payable (cif_number);
CREATE INDEX idx_bills_payable_settlement_at ON bills_payable (settlement_at);
```
