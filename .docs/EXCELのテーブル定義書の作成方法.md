
# コード例

```
import pandas as pd
from datetime import datetime
import os

%pip install openpyxl

catalog = 'dbc_cicd'
schema = 'wish911wish'


def generate_table_definition(catalog, schema, table_name):
    # 基本情報取得
    desc_df = spark.sql(f"DESCRIBE EXTENDED {catalog}.{schema}.{table_name}").toPandas()
    
    # カラム情報のみ抽出（詳細情報の手前まで）
    col_end_idx = desc_df[desc_df['col_name'] == ''].index[0] if '' in desc_df['col_name'].values else len(desc_df)
    columns = desc_df.iloc[:col_end_idx]

    # ディレクトリ作成
    output_dir = "/Workspace/Users/wish911wish@gmail.com/table_definitions"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{table_name}_definition.xlsx")


    # Excelに出力
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        columns.to_excel(writer, sheet_name='カラム定義', index=False)
        desc_df.iloc[col_end_idx:].to_excel(writer, sheet_name='テーブル詳細', index=False)
        
    print(f"✓ {table_name} の定義書を生成しました")
    print(f"   {output_path}")

    return columns


# 複数テーブルの一括生成
tables = spark.sql(f"""
    SELECT table_name 
    FROM system.information_schema.tables 
    WHERE table_catalog = '{catalog}' 
    AND table_schema = '{schema}'
""").toPandas()

for table in tables['table_name']:
    generate_table_definition(catalog, schema, table)

```
