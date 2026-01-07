
# YAMLファイルからEXCELの出力

```

import yaml
import pandas as pd
import os

%pip install openpyxl

def yaml_to_excel_simple(yaml_path, excel_path):
    """YAMLファイルの内容をExcelに変換（シンプル版）"""
    
    # YAML読み込み
    with open(yaml_path, 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f)
    
    # 出力ディレクトリ作成
    os.makedirs(os.path.dirname(excel_path), exist_ok=True)
    
    # Excelに書き込み
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        # YAMLを平坦化してDataFrame化
        flattened = flatten_yaml(data)
        df = pd.DataFrame(list(flattened.items()), columns=['キー', '値'])
        df.to_excel(writer, sheet_name='全体', index=False)
    
    print(f"✓ Excelファイルを生成: {excel_path}")

def flatten_yaml(data, parent_key='', sep='.'):
    """ネストされたYAMLを平坦化"""
    items = []
    if isinstance(data, dict):
        for k, v in data.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_yaml(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                items.append((new_key, str(v)))
            else:
                items.append((new_key, v))
    else:
        items.append((parent_key, data))
    return dict(items)

# 実行例
yaml_path = '/Workspace/Users/wish911wish@gmail.com/dbc_cicd_git01/cc_git_abc_pipeline_bundle/databricks.yml'
output_dir = '/Workspace/Users/wish911wish@gmail.com/.docs'
excel_path = f"{output_dir}/databricks_config.xlsx"
yaml_to_excel_simple(yaml_path, excel_path)


```
