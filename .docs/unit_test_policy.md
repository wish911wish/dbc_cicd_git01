# Delta Live Tables 単体テスト方針書

## 1. 目的

本方針書は、Delta Live Tables（DLT）パイプラインにおける単体テストの実施方針を定義し、コード品質の確保と開発効率の向上を目的とする。

## 2. 適用範囲

- DLTパイプラインで使用するすべてのデータ変換ロジック
- Pythonモジュールとして実装された関数群
- Bronze/Silver/Gold各レイヤーの変換処理

## 3. 基本方針

### 3.1 コード構造化の原則

単体テストを効果的に実施するため、以下のコード構造を採用する。

| 区分 | 配置場所 | 役割 |
|------|----------|------|
| DLTパイプラインコード | `dlt_pipelines/` | DLT実行グラフの定義、データの読み込みと変換関数の呼び出し |
| 変換ロジック | `src/<package_name>/` | DataFrameを受け取り、変換処理を行い、DataFrameを返す純粋な関数 |
| 単体テスト（ローカル） | `tests/unit-local/` | ローカル環境で実行可能なpytestベースのテスト |
| 単体テスト（Notebook） | `tests/unit-notebooks/` | Databricks上で実行するNotebookベースのテスト |

### 3.2 変換ロジックの分離

DLT関数から変換ロジックを分離し、独立したPythonモジュールとして実装する。

**推奨パターン:**

```python
# src/transformations/silver_transforms.py
def transform_to_silver(df: DataFrame) -> DataFrame:
    """Bronze層からSilver層への変換処理"""
    return df.filter(col("type").isin(["A", "B", "C"])) \
             .withColumn("processed_date", current_date())
```

```python
# dlt_pipelines/pipeline.py
import dlt
from transformations.silver_transforms import transform_to_silver

@dlt.table
def silver_table():
    bronze_df = dlt.read("bronze_table")
    return transform_to_silver(bronze_df)
```

この分離により、DLTコードはシンプルになり、変換ロジックは独立してテスト可能となる。

## 4. テスト実装方式

### 4.1 ローカル実行方式（推奨）

pytestを使用してローカル環境またはCI/CD環境で実行する方式。

**利点:**
- IDE上での開発・デバッグが容易
- CI/CDパイプライン内でDatabricksリソースなしで実行可能
- 静的解析・コードカバレッジツールとの連携が可能
- インタラクティブデバッグが可能

**実装例:**

```python
# tests/unit-local/test_silver_transforms.py
import pytest
from pyspark.sql import SparkSession
from transformations.silver_transforms import transform_to_silver

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("unit-tests") \
        .getOrCreate()

def test_transform_to_silver_filters_valid_types(spark):
    # Arrange
    input_data = [
        {"id": 1, "type": "A", "value": 100},
        {"id": 2, "type": "X", "value": 200},  # 除外対象
        {"id": 3, "type": "B", "value": 300},
    ]
    input_df = spark.createDataFrame(input_data)
    
    # Act
    result_df = transform_to_silver(input_df)
    
    # Assert
    assert result_df.count() == 2
    types = [row.type for row in result_df.collect()]
    assert "X" not in types
```

### 4.2 Notebook実行方式

Databricks Notebook上でテストを定義し、Nutterフレームワークで実行する方式。

**利点:**
- Databricks環境での即座のフィードバック
- サンプルデータを使ったインタラクティブなテスト開発
- 実際のSpark環境での動作確認

**実装例:**

```python
# tests/unit-notebooks/test_silver_transforms
from runtime.nutterfixture import NutterFixture
from transformations.silver_transforms import transform_to_silver

class TestSilverTransforms(NutterFixture):
    
    def before_test_filter_valid_types(self):
        self.input_df = spark.createDataFrame([
            {"id": 1, "type": "A", "value": 100},
            {"id": 2, "type": "X", "value": 200},
        ])
    
    def run_test_filter_valid_types(self):
        self.result_df = transform_to_silver(self.input_df)
    
    def assertion_test_filter_valid_types(self):
        assert self.result_df.count() == 1

result = TestSilverTransforms().execute_tests()
```

## 5. テスト観点

### 5.1 必須テスト項目

| テスト観点 | 内容 | 優先度 |
|------------|------|--------|
| 正常系 | 期待される入力に対して正しい出力が得られること | 高 |
| フィルタリング | 条件に合致するデータのみが抽出されること | 高 |
| データ型変換 | カラムのデータ型が正しく変換されること | 高 |
| NULL処理 | NULL値が適切に処理されること | 高 |
| 境界値 | 境界条件でのデータが正しく処理されること | 中 |
| 空データ | 入力が空の場合にエラーなく処理されること | 中 |
| 重複処理 | 重複データが仕様通りに処理されること | 中 |

### 5.2 テストデータ設計

- テストデータは各テストケースで明示的に定義する
- 本番データのサンプルではなく、テスト目的に合わせた最小限のデータセットを使用する
- エッジケースを網羅するデータを含める

## 6. 実行環境

### 6.1 ローカル実行環境

| 項目 | 要件 |
|------|------|
| Python | 3.9以上（Databricksランタイムと合わせる） |
| PySpark | Databricksランタイムと同一バージョン |
| pytest | 7.0以上 |
| 依存管理 | Poetry または pip + requirements.txt |

### 6.2 CI/CD実行環境

- CI/CDパイプライン内でPySpark環境を構築して実行
- Databricksリソースは不要（ローカル実行方式の場合）
- テスト結果はJUnit形式で出力し、CI/CDシステムに連携

## 7. 命名規則

| 対象 | 規則 | 例 |
|------|------|-----|
| テストファイル | `test_<モジュール名>.py` | `test_silver_transforms.py` |
| テスト関数 | `test_<機能>_<条件>` | `test_filter_valid_types_excludes_invalid` |
| テストクラス | `Test<対象クラス/モジュール>` | `TestSilverTransforms` |

## 8. カバレッジ目標

| メトリクス | 目標値 |
|------------|--------|
| ライン カバレッジ | 80%以上 |
| ブランチ カバレッジ | 70%以上 |
| 変換関数のカバレッジ | 100% |

## 9. CI/CDとの統合

### 9.1 実行タイミング

| イベント | 実行内容 |
|----------|----------|
| プッシュ（全ブランチ） | ローカル単体テスト + Notebook単体テスト |
| プルリクエスト | 上記 + コードカバレッジレポート |
| リリースブランチへのマージ | 上記 + 結合テスト |

### 9.2 パイプライン構成例（Azure DevOps）

```yaml
stages:
  - stage: UnitTests
    jobs:
      - job: LocalTests
        steps:
          - script: |
              pip install poetry
              poetry install
              poetry run pytest tests/unit-local --junitxml=test-results.xml
          - task: PublishTestResults@2
            inputs:
              testResultsFiles: 'test-results.xml'
      
      - job: NotebookTests
        steps:
          - script: |
              databricks repos update --path /Repos/staging/project --branch $BUILD_BRANCH
              nutter run /Repos/staging/project/tests/unit-notebooks
```

## 10. 成果物

| 成果物 | 形式 | 保管場所 |
|--------|------|----------|
| テスト結果 | JUnit XML | CI/CDシステム |
| カバレッジレポート | HTML/XML | CI/CDシステム |
| テストログ | テキスト | CI/CDシステム |

## 改訂履歴

| 版 | 日付 | 改訂内容 |
|----|------|----------|
| 1.0 | 2025-01-27 | 初版作成 |
