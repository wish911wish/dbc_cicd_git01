# Delta Live Tables 結合テスト方針書

## 1. 目的

本方針書は、Delta Live Tables（DLT）パイプライン全体の動作を検証する結合テストの実施方針を定義し、パイプラインの品質保証とリリース判定の基準を明確にすることを目的とする。

## 2. 適用範囲

- DLTパイプライン全体（Bronze → Silver → Gold）
- パイプライン間のデータ連携
- データ品質検証

## 3. 基本方針

### 3.1 結合テストの実装方式

DLTの結合テストには以下の2つの方式があり、**方式B（DLT Expectations方式）を推奨**する。

| 方式 | 概要 | 推奨度 |
|------|------|--------|
| 方式A: Databricks Workflows | 複数タスクで構成（データ準備→パイプライン実行→結果検証） | △ |
| 方式B: DLT Expectations | DLT Expectationsを使用してパイプライン内で検証 | ◎（推奨） |

### 3.2 方式B（DLT Expectations）を推奨する理由

1. **追加のコンピュートリソースが不要** - すべての検証がDLTパイプライン内で完結
2. **補助コードの削減** - セットアップ・検証タスク用の追加コードが不要
3. **クラスタ再利用** - 同一DLTクラスタ内で実行されるため効率的
4. **統一されたログ** - 検証結果がDLTイベントログに記録される
5. **シンプルな構成** - パイプライン定義に検証テーブルを追加するだけ

## 4. DLT Expectationsによる結合テスト実装

### 4.1 アーキテクチャ

結合テスト用のDLTパイプラインは、本番パイプラインのコードに検証用テーブルを追加した構成とする。

```
[Bronze Table] → [Silver Table] → [Gold Table]
                      ↓                 ↓
              [Silver検証Table]   [Gold検証Table]
              (Expectations)      (Expectations)
```

### 4.2 検証テーブルの実装パターン

#### パターン1: 許可値の検証

特定カラムの値が許可リストに含まれることを検証する。

```python
@dlt.table
def silver_validation():
    return dlt.read("silver_table")

dlt.expect_all_or_fail({
    "valid_type": "type IN ('A', 'B', 'C')",
    "positive_amount": "amount > 0",
    "non_null_id": "id IS NOT NULL"
})
```

#### パターン2: レコード件数の検証

テーブルのレコード件数が期待範囲内であることを検証する。

```python
@dlt.table
def record_count_validation():
    df = dlt.read("silver_table")
    count = df.count()
    return spark.createDataFrame([(count,)], ["record_count"])

dlt.expect_all_or_fail({
    "minimum_records": "record_count >= 100",
    "maximum_records": "record_count <= 1000000"
})
```

#### パターン3: 一意性の検証

主キーの一意性を検証する。

```python
@dlt.table
def uniqueness_validation():
    df = dlt.read("silver_table")
    return df.groupBy("id").count().filter("count > 1")

dlt.expect_all_or_fail({
    "no_duplicates": "count IS NULL"  # 重複があればレコードが存在する
})
```

#### パターン4: 参照整合性の検証

テーブル間の参照整合性を検証する。

```python
@dlt.table
def referential_integrity_validation():
    silver_df = dlt.read("silver_table")
    master_df = dlt.read("master_table")
    
    orphans = silver_df.join(
        master_df,
        silver_df.master_id == master_df.id,
        "left_anti"
    )
    return orphans

dlt.expect_all_or_fail({
    "all_references_valid": "master_id IS NULL"  # 孤立レコードがなければ成功
})
```

#### パターン5: 集計値の検証

集計結果が期待値と一致することを検証する。

```python
@dlt.table
def aggregation_validation():
    df = dlt.read("gold_summary_table")
    return df.filter("report_date = current_date()")

dlt.expect_all_or_fail({
    "total_amount_positive": "total_amount > 0",
    "record_count_reasonable": "record_count BETWEEN 1000 AND 100000"
})
```

### 4.3 パイプライン構成

結合テスト用パイプラインは、本番用とは別に定義する。

| パイプライン | 用途 | ノートブック構成 |
|--------------|------|------------------|
| 本番パイプライン | 本番データ処理 | `pipeline.py` |
| 結合テストパイプライン | テスト実行 | `pipeline.py` + `validation.py` |

**Terraform定義例:**

```hcl
resource "databricks_pipeline" "integration_test" {
  name        = "dlt-pipeline-integration-test"
  target      = "test_schema"
  development = true
  
  library {
    notebook {
      path = "/Repos/staging/project/dlt_pipelines/pipeline.py"
    }
  }
  
  library {
    notebook {
      path = "/Repos/staging/project/tests/integration/validation.py"
    }
  }
  
  configuration = {
    "source_path" = "/test-data/"
  }
}
```

## 5. テストデータ管理

### 5.1 テストデータの要件

| 要件 | 内容 |
|------|------|
| データ量 | 本番の1-10%程度、または固定のサンプルデータセット |
| データ品質 | 正常系・異常系を含む網羅的なデータ |
| 独立性 | 本番データとは完全に分離された環境 |
| 再現性 | 毎回同一の結果が得られるデータセット |

### 5.2 テストデータの配置

```
/test-data/
├── bronze/
│   └── sample_input.parquet
├── master/
│   └── master_data.parquet
└── expected/
    └── expected_output.parquet
```

## 6. テスト観点

### 6.1 必須検証項目

| 検証観点 | 内容 | 実装方法 |
|----------|------|----------|
| データ型整合性 | 各カラムのデータ型が正しいこと | スキーマ検証 |
| NOT NULL制約 | 必須カラムにNULLがないこと | `expect_all_or_fail` |
| 値域検証 | 値が許可範囲内であること | `expect_all_or_fail` |
| 一意性 | 主キーが一意であること | 集計による検証 |
| 参照整合性 | 外部キーが有効であること | JOINによる検証 |
| レコード件数 | 期待件数の範囲内であること | COUNT検証 |
| 集計値整合性 | 集計値が期待値と一致すること | 集計検証 |

### 6.2 オプション検証項目

| 検証観点 | 内容 |
|----------|------|
| 処理時間 | パイプラインが許容時間内に完了すること |
| リソース使用量 | メモリ・CPU使用量が許容範囲内であること |
| 増分処理 | 増分データのみが正しく処理されること |

## 7. 実行環境

### 7.1 環境構成

| 環境 | 用途 | データソース |
|------|------|--------------|
| 開発環境 | 開発者による個別テスト | テストデータ |
| ステージング環境 | CI/CDによる自動テスト | テストデータ |
| 本番環境 | 本番運用 | 本番データ |

### 7.2 環境分離

- Unity Catalogを使用してカタログ/スキーマレベルで分離
- 各環境で異なるストレージロケーションを使用
- 環境固有の設定はパイプライン設定で管理

```python
# パイプライン設定による環境分離
source_path = spark.conf.get("source_path", "/default-path/")
target_schema = spark.conf.get("target_schema", "default_schema")
```

## 8. CI/CDとの統合

### 8.1 実行タイミング

| イベント | 結合テスト実行 |
|----------|----------------|
| 機能ブランチへのプッシュ | 実行しない（単体テストのみ） |
| プルリクエスト | オプション（レビュアー判断） |
| リリースブランチへのマージ | **必須** |
| 本番デプロイ前 | **必須** |

### 8.2 パイプライン構成例（Azure DevOps）

```yaml
stages:
  - stage: IntegrationTest
    condition: eq(variables['Build.SourceBranch'], 'refs/heads/releases')
    jobs:
      - job: RunDLTPipeline
        steps:
          # ステージング環境のReposを更新
          - script: |
              databricks repos update \
                --path /Repos/staging/project \
                --branch $(Build.SourceBranchName)
            displayName: 'Update Staging Repos'
          
          # 結合テストパイプラインを実行
          - script: |
              PIPELINE_ID=$(databricks pipelines list \
                --filter "name LIKE 'dlt-pipeline-integration-test'" \
                --output json | jq -r '.[0].pipeline_id')
              
              UPDATE_ID=$(databricks pipelines start-update \
                --pipeline-id $PIPELINE_ID \
                --full-refresh \
                --output json | jq -r '.update_id')
              
              # 完了を待機
              while true; do
                STATUS=$(databricks pipelines get-update \
                  --pipeline-id $PIPELINE_ID \
                  --update-id $UPDATE_ID \
                  --output json | jq -r '.update.state')
                
                if [ "$STATUS" = "COMPLETED" ]; then
                  echo "Pipeline completed successfully"
                  exit 0
                elif [ "$STATUS" = "FAILED" ]; then
                  echo "Pipeline failed"
                  exit 1
                fi
                sleep 30
              done
            displayName: 'Run Integration Test Pipeline'
```

## 9. 結果判定

### 9.1 合格基準

| 基準 | 条件 |
|------|------|
| パイプライン完了 | すべてのテーブルが正常に更新されること |
| Expectations合格 | すべてのExpectationsがパスすること |
| エラーなし | パイプラインログにERRORレベルのログがないこと |

### 9.2 結果確認方法

DLTイベントログから検証結果を取得する。

```sql
SELECT
  timestamp,
  details:flow_name AS table_name,
  details:expectations AS expectations
FROM event_log(TABLE(test_catalog.test_schema.pipeline_events))
WHERE event_type = 'flow_progress'
  AND details:expectations IS NOT NULL
ORDER BY timestamp DESC;
```

## 10. 障害対応

### 10.1 テスト失敗時の対応フロー

1. **イベントログ確認** - DLTイベントログで失敗したExpectationを特定
2. **データ確認** - 失敗の原因となったデータを調査
3. **根本原因分析** - コードの問題かデータの問題かを判断
4. **修正・再テスト** - 修正後、パイプラインを再実行

### 10.2 よくある失敗パターンと対処

| 失敗パターン | 考えられる原因 | 対処方法 |
|--------------|----------------|----------|
| Expectation失敗 | データ品質問題、変換ロジックのバグ | ログを確認し原因を特定 |
| タイムアウト | データ量過多、クラスタサイズ不足 | クラスタ設定を見直し |
| スキーマエラー | 上流データの変更 | スキーマ進化の設定を確認 |

## 11. 成果物

| 成果物 | 内容 | 保管場所 |
|--------|------|----------|
| パイプライン実行ログ | DLTイベントログ | Delta Lake（システムテーブル） |
| テスト結果サマリ | 合否判定結果 | CI/CDシステム |
| データ品質レポート | Expectationsの詳細結果 | DLTイベントログ |

## 12. 参考情報

### 12.1 DLT Expectationsの種類

| 関数 | 動作 | 用途 |
|------|------|------|
| `expect` | 違反レコードを記録、処理は継続 | モニタリング |
| `expect_or_drop` | 違反レコードを除外、処理は継続 | データクレンジング |
| `expect_or_fail` | 違反があればパイプライン停止 | **結合テスト（推奨）** |
| `expect_all_or_fail` | 複数条件を一括定義、違反で停止 | **結合テスト（推奨）** |

### 12.2 関連ドキュメント

- [DLT Expectations公式ドキュメント](https://docs.databricks.com/delta-live-tables/expectations.html)
- [DLT イベントログ](https://docs.databricks.com/delta-live-tables/observability.html)
- [サンプルリポジトリ](https://github.com/alexott/dlt-files-in-repos-demo)

## 改訂履歴

| 版 | 日付 | 改訂内容 |
|----|------|----------|
| 1.0 | 2025-01-27 | 初版作成 |
