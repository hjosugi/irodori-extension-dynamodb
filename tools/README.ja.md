<!-- i18n: language-switcher -->
[English](README.md) | [日本語](README.ja.md)

# コネクタメタデータ

`connector.source.json`は各拡張機能の人間が編集可能な真実の源です。
`connector.config.json`および`irodori.extension.json`は、互換性を保つために生成されたパッケージングアーティファクトであり、現在のネイティブABIとマーケットプレイスのレイアウトに対応しています。

共有のコネクタメタデータジェネレーターは、`irodori-table`コーディネーターリポジトリにあります。
この拡張リポジトリは、生成されたアーティファクトとローカルREADMEヘルパーのみを保持しています。

## コマンド

生成されたコネクタメタデータから英語のREADMEファイルを再生成します：

```sh
python3 tools/generate_readmes.py
```