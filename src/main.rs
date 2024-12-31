use anyhow::{Context, Result};
use csv::ReaderBuilder;
use csv::WriterBuilder;
use lindera_core::mode::Mode;
use lindera_dictionary::{DictionaryConfig, DictionaryKind};
use lindera_tokenizer::token::Token;
use lindera_tokenizer::tokenizer::{Tokenizer, TokenizerConfig};
use serde::Deserialize;
use std::collections::HashSet;
use std::env;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};

/// ユーザー辞書のCSVレコードを表す構造体
#[derive(Debug, Deserialize)]
struct UserDictionaryRecord {
    #[serde(rename = "UserDictionary")]
    user_dictionary: String,
}

/// マージされたトークンを表す構造体
struct MergedToken {
    text: String,
    byte_start: usize,
    byte_end: usize,
    position: usize,
    position_length: usize,
}

fn main() -> Result<()> {
    // コマンドライン引数の取得
    let args: Vec<String> = env::args().collect();

    // 引数の数をチェック（3または4）
    if args.len() != 3 && args.len() != 4 {
        eprintln!(
            "使用方法: {} <入力ファイル.txt> <出力ファイル.csv> [<ユーザー辞書.csv>]",
            args[0]
        );
        std::process::exit(1);
    }

    // 引数のパスを取得
    let input_path = &args[1];
    let output_path = &args[2];
    let user_dic_list_path = if args.len() == 4 {
        Some(&args[3])
    } else {
        None
    };

    // 入力ファイルを開く
    let file = File::open(input_path).with_context(|| {
        format!(
            "入力ファイル '{}' を開くことができませんでした。",
            input_path
        )
    })?;
    let mut reader = BufReader::new(file);
    let mut text = String::new();

    // ファイルからテキストを読み込む
    reader
        .read_to_string(&mut text)
        .with_context(|| format!("ファイル '{}' の読み込みに失敗しました。", input_path))?;

    // 辞書の設定
    let dictionary = DictionaryConfig {
        kind: Some(DictionaryKind::IPADIC),
        path: None,
    };

    // トークナイザーの設定
    let config = TokenizerConfig {
        dictionary,
        user_dictionary: None, // オプションのユーザー辞書は後でマージするため、ここではNoneに設定
        mode: Mode::Normal,
    };

    // トークナイザーの作成
    let tokenizer =
        Tokenizer::from_config(config).with_context(|| "トークナイザーの作成に失敗しました。")?;

    // テキストのトークン化
    let tokens = tokenizer
        .tokenize(&text)
        .with_context(|| "テキストのトークン化に失敗しました。")?;

    // ユーザー辞書の読み込み（オプション）
    let user_dic = if let Some(path) = user_dic_list_path {
        Some(
            load_user_dictionary(path).with_context(|| {
                format!(
                    "ユーザー辞書 '{}' の読み込みに失敗しました。",
                    path
                )
            })?,
        )
    } else {
        None
    };

    // ユーザー辞書を使用するかどうかで処理を分岐
    let corrected_tokens = if let Some(user_dic_set) = &user_dic {
        // ユーザー辞書から最大のトークン数を計算
        let max_user_dic_length = user_dic_set
            .iter()
            .map(|name| name.chars().count())
            .max()
            .unwrap_or(1);

        // トークンリストの修正（ユーザー辞書の単語の結合）
        let (merged_tokens, _extracted_user_dictionaries) =
            merge_user_dictionary_words(&tokens, user_dic_set, max_user_dic_length);

        merged_tokens
    } else {
        // ユーザー辞書がない場合は、単純にTokenをMergedTokenに変換
        tokens
            .iter()
            .map(|t| MergedToken {
                text: t.text.to_string().clone(),
                byte_start: t.byte_start,
                byte_end: t.byte_end,
                position: t.position,
                position_length: t.position_length,
            })
            .collect()
    };

    // トークンをCSVに書き込む
    write_tokens_to_csv(output_path, &corrected_tokens)
        .with_context(|| format!("CSVファイル '{}' の作成に失敗しました。", output_path))?;

    println!("トークン化が完了し、{} に出力されました。", output_path);

    Ok(())
}

/// ユーザー辞書リストをCSVから読み込みHashSetに格納する関数
fn load_user_dictionary(path: &str) -> Result<HashSet<String>> {
    let file = File::open(path).with_context(|| {
        format!(
            "ユーザー辞書ファイル '{}' を開くことができませんでした。",
            path
        )
    })?;
    let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);

    let mut user_dic_set = HashSet::new();
    for result in rdr.deserialize() {
        let record: UserDictionaryRecord =
            result.with_context(|| "ユーザー辞書のレコードのデシリアライズに失敗しました。")?;
        user_dic_set.insert(record.user_dictionary.clone());
    }
    Ok(user_dic_set)
}

/// トークンリストを走査し、ユーザー辞書と一致する連続トークンを結合する関数
fn merge_user_dictionary_words(
    tokens: &[Token],
    user_dictionary: &HashSet<String>,
    max_length: usize,
) -> (Vec<MergedToken>, HashSet<String>) {
    let mut corrected_tokens = Vec::with_capacity(tokens.len());
    let mut extracted_user_dictionaries = HashSet::new();
    let mut i = 0;
    let len = tokens.len();

    while i < len {
        let mut matched = false;

        // 最大マッチングの長さを設定（ユーザー辞書の最大単語数）
        let max_match_length = max_length;

        // マッチングを試みる
        for window_size in (1..=max_match_length).rev() {
            if i + window_size > len {
                continue;
            }

            // トークンを連結して候補の単語を生成
            let candidate: String = tokens[i..i + window_size]
                .iter()
                .map(|t| t.text)
                .collect::<String>();

            if user_dictionary.contains(&candidate) {
                // 一致する単語が見つかった場合
                // 新しいマージされたトークンを作成
                let merged_token = MergedToken {
                    text: candidate.clone(),
                    byte_start: tokens[i].byte_start,
                    byte_end: tokens[i + window_size - 1].byte_end,
                    position: tokens[i].position,
                    position_length: tokens[i + window_size - 1].position_length,
                };

                corrected_tokens.push(merged_token);
                extracted_user_dictionaries.insert(candidate.clone());

                i += window_size;
                matched = true;
                break;
            }
        }

        if !matched {
            // 一致する単語が見つからなかった場合、現在のトークンをそのまま追加
            let token = &tokens[i];
            let unmerged_token = MergedToken {
                text: token.text.to_string().clone(),
                byte_start: token.byte_start,
                byte_end: token.byte_end,
                position: token.position,
                position_length: token.position_length,
            };
            corrected_tokens.push(unmerged_token);
            i += 1;
        }
    }

    (corrected_tokens, extracted_user_dictionaries)
}

/// トークンをCSVに書き込む関数（BOM付きUTF-8）
fn write_tokens_to_csv(output_path: &str, tokens: &[MergedToken]) -> Result<()> {
    let file = File::create(output_path)
        .with_context(|| format!("CSVファイル '{}' を作成できませんでした。", output_path))?;
    let mut writer = BufWriter::new(file);

    // UTF-8のBOMを先頭に書き込む
    writer
        .write_all(&[0xEF, 0xBB, 0xBF])
        .with_context(|| "BOMの書き込みに失敗しました。")?;

    // CSVライターを作成
    let mut wtr = WriterBuilder::new().has_headers(true).from_writer(writer);

    // CSVのヘッダーを設定
    wtr.write_record(&[
        "Token",
        "byte_start",
        "byte_end",
        "position",
        "position_length",
    ])
    .with_context(|| "CSVヘッダーの書き込みに失敗しました。")?;

    for token in tokens {
        wtr.write_record(&[
            &token.text,
            &token.byte_start.to_string(),
            &token.byte_end.to_string(),
            &token.position.to_string(),
            &token.position_length.to_string(),
        ])
        .with_context(|| "トークンの書き込みに失敗しました。")?;
    }

    // CSVをフラッシュ
    wtr.flush()
        .with_context(|| "CSVのフラッシュに失敗しました。")?;

    Ok(())
}
