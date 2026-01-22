#!/usr/bin/env python3
"""
積立投資シミュレーター用データ取得スクリプト（修正版）

取得データ：
- ACWI（MSCI ACWI - オルカン連動指数）
- ^NYFANG（NYSE FANG+ - FANG+連動指数）
- ^N225（日経平均）
- USDJPY=X（為替レート）

出力：
- data/index_data.json（日次データ、円換算済み）
"""

import yfinance as yf
import pandas as pd
import json
from datetime import datetime, timedelta
import os

# 設定
OUTPUT_DIR = "data"
OUTPUT_FILE = "index_data.json"
YEARS_BACK = 20

# 取得するティッカー
TICKERS = {
    "acwi": "ACWI",           # オルカン連動（ドル建て）
    "fang": "^NYFANG",        # FANG+連動（ドル建て）
    "nikkei": "^N225",        # 日経平均（円建て）
    "usdjpy": "USDJPY=X"      # 為替レート
}

def fetch_data():
    """Yahoo Financeからデータを取得"""
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=YEARS_BACK * 365)
    
    print(f"データ取得期間: {start_date.strftime('%Y-%m-%d')} 〜 {end_date.strftime('%Y-%m-%d')}")
    print("-" * 50)
    
    data = {}
    
    for name, ticker in TICKERS.items():
        print(f"取得中: {name} ({ticker})...")
        try:
            df = yf.download(ticker, start=start_date, end=end_date, progress=False, auto_adjust=True)
            if len(df) > 0:
                # 新しいyfinanceではカラムがMultiIndexになる場合があるので対応
                if isinstance(df.columns, pd.MultiIndex):
                    # ('Close', 'ACWI') のような形式の場合
                    close_col = [col for col in df.columns if 'Close' in col[0]]
                    if close_col:
                        data[name] = df[close_col[0]]
                    else:
                        # Closeがない場合は最初の列を使用
                        data[name] = df.iloc[:, 0]
                else:
                    # 通常のカラム名の場合
                    if 'Close' in df.columns:
                        data[name] = df['Close']
                    elif 'Adj Close' in df.columns:
                        data[name] = df['Adj Close']
                    else:
                        data[name] = df.iloc[:, 0]
                
                print(f"  ✓ {len(df)}件取得 ({df.index[0].strftime('%Y-%m-%d')} 〜 {df.index[-1].strftime('%Y-%m-%d')})")
            else:
                print(f"  ✗ データなし")
        except Exception as e:
            print(f"  ✗ エラー: {e}")
    
    return data

def process_data(data):
    """データを結合し、円換算を行う"""
    
    print("-" * 50)
    print("データ処理中...")
    
    # 各SeriesをDataFrameに結合
    df = pd.DataFrame()
    for name, series in data.items():
        df[name] = series
    
    # インデックスでソート
    df = df.sort_index()
    
    # 欠損値を前の値で埋める（休場日対応）
    df = df.ffill()
    
    # 円換算（オルカンとFANG+）
    df['acwi_jpy'] = df['acwi'] * df['usdjpy']
    df['fang_jpy'] = df['fang'] * df['usdjpy']
    df['nikkei_jpy'] = df['nikkei']  # 日経平均はそのまま
    
    # 必要な列だけ残す
    result = df[['acwi_jpy', 'fang_jpy', 'nikkei_jpy', 'usdjpy']].copy()
    result.columns = ['acwi', 'fang', 'nikkei', 'usdjpy']
    
    # NaNを含む行を削除
    result = result.dropna()
    
    print(f"  ✓ 処理完了: {len(result)}件")
    
    return result

def save_to_json(df):
    """JSON形式で保存"""
    
    print("-" * 50)
    print("JSON保存中...")
    
    # 出力ディレクトリ作成
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # JSON用のデータ構造
    output = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "start_date": df.index[0].strftime('%Y-%m-%d'),
            "end_date": df.index[-1].strftime('%Y-%m-%d'),
            "total_records": len(df),
            "description": {
                "acwi": "オルカン連動指数（MSCI ACWI）円換算",
                "fang": "FANG+連動指数（NYSE FANG+）円換算",
                "nikkei": "日経平均株価",
                "usdjpy": "為替レート（USD/JPY）"
            }
        },
        "data": []
    }
    
    # データを日付ごとのオブジェクトとして格納
    for date, row in df.iterrows():
        output["data"].append({
            "date": date.strftime('%Y-%m-%d'),
            "acwi": round(float(row['acwi']), 2),
            "fang": round(float(row['fang']), 2) if pd.notna(row['fang']) else None,
            "nikkei": round(float(row['nikkei']), 2),
            "usdjpy": round(float(row['usdjpy']), 2)
        })
    
    # JSON保存
    filepath = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    print(f"  ✓ 保存完了: {filepath}")
    
    return filepath

def show_summary(df):
    """データサマリーを表示"""
    
    print("-" * 50)
    print("データサマリー")
    print("-" * 50)
    
    for col in ['acwi', 'fang', 'nikkei']:
        valid_data = df[col].dropna()
        if len(valid_data) > 0:
            print(f"\n【{col.upper()}】")
            print(f"  期間: {valid_data.index[0].strftime('%Y-%m-%d')} 〜 {valid_data.index[-1].strftime('%Y-%m-%d')}")
            print(f"  件数: {len(valid_data)}")
            print(f"  最小: ¥{valid_data.min():,.0f}")
            print(f"  最大: ¥{valid_data.max():,.0f}")
            print(f"  最新: ¥{valid_data.iloc[-1]:,.0f}")

def main():
    print("=" * 50)
    print("積立投資シミュレーター データ取得")
    print("=" * 50)
    
    # データ取得
    raw_data = fetch_data()
    
    if len(raw_data) < 4:
        print("エラー: 必要なデータが取得できませんでした")
        return
    
    # データ処理
    processed = process_data(raw_data)
    
    # JSON保存
    filepath = save_to_json(processed)
    
    # サマリー表示
    show_summary(processed)
    
    print("\n" + "=" * 50)
    print("完了！")
    print(f"JSONファイル: {os.path.abspath(filepath)}")
    print("=" * 50)

if __name__ == "__main__":
    main()
