from data_pipeline.ingest.fetch_day import fetch_four_snapshots

def main():
    # 注意用真实存在的交易日
    symbol = "AAPL"
    trade_date = "2024-01-05"   # 自己填一个

    result = fetch_four_snapshots(symbol, trade_date)

    print("\n=== fetch_day test result ===")
    print(result)


if __name__ == "__main__":
    main()