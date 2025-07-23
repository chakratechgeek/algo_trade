import psycopg2
import pandas as pd
from datetime import timedelta

# --- Set up DB connection
conn = psycopg2.connect(
    dbname="data_collection", user="airflow", password="airflow", host="localhost", port=5432
)

def safe_sql(query, conn, name="table"):
    try:
        df = pd.read_sql(query, conn)
        print(f"[INFO] {name} rows: {len(df)}")
        if df.empty:
            print(f"[WARN] {name} table/query returned no rows.")
        return df
    except Exception as e:
        print(f"[ERR] Failed loading {name}: {e}")
        return pd.DataFrame()

# --- 1. Load market_snapshot_52weeks as the "spine"
spine = safe_sql("""
    SELECT *
    FROM data.market_snapshot_52weeks
    ORDER BY symbol_id, snapshot_ts
""", conn, "market_snapshot_52weeks")

# --- 2. Fundamentals: last known for each symbol as of snapshot_ts
fund = safe_sql("""
    SELECT symbol_id, period, value as fundamental_value, period_label
    FROM data.fundamentals
    ORDER BY symbol_id, period DESC
""", conn, "fundamentals")

# --- 3. Corporate actions: flag if action in last 14 days for each symbol/timestamp
corp = safe_sql("""
    SELECT symbol_id, ex_date, action_type
    FROM data.corporate_actions
""", conn, "corporate_actions")

# --- 4. News: aggregate sentiment in last 7 days per symbol
news = safe_sql("""
    SELECT symbol_id, timestamp, sentiment_score
    FROM data.news
""", conn, "news")

# --- 5. Orderbook: get spread and top depth at snapshot
orderbook = safe_sql("""
    SELECT symbol_id, snapshot_ts, side, price, size
    FROM data.orderbook_levels
""", conn, "orderbook_levels")

# =============== Feature Engineering ===============
features = []
for idx, row in spine.iterrows():
    sid, ts = row['symbol_id'], row['snapshot_ts']

    # --- Fundamentals (lagged: last known before ts)
    fund_row = fund[(fund.symbol_id == sid) & (fund.period <= ts)].sort_values('period', ascending=False).head(1)
    fund_val = fund_row['fundamental_value'].iloc[0] if not fund_row.empty else None

    # --- Corporate action (indicator if in last 14d)
    ca_flag = int(not corp[(corp.symbol_id == sid) & (corp.ex_date >= ts - timedelta(days=14)) & (corp.ex_date <= ts)].empty)

    # --- News sentiment (mean in last 7d)
    recent_news = news[(news.symbol_id == sid) & (news.timestamp >= ts - timedelta(days=7)) & (news.timestamp <= ts)]
    avg_sentiment = recent_news['sentiment_score'].mean() if not recent_news.empty else 0

    # --- Orderbook (spread, top bid/ask size)
    ob_now = orderbook[(orderbook.symbol_id == sid) & (orderbook.snapshot_ts == ts)]
    if not ob_now.empty:
        bids = ob_now[ob_now.side == 'B']
        asks = ob_now[ob_now.side == 'A']
        if not bids.empty and not asks.empty:
            spread = asks['price'].min() - bids['price'].max()
            bid_size = bids.loc[bids['price'].idxmax(), 'size']
            ask_size = asks.loc[asks['price'].idxmin(), 'size']
        else:
            spread, bid_size, ask_size = None, None, None
    else:
        spread, bid_size, ask_size = None, None, None

    features.append({
        'symbol_id': sid,
        'snapshot_ts': ts,
        'last_traded_price': row['last_traded_price'],
        'fifty_two_week_high': row['fifty_two_week_high'],
        'fifty_two_week_low': row['fifty_two_week_low'],
        'fundamental_value': fund_val,
        'corp_action_flag': ca_flag,
        'news_sentiment': avg_sentiment,
        'orderbook_spread': spread,
        'orderbook_bid_size': bid_size,
        'orderbook_ask_size': ask_size,
    })

df = pd.DataFrame(features)
print(df.head())
conn.close()

# =============== ML Example: XGBoost Regression ===============
import xgboost as xgb
from sklearn.model_selection import train_test_split

if df.shape[0] > 0:
    # Instead of dropping rows with NA, fill with 0 (or any value you prefer)
    df_ml = df.fillna(0)
    X = df_ml.drop(['symbol_id', 'snapshot_ts', 'last_traded_price'], axis=1)
    y = df_ml['last_traded_price']

    if len(X) > 0:
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        model = xgb.XGBRegressor(n_estimators=100)
        model.fit(X_train, y_train)
        print("Test R2:", model.score(X_test, y_test))
    else:
        print("No valid features to train on after filling NA.")
else:
    print("Not enough data for ML. Row count:", df.shape[0])

