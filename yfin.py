import yfinance as yf

df = yf.download(
    "DNB.OL",
    start="2010-01-01",
    end="2026-10-07",
    auto_adjust=False
)

print(df)
