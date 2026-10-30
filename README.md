# Online Grocery List Aggregator

Turns grocery receipt emails into a queryable SQLite database.

After an online order, Hemköp emails a receipt. This tool reads those emails
from your Gmail account, extracts the purchased items and prices, and stores
them in a local database so that spending can be analysed with plain SQL.

## How it works

1. **`email_reader/email_reader.py`** authenticates with the Gmail API
   (read-only scope), lists messages from Hemköp whose subject contains
   "kvitto" (receipt), and parses the receipt out of each email's HTML with
   BeautifulSoup. Item name, price and receipt date are extracted from the
   receipt text.
2. **`email_reader/db_uploader.py`** creates the SQLite database
   (`groceries.db`) on first run and loads receipts incrementally. Receipts
   already in the database are skipped, so it can be re-run safely.
3. **`email_reader/analyser.py`** is a placeholder for the analysis layer. It
   is not implemented yet. For now, analysis is done with SQL.

### Data model

```
receipts(receipt_id, receipt_date, item_count, created_date)
items(item_id, item_name, item_price, receipt_id -> receipts.receipt_id)
```

## Setup

Requires Python 3.

```bash
pip install google-api-python-client google-auth-oauthlib beautifulsoup4 html5lib
```

1. Enable the Gmail API in a Google Cloud project and download an OAuth client
   file as `credentials.json`. See
   [Google's Gmail API quickstart](https://developers.google.com/gmail/api/quickstart/python).
2. Put `credentials.json` in the directory you run the script from.
3. Run the loader. The first run opens a browser window for consent and saves
   `token.pickle` for later runs. If you get an error refreshing the token,
   delete `token.pickle` and run again.

```bash
cd email_reader
python db_uploader.py
```

Never commit `credentials.json`, `token.pickle` or your own `groceries.db`.

## Example queries

```sql
-- Most frequently bought items
SELECT item_name, COUNT(*) AS times_bought, ROUND(AVG(item_price), 2) AS avg_price
FROM items
GROUP BY item_name
ORDER BY times_bought DESC;

-- Spend per receipt
SELECT r.receipt_date, r.item_count, ROUND(SUM(i.item_price), 2) AS total
FROM receipts r JOIN items i ON i.receipt_id = r.receipt_id
GROUP BY r.receipt_id
ORDER BY r.receipt_date;
```

## Limitations

- The parser is written for the layout of Hemköp's receipt emails as they were
  in 2022. If the email format changes, the parsing needs updating.
- SQL inserts are built with string formatting. This is fine for a personal
  tool reading your own receipts, but would need parameterised queries before
  use with any untrusted input.
- Receipt IDs combine a running counter with the receipt date, which is enough
  for personal use but not a robust unique key.
