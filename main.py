from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from bs4 import BeautifulSoup
import requests
import csv
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
import sqlite3
import io

app = FastAPI()

def get_data_for_timestamp(cursor, timestamp):
    cursor.execute('''
    SELECT rank, address, ins, outs, wallet_name
    FROM bitcoin_rich_list
    WHERE timestamp = ?
    ORDER BY rank
    ''', (timestamp,))
    return cursor.fetchall()

def compare_data(prev_data, curr_data):
    changes = []
    for prev, curr in zip(prev_data, curr_data):
        if prev[2] != curr[2] or prev[3] != curr[3]:  # Only check INS and OUTS
            changes.append({
                'rank': curr[0],
                'address': curr[1],
                'prev_ins': prev[2],
                'curr_ins': curr[2],
                'prev_outs': prev[3],
                'curr_outs': curr[3],
                'wallet_name': curr[4]
            })
    return changes

def generate_report():
    conn = sqlite3.connect('bitcoin_rich_list.db')
    cursor = conn.cursor()

    cursor.execute('SELECT DISTINCT timestamp FROM bitcoin_rich_list ORDER BY timestamp')
    timestamps = [row[0] for row in cursor.fetchall()]

    output = io.StringIO()

    for i in range(1, len(timestamps)):
        prev_timestamp = timestamps[i-1]
        curr_timestamp = timestamps[i]

        prev_data = get_data_for_timestamp(cursor, prev_timestamp)
        curr_data = get_data_for_timestamp(cursor, curr_timestamp)

        if prev_data and curr_data:
            changes = compare_data(prev_data, curr_data)
            
            if changes:
                output.write(f"<h2>Változások {prev_timestamp} és {curr_timestamp} között:</h2>")
                for change in changes:
                    output.write(f"<p>Rank: {change['rank']}, Address: {change['address']}<br>")
                    if change['prev_ins'] != change['curr_ins']:
                        output.write(f"INS változás: {change['prev_ins']} -> {change['curr_ins']}<br>")
                    if change['prev_outs'] != change['curr_outs']:
                        output.write(f"OUTS változás: {change['prev_outs']} -> {change['curr_outs']}<br>")
                    output.write(f"Wallet név: {change['wallet_name']}</p>")
                    output.write("<hr>")

    conn.close()
    return output.getvalue()

def scrape_and_save():
    url = "https://bitinfocharts.com/top-100-richest-bitcoin-addresses.html"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    # Find all table IDs
    table_ids = [table.get("id") for table in soup.find_all("table", id=True)]

    # Find the tables with the rich list
    tables = []
    for table_id in table_ids:
        table = soup.find("table", {"id": table_id})
        if table:
            tables.append(table)

    data = []

    for table in tables:
        rows = table.find_all("tr")[1:]  # Skip the header row

        for row in rows:
            cols = row.find_all("td")
            if len(cols) >= 6:  # Feltételezve, hogy több oszlop van
                rank = cols[0].text.strip()
                address = cols[1].text.strip()
                balance = cols[2].text.strip()
                ins = cols[3].text.strip()
                outs = cols[4].text.strip()
                wallet_name = cols[5].text.strip() if len(cols) > 5 else 'None'
                data.append([rank, address, balance, ins, outs, wallet_name])

    if data:
        timestamp = datetime.now().replace(minute=0, second=0, microsecond=0).isoformat()
        
        # Connect to the SQLite database
        conn = sqlite3.connect('bitcoin_rich_list.db')
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS bitcoin_rich_list (
            timestamp TEXT,
            rank INTEGER,
            address TEXT,
            balance TEXT,
            ins TEXT,
            outs TEXT,
            wallet_name TEXT
        )
        ''')

        # Insert data into the database
        for row in data:
            cursor.execute('''
            INSERT INTO bitcoin_rich_list (timestamp, rank, address, balance, ins, outs, wallet_name)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (timestamp, int(row[0]), row[1], row[2], row[3], row[4], row[5]))

        conn.commit()
        conn.close()

        print(f"Data saved to database at {timestamp}")
    else:
        print("No data found on the page")


# Schedule the scraping task
scheduler = BackgroundScheduler()
scheduler.add_job(scrape_and_save, "interval", hours=1)
scheduler.start()

@app.on_event("startup")
async def startup_event():
    scrape_and_save()  # Run once at startup

@app.get("/")
async def root():
    return {"message": "Bitcoin Rich List Scraper is running"}

@app.get("/report", response_class=HTMLResponse)
async def report():
    report_content = generate_report()
    html_content = f"""
    <html>
        <head>
            <title>Bitcoin Rich List Changes Report</title>
        </head>
        <body>
            <h1>Bitcoin Rich List Changes Report</h1>
            {report_content}
        </body>
    </html>
    """
    return HTMLResponse(content=html_content)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001)  # Changed port to 8001
