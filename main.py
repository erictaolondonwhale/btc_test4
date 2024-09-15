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

    cursor.execute('SELECT MIN(timestamp), MAX(timestamp) FROM bitcoin_rich_list')
    min_timestamp, max_timestamp = cursor.fetchone()

    current_time = datetime.fromisoformat(min_timestamp)
    end_time = datetime.fromisoformat(max_timestamp)

    output = io.StringIO()

    while current_time <= end_time:
        next_time = current_time + timedelta(hours=3)
        
        current_data = get_data_for_timestamp(cursor, current_time.isoformat())
        next_data = get_data_for_timestamp(cursor, next_time.isoformat())

        if current_data and next_data:
            changes = compare_data(current_data, next_data)
            
            if changes:
                output.write(f"<h2>Változások {current_time.isoformat()} és {next_time.isoformat()} között:</h2>")
                for change in changes:
                    output.write(f"<p>Rank: {change['rank']}, Address: {change['address']}<br>")
                    if change['prev_ins'] != change['curr_ins']:
                        output.write(f"INS változás: {change['prev_ins']} -> {change['curr_ins']}<br>")
                    if change['prev_outs'] != change['curr_outs']:
                        output.write(f"OUTS változás: {change['prev_outs']} -> {change['curr_outs']}<br>")
                    output.write(f"Wallet név: {change['wallet_name']}</p>")
                    output.write("<hr>")

        current_time = next_time

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
            if len(cols) >= 3:
                rank = cols[0].text.strip()
                address = cols[1].text.strip()
                balance = cols[2].text.strip()
                data.append([rank, address, balance])

    if data:
        timestamp = datetime.now().isoformat()
        
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
            ''', (timestamp, int(row[0]), row[1], row[2], '', '', ''))

        conn.commit()
        conn.close()

        print(f"Data saved to database at {timestamp}")
    else:
        print("No data found on the page")


# Módosítás az ütemezőben
scheduler = BackgroundScheduler()
scheduler.add_job(scrape_and_save, "interval", hours=3)
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

