from fastapi import FastAPI
from bs4 import BeautifulSoup
import requests
import csv
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime

app = FastAPI()


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
        # Save to CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"bitcoin_rich_list_{timestamp}.csv"

        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Rank", "Address", "Balance"])
            writer.writerows(data)

        print(f"Data saved to {filename}")
    else:
        print("No data found on the page")


# Schedule the scraping task
scheduler = BackgroundScheduler()
scheduler.add_job(scrape_and_save, "interval", minutes=1)
scheduler.start()


@app.get("/")
async def root():
    return {"message": "Bitcoin Rich List Scraper is running"}

if __name__ == "__main__":
    import uvicorn
    import socket

    def find_free_port():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0))
            return s.getsockname()[1]

    try:
        port = find_free_port()
        uvicorn.run(app, host="0.0.0.0", port=port)
    except Exception as e:
        print(f"Error starting the server: {e}")
        print("Try running the script with administrator privileges or close any applications using port 8000.")