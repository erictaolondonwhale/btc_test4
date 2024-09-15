import csv
import sqlite3
import re
from datetime import datetime
import os

# Kapcsolódás az adatbázishoz (létrehozza, ha nem létezik)
conn = sqlite3.connect('bitcoin_rich_list.db')
cursor = conn.cursor()

# Tábla létrehozása
cursor.execute('''
CREATE TABLE IF NOT EXISTS bitcoin_rich_list (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    rank INTEGER,
    address TEXT,
    balance TEXT,
    btc_amount REAL,
    usd_amount REAL,
    ins INTEGER,
    outs INTEGER,
    wallet_name TEXT
)
''')

# Változtatások mentése és kapcsolat bezárása
conn.commit()
conn.close()

print("A bitcoin_rich_list tábla sikeresen létrehozva.")



def extract_wallet_name(address):
    match = re.search(r'wallet:\s*([^B]+)', address)
    return match.group(1).strip() if match else None

def extract_btc_amount(balance):
    match = re.search(r'([\d,]+)\s*BTC', balance)
    return float(match.group(1).replace(',', '')) if match else None

def extract_usd_amount(balance):
    match = re.search(r'\$\s*([\d,]+)', balance)
    return float(match.group(1).replace(',', '')) if match else None

def extract_ins_outs(address):
    ins_match = re.search(r'Ins:(\d+)', address)
    outs_match = re.search(r'Outs:(\d+)', address)
    ins = int(ins_match.group(1)) if ins_match else None
    outs = int(outs_match.group(1)) if outs_match else None
    return ins, outs

def extract_timestamp_from_filename(filename):
    match = re.search(r'(\d{8}_\d{6})', filename)
    if match:
        return datetime.strptime(match.group(1), '%Y%m%d_%H%M%S').isoformat()
    return None

conn = sqlite3.connect('bitcoin_rich_list.db')
cursor = conn.cursor()

for filename in os.listdir('.'):
    if filename.endswith('.csv') and filename.startswith('bitcoin_rich_list_'):
        timestamp = extract_timestamp_from_filename(filename)
        
        with open(filename, 'r') as csvfile:
            csvreader = csv.reader(csvfile)
            next(csvreader)  # Skip header row
            for row in csvreader:
                rank = int(row[0])
                address = row[1]
                balance = row[2]
                
                wallet_name = extract_wallet_name(address)
                btc_amount = extract_btc_amount(balance)
                usd_amount = extract_usd_amount(balance)
                ins, outs = extract_ins_outs(address)
                
                cursor.execute('''
                    INSERT INTO bitcoin_rich_list (timestamp, rank, address, balance, btc_amount, usd_amount, ins, outs, wallet_name)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (timestamp, rank, address, balance, btc_amount, usd_amount, ins, outs, wallet_name))
        
        print(f"Feldolgozva: {filename}")

conn.commit()
conn.close()

print("Az összes CSV fájl feldolgozása befejeződött.")
