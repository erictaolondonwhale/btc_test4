import sqlite3
from datetime import datetime, timedelta

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

def main():
    conn = sqlite3.connect('bitcoin_rich_list.db')
    cursor = conn.cursor()

    # Get the earliest and latest timestamps
    cursor.execute('SELECT MIN(timestamp), MAX(timestamp) FROM bitcoin_rich_list')
    min_timestamp, max_timestamp = cursor.fetchone()

    current_time = datetime.fromisoformat(min_timestamp)
    end_time = datetime.fromisoformat(max_timestamp)

    while current_time <= end_time:
        next_time = current_time + timedelta(hours=3)
        
        current_data = get_data_for_timestamp(cursor, current_time.isoformat())
        next_data = get_data_for_timestamp(cursor, next_time.isoformat())

        if current_data and next_data:
            changes = compare_data(current_data, next_data)
            
            if changes:
                print(f"\nVáltozások {current_time.isoformat()} és {next_time.isoformat()} között:")
                for change in changes:
                    print(f"Rank: {change['rank']}, Address: {change['address']}")
                    if change['prev_ins'] != change['curr_ins']:
                        print(f"INS változás: {change['prev_ins']} -> {change['curr_ins']}")
                    if change['prev_outs'] != change['curr_outs']:
                        print(f"OUTS változás: {change['prev_outs']} -> {change['curr_outs']}")
                    print(f"Wallet név: {change['wallet_name']}")
                    print("-" * 50)

        current_time = next_time

    conn.close()

if __name__ == "__main__":
    main()