import pandas as pd
import sqlite3
from flask import Flask, request, jsonify

def process_excel(file_path):
    try:
        df = pd.read_excel(file_path)
        df.columns = df.columns.str.strip()
        print("Column Names:", df.columns.tolist())
        df.rename(columns={'API WELL  NUMBER': 'well'}, inplace=True)
        annual_data = df.groupby('well')[['OIL', 'GAS', 'BRINE']].sum().reset_index()
        return annual_data
    except Exception as e:
        print("Error processing Excel file:", e)
        return None

def save_to_db(dataframe, db_name='wells.db'):
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS production (
                well TEXT PRIMARY KEY,
                oil INTEGER,
                gas INTEGER,
                brine INTEGER
            )
        ''')

        for _, row in dataframe.iterrows():
            cursor.execute('''
                INSERT OR REPLACE INTO production (well, oil, gas, brine)
                VALUES (?, ?, ?, ?)
            ''', (str(row['well']), int(row['OIL']), int(row['GAS']), int(row['BRINE'])))

        conn.commit()
        conn.close()
        print("Data saved to database.")
    except Exception as e:
        print("Error saving to database:", e)

app = Flask(__name__)

@app.route('/data', methods=['GET'])
def get_well_data():
    well = request.args.get('well')
    if not well:
        return jsonify({'error': 'Missing "well" parameter'}), 400

    conn = sqlite3.connect('wells.db')
    cursor = conn.cursor()
    cursor.execute('SELECT oil, gas, brine FROM production WHERE well = ?', (well,))
    row = cursor.fetchone()
    conn.close()

    if row:
        return jsonify({'oil': row[0], 'gas': row[1], 'brine': row[2]})
    else:
        return jsonify({'error': 'Well number not found'}), 404

if __name__ == '__main__':
    excel_file = 'dataset.xls'
    annual_data = process_excel(excel_file)

    if annual_data is not None:
        save_to_db(annual_data)
        app.run(port=8080)
    else:
        print("Failed to process Excel file.")


