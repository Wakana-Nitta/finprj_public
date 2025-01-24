import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
import sqlite3

# WebサイトのURL
url = 'https://www.tourism.jp/tourism-database/stats/inbound/'

# Webページの内容を取得
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')

# Excelシートのダウンロードリンクを動的に取得
excel_links = soup.find_all('a', href=lambda href: href and 'inbound' in href and href.endswith('.xlsx'))

# リンクが見つからない場合の処理
if not excel_links:
    raise Exception('エクセルファイルのダウンロードリンクが見つかりません')

# 取得したリンクを表示し、確認
for link in excel_links:
    print(f'Found Excel link: {link["href"]}')

# 正しいリンクパターンを考慮して条件付け
correct_link_start = "https://www.tourism.jp/wp/wp-content/uploads/"

# 最初のリンクを選択し、URLを修正
for link in excel_links:
    excel_url = link['href']
    if excel_url.startswith("/wp/wp-content/uploads/"):
        excel_url = f"https://www.tourism.jp{excel_url}"
        break

print(f'Downloading from: {excel_url}')

# エクセルファイルをダウンロード
excel_response = requests.get(excel_url)

# 応答ヘッダーを確認する
print(f'Content-Type: {excel_response.headers.get("Content-Type")}')  # MIMEタイプをプリント

# 応答が正常かどうかを確認
if excel_response.status_code == 200:
    # ダウンロードされたデータの一部を確認する
    print(excel_response.content[:100])  # 最初の100バイトをプリント

    # finprj フォルダー内にファイルを保存
    download_dir = 'finprj'
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)

    # ファイル名を動的に取得する例。リンクの末尾をファイル名として取得
    file_name = os.path.basename(excel_url)
    file_path = os.path.join(download_dir, file_name)

    with open(file_path, 'wb') as file:
        file.write(excel_response.content)

    print(f'エクセルファイルを {file_path} にダウンロードしました。')
    
    custom_column_names = ['月', '入国総数', '観光客', '商用客']
    # pandasを使ってダウンロードしたExcelファイルを読み込む
    try:
        # 特定のシートを指定する
        df = pd.read_excel(file_path, sheet_name='国別・目的別（アジア）', engine='openpyxl',
                           usecols="B:E",     # 読み込む列
                           nrows=60,          # 読み込む行数
                           skiprows=231,      # スキップする行数
                           header=None,
                           names=custom_column_names
                           ) 

        # 読み込んだデータフレームの最初の数行を表示
        print(df.head())
        
        # Excelファイルの構造を確認するためにデータフレームの情報を表示
        print("データフレームの構造:")
        print(df.info())

        # データをそのままCSVファイルとして保存する
        output_csv_path = os.path.join(download_dir, 'output.csv')
        df.to_csv(output_csv_path, index=False, encoding='utf-8')
        
        print(f'データが {output_csv_path} に保存されました。')
        
    except Exception as e:
        print(f'Excelファイルの読み込みに失敗しました: {e}')
else:
    print(f'The status code was: {excel_response.status_code}')
    print('The response content was:')
    print(excel_response.text)


#svcファイルを修正して新しいCSVファイルを作成
import os
import sqlite3
import pandas as pd

# 読み込みファイルパスを指定
input_file = 'finprj/output.csv'  # 正しいパスに修正
output_file = 'finprj/revise.csv'  # 保存先も同じディレクトリに修正

# CSVファイルを読み込む
df = pd.read_csv(input_file)

# 「月」列を「年」と「月」に分ける関数を定義
def split_year_month(value):
    # 年が含まれている場合とそうでない場合を処理
    if '年' in value:
        parts = value.split('年')
        year = parts[0].strip()
        month = int(parts[1].strip('月').strip())
    else:
        year = None
        month = int(value.strip('月').strip())
    
    return year, month

# 新しいデータフレームを作成
new_rows = []
current_year = None

for index, row in df.iterrows():
    year, month = split_year_month(row['月'])
    if year:
        current_year = year
    elif current_year:
        year = current_year
    
    new_row = {
        '年': int(year),
        '月': month,
        '入国総数': row['入国総数'],
        '観光客': row['観光客'],
        '商用客': row['商用客']
    }
    new_rows.append(new_row)

# 新しいデータフレームを作成
new_df = pd.DataFrame(new_rows)

# CSVファイルに書き出し
new_df.to_csv(output_file, index=False, encoding='utf-8-sig')

print("新しいCSVファイルが作成されました。")

# データベースにデータを保存
# CSVファイルのパス
csv_file = 'finprj/revise.csv'
db_file = 'finprj/visitors.db'

# CSVファイルを読み込む
df = pd.read_csv(csv_file)

# SQLiteデータベースに接続（存在しない場合は作成される）
conn = sqlite3.connect(db_file)

# データフレームをSQLiteデータベースに格納
df.to_sql('visitors', conn, if_exists='replace', index=False)

# データベース接続を閉じる
conn.close()

print(f"データが {db_file} に格納されました。")

db_file = 'finprj/visitors.db'
if os.path.exists(db_file):
    if os.access(db_file, os.R_OK) and os.access(db_file, os.W_OK):
        print(f'{db_file} is readable and writable.')
    else:
        print(f'{db_file} is not accessible.')

# SQLiteデータベースに接続
try:
    conn = sqlite3.connect(db_file)
    print("Database connection successful")
    
    # クエリを実行してデータを取得
    query = "SELECT * FROM visitors"
    df = pd.read_sql_query(query, conn)
    
    # データの確認
    print("Data in 'visitors' table:")
    print(df.head())

except sqlite3.OperationalError as e:
    print("OperationalError occurred while connecting to the database or fetching data:", e)
except Exception as e:
    print("Error occurred:", e)
finally:
    if 'conn' in locals():  # connが定義されている場合のみ閉じる
        conn.close()
        print("Database connection closed")