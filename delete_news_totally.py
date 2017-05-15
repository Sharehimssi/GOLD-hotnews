import pyodbc
DBfile = r"D:\gold_plan.accdb"  # 数据库文件
conn = pyodbc.connect(
                    r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + DBfile + ";Uid=;Pwd=;")
x = "http://www.2cto.com/kf/201405/304975.html"
cursor = conn.cursor()
cursor.execute('delete * from history_visit_record')
conn.commit()
cursor.execute('delete * from rough_data')
'''
cursor.execute("insert into history_visit_record(url) values (?)", x)

cursor.execute("""insert into [rough_data] ([time],[source],[content]) values (?,?,?)""", [accurate_time, '网易', contents])

record = cursor.fetchall()
print(record)

while 1:
    row = cursor.fetchone()
    if not row:
        break
    else:
        print(row)
'''

conn.commit()

cursor.close()
conn.close()
