import pyodbc
import jieba.analyse
import xlwt
import xlrd
import datetime

key_word_wanted = '黄金'
# 创业板指
key_word_wanted = "%" + key_word_wanted + "%"
DBfile = r"D:\web_info.accdb"  # 数据库文件
conn = pyodbc.connect(
    r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" +
    DBfile +
    ";Uid=;Pwd=;")
cursor = conn.cursor()
cursor.execute(
    'select [time], [content] from rough_data where content like ?',
    [key_word_wanted])
contents = []
times = []
while True:
    row = cursor.fetchone()
    if not row:
        break
    else:
        times.append(row[0])
        contents.append(row[1])
# print(contents)
my_str = ''
for i in range(0, len(contents)):
    my_str = my_str + contents[i]
tags = jieba.analyse.extract_tags(my_str, 50, withWeight=True)

bk = xlrd.open_workbook("备选词库.xls")
shxrange = range(bk.nsheets)
try:
    sh = bk.sheet_by_name("Sheet1")
except BaseException:
    print("no sheet in %s named Sheet1")
nrows = sh.nrows  # 获取行数
ncols = sh.ncols  # 获取列数
Professional_dictionary = []
for i in range(0, nrows):
    row_data = sh.row_values(i)
    Professional_dictionary.append(row_data[0])

my_tags = []
for i in range(1, len(tags) + 1):
    if tags[i - 1][0] in Professional_dictionary:
        my_tags.append([tags[i - 1][0], tags[i - 1][1]])  # 植入

w = xlwt.Workbook()  # 创建一个工作簿
ws1 = w.add_sheet('重要词汇及其权重')  # 创建一个工作表
ws1.write(0, 0, '重要词汇')  # 在1行1列写入
ws1.write(0, 1, '权重（权重越大，对应的词汇重要性越高）')  # 在1行2列写入
for i in range(1, len(my_tags) + 1):
    ws1.write(i, 0, my_tags[i - 1][0])  # 在i行1列写入词汇
    ws1.write(i, 1, my_tags[i - 1][1])  # 在i行2列写入权重
a = datetime.datetime.now()
b = str(a)
b = b.replace(' ', '-')
b = b.replace(':', '-')
w.save(key_word_wanted + '词典-生成时间-' + b + '.xls')  # 保存
