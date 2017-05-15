import urllib.request
import queue
from bs4 import BeautifulSoup
import socket
import chardet
import time
import pyodbc
import tkinter
import http
import copy


#SQL = 'SELECT * from rough_data'


def is_valid_date(str):
  #判断是否是一个有效的日期字符串
  try:
    time.strptime(str, "%Y-%m-%d")
    return True
  except:
    return False


def get_info_from_web(start_url, iteration_limit, duplicate_tolerance, re_start_time, show_info_window):
    while 1:  # 永不停息
        start_time = time.clock()
        # 爬虫部分
        url_stream = queue.Queue(maxsize=-1)
        url_stream.put(start_url)  # 起点位置
        restrict_area = set()
        end_loca = start_url.find('.com')
        restrict_area.add(start_url[0:end_loca + 4])
        iteration_time = 1

        socket.setdefaulttimeout(60)
        # 核心代码区
        DBfile = r"D:\gold_plan.accdb"  # 数据库文件
        conn = pyodbc.connect(
            r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + DBfile + ";Uid=;Pwd=;")
        cursor = conn.cursor()
        cursor.execute('select url from [history_visit_record]')
        history_set = set()
        while 1:
            row = cursor.fetchone()
            if not row:
                break
            else:
                z = list(row)
                history_set.add(z[0])
        formal_history_set = copy.deepcopy(history_set)
        duplicate_count = 0    #允许访问的重复的历史记录数
        current_time = start_time
        while (not url_stream.empty()) and iteration_time < iteration_limit and current_time-start_time <= re_start_time:
            current_time = time.clock()
            only_record_url_not_record_contents = 0
            try:
                x = url_stream.get()
                end_loca = x.find('.com')
                if not x[0:end_loca + 4] in restrict_area:
                    continue
                elif x in formal_history_set and duplicate_count <= duplicate_tolerance:  # 该url已经访问过，但重复访问的网页总数尚未超过忍耐度
                    only_record_url_not_record_contents = 1
                    duplicate_count += 1
                    # print('当前重复访问的网页数：', str(duplicate_count))
                elif x in formal_history_set and duplicate_count > duplicate_tolerance:  # 该url已经访问过，且重复访问的网页总数超过了忍耐度
                    continue
                elif x in history_set:  #本轮已访问
                    # print('避免重复访问，重复的url：', x)
                    continue
                else: #可以访问
                    DBfile = r"D:\gold_plan.accdb"  # 数据库文件
                    conn = pyodbc.connect(
                        r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + DBfile + ";Uid=;Pwd=;")
                    cursor = conn.cursor()
                    cursor.execute("insert into history_visit_record(url) values (?)", x)
                    conn.commit()
            except AttributeError:
                continue
            else:
                pass
            finally:
                pass
            if iteration_time % 1 == 0 and show_info_window:
                w = tkinter.Tk()
                w.wm_state('zoomed')
                show_word = '当前已访问的网页数：'+str(len(history_set))
                label = tkinter.Label(w, text=show_word, font=("黑体", 80, "bold"))
                label.pack()
                w.after(2000, lambda: w.destroy())  # Destroy the widget after 2 seconds
                w.mainloop()
            if '163.com' in x:
                if only_record_url_not_record_contents == 0:  # 将不在历史记录库中的url记录
                    history_set.add(x)
                if iteration_time % 100 == 0:   #节省内存，每100个输出一次
                    print('当前正在访问：', x, '     当前已访问网页总数：', iteration_time, '待访问链接量：', url_stream.qsize())
                try:
                    page = urllib.request.Request(x, headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36"})
                    # page = urllib.request.urlopen(url, timeout=60)
                    f = urllib.request.urlopen(page)
                    html_doc = f.read()
                    chardit1 = chardet.detect(html_doc)
                    if 'iso-8859-1' == chardit1['encoding']:
                        soup = BeautifulSoup(html_doc, "html.parser", from_encoding=chardit1['encoding'])
                    else:
                        soup = BeautifulSoup(html_doc, "html.parser", from_encoding='gb18030')
                    # time.sleep(1)
                    for link in soup.find_all('a'):  # 记录链接
                        url_stream.put(link.get('href'))
                    if only_record_url_not_record_contents == 0:  # 该页面不属于种子页面，需要记录内容
                        rough_time = soup.find('div', attrs={'class': 'post_time_source'}).get_text()
                        rough_time.strip()
                        rough_time.replace("\n", "")
                        x = rough_time.find('-')
                        if x == -1:
                            continue
                        accurate_time = rough_time[x-4:x+6]
                        if not is_valid_date(accurate_time):
                            continue
                        iteration_time += 1
                        [s.extract() for s in soup('div', attrs={'class': "caijing_bq_ttl"})]
                        [s.extract() for s in soup('div', attrs={'class': "caijing_bq_list"})]
                        [s.extract() for s in soup('div', attrs={'class': "detail"})]
                        [s.extract() for s in soup('div', attrs={'class': "ep-source cDGray"})]
                        [s.extract() for s in soup('style')]
                        contents = soup.find('div', attrs={'class': "post_text"}).get_text()
                        contents.strip()
                        contents = ''.join(contents.split())
                        if contents == '':
                            continue
                        DBfile = r"D:\gold_plan.accdb"  # 数据库文件
                        conn = pyodbc.connect(
                            r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + DBfile + ";Uid=;Pwd=;")
                        cursor = conn.cursor()
                        cursor.execute("""insert into [rough_data] ([time],[source],[content]) values (?,?,?)""",
                                        [accurate_time, '网易', contents])
                        conn.commit()
                        cursor.close()
                        conn.close()
                except urllib.error.URLError:
                    pass
                    # print('跳过无效链接')
                except UnicodeEncodeError:
                    pass
                    # print('跳过无效文本')
                except AttributeError:
                    pass
                    # print('跳过无效属性')
                except socket.timeout:
                    pass
                    # print('跳过超时网页')
                except ValueError:
                    pass
                    # print('跳过返回值无效网页')
                except ConnectionResetError:
                    pass
                    # print('跳过拒绝爬虫网页')
                except http.client.IncompleteRead:
                    pass
                    # print('跳过不完全网页')
                else:
                    pass
                finally:
                    pass
            elif 'sina.com' in x:
                if only_record_url_not_record_contents == 0:  # 将不在历史记录库中的url记录
                    history_set.add(x)
                if iteration_time % 100 == 0:   #节省内存，每100个输出一次
                    print('当前正在访问：', x, '     当前已访问网页总数：', iteration_time,  '待访问链接量：',  url_stream.qsize())
                try:
                    page = urllib.request.Request(x, headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36"})
                    # page = urllib.request.urlopen(url, timeout=60)
                    f = urllib.request.urlopen(page)
                    html_doc = f.read()
                    chardit1 = chardet.detect(html_doc)
                    if 'iso-8859-1' == chardit1['encoding']:
                        soup = BeautifulSoup(html_doc, "html.parser", from_encoding=chardit1['encoding'])
                    else:
                        soup = BeautifulSoup(html_doc, "html.parser", from_encoding='gb18030')
                    # time.sleep(1)
                    for link in soup.find_all('a'):  # 记录链接
                        url_stream.put(link.get('href'))
                    if only_record_url_not_record_contents == 0:  # 该页面不属于种子页面，需要记录内容
                        rough_time = soup.find('span', attrs={'class': 'time-source'}).get_text()
                        rough_time.strip()
                        rough_time.replace("\n", "")
                        x = rough_time.find('年')
                        if x == -1:
                            continue
                        accurate_time = rough_time[x - 4:x]+'-'+rough_time[x+1:x+3]+'-'+rough_time[x+4:x+6]
                        if not is_valid_date(accurate_time):
                            continue
                        iteration_time += 1
                        [s.extract() for s in soup('div', attrs={'class': "finance_app_zqtg"})]
                        [s.extract() for s in soup('p', attrs={'class': "article-editor"})]
                        [s.extract() for s in soup('div', attrs={'data-sudaclick': "suda_1028_guba"})]
                        [s.extract() for s in soup('div', attrs={'class': "page-footer"})]
                        [s.extract() for s in soup('div', attrs={'class': "seo_data_list"})]
                        contents = ''
                        for text in soup.find_all('p'):
                            contents = contents + text.get_text()
                        contents.strip()
                        contents = ''.join(contents.split())
                        if contents == '':
                            continue
                        DBfile = r"D:\gold_plan.accdb"  # 数据库文件
                        conn = pyodbc.connect(
                            r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + DBfile + ";Uid=;Pwd=;")
                        cursor = conn.cursor()
                        cursor.execute("""insert into [rough_data] ([time],[source],[content]) values (?,?,?)""",
                                        [accurate_time, '新浪', contents])
                        conn.commit()
                        cursor.close()
                        conn.close()
                except urllib.error.URLError:
                    pass
                    # print('跳过无效链接')
                except UnicodeEncodeError:
                    pass
                    # print('跳过无效文本')
                except AttributeError:
                    pass
                    # print('跳过无效属性')
                except socket.timeout:
                    pass
                    # print('跳过超时网页')
                except ValueError:
                    pass
                    # print('跳过返回值无效网页')
                except ConnectionResetError:
                    pass
                    # print('跳过拒绝爬虫网页')
                except http.client.IncompleteRead:
                    pass
                    # print('跳过不完全网页')
                else:
                    pass
                finally:
                    pass
            elif 'eastmoney.com' in x:
                if only_record_url_not_record_contents == 0:  # 将不在历史记录库中的url记录
                    history_set.add(x)
                if iteration_time % 100 == 0:   #节省内存，每100个输出一次
                    print('当前正在访问：', x, '     当前已访问网页总数：', iteration_time, ';  待访问链接量：',url_stream.qsize())
                try:
                    page = urllib.request.Request(x, headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36"})
                    # page = urllib.request.urlopen(url, timeout=60)
                    f = urllib.request.urlopen(page)
                    html_doc = f.read()
                    chardit1 = chardet.detect(html_doc)
                    if 'iso-8859-1' == chardit1['encoding']:
                        soup = BeautifulSoup(html_doc, "html.parser", from_encoding=chardit1['encoding'])
                    else:
                        soup = BeautifulSoup(html_doc, "html.parser", from_encoding='gb18030')
                    # time.sleep(1)
                    for link in soup.find_all('a'):  # 记录链接
                        url_stream.put(link.get('href'))
                    if only_record_url_not_record_contents == 0:  # 该页面不属于种子页面，需要记录内容
                        rough_time = soup.find('div', attrs={'class': 'time'}).get_text()
                        rough_time.strip()
                        rough_time.replace("\n", "")
                        x = rough_time.find('年')
                        if x == -1:
                            continue
                        accurate_time = rough_time[x - 4:x] + '-' + rough_time[x + 1:x + 3] + '-' + rough_time[x + 4:x + 6]
                        if not is_valid_date(accurate_time):
                            continue
                        iteration_time += 1
                        [s.extract() for s in soup('p', attrs={'style': "text-align:center"})]
                        [s.extract() for s in soup('a', attrs={'target': "_blank"})]
                        [s.extract() for s in soup('p', attrs={'class': "res-edit"})]
                        [s.extract() for s in soup('span', attrs={'style': "color:#ff0000"})]
                        contents = ''
                        for text in soup.find_all('p'):
                            contents = contents+text.get_text()
                        contents.strip()
                        contents = ''.join(contents.split())
                        if contents == '':
                            continue
                        DBfile = r"D:\gold_plan.accdb"  # 数据库文件
                        conn = pyodbc.connect(
                            r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + DBfile + ";Uid=;Pwd=;")
                        cursor = conn.cursor()
                        cursor.execute("""insert into [rough_data] ([time],[source],[content]) values (?,?,?)""",
                                        [accurate_time, '东方财富', contents])
                        conn.commit()
                        cursor.close()
                        conn.close()
                except urllib.error.URLError:
                    pass
                    # print('跳过无效链接')
                except UnicodeEncodeError:
                    pass
                    # print('跳过无效文本')
                except AttributeError:
                    pass
                    # print('跳过无效属性')
                except socket.timeout:
                    pass
                    # print('跳过超时网页')
                except ValueError:
                    pass
                    # print('跳过返回值无效网页')
                except ConnectionResetError:
                    pass
                    # print('跳过拒绝爬虫网页')
                except http.client.IncompleteRead:
                    pass
                    # print('跳过不完全网页')
                else:
                    pass
                finally:
                    pass
            elif 'hexun.com' in x:
                if only_record_url_not_record_contents == 0:  # 将不在历史记录库中的url记录
                    history_set.add(x)
                if iteration_time % 100 == 0:   #节省内存，每100个输出一次
                    print('当前正在访问：', x, '     当前已访问网页总数：', iteration_time, ';   待访问链接量：',url_stream.qsize())
                try:
                    page = urllib.request.Request(x, headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36"})
                    # page = urllib.request.urlopen(url, timeout=60)
                    f = urllib.request.urlopen(page)
                    html_doc = f.read()
                    chardit1 = chardet.detect(html_doc)
                    if 'iso-8859-1' == chardit1['encoding']:
                        soup = BeautifulSoup(html_doc, "html.parser", from_encoding=chardit1['encoding'])
                    else:
                        soup = BeautifulSoup(html_doc, "html.parser", from_encoding='gb18030')
                    # time.sleep(1)
                    for link in soup.find_all('a'):  # 记录链接
                        url_stream.put(link.get('href'))
                    if only_record_url_not_record_contents == 0:  # 该页面不属于种子页面，需要记录内容
                        rough_time = soup.find('span', attrs={'class': 'pr20'}).get_text()
                        rough_time.strip()
                        rough_time.replace("\n", "")
                        x = rough_time.find('-')
                        if x == -1:
                            continue
                        accurate_time = rough_time[x - 4:x + 6]
                        if not is_valid_date(accurate_time):
                            continue
                        iteration_time += 1
                        [s.extract() for s in soup('div', attrs={'style': "text-align:right;font-size:12px"})]
                        contents = soup.find('div', attrs={'class': "art_contextBox"}).get_text()
                        contents.strip()
                        contents = ''.join(contents.split())
                        if contents == '':
                            continue
                        DBfile = r"D:\gold_plan.accdb"  # 数据库文件
                        conn = pyodbc.connect(
                            r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + DBfile + ";Uid=;Pwd=;")
                        cursor = conn.cursor()
                        cursor.execute("""insert into [rough_data] ([time],[source],[content]) values (?,?,?)""",
                                        [accurate_time, '和讯', contents])
                        conn.commit()
                        cursor.close()
                        conn.close()
                except urllib.error.URLError:
                    pass
                    # print('跳过无效链接')
                except UnicodeEncodeError:
                    pass
                    # print('跳过无效文本')
                except AttributeError:
                    pass
                    # print('跳过无效属性')
                except socket.timeout:
                    pass
                    # print('跳过超时网页')
                except ValueError:
                    pass
                    # print('跳过返回值无效网页')
                except ConnectionResetError:
                    pass
                    # print('跳过拒绝爬虫网页')
                except http.client.IncompleteRead:
                    pass
                    # print('跳过不完全网页')
                else:
                    pass
                finally:
                    pass
            elif 'qq.com' in x:
                if only_record_url_not_record_contents == 0:  # 将不在历史记录库中的url记录
                    history_set.add(x)
                if iteration_time % 100 == 0:  # 节省内存，每100个输出一次
                    print('当前正在访问：', x, '     当前已访问网页总数：', iteration_time,  ';   待访问链接量：',url_stream.qsize())
                try:
                    page = urllib.request.Request(x, headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36"})
                    # page = urllib.request.urlopen(url, timeout=60)
                    f = urllib.request.urlopen(page)
                    html_doc = f.read()
                    chardit1 = chardet.detect(html_doc)
                    if 'iso-8859-1' == chardit1['encoding']:
                        soup = BeautifulSoup(html_doc, "html.parser", from_encoding=chardit1['encoding'])
                    elif 'None' == chardit1['encoding']:
                        soup = BeautifulSoup(html_doc, "html.parser", from_encoding='gb2312')
                    elif 'GB2312' == chardit1['encoding']:
                        soup = BeautifulSoup (html_doc, "html.parser", from_encoding='gb2312')
                    else:
                        soup = BeautifulSoup(html_doc, "html.parser", from_encoding='gb18030')
                    # time.sleep(1)
                    for link in soup.find_all('a'):  # 记录链接
                        url_stream.put(link.get('href'))
                    if only_record_url_not_record_contents == 0:  # 该页面不属于种子页面，需要记录内容
                        rough_time = soup.find('span', attrs={'class': 'a_time'}).get_text()
                        rough_time.strip()
                        rough_time.replace("\n", "")
                        x = rough_time.find('-')
                        if x == -1:
                            continue
                        accurate_time = rough_time[x - 4:x + 6]
                        if not is_valid_date(accurate_time):
                            continue
                        [s.extract() for s in soup('div', attrs={'id': "fin_kline_mod"})]
                        contents = soup.find('div', attrs={'id': "Cnt-Main-Article-QQ"}).get_text()
                        contents.strip()
                        contents = ''.join(contents.split())
                        if contents == '':
                            continue
                        DBfile = r"D:\gold_plan.accdb"  # 数据库文件
                        conn = pyodbc.connect(
                            r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + DBfile + ";Uid=;Pwd=;")
                        cursor = conn.cursor()
                        cursor.execute("""insert into [rough_data] ([time],[source],[content]) values (?,?,?)""",
                                        [accurate_time, '腾讯', contents])
                        conn.commit()
                        cursor.close()
                        conn.close()
                except urllib.error.URLError:
                    pass
                    # print('跳过无效链接')
                except UnicodeEncodeError:
                    pass
                    # print('跳过无效文本')
                except AttributeError:
                    pass
                    # print('跳过无效属性')
                except socket.timeout:
                    pass
                    # print('跳过超时网页')
                except ValueError:
                    pass
                    # print('跳过返回值无效网页')
                except ConnectionResetError:
                    pass
                    # print('跳过拒绝爬虫网页')
                except http.client.IncompleteRead:
                    pass
                    # print('跳过不完全网页')
                else:
                    pass
                finally:
                    pass
            elif '10jqka.com' in x:
                if only_record_url_not_record_contents == 0:  # 将不在历史记录库中的url记录
                    history_set.add(x)
                if iteration_time % 100 == 0:  # 节省内存，每100个输出一次
                    print('当前正在访问：', x, '     当前已访问网页总数：', iteration_time, '；  待访问链接量：',url_stream.qsize())
                try:
                    page = urllib.request.Request(x, headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36"})
                    # page = urllib.request.urlopen(url, timeout=60)
                    f = urllib.request.urlopen(page)
                    html_doc = f.read()
                    chardit1 = chardet.detect(html_doc)
                    #soup = BeautifulSoup(html_doc, "html.parser")
                    if 'iso-8859-1' == chardit1['encoding']:
                        soup = BeautifulSoup(html_doc, "html.parser", from_encoding=chardit1['encoding'])
                    elif 'None' == chardit1['encoding'] or 'none' == chardit1['encoding'] or 'NONE' == chardit1['encoding']:
                        soup = BeautifulSoup(html_doc, "html.parser")
                    else:
                        soup = BeautifulSoup(html_doc, "html.parser", from_encoding='gb18030')
                    # time.sleep(1)
                    for link in soup.find_all('a'):  # 记录链接
                        url_stream.put(link.get('href'))
                    if only_record_url_not_record_contents == 0:  # 该页面不属于种子页面，需要记录内容
                        rough_time = soup.find('span', attrs={'class': 'time'}).get_text()
                        rough_time.strip()
                        rough_time.replace("\n", "")
                        x = rough_time.find('-')
                        if x == -1:
                            continue
                        accurate_time = rough_time[x - 4:x + 6]
                        if not is_valid_date(accurate_time):
                            continue
                        iteration_time += 1
                        [s.extract() for s in soup('p', attrs={'class': "bottomSign"})]
                        [s.extract() for s in soup('script', attrs={'type': "text/javascript"})]
                        [s.extract() for s in soup('div', attrs={'class': "editor"})]
                        contents = soup.find('div', attrs={'class': "atc-content"}).get_text()
                        contents.strip()
                        contents = ''.join(contents.split())
                        if contents == '':
                            continue
                        DBfile = r"D:\gold_plan.accdb"  # 数据库文件
                        conn = pyodbc.connect(
                            r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + DBfile + ";Uid=;Pwd=;")
                        cursor = conn.cursor()
                        cursor.execute("""insert into [rough_data] ([time],[source],[content]) values (?,?,?)""",
                                        [accurate_time, '同花顺', contents])
                        conn.commit()
                        cursor.close()
                        conn.close()
                except urllib.error.URLError:
                    pass
                    # print('跳过无效链接')
                except UnicodeEncodeError:
                    pass
                    # print('跳过无效文本')
                except AttributeError:
                    pass
                    # print('跳过无效属性')
                except socket.timeout:
                    pass
                    # print('跳过超时网页')
                except ValueError:
                    pass
                    # print('跳过返回值无效网页')
                except ConnectionResetError:
                    pass
                    # print('跳过拒绝爬虫网页')
                except http.client.IncompleteRead:
                    pass
                    # print('跳过不完全网页')
                else:
                    pass
                finally:
                    pass
        print('本轮爬虫暂时性结束！稍后自动开启下一轮……', current_time, '  minus   ', start_time, '= ', current_time-start_time)
