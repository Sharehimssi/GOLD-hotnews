from multiprocessing.dummy import Pool as ThreadPool
from quick_web_gold import crazy_core_for_news
import parmap

# 可变参数区
iteration_limit = 100  # 最大迭代次数
poor_num = 500  # 并行的最大线程数
re_start_time = 24  # 小时
show_info_window = False
# 内部参数区
duplicate_tolerance = 100
re_start_time = re_start_time*3600
start_urls = ['http://finance.qq.com/', 'http://money.163.com/', 'http://news.10jqka.com.cn/', 'http://gold.hexun.com/', 'http://finance.sina.com.cn/nmetal/', 'http://gold.eastmoney.com/']

pool = ThreadPool(poor_num)

if __name__ == '__main__':
    parmap.map(crazy_core_for_news.get_info_from_web, start_urls, iteration_limit, duplicate_tolerance, re_start_time, show_info_window)
pool.close()
pool.join()

