import pyttsx3
from datetime import datetime as Datetime
from datetime import time as dttime, timedelta
import time
import pandas as pd
from ashare import ashare
import config
from typing import Dict, List, NoReturn, Optional
from abc import abstractmethod
from loguru import logger
import collections
import pickle
from threading import Thread
collections.Callable = collections.abc.Callable

ASTOCK_MORNING_START   = dttime(hour=9, minute=30, second=0)
ASTOCK_MORNING_END     = dttime(hour=11, minute=30, second=0)

ASTOCK_AFTERNOON_START = dttime(hour=13, minute=0, second=0)
ASTOCK_AFTERNOON_END   = dttime(hour=15, minute=0, second=0)

CALANDER: pd.DataFrame = pd.read_csv("./storage/calander.csv")
engine = pyttsx3.init()

def text_to_speech(text):
    # 初始化语音引擎
    
    # 将文本输入到引擎
    engine.say(text)
    
    # 等待语音播放完成
    engine.runAndWait()

from qywxbot import qywx as wx

bot = wx.Bot()

def log(text: str) -> None:

    speech = Thread(target=text_to_speech, args=(text,))
    send = Thread(target=bot.send_msg, args=(text,))
    
    speech.start()
    send.start()
    
    speech.join()
    send.join()

def is_trader_period(t: dttime) -> bool:

    if config.enable_test:
        # 无视时间测试
        return True
        
    return ASTOCK_MORNING_START <= t <= ASTOCK_MORNING_END or \
            ASTOCK_AFTERNOON_START <= t <= ASTOCK_AFTERNOON_END

def generate_second_dataseq() -> List[dttime]:
    '''
    产生大A交易时间的时间序列
    '''
    # TODO: 后续增加对today的设置
    s = Datetime.combine(Datetime.today(), ASTOCK_MORNING_START)
    e = Datetime.combine(Datetime.today(), ASTOCK_MORNING_END)
    seqs = []

    while s <= e:
        seqs.append(s)
        s += timedelta(minutes=1) # 一分钟的时间周期

    s = Datetime.combine(Datetime.today(), ASTOCK_AFTERNOON_START)
    e = Datetime.combine(Datetime.today(), ASTOCK_AFTERNOON_END)

    while s <= e:
        seqs.append(s)
        s += timedelta(seconds=1)

    return seqs


NAME2CODE: Optional[pd.DataFrame] = None
CALANDER: pd.DataFrame = pd.read_csv("./storage/calander.csv")

def load():
    '''
    加载所有storage文件
    '''
    global NAME2CODE
    NAME2CODE = pd.read_csv("storage/name2code.csv")
    
name2code = {}

def check():
    global name2code

    for (idx, line) in NAME2CODE.iterrows():
        name2code[line["name"]] = line["ts_code"]
        
    # 检查config中的股票是否能找到股票代码
    for (stockname, desc) in config.STOCKS:

        if name2code.get(stockname) is None:
            raise RuntimeError(f"can't found code for name {stockname}")


from collections import defaultdict

class EmuApi:
    def __init__(self) -> None:
        self.timeseqs = generate_second_dataseq()
        self.counter: Dict[str, int] = defaultdict(lambda: 0)
        # 今天的真实数据
        self.cache: Dict[str, pd.DataFrame] = {}
    
    def feed(self, sec: str) -> pd.Series:
        '''
        获得小于等于本次查询时间的数据，每查询一次就增加一次时间
        '''
        emu_time = self.timeseqs[self.counter[sec]]
        self.counter[sec] += 1

        if self.cache.get(sec) is None:
            self.cache[sec] = ashare.api.query_data_in_day(security=sec)
        
        data = self.cache[sec]
        return data[data.index <= emu_time]

if config.enable_test:
    emuapi = EmuApi()

def query_intraday(security : str) -> pd.DataFrame:

    # TODO: 模拟接口
    if not config.enable_test:
        return ashare.api.query_data_in_day(
            security=security
        )
    else:
        return emuapi.feed(sec=security)


def on_data(datas: Dict[str, pd.DataFrame]):
    logger.debug(f"ondata {datas}")
    pass


def poll():

    interval = 1 if config.enable_test else 60

    while is_trader_period(Datetime.now().time()):

        start = time.time()

        datas = {}
        for stock in config.STOCKS:

            code = name2code[stock[0]]
            # reformat
            nr, sfx = code.split(".")
            code = sfx.lower() + nr

            data = query_intraday(code)

            # 这里只会拿到当天的数据
            datas[code] = data

        # 回调函数
        on_data(datas)
        
        end = time.time()

        elapse = end - start

        time.sleep(interval - elapse)

def today_is_open() -> bool:
    '''
    判断今天赌场是不是开门
    '''

    if config.enable_test:
        return True
    
    now = Datetime.now()
    nows = now.strftime(r"%Y%m%d")
    
    return CALANDER.loc[CALANDER['cal_date'].astype(str) == nows, 'is_open'].values[0]

if config.enable_test:
    logger.warning("testing now")


def main():

    load()
    check()

    if config.enable_test:
        logger.info("test enabled")
    
    while True:
        
        if today_is_open():

            poll()
    
            logger.info("收盘")

        time.sleep(60)

if __name__ == "__main__":
    main()