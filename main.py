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
    # logger.debug("say over")
    engine.runAndWait()
    # logger.debug("text_to_speech over")
    return

from qywxbot import qywx as wx

bot = wx.Bot()

def bot_log(text: str, time: Datetime) -> None:
    now = time.strftime(r"%Y-%m-%d,%H:%M:%S")
    text = f"[{now}] " + text
    bot.send_msg(text)

def log(text: str, time: Datetime) -> None:

    # speech = Thread(target=text_to_speech, args=(text,))
    # send = Thread(target=bot.send_msg, args=(text,))
    
    # speech.start()
    # send.start()
    
    # logger.debug("start over")
    # speech.join()
    # logger.debug("speech join over")
    # send.join()
    # logger.debug("log over")
    bot_log(text, time)
    text_to_speech(text)

def is_trader_period(t: dttime) -> bool:
    '''
    打开test后无视时间限制
    '''

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
code2name = {}

def check():
    global name2code
    global code2name

    for (idx, line) in NAME2CODE.iterrows():
        name2code[line["name"]] = line["ts_code"]
        code2name[line["ts_code"]] = line["name"]
        
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
    
    def feed(self, secu: str) -> pd.Series:
        '''
        获得小于等于本次查询时间的数据，每查询一次就增加一次时间
        '''
        emu_time = self.timeseqs[self.counter[secu]]
        self.counter[secu] += 1

        day = Datetime.now() - timedelta(days=1) # TODO:

        if self.cache.get(secu) is None:
            self.cache[secu] = ashare.api.query_data_in_day(security=secu, day=day)
        
        data = self.cache[secu]
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
        return emuapi.feed(secu=security)

def slopeize(s: pd.Series) -> pd.Series:
    '''
    生成这个序列的增长斜率
    (最后一个元素的收益率 - 前面所有元素的收益率) * 1000 / 两者间的索引间隔
    '''
    if s.empty:
        raise RuntimeError
    
    # s = (s.pct_change() + 1).cumprod()
    s = pd.DataFrame({"pctchg": s.pct_change() + 1})["pctchg"].cumprod() * 1000 * 10

    ret = []
    length = len(s)
    last = s[-1]

    for (idx, elem) in enumerate(s):

        if idx + 1 == length:
            ret.append(0)
            
        # elif idx == 0:
        #     ret.append(0)

        else:
            ret.append(
                (last - elem) / (length - 1 - idx)
            )
    
    return pd.Series(ret)

def on_data(datas: Dict[str, pd.DataFrame]):
    # logger.debug(f"ondata {datas}")
    '''
    得到一个股票集合
    '''
    for code, data in datas.items():

        if data.empty:
            RuntimeError

        close_max = data["close"].max()
        close_min = data["close"].min()

        # logger.debug(data["close"])
        close_cur = data["close"].iloc[-1]

        close_at_min = close_cur == close_min
        close_at_max = close_cur == close_max

        # 增长斜率
        # TODO: 反复计算了 需要缓存
        slopes = []
        for i in range(len(data["close"])):
            slopes.append(
                slopeize(data["close"][:i+1]).max()
            )

        # NOTE: 默认创造的RangeIndex不能用-1去索引
        slopes = pd.Series(slopes, index=data.index)

        # TODO 要处理这种转换很麻烦 需要标准化
        prefix = code[0:2]
        nr = code[2:]
        stock = nr + "." + prefix.upper()

        name = code2name[stock]

        if close_at_max:
            log(f"{name} 达到最高点 {close_max}", data.index[-1])

        elif close_at_min:
            log(f"{name} 达到最低点 {close_min}", data.index[-1])

        elif close_cur < close_at_max and len(data["close"]) >= 3 and close_at_max == data["close"].iloc[-3:].max():
            # 最近处于最低点
            log(f"{name} 当前价格 {close_cur} 开始从最高点 {close_max} 跌下", data.index[-1])
        
        elif close_cur > close_at_min and len(data["close"]) >= 3 and close_at_min == data["close"].iloc[-3:].min():
            log(f"{name} 当前价格 {close_cur} 开始从最低点 {close_min} 爬升", data.index[-1])

        else:
            pass
        
        volume_cur = data["volume"][-1]
        if len(data["close"]) >= 60: # 出现60个bar后再动手 也就是60分钟
            if volume_cur >= data["volume"].quantile(0.80):
                # 放量百分位超过80%
                logger.debug(f"{name}: {slopes}")
                if data["close"].iloc[-1] >= data["close"].quantile(0.90): # 当前的价格超过90%的价格
                    # 斜率接近最高点
                    if slopes.iloc[-1] == slopes.quantile(0.90):
                        log(f"请注意 : {name} 放量上升, 斜率为 {slopes.iloc[-1]:.2f}, 有可能达到最高点", data.index[-1])



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

        time.sleep(interval - elapse if interval - elapse > 0 else 0)

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