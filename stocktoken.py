class StockToken:

    def __init__(self, code: str, name: str="", industry: str="") -> None:
        a, b = code.split(".")

        self._name    = name
        self.industry = industry

        if a.lower() in ["sh", "sz"]:
            self.exchange = a
            self.code = b
            
        elif b.lower() in ["sh", "sz"]:
            self.exchange = b
            self.code = a
            
        else:
            raise RuntimeError("illegal stock code") # TODO:
    
    def __repr__(self) -> str:
        return self.repr()
    
    def repr(self, exchg_as_suffix=True, exchg_as_upper=True) -> str:
        '''
        变成代码+交易所后缀的形式
        '''

        exchg = self.exchange.upper() \
                    if exchg_as_upper \
                    else self.exchange.lower()

        if exchg_as_suffix:
            return self.code + "." + exchg
        else:
            return exchg + "." + self.code
    
    @property
    def name(self) -> str:
        return self._name