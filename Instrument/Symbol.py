

class Symbol(object):
    def __init__(self, market, sub_market, symbol_code):
        """
        :param market: EMarketTargetType enum对象
        :param sub_market: EMarketSubType enum对象
        :param symbol_code: str对象，不包含市场信息的code
        """

        self.market = market
        self.sub_market = sub_market
        self.symbol_code = symbol_code
        self.source = None
        # else:
        #     raise TypeError('market type error')

    def __str__(self):
        """打印对象显示：market， sub_market， symbol_code"""
        return '{}_{}:{}'.format(self.market.value, self.sub_market.value, self.symbol_code)

    __repr__ = __str__

    def __len__(self):
        """对象长度：拼接市场＋子市场＋code的字符串长度"""
        m_symbol = '{}_{}:{}'.format(self.market.value, self.sub_market.value, self.symbol_code)
        return len(m_symbol)
 # @LazyFunc
    def value(self):
            return '{}{}'.format(self.market.value, self.symbol_code)
