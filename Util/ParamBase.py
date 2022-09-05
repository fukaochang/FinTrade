from inspect import signature, Parameter
import pandas as pd


class ParamBase(object):
    """对象基础类，实现对象基本信息打印，调试查看接口"""

    @classmethod
    def get_params(cls):
        # init中特意找了被类装饰器替换了的deprecated_original方法，即原始init方法
        init = getattr(cls.__init__, 'deprecated_original', cls.__init__)
        if init is object.__init__:
            # 非自定义init返回空
            return list()
        # 获取init的参数签名
        init_signature = signature(init)
        # 过滤self和func(*args), 和func(**kwargs)
        parameters = [p for p in init_signature.parameters.values()
                      if p.name != 'self' and p.kind != Parameter.VAR_KEYWORD and p.kind != Parameter.VAR_POSITIONAL]
        return sorted([p.name for p in parameters])

    def _filter_attr(self, user):
        """根据user设置，返回所有类属性key或者用户定义类属性key"""
        if not user:
            return self.__dict__.keys()

        # 只筛选用户定义类属性key
        user_attr = list(filter(
            lambda attr: not attr.startswith('_'), self.__dict__.keys()))
        return user_attr

    def to_dict(self, user=True):
        """for debug show dict"""
        return {attr: self.__dict__[attr] for attr in self._filter_attr(user)}

    def to_series(self, user=True):
        """for notebook debug show series"""
        return pd.Series(self.to_dict(user))

    def __str__(self):
        """打印对象显示：class name, params"""
        class_name = self.__class__.__name__
        return '%s(%s)' % (class_name, self.get_params())

    __repr__ = __str__