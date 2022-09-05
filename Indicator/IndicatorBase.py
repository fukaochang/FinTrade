from enum import Enum

class ECalcType(Enum):
    """
        技术指标技术方式类
    """
    """使用talib透传技术指标计算"""
    E_FROM_TA = 0
    """使用pandas等库实现技术指标计算"""
    E_FROM_PD = 1

"""
    彻底不用talib，完全都使用自己计算的指标结果
"""
g_calc_type = ECalcType.E_FROM_PD



