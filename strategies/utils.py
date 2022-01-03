import numpy as np


def cal_mdd(value):
    return np.max(np.maximum.accumulate(value) - value)


def cal_sharpe(value, base=100):

    ret = np.mean(np.diff(value) / base) * 1440 * 365
    std = np.std(np.diff(value) / base) * np.sqrt(1440 * 365)

    return ret / std
