from collections import Counter
import math
import scipy.stats as ss
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

from statsmodels.stats.outliers_influence import variance_inflation_factor


def conditional_entropy(x,y):
    # entropy of x given y
    y_counter = Counter(y)
    xy_counter = Counter(list(zip(x,y)))
    total_occurrences = sum(y_counter.values())
    entropy = 0
    for xy in xy_counter.keys():
        p_xy = xy_counter[xy] / total_occurrences
        p_y = y_counter[xy[1]] / total_occurrences
        entropy += p_xy * math.log(p_y/p_xy)
    return entropy

def theil_u(x,y):
    s_xy = conditional_entropy(x,y)
    x_counter = Counter(x)
    total_occurrences = sum(x_counter.values())
    p_x = list(map(lambda n: n/total_occurrences, x_counter.values()))
    s_x = ss.entropy(p_x)
    if s_x == 0:
        return 1
    else:
        return (s_x - s_xy) / s_x

def cat_heat_map(df, cols):
    theilu = pd.DataFrame(index = cols, columns = cols)

    for i in range(0, len(cols)):
        for j in range(0, len(cols)):
            if i == j:
                theilu.loc[cols[i], cols[j]] = 1.0
            
            u = theil_u(df[cols[i]].tolist(),df[cols[j]].tolist())
            theilu.loc[cols[i], cols[j]] = u
    
    theilu.fillna(value = np.nan, inplace = True)
    plt.figure(figsize = (12, 12))
    sns.heatmap(theilu, annot = True, fmt = '.2f')
    plt.show()


def calc_vif(X):

    # Calculating VIF
    vif = pd.DataFrame()
    vif["variables"] = X.columns
    vif["VIF"] = [variance_inflation_factor(X.values, i) for i in range(X.shape[1])]

    return(vif)