import numpy as np
import pandas as pd

import statsmodels.api as sm
from statsmodels.stats.power import tt_ind_solve_power, GofChisquarePower
from statsmodels.stats.gof import chisquare_effectsize
from statsmodels.sandbox.stats.multicomp import multipletests
from scipy.stats import norm, chi2_contingency

def get_bootstrap(
    data_column_1, # числовые значения первой выборки
    data_column_2, # числовые значения второй выборки
    boot_it = 1000, # количество бутстрэп-подвыборок
    statistic = np.mean, # интересующая нас статистика
    bootstrap_conf_level = 0.99 # уровень значимости
):
    
    '''Bootstrap для непрерывной метрики'''
    
    boot_data = []
    for i in range(boot_it): # извлекаем подвыборки
#    for i in tqdm(range(boot_it)): # извлекаем подвыборки
        samples_1 = data_column_1.sample(
            len(data_column_1), 
            replace = True # параметр возвращения
        ).values
        
        samples_2 = data_column_2.sample(
            len(data_column_1), 
            replace = True
        ).values
        
        boot_data.append(statistic(samples_1)-statistic(samples_2)) # mean() - применяем статистику
        
    pd_boot_data = pd.DataFrame(boot_data)
        
    left_quant = (1 - bootstrap_conf_level)/2
    right_quant = 1 - (1 - bootstrap_conf_level) / 2
    quants = pd_boot_data.quantile([left_quant, right_quant])
        
    p_1 = norm.cdf(
        x = 0, 
        loc = np.mean(boot_data), 
        scale = np.std(boot_data)
    )
    p_2 = norm.cdf(
        x = 0, 
        loc = -np.mean(boot_data), 
        scale = np.std(boot_data)
    )
    p_value = min(p_1, p_2) * 2
       
    return {"boot_data": boot_data, 
            "quants": quants, 
            "p_value": p_value}

def bootstrap_ratio(
        data: pd.DataFrame,
        x: str,
        y: str,
        x_f,
        y_f,
        split='EXP_GROUP',
        user_level_col='DRIVER_RK',
        boot_it=1000,
        conf_level=0.99):
    
    '''Bootstrap для метрики отношения'''
    
    data = data.sort_values(by=[split], ascending=[True])
    
    data_splitted = [x for _, x in data.groupby(split)]
    boot_data = []

    for i in range(boot_it):
        s0 = data_splitted[0][data_splitted[0][user_level_col].isin(
            data_splitted[0][user_level_col].sample(data_splitted[0][user_level_col].nunique(), replace=True))]
        s1 = data_splitted[1][data_splitted[1][user_level_col].isin(
            data_splitted[1][user_level_col].sample(data_splitted[1][user_level_col].nunique(), replace=True))]

        y0 = y_f(s0[y])
        y1 = y_f(s1[y])
        x0 = x_f(s0[x])
        x1 = x_f(s1[x])

        if y0 == 0 or y1 == 0:
            return None, False
        elif x0 == 0 or x1 == 0:
            return None, False

        s0_ratio = x0 / y0
        s1_ratio = x1 / y1
        boot_data.append(s1_ratio - s0_ratio)

    pd_boot_data = pd.DataFrame(boot_data)

    p_1 = norm.cdf(x=0, loc=np.mean(boot_data), scale=np.std(boot_data))
    p_2 = norm.cdf(x=0, loc=-np.mean(boot_data), scale=np.std(boot_data))
    pvalue = min(p_1, p_2) * 2
    mark = (pvalue < 1 - conf_level)

    return pvalue, mark

def stat_res_calculation(metric: str, m_type: str, df: pd.DataFrame, splits: list, cfg: dict):
    '''Возвращает массив со всеми расчитанными метриками'''
    
    split_1, split_2 = splits
    
    if m_type == 'binomial':
        a_s = len(df[(df['EXP_GROUP'] == split_1)&(df[cfg['metrics'][metric]['options']['x'][0]] > 0)])
        a_f = len(df[(df['EXP_GROUP'] == split_1)&(df[cfg['metrics'][metric]['options']['x'][0]] == 0)])
        b_s = len(df[(df['EXP_GROUP'] == split_2)&(df[cfg['metrics'][metric]['options']['x'][0]] > 0)])
        b_f = len(df[(df['EXP_GROUP'] == split_2)&(df[cfg['metrics'][metric]['options']['x'][0]] == 0)])
        
        act_nobs_a, act_nobs_b = a_s+a_f, b_s+b_f
        successes, fails = [a_s, b_s], [a_f, b_f]
        probs0, probs1 = np.array([a_s, a_f]), np.array([b_s, b_f])
        effect_size = chisquare_effectsize(probs0, probs1, correction=None, cohen=True, axis=0)
        
        p_val = chi2_contingency(np.array([fails, successes]))[1]
        mde = int(GofChisquarePower().solve_power(effect_size=effect_size, alpha=cfg.get('alpha'),
                                                  power=cfg.get('power'), n_bins=2))
        mde_marker_value = 1 if (act_nobs_a>=mde)&(act_nobs_b>=mde) else np.nan
        
        a = a_s/act_nobs_a
        b = b_s/act_nobs_b
        f = cfg['metrics'][metric]['options']['n'][1]
        a_drivers = df[df['EXP_GROUP'] == split_1]['DRIVER_RK'].nunique()
        b_drivers = df[df['EXP_GROUP'] == split_2]['DRIVER_RK'].nunique()
        lift = b-a

        
    elif m_type == 'ratio':
        x = cfg['metrics'][metric]['options']['x'][0]
        x_f = cfg['metrics'][metric]['options']['x'][1]
        y = cfg['metrics'][metric]['options']['y'][0]
        y_f = cfg['metrics'][metric]['options']['y'][1]
        
        f = cfg['metrics'][metric]['options']['n'][1]
        act_nobs_a = f(df[df['EXP_GROUP'] == split_1][cfg['metrics'][metric]['options']['n'][0]])
        act_nobs_b = f(df[df['EXP_GROUP'] == split_2][cfg['metrics'][metric]['options']['n'][0]])
        
        mde_ttl = df.groupby('EXP_GROUP').mean().reset_index()
        mean = x_f(df[df['EXP_GROUP']==split_1][x]) / y_f(df[df['EXP_GROUP']==split_1][y])
        sd = pd.Series(map(np.divide, df[df['EXP_GROUP']==split_1][x], df[df['EXP_GROUP']==split_1][y])).std()
        lift = mde_ttl[x][1] / mde_ttl[x][0]-1
        effect_size = mean / sd * lift
        
        p_val = bootstrap_ratio(data=df, x=x, y=y, x_f=x_f, y_f=y_f)[0]
        mde = int(tt_ind_solve_power(effect_size=effect_size, alpha=cfg.get('alpha'),
                                     power=cfg.get('power'), nobs1=None, ratio=1))
        mde_marker_value = 1 if (act_nobs_a>=mde)&(act_nobs_b>=mde) else np.nan
        
        a = x_f(df[df['EXP_GROUP']==split_1][x]) / y_f(df[df['EXP_GROUP']==split_1][y])
        b = x_f(df[df['EXP_GROUP']==split_2][x]) / y_f(df[df['EXP_GROUP']==split_2][y])
        a_drivers, b_drivers = act_nobs_a, act_nobs_b
        lift = (b-a)/abs(a)
        
    elif m_type == 'continious':
        x = cfg['metrics'][metric]['options']['x'][0]
        f = cfg['metrics'][metric]['options']['x'][1]
        f_n = cfg['metrics'][metric]['options']['n'][1]

        a = f(df[df['EXP_GROUP']==split_1][x])
        b = f(df[df['EXP_GROUP']==split_2][x])



        act_nobs_a = f_n(df[df['EXP_GROUP'] == split_1][cfg['metrics'][metric]['options']['n'][0]])
        act_nobs_b = f_n(df[df['EXP_GROUP'] == split_2][cfg['metrics'][metric]['options']['n'][0]])

        #mde_ttl = df.groupby('EXP_GROUP').mean().reset_index()
        mean = df[df['EXP_GROUP']==split_1][x].mean()
        sd = df[df['EXP_GROUP']==split_1][x].std()
        lift = b / a - 1
        effect_size = mean / sd * lift

        booted_data_ab = get_bootstrap(data_column_1=df[df['EXP_GROUP']==split_1][x], # числовые значения первой выборки
                                       data_column_2=df[df['EXP_GROUP']==split_2][x], # числовые значения второй выборки
                                       statistic = f)
        p_val = booted_data_ab["p_value"]
        q_low = float(booted_data_ab["quants"].iloc[0])
        q_high = float(booted_data_ab["quants"].iloc[1])

        if q_low < 0 and q_high > 0:
            p_val = 1
        else:
            p_val = p_val

        try:
            mde = int(tt_ind_solve_power(effect_size=effect_size, alpha=cfg.get('alpha'),
                                         power=cfg.get('power'), nobs1=None, ratio=1))
        except:
            mde = 990999999
        mde_marker_value = 1 if (act_nobs_a>=mde)&(act_nobs_b>=mde) else np.nan

        a_drivers = df[df['EXP_GROUP'] == split_1]['DRIVER_RK'].nunique()
        b_drivers = df[df['EXP_GROUP'] == split_2]['DRIVER_RK'].nunique()

    else:
        return('Unknown metric type')
    
    nobs_diff_value = (act_nobs_b-act_nobs_a)/abs(act_nobs_a)
    dn_diff_value = (b_drivers-a_drivers)/abs(a_drivers)
    
    return({'p_val':p_val, 'nobs_needed':mde, 'mde_ok':mde_marker_value,
            'split_a':a, 'split_b':b, 'lift':lift, 'split_a_n_obs':act_nobs_a, 'split_b_n_obs':act_nobs_b,
            'n_obs_difference':nobs_diff_value, 'split_a_drivers':a_drivers, 'split_b_drivers':b_drivers,
            'unique_drivers_difference':dn_diff_value})

def mde_calculation(metric: str, m_type: str, df: pd.DataFrame, lift: dict, cfg: dict):
    '''Рассчитывает необходимое количество наблюдений для указанного в массиве lift_dict размера лифта'''
    
    if m_type == 'binomial':
        a_s = len(df[(df[cfg['metrics'][metric]['options']['x'][0]] > 0)])
        a_f = len(df[(df[cfg['metrics'][metric]['options']['x'][0]] == 0)])
        a_ttl = a_s + a_f
        b_s = int(a_ttl * (a_s / a_ttl + lift))
        probs0, probs1 = np.array([a_s, a_f]), np.array([b_s, a_ttl - b_s])
        effect_size = chisquare_effectsize(probs0, probs1, correction=None, cohen=True, axis=0)
        mde = int(GofChisquarePower().solve_power(effect_size=effect_size, alpha=cfg.get('alpha'),
                                                  power=cfg.get('power'), n_bins=2))

        
    elif m_type == 'ratio':
        x = cfg['metrics'][metric]['options']['x'][0]
        x_f = cfg['metrics'][metric]['options']['x'][1]
        y = cfg['metrics'][metric]['options']['y'][0]
        y_f = cfg['metrics'][metric]['options']['y'][1]

        mean = x_f(df[x]) / y_f(df[y])
        sd = pd.Series(map(np.divide, df[x], df[y])).std()
        effect_size = mean / sd * lift

        mde = int(tt_ind_solve_power(effect_size=effect_size, alpha=cfg.get('alpha'),
                                     power=cfg.get('power'), nobs1=None, ratio=1))
        
    elif m_type == 'continious':
        x = cfg['metrics'][metric]['options']['x'][0]
        f = cfg['metrics'][metric]['options']['x'][1]
        f_n = cfg['metrics'][metric]['options']['n'][1]

        mean = df[x].mean()
        sd = df[x].std()
        effect_size = mean / sd * lift

        try:
            mde = int(tt_ind_solve_power(effect_size=effect_size, alpha=cfg.get('alpha'),
                                         power=cfg.get('power'), nobs1=None, ratio=1))
        except:
            mde = 999999999

    else:
        return('Unknown metric type')
    
    return(mde)