import streamlit as st
from streamlit_modal import Modal
from streamlit_cookies_manager import EncryptedCookieManager
from code_editor import code_editor
from decimal import Decimal
from dotenv import dotenv_values
import pandas as pd
import statsmodels.stats.multitest
import scipy.stats as stats
import numpy as np
import scipy.special as special
from statsmodels.stats.power import TTestIndPower
import ast

from sql_worker import SqlWorker


secrets: dict = dotenv_values(".env")
cookies = EncryptedCookieManager(
    prefix=secrets["cookie_prefix"],
    password=secrets["cookie_password"]
)

if not cookies.ready():
    st.stop()

def calc_stats(mean_0, mean_1, var_0, var_1, len_0, len_1, alpha=0.05, required_power=0.8, pvalue=None, calc_mean=False):
    std = np.sqrt(var_0 / len_0 + var_1 / len_1)
    mean_abs = abs(mean_1 - mean_0)
    mean = mean_1 - mean_0
    sd = np.sqrt((var_0 * len_0 + var_1 * len_1) / 
                 (len_0 + len_1 - 2))

    if pvalue is None:
        pvalue = stats.norm.cdf(x=0, loc=mean_abs, scale=std) * 2
    elif calc_mean == False:
        std_corrected = np.abs(special.nrdtrisd(0, pvalue / 2, mean_abs))
        sd *= 1 + (std_corrected - std) / std
        std = std_corrected
    else:
        mean_abs = special.nrdtrimn(pvalue / 2, std, 0)
        mean = mean_abs
        if mean_0 > mean_1:
            mean *= -1

    # cohen_d = mean_abs / sd
    # bound_value = special.nrdtrimn(alpha / 2, std, 0)
    # power = 1 - (stats.norm.cdf(x=bound_value, loc=mean_abs, scale=std) - 
    #              stats.norm.cdf(x=-bound_value, loc=mean_abs, scale=std))
    # analysis = TTestIndPower()
    # # todo: добавить обработчик для нуля
    # sample_size = analysis.solve_power(cohen_d, power=required_power, 
    #                               nobs1=None, alpha=alpha)

    # return {"pvalue": pvalue, "power": power, 
    #         "cohen_d": cohen_d, "sample_size": np.ceil(sample_size), 
    #         "enough": sample_size <= min(len_0, len_1),
    #         "ci": [np.array([stats.norm.ppf(alpha / 2, mean_abs, std), 
    #                stats.norm.ppf(1 - alpha / 2, mean_abs, std)])]}
    return {"pvalue": pvalue}


sql_worker: SqlWorker = SqlWorker()
@st.cache_data
def load_data_h():
    return sql_worker.get_data_h()

@st.cache_data
def load_data_d():
    return sql_worker.get_data_d()

df_h = load_data_h()
df_d = load_data_d()

@st.cache_data
def calc_data_h():
    df_h['hour'] = pd.to_datetime(df_h['hour'], unit='s')
    source_list = df_h['source'].unique()
    event_list = df_h['event'].unique()
    alpha = 0.01
    alerts = []
    for source in source_list:
        for event in event_list:
            df_slice = df_h.loc[(df_h['source'] == source) & (df_h['event'] == event)].sort_values('hour').reset_index(drop=True)
            if df_slice.shape[0] != 2:
                continue
            nobs_1, succ_1 = df_slice['dau'][0], df_slice['unified_cnt'][0]
            nobs_2, succ_2 = df_slice['dau'][1], df_slice['unified_cnt'][1]
            if nobs_1 == 0:
                continue
            result = calc_stats(succ_1/nobs_1, succ_2/nobs_2, (succ_1/nobs_1)*(1-succ_1/nobs_1), (succ_2/nobs_2)*(1-succ_2/nobs_2), nobs_1, nobs_2)
            change_sign = '↑'
            if result['pvalue'] < alpha:
                if nobs_2/succ_2 < succ_1/nobs_1:
                    change_sign = '↓'
                alerts.append({(source, event): [change_sign, round(result['pvalue'], 3), [succ_1, nobs_1], [succ_2, nobs_2]]})
    return alerts

@st.cache_data
def calc_data_d():
    print(df_d)
    df_d['date'] = pd.to_datetime(df_d['date'], unit='s')
    source_list = df_d['source'].unique()
    event_list = df_d['event'].unique()
    alpha = 0.01
    alerts = []
    for source in source_list:
        for event in event_list:
            df_slice = df_d.loc[(df_d['source'] == source) & (df_d['event'] == event)].sort_values('date').reset_index(drop=True)
            if df_slice.shape[0] != 2:
                continue
            nobs_1, succ_1 = df_slice['dau'][0], df_slice['unified_cnt'][0]
            nobs_2, succ_2 = df_slice['dau'][1], df_slice['unified_cnt'][1]
            if nobs_1 == 0:
                continue
            result = calc_stats(succ_1/nobs_1, succ_2/nobs_2, (succ_1/nobs_1)*(1-succ_1/nobs_1), (succ_2/nobs_2)*(1-succ_2/nobs_2), nobs_1, nobs_2)
            change_sign = '↑'
            if result['pvalue'] < alpha:
                if nobs_2/succ_2 < succ_1/nobs_1:
                    change_sign = '↓'
                alerts.append({(source, event): [change_sign, round(result['pvalue'], 3), [succ_1, nobs_1], [succ_2, nobs_2]]})
    return alerts

def display_h(selected_platforms, selected_events):
    alerts_h = calc_data_h()
    processed_data = []
    for item in alerts_h:
        for key, value in item.items():
            platform = key[0]
            event = key[1]
            arrow = value[0]
            p_value = value[1]
            events1 = f"{value[2][0]} / {value[2][1]}"
            events2 = f"{value[3][0]} / {value[3][1]}"
            processed_data.append({
                'Platform': platform,
                'Event': event,
                'Change': arrow,
                'p-value': p_value,
                'Events / DAU last week': events1,
                'Events / DAU this week': events2
            })

    display_df = pd.DataFrame(processed_data)

    # unique_platforms = sorted(display_df['Platform'].unique())
    # unique_events = sorted(display_df['Event'].unique())

    # st.sidebar.header('Observed Events')
    # selected_platforms = st.sidebar.multiselect('Select Platform(s)', unique_platforms)
    # selected_events = st.sidebar.multiselect('Select Event(s)', unique_events)
    mask = pd.Series(True, index=display_df.index)
    if selected_platforms:
        mask = mask & display_df['Platform'].isin(selected_platforms)
    if selected_events:
        mask = mask & display_df['Event'].isin(selected_events)

    filtered_df = display_df[mask]

    filtered_df = filtered_df.sort_values(by=['Platform', 'Event'])

    filtered_df = filtered_df[['Platform', 'Event', 'Change', 'p-value', 'Events / DAU last week', 'Events / DAU this week']]

    st.caption('Last Hour')
    st.dataframe(filtered_df)


def display_d(selected_platforms, selected_events):
    alerts_d = calc_data_d()
    processed_data = []
    for item in alerts_d:
        for key, value in item.items():
            platform = key[0]
            event = key[1]
            arrow = value[0]
            p_value = value[1]
            events1 = f"{value[2][0]} / {value[2][1]}"
            events2 = f"{value[3][0]} / {value[3][1]}"
            processed_data.append({
                'Platform': platform,
                'Event': event,
                'Change': arrow,
                'p-value': p_value,
                'Events / DAU last week': events1,
                'Events / DAU this week': events2
            })

    display_df = pd.DataFrame(processed_data)

    # unique_platforms = sorted(display_df['Platform'].unique())
    # unique_events = sorted(display_df['Event'].unique())

    # st.sidebar.header('Observed Events')
    # selected_platforms = st.sidebar.multiselect('Select Platform(s)', unique_platforms)
    # selected_events = st.sidebar.multiselect('Select Event(s)', unique_events)
    mask = pd.Series(True, index=display_df.index)
    if selected_platforms:
        mask = mask & display_df['Platform'].isin(selected_platforms)
    if selected_events:
        mask = mask & display_df['Event'].isin(selected_events)

    filtered_df = display_df[mask]

    filtered_df = filtered_df.sort_values(by=['Platform', 'Event'])

    filtered_df = filtered_df[['Platform', 'Event', 'Change', 'p-value', 'Events / DAU last week', 'Events / DAU this week']]

    st.caption('Today')
    st.dataframe(filtered_df)

@st.cache_data
def load_saved_filters():
    saved_platforms = cookies.get('selected_platforms')
    print('selected_platforms=', cookies.get('selected_platforms'))
    saved_events = cookies.get('selected_events')
    if saved_platforms:
        saved_platforms = ast.literal_eval(saved_platforms)
    else:
        saved_platforms = []
    if saved_events:
        saved_events = ast.literal_eval(saved_events)
    else:
        saved_events = []
    
    return saved_platforms, saved_events


st.title('Alerts')
st.sidebar.header('Observed Events')
saved_platforms, saved_events = load_saved_filters()
selected_platforms = st.sidebar.multiselect(
    'Select Platforms', df_d['source'].unique(), default=saved_platforms
)
selected_events = st.sidebar.multiselect(
    'Select Events', df_d['event'].unique(), default=saved_events
)
# selected_platforms = st.sidebar.multiselect('Select Platform(s)', df_d['source'].unique())
# selected_events = st.sidebar.multiselect('Select Event(s)', df_d['event'].unique())
display_h(selected_platforms, selected_events)
display_d(selected_platforms, selected_events)

print(cookies.get('selected_platforms'))

cookies['selected_platforms'] = str(selected_platforms)
cookies['selected_events'] = str(selected_events)
cookies.save()

print(cookies.get('selected_platforms'))
