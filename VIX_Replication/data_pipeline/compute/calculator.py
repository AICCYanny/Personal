import numpy as np
import pandas as pd

def calc_K0(call_data_near_term, put_data_near_term, call_data_next_term, put_data_next_term, r1, r2, T1, T2):
    '''
    call_data_near_term['diff'] = abs(call_data_near_term['mid'] - put_data_near_term['mid'])
    call_data_next_term['diff'] = abs(call_data_next_term['mid'] - put_data_next_term['mid'])
    '''
    
    # Filter rows where both prices are not zero
    valid_near_term = (call_data_near_term['mid'] != 0) & (put_data_near_term['mid'] != 0)
    call_data_near_term['diff'] = float('inf')  # Initialize with a high value to exclude invalid rows
    call_data_near_term.loc[valid_near_term, 'diff'] = abs(call_data_near_term.loc[valid_near_term, 'mid'] - put_data_near_term.loc[valid_near_term, 'mid'])
    
    # Filter rows where both prices are not zero for next term
    valid_next_term = (call_data_next_term['mid'] != 0) & (put_data_next_term['mid'] != 0)
    call_data_next_term['diff'] = float('inf')  # Initialize with a high value to exclude invalid rows
    call_data_next_term.loc[valid_next_term, 'diff'] = abs(call_data_next_term.loc[valid_next_term, 'mid'] - put_data_next_term.loc[valid_next_term, 'mid'])

    strike_near_term = call_data_near_term.loc[call_data_near_term[call_data_near_term['diff'] != 0]['diff'].idxmin(), 'strike']
    strike_next_term = call_data_next_term.loc[call_data_next_term[call_data_next_term['diff'] != 0]['diff'].idxmin(), 'strike']

    forward_near_term = (strike_near_term + np.exp(r1*T1) * 
                         (call_data_near_term.loc[call_data_near_term['strike']==strike_near_term, 'mid'].values[0] - 
                                                            put_data_near_term.loc[put_data_near_term['strike']==strike_near_term, 'mid'].values[0]))
    forward_next_term = (strike_next_term + np.exp(r2*T2) * 
                         (call_data_next_term.loc[call_data_next_term['strike']==strike_next_term, 'mid'].values[0] - 
                                                            put_data_next_term.loc[put_data_next_term['strike']==strike_next_term, 'mid'].values[0]))

    K0_near_term = call_data_near_term[call_data_near_term['strike'] <= forward_near_term]['strike'].max()
    K0_next_term = call_data_next_term[call_data_next_term['strike'] <= forward_next_term]['strike'].max()
    
    return forward_near_term, forward_next_term, K0_near_term, K0_next_term

def construct_dataframe(call_data_near_term, put_data_near_term, call_data_next_term, put_data_next_term, K0_near_term, K0_next_term):

    filtered_call_near_term = call_data_near_term[call_data_near_term['strike']>=K0_near_term].reset_index(drop=True)
    filtered_put_near_term = put_data_near_term[put_data_near_term['strike']<=K0_near_term].reset_index(drop=True)

    filtered_call_next_term = call_data_next_term[call_data_next_term['strike']>=K0_next_term].reset_index(drop=True)
    filtered_put_next_term = put_data_next_term[put_data_next_term['strike']<=K0_next_term].reset_index(drop=True)

    cutoff_put_near_term = len(filtered_put_near_term)
    put_found_near = False
    
    for i in range(len(filtered_put_near_term) - 2, 0, -1):
        if filtered_put_near_term.loc[i, "bid"] == 0 and filtered_put_near_term.loc[i - 1, "bid"] == 0:
            cutoff_put_near_term = i + 1
            put_found_near = True
            break
    
    if put_found_near:
        result_put_near = filtered_put_near_term.iloc[cutoff_put_near_term:][['cp', 'strike', 'mid']]
    else:
        result_put_near = filtered_put_near_term[['cp', 'strike', 'mid']]
    
    cutoff_call_near_term = len(filtered_call_near_term)
    call_found_near = False
    
    for i in range(1, len(filtered_call_near_term) - 1):
        if filtered_call_near_term.loc[i, "bid"] == 0 and filtered_call_near_term.loc[i + 1, "bid"] == 0:
            cutoff_call_near_term = i
            call_found_near = True
            break
    
    if call_found_near:
        result_call_near = filtered_call_near_term.iloc[:cutoff_call_near_term][['cp', 'strike', 'mid']]
    else:
        result_call_near = filtered_call_near_term[['cp', 'strike', 'mid']]
    
    combined_df_near = pd.concat([result_call_near, result_put_near], ignore_index=True)
    
    row_K0_call_near = result_call_near[result_call_near["strike"] == K0_near_term]
    row_K0_put_near = result_put_near[result_put_near["strike"] == K0_near_term]

    if not row_K0_call_near.empty and not row_K0_put_near.empty:
        average_price_near = (row_K0_call_near["mid"].values[0] + row_K0_put_near["mid"].values[0]) / 2
    elif not row_K0_call_near.empty:
        average_price_near = row_K0_call_near["mid"].values[0]
    elif not row_K0_put_near.empty:
        average_price_near = row_K0_put_near["mid"].values[0]
    else:
        raise ValueError("No valid option at K0 (near term)")
    
    row_K0_combined_near = pd.DataFrame({
        "cp": ["P/C Average"],
        "strike": [K0_near_term],
        "mid": [average_price_near]
    })
    
    combined_df_near = combined_df_near[combined_df_near["strike"] != K0_near_term]
    combined_df_near = pd.concat([combined_df_near, row_K0_combined_near], ignore_index=True)
    combined_df_near = combined_df_near.sort_values(by="strike").reset_index(drop=True)

    cutoff_put_next_term = len(filtered_put_next_term)
    put_found_next = False
    
    for i in range(len(filtered_put_next_term) - 2, 0, -1):
        if filtered_put_next_term.loc[i, "bid"] == 0 and filtered_put_next_term.loc[i - 1, "bid"] == 0:
            cutoff_put_next_term = i + 1
            put_found_next = True
            break
    
    if put_found_next:
        result_put_next = filtered_put_next_term.iloc[cutoff_put_next_term:][['cp', 'strike', 'mid']]
    else:
        result_put_next = filtered_put_next_term[['cp', 'strike', 'mid']]
    
    cutoff_call_next_term = len(filtered_call_next_term)
    call_found_next = False
    
    for i in range(1, len(filtered_call_next_term) - 1):
        if filtered_call_next_term.loc[i, "bid"] == 0 and filtered_call_next_term.loc[i + 1, "bid"] == 0:
            cutoff_call_next_term = i
            call_found_next = True
            break
    
    if call_found_next:
        result_call_next = filtered_call_next_term.iloc[:cutoff_call_next_term][['cp', 'strike', 'mid']]
    else:
        result_call_next = filtered_call_next_term[['cp', 'strike', 'mid']]
    
    combined_df_next = pd.concat([result_call_next, result_put_next], ignore_index=True)
    
    row_K0_call_next = result_call_next[result_call_next["strike"] == K0_next_term]
    row_K0_put_next = result_put_next[result_put_next["strike"] == K0_next_term]

    if not row_K0_call_next.empty and not row_K0_put_next.empty:
        average_price_next = (row_K0_call_next["mid"].values[0] + row_K0_put_next["mid"].values[0]) / 2
    elif not row_K0_call_next.empty:
        average_price_next = row_K0_call_next["mid"].values[0]
    elif not row_K0_put_next.empty:
        average_price_next = row_K0_put_next["mid"].values[0]
    else:
        raise ValueError("No valid option at K0 (next term)")
    
    row_K0_combined_next = pd.DataFrame({
        "cp": ["P/C Average"],
        "strike": [K0_next_term],
        "mid": [average_price_next]
    })
    
    combined_df_next = combined_df_next[combined_df_next["strike"] != K0_next_term]
    combined_df_next = pd.concat([combined_df_next, row_K0_combined_next], ignore_index=True)
    combined_df_next = combined_df_next.sort_values(by="strike").reset_index(drop=True)

    return combined_df_near, combined_df_next

def calc_contribution(df_near_term, df_next_term, r1, r2, T1, T2):
    price_strike_near = df_near_term['strike']
    price_strike_next = df_next_term['strike']

    delta_K_near = []
    delta_K_next = []

    for i in range(len(price_strike_near)):
        if i == 0: 
            delta_K_near.append(abs(price_strike_near[i + 1] - price_strike_near[i]))
        elif i == len(price_strike_near) - 1: 
            delta_K_near.append(abs(price_strike_near[i] - price_strike_near[i - 1]))
        else:  
            delta_K_near.append(abs(price_strike_near[i + 1] - price_strike_near[i - 1]) / 2)

    for j in range(len(price_strike_next)):
        if j == 0: 
            delta_K_next.append(abs(price_strike_next[j + 1] - price_strike_next[j]))
        elif j == len(price_strike_next) - 1: 
            delta_K_next.append(abs(price_strike_next[j] - price_strike_next[j - 1]))
        else:  
            delta_K_next.append(abs(price_strike_next[j + 1] - price_strike_next[j - 1]) / 2)

    df_near_term['delta_K'] = delta_K_near
    df_next_term['delta_K'] = delta_K_next

    df_near_term['contribution'] = (df_near_term['delta_K'] / (df_near_term['strike'] ** 2) * np.exp(r1 * T1) * df_near_term['mid'])
    df_next_term['contribution'] = (df_next_term['delta_K'] / (df_next_term['strike'] ** 2) * np.exp(r2 * T2) * df_next_term['mid'])

    return df_near_term, df_next_term

def calc_total_contribution(df_near_term, df_next_term, T1, T2):
    
    result_near = 2 / T1 * df_near_term['contribution'].sum()
    result_next = 2 / T2 * df_next_term['contribution'].sum()

    return result_near, result_next

def calc_total_sigma(result_near, result_next, forward_near_term, forward_next_term, K0_near_term, K0_next_term, T1, T2):
    
    sigma1 = result_near - (forward_near_term/K0_near_term - 1)**2 / T1
    sigma2 = result_next - (forward_next_term/K0_next_term - 1)**2 / T2

    return sigma1, sigma2

def calc_vix(sigma1, sigma2, T1, T2, M_T1, M_T2, M_CM):
    vix = 100 * np.sqrt((T1 * sigma1 * (M_T2-M_CM)/(M_T2-M_T1) + T2 * sigma2 * (M_CM-M_T1)/(M_T2-M_T1)) * 365/M_CM)

    return vix

def compute_vix_from_dataframes(
    call_near, put_near,
    call_next, put_next,
    trade_date,
    M_CM,
    r1=0.0,
    r2=0.0,
):
    M_T1 = call_near["dte"].iloc[0]
    M_T2 = call_next["dte"].iloc[0]
    T1 = M_T1 / 365
    T2 = M_T2 / 365

    forward_near, forward_next, K0_near, K0_next = calc_K0(
        call_near, put_near,
        call_next, put_next,
        r1, r2, T1, T2
    )

    df_near, df_next = construct_dataframe(
        call_near, put_near,
        call_next, put_next,
        K0_near, K0_next
    )

    df_near, df_next = calc_contribution(df_near, df_next, r1, r2, T1, T2)

    var_near, var_next = calc_total_contribution(df_near, df_next, T1, T2)
    sigma1, sigma2 = calc_total_sigma(
        var_near, var_next,
        forward_near, forward_next,
        K0_near, K0_next,
        T1, T2
    )

    vix = calc_vix(sigma1, sigma2, T1, T2, M_T1, M_T2, M_CM)

    return {
        "vix": float(vix),
        "variance_near": float(sigma1),
        "variance_next": float(sigma2),
        "t_near": float(T1),
        "t_next": float(T2),
    }