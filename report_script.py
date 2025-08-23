import streamlit as st
import re

st.title("期权数据日更 - 摘要生成")

# ------------------ Inputs ------------------
today_str = st.text_input("Today（总量 Call Put 成交额 …）")
yesterday_str = st.text_input("Yesterday（同上）")
lastweek_str = st.text_input("Last week（同上）")
gamma_str = st.text_input("Gamma（三项：今天 昨天 单位；示例：-1.79  -1.39  M）")
other = st.text_input("Ticker, Vol/30D, OI（示例：SPY, 1.26, 0.37）")
pc_prem_str = st.text_input("Call Prem, Put Prem, 单位")

# ------------------ Helpers ------------------
def parse_numbers_ints(s):
    """将'1 2 3 4'或'1,2,3,4'等解析为int列表。"""
    if not s or not s.strip():
        return []
    parts = re.split(r"[,\s]+", s.strip().replace("，", ","))
    return [int(x) for x in parts if x != ""]

def parse_gamma(s):
    """
    解析Gamma输入：三项（今天 昨天 单位）
    单位必须是 'M' 或 'K'（不区分大小写）
    """
    if not s or not s.strip():
        return None
    parts = re.split(r"[,\s]+", s.strip().replace("，", ","))
    parts = [p for p in parts if p != ""]
    if len(parts) != 3:
        return None
    try:
        a = float(parts[0])
        b = float(parts[1])
        unit = parts[2].upper()
        if unit not in ("M", "K"):
            return None
        return a, b, unit
    except Exception:
        return None

def parse_other(s):
    """
    解析other：'Ticker, Vol/30D, OI'
    - Ticker：字符串，原样
    - Vol/30D：字符串，原样复述
    - OI：数字（可带%号），用于“较上一日 增加/减少 X%”
    """
    if not s or not s.strip():
        return None
    s = s.replace("，", ",")
    parts = [p.strip() for p in s.split(",")]
    if len(parts) != 3:
        return None
    ticker = parts[0]
    vol30d_str = parts[1]  # 原样复述
    oi_raw = parts[2].replace("%", "").replace("％", "").strip()
    try:
        oi_val = float(oi_raw)
    except Exception:
        return None
    return ticker, vol30d_str, oi_val

def humanize_2dec(n: int) -> str:
    """
    单位换算：
      >= 1e9 -> B
      >= 1e6 -> M
      >= 1e3 -> K
      <  1e3 -> 原始整数
    保留两位小数（有单位时）。
    """
    if n >= 1_000_000_000:
        return f"{n/1_000_000_000:.2f}B"
    if n >= 1_000_000:
        return f"{n/1_000_000:.2f}M"
    if n >= 1_000:
        return f"{n/1_000:.2f}K"
    return str(int(n))

def fmt_change_from_ratio(ratio: float) -> str:
    """
    输入 ratio = today/denom - 1（小数），
    输出：增加/减少 X.X% 或 基本持平（四舍五入到一位小数为0.0时）
    """
    val = round(abs(ratio) * 100.0, 1)
    if val == 0.0:
        return "基本持平"
    direction = "增加" if ratio > 0 else "减少"
    return f"{direction}{val:.1f}%"

def fmt_oi_change(oi_val: float) -> str:
    """
    OI较上一日 … ：只根据符号判断，数值保留两位小数；若为0.00 => 基本持平
    """
    v = round(abs(oi_val), 2)
    if v == 0.0:
        return "OI较上一日基本持平。"
    direction = "增加" if oi_val > 0 else "减少"
    return f"OI较上一日{direction}{v:.2f}%。"

def gamma_change_text(a: float, b: float, unit: str) -> str:
    """
    根据四种符号组合规则给出“较上一日 …”的表述：
    - 同为正：r = a/b - 1；r>0 增加，r<0 减少
    - 同为负：r = a/b - 1；方向反转（r<0 增加，r>0 减少）
    - 负->正：固定 增加；r = (a - b)/(-b) = a/|b| + 1
    - 正->负：固定 减少；r = (b - a)/b = 1 + |a|/b
    - 跨零/除零等极端：尽量给出合理表述；a=b时“基本持平”
    百分比保留一位小数；若为0.0 => 基本持平
    """
    # 完全相等
    if a == b:
        return "较上一日基本持平"
    # b 为 0 的边界处理
    if b == 0:
        # a==0 情况已在上方捕获
        return "较上一日增加"+str(abs(a))+unit if a > 0 else "较上一日减少"+str(abs(a))+unit

    # 同为正
    if a > 0 and b > 0:
        r = a / b - 1.0
        txt = fmt_change_from_ratio(r)
        return f"较上一日{txt}"

    # 同为负（方向反转）
    if a < 0 and b < 0:
        r = a / b - 1.0
        val = round(abs(r) * 100.0, 1)
        if val == 0.0:
            return "较上一日基本持平"
        direction = "增加" if r < 0 else "减少"
        return f"较上一日{direction}{val:.1f}%"

    # 负 -> 正：固定 增加
    if a > 0 and b < 0:
        r = (a - b) / (-b)  # = a/|b| + 1
        val = round(abs(r) * 100.0, 1)
        if val == 0.0:
            return "较上一日基本持平"
        return f"较上一日增加{val:.1f}%"

    # 正 -> 负：固定 减少
    if a < 0 and b > 0:
        r = (b - a) / b  # = 1 + |a|/b
        val = round(abs(r) * 100.0, 1)
        if val == 0.0:
            return "较上一日基本持平"
        return f"较上一日减少{val:.1f}%"

    # a==0 或其他极端情况兜底
    if a == 0 and b > 0:
        return "较上一日减少100.0%"
    if a == 0 and b < 0:
        return "较上一日增加100.0%"
    return "较上一日基本持平"

# ------------------ Main ------------------
if st.button("Calculate"):
    # 解析 today/yesterday/lastweek
    try:
        today = parse_numbers_ints(today_str)
        yesterday = parse_numbers_ints(yesterday_str)
        lastweek = parse_numbers_ints(lastweek_str)
        pc_prem = parse_gamma(pc_prem_str)
    except Exception:
        st.error("Today/Yesterday/Last week 解析失败：请只输入用空格或逗号分隔的正整数。")
        st.stop()

    if len(today) < 4 or len(yesterday) < 4 or len(lastweek) < 4:
        st.error("Today/Yesterday/Last week 至少需要前4项（总量、Call、Put、成交额）。")
        st.stop()

    other_parsed = parse_other(other)
    if other_parsed is None:
        st.error("other 解析失败：请按 'Ticker, Vol/30D, OI' 格式输入（示例：SPY, 1.26, 0.37）。")
        st.stop()
    ticker, vol30d_str, oi_val = other_parsed

    gamma_parsed = parse_gamma(gamma_str)
    if gamma_parsed is None:
        st.error("Gamma 解析失败：请输入三项（今天 昨天 单位M/K），示例：-1.79  -1.39  M")
        st.stop()
    a_gamma, b_gamma, gamma_unit = gamma_parsed

    # 取前4项：总量、Call、Put、成交额
    t_total, t_call, t_put, t_amt = today
    y_total, y_call, y_put, y_amt = yesterday
    w_total, w_call, w_put, w_amt = lastweek
    c_prem, p_prem, prem_unit = pc_prem

    # 首行复述（单位换算，两位小数）
    line1 = (
        f"今天{ticker}总成交量{humanize_2dec(t_total)}（call {humanize_2dec(t_call)}，"
        f"put {humanize_2dec(t_put)}），总成交额{humanize_2dec(t_amt)}，"
        f"call成交额 {c_prem}{prem_unit}，put成交额 {p_prem}{prem_unit}。"
    )

    # 第二行：均值比原样 + 总量环比/同比
    mom_total = (t_total / y_total) - 1.0
    yoy_total = (t_total / w_total) - 1.0
    line2 = (
        f"成交量与30天均值比为{vol30d_str}，"
        f"环比{fmt_change_from_ratio(mom_total)}，"
        f"同比{fmt_change_from_ratio(yoy_total)}。"
    )

    # 第三行：Call
    mom_call = (t_call / y_call) - 1.0
    yoy_call = (t_call / w_call) - 1.0
    line3 = (
        f"Call成交量环比{fmt_change_from_ratio(mom_call)}，"
        f"同比{fmt_change_from_ratio(yoy_call)}。"
    )

    # 第四行：Put
    mom_put = (t_put / y_put) - 1.0
    yoy_put = (t_put / w_put) - 1.0
    line4 = (
        f"Put成交量环比{fmt_change_from_ratio(mom_put)}，"
        f"同比{fmt_change_from_ratio(yoy_put)}。"
    )

    # 第五行：成交额
    mom_amt = (t_amt / y_amt) - 1.0
    yoy_amt = (t_amt / w_amt) - 1.0
    line5 = (
        f"成交额环比{fmt_change_from_ratio(mom_amt)}，"
        f"同比{fmt_change_from_ratio(yoy_amt)}。"
    )

    # 第六行：OI复述（按符号判断，0视为基本持平）
    line6 = fmt_oi_change(oi_val)

    # 第七行：Gamma（显示值用两位小数 + 单位，涨跌按规则；百分比一位小数）
    gamma_display = f"{a_gamma:.2f}{gamma_unit}"
    line7 = f"净gamma exposure为{gamma_display}，{gamma_change_text(a_gamma, b_gamma, gamma_unit)}。"

    summary = "\n\n".join([line1, line2, line3, line4, line5, line6, line7])

    st.subheader("总结")
    st.text_area("可复制：", summary, height=360)
