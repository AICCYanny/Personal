from AlgorithmImports import *
import numpy as np
from datetime import datetime, timedelta

class EnhancedAlertWindowStrategy(QCAlgorithm):
    def initialize(self):
        # 基础设置
        self.set_start_date(2010, 1, 1)
        self.set_end_date(2025, 6, 30)
        self.set_cash(100000)
        
        # 添加QQQ - 主要交易资产
        self.qqq = self.add_equity("QQQ", Resolution.DAILY).symbol
        
        # 策略参数
        self.sell_threshold = 2.5
        self.strong_long_threshold = -30
        self.sma_length = 30
        
        # 状态变量
        self.current_position = 1  # 1: 多头, -1: 空头
        self.alert_window_active = False
        self.first_trade = True
        
        # 数据存储
        self.signal_data = {}
        self.vix_data = {}
        self.vix3m_data = {}
        self.vix_ratio_history = []
        
        # 加载所有外部数据
        self.load_external_data()
        
        self.debug("策略初始化完成")

    def load_external_data(self):
        """加载所有外部数据"""
        self.load_signal_data()
        self.load_vix_data()
        self.load_vix3m_data()

    def load_signal_data(self):
        """加载外部信号数据"""
        try:
            signal_url = "https://raw.githubusercontent.com/ianzhangyi/QuantConnect-Lean-Strategy-Backtesting/main/QQQ_sentiment_signal_analysis/signals.csv"
            
            signal_content = self.download(signal_url)
            if signal_content:
                lines = signal_content.strip().split('\n')
                
                for line in lines[1:]:
                    if line and 'date' not in line.lower():
                        parts = line.split(',')
                        if len(parts) >= 2:
                            try:
                                date_str = parts[0].strip()
                                signal = float(parts[1].strip())
                                date = datetime.strptime(date_str, "%Y-%m-%d").date()
                                self.signal_data[date] = signal
                            except:
                                continue
                
                self.debug(f"信号数据加载成功: {len(self.signal_data)} 条记录")
            else:
                self.debug("信号数据下载失败")
                
        except Exception as e:
            self.debug(f"信号数据加载错误: {e}")

    def load_vix_data(self):
        """加载外部VIX数据"""
        try:
            # 请替换为您的实际VIX数据URL
            vix_url = "https://raw.githubusercontent.com/ianzhangyi/QuantConnect-Lean-Strategy-Backtesting/main/QQQ_sentiment_signal_analysis/VIX.csv"
            
            vix_content = self.download(vix_url)
            if vix_content:
                self.vix_data = self.parse_vix_csv_data(vix_content)
                self.debug(f"VIX数据加载成功: {len(self.vix_data)} 条记录")
            else:
                self.debug("VIX数据下载失败")
                
        except Exception as e:
            self.debug(f"VIX数据加载错误: {e}")

    def load_vix3m_data(self):
        """加载外部VIX3M数据"""
        try:
            # 请替换为您的实际VIX3M数据URL
            vix3m_url = "https://raw.githubusercontent.com/ianzhangyi/QuantConnect-Lean-Strategy-Backtesting/main/QQQ_sentiment_signal_analysis/VIX3M.csv"
            
            vix3m_content = self.download(vix3m_url)
            if vix3m_content:
                self.vix3m_data = self.parse_vix_csv_data(vix3m_content)
                self.debug(f"VIX3M数据加载成功: {len(self.vix3m_data)} 条记录")
            else:
                self.debug("VIX3M数据下载失败")
                
        except Exception as e:
            self.debug(f"VIX3M数据加载错误: {e}")

    def parse_vix_csv_data(self, content):
        """解析VIX CSV数据"""
        data = {}
        try:
            lines = content.strip().split('\n')
            
            for i, line in enumerate(lines):
                if i == 0:  # 跳过标题行
                    continue
                    
                if line.strip():
                    parts = line.split(',')
                    if len(parts) >= 2:
                        try:
                            date_str = parts[0].strip()
                            value = float(parts[1].strip())
                            
                            # 支持多种日期格式
                            date_formats = ["%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"]
                            date_obj = None
                            
                            for fmt in date_formats:
                                try:
                                    date_obj = datetime.strptime(date_str, fmt).date()
                                    break
                                except:
                                    continue
                            
                            if date_obj:
                                data[date_obj] = value
                                
                        except Exception as e:
                            continue
                            
        except Exception as e:
            self.debug(f"VIX CSV解析错误: {e}")
            
        return data

    def get_current_signal(self, current_date):
        """获取当前日期的信号数据"""
        date_to_check = current_date.date()
        
        for i in range(10):
            check_date = date_to_check - timedelta(days=i)
            if check_date in self.signal_data:
                return self.signal_data[check_date]
        
        return None

    def get_current_vix_data(self, current_date):
        """获取当前日期的VIX和VIX3M数据"""
        date_to_check = current_date.date()
        
        # 查找最近的VIX数据
        vix_value = None
        for i in range(10):
            check_date = date_to_check - timedelta(days=i)
            if check_date in self.vix_data:
                vix_value = self.vix_data[check_date]
                break
        
        # 查找最近的VIX3M数据
        vix3m_value = None
        for i in range(10):
            check_date = date_to_check - timedelta(days=i)
            if check_date in self.vix3m_data:
                vix3m_value = self.vix3m_data[check_date]
                break
        
        return vix_value, vix3m_value

    def on_data(self, slice):
        """处理每个数据切片"""
        try:
            # 更安全的数据检查
            if slice is None or not slice.contains_key(self.qqq):
                return
                
            qqq_data = slice.get(self.qqq)
            if qqq_data is None:
                return
                
            current_price = qqq_data.price
            current_date = self.time
            
            # 获取信号数据
            current_signal = self.get_current_signal(current_date)
            if current_signal is None:
                return
            
            # 获取VIX数据
            vix_value, vix3m_value = self.get_current_vix_data(current_date)
            if vix_value is None or vix3m_value is None:
                return
            
            # 检查VIX3M值有效性
            if vix3m_value <= 0:
                return
            
            # 初始交易 - 从多头开始
            if self.first_trade:
                self.set_holdings(self.qqq, 1.0)
                self.current_position = 1
                self.first_trade = False
                self.debug(f"初始建仓 | 日期: {current_date.date()} | 多头 | 信号: {current_signal:.3f}")
                return
            
            # 计算VIX比率和SMA
            vix_ratio = vix_value / vix3m_value
            self.vix_ratio_history.append(vix_ratio)
            
            if len(self.vix_ratio_history) > self.sma_length:
                self.vix_ratio_history.pop(0)
                
            vix_ratio_sma = np.mean(self.vix_ratio_history) if self.vix_ratio_history else vix_ratio
            
            # 警戒窗口状态管理
            if not self.alert_window_active and current_signal >= self.sell_threshold:
                self.alert_window_active = True
            
            elif self.alert_window_active and current_signal < 0:
                self.alert_window_active = False
            
            # 交易决策
            action = None
            reason = ""
            
            # 条件1: 做空条件
            if (self.alert_window_active and 
                vix_ratio > vix_ratio_sma and 
                self.current_position != -1):
                action = 'SHORT'
                reason = f"警戒窗口做空 | VIX比率: {vix_ratio:.4f} > SMA: {vix_ratio_sma:.4f}"
            
            # 条件2: 做多条件
            elif self.current_position != 1:
                strong_long_signal = current_signal <= self.strong_long_threshold
                normal_long_signal = (current_signal < 0 and vix_ratio < vix_ratio_sma)
                
                if strong_long_signal:
                    action = 'LONG'
                    reason = f"强做多信号 | 信号: {current_signal:.3f}"
                elif normal_long_signal:
                    action = 'LONG'
                    reason = f"普通做多 | 信号: {current_signal:.3f} | VIX比率: {vix_ratio:.4f} < SMA: {vix_ratio_sma:.4f}"
            
            # 执行交易
            if action:
                self.liquidate(self.qqq)
                
                if action == 'SHORT':
                    self.set_holdings(self.qqq, -1.0)
                    self.current_position = -1
                else:
                    self.set_holdings(self.qqq, 1.0)
                    self.current_position = 1
                
                # 只在换仓时记录日志
                self.debug(f"换仓 | 日期: {current_date.date()} | {action} | 信号: {current_signal:.3f} | {reason}")
            
        except Exception as e:
            # 更详细的错误信息
            self.debug(f"数据处理错误: {str(e)}")

    def on_end_of_algorithm(self):
        """策略结束时的处理"""
        portfolio_value = self.portfolio.total_portfolio_value
        total_return = (portfolio_value - 100000) / 100000
        self.debug(f"策略结束 | 最终价值: {portfolio_value:.2f} | 总回报: {total_return:.2%}")