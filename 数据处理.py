# -*- coding: utf-8 -*-
"""
Created on Wed Nov 30 17:20:36 2022
不需要排重
销售-商品数据合并处理
合并商品未拆分
2021-01-01 ~ 
@author: ANU
"""

#%%
import os
import pandas as pd
import numpy as np


def target_files(path, fmt):
    target = []
    for root, dirs, files in os.walk(path):
        for fn in files:
            name, ext = os.path.splitext(fn)
            if ext in fmt:
                target.append(os.path.join(root, fn))
    return sorted(target, key=os.path.getmtime, reverse=False)

        
#%%数据合并
# 历史
f1 = target_files('C:\数据资料\erp商品分析\商品数据', ('.csv'))

d = []
for file in f1:
    print(file)
    dt = pd.read_csv(file,dtype=str,encoding='gb18030',encoding_errors='backslashreplace')
    d.append(dt)

# 增量
f2 = target_files('C:\数据资料\erp商品分析\商品数据', ('.xlsx'))

d1 = []
for file in f2:
    print(file)
    dt1 = pd.read_excel(file,dtype=str)
    d1.append(dt1)    
    
d2 = d+d1

# 合并
df = pd.concat(d2)

#%%格式处理
def detect_date_columns(df):
    date_cols = []
    for col in df.columns:
        if df[col].isna().all():
            continue
        try:
            pd.to_datetime(df[col], errors='raise')
            date_cols.append(col)
        except:
            pass
    return date_cols


t = detect_date_columns(df)
            
n = [ '基本售价', '销售数量', '实发金额', '销售金额',
       '已付金额', '应付金额', '售价', '当期退货数量', '当期实退数量', '当期退货金额',
        '当期实退金额', '运费收入', '运费收入分摊', '运费支出', '运费支出分摊',
       '优惠金额', '订单重量', '订单商品重量','日常到手最低价',
       'B级活动（小主题活动）最低价格', 'A级活动（99、每月一次）最低价格', 'S级活动（三八节、618）最低价格',
       'S+级活动（618预售、双十一）最低价格','买家实付金额']

st = ['线上订单号','快递单号','原始线上订单号','线上子订单编号','商品编码','款式编码','组合装商品编码']
# 时间格式
for column in t:
    df[column] = pd.to_datetime(df[column], errors='raise')

# 空值填充
df.fillna('',inplace=True)

# 数字格式
for num in n:
    # print(num)    
    df[num] = df[num].astype(str).str.replace(',','',regex=False)
    df[num] = pd.to_numeric(df[num])
    
#符号处理
for i in st:
    df[i] = df[i].astype(str).str.replace('=|"','',regex=True)
    
#%% 字段内容处理
# '商品简称','线上商品名'
p = [0,1,2,np.nan]

df['商品名'] = df.apply(lambda row : row['线上商品名'] if row['商品简称'] in p else row['商品简称'],axis=1)

# '颜色规格', '线上颜色规格'
df['规格'] = df.apply(lambda row : row['线上颜色规格'] if row['颜色规格'] in p else row['颜色规格'],axis=1)

# 筛选字段
df2 = df[['内部订单号', '订单类型', '线上订单号', '订单状态','店铺',
          '买家留言', '卖家备注', '订单日期', '发货日期', '付款日期',
          '省','市', '区县','收货人', '商品编码', '原始线上订单号', 
          '款式编码', '产品分类','虚拟分类', '商品简称', '商品名', '规格',
          '品牌',  '销售数量', '销售金额','已付金额', '售价',
          '订单商品重量', '商店站点', '订单来源', '线上商品名','组合装商品编码',
          '商品状态', '退款状态','线上子订单编号']].copy()

# 导出
df2.to_csv('销售数据-商品汇总.csv',index=False)
df2.to_pickle('销售数据-商品汇总.pkl')

#%%组合商品拆分

dt = pd.read_pickle(r'销售数据-商品汇总.pkl')
# 价格为0为赠品
dt['赠品数量'] = dt.apply(lambda x: x['销售数量'] if x['销售金额']==0 else 0,axis=1)

dt_gro = dt.groupby(by='商品编码').agg(商品名=('商品名','last'),
                                      销售金额=('销售金额','sum'),
                                      销售数量=('销售数量','sum'),
                                      赠品数量=('赠品数量','sum'))

dt_gro['实售数量'] = dt_gro['销售数量']-dt_gro['赠品数量']
dt_gro['商品单价'] = dt_gro.apply(lambda row: row['销售金额']/row['实售数量'] if row['销售金额']!=0 else 0,axis=1)


# 组合商品
zh = pd.read_excel(r"C:\数据资料\财务—抖音\组合装商品\组合装商品_2023-04-04_09-38-38.10832679.12595055_1.xlsx",dtype=str)
zh1 = zh[['组合商品编码','商品编码', '商品名称', '颜色及规格', '数量']].copy()
zh1['数量'] = pd.to_numeric(zh1['数量'])

# 组合品单价
zh1['单品价格'] = zh1['商品编码'].map(dt_gro['商品单价'])

# 未作为正品单独售卖过的品
zh1['单品价格'].fillna(0,inplace=True)
zh1['单品价格'] = zh1['单品价格']*zh1['数量']

# 合并组合品价格
gro1 = zh1.groupby('组合商品编码').agg(组合品价格=('单品价格','sum'))
zh1['组合品价格'] = zh1['组合商品编码'].map(gro1['组合品价格'])
zh1['单品应占比例'] = zh1.apply(lambda row: row['单品价格']/row['组合品价格'] if row['组合品价格']!=0 else 0 ,axis=1)

# 订单组合商品拆分
dt1 = dt[dt['商品编码'].isin(zh1['组合商品编码'])].copy()
dt2 = dt[~dt['商品编码'].isin(zh1['组合商品编码'])].copy()
# 组合品拆分

dt11 = dt1.merge(zh1,how='left',left_on='商品编码',right_on='组合商品编码')

dt12 = dt11[['内部订单号', '订单类型', '线上订单号', '订单状态', '店铺', '买家留言', '卖家备注', '订单日期', '发货日期',
             '付款日期', '省', '市', '区县', '收货人', '原始线上订单号', '款式编码', '产品分类',
             '虚拟分类', '商品简称', '品牌', '销售数量', '销售金额', '已付金额', '售价',
             '订单商品重量', '商店站点', '订单来源', '线上商品名', '组合装商品编码', '商品状态', '退款状态', '线上子订单编号',
             '赠品数量', '组合商品编码', '商品编码_y', '商品名称', '颜色及规格', '数量', '单品价格','组合品价格','单品应占比例']].copy()

dt12['单品总销售数量'] = dt12['销售数量']*dt12['数量']
dt12['应占金额'] = dt12['销售金额']*dt12['单品应占比例']

dt13 = dt12[['内部订单号', '订单类型', '线上订单号', '订单状态', '店铺', '买家留言', '卖家备注', '订单日期', '发货日期',
             '付款日期', '省', '市', '区县', '收货人', '商品编码_y', '原始线上订单号', '款式编码', '产品分类',
             '虚拟分类', '商品简称', '商品名称', '颜色及规格', '品牌', '单品总销售数量', '应占金额', '已付金额', '售价',
             '订单商品重量', '商店站点', '订单来源', '线上商品名', '组合装商品编码', '商品状态', '退款状态', '线上子订单编号',
             '赠品数量']].copy()

dt13.rename(columns={'商品编码_y':'商品编码','商品名称':'商品名','颜色及规格':'规格',
                     '单品总销售数量':'销售数量','应占金额':'销售金额'},inplace=True)

final = pd.concat([dt13,dt2],ignore_index=True)


f = final[['店铺', '订单日期', '发货日期',
       '付款日期', '省', '市', '区县', '商品编码', '原始线上订单号', '款式编码', '产品分类',
       '虚拟分类', '商品简称', '商品名', '规格', '品牌', '销售数量', '销售金额', '已付金额', '售价',
       '订单商品重量', '商店站点', '线上商品名', '组合装商品编码',
       '赠品数量']].copy()
#%%
# 商品名称
f1 = f[['商品编码','商品名']].copy()
f1['len'] = f1['商品名'].apply(lambda x :len(str(x)))
f1.sort_values(by='len',inplace=True)
f1.drop_duplicates(subset=['商品编码'],keep='first',inplace=True)

f['商品名'] = f['商品编码'].map(f1.set_index('商品编码')['商品名'])

# 拆分后订单导出
f.to_csv('销售数据-商品拆分.csv',index=False)
f.to_pickle('销售数据-商品拆分.pkl')


# dt = pd.read_pickle(r'销售数据-商品拆分.pkl')
# pin = dt[['商品编码','商品名','线上商品名','产品分类']].drop_duplicates().copy()

#%%
# 验证
# f = final[(final['订单日期']>='2023-01-01 00:00:00')].copy()
# f1 = f.groupby('店铺').agg(num=('销售数量','sum'),m=('销售金额','sum'))

# f1.to_excel(r'ceshi.xlsx')

# 验证数据不需要排重
# dd = pd.read_excel(r'C:\数据资料\erp商品分析\订单数据\订单_2023-01-14_17-23-04.10832679.14954923_1.xlsx',dtype=str)

# dd['已付金额'] = pd.to_numeric(dd['已付金额'])
# dd['付款日期'] = pd.to_datetime(dd['付款日期'])
# dd['年'] = dd['付款日期'].apply(lambda x : x.year)

# dd.groupby(by=['平台站点','年']).agg(金额=('已付金额','sum'))  
# dd['已付金额'].sum()   #129184903.90000005

# df['销售金额'].sum()  #129184874.20000002


