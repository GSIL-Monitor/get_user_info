#!/usr/bin/python
# encoding=utf-8


from ti_config.bootstrap import init_ti_srv_cfg
from ti_daf.sql_context import SqlContext, session_scope, iselect_rows_by_sql
from ti_daf import SqlTemplate,sql_util
import json
import pandas as pd
import numpy as np
from datetime import datetime
import datetime as dt
from get_user_info.data_merge.send_email import EmailSend
import os
import calendar


init_ti_srv_cfg('ti-daf')
db_1 = SqlContext('/python/db/dev_dw_db')


def get_daily_df():

    yesterday = datetime.date(datetime.today()) - dt.timedelta(days=1)
    origin_date = yesterday - dt.timedelta(days=6)

    ytd_s = "'" + str(yesterday) + "'"
    ord_s = "'" + str(origin_date) + "'"

    sql_1 = '''
        select a.mon mon,a.apply_times apply_times,b.acess_times access_times,round(b.acess_times/a.apply_times,4)  access_rate,
        c.loan_times loan_times,round(c.loan_times/b.acess_times,4) withdraw_rate ,c.amt loanamt
        from
        (
            select  to_char(applydate,'yyyy-mm-dd') mon,count(distinct id) apply_times 
            from dev_dw.f_loanapplyinfo
            where applytype not in ('repayCredit')
            group by to_char(applydate,'yyyy-mm-dd')
        ) a
        left join 
        (
            select  to_char(applydate,'yyyy-mm-dd') mon,count(distinct id) acess_times 
            from dev_dw.f_loanapplyinfo
            where status like '%A%' and applytype not in ('repayCredit')
            group by to_char(applydate,'yyyy-mm-dd')
        ) b
        on a.mon=b.mon
        left join 
        (
            select to_char(loantime,'yyyy-mm-dd') mon,count(distinct id) loan_times,sum(loanamt) amt 
            from dev_dw.f_loanagreement 
            where loanstatus in ('D','O','R','E')  and idproduct<>21
            group by to_char(loantime,'yyyy-mm-dd')
        ) c
        on a.mon=c.mon
        where a.mon >=''' + ord_s + ''' and a.mon<=''' + ytd_s + ''' order by mon'''

    with session_scope(db_1) as session:
        re_list = []
        for row in iselect_rows_by_sql(session, sql_1, []):
            re_list.append(list(row))

    daily_df = pd.DataFrame(re_list, columns=['日期', '申请笔数', '通过笔数', '通过率', '提现笔数', '提现率', '放款额'])

    return daily_df



def get_end_week_df():

    yesterday = datetime.date(datetime.today()) - dt.timedelta(days=1)
    origin_date = yesterday - dt.timedelta(days=6)
    today = datetime.date(datetime.today())

    ytd_s = "'" + str(yesterday) + "'"
    ord_s = "'" + str(origin_date) + "'"

    re_list = []
    for i in range(4):
        time_after_s = today - dt.timedelta(days=i * 7)
        time_before_s = today - dt.timedelta(days=(i + 1) * 7)

        time_after_turn = yesterday - dt.timedelta(days=i * 7)
        b_str = datetime.strftime(time_before_s, '%m-%d')
        a_str = datetime.strftime(time_after_turn, '%m-%d')
        string = b_str + "&" + a_str

        time_before = "'" + str(time_before_s) + "'"
        time_after = "'" + str(time_after_s) + "'"

        sql_1 = '''
            select sum(apply_times) apt,sum(access_times) pt,round(sum(access_times)/sum(apply_times),4) pr,
            sum(loan_times) lt,round(sum(loan_times)/sum(access_times),4) wr,sum(loanamt) amt from
            (
                select a.mon mon,a.apply_times apply_times,b.acess_times access_times,
                c.loan_times loan_times,c.amt loanamt
                from
                (
                    select  to_char(applydate,'yyyy-mm-dd') mon,count(distinct id) apply_times 
                    from dev_dw.f_loanapplyinfo
                    where applytype not in ('repayCredit')
                    group by to_char(applydate,'yyyy-mm-dd')
                ) a
                left join 
                (
                    select  to_char(applydate,'yyyy-mm-dd') mon,count(distinct id) acess_times 
                    from dev_dw.f_loanapplyinfo
                    where status like '%A%'  and applytype not in ('repayCredit')
                    group by to_char(applydate,'yyyy-mm-dd')
                ) b
                on a.mon=b.mon
                left join 
                (
                    select to_char(loantime,'yyyy-mm-dd') mon,count(distinct id) loan_times,sum(loanamt) amt 
                    from dev_dw.f_loanagreement 
                    where loanstatus in ('D','O','R','E') and idproduct<>21
                    group by to_char(loantime,'yyyy-mm-dd')
                ) c
                on a.mon=c.mon
                where a.mon >=''' + time_before + ''' and a.mon<''' + time_after + ''' 
                order by mon
            )'''

        with session_scope(db_1) as session:
            for row in iselect_rows_by_sql(session, sql_1, []):
                turn_list = [string] + list(row)
                re_list.append(turn_list)

    end_week_df = pd.DataFrame(re_list, columns=['周期', '申请笔数', '通过笔数', '通过率', '提现笔数', '提现率', '放款额'])
    end_week_df = end_week_df.sort_values(by=['周期'], ascending=[1])

    return end_week_df



def get_creditrate_df():
    today = datetime.date(datetime.today())
    origin_date = datetime.date(datetime.today()) - dt.timedelta(days=7)

    ytd_s = "'" + str(today) + "'"
    ord_s = "'" + str(origin_date) + "'"


    sql = '''
    select     
    a.creditrating,
    count(distinct a.id) apply_num,
    count(distinct case when a.status like '%A%'  then  a.id  end) pass_num,
    count(distinct case when b.id is not null and b.loanstatus in ('D','O','R','E')  then a.id end) loan_num
    from  dev_dw.f_loanapplyinfo a
    left join dev_dw.f_loanagreement b
    on a.applyno=b.applyid
    where a.idproduct<>21
    and a.applydate>=to_date(''' + ord_s + ''','yyyy-mm-dd')  
    and a.applydate<to_date(''' + ytd_s + ''','yyyy-mm-dd') 
    group by a.creditrating
    '''

    sql_row = sql_util.select_rows_by_sql(sql_text=sql, sql_paras={}, ns_server_id='/python/db/dev_dw_db', max_size=-1)

    sql_list = []
    for row in sql_row:
        sql_list.append(list(row))

    creditrate_df = pd.DataFrame(sql_list, columns=['partyid', 'apply_num', 'pass_num', 'loan_num'])

    return creditrate_df



def get_end_over_df():

    today = datetime.date(datetime.today())
    num = 11
    this_mon = pd.Period(today, freq='M')
    first_mon = this_mon - num
    first_mon_s = "'" + str(first_mon) + '-01' + "'"

    sql_1 = '''
    select aa.mon,round(aa.amt/ee.amt,4) D4,round(bb.amt/ee.amt,4) D15,
           round(cc.amt/ee.amt,4) M2,round(dd.amt/ee.amt,4) M2p,
           round(ff.amt/ee.amt,4) M2_pred
    from
    (
      select to_char(loantime,'yyyy-mm') mon,sum(dueprincipal) amt  
      from  dev_dw.f_loanagreement a
      left join dev_dw.f_loanrepayschedule b
      on a.id=b.idloanagreement
      where loanstatus in ('D','O','R','E') and loantime>=to_date(''' + first_mon_s + ''','yyyy-mm-dd')
            and( b.repaytime-b.duedate >4 or (b.repaytime is null and sysdate-b.duedate>4)) and a.idproduct<>21 
      group by to_char(loantime,'yyyy-mm')
     ) aa
    left join 
    (
      select to_char(loantime,'yyyy-mm') mon,sum(dueprincipal) amt  from  dev_dw.f_loanagreement a
      left join dev_dw.f_loanrepayschedule b
      on a.id=b.idloanagreement
      where loanstatus in ('D','O','R','E') and loantime>=to_date(''' + first_mon_s + ''','yyyy-mm-dd')
            and( b.repaytime-b.duedate>15 or (b.repaytime is null and sysdate-b.duedate>15)) and a.idproduct<>21 
      group by to_char(loantime,'yyyy-mm')
     ) bb
    on aa.mon=bb.mon
    left join
    (
      select to_char(loantime,'yyyy-mm') mon,sum(dueprincipal) amt  from  dev_dw.f_loanagreement a
      left join dev_dw.f_loanrepayschedule b
      on a.id=b.idloanagreement
      where loanstatus in ('D','O','R','E')  and  loantime>=to_date(''' + first_mon_s + ''','yyyy-mm-dd')
             and( b.repaytime-b.duedate>33 or (b.repaytime is null and sysdate-b.duedate>33)) and a.idproduct<>21
      group by to_char(loantime,'yyyy-mm')
     ) cc
    on aa.mon=cc.mon
    left join
    (
      select to_char(loantime,'yyyy-mm') mon,sum(dueprincipal) amt  from  dev_dw.f_loanagreement a
      left join dev_dw.f_loanrepayschedule b
      on a.id=b.idloanagreement
      where loanstatus in ('D','O','R','E') and loantime>=to_date(''' + first_mon_s + ''','yyyy-mm-dd') 
            and( b.repaytime-b.duedate>60 or (b.repaytime is null and sysdate-b.duedate>60)) and a.idproduct<>21 
      group by to_char(loantime,'yyyy-mm')
     ) dd
    on aa.mon=dd.mon
    left join
    (
      select to_char(loantime,'yyyy-mm') mon,sum(loanamt) amt 
      from dev_dw.f_loanagreement
      where idproduct<>21 
      group by to_char(loantime,'yyyy-mm') 
    ) ee
    on aa.mon=ee.mon
    left join
    (
      select to_char(loantime,'yyyy-mm') mon,sum(dueprincipal) amt  from  dev_dw.f_loanagreement a
      left join dev_dw.f_loanrepayschedule b
      on a.id=b.idloanagreement
      where loanstatus in ('D','O','R','E')  and  loantime>=to_date(''' + first_mon_s + ''','yyyy-mm-dd')
             and( b.repaytime-b.duedate>33 or (b.repaytime is null and sysdate-b.duedate>15)) and a.idproduct<>21
      group by to_char(loantime,'yyyy-mm')
     ) ff
    on aa.mon=ff.mon
    order by aa.mon
      '''

    with session_scope(db_1) as session:
        over_list = []
        for row in iselect_rows_by_sql(session, sql_1, []):
            over_list.append(list(row))

    over_df = pd.DataFrame(over_list, columns=['mon', 'D4', 'D15', 'M2', 'M2+', 'M2_pred'])
    over_df = over_df.fillna(0)

    # -----------------------------------------

    '''
    over_table=pd.pivot_table(over_df,values='loanamt',index=['mon'],columns=['categroy'],aggfunc=np.sum,margins=1,fill_value=0)
    over_table=over_table.reset_index()

    columns_list=['D4','D15','M2','M2+']
    for item in columns_list:
        over_table[item+'_rate']=over_table[item]/over_table['All']

    over_table_num=len(over_table['mon'])
    end_over_df=over_table.ix[:over_table_num-2,['mon','D4_rate','D15_rate','M2_rate','M2+_rate']]


    sum_amt=over_df['loanamt'].groupby(over_df['mon']).sum()
    sum_df=sum_amt.reset_index()
    '''

    sql_2 = '''
    select  to_char(loantime,'yyyy-mm') mon,sum(loanamt) amt,count(distinct id) times from dev_dw.f_loanagreement
    where loanstatus in ('D','O','R','E') and loantime>=to_date(''' + first_mon_s + ''','yyyy-mm-dd') and idproduct<>21 
    group by to_char(loantime,'yyyy-mm')
    '''

    with session_scope(db_1) as session:
        loan_list = []
        for row in iselect_rows_by_sql(session, sql_2, []):
            loan_list.append(list(row))

    loan_df = pd.DataFrame(loan_list, columns=['mon', 'loanamt', 'loantimes'])
    loan_df['loanamt'] = loan_df['loanamt'] / 10000

    # ----------------------------------------------

    yue_list = []
    for i in range(num + 1):
        if i == 0:
            this_mon = str(pd.Period(today, freq='M') - i)
            next_mon = "'" + str(today) + "'"
        else:
            this_mon = str(pd.Period(today, freq='M') - i)
            next_mon = "'" + str(pd.Period(today, freq='M') + 1 - i) + '-01' + "'"

        sql_3 = '''
        select sum(b.dueprincipal) from dev_dw.f_loanagreement a
        left join dev_dw.f_loanrepayschedule b
        on a.id=b.idloanagreement 
        where loantime<to_date(''' + next_mon + ''' ,'yyyy-mm-dd') and (b.repaytime>=to_date(''' + next_mon + ''','yyyy-mm-dd') or b.repaytime is null)
        and a.idproduct<>21 and ( b.repaytime-b.duedate<33 or (b.repaytime is null and to_date(''' + next_mon + ''','yyyy-mm-dd')-b.duedate<33+1))
        '''

        with session_scope(db_1) as session:
            for row in iselect_rows_by_sql(session, sql_3, []):
                yue_list.append([this_mon, row[0]])

    yue_df = pd.DataFrame(yue_list, columns=['mon', 'yue'])
    yue_df['yue'] = yue_df['yue'] / 10000

    all_result_df = pd.merge(loan_df, yue_df, on='mon')
    all_result_df = pd.merge(all_result_df, over_df, on='mon', how='outer')
    end_over_df = all_result_df.sort_values(by=['mon'], ascending=[1])
    end_over_df = end_over_df.fillna(0)

    return end_over_df


def get_sameterm_df():
    # 获取月份的最后一天
    def getFirstDay_LastDay(year=None, month=None):
        """
        :param year: 年份，默认是本年，可传int或str类型
        :param month: 月份，默认是本月，可传int或str类型
        :return: firstDay: 当月的第一天，datetime.date类型
                  lastDay: 当月的最后一天，datetime.date类型
        """
        if year:
            year = int(year)
        else:
            year = datetime.date(datetime.today()).year

        if month:
            month = int(month)
        else:
            month = datetime.date(datetime.today()).month

        # 获取当月第一天的星期和当月的总天数
        firstDayWeekDay, monthRange = calendar.monthrange(year, month)

        # 获取当月的第一天
        firstDay = dt.date(year=year, month=month, day=1)
        lastDay = dt.date(year=year, month=month, day=monthRange)

        return lastDay

    # 判断是否是闰年
    def isYear(year):
        if (year % 4 == 0) & (year % 100 != 0):
            return 'yes'
        elif year % 400 == 0:
            return 'yes'
        else:
            return 'no'

    today = datetime.date(datetime.today())
    riqi = today.day
    this_mon = pd.Period(today, freq='M')

    # 获取当月最后一天数据
    lastday = getFirstDay_LastDay(year=this_mon.year, month=this_mon.month)

    overdue_mx = []
    loan_amt = []
    for i in range(12):
        month_time = this_mon - i
        last_month = this_mon - i - 1
        last_month_first_s = "'" + str(last_month) + "-01" + "'"
        # print (last_month_first_s)
        last_month_last_s = "'" + str(month_time) + "-01" + "'"
        # print (last_month_last_s)


        # 判断今天是否是当月最后一天
        if lastday.day == riqi:
            month_time_s = "'" + str(getFirstDay_LastDay(year=month_time.year, month=month_time.month)) + "'"
        elif isYear(month_time.year) == 'yes' and month_time.month == 2 and riqi > 29:
            month_time_s = "'" + str(month_time) + "-" + '29' + "'"
        elif isYear(month_time.year) == 'no' and month_time.month == 2 and riqi > 28:
            month_time_s = "'" + str(month_time) + "-" + '28' + "'"
        else:
            month_time_s = "'" + str(month_time) + "-" + str(riqi) + "'"

        sql_1 = '''
            select to_char(to_date(''' + last_month_first_s + ''','yyyy-mm-dd'),'yyyy-mm') as time,  
            sum(b.dueprincipal) from dev_dw.f_loanagreement a
            left join dev_dw.f_loanrepayschedule b
            on a.id=b.idloanagreement
            where a.loanstatus in ('D','E','R','O')
            and a.idproduct in (6,12)
            and a.loantime>=to_date(''' + last_month_first_s + ''','yyyy-mm-dd')
            and a.loantime<to_date(''' + last_month_last_s + ''','yyyy-mm-dd')
            and a.duedate<to_date(''' + month_time_s + ''','yyyy-mm-dd')
            and (
            (a.repaytime is null and to_date(''' + month_time_s + ''','yyyy-mm-dd')-a.duedate>=4)
            or
            (a.repaytime <to_date(''' + month_time_s + ''','yyyy-mm-dd') and a.repaytime-a.duedate>=4)
            or 
            (a.repaytime >to_date(''' + month_time_s + ''','yyyy-mm-dd') and to_date(''' + month_time_s + ''','yyyy-mm-dd')-a.duedate>=4)
            )
            '''
        with session_scope(db_1) as session:
            for row in iselect_rows_by_sql(session, sql_1, []):
                overdue_mx.append(row)

        sql_2 = '''
            select  to_char(to_date(''' + last_month_first_s + ''','yyyy-mm-dd'),'yyyy-mm') as time,  
            sum(loanamt)
            from dev_dw.f_loanagreement 
            where loanstatus in ('D','E','R','O')
            and idproduct in (6,12)
            and loantime>=to_date(''' + last_month_first_s + ''','yyyy-mm-dd')
            and loantime<to_date(''' + last_month_last_s + ''','yyyy-mm-dd')
            '''

        with session_scope(db_1) as session:
            for row1 in iselect_rows_by_sql(session, sql_2, []):
                loan_amt.append(row1)

    overdue_mx = pd.DataFrame(overdue_mx, columns=['日期', 'D4逾期金额'])
    loan_amt = pd.DataFrame(loan_amt, columns=['日期', '放款金额'])
    final_df = pd.merge(overdue_mx, loan_amt, on='日期', how='outer')

    return final_df



def get_end_collect_df():
    today = datetime.date(datetime.today())
    yesterday = datetime.date(datetime.today()) - dt.timedelta(days=1)

    collection_list = []
    for i in range(6):
        time_b = today - dt.timedelta(days=(i + 1) * 7)
        time_a = today - dt.timedelta(days=7 * i)

        time_bs = "'" + str(time_b) + "'"
        time_as = "'" + str(time_a) + "'"

        time_a_turn = yesterday - dt.timedelta(days=7 * i)
        time_bl = datetime.strftime(time_b, '%m-%d')
        time_al = datetime.strftime(time_a_turn, '%m-%d')
        time_str = time_bl + "&" + time_al

        sql = '''
        select  yqfl,sum(je),count(*)  from 
        (
            select duedate,rcrq,hkrq,amt+lixi+faxi je,agent,
                   case WHEN (overduedate BETWEEN 4 AND 15) THEN '4-15D'
                        WHEN (overduedate BETWEEN 16 AND 30 ) THEN '16-30D'
                        WHEN (overduedate BETWEEN 31 AND 60) THEN 'M2'
                        WHEN (overduedate > 60) THEN 'M3+'
                        ELSE 'no_kwon' end yqfl
            from 
            (
              select c.duedate,
                  c.duedate + 4 rcrq,
                  c.REPAYTIME AS hkrq,
                  c.dueprincipal amt,
                  -(INTEREST + PAYFEE) AS lixi,
                  OVERDUEFEE + PENALTY AS faxi,
                  CASE
                      WHEN (agentpartyid = '1014816000030982') THEN 'A1'
                      WHEN (agentpartyid = '1014816000005443') THEN 'A2'
                      WHEN (agentpartyid = '1014816000004520') THEN 'A3'
                      ELSE 'A0'
                  END AS agent,
                  CASE
                      WHEN (c.REPAYTIME IS NULL) THEN trunc(sysdate) - 1 - trunc(c.duedate)
                      ELSE trunc(c.REPAYTIME) - trunc(c.duedate)
                  END AS overduedate
                FROM
                    dev_dw.F_LOANAGREEMENT a
                LEFT JOIN dev_dw.F_LOANACCOUNT b 
                ON a.id = b.idloanagreement
                left join dev_dw.f_loanrepayschedule c
                on a.id=c.idloanagreement
                where a.idproduct<>21
                ORDER BY c.REPAYTIME
            )
            where  overduedate>=4
        )
        where hkrq>=to_date(''' + time_bs + ''','yyyy-mm-dd') and hkrq<to_date(''' + time_as + ''','yyyy-mm-dd') group by yqfl'''

        with session_scope(db_1)  as session:
            for row in iselect_rows_by_sql(session, sql, []):
                collection_list.append([time_str] + list(row))

    collection_df = pd.DataFrame(collection_list, columns=['timecircle', 'categroy', 'collectamt', 'collecttimes'])

    day_list = collection_df['timecircle'].unique()

    middle_df = pd.DataFrame(
        {'day': day_list, '4-15D_times': np.nan, '16-30D_times': np.nan, 'M2_times': np.nan, 'M3+_times': np.nan,
         '4-15D_amt': np.nan, '16-30D_amt': np.nan, 'M2_amt': np.nan, 'M3+_amt': np.nan},
        columns=['day', '4-15D_times', '16-30D_times', 'M2_times', 'M3+_times', '4-15D_amt', '16-30D_amt', 'M2_amt',
                 'M3+_amt'])

    for day, cate in zip(collection_df['timecircle'], collection_df['categroy']):
        collect_times = collection_df[(collection_df['timecircle'] == day) & (collection_df['categroy'] == cate)][
            'collecttimes'].values[0]
        collect_amt = \
        collection_df[(collection_df['timecircle'] == day) & (collection_df['categroy'] == cate)]['collectamt'].values[
            0]
        middle_df.ix[middle_df['day'] == day, cate + '_times'] = collect_times
        middle_df.ix[middle_df['day'] == day, cate + '_amt'] = collect_amt

    middle_df = middle_df.fillna(0)
    col = middle_df.columns[1:9]
    days = middle_df['day']
    end_collect_df = middle_df[col].astype(float)
    end_collect_df.insert(0, 'day', days)
    all_times = end_collect_df['4-15D_times'] + end_collect_df['16-30D_times'] + end_collect_df['M2_times'] + \
                end_collect_df['M3+_times']
    all_amt = end_collect_df['4-15D_amt'] + end_collect_df['16-30D_amt'] + end_collect_df['M2_amt'] + end_collect_df[
        'M3+_amt']
    end_collect_df.insert(5, 'all_times', all_times)
    end_collect_df.insert(10, 'all_amt', all_amt)
    end_collect_df = end_collect_df.sort_values(by='day', ascending=1)

    return end_collect_df



def get_repay_df():
    today = datetime.date(datetime.today())
    yesterday = today - dt.timedelta(days=1)

    day_list = []
    for i in range(4):
        after = today - dt.timedelta(days=i * 7)
        before = today - dt.timedelta(days=(i + 1) * 7)
        mid = yesterday - dt.timedelta(days=i * 7)

        after_s = "'" + str(after) + "'"
        before_s = "'" + str(before) + "'"

        time_bl = datetime.strftime(before, '%m-%d')
        time_al = datetime.strftime(mid, '%m-%d')
        time_str = time_bl + "&" + time_al

        sql = '''
            select sum(apply_num),sum(help_num),sum(circle_num),sum(firstcredit_num),sum(helpsettle_num),
            sum(circlesettle_num),sum(help_amt),sum(circle_amt),sum(firstcredit_amt),avg(avgamt),sum(help_amt_all),sum(circle_amt_all)
            from 
            ( 
                select date_format(applytime,'%Y-%m-%d') days,count(distinct a.applyinfoid) apply_num,
                count(distinct case when repaymode=0 
                and (json_extract(b.txndata,'$.supportFirstCredit')=False or json_extract(b.txndata,'$.supportFirstCredit') is null) 
                then a.applyinfoid end) help_num,
                count(distinct case when repaymode=1 
                and (json_extract(b.txndata,'$.supportFirstCredit')=False or json_extract(b.txndata,'$.supportFirstCredit') is null) 
                then a.applyinfoid end) circle_num,
                count(distinct case when json_extract(txndata,'$.supportFirstCredit')=True then a.applyinfoid end) firstcredit_num,
                count(distinct case when repaymode=0 and e.deal_status in ('用户终止结清','中途失败结清','全额结清') 
                and (json_extract(b.txndata,'$.supportFirstCredit')=False or json_extract(b.txndata,'$.supportFirstCredit') is null)
                then a.applyinfoid end) helpsettle_num,
                count(distinct case when repaymode=1 and e.deal_status in ('用户终止结清','中途失败结清','全额结清') 
                and (json_extract(b.txndata,'$.supportFirstCredit')=False or json_extract(b.txndata,'$.supportFirstCredit') is null)
                then a.applyinfoid end) circlesettle_num,
                sum(case when repaymode=0 then b.repayamt end) help_amt_all,
                sum(case when repaymode=1 then b.repayamt end) circle_amt_all
                from ac_bts_db.ApplyInfo a
                left join ac_bts_db.InsteadRepayTxnCtrl b
                on a.applyinfoid=b.applyinfoid
                left join
                (
                    select distinct  a.applyinfoid ids,
                    case when  applystatus='O' and json_extract(txndata,'$.userCancelFlag')=TRUE then '用户终止结清'
                         when  applystatus='O' and hasrepayamt<repayamt  then '中途失败结清'
                         when  applystatus='O' then '全额结清'
                         when  applystatus='C' and json_extract(txndata,'$.userCancelFlag')=TRUE then '用户终止撤销'
                         when  applystatus='C' then '首笔失败撤销'
                         else  '其他' end  deal_status
                    from ac_bts_db.ApplyInfo a
                    left join ac_bts_db.InsteadRepayTxnCtrl b
                    on a.applyinfoid=b.applyinfoid
                ) e
                on e.ids=a.applyinfoid
                where a.applytime>=''' + before_s + ''' and a.applytime<''' + after_s + '''
                group by date_format(applytime,'%Y-%m-%d')
            ) xx
            left join
            (
                select  date_format(exestarttime,'%Y-%m-%d') days,
                sum(case when c.repaymode=0 and (json_extract(b.txndata,'$.supportFirstCredit')=False or json_extract(b.txndata,'$.supportFirstCredit') is null) 
                then a.amt end) help_amt,
                sum(case when c.repaymode=1 and (json_extract(b.txndata,'$.supportFirstCredit')=False or json_extract(b.txndata,'$.supportFirstCredit') is null) 
                then a.amt end) circle_amt,
                sum(case when  json_extract(b.txndata,'$.supportFirstCredit') =True then a.amt end) firstcredit_amt
                from  ac_bts_db.InsteadRepaySchedule a
                left join ac_bts_db.InsteadRepayTxnCtrl b
                on b.insteadrepaytxnctrlid=a.insteadrepaytxnctrlid
                left join ac_bts_db.ApplyInfo c
                on c.applyinfoid=b.applyinfoid
                where scheduletype='RT' and a.status='S' 
                and  a.exestarttime>=''' + before_s + ''' and a.exestarttime<''' + after_s + '''
                group by date_format(exestarttime,'%Y-%m-%d')
            ) yy
            on xx.days=yy.days
            left join 
            (
                select  date_format(applytime,'%Y-%m-%d') days, 
                avg(json_extract(txndata,'$.evaluateOpenAmt')) avgamt
                from ac_bts_db.ApplyInfo a 
                left join ac_bts_db.InsteadRepayTxnCtrl b
                on a.applyinfoid=b.applyinfoid
                where repaymode=0  and json_extract(txndata,'$.evaluateOpenAmt') is not null
                and applytime>=''' + before_s + ''' and  applytime<''' + after_s + '''
                group by date_format(applytime,'%Y-%m-%d')
            ) zz
            on xx.days=zz.days
            '''

        day_row = sql_util.select_rows_by_sql(sql_text=sql, sql_paras={}, ns_server_id='/python/db/ac_cif_db',
                                              max_size=-1)

        for row in day_row:
            day_list.append([time_str] + list(row))

    day_df = pd.DataFrame(day_list,
                          columns=['日期', '提交笔数', '垫付笔数', '循环笔数', '首笔授信笔数', '垫付结清笔数', '循环结清笔数', '垫付金额', '循环金额', '首笔授信金额',
                                   '平均授信', '垫付申请金额', '循环申请金额'])
    day_df = day_df.sort_values(by='日期', ascending=[1])

    return day_df


def get_way_df():
    yesterday = datetime.date(datetime.today()) - dt.timedelta(days=1)
    origin_date = yesterday - dt.timedelta(days=6)
    today = datetime.date(datetime.today())

    ytd_s = "'" + str(yesterday) + "'"
    ord_s = "'" + str(origin_date) + "'"

    week_list = []
    fail_list = []
    for i in range(4):
        time_after_s = today - dt.timedelta(days=i * 7)
        time_before_s = today - dt.timedelta(days=(i + 1) * 7)

        time_after_turn = yesterday - dt.timedelta(days=i * 7)
        b_str = datetime.strftime(time_before_s, '%m-%d')
        a_str = datetime.strftime(time_after_turn, '%m-%d')
        string = b_str + "&" + a_str

        time_before = "'" + str(time_before_s) + "'"
        time_after = "'" + str(time_after_s) + "'"

        # 每个通道数据
        sql = '''
        select  a.paychannelid,case when fee=0 then 'companypay' else 'userpay' end  cate,
        count(distinct a.idinvoice),
        sum(amt)
        from dev_dw.f_payment a
        left join dev_dw.f_repayloantxnlist b
        on a.idinvoice=b.idinvoice
        where a.status='S' 
        and a.paytime>=to_date(''' + time_before + ''','yyyy-mm-dd')
        and a.paytime<to_date(''' + time_after + ''','yyyy-mm-dd')
        and b.applytype<>'repayCredit'
        group by a.paychannelid,
        case when fee=0 then 'companypay' else 'userpay' end 
        '''

        way_row = sql_util.select_rows_by_sql(sql_text=sql, sql_paras={}, ns_server_id='/python/db/dev_dw_db',
                                              max_size=-1)

        for row in way_row:
            week_list.append([string] + list(row))

        sql_fail = '''
        select   
        case when failcode='IPS.009' then '通道不支持'
             when failcode='IPS.027' then '查询支付结果失败'
             when failcode='IPS.028' then '外部代扣编号不存在'
             when failcode='IPS.029' then '交易记录不存在'
             when failcode='IPS.030' then '支付通道银行不存在'
             when failcode='IPS.031' then '支付路由异常'
             when failcode='IPS.032' then '查询对账文件失败'
             when failcode='IPS.033' then '支付同步次数达到最大限制'
             when failcode='IPS.034' then '余额不足'
             when failcode='IPS.035' then '不匹配的支付金额'
             when failcode='IPS.036' then '不匹配的交易状态'
             when failcode='IPS.037' then '不匹配的票据状态'
             when failcode='IPS.038' then '支付失败(因为再次支付)' end pay_status,
             count(distinct idinvoice) counts
        from dev_dw.f_payment
        where paychannelid in ('DDP.QTO.PAY','DDP.KFT.PAY','DDP.KQ.FAST.PAY') 
        and paytime>=to_date(''' + time_before + ''','yyyy-mm-dd')  and  paytime<to_date(''' + time_after + ''','yyyy-mm-dd') 
        and status='F'
        group by 
        case when failcode='IPS.009' then '通道不支持'
             when failcode='IPS.027' then '查询支付结果失败'
             when failcode='IPS.028' then '外部代扣编号不存在'
             when failcode='IPS.029' then '交易记录不存在'
             when failcode='IPS.030' then '支付通道银行不存在'
             when failcode='IPS.031' then '支付路由异常'
             when failcode='IPS.032' then '查询对账文件失败'
             when failcode='IPS.033' then '支付同步次数达到最大限制'
             when failcode='IPS.034' then '余额不足'
             when failcode='IPS.035' then '不匹配的支付金额'
             when failcode='IPS.036' then '不匹配的交易状态'
             when failcode='IPS.037' then '不匹配的票据状态'
             when failcode='IPS.038' then '支付失败(因为再次支付)' end
             '''

        fail_row = sql_util.select_rows_by_sql(sql_text=sql_fail, sql_paras={}, ns_server_id='/python/db/dev_dw_db',
                                               max_size=-1)

        for row in fail_row:
            fail_list.append([string] + list(row))

    week_way_df = pd.DataFrame(week_list, columns=['day', 'channel', 'cate', 'successnum', 'successamt'])
    week_way_df[['successnum', 'successamt']] = week_way_df[['successnum', 'successamt']].apply(pd.to_numeric)
    week_way_df = pd.pivot_table(week_way_df, index='day', columns=['cate', 'channel'],
                                 values=['successnum', 'successamt']).reset_index()


    fail_way_df = pd.DataFrame(fail_list, columns=['day', 'fail_reason', 'fail_num'])
    fail_way_df = pd.pivot_table(fail_way_df, index='day', columns='fail_reason', values='fail_num')

    return week_way_df,fail_way_df



def email_send():

    excel_writer = pd.ExcelWriter('/home/andpay/data/excel/tuesday_report.xlsx')
    daily_df=get_daily_df()
    daily_df.to_excel(excel_writer, '日数据')

    end_week_df=get_end_week_df()
    end_week_df.to_excel(excel_writer, '周数据')

    creditrate_df=get_creditrate_df()
    creditrate_df.to_excel(excel_writer, '用户评级', index=False)

    end_over_df=get_end_over_df()
    end_over_df.to_excel(excel_writer, '逾期数据(All)')

    sameterm_df=get_sameterm_df()
    sameterm_df.to_excel(excel_writer, '同期对比数据')

    end_collect_df=get_end_collect_df()
    end_collect_df.to_excel(excel_writer, '催收数据')

    repay_df=get_repay_df()
    repay_df.to_excel(excel_writer, '帮还数据')

    week_way_df,fail_way_df=get_way_df()
    week_way_df.to_excel(excel_writer, '通道数据')
    fail_way_df.to_excel(excel_writer, '通道数据', startrow=20)

    excel_writer.save()


    subject = '周二报表数据源'
    to_addrs = ['feng.feng@andpay.me', 'kesheng.wang@andpay.me']
    body_text = '周二报表数据'
    attachment_file = "/home/andpay/data/excel/tuesday_report.xlsx"

    EmailSend.send_email(subject, to_addrs, body_text, attachment_files=[attachment_file])





