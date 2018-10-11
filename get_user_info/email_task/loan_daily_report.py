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

init_ti_srv_cfg('ti-daf')
db_1 = SqlContext('/python/db/dev_dw_db')

def get_first_df():
    yesterday = datetime.date(datetime.today()) - dt.timedelta(days=1)
    today = datetime.date(datetime.today())
    org_day = yesterday - dt.timedelta(days=59)

    ytd_s = "'" + str(yesterday) + "'"
    org_day_s = "'" + str(org_day) + "'"

    sql_1 = '''
        select a.mon mon,a.apply_times apply_times,b.acess_times access_times,round(b.acess_times/a.apply_times,4)  access_rate,
        c.loan_times loan_times,round(c.loan_times/b.acess_times,4) withdraw_rate ,c.amt loanamt,round(c.amt/c.loan_times,2) avg_amt 
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
            where status like '%A%' and  applytype not in ('repayCredit')
            group by to_char(applydate,'yyyy-mm-dd')
        ) b
        on a.mon=b.mon
        left join 
        (
            select to_char(loantime,'yyyy-mm-dd') mon,count(distinct a.id) loan_times,sum(loanamt) amt 
            from dev_dw.f_loanagreement  a
            left join dev_dw.f_loanapplyinfo b
            on a.applyid=b.applyno
            where loanstatus in ('D','O','R','E')  and  b.applytype not in ('repayCredit') 
            group by to_char(loantime,'yyyy-mm-dd')
        ) c
        on a.mon=c.mon
        where a.mon >=''' + org_day_s + '''  and a.mon<=''' + ytd_s
    # 起始日期需要修改与底下orangin_time同步


    with session_scope(db_1) as session:
        re_list = []
        for row in iselect_rows_by_sql(session, sql_1, []):
            re_list.append(list(row))

        first_df = pd.DataFrame(re_list,
                                columns=['date', 'applytimes', 'passtimes', 'passrate', 'withdrawtimes', 'withdrawrate',
                                         'loanamt', 'avg_amt'])
        first_df = first_df.sort_values(by=['date'], ascending=[1])

    orangin_time = datetime.date(datetime.strptime('2017-09-18', '%Y-%m-%d'))
    # 起始日期需要修改
    circle = (yesterday - orangin_time).days + 1

    # 余额统计
    loan_yue = []
    for i in range(60):
        time_s = today - dt.timedelta(days=i + 1)
        time = "'" + str(time_s) + "'"

        sql_2 = '''
            select sum(dueprincipal)
            from dev_dw.f_loanagreement a
            left join dev_dw.f_loanrepayschedule b
            on a.id=b.idloanagreement 
            left join dev_dw.f_loanapplyinfo c
            on a.applyid=c.applyno
            where (b.repaytime is null or  b.repaytime-1>to_date(''' + time + ''','yyyy-mm-dd'))  and a.loantime-1<to_date(''' + time + ''','yyyy-mm-dd')
            and (to_date(''' + time + ''','yyyy-mm-dd')-b.duedate<33 or (b.repaytime is null and to_date(''' + time + ''','yyyy-mm-dd')-b.duedate<33))
            and b.idproduct<>21 and a.loanstatus in ('D','O','R','E')
            '''

        with session_scope(db_1) as session:
            for row in iselect_rows_by_sql(session, sql_2, []):
                loan_yue.append([str(time_s), row[0]])

    yue_df = pd.DataFrame(loan_yue, columns=['date', 'yue'])
    yue_df = yue_df.sort_values(by=['date'], ascending=[1])

    first_df = pd.merge(first_df, yue_df, on='date', how='outer')

    first_df = first_df.rename(
        columns={'date': '日期', 'applytimes': '申请笔数', 'passtimes': '通过笔数', 'passrate': '通过率', 'withdrawtimes': '提现笔数',
                 'withdrawrate': '提现率', 'loanamt': '放款额', 'yue': '贷款余额', 'avg_amt': '笔均放款额'})

    first_df = first_df.sort_values(by='日期', ascending=0)
    first_df = first_df.fillna(0)

    return first_df


def get_loan_after_df():
    today = datetime.date(datetime.today())
    yesterday = today - dt.timedelta(days=1)
    org_day = yesterday - dt.timedelta(days=59)

    today_s = "'" + str(today) + "'"
    yesterday_s = "'" + str(yesterday) + "'"
    org_day_s = "'" + str(org_day) + "'"

    sql_1 = '''
    select a.day,a.times,b.times,a.amt,b.amt,c.times,c.amt,d.times,d.amt,e.times,e.amt from 
    (
        select to_char(duedate,'yyyy-mm-dd') day,sum(dueprincipal) amt,count(distinct idloanagreement) times
        from dev_dw.f_loanrepayschedule
        where (repaytime>=duedate or repaytime is null) and idproduct<>21
        group by to_char(duedate,'yyyy-mm-dd')
    ) a
    left join 
    (
        select to_char(duedate,'yyyy-mm-dd') day,sum(dueprincipal) amt,count(distinct idloanagreement) times
        from dev_dw.f_loanrepayschedule
        where to_char(repaytime,'yyyy-mm-dd')=to_char(duedate,'yyyy-mm-dd') and idproduct<>21
        group by to_char(duedate,'yyyy-mm-dd')
    ) b
    on a.day=b.day
    left join
    (
        select to_char(duedate,'yyyy-mm-dd') day,sum(dueprincipal) amt,count(distinct idloanagreement) times
        from dev_dw.f_loanrepayschedule
        where (repaytime >duedate+1 or repaytime is null) and idproduct<>21
        group by to_char(duedate,'yyyy-mm-dd')
    ) c
    on a.day=c.day
    left join 
    (
        select to_char(repaytime,'yyyy-mm-dd') day,sum(dueprincipal) amt,count(distinct idloanagreement) times
        from dev_dw.f_loanrepayschedule
        where repaytime<duedate and idproduct<>21
        group by to_char(repaytime,'yyyy-mm-dd')
    ) d
    on a.day=d.day
    left join
    (
        select to_char(repaytime,'yyyy-mm-dd') day,sum(dueprincipal) amt,count(distinct idloanagreement) times
        from dev_dw.f_loanrepayschedule
        where repaytime>duedate+1 and idproduct<>21
        group by to_char(repaytime,'yyyy-mm-dd')
    ) e
    on a.day=e.day
    where a.day>=''' + org_day_s + ''' and a.day<=''' + yesterday_s + '''
    order by a.day'''

    repay_list = []
    with session_scope(db_1) as session:
        for row in iselect_rows_by_sql(session, sql_1, []):
            ls = [datetime.date(datetime.strptime(row[0], '%Y-%m-%d'))] + list(row[1:11])
            repay_list.append(ls)

    repay_df = pd.DataFrame(repay_list,
                            columns=['day', 'repay_t', 'repaid_t', 'repay_amt', 'repaid_amt', 'nopay_t', 'nopay_amt',
                                     'advance_t', 'advance_amt', 'over_t', 'over_amt'])
    repay_df = repay_df.sort_values(by=['day'], ascending=[1])
    repay_df = repay_df.fillna(0)

    # print(repay_df.shape[0])

    # --------------------------------------------

    oragin_day = datetime.date(datetime.strptime('2017-09-18', '%Y-%m-%d'))
    circle = (today - oragin_day).days

    in_collect_list = []
    day_list = []
    back_collect_list = []
    dayback_collect_list = []
    now_over_list = []
    for i in range(60):
        time = today - dt.timedelta(days=i + 1)

        day_list.append(time)

        time_s = "'" + str(time) + "'"

        sql_2 = '''
        select * from 
        (
        select case when  to_date(''' + time_s + ''' ,'yyyy-mm-dd') -duedate>3 and to_date(''' + time_s + ''','yyyy-mm-dd') -duedate<=4 then 'M1'
                    when  to_date(''' + time_s + ''','yyyy-mm-dd')-duedate>33 and  to_date(''' + time_s + ''','yyyy-mm-dd')-duedate<=34 then 'M2' end categroy,
                sum(dueprincipal),count(distinct idloanagreement)
        from   dev_dw.f_loanrepayschedule 
        where (repaytime>=to_date(''' + time_s + ''','yyyy-mm-dd') or repaytime is null ) and idproduct<>21
        group by 
        case when  to_date(''' + time_s + ''' ,'yyyy-mm-dd') -duedate>3 and to_date(''' + time_s + ''','yyyy-mm-dd') -duedate<=4 then 'M1'
                    when  to_date(''' + time_s + ''','yyyy-mm-dd')-duedate>33 and  to_date(''' + time_s + ''','yyyy-mm-dd')-duedate<=34 then 'M2' end
        )
        where categroy is not null
        '''
        # print (sql_2)

        with session_scope(db_1) as session:
            for row in iselect_rows_by_sql(session, sql_2, []):
                ls = [time] + list(row)
                in_collect_list.append(ls)

        sql_2_1 = '''
        select b.categroy,sum(a.dueprincipal),count(distinct a.idloanagreement) from dev_dw.f_loanrepayschedule a
        right join
        (
        select case when  to_date(''' + time_s + ''' ,'yyyy-mm-dd') -duedate>3 and to_date(''' + time_s + ''','yyyy-mm-dd') -duedate<=4 then 'M1'
                    when  to_date(''' + time_s + ''','yyyy-mm-dd')-duedate>33 and  to_date(''' + time_s + ''','yyyy-mm-dd')-duedate<=34 then 'M2' end categroy,idloanrepayschedule
        from   dev_dw.f_loanrepayschedule 
        where (repaytime>=to_date(''' + time_s + ''','yyyy-mm-dd') or repaytime is null) and idproduct<>21
        ) b
        on a.idloanrepayschedule=b.idloanrepayschedule
        where categroy is not null and a.repaytime is not null
        group by b.categroy
        '''

        with session_scope(db_1) as session:
            for row in iselect_rows_by_sql(session, sql_2_1, []):
                ls = [time] + list(row)
                back_collect_list.append(ls)

        sql_2_2 = '''
        select * from 
        (
            select  case when repaytime-duedate>3 and repaytime-duedate<=33 then 'M1' 
                         when repaytime-duedate>33  then 'M2' end categroy,sum(dueprincipal),count(distinct idloanagreement)
            from dev_dw.f_loanrepayschedule
            where to_char(repaytime,'yyyy-mm-dd')=''' + time_s + ''' and idproduct<>21
            group by 
            case when repaytime-duedate>3 and repaytime-duedate<=33 then 'M1' 
                         when repaytime-duedate>33  then 'M2' end 
        )
        where categroy is not null
        '''

        with session_scope(db_1) as session:
            for row in iselect_rows_by_sql(session, sql_2_2, []):
                ls = [time] + list(row)
                dayback_collect_list.append(ls)

        # 当前逾期数据
        sql_5 = '''
        select * from 
        (
        select  case when to_date(''' + time_s + ''','yyyy-mm-dd')-a.duedate>3  and to_date(''' + time_s + ''','yyyy-mm-dd')-a.duedate<=33 then 'M1'
                     when to_date(''' + time_s + ''','yyyy-mm-dd')-a.duedate>33 then 'M2' end categroy,sum(a.dueprincipal),count(distinct idloanagreement)
        from dev_dw.f_loanrepayschedule a
        left join dev_dw.f_loanagreement b
        on a.idloanagreement=b.id
        where (to_date(''' + time_s + ''','yyyy-mm-dd')<a.repaytime or a.repaytime is null)  
        and loantime>=to_date('2017-01-01','yyyy-mm-dd') and a.idproduct<>21
        group by 
        case when to_date(''' + time_s + ''','yyyy-mm-dd')-a.duedate>3  and to_date(''' + time_s + ''','yyyy-mm-dd')-a.duedate<=33 then 'M1'
             when to_date(''' + time_s + ''','yyyy-mm-dd')-a.duedate>33 then 'M2' end 
        )
        where categroy is not null
        '''

        with session_scope(db_1) as session:
            for row in iselect_rows_by_sql(session, sql_5, []):
                ls = [time] + list(row)
                now_over_list.append(ls)

    in_collect_df = pd.DataFrame(in_collect_list, columns=['day', 'categroy', 'in_amt', 'in_times'])
    back_collect_df = pd.DataFrame(back_collect_list, columns=['day', 'categroy', 'back_amt', 'back_times'])
    dayback_collect_df = pd.DataFrame(dayback_collect_list, columns=['day', 'categroy', 'dayback_amt', 'dayback_times'])
    now_over_df = pd.DataFrame(now_over_list, columns=['day', 'categroy', 'over_amt', 'over_times'])

    collect_df = pd.merge(in_collect_df, back_collect_df, on=['day', 'categroy'], how='outer')
    collect_df = pd.merge(collect_df, dayback_collect_df, on=['day', 'categroy'], how='outer')
    collect_df = pd.merge(collect_df, now_over_df, on=['day', 'categroy'], how='outer')

    assist_collect_df = pd.DataFrame(
        {'day': day_list, 'M1_in_times': np.nan, 'M1_back_times': np.nan, 'M1_in_amt': np.nan, 'M1_back_amt': np.nan,
         'M2_in_times': np.nan, 'M2_back_times': np.nan, 'M2_in_amt': np.nan, 'M2_back_amt': np.nan,
         'M1_dayback_times': np.nan,
         'M1_dayback_amt': np.nan, 'M2_dayback_times': np.nan, 'M2_dayback_amt': np.nan, 'M1_over_times': np.nan,
         'M1_over_amt': np.nan,
         'M2_over_times': np.nan, 'M2_over_amt': np.nan},
        columns=['day', 'M1_in_times', 'M1_back_times', 'M1_in_amt', 'M1_back_amt', 'M2_in_times', 'M2_back_times',
                 'M2_in_amt', 'M2_back_amt',
                 'M1_dayback_times', 'M1_dayback_amt', 'M2_dayback_times', 'M2_dayback_amt', 'M1_over_times',
                 'M1_over_amt', 'M2_over_times', 'M2_over_amt'])

    for day, cate in zip(collect_df['day'], collect_df['categroy']):
        in_times = collect_df[(collect_df['day'] == day) & (collect_df['categroy'] == cate)]['in_times'].values[0]
        in_amt = collect_df[(collect_df['day'] == day) & (collect_df['categroy'] == cate)]['in_amt'].values[0]
        back_times = collect_df[(collect_df['day'] == day) & (collect_df['categroy'] == cate)]['back_times'].values[0]
        back_amt = collect_df[(collect_df['day'] == day) & (collect_df['categroy'] == cate)]['back_amt'].values[0]
        dayback_times = \
        collect_df[(collect_df['day'] == day) & (collect_df['categroy'] == cate)]['dayback_times'].values[0]
        dayback_amt = collect_df[(collect_df['day'] == day) & (collect_df['categroy'] == cate)]['dayback_amt'].values[0]
        over_times = collect_df[(collect_df['day'] == day) & (collect_df['categroy'] == cate)]['over_times'].values[0]
        over_amt = collect_df[(collect_df['day'] == day) & (collect_df['categroy'] == cate)]['over_amt'].values[0]
        assist_collect_df.ix[assist_collect_df['day'] == day, cate + '_in_times'] = in_times
        assist_collect_df.ix[assist_collect_df['day'] == day, cate + '_in_amt'] = in_amt
        assist_collect_df.ix[assist_collect_df['day'] == day, cate + '_back_times'] = back_times
        assist_collect_df.ix[assist_collect_df['day'] == day, cate + '_back_amt'] = back_amt
        assist_collect_df.ix[assist_collect_df['day'] == day, cate + '_dayback_times'] = dayback_times
        assist_collect_df.ix[assist_collect_df['day'] == day, cate + '_dayback_amt'] = dayback_amt
        assist_collect_df.ix[assist_collect_df['day'] == day, cate + '_over_times'] = over_times
        assist_collect_df.ix[assist_collect_df['day'] == day, cate + '_over_amt'] = over_amt

    assist_collect_df = assist_collect_df.fillna(0)

    # ---------------------------------------------------------------------
    # 累计逾期数据
    cumulative_over = []
    for i in range(60):
        time = today - dt.timedelta(days=i + 1)

        time_s = "'" + str(time) + "'"

        sql_3 = '''
        select count(distinct b.idloanagreement),sum(dueprincipal) from dev_dw.f_loanagreement a
        left join dev_dw.f_loanrepayschedule b
        on a.id=b.idloanagreement
        where   loantime>=to_date('2017-01-01','yyyy-mm-dd') 
        and ( b.repaytime is null  or b.repaytime>to_date(''' + time_s + ''','yyyy-mm-dd'))
        and  to_date(''' + time_s + ''','yyyy-mm-dd')-b.duedate>3  and  a.idproduct<>21 '''

        with session_scope(db_1) as session:
            for row in iselect_rows_by_sql(session, sql_3, []):
                ls = [time] + list(row)
                cumulative_over.append(ls)

    cumulative_over_df = pd.DataFrame(cumulative_over, columns=['day', 'cu_times', 'cu_amt'])

    # ---------------------------------------------------
    # 逾期率计算
    over_rate = []
    for i in range(60):
        time = today - dt.timedelta(days=i + 1)

        time_s = "'" + str(time) + "'"

        sql_4 = '''
        select case when((b.repaytime is null and to_date(''' + time_s + ''','yyyy-mm-dd')-b.duedate>33) or 
                (b.repaytime-b.duedate>33 and  to_date(''' + time_s + ''','yyyy-mm-dd')<b.repaytime)) then 'M2'
               else 'normal' end fl,sum(dueprincipal)
        from dev_dw.f_loanagreement a
        left join dev_dw.f_loanrepayschedule b
        on a.id=b.idloanagreement
        where loantime>=to_date('2017-01-01','yyyy-mm-dd') and loantime<=to_date(''' + time_s + ''','yyyy-mm-dd')-60  
        and a.idproduct<>21 
        group by 
        case when ((b.repaytime is null and to_date(''' + time_s + ''','yyyy-mm-dd')-b.duedate>33) or 
                    (b.repaytime-b.duedate>33 and  to_date(''' + time_s + ''','yyyy-mm-dd')<b.repaytime)) then 'M2'
               else 'normal' end
        '''

        with session_scope(db_1) as session:
            for row in iselect_rows_by_sql(session, sql_4, []):
                ls = [time] + list(row)
                over_rate.append(ls)

    over_rate_df = pd.DataFrame(over_rate, columns=['day', 'categroy', 'amt'])
    over_rate_table = pd.pivot_table(over_rate_df, values='amt', index='day', columns='categroy',
                                     aggfunc=np.sum).reset_index()
    over_rate_table['rate'] = over_rate_table['M2'] / (over_rate_table['M2'] + over_rate_table['normal'])
    end_over_rate = over_rate_table.ix[:, ['day', 'rate']]

    # -------------------------------------------------------------------


    loan_after_df = pd.merge(repay_df, assist_collect_df, on='day')
    loan_after_df = pd.merge(loan_after_df, cumulative_over_df, on='day')
    loan_after_df = pd.merge(loan_after_df, end_over_rate, on='day')

    loan_after_df = loan_after_df.rename(
        columns={'day': '日期', 'repay_t': '应还笔数', 'repaid_t': '已还笔数', 'repay_amt': '应还金额', 'repaid_amt': '已还金额',
                 'nopay_t': '未还笔数', 'nopay_amt': '未还金额', 'M1_in_times': 'M1入催笔数', 'M1_in_amt': 'M1入催金额',
                 'M2_in_times': 'M2入催笔数', 'M2_in_amt': 'M2入催金额',
                 'M1_back_times': 'M1已催回笔数', 'M1_back_amt': 'M1已催回金额', 'M2_back_times': 'M2已催回笔数',
                 'M2_back_amt': 'M2已催回金额', 'M1_dayback_times': 'M1当日催回笔数',
                 'M1_dayback_amt': 'M1当日催回金额', 'M2_dayback_times': 'M2当日催回笔数', 'M2_dayback_amt': 'M2当日催回金额',
                 'cu_times': '2017累计逾期笔数', 'cu_amt': '2017累计逾期金额',
                 'rate': 'M2逾期率', 'M1_over_times': 'M1当前逾期笔数', 'M1_over_amt': 'M1当前逾期金额', 'M2_over_times': 'M2当前逾期笔数',
                 'M2_over_amt': 'M2当前逾期金额',
                 'advance_t': '提前还款笔数', 'advance_amt': '提前还款金额', 'over_t': '逾期还款笔数', 'over_amt': '逾期还款金额'})

    loan_after_df = loan_after_df.sort_values(by='日期', ascending=0)
    loan_after_df = loan_after_df.fillna(0)

    return loan_after_df


def get_end_audit_df():
    today = datetime.date(datetime.today())
    yesterday = today - dt.timedelta(days=1)
    org_day = yesterday - dt.timedelta(days=59)

    today_s = "'" + str(today) + "'"
    yesterday_s = "'" + str(yesterday) + "'"
    org_day_s = "'" + str(org_day) + "'"

    sql_1 = '''
    select * from 
    (
        select  to_char(applydate,'yyyy-mm-dd'),
                case when status ='SA' then 'system_pass'
                    when status='S'   then 'system_refuse'
                    when status in ('RA','GA','LA','A') then 'manual_pass' 
                    when status in ('TD','GD','CD','LD','D') then 'manual_refuse'
                end categroy,count(distinct id)
        from  dev_dw.f_loanapplyinfo
        where applydate>=to_date(''' + org_day_s + ''','yyyy-mm-dd') and applydate-1<=to_date(''' + yesterday_s + ''','yyyy-mm-dd') 
        and  (applytype<>'repayCredit' or  applytype is null)
        group by 
        to_char(applydate,'yyyy-mm-dd'),
        case when status ='SA' then 'system_pass'
             when status='S'   then 'system_refuse'
             when status in ('RA','GA','LA','A') then 'manual_pass' 
             when status in ('TD','GD','CD','LD','D') then 'manual_refuse'
        end
    )
    where categroy  is not null
    '''

    audit_list = []
    with session_scope(db_1) as session:
        for row in iselect_rows_by_sql(session, sql_1, []):
            audit_list.append(list(row))

    audit_df = pd.DataFrame(audit_list, columns=['day', 'categroy', 'times'])

    # audit_table=pd.pivot_table(audit_df,values='times',index='day',columns='categroy',aggfunc=np.sum).reset_index()

    day_list = sorted(list(audit_df['day'].unique()))
    audit_table = pd.DataFrame({'day': day_list, 'manual_pass': np.nan, 'manual_refuse': np.nan, 'system_pass': np.nan,
                                'system_refuse': np.nan},
                               columns=['day', 'manual_pass', 'manual_refuse', 'system_pass', 'system_refuse'])

    # def turn_df(df,first_para,second_para):
    #     for day,cate in zip(df[first_para],df[second_para]):
    #         result=df[(df[first_para]==day) & (df[second_para]==cate)]['in_times'].values[0]
    #         assist_collect_df.ix[assist_collect_df['day']==day,cate+'_in_times']=in_times

    #     pass


    for day, cate in zip(audit_df['day'], audit_df['categroy']):
        result = audit_df[(audit_df['day'] == day) & (audit_df['categroy'] == cate)]['times'].values[0]
        audit_table.ix[audit_table['day'] == day, cate] = result

    # audit_table=audit_table.fillna(0)


    manual_rate = audit_table['manual_pass'] / (audit_table['manual_pass'] + audit_table['manual_refuse'])
    system_rate = audit_table['system_pass'] / (audit_table['system_pass'] + audit_table['system_refuse'])
    audit_table.insert(3, 'manual_rate', manual_rate)
    audit_table.insert(6, 'system_rate', system_rate)
    audit_table = audit_table.fillna(0)

    sql_2 = '''
    select to_char(applydate,'yyyy-mm-dd'),count(distinct id)  from dev_dw.f_loanapplyinfo
    where applydate>=to_date(''' + org_day_s + ''','yyyy-mm-dd') and applydate-1<=to_date(''' + yesterday_s + ''','yyyy-mm-dd')
    and (applytype<>'repayCredit' or  applytype is null)
    group by to_char(applydate,'yyyy-mm-dd')
    '''

    apply_list = []
    with session_scope(db_1) as session:
        for row in iselect_rows_by_sql(session, sql_2, []):
            apply_list.append(list(row))

    apply_df = pd.DataFrame(apply_list, columns=['day', 'apply_times'])

    end_audit_df = pd.merge(apply_df, audit_table, on='day')
    end_audit_df = end_audit_df.sort_values(by='day', ascending=1)

    end_audit_df = end_audit_df.rename(
        columns={'manual_pass': '人工通过笔数', 'manual_refuse': '人工拒绝笔数', 'manual_rate': '人工通过率', 'apply_times': '申请笔数',
                 'system_pass': '系统通过笔数', 'system_refuse': '系统拒绝笔数', 'system_rate': '系统通过率', 'day': '日期'})

    end_audit_df = end_audit_df.sort_values(by='日期', ascending=0)
    end_audit_df = end_audit_df.fillna(0)

    return end_audit_df


def get_end_user_categroy_df():
    today = datetime.date(datetime.today())
    yesterday = today - dt.timedelta(days=1)
    org_day = yesterday - dt.timedelta(days=59)

    today_s = "'" + str(today) + "'"
    yesterday_s = "'" + str(yesterday) + "'"
    org_day_s = "'" + str(org_day) + "'"

    # 分类申请审批数据
    user_list = []
    new_user_category_list = []
    for i in range(60):
        time = today - dt.timedelta(days=i + 1)

        time_s = "'" + str(time) + "'"

        sql_1 = ''' 
        select  case when b.partyid is  null  then 'new_u' else 'old_u' end user_categroy,
                case when status like '%A%' then 'pass' else 'refuse' end audit_categroy, 
                count(distinct id) 
        from dev_dw.f_loanapplyinfo a
        left join 
        (
            select distinct partyid from dev_dw.f_loanagreement
            where loantime <to_date(''' + time_s + ''','yyyy-mm-dd') and loanstatus in ('D','O','R','E') and idproduct<>21
        ) b
        on a.partyid=b.partyid
        where to_char(applydate,'yyyy-mm-dd')=''' + time_s + ''' and (applytype<>'repayCredit' or  applytype is null)
        group by case when b.partyid is null then 'new_u' else 'old_u' end,
        case when status like '%A%' then 'pass' else 'refuse' end
        '''

        sql_add1 = ''' 
        select  
        count(distinct case when  d.partyid  in ('1014816000447488','1014816000774940','1014816000735566')  then a.partyid end ) wbdl,
        count(distinct case when c.lasttxndate  is null then a.partyid end) wjy,
        count(distinct case when  d.hhflag='Y' then a.partyid end ) hhr
        from dev_dw.f_loanapplyinfo a
        left join 
        (
            select distinct partyid from dev_dw.f_loanagreement
            where loantime <to_date(''' + time_s + ''','yyyy-mm-dd') and loanstatus in ('D','O','R','E') and idproduct<21
        ) b
        on a.partyid=b.partyid
        left join dev_dw.dim_txnparty c
        on a.partyid=c.partyid
        left join dev_dw.f_agentparty d
        on c.agentpartygrpid=d.partyid
        where b.partyid is null and to_char(applydate,'yyyy-mm-dd')=''' + time_s + '''
        and (a.applytype<>'repayCredit' or  a.applytype is null) '''

        with session_scope(db_1) as session:
            for row in iselect_rows_by_sql(session, sql_1, []):
                ls = [time] + list(row)
                user_list.append(ls)

        with session_scope(db_1)  as session:
            for row in iselect_rows_by_sql(session, sql_add1, []):
                ls = [time] + list(row)
                new_user_category_list.append(ls)

    user_df = pd.DataFrame(user_list, columns=['day', 'user_categroy', 'audit_categroy', 'times'])
    new_user_category_df = pd.DataFrame(new_user_category_list, columns=['day', 'wbdl', 'wjyyh', 'hhr'])

    user_categroy_df = pd.pivot_table(user_df, values='times', index='day', columns='user_categroy',
                                      aggfunc=np.sum).reset_index()
    user_categroy_df = user_categroy_df.sort_values(by='day', ascending=1)
    user_categroy_df['new_proportion'] = user_categroy_df['new_u'] / (
    user_categroy_df['new_u'] + user_categroy_df['old_u'])
    all_user = user_categroy_df['new_u'] + user_categroy_df['old_u']
    user_categroy_df.insert(1, 'apply_user', all_user)

    user_audit_df = pd.pivot_table(user_df, values='times', index='day', columns=['user_categroy', 'audit_categroy'],
                                   aggfunc=np.sum).reset_index()
    user_audit_df = user_audit_df.sort_values(by='day', ascending=1)
    new_pass_rate = user_audit_df['new_u']['pass'] / (user_audit_df['new_u']['pass'] + user_audit_df['new_u']['refuse'])
    old_pass_rate = user_audit_df['old_u']['pass'] / (user_audit_df['old_u']['pass'] + user_audit_df['old_u']['refuse'])
    user_audit_df.insert(3, 'new_pass_rate', new_pass_rate)
    user_audit_df.insert(6, 'old_pass_rate', old_pass_rate)

    apply_df = pd.merge(user_categroy_df, new_user_category_df, on='day', how='outer')
    apply_df = pd.merge(apply_df, user_audit_df, on='day', how='outer')

    apply_df.columns = ['day', 'apply_user', 'new_u', 'old_u', 'new_pro', 'wbdl', 'hhr', 'wjyyh', 'new_u_pass',
                        'new_u_refuse', 'new_u_passreate', 'old_u_pass', 'old_u_refuse', 'old_u_passrate']
    apply_df = apply_df.drop(['new_u_refuse', 'old_u_refuse'], axis=1)

    # apply_df=apply_df.rename(columns={'apply_user':'申请人数','new_u':'新用户','old_u':'老用户','new_proportion':'新用户占比',('new_u','pass')='新用户通过人数'})
    apply_df.columns = ['日期', '申请人数', '新用户', '旧用户', '新用户占比', '新用户(外部导流)', '新用户(无交易)', '新用户(合伙人)', '新户通过笔数', '新户通过率',
                        '旧户通过笔数', '旧户通过率']

    # --------------------------------------------------------
    # 分类放款及余额数据

    withdraw_list = []
    balance_list = []
    nowloan_list = []
    for i in range(60):
        time = today - dt.timedelta(days=i + 1)

        time_s = "'" + str(time) + "'"

        # 放款计算
        sql_2 = '''
        select   case when a.id=b.id then 'new_u' else 'old_u' end user_categroy,count(distinct a.id),sum(loanamt)  
        from dev_dw.f_loanagreement a
        left join
        (
            select  partyid,id,row_number() over (partition by partyid order by id ) ranks from dev_dw.f_loanagreement
            where loanstatus in ('D','O','R','E') 
            and idproduct<>21
        ) b
        on a.partyid=b.partyid and ranks=1
        where to_char(loantime,'yyyy-mm-dd')=''' + time_s + ''' and idproduct<>21  and loanstatus in ('D','O','R','E') 
        group by  case when a.id=b.id then 'new_u' else 'old_u' end  '''

        with session_scope(db_1) as session:
            for row in iselect_rows_by_sql(session, sql_2, []):
                ls = [time] + list(row)
                withdraw_list.append(ls)

        # 余额计算
        sql_3 = '''
        select  case when c.partyid is not  null then 'new_u' else 'old_u' end categroy,sum(dueprincipal)
        from dev_dw.f_loanagreement a
        left join
        (
            select distinct partyid,loantime from 
            (
                select x.*,row_number() over (partition by partyid order by loantime) ranks from dev_dw.f_loanagreement x
                where idproduct<>21 and loantime-1<to_date(''' + time_s + ''','yyyy-mm-dd') and loanstatus in ('D','O','R','E') 
            )
            where ranks=1 
        ) c
        on a.partyid=c.partyid and a.loantime=c.loantime
        left join dev_dw.f_loanrepayschedule b
        on a.id=b.idloanagreement
        where (b.repaytime is null or  b.repaytime-1>to_date(''' + time_s + ''','yyyy-mm-dd')) 
              and a.loantime-1<to_date(''' + time_s + ''','yyyy-mm-dd') and a.idproduct<>21
              and (to_date(''' + time_s + ''','yyyy-mm-dd')-b.duedate<33 or (b.repaytime is null and to_date(''' + time_s + ''','yyyy-mm-dd')-b.duedate<33))
        group by  case when c.partyid is not  null then 'new_u' else 'old_u' end '''

        with session_scope(db_1) as session:
            for row in iselect_rows_by_sql(session, sql_3, []):
                ls = [time] + list(row)
                balance_list.append(ls)

        # 在贷数据计算
        sql_4 = '''
        select case when b.partyid is not null then 'new_u' else 'old_u' end categroy,count(distinct a.id),count(distinct a.partyid)  
        from dev_dw.f_loanagreement a
        left join 
        (
            select distinct partyid,loantime from 
            (
                select x.*,row_number() over (partition by partyid order by loantime) ranks from dev_dw.f_loanagreement x
                where idproduct<>21 and loantime-1<to_date(''' + time_s + ''','yyyy-mm-dd') and loanstatus in ('D','O','R','E') 
            )
            where ranks=1 
        ) b
        on a.partyid=b.partyid and a.loantime=b.loantime
        where a.loantime-1<to_date(''' + time_s + ''','yyyy-mm-dd')  and  (a.repaytime-1>to_date(''' + time_s + ''','yyyy-mm-dd') or a.repaytime is null) 
        and a.idproduct<>21 and a.loanstatus in ('D','O','R','E') 
        group by case when b.partyid is not null then 'new_u' else 'old_u' end '''

        with session_scope(db_1) as session:
            for row in iselect_rows_by_sql(session, sql_4, []):
                ls = [time] + list(row)
                nowloan_list.append(ls)

    withdraw_df = pd.DataFrame(withdraw_list, columns=['day', 'user_categroy', 'times', 'amt'])
    withdraw_df['amt'] = withdraw_df['amt'].astype(float)
    withdraw_table = pd.pivot_table(withdraw_df, index='day', columns='user_categroy', values=['times', 'amt'],
                                    aggfunc='sum').reset_index()

    balance_df = pd.DataFrame(balance_list, columns=['day', 'user_categroy', 'balance'])
    balance_table = pd.pivot_table(balance_df, index='day', columns='user_categroy', values='balance',
                                   aggfunc=np.sum).reset_index()
    balance_table['all'] = balance_table['new_u'] + balance_table['old_u']

    nowloan_df = pd.DataFrame(nowloan_list, columns=['day', 'user_categroy', 'times', 'persons'])
    nowloan_table = pd.pivot_table(nowloan_df, index='day', columns='user_categroy', values=['times', 'persons'],
                                   aggfunc=np.sum).reset_index()
    all_persons = nowloan_table['persons']['new_u'] + nowloan_table['persons']['old_u']
    all_times = nowloan_table['times']['new_u'] + nowloan_table['times']['old_u']
    nowloan_table.insert(3, 'all_persons', all_persons)
    nowloan_table.insert(6, 'all_times', all_times)
    # nowloan_table.insert(3,'all',all)


    loan_df = pd.merge(withdraw_table, balance_table, on='day', how='outer')
    loan_df = pd.merge(loan_df, nowloan_table, on='day', how='outer')
    print(loan_df.head())

    loan_df.columns = ['日期', 'rq', 'rq1', '新户放款金额', '旧户放款金额', '新户放款笔数', '旧户放款笔数', '新户贷款余额', '旧户贷款余额', '总余额', '新户在贷人数',
                       '旧户在贷人数',
                       '在贷总人数', '新户在贷笔数', '旧户在贷人笔数', '在贷总笔数']

    loan_df = loan_df.drop(['rq', 'rq1'], axis=1)

    print(loan_df.head())
    column_name = ['日期', '新户放款笔数', '旧户放款笔数', '新户放款金额', '旧户放款金额', '新户贷款余额', '旧户贷款余额', '总余额', '新户在贷人数', '旧户在贷人数',
                   '在贷总人数', '新户在贷笔数', '旧户在贷人笔数', '在贷总笔数']
    loan_df = loan_df.reindex(columns=column_name)

    end_user_categroy_df = pd.merge(apply_df, loan_df, on='日期', how='outer')

    # 操作最终结果，以上数据修改注意是否影响到以下操作
    newu_wrate = end_user_categroy_df['新户放款笔数'] / end_user_categroy_df['新户通过笔数']
    oldu_wrate = end_user_categroy_df['旧户放款笔数'] / end_user_categroy_df['旧户通过笔数']
    end_user_categroy_df.insert(16, '新户提现率', newu_wrate)
    end_user_categroy_df.insert(17, '旧户提现率', oldu_wrate)

    end_user_categroy_df = end_user_categroy_df.sort_values(by='日期', ascending=0)
    end_user_categroy_df = end_user_categroy_df.fillna(0)

    return end_user_categroy_df


def get_end_categroy_table():
    today = datetime.date(datetime.today())
    yesterday = today - dt.timedelta(days=1)
    org_day = yesterday - dt.timedelta(days=59)

    today_s = "'" + str(today) + "'"
    yesterday_s = "'" + str(yesterday) + "'"
    org_day_s = "'" + str(org_day) + "'"

    sql_1 = '''
    select to_char(loantime,'yyyy-mm-dd'),
    sum(case when idproduct in (19,20) then  loanamt end),
    sum(case when idproduct in (6,12)  then  loanamt end),
    sum(case when idproduct in (10,13) then  loanamt end), 
    sum(case when idproduct in (11,14) then  loanamt end),
    sum(case when idproduct in (17,18) then  loanamt end),
    sum(loanamt),
    count(distinct case when idproduct in (19,20) then id end),
    count(distinct case when idproduct in (6,12)  then id end),
    count(distinct case when idproduct in (10,13) then id end),
    count(distinct case when idproduct in (11,14) then id end),
    count(distinct case when idproduct in (17,18) then id end),
    count(distinct id)
    from dev_dw.f_loanagreement 
    where loantime>=to_date(''' + org_day_s + ''','yyyy-mm-dd') and loantime<to_date(''' + today_s + ''','yyyy-mm-dd') 
    and idproduct<>21 and loanstatus in ('D','O','R','E')
    group by  to_char(loantime,'yyyy-mm-dd')
    '''

    loan_list = []
    with session_scope(db_1) as session:
        for row in iselect_rows_by_sql(session, sql_1, []):
            loan_list.append(list(row))

    loan_df = pd.DataFrame(loan_list,
                           columns=['day', '7d_amt', '1m_amt', '3m_amt', '6m_amt', '12m_amt', 'all_amt', '7d_count',
                                    '1m_count', '3m_count',
                                    '6m_count', '12m_count', 'all_count'])
    loan_df = loan_df.fillna(0)

    # 分期余额计算
    balance_list = []
    for i in range(60):
        time_s = today - dt.timedelta(days=i + 1)
        time = "'" + str(time_s) + "'"

        sql_2 = '''
            select 
            sum(case when b.idproduct in (19,20) then  dueprincipal end),
            sum(case when b.idproduct in (6,12)  then  dueprincipal end),
            sum(case when b.idproduct in (10,13) then  dueprincipal end), 
            sum(case when b.idproduct in (11,14) then  dueprincipal end),
            sum(case when b.idproduct in (17,18) then  dueprincipal end),
            sum(dueprincipal)
            from dev_dw.f_loanagreement a
            left join dev_dw.f_loanrepayschedule b
            on a.id=b.idloanagreement
            where (b.repaytime is null or  b.repaytime-1>to_date(''' + time + ''','yyyy-mm-dd'))  and a.loantime-1<to_date(''' + time + ''','yyyy-mm-dd')
            and a.idproduct<>21 and a.loanstatus in ('D','O','R','E')
            and (to_date(''' + time + ''','yyyy-mm-dd')-b.duedate<33 or (b.repaytime is null and to_date(''' + time + ''','yyyy-mm-dd')-b.duedate<33))
        '''

        with session_scope(db_1) as session:
            for row in iselect_rows_by_sql(session, sql_2, []):
                balance_list.append([str(time_s)] + list(row))

    balance_df = pd.DataFrame(balance_list,
                              columns=['day', '7d_balance', '1m_balance', '3m_balance', '6m_balance', '12m_balance',
                                       'all_balance'])
    balance_df = balance_df.fillna(0)

    # 在贷数据计算
    categroy_list = []
    for i in range(60):
        time_s = today - dt.timedelta(days=i + 1)
        time = "'" + str(time_s) + "'"

        sql_3 = '''
        select 
            count(distinct case when idproduct in (19,20) then id end),
            count(distinct case when idproduct in (6,12)  then id end),
            count(distinct case when idproduct in (10,13) then id end),
            count(distinct case when idproduct in (11,14) then id end),
            count(distinct case when idproduct in (17,18) then id end),
            count(distinct id)
        from  dev_dw.f_loanagreement
        where  loantime-1<to_date(''' + time + ''','yyyy-mm-dd') and (repaytime-1>to_date(''' + time + ''','yyyy-mm-dd') or repaytime is null)
        and idproduct<>21  and  loanstatus in ('D','O','R','E') 
        and (to_date(''' + time + ''','yyyy-mm-dd')-duedate<33 or (repaytime is null and to_date(''' + time + ''','yyyy-mm-dd')-duedate<33))
        '''

        with session_scope(db_1) as session:
            for row in iselect_rows_by_sql(session, sql_3, []):
                categroy_list.append([str(time_s)] + list(row))

    categroy_df = pd.DataFrame(categroy_list,
                               columns=['day', '7d_count', '1m_count', '3m_count', '6m_count', '12m_count',
                                        'all_count'])
    categroy_df = categroy_df.fillna(0)

    end_categroy_table = pd.merge(loan_df, balance_df, on='day', how='outer')
    end_categroy_table = pd.merge(end_categroy_table, categroy_df, on='day', how='outer')
    end_categroy_table.columns = ['日期', '7天放款额', '1期放款额', '3期放款额', '6期放款额', '12期放款额', '总放款额', '7天放款笔数', '1期放款笔数',
                                  '3期放款笔数', '6期放款笔数',
                                  '12期放款笔数', '总放款笔数', '7天贷款余额', '1期贷款余额', '3期贷款余额', '6期贷款余额', '12期贷款余额', '总贷款余额',
                                  '7天在贷笔数', '1期在贷笔数', '3期在贷笔数', '6期在贷笔数',
                                  '12期在贷笔数', '总在贷笔数']
    end_categroy_table = end_categroy_table.sort_values(by='日期', ascending=0)
    end_categroy_table = end_categroy_table.fillna(0)

    return  end_categroy_table


def get_end_newuser_df():
    yesterday = datetime.date(datetime.today()) - dt.timedelta(days=1)
    today = datetime.date(datetime.today())
    org_day = yesterday - dt.timedelta(days=59)

    ytd_s = "'" + str(yesterday) + "'"
    org_day_s = "'" + str(org_day) + "'"

    cate_list = []
    for i in range(60):
        time = today - dt.timedelta(days=i + 1)

        time_s = "'" + str(time) + "'"

        sql = '''
        select 
        case when e.partyid='1014816000774940' then '导流'
             when e.companytype='9'  then '秒分' 
             when e.hhflag='Y' then '合伙人'
             when e.partyid in ('1014816000387953','1014816000733855','1017278000148183','1014816000360130','1014816000248763','1015734000437922',
             '1017011000170016','1017911000006218','1015812000000174','1014816000403728','1014816000885629','1014816000735566','1016012000734415',
             '1014816000447488') then '官方'
             else '代理商' end  type,
        count(distinct a.id) apply_num,
        count(distinct case when a.status like '%A%' then a.id end ) pass_num,
        count(distinct case when b.loanstatus in ('D','O','R','E') then b.id end) loan_num,
        sum(case when b.loanstatus in ('D','O','R','E') then loanamt end ) loan_amt
        from  dev_dw.f_loanapplyinfo a
        left join dev_dw.f_loanagreement b
        on a.applyno=b.applyid
        left join 
        (--信用通老户
            select distinct partyid  from  dev_dw.f_loanagreement 
            where idproduct<>21 and loanstatus in ('D','O','R','E')
            and loantime<to_date(''' + time_s + ''','yyyy-mm-dd')
        ) c
        on a.partyid=c.partyid
        left join dev_dw.dim_txnparty d
        on a.partyid=d.partyid
        left join dev_dw.f_agentparty e
        on d.agentpartygrpid=e.partyid
        where a.idproduct<>21 and c.partyid is null
        and to_char(a.applydate,'yyyy-mm-dd')=''' + time_s + '''
        group by 
        case when e.partyid='1014816000774940' then '导流'
             when e.companytype='9'  then '秒分' 
             when e.hhflag='Y' then '合伙人'
             when e.partyid in ('1014816000387953','1014816000733855','1017278000148183','1014816000360130','1014816000248763','1015734000437922',
             '1017011000170016','1017911000006218','1015812000000174','1014816000403728','1014816000885629','1014816000735566','1016012000734415',
             '1014816000447488') then '官方'
             else '代理商' end
        '''

        cate_row = sql_util.select_rows_by_sql(sql_text=sql, sql_paras={}, ns_server_id='/python/db/dev_dw_db',
                                               max_size=-1)

        for row in cate_row:
            cate_list.append([time] + list(row))

    newuser_df = pd.DataFrame(cate_list, columns=['日期', 'cate', '申请笔数', '通过笔数', '放款笔数', '放款金额'])
    # newuser_df['num']=newuser_df['num'].astype(float)
    newuser_df = pd.pivot_table(newuser_df, index='日期', columns='cate',
                                values=['申请笔数', '通过笔数', '放款笔数', '放款金额']).reset_index()
    newuser_df.columns.names = ['cate', 'num']
    loan_amt = newuser_df[['放款笔数', '放款金额']]
    newuser_df = newuser_df.drop(['放款笔数', '放款金额'], axis=1)
    newuser_df = pd.concat([newuser_df, loan_amt], axis=1)
    end_newuser_df = newuser_df.sort_values(by='日期', ascending=0).fillna(0)
    # newuser_df=newuser_df.swaplevel('num','cate',axis=1)
    # newuser_df=newuser_df.sort_index(level=[0,1],ascending=[1,1],axis=1)
    end_newuser_df.columns.names = [None, None]


def get_end_renew_df():
    yesterday = datetime.date(datetime.today()) - dt.timedelta(days=1)
    today = datetime.date(datetime.today())
    org_day = yesterday - dt.timedelta(days=59)

    ytd_s = "'" + str(yesterday) + "'"
    org_day_s = "'" + str(org_day) + "'"

    sql = '''
    select   to_char(a.repaytime,'yyyy-mm-dd'),count(distinct a.id) repaynum,
    count(distinct case when d.ranks-c.ranks=1 and trunc(b.loantime)-trunc(a.repaytime)=0 then a.id end) currentday_renew,
    count(distinct case when d.ranks-c.ranks=1 and trunc(b.loantime)-trunc(a.repaytime)=1 then a.id end) dayone_renew,
    count(distinct case when d.ranks-c.ranks=1 and trunc(b.loantime)-trunc(a.repaytime)=2 then a.id end) daytwo_renew,
    count(distinct case when d.ranks-c.ranks=1 and trunc(b.loantime)-trunc(a.repaytime)=3 then a.id end) daythree_renew,
    count(distinct case when d.ranks-c.ranks=1 and trunc(b.loantime)-trunc(a.repaytime)>3 then a.id end) updaythree_renew
    from 
    (--到期名单
        select  partyid,repaytime,id  
        from dev_dw.f_loanagreement 
        where  loanstatus in ('D','O','R','E') and idproduct<>21
        and repaytime>=to_date(''' + org_day_s + ''','yyyy-mm-dd') and repaytime<to_date(''' + ytd_s + ''','yyyy-mm-dd')+1
    ) a
    left join 
    (--贷款名单
        select partyid,id,loantime
        from dev_dw.f_loanagreement 
        where  loanstatus in ('D','O','R','E') and idproduct<>21
    ) b 
    on a.partyid=b.partyid 
    left join
    (--到期位置排序
        select partyid,id,  
        row_number() over (partition by partyid order by id ) ranks
        from dev_dw.f_loanagreement 
        where loanstatus in ('D','O','R','E')  and idproduct<>21
    ) c
    on a.id=c.id
    left join
    (--贷款位置排序
        select partyid,id,  
        row_number() over (partition by partyid order by id ) ranks
        from dev_dw.f_loanagreement 
        where loanstatus in ('D','O','R','E')  and idproduct<>21
    ) d
    on b.id=d.id
    group by to_char(a.repaytime,'yyyy-mm-dd')
    order by to_char(a.repaytime,'yyyy-mm-dd') desc
    '''

    sql_row = sql_util.select_rows_by_sql(sql_text=sql, sql_paras={}, ns_server_id='/python/db/dev_dw_db', max_size=-1)

    renew_list = []
    for row in sql_row:
        renew_list.append(list(row))

    renew_df = pd.DataFrame(renew_list, columns=['day', 'repaynum', 'currentday_renew', 'dayone_renew', 'daytwo_renew',
                                                 'daythree_renew', 'updaythree_renew'])

    currentday_rate = renew_df['currentday_renew'] / renew_df['repaynum']
    dayone_rate = renew_df['dayone_renew'] / renew_df['repaynum']
    daytwo_rate = renew_df['daytwo_renew'] / renew_df['repaynum']
    daythree_rate = renew_df['daythree_renew'] / renew_df['repaynum']
    updaythree_rate = renew_df['updaythree_renew'] / renew_df['repaynum']
    renew_df.insert(7, 'currentday_rate', currentday_rate)
    renew_df.insert(8, 'dayone_rate', dayone_rate)
    renew_df.insert(9, 'daytwo_rate', daytwo_rate)
    renew_df.insert(10, 'daythree_rate', daythree_rate)
    renew_df.insert(11, 'updaythree_rate', updaythree_rate)
    renew_df.columns = ['日期', '还款笔数', '当天续借笔数', '第1天续借笔数', '第2天续借笔数', '第3天续借笔数', '3天以上续借笔数', '当天续借率', '第1天续借率',
                        '第2天续借率', '第3天续借率', '3天以上续借率']

    return renew_df



def get_call_result_df():
    today = datetime.date(datetime.today())
    yesterday = today - dt.timedelta(days=1)
    afterday = today + dt.timedelta(days=2)

    data_list = []
    for i in [1, 7, 15, 30]:
        day_before = today - dt.timedelta(days=i)

        # 判断是否是周日和周一
        if datetime.strftime(day_before, '%w') in (0, 6) and i == 1:
            continue
        else:
            sql = '''
            select  a.source,json_value(b.extattrs,'$."回电结果"') result,count(distinct a.id) 
            from  dev_dw.f_customer a
            left join  dev_dw.f_callinfo b
            on a.id=b.customerid
            where  a.source  in  ('未续借信用通商户','未提现信用通商户')
            and  b.crttime>=to_date(''' + "'" + str(day_before) + "'" + ''','yyyy-mm-dd')
            and  b.crttime<to_date(''' + "'" + str(today) + "'" + ''','yyyy-mm-dd')
            group by a.source,json_value(b.extattrs,'$."回电结果"') 
            '''

            sql_row = sql_util.select_rows_by_sql(sql_text=sql, sql_paras={}, ns_server_id='/python/db/dev_dw_db',
                                                  max_size=-1)

            for row in sql_row:
                data_list.append(['过去' + str(i) + '天'] + list(row))

    call_result_df = pd.DataFrame(data_list, columns=['cate', 'source', 'call_result', 'count'])
    call_result_df = pd.pivot_table(call_result_df, index='call_result', columns=['cate', 'source'],
                                    values='count').reset_index().fillna(0)
    call_result_df = call_result_df.reindex(columns=['call_result', '过去1天', '过去7天', '过去15天', '过去30天'], level=0)
    call_result_df = call_result_df.sort_values(by=[('过去30天', '未提现信用通商户')], ascending=0)

    return call_result_df


def get_m1_df():
    today = datetime.date(datetime.today())

    oragin_day = datetime.date(datetime.strptime('2017-09-18', '%Y-%m-%d'))
    circle = (today - oragin_day).days

    day_list = []
    M1_list = []
    for i in range(60):
        time = today - dt.timedelta(days=i + 1)

        day_list.append(time)

        time_s = "'" + str(time) + "'"

        sql = '''select c.personname,c.id,b.dueprincipal,a.loanamt,a.loantime,a.term,
            case when b.repaytime is not null  then '已还款' 
                 when b.repaytime is null  then '逾期中' end status
            from  dev_dw.f_loanagreement a
            left join dev_dw.f_loanrepayschedule b
            on a.id=b.idloanagreement 
            left join dev_dw.f_loanapplyinfo c
            on a.applyid=c.applyno
            where  b.duedate+4=to_date(''' + time_s + ''','yyyy-mm-dd') and (b.duedate+4<=b.repaytime or b.repaytime is null) and a.idproduct<>21 '''

        with session_scope(db_1) as session:
            for row in iselect_rows_by_sql(session, sql, []):
                m1_list = [time] + list(row)
                M1_list.append(m1_list)

    M1_df = pd.DataFrame(M1_list,
                         columns=['day', 'name', 'applyid', 'overamt', 'loanamt', 'loantime', 'term', 'status'])
    M1_df.columns = ['日期', '姓名', '申请编号', '逾期金额', '放款金额', '放款时间', '期数', '状态']
    M1_df = M1_df.sort_values(by='日期', ascending=0)
    M1_df = M1_df.fillna(0)

    return M1_df


def get_m2_df():
    today = datetime.date(datetime.today())

    oragin_day = datetime.date(datetime.strptime('2017-09-18', '%Y-%m-%d'))
    circle = (today - oragin_day).days

    day_list = []
    M2_list = []
    for i in range(60):
        time = today - dt.timedelta(days=i + 1)

        day_list.append(time)

        time_s = "'" + str(time) + "'"

        sql = '''select c.personname,c.id,b.dueprincipal,a.loanamt,a.loantime,a.term,
            case when b.repaytime is not null  then '已还款' 
                 when b.repaytime is null  then '逾期中' end status
            from  dev_dw.f_loanagreement a
            left join dev_dw.f_loanrepayschedule b
            on a.id=b.idloanagreement 
            left join dev_dw.f_loanapplyinfo c
            on a.applyid=c.applyno
            where  b.duedate+33=to_date(''' + time_s + ''','yyyy-mm-dd') and (b.duedate+33<=b.repaytime or b.repaytime is null) and a.idproduct<>21 '''

        with session_scope(db_1) as session:
            for row in iselect_rows_by_sql(session, sql, []):
                m2_list = [time] + list(row)
                M2_list.append(m2_list)

    M2_df = pd.DataFrame(M2_list,
                         columns=['day', 'name', 'applyid', 'overamt', 'loanamt', 'loantime', 'term', 'status'])
    M2_df.columns = ['日期', '姓名', '申请编号', '逾期金额', '放款金额', '放款时间', '期数', '状态']
    M2_df = M2_df.sort_values(by='日期', ascending=0)
    M2_df = M2_df.fillna(0)

    return M2_df


def get_end_rule_df():
    today = datetime.date(datetime.today())

    begin_time = str(today - dt.timedelta(days=60))
    end_time = '2017-11-15'

    begin_day = datetime.date(datetime.strptime(begin_time, '%Y-%m-%d'))
    end_day = datetime.date(datetime.strptime(end_time, '%Y-%m-%d'))

    begin_day_s = "'" + str(begin_day) + "'"
    end_day_s = "'" + str(today) + "'"

    sql = '''
     select  day,big_rule,small_rule,user_categroy,count(distinct lid) applyid_times   
        from 
        (
            select  distinct a.day day, a.lid lid,b.big_rule big_rule,b.small_rule  small_rule,
                case when c.partyid is null then '新用户' else '旧用户' end user_categroy
            from 
            (
                select distinct to_char(la.applydate,'yyyy-mm-dd')  day, la.id lid, rk.startexectime s_time,la.partyid partyid, 
                       substr(rulefullfuncname,instr(rulefullfuncname,'.',1,7)+1,instr(rulefullfuncname,'.',1,8)-instr(rulefullfuncname,'.',1,7)-1) set_rulename,
                       substr(rulefullfuncname,instr(rulefullfuncname,'.',1,8)+1,length(rulefullfuncname)-instr(rulefullfuncname,'.',1,8)) rulename,rulefullfuncname
                from dev_dw.f_loanapplyinfo la
                left join dev_dw.f_ruletaskexeclog rk
                on to_char(la.id)=rk.businesskey
                where applydate>=to_date(''' + begin_day_s + ''','yyyy-mm-dd') and applydate<to_date(''' + end_day_s + ''','yyyy-mm-dd') and ruledatainjson not like '%trialRun%'
                      and substr(rulefullfuncname,instr(rulefullfuncname,'.',1,7)+1,instr(rulefullfuncname,'.',1,8)-instr(rulefullfuncname,'.',1,7)-1)<>'storeblacklist'
                      and rk.conclusion='D' and (la.applytype<>'repayCredit' or  la.applytype is null)
            ) a
            left join dev_dw.wks_rule_describe b
            on a.rulename=b.rule_code 
            left join
            (
                select distinct partyid from dev_dw.f_loanagreement
                where loantime <to_date(''' + begin_day_s + ''','yyyy-mm-dd') and loanstatus in ('D','O','R','E')
            ) c
            on a.partyid=c.partyid
            where  a.set_rulename<>'flow' 
        )
        group by day,big_rule,small_rule,user_categroy

        '''

    data_list = []
    with session_scope(db_1) as session:
        for row in iselect_rows_by_sql(session, sql, []):
            data_list.append(list(row))

    rule_df = pd.DataFrame(data_list, columns=['日期', 'big_rule', 'small_rule', 'user_categroy', 'counts'])
    end_rule_df = pd.pivot_table(rule_df, values='counts', index=['日期'],
                                 columns=['big_rule', 'small_rule', 'user_categroy']).reset_index().fillna(0)
    end_rule_df = end_rule_df.sort_values(by='日期', ascending=0)
    end_rule_df = end_rule_df.fillna(0)

    return end_rule_df


def email_send():

    excel_writer = pd.ExcelWriter('/home/andpay/data/excel/daily_report.xlsx', engine='xlsxwriter')
    work_book = excel_writer.book
    format_1 = work_book.add_format({'align': 'center', 'font_name': '微软雅黑'})
    format_2 = work_book.add_format({'align': 'center', 'num_format': '0.00%', 'font_name': '微软雅黑'})
    format_3 = work_book.add_format({'align': 'center', 'num_format': '0.0000%', 'font_name': '微软雅黑'})

    first_df = get_first_df()
    first_df.to_excel(excel_writer, '业务日报表', index=False)
    first_sheet = excel_writer.sheets['业务日报表']
    first_sheet.set_column('A:Z', 16, format_1)
    first_sheet.set_column('D:D', 16, format_2)
    first_sheet.set_column('F:F', 16, format_2)
    first_sheet.freeze_panes(1, 1)

    loan_after_df = get_loan_after_df()
    loan_after_df.to_excel(excel_writer, '贷后日报表', index=False)
    loan_sheet = excel_writer.sheets['贷后日报表']
    loan_sheet.set_column('A:AD', 16, format_1)
    loan_sheet.set_column('AD:AD', 16, format_3)
    loan_sheet.freeze_panes(1, 1)

    end_audit_df = get_end_audit_df()
    end_audit_df.to_excel(excel_writer, '审批日报表', index=False)
    end_audit_sheet = excel_writer.sheets['审批日报表']
    end_audit_sheet.set_column('A:Z', 16, format_1)
    end_audit_sheet.set_column('E:E', 16, format_2)
    end_audit_sheet.set_column('H:H', 16, format_2)
    end_audit_sheet.freeze_panes(1, 1)

    end_user_categroy_df = get_end_user_categroy_df()
    end_user_categroy_df.to_excel(excel_writer, '新老户数据', index=False)
    end_user_categroy_sheet = excel_writer.sheets['新老户数据']
    end_user_categroy_sheet.set_column('A:AZ', 16, format_1)
    end_user_categroy_sheet.set_column('E:E', 16, format_2)
    end_user_categroy_sheet.set_column('J:J', 16, format_2)
    end_user_categroy_sheet.set_column('L:L', 16, format_2)
    end_user_categroy_sheet.set_column('Q:R', 16, format_2)
    end_user_categroy_sheet.freeze_panes(1, 1)

    end_categroy_table = get_end_categroy_table()
    end_categroy_table.to_excel(excel_writer, '分期数据', index=False)
    end_categroy_sheet = excel_writer.sheets['分期数据']
    end_categroy_sheet.set_column('A:Z', 16, format_1)
    end_categroy_sheet.freeze_panes(1, 1)

    end_rule_df = get_end_rule_df()
    end_rule_df.to_excel(excel_writer, '规则命中')
    end_rule_sheet = excel_writer.sheets['规则命中']
    end_rule_sheet.set_column('A:BA', 14, format_1)
    end_rule_sheet.set_column('B:B', 16, format_1)
    end_rule_sheet.set_column('A:A', None, None, {'hidden': 1})
    end_rule_sheet.freeze_panes(3, 2)

    end_newuser_df=get_end_newuser_df()
    end_newuser_df.to_excel(excel_writer, '新户渠道数据')
    end_newuser_sheet = excel_writer.sheets['新户渠道数据']
    end_newuser_sheet.set_column('A:Z', 16, format_1)
    end_newuser_sheet.set_column('B:B', 16, format_1)
    end_newuser_sheet.set_column('A:A', None, None, {'hidden': 1})
    end_newuser_sheet.freeze_panes(2, 2)

    renew_df=get_end_renew_df()
    renew_df.to_excel(excel_writer, '老户续借数据', index=False)
    renew_sheet = excel_writer.sheets['老户续借数据']
    renew_sheet.set_column('A:G', 16, format_1)
    renew_sheet.set_column('H:N', 16, format_2)
    renew_sheet.freeze_panes(1, 1)

    call_result_df=get_call_result_df()
    call_result_df.to_excel(excel_writer, '用户反馈(未提现+未续借)')
    call_result_sheet = excel_writer.sheets['用户反馈(未提现+未续借)']
    call_result_sheet.set_column('A:A', None, None, {'hidden': 1})
    call_result_sheet.set_column('B:Z', 18, format_1)
    call_result_sheet.freeze_panes(2, 2)

    M1_df=get_m1_df()
    M1_df.to_excel(excel_writer, '新增M1客户明细', index=False)
    M1_sheet = excel_writer.sheets['新增M1客户明细']
    M1_sheet.set_column('A:Z', 18, format_1)
    M1_sheet.freeze_panes(1, 1)

    M2_df=get_m2_df()
    M2_df.to_excel(excel_writer, '新增M2客户明细', index=False)
    M2_sheet = excel_writer.sheets['新增M2客户明细']
    M2_sheet.set_column('A:Z', 18, format_1)
    M2_sheet.freeze_panes(1,1)

    excel_writer.save()

    subject = '信用通业务日报'

    # to_addrs = ['fan.chen@andpay.me', 'youkun.xie@andpay.me', 'xiaofei.xiong@andpay.me', 'feng.feng@andpay.me',
    #             'hao.sun@andpay.me', 'kesheng.wang@andpay.me', 'shenglu.chen@andpay.me', 'stephanie.shao@andpay.me',
    #             'liping.peng@andpay.me', 'sea.bao@andpay.me']


    to_addrs = ['kesheng.wang@andpay.me']
    body_text = 'Daily_Report'
    attachment_file = "/home/andpay/data/excel/daily_report.xlsx"

    EmailSend.send_email(subject, to_addrs, body_text, attachment_files=[attachment_file])
