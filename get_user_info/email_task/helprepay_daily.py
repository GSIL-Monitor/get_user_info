from ti_daf import SqlTemplate,sql_util
import pandas as pd
from datetime import datetime
import datetime as dt
from get_user_info.config import init_app
from get_user_info.data_merge.send_email import EmailSend
import time


def get_basicdata():
    init_app()

    today = datetime.date(datetime.today())
    origin_day = today - dt.timedelta(days=59)

    today_s = "'" + str(today) + "'"
    origin_day_s = "'" + str(origin_day) + "'"

    sql = '''
        select uu.register_count,xx.*,yy.loanamt,yy.new_loanamt,yy.old_loanamt,zz.backamt from 
        ( 
            select date_format(applytime,'%Y-%m-%d') days,count(distinct a.applyinfoid)  apply_num,
            count(distinct case when repaymode=0 then a.applyinfoid end)  help_num,
            count(distinct case when repaymode=1 then a.applyinfoid end)  circle_num,
            count(distinct case when e.deal_status='用户终止撤销' then a.applyinfoid end)  usercancel_num,
            count(distinct case when e.deal_status='首笔失败撤销' then a.applyinfoid end)  firstcancel_num,
            count(distinct case when e.deal_status='用户终止结清' then a.applyinfoid end)  usersettle_num,
            count(distinct case when e.deal_status='中途失败结清' then a.applyinfoid end)  midfailsettle_num,
            count(distinct case when e.deal_status='全额结清'  then a.applyinfoid end)  fullamountsettle_num,
            count(distinct case when e.deal_status='其他' then a.applyinfoid end)  other_num,
            count(distinct case when e.deal_status='逾期中' then a.applyinfoid end )   overdue_num
            from ac_bts_db.ApplyInfo a
            left join ac_bts_db.InsteadRepayTxnCtrl b
            on a.applyinfoid=b.applyinfoid
            left join ac_bts_db.InsteadRepaySchedule c
            on b.insteadrepaytxnctrlid=c.insteadrepaytxnctrlid
            left join ac_lms_db.LoanApplyInfo d
            on c.exttxnid=d.id and c.scheduletype='RT'
            left join
            (
                select a.applyinfoid ids,
                case when  applystatus='O' and json_extract(txndata,'$.userCancelFlag')=TRUE then '用户终止结清'
                     when  applystatus='O' and hasrepayamt<repayamt  then '中途失败结清'
                     when  applystatus='O' then '全额结清'
                     when  applystatus='C' and json_extract(txndata,'$.userCancelFlag')=TRUE then '用户终止撤销'
                     when  applystatus='C' then '首笔失败撤销'
                     when  hasrepayamt>0 and payedamt<hasrepayamt and applystatus<>'O'  then '逾期中'
                     else  '其他' end  deal_status
                from ac_bts_db.ApplyInfo a
                left join ac_bts_db.InsteadRepayTxnCtrl b
                on a.applyinfoid=b.applyinfoid
            ) e
            on e.ids=a.applyinfoid
            where applytime<''' + today_s + '''  and  applytime>=''' + origin_day_s + '''
            group by date_format(applytime,'%Y-%m-%d')
        ) xx
        left join
        (
            select  date_format(exestarttime,'%Y-%m-%d') days,sum(a.amt) loanamt,
            sum(case when TIMESTAMPDIFF(day,c.crttime,a.prestarttime)<=30 then a.amt end) new_loanamt, 
            sum(case when TIMESTAMPDIFF(day,c.crttime,a.prestarttime)>30 then a.amt end) old_loanamt
            from  ac_bts_db.InsteadRepaySchedule a
            left join ac_bts_db.InsteadRepayTxnCtrl b
            on a.insteadrepaytxnctrlid=b.insteadrepaytxnctrlid
            left join ac_cif_db.Party c
            on b.partyid=c.partyid
            where scheduletype='RT' and a.status='S'
            and a.prestarttime<''' + today_s + '''  and  a.prestarttime>=''' + origin_day_s + '''
            group by date_format(exestarttime,'%Y-%m-%d')
        ) yy
        on xx.days=yy.days
        left join
        (
            select  date_format(exestarttime,'%Y-%m-%d') days,sum(a.amt) backamt
            from  ac_bts_db.InsteadRepaySchedule a
            where scheduletype in ('PT','FT') and a.status='S'
            and  exestarttime<''' + today_s + '''  and  exestarttime>=''' + origin_day_s + '''
            group by date_format(exestarttime,'%Y-%m-%d')
        ) zz 
        on xx.days=zz.days
        left join 
        (
            select date_format(crttime,'%Y-%m-%d') days,count(distinct partyid) register_count  from ac_cif_db.Party
            where crttime<''' + today_s + '''  and  crttime>=''' + origin_day_s + '''
            group by date_format(crttime,'%Y-%m-%d')
        ) uu
        on xx.days=uu.days
        '''


    day_row = sql_util.select_rows_by_sql(sql_text=sql, sql_paras={}, ns_server_id='/python/db/ac_bts_db', max_size=-1)

    day_list = []
    for row in day_row:
        day_list.append(list(row))

    day_df = pd.DataFrame(day_list, columns=['register', 'day', 'apply_num', 'help_num', 'circle_num', 'usercancel_num',
                                             'firstcancel_num', 'usersettle_num', 'midfailsettle_num',
                                             'fullamountsettle_num', 'other_num', 'overdue_num', 'loan_amt',
                                             'new_loanamt', 'old_loanamt', 'return_amt'])

    # ---------------------------------------------------------


    # today=datetime.date(datetime.today())
    # origin_day=datetime.date(datetime.strptime('2017-12-21','%Y-%m-%d'))
    # num=(today-origin_day).days

    card_list = []
    for i in range(-1, 60):
        af_day = today - dt.timedelta(days=i)
        bf_day = today - dt.timedelta(days=i + 1)

        af_day_s = "'" + str(af_day) + "'"
        bf_day_s = "'" + str(bf_day) + "'"

        sql_1 = '''
            select  date_format(x.ctime,'%Y-%m-%d'),count(distinct x.mcid),count(distinct x.cid)
            from 
            (
            select   a.crttime ctime,b.merchantCustomerId mcid,c.cardno cid
            from  ac_agw_db.AuthBindCard a
            left join  ac_agw_db.MerchantUser b
            on a.merchantUserId=b.merchantUserId
            left join ac_agw_db.MerchantUserCard c
            on a.merchantusercardid=c.merchantusercardid
            where a.authNetId like '08470010-00%'  and  a.status='1'
            and a.crttime<''' + af_day_s + ''' and  a.crttime>=''' + bf_day_s + '''
            ) x
            left join 
            (
            select  
            distinct c.cardno cid
            from  ac_agw_db.AuthBindCard a
            left join  ac_agw_db.MerchantUser b
            on a.merchantUserId=b.merchantUserId
            left join ac_agw_db.MerchantUserCard c
            on a.merchantusercardid=c.merchantusercardid
            where a.authNetId like '08470010-00%'  and  a.status='1' 
            and a.crttime<''' + bf_day_s + '''
            ) y
            on x.cid=y.cid
            where y.cid is null
            group by date_format(x.ctime,'%Y-%m-%d')
            '''

        card_row = sql_util.select_rows_by_sql(sql_text=sql_1, sql_paras={}, ns_server_id='/python/db/ac_agw_db',
                                               max_size=-1)

        for row in card_row:
            card_list.append(list(row))

    card_df = pd.DataFrame(card_list, columns=['day', 'person_num', 'card_num'])

    end_day_df = pd.merge(card_df, day_df, on='day', how='right')

    end_day_df = end_day_df.sort_values(by='day', ascending=[0])
    col = list(end_day_df.columns)
    col.remove('day')
    end_day_df.loc['row_sum'] = end_day_df[col].apply(lambda x: x.sum(), axis=0)
    end_day_df.loc[end_day_df.index == 'row_sum', 'day'] = '总计'
    pass_rate = end_day_df['help_num'] / end_day_df['apply_num']
    end_day_df.insert(7, 'pass_rate', pass_rate)
    end_day_df.columns = ['日期', '绑卡人数', '绑卡张数', '新增注册人数', '提交笔数', '垫付笔数', '循环笔数', '垫付占比',
                          '用户终止撤销笔数', '首笔失败撤销笔数','用户终止结清笔数',
                          '中途失败结清笔数', '全额结清笔数', '其他笔数', '逾期笔数', '放款金额', '新户放款额', '旧户放款额', '回款金额']

    end_day_df = end_day_df.fillna(0)

    usercancel_proportion = end_day_df['用户终止撤销笔数'] / end_day_df['提交笔数']
    userfail_proportion = end_day_df['首笔失败撤销笔数'] / end_day_df['提交笔数']
    partsettle_proportion = end_day_df['用户终止结清笔数'] / end_day_df['提交笔数']
    midfailsettle_proportion = end_day_df['中途失败结清笔数'] / end_day_df['提交笔数']
    allsettle_proportion = end_day_df['全额结清笔数'] / end_day_df['提交笔数']

    end_day_df.insert(9, '用户终止撤销占比', usercancel_proportion)
    end_day_df.insert(11, '首笔失败撤销占比', userfail_proportion)
    end_day_df.insert(13, '用户终止结清占比', partsettle_proportion)
    end_day_df.insert(15, '中途失败结清占比', midfailsettle_proportion)
    end_day_df.insert(17, '全额结清占比', allsettle_proportion)

    return end_day_df



def get_helpdata():
    init_app()

    today = datetime.date(datetime.today())
    origin_day = today - dt.timedelta(days=59)

    today_s = "'" + str(today) + "'"
    origin_day_s = "'" + str(origin_day) + "'"

    sql = '''
        select xx.*,ww.loanamt,yy.avgamt,round(zz.debittime/zz.complete_num,1) 
        from 
        ( 
            select date_format(applytime,'%Y-%m-%d') days,
            count(distinct case when repaymode=0 then a.applyinfoid end) help_num,
            count(distinct case when repaymode=0 and e.deal_status='用户终止撤销' then a.applyinfoid end) usercancel_num,
            count(distinct case when repaymode=0 and e.deal_status='首笔失败撤销' then a.applyinfoid end) firstcancel_num,
            count(distinct case when repaymode=0 and e.deal_status='用户终止结清' then a.applyinfoid end) usersettle_num,
            count(distinct case when repaymode=0 and e.deal_status='中途失败结清' then a.applyinfoid end) midfailsettle_num,
            count(distinct case when repaymode=0 and e.deal_status='全额结清'  then a.applyinfoid end) fullamountsettle_num,
            count(distinct case when repaymode=0 and e.deal_status='其他' then a.applyinfoid end) other_num
            from ac_bts_db.ApplyInfo a
            left join ac_bts_db.InsteadRepayTxnCtrl b
            on a.applyinfoid=b.applyinfoid
            left join ac_bts_db.InsteadRepaySchedule c
            on b.insteadrepaytxnctrlid=c.insteadrepaytxnctrlid
            left join ac_lms_db.LoanApplyInfo d
            on c.exttxnid=d.id and c.scheduletype='RT'
            left join
            (
                select a.applyinfoid ids,
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
            where applytime<''' + today_s + ''' and applytime>=''' + origin_day_s + '''
            group by date_format(applytime,'%Y-%m-%d')
        ) xx
        left join 
        (   
            select  date_format(exestarttime,'%Y-%m-%d') days,sum(a.amt) loanamt 
            from  ac_bts_db.InsteadRepaySchedule a
            left join ac_bts_db.InsteadRepayTxnCtrl b
            on b.insteadrepaytxnctrlid=a.insteadrepaytxnctrlid
            left join ac_bts_db.ApplyInfo c
            on c.applyinfoid=b.applyinfoid
            where scheduletype='RT' and a.status='S' and c.repaymode=0
            group by date_format(exestarttime,'%Y-%m-%d')
        ) ww
        on xx.days=ww.days
        left join
        (
            select  date_format(applytime,'%Y-%m-%d') days, 
            avg(json_extract(applydata,'$.evaluateOpenAmt')) avgamt
            from ac_bts_db.ApplyInfo  
            where repaymode=0  and json_extract(applydata,'$.evaluateOpenAmt') is not null
            group by date_format(applytime,'%Y-%m-%d')
        )  yy
        on xx.days=yy.days
        left join
        (   
            select date_format(applytime,'%Y-%m-%d') days,
            count(distinct case when b.completetime is not null then a.applyinfoid end) complete_num,
            sum(case when b.completetime is not null then 
                 TIMESTAMPDIFF(MINUTE,a.applytime,b.completetime) end)  debittime
            from ac_bts_db.ApplyInfo a
            left join ac_bts_db.InsteadRepayTxnCtrl b
            on a.applyinfoid=b.applyinfoid
            where repaymode=0
            group by date_format(applytime,'%Y-%m-%d')
        ) zz 
        on xx.days=zz.days
        '''

    help_row = sql_util.select_rows_by_sql(sql_text=sql, sql_paras={}, ns_server_id='/python/db/ac_bts_db', max_size=-1)

    help_list = []
    for row in help_row:
        help_list.append(list(row))

    help_df = pd.DataFrame(help_list, columns=['日期', '垫付笔数', '用户终止撤销笔数', '首笔失败撤销笔数', '用户终止结清笔数',
                                               '中途失败结清笔数', '全额结清笔数', '其他笔数', '垫付交易额', '笔均敞口', '平均扣款时长(分)'])
    help_df = help_df.sort_values(by='日期', ascending=0)
    avgamt = help_df['垫付交易额'] / (help_df['用户终止结清笔数'] + help_df['中途失败结清笔数'] + help_df['全额结清笔数'])
    avgamt = avgamt.apply(lambda x: round(x, 2))
    help_df.insert(9, '垫付笔均交易额', avgamt)
    help_df = help_df.fillna(0)

    usercancel_proportion = help_df['用户终止撤销笔数'] / help_df['垫付笔数']
    userfail_proportion = help_df['首笔失败撤销笔数'] / help_df['垫付笔数']
    partsettle_proportion = help_df['用户终止结清笔数'] / help_df['垫付笔数']
    midfailsettle_proportion = help_df['中途失败结清笔数'] / help_df['垫付笔数']
    allsettle_proportion = help_df['全额结清笔数'] / help_df['垫付笔数']

    help_df.insert(3, '用户终止撤销占比', usercancel_proportion)
    help_df.insert(5, '首笔失败撤销占比', userfail_proportion)
    help_df.insert(7, '用户终止结清占比', partsettle_proportion)
    help_df.insert(9, '中途失败结清占比', midfailsettle_proportion)
    help_df.insert(11, '全额结清占比', allsettle_proportion)

    return help_df


def get_circledata():
    init_app()

    today = datetime.date(datetime.today())
    origin_day = today - dt.timedelta(days=59)

    today_s = "'" + str(today) + "'"
    origin_day_s = "'" + str(origin_day) + "'"

    sql = '''
        select xx.*,ww.loanamt,round(zz.debittime/zz.complete_num,1) 
        from 
        ( 
            select date_format(applytime,'%Y-%m-%d') days,
            count(distinct case when repaymode=1 then a.applyinfoid end)  circle_num,
            count(distinct case when repaymode=1 and e.deal_status='用户终止撤销' then a.applyinfoid end) usercancel_num,
            count(distinct case when repaymode=1 and e.deal_status='首笔失败撤销' then a.applyinfoid end) firstcancel_num,
            count(distinct case when repaymode=1 and e.deal_status='用户终止结清' then a.applyinfoid end) usersettle_num,
            count(distinct case when repaymode=1 and e.deal_status='中途失败结清' then a.applyinfoid end) midfailsettle_num,
            count(distinct case when repaymode=1 and e.deal_status='全额结清'  then a.applyinfoid end) fullamountsettle_num,
            count(distinct case when repaymode=1 and e.deal_status='其他' then a.applyinfoid end) other_num
            from ac_bts_db.ApplyInfo a
            left join ac_bts_db.InsteadRepayTxnCtrl b
            on a.applyinfoid=b.applyinfoid
            left join ac_bts_db.InsteadRepaySchedule c
            on b.insteadrepaytxnctrlid=c.insteadrepaytxnctrlid
            left join ac_lms_db.LoanApplyInfo d
            on c.exttxnid=d.id and c.scheduletype='RT'
            left join
            (
                select a.applyinfoid ids,
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
            where applytime<''' + today_s + ''' and applytime>=''' + origin_day_s + '''
            group by date_format(applytime,'%Y-%m-%d')
        ) xx
        left join 
        (   
            select  date_format(exestarttime,'%Y-%m-%d') days,sum(a.amt) loanamt 
            from  ac_bts_db.InsteadRepaySchedule a
            left join ac_bts_db.InsteadRepayTxnCtrl b
            on b.insteadrepaytxnctrlid=a.insteadrepaytxnctrlid
            left join ac_bts_db.ApplyInfo c
            on c.applyinfoid=b.applyinfoid
            where scheduletype='RT' and a.status='S' and c.repaymode=1
            group by date_format(exestarttime,'%Y-%m-%d')
        ) ww
        on xx.days=ww.days
        left join
        (   
            select date_format(applytime,'%Y-%m-%d') days,
            count(distinct case when b.completetime is not null then a.applyinfoid end) complete_num,
            sum(case when b.completetime is not null then 
                 TIMESTAMPDIFF(MINUTE,a.applytime,b.completetime) end)  debittime
            from ac_bts_db.ApplyInfo a
            left join ac_bts_db.InsteadRepayTxnCtrl b
            on a.applyinfoid=b.applyinfoid
            where repaymode=1
            group by date_format(applytime,'%Y-%m-%d')
        ) zz 
        on xx.days=zz.days
        '''

    circle_row = sql_util.select_rows_by_sql(sql_text=sql, sql_paras={}, ns_server_id='/python/db/ac_bts_db',
                                             max_size=-1)

    circle_list = []
    for row in circle_row:
        circle_list.append(list(row))

    circle_df = pd.DataFrame(circle_list, columns=['日期', '循环笔数', '用户终止撤销笔数', '首笔失败撤销笔数', '用户终止结清笔数',
                                                   '中途失败结清笔数', '全额结清笔数', '其他笔数', '循环交易额', '平均扣款时长(分)'])

    circle_df = circle_df.sort_values(by='日期', ascending=0)
    circle_df = circle_df[circle_df['日期'] >= '2018-01-15']
    avgamt = circle_df['循环交易额'] / (circle_df['用户终止结清笔数'] + circle_df['中途失败结清笔数'] + circle_df['全额结清笔数'])
    avgamt = avgamt.apply(lambda x: round(x, 2))
    circle_df.insert(9, '循环笔均金额', avgamt)
    circle_df = circle_df.fillna(0)

    usercancel_proportion = circle_df['用户终止撤销笔数'] / circle_df['循环笔数']
    userfail_proportion = circle_df['首笔失败撤销笔数'] / circle_df['循环笔数']
    partsettle_proportion = circle_df['用户终止结清笔数'] / circle_df['循环笔数']
    midfailsettle_proportion = circle_df['中途失败结清笔数'] / circle_df['循环笔数']
    allsettle_proportion = circle_df['全额结清笔数'] / circle_df['循环笔数']

    circle_df.insert(3, '用户终止撤销占比', usercancel_proportion)
    circle_df.insert(5, '首笔失败撤销占比', userfail_proportion)
    circle_df.insert(7, '用户终止结清占比', partsettle_proportion)
    circle_df.insert(9, '中途失败结清占比', midfailsettle_proportion)
    circle_df.insert(11, '全额结清占比', allsettle_proportion)

    return circle_df


def get_loanafterdata():
    init_app()

    today = datetime.date(datetime.today())
    origin_day = today - dt.timedelta(days=59)

    today_s = "'" + str(today) + "'"
    origin_day_s = "'" + str(origin_day) + "'"

    sql = '''
        select xx.*
        from 
        ( 
            select date_format(applytime,'%Y-%m-%d') days,
            count(distinct case when repaymode=0 and b.hasrepayamt>0 then a.applyinfoid end) deal_num,
            count(distinct case when repaymode=0 and b.hasrepayamt>0 
            and (a.applystatus='O' or hasrepayamt-payedamt<0 ) then a.applyinfoid end) repaied_num,
            sum(case when repaymode=0 and b.hasrepayamt>0 then hasrepayamt end) deal_amt,
            sum(case when repaymode=0 and b.hasrepayamt>0 
            and (a.applystatus='O' or hasrepayamt-payedamt<0 ) then hasrepayamt end) repaied_amt,
            count(distinct case when repaymode=0 and hasrepayamt>0 
            and payedamt<hasrepayamt and applystatus<>'O' then a.applyinfoid end ) nopay_num,
            sum(case when repaymode=0 and hasrepayamt>0 and payedamt<hasrepayamt 
            and applystatus<>'O' then hasrepayamt-payedamt end) nopay_amt,
            count(distinct case when repaymode=0 and b.status in ('L','LS') then a.applyinfoid end)  turnloan_num,
            count(distinct case when repaymode=0 and b.status in ('LS') then a.applyinfoid end) loanback_num,
            sum(case when repaymode=0 and b.status in ('L','LS') then hasrepayamt-payedamt  end) turnloan_amt,
            sum(case when repaymode=0 and b.status in ('LS') then hasrepayamt-payedamt  end) loanback_amt,
            count(distinct case when repaymode=0 and b.status in ('L') then a.applyinfoid end) nowloan_num,
            sum(case when repaymode=0 and b.status in ('L') then hasrepayamt-payedamt  end) nowloan_amt
            from ac_bts_db.ApplyInfo a
            left join ac_bts_db.InsteadRepayTxnCtrl b
            on a.applyinfoid=b.applyinfoid
            where applytime<''' + today_s + ''' and applytime>=''' + origin_day_s + '''
            group by date_format(applytime,'%Y-%m-%d')

        ) xx
        '''

    afterloan_row = sql_util.select_rows_by_sql(sql_text=sql, sql_paras={}, ns_server_id='/python/db/ac_bts_db',
                                                max_size=-1)

    afterloan_list = []
    for row in afterloan_row:
        afterloan_list.append(list(row))

    afterloan_df = pd.DataFrame(afterloan_list, columns=['日期', '应还笔数', '已还笔数', '应还金额', '已还金额', '未还笔数', '未还金额',
                                                         '转贷笔数', '转贷催回笔数', '转贷金额', '转贷催回金额', '当前逾期笔数',
                                                         '当前逾期金额'])
    afterloan_df = afterloan_df.sort_values(by='日期', ascending=0)
    afterloan_df = afterloan_df.fillna(0)
    afterloan_df = afterloan_df[afterloan_df['日期'] >= '2018-01-15']

    return afterloan_df



def get_funneldata():
    init_app()

    today = datetime.date(datetime.today())
    today_s = "'" + str(today) + "'"

    yesterday = today - dt.timedelta(days=1)
    yesterday_s = "'" + str(yesterday) + "'"

    sql = '''
        select  
        case when sysdate-registertime<30 and HASCLEARREPAY=0  then  'new_u' else 'old_u' end cate,
        count(distinct case when hasloginapp=1 then x.partyid end )  allcount,
        count(distinct case when hasloginapp=1 and ENTRYREPAYHOME=1 then x.partyid end)  enterhome,
        count(distinct case when BINDCARDONCE=1   and hasloginapp=1 
              and ENTRYREPAYHOME=1 then x.partyid end)  hasbindcard,
        count(distinct case when BINDCARDNOW=1 and hasloginapp=1 
              and ENTRYREPAYHOME=1 then x.partyid end)  nowbindcard,
        count(distinct case when CLICKAPPLYONAPP=1  and BINDCARDONCE=1  
        and hasloginapp=1 and ENTRYREPAYHOME=1 and BINDCARDONCE=1 
        and lastbindcardtime>=to_date(''' + yesterday_s + ''','yyyy-mm-dd') 
        and lastbindcardtime<to_date(''' + today_s + ''','yyyy-mm-dd')  then x.partyid end )  hitapply,
        count(distinct case when HASAPPLYREPAY=1 and CLICKAPPLYONAPP=1  
        and BINDCARDONCE=1 and hasloginapp=1 and ENTRYREPAYHOME=1 and BINDCARDONCE=1 
        and lastapplyrepaytime>=to_date(''' + yesterday_s + ''','yyyy-mm-dd') 
        and lastapplyrepaytime<to_date(''' + today_s + ''','yyyy-mm-dd')   then x.partyid end ) hasapply,
        count(distinct case when HASCLEARREPAY=1 and HASAPPLYREPAY=1 
        and CLICKAPPLYONAPP=1 and BINDCARDONCE=1  and hasloginapp=1 and ENTRYREPAYHOME=1  and BINDCARDONCE=1 
         and lastclearrepaytime>=to_date(''' + yesterday_s + ''','yyyy-mm-dd') 
         and lastclearrepaytime<to_date(''' + today_s + ''','yyyy-mm-dd')  then x.partyid end )  settle
        from  dev_dw.f_repaytags x 
        where lastlogintime<''' + today_s + ''' and lastlogintime>=''' + yesterday_s + '''
        group by  case when sysdate-registertime<30 and HASCLEARREPAY=0  then  'new_u' else 'old_u' end '''

    card_row = sql_util.select_rows_by_sql(sql_text=sql, sql_paras={}, ns_server_id='/python/db/dev_dw_db', max_size=-1)

    card_list = []
    for row in card_row:
        card_list.append(list(row))

    card_df = pd.DataFrame(card_list, columns=['类别', '登入过app', '进入帮还首页', '绑过银行卡', '当前有绑卡', '点击申请按钮', '有申请记录', '有结清记录'])

    new_u = card_df[card_df['类别'] == 'new_u'].ix[:, 1:].values[0]
    old_u = card_df[card_df['类别'] == 'old_u'].ix[:, 1:].values[0]

    # num_list=list(card_row[0])
    # print(num_list)

    name_list = ['登入过app', '进入帮还首页', '绑过银行卡', '当前有绑卡', '点击申请按钮', '有申请记录', '有结清记录']

    sum_num_new = new_u[0]
    sum_num_old = old_u[0]

    result_list = []
    for i in range(len(name_list)):

        cate = name_list[i]
        num_new = new_u[i]
        num_old = old_u[i]
        proportion_new = new_u[i] / sum_num_new
        proportion_old = old_u[i] / sum_num_old

        if i == 0:
            propor_descend_new = propor_descend_old = 1
        else:
            propor_descend_new = new_u[i] / new_u[i - 1]
            propor_descend_old = old_u[i] / old_u[i - 1]

        result_list.append(['新商户', cate, num_new, propor_descend_new, proportion_new])
        result_list.append(['旧商户', cate, num_old, propor_descend_old, proportion_old])

    funnel_df = pd.DataFrame(result_list, columns=['商户类型', '过程', '人数', '上一步转化率', '整体转化率'])
    funnel_df = funnel_df.sort_values(by='商户类型', ascending=0)

    return funnel_df


def get_checkdetail():
    init_app()

    today = datetime.date(datetime.today())
    org_day = today - dt.timedelta(days=1)

    today_s = "'" + str(today) + "'"
    org_day_s = "'" + str(org_day) + "'"

    sql = ''' select a.applytime,a.applyinfoid,a.partyid,
        case when a.applystatus='T' then '申请提交成功' 
             when a.applystatus='M' then '人工审核状态'
             when a.applystatus='S' then '申请审核通过'
             when a.applystatus='R' then '审核拒绝(最终状态)'
             when a.applystatus='C' then '申请被撤销(最终状态)'
             when a.applystatus='O' then '代还交易完成(最终状态)' end applystatus,
        json_extract(a.applydata,'$.issuerName'),a.applyamt,
        case when c.scheduletype='FT' then '手续费交易'
             when c.scheduletype='RT' then '还款交易'
             when c.scheduletype='PT' then '扣款交易' end trade_status,
        case when c.status='W' then '等待执行'
             when c.status='S' then '执行成功'
             when c.status='F' then '执行失败' end  repaystatus,
        c.amt,
        case when b.completetime is null then '未完成'
             else TIMESTAMPDIFF(MINUTE,a.applytime,b.completetime) end  alltime
        from ac_bts_db.ApplyInfo a
        left join ac_bts_db.InsteadRepayTxnCtrl b
        on a.applyinfoid=b.applyinfoid
        left join ac_bts_db.InsteadRepaySchedule c
        on b.insteadrepaytxnctrlid=c.insteadrepaytxnctrlid
        left join 
        (
         select insteadrepaytxnctrlid,exestarttime
         from ac_bts_db.InsteadRepaySchedule
         where serialno=1 
        ) f 
        on c.insteadrepaytxnctrlid=f.insteadrepaytxnctrlid
        where a.applystatus not in ('R','C') and a.repaymode=0
        and a.applytime>=''' + org_day_s + ''' and a.applytime<''' + today_s + '''
        '''

    sql_row = sql_util.select_rows_by_sql(sql_text=sql, sql_paras={}, ns_server_id='/python/db/ac_bts_db', max_size=-1)

    data_list = []
    for row in sql_row:
        data_list.append(list(row))

    repay_df = pd.DataFrame(data_list, columns=['申请时间', '申请id', 'partyid', '状态', '银行卡', '申请额度', '操作类型', '操作状态', '操作金额',
                                                '代扣用时(分)'])

    repay_df = repay_df.sort_values(by='申请时间', ascending=[0])

    return repay_df




def get_failreason():
    init_app()

    today = datetime.date(datetime.today())
    org_day = today - dt.timedelta(days=1)

    today_s = "'" + str(today) + "'"
    org_day_s = "'" + str(org_day) + "'"

    sql = '''
        select date_format(a.applytime,'%Y-%m-%d'),
        json_extract(applyData,'$.issuerName'),
        errorcode,errormsg,
        count(distinct case when a.applystatus<>'O' and c.status='F' then a.applyinfoid end),
        count(distinct a.applyinfoid)
        from ac_bts_db.ApplyInfo a
        left join ac_bts_db.InsteadRepayTxnCtrl b
        on a.applyinfoid=b.applyinfoid
        left join ac_bts_db.InsteadRepaySchedule c
        on b.insteadrepaytxnctrlid=c.insteadrepaytxnctrlid
        where  c.scheduletype='PT' 
        and a.applytime<date_format(''' + today_s + ''','%Y-%m-%d')
        group by date_format(a.applytime,'%Y-%m-%d'),
        json_extract(applyData,'$.issuerName'),
        errorcode,errormsg
        '''

    sql_row = sql_util.select_rows_by_sql(sql_text=sql, sql_paras={}, ns_server_id='/python/db/ac_bts_db', max_size=-1)

    fail_list = []
    for row in sql_row:
        fail_list.append(list(row))

    fail_df = pd.DataFrame(fail_list, columns=['日期', '银行', '失败code', '失败原因', '失败笔数', '总笔数'])
    fail_df = fail_df.sort_values(by=['日期', '失败code', '失败笔数'], ascending=[0, 1, 0])
    fail_df = fail_df[(fail_df['失败笔数'] != 0) & (fail_df['失败原因'].notnull())]

    return fail_df


def excel_format(excel_writer, dataframe, sheet_name, col_wide=18,
                 freeze_row=1, freeze_col=1, format_dict={}):
    work_book = excel_writer.book
    # 这里可自行添加excel的设置格式
    format_1 = work_book.add_format({'align': 'center', 'font_name': '微软雅黑'})
    format_2 = work_book.add_format({'align': 'center', 'num_format': '0.0%', 'font_name': '微软雅黑'})
    format_3 = work_book.add_format({'align': 'center', 'num_format': '0.0000%', 'font_name': '微软雅黑'})

    dataframe.to_excel(excel_writer, sheet_name, index=False)
    excel_sheet = excel_writer.sheets[sheet_name]

    for key in format_dict.keys():
        if key == 'format_1':
            for col in format_dict[key]:
                excel_sheet.set_column(col, col_wide, format_1)
        elif key == 'format_2':
            for col in format_dict[key]:
                excel_sheet.set_column(col, col_wide, format_2)
        elif key == 'format_3':
            for col in format_dict[key]:
                excel_sheet.set_column(col, col_wide, format_3)

    excel_sheet.freeze_panes(freeze_row, freeze_col)

    return excel_writer


def email_task():

    end_day_df=get_basicdata()
    help_df=get_helpdata()
    circle_df=get_circledata()
    afterloan_df=get_loanafterdata()
    funnel_df=get_funneldata()
    repay_df=get_checkdetail()
    fail_df=get_failreason()


    name_list = ['业务概览', '垫付交易表', '循环交易表', '贷后日报表', '帮还每日漏斗', '通过详情', '失败原因统计']
    format1 = {'format_1': ['A:G', 'I:I', 'K:K', 'M:M', 'O:O', 'Q:Q', 'S:Z'],
               'format_2': ['H:H', 'J:J', 'L:L', 'N:N', 'P:P', 'R:R']}
    format2 = {'format_2': ['D:D', 'F:F', 'H:H', 'J:J', 'L:L'], 'format_1': ['A:C', 'E:E', 'G:G', 'I:I', 'K:K', 'M:Z']}
    format3 = {'format_2': ['D:D', 'F:F', 'H:H', 'J:J', 'L:L'], 'format_1': ['A:C', 'E:E', 'G:G', 'I:I', 'K:K', 'M:Z']}
    format4 = {'format_1': ['A:Z']}
    format5 = {'format_1': ['A:C', 'F:Z'], 'format_2': ['D:E']}
    format6 = {'format_1': ['A:Z']}
    format7 = {'format_1': ['A:Z']}
    format_list = [format1, format2, format3, format4, format5, format6, format7]
    df_list = [end_day_df, help_df, circle_df, afterloan_df, funnel_df, repay_df, fail_df]

    excel_writer = pd.ExcelWriter('/home/andpay/data/excel/helprepay_report.xlsx', engine='xlsxwriter')
    for sheet_name, format, df in zip(name_list, format_list, df_list):
        excel_writer = excel_format(excel_writer, df, sheet_name=sheet_name, format_dict=format)

    excel_writer.save()

    subject = '帮还数据报表'
    '''
    to_addrs = ['shenglu.chen@andpay.me', 'feng.feng@andpay.me', 'youkun.xie@andpay.me', 'liancun.bai@andpay.me',
                'qianqian.miao@andpay.me', 'yiao.chen@andpay.me', 'liping.peng@andpay.me', 'hao.sun@andpay.me',
                'shuihan.yi@andpay.me', 'puzhou.cen@andpay.me', 'zhongfeng.zhou@andpay.me', 'stephanie.shao@andpay.me',
                'sarah.qin@andpay.me', 'nikki.gao@andpay.me',
                'lishan.ma@andpay.me', 'jeff.xiao@andpay.me', 'tony.jin@andpay.me', 'sea.bao@andpay.me']
    '''
    to_addrs = ['kesheng.wang@andpay.me']
    body_text = 'Helprepay_Report'
    attachment_file = "/home/andpay/data/excel/helprepay_report.xlsx"

    EmailSend.send_email(subject, to_addrs, body_text, attachment_files=[attachment_file])