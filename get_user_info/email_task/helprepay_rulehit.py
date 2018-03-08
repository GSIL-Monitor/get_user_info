from ti_daf import SqlTemplate,sql_util
import pandas as pd
from datetime import datetime
import datetime as dt
from get_user_info.config import init_app
from get_user_info.data_merge.send_email import EmailSend


def get_allrule_hit():
    init_app()

    today = datetime.date(datetime.today())
    bf_day = today - dt.timedelta(days=10)
    yesterday = today - dt.timedelta(days=1)

    today_s = "'" + str(today) + "'"
    bf_day_s = "'" + str(bf_day) + "'"

    sql = '''
        select day,conclusion,
        case when rulename='B_ZMF_R001_RC' and y.keys is not null then 'B_ZMF_R001_RC_01'    
             when rulename='B_ZMF_R001_RC' and z.keys is not null then 'B_ZMF_R001_RC_02' 
             else rulename end rulenames,count(distinct x.keys)
        from
        (
          select a.partyid pid,b.businesskey keys,to_char(startexectime,'yyyy-mm-dd') day,conclusion,
          substr(rulefullfuncname,instr(rulefullfuncname,'.',1,8)+1,
          length(rulefullfuncname)-instr(rulefullfuncname,'.',1,8)) rulename
          from  dev_dw.f_loanapplyinfo a
          join dev_dw.f_ruletaskexeclog b
          on to_char(a.id)=b.businesskey
          where applytype='repayCredit' 
          and b.startexectime>=to_date(''' + bf_day_s + ''','yyyy-mm-dd') 
          and b.startexectime<to_date(''' + today_s + ''','yyyy-mm-dd')
          and conclusion in ('A','D')  and ruledatainjson not like '%trialRun%'
          and substr(rulefullfuncname,instr(rulefullfuncname,'.',1,8)+1,
          length(rulefullfuncname)-instr(rulefullfuncname,'.',1,8)) 
          not like 'flow%'
        ) x
        left join 
        (
          select distinct businesskey keys from  dev_dw.f_ruletaskexeclog
          where json_value(ruledatainjson,'$.checkPcrDataResult')='A'
        ) y
        on x.keys=y.keys
        left join
        (
          select distinct businesskey keys from  dev_dw.f_ruletaskexeclog
          where json_value(ruledatainjson,'$.checkPcrDataResult')='D'
        ) z
        on x.keys=z.keys
        group by day,conclusion,
        case when rulename='B_ZMF_R001_RC' and y.keys is not null then 'B_ZMF_R001_RC_01'    
             when rulename='B_ZMF_R001_RC' and z.keys is not null then 'B_ZMF_R001_RC_02' 
             else  rulename  end 
         '''

    rule_row = sql_util.select_rows_by_sql(sql_text=sql, sql_paras={}, ns_server_id='/db/oracle/dev_dw_db', max_size=-1)

    rule_list = []
    for row in rule_row:
        rule_list.append(list(row))

    rule_df = pd.DataFrame(rule_list, columns=['day', 'conclusion', 'rule_code', 'hit_num'])
    # rule_df=rule_df.sort_values(by=['day','conclusion','hit_num'],ascending=[0,0,0])
    rule_df = pd.pivot_table(rule_df, index=['rule_code', 'conclusion'], columns='day',
                             values='hit_num').reset_index().fillna(0)

    sql_rule = '''select rule_name,rule_code from helprepay_rule '''

    # db= SqlContext('/python/db/scratch')
    map_row = sql_util.select_rows_by_sql(sql_text=sql_rule, sql_paras={}, ns_server_id='/db/oracle/scratch_db',
                                          max_size=-1)

    map_list = []
    for row in map_row:
        map_list.append(list(row))

    map_df = pd.DataFrame(map_list, columns=['rule_name', 'rule_code'])

    rule_df = pd.merge(map_df, rule_df, on='rule_code', how='right')
    # rule_df=pd.pivot_table(rule_df,index='day',columns='rule_code',values='hit_num').reset_index().fillna(0)


    rule_df1 = rule_df[rule_df['conclusion'] == 'D']
    rule_df2 = rule_df[rule_df['conclusion'] == 'A']
    # rule_df1=pd.pivot_table(rule_df1,index=['rule_code','conclusion'],columns='day',values='hit_num').reset_index().fillna(0)
    rule_df1 = rule_df1.sort_values(by=str(yesterday), ascending=0)
    # rule_df2=pd.pivot_table(rule_df2,index=['rule_code','conclusion'],columns='day',values='hit_num').reset_index().fillna(0)
    rule_df2 = rule_df2.sort_values(by=str(yesterday), ascending=0)

    rule_df = pd.concat([rule_df1, rule_df2], axis=0)

    return rule_df


def get_siglerule_hit():
    init_app()

    today = datetime.date(datetime.today())
    bf_day = today - dt.timedelta(days=10)
    yesterday = today - dt.timedelta(days=1)

    today_s = "'" + str(today) + "'"
    bf_day_s = "'" + str(bf_day) + "'"

    sql = '''
        select day,conclusion,
           case when rulename='B_ZMF_R001_RC' and p.keys is not null then 'B_ZMF_R001_RC_01'    
                when rulename='B_ZMF_R001_RC' and q.keys is not null then 'B_ZMF_R001_RC_02' 
                else rulename end rulename,count(distinct x.bskey)
        from
        (
            select a.partyid pid,b.businesskey bskey,to_char(startexectime,'yyyy-mm-dd')  day,conclusion,
            json_value(ruledatainjson,'$.checkPcrDataResult') checkrule,
            substr(rulefullfuncname,instr(rulefullfuncname,'.',1,8)+1,
            length(rulefullfuncname)-instr(rulefullfuncname,'.',1,8)) rulename
            from  dev_dw.f_loanapplyinfo a
            join dev_dw.f_ruletaskexeclog b
            on to_char(a.id)=b.businesskey
            where applytype='repayCredit' and b.startexectime>=to_date(''' + bf_day_s + ''','yyyy-mm-dd') 
            and b.startexectime<to_date(''' + today_s + ''','yyyy-mm-dd')
            and conclusion in ('D')  and ruledatainjson not like '%trialRun%'
            and substr(rulefullfuncname,instr(rulefullfuncname,'.',1,8)+1,
            length(rulefullfuncname)-instr(rulefullfuncname,'.',1,8)) not like 'flow%'
        ) x
        left join 
        (
            select  businesskey,
            count(distinct substr(rulefullfuncname,instr(rulefullfuncname,'.',1,8)+1,
            length(rulefullfuncname)-instr(rulefullfuncname,'.',1,8))) num
            from  dev_dw.f_loanapplyinfo a
            join dev_dw.f_ruletaskexeclog b
            on to_char(a.id)=b.businesskey
            where  applytype='repayCredit' and startexectime>=to_date(''' + bf_day_s + ''','yyyy-mm-dd') 
            and startexectime<to_date(''' + today_s + ''','yyyy-mm-dd')
            and conclusion in ('D')  and ruledatainjson not like '%trialRun%'
            and substr(rulefullfuncname,instr(rulefullfuncname,'.',1,8)+1,
            length(rulefullfuncname)-instr(rulefullfuncname,'.',1,8)) not like 'flow%'
            group by businesskey 
        ) y
        on x.bskey=y.businesskey
        left join 
        (
          select distinct businesskey keys from  dev_dw.f_ruletaskexeclog
          where json_value(ruledatainjson,'$.checkPcrDataResult')='A'
        ) p
        on x.bskey=p.keys
        left join
        (
          select distinct businesskey keys from  dev_dw.f_ruletaskexeclog
          where json_value(ruledatainjson,'$.checkPcrDataResult')='D'
        ) q
        on x.bskey=q.keys
        where y.num=1
        group by day,conclusion,
        case when rulename='B_ZMF_R001_RC' and p.keys is not null then 'B_ZMF_R001_RC_01'    
             when rulename='B_ZMF_R001_RC' and q.keys is not null then 'B_ZMF_R001_RC_02' 
             else rulename end

        '''

    sigle_rule_row = sql_util.select_rows_by_sql(sql_text=sql, sql_paras={}, ns_server_id='/db/oracle/dev_dw_db',
                                                 max_size=-1)

    rule_list = []
    for row in sigle_rule_row:
        rule_list.append(list(row))

    single_rule_df = pd.DataFrame(rule_list, columns=['day', 'conclusion', 'rule_code', 'refuse_person'])

    single_rule_df = pd.pivot_table(single_rule_df, index=['rule_code', 'conclusion'], columns='day',
                                    values='refuse_person').reset_index().fillna(0)

    sql_rule = '''select rule_name,rule_code from helprepay_rule '''

    # db= SqlContext('/python/db/scratch')
    map_row = sql_util.select_rows_by_sql(sql_text=sql_rule, sql_paras={}, ns_server_id='/db/oracle/scratch_db',
                                          max_size=-1)

    map_list = []
    for row in map_row:
        map_list.append(list(row))

    map_df = pd.DataFrame(map_list, columns=['rule_name', 'rule_code'])

    single_rule_df = pd.merge(map_df, single_rule_df, on='rule_code', how='right')
    single_rule_df = single_rule_df.sort_values(by=str(yesterday), ascending=0)

    return single_rule_df


def get_tryrule_hit():
    init_app()

    today = datetime.date(datetime.today())
    bf_day = today - dt.timedelta(days=10)
    yesterday = today - dt.timedelta(days=1)

    today_s = "'" + str(today) + "'"
    bf_day_s = "'" + str(bf_day) + "'"

    sql = '''
        select day,conclusion,
        case when rulename='B_ZMF_R001_RC' and checkrule='A' then 'B_ZMF_R001_RC_01'    
             when rulename='B_ZMF_R001_RC' and checkrule='D' then 'B_ZMF_R001_RC_02' 
             else rulename end rulename,count(distinct keys )
        from
        (
            select a.partyid pid,b.businesskey keys,to_char(startexectime,'yyyy-mm-dd') day,
            conclusion,json_value(ruledatainjson,'$.checkRulesDataExec_RC') checkrule,
            substr(rulefullfuncname,instr(rulefullfuncname,'.',1,8)+1,
            length(rulefullfuncname)-instr(rulefullfuncname,'.',1,8)) rulename
            from  dev_dw.f_loanapplyinfo a
            join dev_dw.f_ruletaskexeclog b
            on to_char(a.id)=b.businesskey
            where applytype='repayCredit' and b.startexectime>=to_date(''' + bf_day_s + ''','yyyy-mm-dd') 
            and b.startexectime<to_date(''' + today_s + ''','yyyy-mm-dd')
            and conclusion in ('D')  and ruledatainjson  like '%trialRun%'
            and substr(rulefullfuncname,instr(rulefullfuncname,'.',1,8)+1,
            length(rulefullfuncname)-instr(rulefullfuncname,'.',1,8)) not like 'flow%'
        )
        group by day,conclusion,
        case when rulename='B_ZMF_R001_RC' and checkrule='A' then 'B_ZMF_R001_RC_01'    
             when rulename='B_ZMF_R001_RC' and checkrule='D' then 'B_ZMF_R001_RC_02' 
             else rulename end
        '''

    try_rule_row = sql_util.select_rows_by_sql(sql_text=sql, sql_paras={}, ns_server_id='/db/oracle/dev_dw_db',
                                               max_size=-1)

    try_rule_list = []
    for row in try_rule_row:
        try_rule_list.append(list(row))

    try_rule_df = pd.DataFrame(try_rule_list, columns=['day', 'conclusion', 'rule_code', 'hit_num'])
    try_rule_df = pd.pivot_table(try_rule_df, index=['rule_code', 'conclusion'], columns='day',
                                 values='hit_num').reset_index().fillna(0)

    sql_rule = '''select rule_name,rule_code from helprepay_rule '''

    # db= SqlContext('/python/db/scratch')
    map_row = sql_util.select_rows_by_sql(sql_text=sql_rule, sql_paras={}, ns_server_id='/db/oracle/scratch_db',
                                          max_size=-1)

    map_list = []
    for row in map_row:
        map_list.append(list(row))

    map_df = pd.DataFrame(map_list, columns=['rule_name', 'rule_code'])
    try_rule_df = pd.merge(map_df, try_rule_df, on='rule_code', how='right')
    try_rule_df = try_rule_df.sort_values(by=str(yesterday), ascending=0)

    return try_rule_df


def get_rulecategroy():
    init_app()

    today = datetime.date(datetime.today())
    bf_day = today - dt.timedelta(days=10)
    yesterday = today - dt.timedelta(days=1)

    today_s = "'" + str(today) + "'"
    bf_day_s = "'" + str(bf_day) + "'"

    sql = '''
        select x.*,y.zmpass,y.zmrefuse,y.pcrpass,y.pcrrefuse  from
        (
            select to_char(applydate,'yyyy-mm-dd') days, count(distinct b.businesskey) all_count,
            count(distinct case when conclusion='A' then b.businesskey end ) pass,
            count(distinct case when conclusion='D' then b.businesskey end ) refuse
            from  dev_dw.f_loanapplyinfo a
            join dev_dw.f_ruletaskexeclog b
            on to_char(a.id)=b.businesskey
            where a.applytype='repayCredit'
            and rulefullfuncname like '%genFinalDecision_RC%'
            and applydate>=to_date(''' + bf_day_s + ''','yyyy-mm-dd') 
            and applydate<to_date(''' + today_s + ''','yyyy-mm-dd')
            group by to_char(applydate,'yyyy-mm-dd')
        ) x
        left join 
        (
            select to_char(applydate,'yyyy-mm-dd') days,
            count(distinct case when conclusion='A' and json_value(ruledatainjson,'$.checkPcrDataResult')='D' then b.businesskey end ) zmpass,
            count(distinct case when conclusion='D' and json_value(ruledatainjson,'$.checkPcrDataResult')='D' then b.businesskey end ) zmrefuse,
            count(distinct case when conclusion='A' and json_value(ruledatainjson,'$.checkPcrDataResult')='A' then b.businesskey end ) pcrpass,
            count(distinct case when conclusion='D' and json_value(ruledatainjson,'$.checkPcrDataResult')='A' then b.businesskey end ) pcrrefuse
            from  dev_dw.f_loanapplyinfo a
            join dev_dw.f_ruletaskexeclog b
            on to_char(a.id)=b.businesskey
            where a.applytype='repayCredit'
            and rulefullfuncname like '%checkRulesDataExec_RC%'
            and applydate>=to_date(''' + bf_day_s + ''','yyyy-mm-dd') and applydate<to_date(''' + today_s + ''','yyyy-mm-dd')
            group by to_char(applydate,'yyyy-mm-dd')
        ) y
        on x.days=y.days
        '''

    categroy_row = sql_util.select_rows_by_sql(sql_text=sql, sql_paras={}, ns_server_id='/db/oracle/dev_dw_db',
                                               max_size=-1)

    categroy_list = []
    for row in categroy_row:
        categroy_list.append(list(row))

    rulecategroy_df = pd.DataFrame(categroy_list,
                                   columns=['day', 'applytimes', 'passtimes', 'refusetimes', 'zmpass', 'zmrefuse',
                                            'pcrpass', 'pcrrefuse'])
    pass_rate = rulecategroy_df['passtimes'] / rulecategroy_df['applytimes']
    zmpass_rate = rulecategroy_df['zmpass'] / (rulecategroy_df['zmpass'] + rulecategroy_df['zmrefuse'])
    pcrpass_rate = rulecategroy_df['pcrpass'] / (rulecategroy_df['pcrpass'] + rulecategroy_df['pcrrefuse'])
    zmproportion = (rulecategroy_df['zmpass'] + rulecategroy_df['zmrefuse']) / rulecategroy_df['applytimes']
    pcrproportion = (rulecategroy_df['pcrpass'] + rulecategroy_df['pcrrefuse']) / rulecategroy_df['applytimes']
    zmall = rulecategroy_df['zmpass'] + rulecategroy_df['zmrefuse']
    pcrall = rulecategroy_df['pcrpass'] + rulecategroy_df['pcrrefuse']

    rulecategroy_df.insert(4, 'pass_rate', pass_rate)
    rulecategroy_df.insert(5, 'zmall', zmall)
    rulecategroy_df.insert(6, 'zmproportion', zmproportion)
    rulecategroy_df.insert(7, 'pcrall', pcrall)
    rulecategroy_df.insert(8, 'pcrproportion', pcrproportion)
    rulecategroy_df.insert(11, 'zmpass_rate', zmpass_rate)
    rulecategroy_df.insert(14, 'pcrpass_rate', pcrpass_rate)
    rulecategroy_df = rulecategroy_df.fillna(0)

    rulecategroy_df.columns = ['日期', '运行笔数', '通过笔数', '拒绝笔数', '通过率', '芝麻运行笔数', '芝麻占比', '人行运行笔数',
                               '人行占比', '芝麻通过笔数','芝麻拒绝笔数', '芝麻通过率', '人行通过笔数', '人行拒绝笔数', '人行通过率']

    return rulecategroy_df


def email_task():

    rule_df=get_allrule_hit()
    single_rule_df=get_siglerule_hit()
    try_rule_df=get_tryrule_hit()
    rulecategroy_df=get_rulecategroy()

    excel_writer = pd.ExcelWriter('/home/andpay/data/excel/helprepay_rulehit.xlsx', engine='xlsxwriter')
    work_book = excel_writer.book
    format_1 = work_book.add_format({'align': 'center', 'font_name': '微软雅黑'})
    format_2 = work_book.add_format({'align': 'center', 'num_format': '0.00%', 'font_name': '微软雅黑'})
    format_3 = work_book.add_format({'align': 'center', 'num_format': '0.0000%', 'font_name': '微软雅黑'})

    rule_df.to_excel(excel_writer, '规则命中分布', index=False)
    rule_sheet = excel_writer.sheets['规则命中分布']
    rule_sheet.set_column('A:Z', 16, format_1)
    rule_sheet.freeze_panes(1, 1)

    single_rule_df.to_excel(excel_writer, '规则单独命中', index=False)
    single_rule_sheet = excel_writer.sheets['规则单独命中']
    single_rule_sheet.set_column('A:Z', 16, format_1)
    single_rule_sheet.freeze_panes(1, 1)

    try_rule_df.to_excel(excel_writer, '试运行规则命中', index=False)
    try_rule_sheet = excel_writer.sheets['试运行规则命中']
    try_rule_sheet.set_column('A:Z', 16, format_1)
    try_rule_sheet.freeze_panes(1, 1)

    rulecategroy_df.to_excel(excel_writer, '规则分类运行数据', index=False)
    rulecategroy_sheet = excel_writer.sheets['规则分类运行数据']
    rulecategroy_sheet.set_column('A:Z', 16, format_1)
    rulecategroy_sheet.set_column('E:E', 16, format_2)
    rulecategroy_sheet.set_column('G:G', 16, format_2)
    rulecategroy_sheet.set_column('I:I', 16, format_2)
    rulecategroy_sheet.set_column('L:L', 16, format_2)
    rulecategroy_sheet.set_column('O:O', 16, format_2)
    rulecategroy_sheet.freeze_panes(1, 1)

    excel_writer.save()

    subject = '帮还风控数据'
    to_addrs = ['kesheng.wang@andpay.me', 'yiao.chen@andpay.me', 'hao.sun@andpay.me', 'sea.bao@andpay.me']
    #['kesheng.wang@andpay.me', 'yiao.chen@andpay.me', 'hao.sun@andpay.me', 'sea.bao@andpay.me']
    body_text = 'helprepay_rulehit'
    attachment_file = "/home/andpay/data/excel/helprepay_rulehit.xlsx"

    EmailSend.send_email(subject, to_addrs, body_text, attachment_files=[attachment_file])
