#Обязательные параметры:
#table_1 - Название таблицы вместе с схемой через точку.              Пример: table_1 = 'custom_risk_tsmr_ext.t_applret_param_dc'
#table_2 - Название таблицы вместе с схемой через точку.              Пример: table_2 = 'custom_risk_tsmr_ext.t_applret_param_dc_proto'
#key - Ключевые поля, через запятую и без алиасов.                    Пример: key = 'appl_id, start_dt, end_dt'
#attributes - Поля которые хотим сравнить. Через запятую без алиасов. Пример: attributes = '''appl_decsn_journal_sid ,end_dt ,agr_cred_sid ,appl_id ,appl_num ,start_dt ,appl_dt ,reqstd_cred_ccy_amt'''

#Важные параметры:
#test_base - Название базы в которой создадутся сверочные таблицы.   Пример: test_base = 'custom_risk_kfl_stg'
#self_mark - Метка для создания уникальных сверочных таблиц.          Пример: self_mark = 'fedn'

#Необязательные параметры:
#where_table_1 - Фильтр для table_1                                   Пример: where_table_1 = '''cast(\'2021-02-16\' as date) between start_dt and end_dt and ctl_action <> \'D\''''
#where_table_2 - Фильтр для table_2                                   Пример: where_table_2 = '''ctl_action <> \'D\''''

import re

def tab(string, spases=4):
	return string.replace('\n', '\n' + (' '*spases))

def selwhere(table = '', where = '1=1'):
	return f'select *\nfrom {table}\nwhere {where}\n'

def gencntcheck(**kw):
    str_pk = tab('\n,'.join(kw['clean_key']), 2)
    or_pk = ' is null\n or '.join(kw['clean_key']) + ' is null'
    return f"""CREATE TABLE {kw['table_cntcheck']} STORED AS PARQUET AS
select
  'count' as check_name
  ,'{kw['table_1']}' as table_name
  ,count(*) as cnt
from {kw['table_1']}
where {kw['where_table_1']}
union all
select
  'count' as check_name
  ,'{kw['table_2']}' as table_name
  ,count(*) as cnt
from {kw['table_2']}
where {kw['where_table_2']}

union all

select
  'null_pk' as check_name
  ,'{kw['table_1']}' as table_name
  ,count(*) as cnt
from {kw['table_1']}
where {kw['where_table_1']}
and ({or_pk})
union all
select
  'null_pk' as check_name
  ,'{kw['table_2']}' as table_name
  ,count(*) as cnt
from {kw['table_2']}
where {kw['where_table_2']}
and ({or_pk})

union all

select
  'doubles_pk' as check_name
  ,'{kw['table_1']}' as table_name
  ,count(*) as cnt
from (
select
  {str_pk}
  ,count(*) as count_doubles
from {kw['table_1']}
where {kw['where_table_1']}
group by\n  {str_pk}
having count(*) > 1
) as table_1
union all
select
  'doubles_pk' as check_name
  ,'{kw['table_2']}' as table_name
  ,count(*) as cnt
from (
select
  {str_pk}
  ,count(*) as count_doubles
from {kw['table_2']}
where {kw['where_table_2']}
group by\n  {str_pk}
having count(*) > 1
) as table_2\n
"""

def gendiffcheck(**kw):
    str_atr = 'table_.' + '\n,table_.'.join(kw['clean_attributes'])

    cmp_diff = f"""select
  '{kw['table_1']}' as table_name
  ,{tab(str_atr.replace('_.', '_1.'), 2)}
from {kw['table_1']} as table_1
where {kw['where_table_1']}

union all

select
  '{kw['table_2']}' as table_name
  ,{tab(str_atr.replace('_.', '_2.'), 2)}
from {kw['table_2']} as table_2
where {kw['where_table_2']}"""

    return f"""CREATE TABLE {kw['test_base']}.gck_{kw['self_mark']}_diffrows STORED AS PARQUET AS
with
un as (
{cmp_diff}
)
select
  table_name
  ,{tab(str_atr, 2)}
from
  (select
    table_name
    ,{tab(str_atr, 4)}
    ,count(*) over(partition by {', '.join(kw['clean_attributes'])}) as cnt
  from un
  ) as prep
where cnt <> 2
order by {', '.join(kw['clean_key'])}
"""

def gencntatrcheck(**kw):
    tmp = []
    for attr in kw['clean_attributes']:
        tmp.append(f'sum (if (table_1.{attr} <=> table_2.{attr}, 0, 1)) as {attr}')
    cmp_atr = '\n,'.join(tmp)

    tmp = []
    for pk in kw['clean_key']:
        tmp.append(f'and table_1.{pk} = table_2.{pk}')
    join_pk = '\n'.join(tmp)

    return f"""
select
  {tab(cmp_atr, 2)}
from
  ({tab(selwhere(table = kw['table_1'], where = kw['where_table_1']), 2)}) as table_1
full outer join
  ({tab(selwhere(table = kw['table_2'], where = kw['where_table_2']), 2)}) as table_2
  on 1=1
  {tab(join_pk, 2)}"""

def gencheck(table_1 = '/*base*/./*table_1*/', table_2 = '/*base*/./*table_2*/', key = '/*key*/', attributes = '/*attributes*/', where_table_1 = '1=1', where_table_2 = '1=1', test_base = 'DEFAULT', self_mark = 'default'):
    table_cntcheck  = f'{test_base}.gck_{self_mark}_countcheck'
    table_diffcheck = f'{test_base}.gck_{self_mark}_diffrows'

    reg = re.compile('[^a-zA-Z0-9_,.]')
    clean_attributes = (reg.sub('', attributes)).split(',')
    clean_key = (reg.sub('', key)).split(',')
    #self_mark without \s
    tmp = ''
    requests = []

    requests.append('/*-----------------------------------/TABLES/-----------------------------------*/\n')

    requests.append(f'DROP TABLE IF EXISTS {table_cntcheck}\n')

    requests.append(gencntcheck(
    	table_1 = table_1,
        table_2 = table_2,
        clean_key = clean_key,
        where_table_1 = where_table_1,
        where_table_2 = where_table_2,
        table_cntcheck = table_cntcheck))

    requests.append(f'DROP TABLE IF EXISTS {test_base}.gck_{self_mark}_diffrows\n')

    requests.append(gendiffcheck(
    	table_1 = table_1,
        table_2 = table_2,
        where_table_1 = where_table_1,
        where_table_2 = where_table_2,
    	test_base = test_base,
        self_mark = self_mark,
        clean_attributes = clean_attributes,
        clean_key = clean_key))

    requests.append('/*-----------------------------------/QUERIES/-----------------------------------*/\n')

    requests.append(f'select * from {table_cntcheck} order by check_name, table_name\n')

    requests.append(f'select * from {table_diffcheck} order by {key}, table_name\n')

    requests.append(f"""
select
 count(*) as CNT_diff_all
 ,count(case when table_name = '{table_1}' then 1 end) as CNT_diff_table_1
 ,count(case when table_name = '{table_2}' then 1 end) as CNT_diff_table_2
from {table_diffcheck}
""")

    requests.append(gencntatrcheck(
    	clean_key = clean_key,
    	clean_attributes = clean_attributes,
        table_1 = table_1,
        table_2 = table_2,
        where_table_1 = where_table_1,
        where_table_2 = where_table_2))

    tmp = ''
    for i in requests:
                   tmp = tmp + i
    return tmp

def main():
                print(gencheck(
                               table_1 = 'custom_risk_np_dflt_stg.v_cust_np_restruct_npv_detail_test',
                               table_2 = 'custom_risk_np_dflt_stg.proto_v2_init_restr_plan_npv_pre_feddl_2',
                               key = 'agr_cred_sid, restruct_dt',
                               attributes = '''
agr_cred_sid
,restruct_dt
,grace_end_dt
,fin_prob_flag
,first_restruct_dt
,no_calc_flag
,key_rate
,before_restruct_plan_pay_sid
,before_restruct_plan_pay_npv_amt
,before_first_key_rate
,before_first_restruct_plan_pay_sid
,before_first_restruct_plan_pay_npv_amt
,after_restruct_calc_npv_amt
,after_restruct_plan_pay_sid
,after_restruct_plan_pay_npv_amt
,dflt_flag
                ''',
                               test_base = 'custom_risk_np_dflt_stg',
                               self_mark = 'fedn_restr',
                               where_table_1 = 'restruct_dt between \'2021-02-01\' and \'2021-04-02\'',
                               where_table_2 = 'restruct_dt between \'2021-02-01\' and \'2021-04-02\''
                               ))

if __name__ == '__main__':
                main()