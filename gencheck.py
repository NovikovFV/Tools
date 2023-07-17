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
def gencheck(table_1 = '/*base*/./*table_1*/', table_2 = '/*base*/./*table_2*/', key = '/*key*/', attributes = '/*attributes*/', where_table_1 = '1=1', where_table_2 = '1=1', test_base = 'DEFAULT', self_mark = 'default'):
                start_query = 'spark.sql("""\n'
                end_query = '""").show()\n\n'

                attributes = attributes.replace('\n', '')
                key =  key.replace('\n', '')
                where_table_1 = where_table_1.replace('\n', '\n  ') #!!! where atr_1 = '\n'
                where_table_2 = where_table_2.replace('\n', '\n  ') #!!! where atr_1 = '\n'
                #self_mark without \s
                tmp = ''
                requests = []

                or_pk = ''
                for pk in key.split(','):
                               or_pk = or_pk + '  or ' + pk.strip() + ' is null\n'

                or_not_atr = ''
                for atr in attributes.split(','):
                               or_not_atr = or_not_atr + '  or not table_1.' + atr.strip() + ' <=> table_2.' + atr.strip() + '\n'

                join_pk = ''
                for pk_attr in key.split(','):
                               join_pk = join_pk + '  and ' + 'table_1.'+ pk_attr.strip() + ' = ' + 'table_2.' + pk_attr.strip() + '\n'

                str_pk = ''
                for pk in key.split(','):
                               str_pk = str_pk + '  ' +  pk.strip() + ',\n'
                str_pk = str_pk.rstrip(',\n')

                str_atr_1 = ''
                for atr in attributes.split(','):
                               str_atr_1 = str_atr_1 + '  table_1.' + atr.strip() + ',\n'
                str_atr_1 = str_atr_1.rstrip(',\n')

                str_atr_2 = ''
                for atr in attributes.split(','):
                               str_atr_2 = str_atr_2 + '  table_2.' + atr.strip() + ',\n'
                str_atr_2 = str_atr_2.rstrip(',\n')

                cmp_atr = ''
                for attr in attributes.split(','):
                               cmp_atr = cmp_atr + '  sum (if (table_1.' + attr.strip() + ' <=> table_2.' + attr.strip() + ', 0, 1)) as ' + attr.strip() + ',\n'
                cmp_atr = cmp_atr.rstrip(',\n') + '\n'

                cmp_diff = (
                  'select\n'
                + '  \'' + table_1 + '\'' + ' as NAME,\n'
                + str_atr_1 + '\n'
                + 'from ' + table_1 + ' as table_1' + '\n'
                + 'where ' + where_table_1 + '\n'
                + '\nunion all\n\n'
                + 'select\n'
                + '  \'' + table_2 + '\'' + ' as NAME,\n'
                + str_atr_2 + '\n'
                + 'from ' + table_2 + ' as table_2' + '\n'
                + 'where ' + where_table_2 + '\n')

                requests.append('/*-----------------------------------/TABLES/-----------------------------------*/\n')

                requests.append('spark.sql("""\n' + 'DROP TABLE IF EXISTS ' + test_base + '.gck_' + self_mark + '_countcheck' + '\n' + '""")\n\n')

                requests.append(
                                 'spark.sql("""\n'
                               + 'CREATE TABLE ' + test_base + '.gck_' + self_mark + '_countcheck STORED AS PARQUET AS\n'
                               + 'select\n'
                               + '  \'count\' as CHECK,\n'
                               + '  \'' + table_1 + '\'' + ' as NAME,\n'
                               + '  count(*) as CNT\n'
                               + 'from ' + table_1 + '\n'
                               + 'where ' + where_table_1 + '\n'
                               + '\nunion all\n\n'
                               + 'select\n'
                               + '  \'count\' as CHECK,\n'
                               + '  \'' + table_2 + '\'' + ' as NAME,\n'
                               + '  count(*) as CNT\n'
                               + 'from ' + table_2 + '\n'
                               + 'where ' + where_table_2 + '\n'
                               + '\nunion all\n\n'
                               + 'select\n'
                               + '  \'null_pk\' as CHECK,\n'
                               + '  \'' + table_1 + '\'' + ' as NAME,\n'
                               + '  count(*) as CNT\n'
                               + 'from ' + table_1 + '\n'
                               + 'where ' + where_table_1 + '\n'
                               + 'and (1=0\n'
                               + or_pk
                               + ')\n'
                               + '\nunion all\n\n'
                               + 'select\n'
                               + '  \'null_pk\' as CHECK,\n'
                               + '  \'' + table_2 + '\'' + ' as NAME,\n'
                               + '  count(*) as CNT\n'
                               + 'from ' + table_2 + '\n'
                               + 'where ' + where_table_2 + '\n'
                               + 'and (1=0\n'
                               + or_pk
                               + ')\n'
                               + '\nunion all\n\n'
                               + 'select\n'
                               + '  \'doubles_pk\' as CHECK,\n'
                               + '  \'' + table_1 + '\'' + ' as NAME,\n'
                               + '  count(*) as CNT\n'
                               + 'from (\n'
                               + 'select\n'
                               + str_pk + ',\n'
                               + 'count(*) as COUNT_DOUBLES\n'
                               + 'from ' + table_1 + '\n'
                               + 'where ' + where_table_1 + '\n'
                               + 'group by\n'
                               + str_pk + '\n'
                               + 'having count(*) > 1\n'
                               + ') as table_1\n'
                               + '\nunion all\n\n'
                               + 'select\n'
                               + '  \'doubles_pk\' as CHECK,\n'
                                + '  \'' + table_2 + '\'' + ' as NAME,\n'
                               + '  count(*) as CNT\n'
                               + 'from (\n'
                               + 'select\n'
                               + str_pk + ',\n'
                               + 'count(*) as COUNT_DOUBLES\n'
                               + 'from ' + table_2 + '\n'
                               + 'where ' + where_table_2 + '\n'
                               + 'group by\n'
                               + str_pk + '\n'
                               + 'having count(*) > 1\n'
                               + ') as table_2\n'
                               + '""")\n\n')

                requests.append('spark.sql("""\n' + 'DROP TABLE IF EXISTS ' + test_base + '.gck_' + self_mark + '_diffrows' + '\n' + '""")\n\n')

                requests.append(
                                 'spark.sql("""\n'
                               + 'CREATE TABLE ' + test_base + '.gck_' + self_mark + '_diffrows STORED AS PARQUET AS\n'
                               + 'with\n'
                               + 'un as (\n'
                               + cmp_diff
                               + ')\n\n'
        + 'select\n'
        + '  NAME,\n'
        + ' ' + ',\n  '.join(attributes.split(',')) + '\n'
        + 'from\n'
        + '  (select\n'
        + '    NAME,\n'
        + '   ' + ',\n    '.join(attributes.split(',')) + ',\n'
        + '    count(*) over(partition by ' + attributes.strip().replace(',', ', ') + ') as cnt\n'
        + '  from un\n'
        + '  ) as prep\n'
        + 'where cnt <> 2\n'
        + 'order by ' + str_pk.strip().replace('\n ', '') + '\n'
        + '""")\n\n')

                requests.append('/*-----------------------------------/QUERIES/-----------------------------------*/\n')

                requests.append(
                               'spark.sql("""select * from ' + test_base + '.gck_' + self_mark + '_countcheck order by CHECK, NAME' + '""").show(false)\n')

                requests.append(
                               'spark.sql("""select * from ' + test_base + '.gck_' + self_mark + '_diffrows order by ' + str_pk.strip().replace('\n ', '') + ', NAME""").show(false)\n\n')
                requests.append(
                               'spark.sql("""\n'
                               + 'select \n'
                               + '  count(*) as CNT_diff_gck_' + self_mark + '_diffrow' + ',\n'
                               + '  count(case when NAME = \'' + table_1 +'\' then 1 end) as CNT_diff_table_1' + ',\n'
                               + '  count(case when NAME = \'' + table_2 +'\' then 1 end) as CNT_diff_table_2' + '\n'
                               + 'from ' + test_base + '.gck_' + self_mark + '_diffrows \n'
                               + '""").show(false)\n\n')

                requests.append(
                                 'spark.sql("""\n'
                               + 'select\n'
                               + cmp_atr
                               + 'from\n'
                               + '  (select *\n'
                               + '  from ' + table_1 + '\n'
                               + '  where ' + where_table_1 + '\n'
                               + '  ) as table_1\n'
                               + 'full outer join\n'
                               + '  (select *\n'
                               + '  from ' + table_2 + '\n'
                               + '  where ' + where_table_2 + '\n'
                               + '  ) as table_2\n'
                               + '  on 1=1\n'
                               + join_pk
                               + '""").show(false)\n\n')

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