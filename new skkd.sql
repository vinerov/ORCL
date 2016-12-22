  CREATE TABLE "COMMON"."DIM_SKKD_MAIN" 
   (	"CHECK_ID" NUMBER(*,0) NOT NULL ENABLE, 
	"CHECK_DESCRIPT" VARCHAR2(4000), 
	"SUBJ" VARCHAR2(4000), 
	"ERROR_MAILS" VARCHAR2(4000), 
	"NO_ERROR_MAILS" VARCHAR2(4000), 
	"JOB_START_TIME" DATE, 
	"IS_STARTING_IN_JOB" NUMBER(*,0), 
	"IS_ACTIVE" NUMBER(*,0), 
	"PARAM_PROCESS" VARCHAR2(100), 
	"QUORUM_LINK_IND" NUMBER, 
	"BODY_MESSAGE" VARCHAR2(4000), 
	"PRE_SQL" VARCHAR2(4000), 
	"POST_SQL" VARCHAR2(4000), 
	"DEVELOPER" VARCHAR2(100) DEFAULT SYS_CONTEXT ('USERENV', 'OS_USER')
   ) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING
  STORAGE(INITIAL 1048576 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "STORAGE_DATA" 
 ;



  CREATE TABLE "COMMON"."DIM_SKKD_PARTS" 
   (	"CHECK_ID" NUMBER(*,0), 
	"PART_ID" NUMBER(*,0), 
	"VW_OWNER" VARCHAR2(30), 
	"VW_NAME" VARCHAR2(30), 
	"BODY_MESSAGE" VARCHAR2(4000), 
	"GOOD_ANSWER" VARCHAR2(1000) DEFAULT 'Ошибок не обнаружено', 
	"BAD_ANSWER" VARCHAR2(1000) DEFAULT 'Ошибки обнаружены', 
	"IS_ACTIVE" NUMBER DEFAULT 0
   ) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING
  STORAGE(INITIAL 1048576 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "STORAGE_DATA" 
 
;


  CREATE TABLE "COMMON"."DIM_SKKD_PARAMS" 
   (	"CHECK_ID" NUMBER, 
	"PARAM_NAME" VARCHAR2(50), 
	"PARAM_VALUE" VARCHAR2(1024), 
	"PARAM_ACTIVE" NUMBER
   ) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING
  STORAGE(INITIAL 1048576 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  TABLESPACE "STORAGE_DATA" 
;



  CREATE OR REPLACE FUNCTION "COMMON"."FN_VIEW_TO_HTML_TABLE" (p_owner varchar2,
                                                 p_view  varchar2,
                                                 p_param number)
  return varchar2 as
  v_td_th varchar2(5); 
  v_str varchar2(1000);
begin
  
  if p_param = 0 then --для строк <th>ЗАГОЛОВОК1</th><th>ЗАГОЛОВОК2</th>..
    v_td_th := 'th';
  elsif p_param = 1 then --....
    v_td_th := 'td';
  elsif p_param = 2 then
    v_td_th := 'zz';
  end if;
  with dat as
   (select '<th>' || trim(nvl(com.COMMENTS,col.COLUMN_NAME)) || '</th>' th,
           case
             when data_type = 'VARCHAR2' then '''<td>''||Z.' || col.COLUMN_NAME || '||''</td>'''
             when data_type = 'NUMBER' then '''<td align = right>''||rtrim(to_char(Z.' || col.COLUMN_NAME || ',''fm9999999990d999''),'',.'')||''</td>'''
             when data_type = 'DATE' then '''<td>''||to_char(Z.' || col.COLUMN_NAME || ', ''dd.mm.yyyy'')||''</td>'''
             else '''<td>''||to_char(Z.' || col.COLUMN_NAME || ')||''</td>'''
           end td,
           col.COLUMN_NAME ZZ, 
           col.column_id
      from ALL_TAB_COLUMNS col
      left join ALL_COl_COMMENTS com
        on col.OWNER = com.OWNER
       and col.TABLE_NAME = com.TABLE_NAME
       and col.COLUMN_NAME = com.COLUMN_NAME
     where col.table_name = UPPER(p_view)
       and col.owner = UPPER(p_owner)
     order by col.COLUMN_ID),
  lg as
   (Select th,
           lag(th) over(order by COLUMN_ID) as prev_th,
           td,
           lag(td) over(order by COLUMN_ID) as prev_td,
           zz,
           lag(zz) over(order by COLUMN_ID) as prev_zz
      from dat),
  fin as
   (select ltrim(sys_connect_by_path(th, '@'), '@') as th_list,
           ltrim(sys_connect_by_path(td, '@'), '@') as td_list,
           ltrim(sys_connect_by_path(zz, ','), ',') as zz_list,
           level lvl
      from lg
     start with prev_td is null
    connect by prev_td = prior td),
  tab as
   (select '<tr>' ||
           replace(max(th_list) keep(dense_rank last order by lvl), '@', '') ||
           '</tr>' t_list,
           'TH' td_th
      from fin
    union all
    select '<tr>''||' ||
           replace(max(td_list) keep(dense_rank last order by lvl),
                   '@',
                   '||') || '||''</tr>' t_list,
           'TD'
      from fin
    union all
    select max(zz_list) keep(dense_rank last order by lvl) t_list,
           'ZZ'
      from fin)
  Select t_list into v_str from tab where td_th = UPPER(v_td_th);
  return v_str;
end;

 




  CREATE OR REPLACE PROCEDURE "COMMON"."SKKD_RUN" (p_check_id in number, p_anon in number default 0) as
  v_str      varchar2(32000);
  v_check_id number := p_check_id;
  v_log_id   number;
  v_html     clob;
  v_imp      number := 0; --при необходимости можно тоже сделать рекордсетом,
                          --например, если нужно разные адреса добавлять для разных part-ов
  v_Pre_Sql  varchar2(4000);
  v_Post_Sql varchar2(4000);
  TYPE ItemRec IS RECORD(item_str varchar2(4000));
  TYPE ItemSet IS TABLE OF ItemRec;
  dup_items ItemSet;

begin
  --записываем стиль таблички
  v_log_id := common.pckg_skkd_log.starts_load(v_check_id);
  dbms_lob.createtemporary(v_html, true);
  dbms_lob.open(v_html, dbms_lob.lob_readwrite);
  v_str := '<html><head><style type="text/css">
     TABLE {background: DimGray;  color: black;  /*Цвет текста*/ }
        TH {background: #B0C4DE; /* Цвет фона шапки */}
        TD {background: white; /* Цвет фона ячеек */}
   </style></head><body> <p> Это автоматическое сообщение. Пожалуйста, не отвечайте на него. <br>';

  --записываем общий текст из dim_skkd_main
  dbms_lob.writeappend(v_html, dbms_lob.getlength(v_str), v_str);
  Select '<b>'|| Body_Message || '</b><br><br>', Pre_Sql, Post_Sql
    into v_str, v_Pre_Sql, v_Post_Sql
    from Common.DIM_SKKD_MAIN
   where Check_Id = v_check_id;
  dbms_lob.writeappend(v_html, dbms_lob.getlength(v_str), v_str);
  
  --PRE_SQL 
  if v_Pre_Sql is not null then 
    execute immediate v_Pre_Sql;
  end if;
  
  --пробегаем по частям одной проверки
  for cur in (Select p.check_id, p.part_id, p.vw_owner, p.vw_name, p.body_message, p.good_answer, p.bad_answer
                from common.dim_skkd_parts p
               where p.check_id = v_check_id
               and p.is_active = 1
               order by part_id) loop
    --записываем частный текст из dim_skkd_parts
    if cur.body_message is not null then 
      v_str := '<br>'||cur.part_id ||') '|| cur.body_message ||'<br>';
      dbms_lob.writeappend(v_html, dbms_lob.getlength(v_str), v_str);
    end if;

    execute immediate 'select ''' || common.fn_view_to_html_table(cur.vw_owner, cur.vw_name, 1) ||
                      ''' from '||cur.vw_owner||'.'||cur.vw_name||' z' BULK COLLECT
      INTO dup_items;
    --проверяем есть ли ошибки
    if dup_items.COUNT > 0 then
      v_imp := 1;
      v_str := '<font color = "red">'||cur.bad_answer||'</font><table border=1 cellspacing=0 bordercolor=DimGray cellpadding=6 style=''Font-size: 12pt;''>' ||
               fn_view_to_html_table(cur.vw_owner, cur.vw_name, 0);
      dbms_lob.writeappend(v_html, dbms_lob.getlength(v_str), v_str);
      for i in dup_items.FIRST .. dup_items.LAST LOOP
        v_str := dup_items(i).item_str;
        dbms_lob.writeappend(v_html, dbms_lob.getlength(v_str), v_str);
      END LOOP;
      v_str := '</table>';
      dbms_lob.writeappend(v_html, dbms_lob.getlength(v_str), v_str);
    else
      v_str := '<font color = "green">'||cur.good_answer||'</font><br>';
      dbms_lob.writeappend(v_html, dbms_lob.getlength(v_str), v_str);
    end if;
  end loop;
  
  --POST_SQL 
  if v_Post_Sql is not null then 
    execute immediate v_Post_Sql;
  end if;
  
  v_str := '</body></html>';
  dbms_lob.writeappend(v_html, dbms_lob.getlength(v_str), v_str);
  
  
  delete from storage.mail_file_blob;
  for send in (Select case 
                        when p_anon = 1 then sys_context('userenv', 'os_user')||'@mkb.ru'
                        when v_imp = 0 then no_error_mails 
                        else error_mails
                      end v_email, 
                      subj
                 from common.dim_skkd_main
                where check_id = v_check_id) loop
    if send.v_email is not null then 
      storage.send_smtp_file_html(s_compress     => false,
                                  s_from         => 'SKKD <skkd@skkd.ru>',
                                  s_sender_email => 'Salmanovia@skkd.ru',
                                  s_recipient    => send.v_email,
                                  s_subject      => 'СККД ' || v_check_id || '(ST): ' || send.subj,
                                  s_message      => v_html,
                                  s_importance   => v_imp);
    end if;
  end loop;
  common.pckg_skkd_log.fin_load(v_log_id, '', v_html);
  commit;
EXCEPTION
  when others then
    common.pckg_skkd_log.fin_err(v_log_id, sqlerrm);
END;

 
 