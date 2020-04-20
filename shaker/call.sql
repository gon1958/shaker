-- Пример запуска обфускатора данных
set trimspool on
set trimout on
spool on
spool gen.log
set serveroutput on size unlimited;
set linesize 256
-- загружаем программу
@shaker
show error

declare
	test_mode pls_integer := 0;
	cnt_parallel pls_integer := 2;
begin
--инициализируем
  shaker.before_schema('IBS');
  
-- перемешиваем
-- физиков
  shaker.exec_schema('Z#CLIENT',  'ID,C_NAME,C_I_NAME', cnt_parallel, 5000, test_mode, 'class_id=''CL_PRIV''');
  shaker.exec_schema('Z#CL_PRIV', 'ID,C_FAMILY_CL,C_NAME_CL,C_SNAME_CL', cnt_parallel, 5000, test_mode);
-- юриков
  shaker.exec_schema('Z#CLIENT',  'ID,C_NAME,C_I_NAME', cnt_parallel, 5000, test_mode, 'class_id=''CL_ORG''');
  shaker.exec_schema('Z#CL_CORP', 'ID,C_LONG_NAME,C_INFO_ADDR', cnt_parallel, 5000, test_mode, 'class_id=''CL_ORG''');
  shaker.exec_schema('Z#CL_ORG',  'ID', cnt_parallel, 5000, test_mode);
-- названия счетов
  shaker.exec_schema('Z#ACCOUNT',  'C_NAME', cnt_parallel, 50000, test_mode);
-- collection адресов
  shaker.exec_schema('Z#PERSONAL_ADDRESS',  'COLLECTION_ID', cnt_parallel, 5000, test_mode);
-- collection удостоверений
  shaker.exec_schema('Z#CERTIFICATE',  'COLLECTION_ID', cnt_parallel, 5000, test_mode);

-- восстанавливаем индексы и подключаем триггеры и все остальное
  shaker.after_schema(cnt_parallel, test_mode);
end;      
/
-- удаляем программы
drop package shaker;
spool off
exit

