/* Пакет для обфускации (перемешивания) данных IBSO

Необходимые права:
выполнение dbms_parallel_execute (обычно имеет public доступ)

Запускается от имени владельца схемы.

Параметры запуска
p_user - схема
p_tablename - таблица в которой необходимо выполнить перемешивание данных
p_columns - поля для перемешивания, разделитель запятая ('FIELD1, FIELD2, ...')
p_hmjobs - количество пареллельных процессов (джобов)
p_hmrows - количество одновременно обрабатываемых записей
p_test - только вывод скриптов, без выполнения (не 0)
p_where - дополнительное условие огрпничивающее подмножество для перемешивания

Результат работы контролируется по представлениям USER_PARALLEL_EXECUTE_TASKS, USER_PARALLEL_EXECUTE_CHUNKS
Если имеются chunks в status PROCESSED_WITH_ERROR и ERROR_MESSAGE - User-Defined Exception - 
это означает, что в выборку попала всего одна строка и выполнить перемешивание данных невозможно.


Алгоритм выполнения
1. Для таблицы <p_user>.<p_tablename>  отключаются все триггеры
2. Отключаются индексы таблицы <p_user>.<p_tablename> содержащие столбцы из списка <p_columns>
3. Отключаются foreign key на primary key таблицы <p_user>.<p_tablename>
4. Отключаются constraint таблицы таблицы <p_user>.<p_tablename>
5. Создается p_hmjobs заданий для параллельного выполнения. Задание считывает p_hmrows строк таблицы и
	для каждого столбца из списка p_columns выполняет обмен значениями с выбираемой случайным образом строкой.
5.1. Каждому заданию для обработки передается интервал [id_min, id_max] содержащий 
	p_hmrows строк таблицы. Операция повторяется пока не будет исчерпаны все строки таблицы.
6. Выполняется перестройка индексов в многопоточном режиме. Количество потоков p_hmjobs.
7. Подключаются constraint
8. Подключаются триггеры.

*/
create or replace package shaker is
	procedure before_schema(p_user in varchar2);
	procedure exec_schema (p_tablename in varchar2, p_columns in varchar2,
		p_hmjobs in pls_integer, p_hmrows in pls_integer,
		p_test in pls_integer default 0, p_where in varchar2 default '');
	procedure after_schema(p_hmjobs in pls_integer, p_test in pls_integer default 0);
end shaker;
/
------------------------------------------------------------------------------------------
create or replace package body shaker is
	v_user varchar2(60);
	type varchar_table  is table of varchar2(2000);
	CRLF varchar2(10) := CHR(13) || CHR(10);
	proc_name varchar2(30) := 'SHAKER_';
	v_indexes varchar_table := varchar_table();
	v_triggers varchar_table := varchar_table();
	v_primary varchar_table := varchar_table();
	v_foreign varchar_table := varchar_table();
	v_call_count pls_integer := 0;
------------------------------------------------------------------------------------------
procedure rebuild_indexes (p_hmjobs in pls_integer, p_test in pls_integer default 0);
                             
procedure disable_indexes(v_task_name varchar2, a_columns varchar_table,
	p_tablename in varchar2, p_test in pls_integer default 0);
    
function create_sql_text (p_tablename in varchar2, a_columns varchar_table, 
	p_where in varchar2 default '') return varchar2;
    
procedure disable_triggers(p_tablename varchar2, p_test in pls_integer default 0);
	
procedure enable_triggers(p_test in pls_integer default 0);
------------------------------------------------------------------------------------------
procedure before_schema(p_user in varchar2) is 
begin
	v_user := p_user;
end before_schema;
------------------------------------------------------------------------------------------
procedure  exec_schema (p_tablename in varchar2, p_columns in varchar2,
	p_hmjobs in pls_integer, p_hmrows in pls_integer, p_test in pls_integer default 0, p_where in varchar2 default '')
is
	v_task_name varchar2(30);
	v_sql varchar2(3000);
	a_columns varchar_table;
	user_empty exception;
	v_count integer;
	v_sql_chunk varchar2(3000);
begin
	if v_user is null then 
		raise user_empty;
	end if;
	
	v_sql_chunk := 'select count(*) from ' || v_user || '.' || p_tablename;
	if p_where is not null then 
		v_sql_chunk := v_sql_chunk || ' where ' || p_where;
	end if;
	execute immediate v_sql_chunk into v_count;
	
	v_count := ceil(v_count / p_hmrows);
	v_sql_chunk := 'select min(id), max(id) from ' ||
			'(select id, ntile(' || to_char(v_count) || ') over (order by id) as num ' || 
			'from ' || v_user || '.' || p_tablename;
	if p_where is not null then 
		v_sql_chunk := v_sql_chunk || ' where ' || p_where;
	end if;
	v_sql_chunk := v_sql_chunk || ') group by num';
	
	execute immediate 'alter session set skip_unusable_indexes=true';
	v_call_count := v_call_count + 1;
	v_task_name := proc_name || p_tablename || '_' || v_call_count;
--разбор полей в массив a_columns
	select TRIM(REGEXP_SUBSTR(p_columns, '[^,]+', 1, level)) val bulk collect
		into a_columns
	from dual
		connect by REGEXP_SUBSTR(p_columns, '[^,]+', 1, level) is not null;
--выборка индексов по полям перемешивания в массив v_indexes
	disable_indexes(v_task_name, a_columns, p_tablename, p_test);
--отключение триггеров
	disable_triggers(p_tablename, p_test);
--подготовка скрипта для передачи в dbms_parallel_execute
	v_sql := create_sql_text(p_tablename, a_columns, p_where);
--запуск задачи на параллельную обработку
	if p_test = 0 then
		dbms_parallel_execute.create_task(v_task_name);
		dbms_parallel_execute.create_chunks_by_sql(v_task_name, v_sql_chunk, false);
		dbms_parallel_execute.run_task(v_task_name, v_sql, DBMS_SQL.NATIVE, parallel_level => p_hmjobs);
    	else
		dbms_output.put_line(v_sql_chunk || '/' || CRLF);
		dbms_output.put_line(v_sql || '/' || CRLF);
    end if;
exception
WHEN user_empty then 
	dbms_output.put_line('User name empty. Call before_schema procedure.');	
WHEN OTHERS THEN
	dbms_output.put_line(proc_name||' ERROR : Unhandled: ' || SQLCODE || '-' || SQLERRM);
	dbms_output.put_line(dbms_utility.format_error_backtrace);
end exec_schema;
------------------------------------------------------------------------------------------
procedure disable_foreign(p_primary varchar2, p_test in pls_integer default 0) is
	lv_foreign varchar_table;

	procedure add_foreign(p_foreign varchar2) is
		entry pls_integer;
		v_sql varchar2(3000);
	begin 
		entry := 0;
		for ind in 1..v_foreign.count loop
			if v_foreign(ind) = p_foreign then 
				entry := ind;
				exit;
			end if;
		end loop;
		if entry = 0 then 
			v_foreign.extend(1);
			v_foreign(v_foreign.last) := p_foreign;
			v_sql := 'alter table ' || substr(p_foreign, 1, instr(p_foreign, '.') - 1) || 
				' disable constraint ' || substr(p_foreign, instr(p_foreign, '.') + 1);
			if p_test = 0 then
				execute immediate v_sql;
			else
				dbms_output.put_line(v_sql || ';' || CRLF);
			end if;
		end if;
	end;

begin
	select table_name || '.' || constraint_name bulk collect into lv_foreign from user_constraints 
		where constraint_type = 'R' and r_constraint_name = p_primary;
	for ind in 1..lv_foreign.count loop
		add_foreign(lv_foreign(ind));
	end loop;
end disable_foreign;
------------------------------------------------------------------------------------------
procedure disable_primary(p_tablename varchar2, p_indexname varchar2, p_test in pls_integer default 0) is
	lv_primary varchar_table;

	procedure add_primary(p_primary varchar2) is
		entry pls_integer;
		v_sql varchar2(3000);
	begin 
		entry := 0;
		for ind in 1..v_primary.count loop
			if v_primary(ind) = p_tablename || '.' || p_primary then 
				entry := ind;
				exit;
			end if;
		end loop;
		if entry = 0 then 
			disable_foreign(p_primary, p_test);
			v_primary.extend(1);
			v_primary(v_primary.last) := p_tablename || '.' || p_primary;
			v_sql := 'alter table ' || p_tablename || ' disable constraint ' || p_primary;
			if p_test = 0 then
				execute immediate v_sql;
			else
				dbms_output.put_line(v_sql || ';' || CRLF);
			end if;
		end if;
	end;

begin
	select p_indexname bulk collect into lv_primary from user_constraints 
		where constraint_type in ('P', 'U') and table_name = p_tablename 
		and constraint_name = p_indexname;
	for ind in 1..lv_primary.count loop
		add_primary(lv_primary(ind));
	end loop;
end disable_primary;
------------------------------------------------------------------------------------------
procedure disable_triggers(p_tablename varchar2, p_test in pls_integer default 0) is
	lv_triggers varchar_table;

	procedure add_trigger(p_trigger varchar2) is
		entry pls_integer;
		v_sql varchar2(3000);
	begin 
		entry := 0;
		for ind in 1..v_triggers.count loop
			if v_triggers(ind) = p_trigger then 
				entry := ind;
				exit;
			end if;
		end loop;
		if entry = 0 then 
			v_triggers.extend(1);
			v_triggers(v_triggers.last) := p_trigger;
			v_sql := 'alter trigger ' || p_trigger || ' disable';
			if p_test = 0 then
				execute immediate v_sql;
			else
				dbms_output.put_line(v_sql || ';' || CRLF);
			end if;
		end if;
	end;

begin
	select owner||'.'||trigger_name bulk collect into lv_triggers from all_triggers 
		where table_owner = v_user and table_name = p_tablename ;
	for ind in 1..lv_triggers.count loop
		add_trigger(lv_triggers(ind));
	end loop;
end disable_triggers;
------------------------------------------------------------------------------------------
procedure enable_triggers(p_test in pls_integer default 0) is
	v_sql varchar2(3000);
begin
	for ind in 1..v_triggers.count loop
		v_sql := 'alter trigger '|| v_triggers(ind) || ' enable';
		if p_test = 0 then 
			execute immediate v_sql;
		else
			dbms_output.put_line(v_sql || ';' ||CRLF);
		end if;
	end loop;
end enable_triggers;
------------------------------------------------------------------------------------------
procedure enable_constraints(p_hmjobs in pls_integer, p_test in pls_integer default 0) is
	v_sql varchar2(3000);
	degreeTable integer;
begin
	for ind in 1..v_primary.count loop
		v_sql := 'alter table '|| substr(v_primary(ind), 1, instr(v_primary(ind), '.') - 1 ) || 
			' enable constraint '|| substr(v_primary(ind), instr(v_primary(ind), '.') + 1 );
		if p_test = 0 then 
			execute immediate v_sql;
		else
			dbms_output.put_line(v_sql || ';' ||CRLF);
		end if;
	end loop;
	for ind in 1..v_foreign.count loop
		select degree into degreeTable from user_tables where table_name = 
			substr(v_foreign(ind), 1, instr(v_foreign(ind), '.') - 1 );
		
		v_sql := 'alter table '|| substr(v_foreign(ind), 1, instr(v_foreign(ind), '.') - 1 ) || 
			' parallel ' || p_hmjobs;
		if p_test = 0 then 
			execute immediate v_sql;
		else
			dbms_output.put_line(v_sql || ';' ||CRLF);
		end if;

		v_sql := 'alter table '|| substr(v_foreign(ind), 1, instr(v_foreign(ind), '.') - 1 ) || 
			' enable novalidate constraint '|| substr(v_foreign(ind), instr(v_foreign(ind), '.') + 1 );
		if p_test = 0 then 
			execute immediate v_sql;
		else
			dbms_output.put_line(v_sql || ';' ||CRLF);
		end if;

		v_sql := 'alter table '|| substr(v_foreign(ind), 1, instr(v_foreign(ind), '.') - 1 ) || 
			' modify constraint '|| substr(v_foreign(ind), instr(v_foreign(ind), '.') + 1 ) ||
			' validate';
		if p_test = 0 then 
			execute immediate v_sql;
		else
			dbms_output.put_line(v_sql || ';' ||CRLF);
		end if;

		v_sql := 'alter table '|| substr(v_foreign(ind), 1, instr(v_foreign(ind), '.') - 1 ) || 
			' parallel ' || degreeTable;
		if p_test = 0 then 
			execute immediate v_sql;
		else
			dbms_output.put_line(v_sql || ';' ||CRLF);
		end if;
	end loop;
end enable_constraints;
------------------------------------------------------------------------------------------
procedure disable_indexes(v_task_name varchar2, a_columns varchar_table,
                            p_tablename in varchar2, p_test in pls_integer default 0) is
	v_sql varchar2(3000);
	lv_indexes varchar_table;
	
	procedure add_index(p_index varchar2) is
		entry pls_integer;
		v_sql varchar2(3000);
	begin 
		entry := 0;
		for ind in 1..v_indexes.count loop
			if v_indexes(ind) = p_index then 
				entry := ind;
				exit;
			end if;
		end loop;
		if entry = 0 then 
			v_indexes.extend(1);
			v_indexes(v_indexes.last) := p_index;
			v_sql := 'alter index ' || v_user || '.' || p_index || ' unusable';
			if p_test = 0 then
				execute immediate v_sql;
			else
				dbms_output.put_line(v_sql || ';' || CRLF);
			end if;
			disable_primary(p_tablename, p_index, p_test);
		end if;
	end;
begin
	v_sql := 'create table ' || v_task_name || ' as select' || CRLF;
    v_sql := v_sql || 'index_name, to_lob(column_expression) index_expr from all_ind_expressions' || CRLF;
    v_sql := v_sql || 'where index_owner=''' || upper(v_user) || '''' || CRLF;
    v_sql := v_sql || 'and table_name=''' || upper(p_tablename) || '''';
--    dbms_output.put_line(v_sql || CRLF);
    execute immediate v_sql;
    v_sql := 'select index_name from all_ind_columns' || CRLF;
    v_sql := v_sql || 'where index_owner=''' || upper(v_user) || '''' || CRLF;
    v_sql := v_sql || 'and table_name=''' || upper(p_tablename) || ''' and column_name in (' || CRLF;
    for ind in 1..a_columns.COUNT loop
		v_sql := v_sql || '''' || UPPER(a_columns(ind)) || '''';
		if ind <> a_columns.COUNT then
			v_sql := v_sql || ', ';
		else
			v_sql := v_sql || ')' || CRLF;
		end if;
	end loop;
    v_sql := v_sql || 'union' || CRLF;
    v_sql := v_sql || 'select index_name from ' || v_task_name || CRLF;
    v_sql := v_sql || 'where regexp_like(index_expr,''';
    for ind in 1..a_columns.COUNT loop
		v_sql := v_sql || UPPER(a_columns(ind));
		if ind <> a_columns.COUNT then
			v_sql := v_sql || '|';
		else
			v_sql := v_sql || ''')';
		end if;
	end loop;
--    dbms_output.put_line(v_sql || CRLF);
    execute immediate v_sql bulk collect into lv_indexes;
    v_sql := 'drop table ' || v_task_name;
--    dbms_output.put_line(v_sql || CRLF);
    execute immediate v_sql;
    for ind in 1..lv_indexes.count loop
		add_index(lv_indexes(ind));
    end loop;
end disable_indexes;
------------------------------------------------------------------------------------------
procedure rebuild_indexes (p_hmjobs in pls_integer, p_test in pls_integer default 0) is
	v_sql varchar2(3000);
begin
    for ind in 1..v_indexes.COUNT loop
--     	v_sql := 'alter index ' || v_user || '.' || v_indexes(ind) || ' rebuild online parallel ' || p_hmjobs;
		v_sql := 'alter index ' || v_user || '.' || v_indexes(ind) || ' rebuild parallel ' || p_hmjobs;
		if p_test = 0 then
			begin
				execute immediate v_sql;
			exception 
			-- индексы обслуживающие constraint могут отсутствовать
			when others then null;
			end;
		else
			dbms_output.put_line(v_sql || ';' || CRLF);
		end if;
	end loop;
end rebuild_indexes;
------------------------------------------------------------------------------------------
function create_sql_text (p_tablename in varchar2, a_columns varchar_table
	, p_where in varchar2 default '') return varchar2 is
	v_sql varchar2(3000);
begin
    v_sql:= 'declare' || CRLF || ' TYPE v_record_type is RECORD (' || CRLF || '  ROWID_col ROWID,' || CRLF;
    for ind in 1..a_columns.COUNT loop
		v_sql := v_sql || '  col' || ind || ' ' || v_user || '.' || p_tablename || '.' || a_columns(ind) || '%TYPE';
		if ind <> a_columns.COUNT then
			v_sql := v_sql || ',' || CRLF;
		else
			v_sql := v_sql || ');' || CRLF;
		end if;
    end loop;
    v_sql := v_sql || ' TYPE v_record_type_t IS TABLE OF v_record_type;' || CRLF;
    v_sql := v_sql || ' v_collect_t v_record_type_t;' || CRLF;
    v_sql := v_sql || ' v_collect_r v_record_type;' || CRLF;
    v_sql := v_sql || ' ind1 pls_integer;' || CRLF;
    v_sql := v_sql || ' v_cnt pls_integer;' || CRLF;
    v_sql := v_sql || ' single_row_set exception;' || CRLF;
    v_sql := v_sql || 'begin' || CRLF;
    v_sql := v_sql || '    execute immediate ''alter session set skip_unusable_indexes=true'';' || CRLF;

    v_sql := v_sql || '    select /*+ ROWID(t) */ rowid, ';
    for ind in 1..a_columns.COUNT loop
		v_sql := v_sql || 't.' || a_columns(ind);
		if ind <> a_columns.COUNT then
			v_sql := v_sql || ', ';
		else
			v_sql := v_sql || CRLF;
		end if;
    end loop;
    v_sql := v_sql || '    bulk collect into v_collect_t' || CRLF;
    v_sql := v_sql || '    from ' || v_user || '.' || p_tablename || ' t' || CRLF;
    v_sql := v_sql || '    where id between :start_id and :end_id';
    if p_where is not null then 
		v_sql := v_sql || ' and ' || p_where;
	end if;
    v_sql := v_sql || ';' || CRLF;
    v_sql := v_sql || '    v_cnt := v_collect_t.COUNT;' || CRLF;
    v_sql := v_sql || '    if v_cnt <= 1 then ' || CRLF;
    v_sql := v_sql || '        raise single_row_set;' || CRLF;
    v_sql := v_sql || '    end if;' || CRLF;
    
    v_sql := v_sql || '    for ind in 1..v_cnt loop' || CRLF;
    for ind in 1..a_columns.COUNT loop
		v_sql := v_sql || '     while true loop' || CRLF;
		v_sql := v_sql || '          ind1 := dbms_random.value(1,v_cnt);' || CRLF;
		v_sql := v_sql || '          exit when ind1 != ind;' || CRLF;
		v_sql := v_sql || '     end loop;' || CRLF;
		v_sql := v_sql || '     v_collect_r.col' || ind || ' := v_collect_t(ind1).col' || ind || ';' || CRLF;
		v_sql := v_sql || '     v_collect_t(ind1).col' || ind || ' := v_collect_t(ind).col' || ind || ';' || CRLF;
		v_sql := v_sql || '     v_collect_t(ind).col' || ind || ' := v_collect_r.col' || ind || ';' || CRLF;
   	end loop;
   	v_sql := v_sql || '    end loop;' || CRLF;
   	v_sql := v_sql || '    forall ind in 1..v_cnt' || CRLF;
   	v_sql := v_sql || '     update ' || v_user || '.' || p_tablename || ' t set' || CRLF;
   	for ind in 1..a_columns.COUNT loop
		v_sql := v_sql || '           t.' || a_columns(ind) || ' = v_collect_t(ind).col' || ind;
		if ind <> a_columns.COUNT then
			v_sql := v_sql || ',' || CRLF;
		else
			v_sql := v_sql || CRLF;
		end if;
   	end loop;
   	v_sql := v_sql || '          where ROWID = v_collect_t(ind).ROWID_col;' || CRLF;
   	v_sql := v_sql || 'end;' || CRLF;
   	return v_sql;
end create_sql_text;
------------------------------------------------------------------------------------------
procedure after_schema(p_hmjobs in pls_integer, p_test in pls_integer default 0) is
begin
	--перестройка индексов
   	rebuild_indexes(p_hmjobs, p_test);
   	--подключение constraints
   	enable_constraints(p_hmjobs, p_test);
   	--подключение триггеров
   	enable_triggers(p_test);
exception
WHEN OTHERS THEN
	dbms_output.put_line(proc_name||' ERROR : Unhandled: ' || SQLCODE || '-' || SQLERRM);
	dbms_output.put_line(dbms_utility.format_error_backtrace);
end after_schema;
------------------------------------------------------------------------------------------
end shaker;
/