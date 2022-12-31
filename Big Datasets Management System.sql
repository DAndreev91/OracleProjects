/*
* Create API for management temporary SQL datasets.
* These datasets forms in separate partitions and would be dropped after expiration date by himself.
* Also there are possible to start gathering data from last save point.
* Tests included
*/

-- create user

set serveroutput on;
drop user tst cascade;
create user tst identified by tst;
grant connect, resource to tst;
grant select on v_$session to tst;
alter user tst quota 1024M on users;

-- create detail table with list partition (1..10)
create table tst.data_list(
	partition_id number not null,
	num_1 number,
	num_2 number,
	num_3 number,
	var_1 varchar2(4000),
	var_2 varchar2(4000),
	var_3 varchar2(4000),
	date_1 date
)
partition by range (partition_id) interval (1) (
    partition part0 values less than (0)
);
/



-- create header table for partitions metadata (parition_id, task_key, duedate, query_text, complete_flag, can_be_continued, processing_now)

create table tst.data_head(
  partition_id number generated always as identity(start with 1 increment by 1),
	task_key varchar2(36),
	due_date date,
	query_text clob,
	complete_flag number default 0
  constraint complete_flag_check check (complete_flag between 0 and 1),
	can_be_continued number
  constraint can_be_continued_check check (can_be_continued between 0 and 1),
  processing_now number default 0
  constraint processing_now_check check (processing_now between 0 and 1)
);
/




-- can_be_continued fills with Y when session with action = task_key not found in ACTIVE sessions. Search by job in 5 min intervals
-- All tasks processed by jobs corresponding to partition_id. So each partition would have job.



-- create api package for gathering data (one partition = one task, header stores duedate when expires partition reade to be reused)
-- Within package partition assigned to task, locks by that task before duedate, data queried by single thread or parallel execution (dbms_parallel_execute) and stored in that partition
-- Stored data returns by api (header contains key to that task)
-- In case of error we can use built-in log system that helps us run task again from point where we failed (only for tasks with unique rows ID (PK))
-- Realization: 
-- 1) assign partition and set session action = task_key
-- 2) built cursor from query_text and in case of can_be_continued add left join with that partition on PK and search rows only for left join PK is null
-- 3) fetch cursor by limit for can_be_contined or all in other case and stores inside partition
-- 4) fill complete_flag
-- For quering use some pipeline function

-- For simplicity assumes that we get all columns from detail table in resulting function without their logical names (don't do any mapping)


-- Two package for security management

-- Interface package
create or replace package tst.data_api 
as

  type tDataList is table of tst.data_list%rowtype;

  -- Create tasks that'll be assigned to one of partitions
  -- pTaskKey - Task name or GUID
  -- pDueDate - expire date for result dataset
  -- pQueryText - text of query
  -- pCanBeContinued - 1 if query has unique field
  procedure createTask (
    pTaskKey in tst.data_head.task_key%type,
    pDueDate in tst.data_head.due_date%type := sysdate + 90,
    pQueryText in tst.data_head.query_text%type,
    pCanBeContinued in tst.data_head.can_be_continued%type := 1
  );
  
  -- Getting task result in pipelined function
  function getTaskResult (
    pTaskKey in tst.data_head.task_key%type
  ) return tDataList pipelined;

end data_api;
/

create or replace package body tst.data_api
as

  procedure createTask (
    pTaskKey in tst.data_head.task_key%type,
    pDueDate in tst.data_head.due_date%type := sysdate + 90,
    pQueryText in tst.data_head.query_text%type,
    pCanBeContinued in tst.data_head.can_be_continued%type := 1
  ) is
  begin
    insert into tst.data_head(task_key, due_date, query_text, can_be_continued)
    values(pTaskKey, pDueDate, pQueryText, pCanBeContinued);
    commit;
  end createTask;
  
  
  -- Getting task result in pipelined function
  function getTaskResult (
    pTaskKey in tst.data_head.task_key%type
  ) return tDataList pipelined is
    vDataListRow tst.data_list%rowtype;
  begin
  
  for r in (
    select * 
    from tst.data_list
    where partition_id = (
      select partition_id
      from tst.data_head
      where task_key = pTaskKey
    )
  ) loop
    vDataListRow := r;
    pipe row(vDataListRow);
  end loop;
  return;
  end getTaskResult;
  
end data_api;
/



-- Utility package for jobs
create or replace package tst.data_utils
as

  cModuleName varchar2(16) := 'tst.data_utils';
  vTestFlag number := 0;
  cLimit number := 1000;
  
  -- Fetch first free task and process it
  procedure processTasks;
  
  -- Get 1 when there is active session with action = pTaskKey and 0 when not
  function isSessionActive(pTaskKey in tst.data_head.task_key%type)
  return number;
  
  -- Set processing_now = 0 for inactive not finished tasks
  procedure returnUnfinishedTasks;
  
  -- Drop partition with expired data
  procedure dropExpiredPartition;
  
  -- Get next task by select for update skip locked because with simple update we can't see locks
  function getNextTask return tst.data_head.task_key%type;
  
  -- Get cursor with left join with partition data for continuable tasks and plain cursor for others
  function getDataCursor(
    pQueryText in tst.data_head.query_text%type,
    pCanBeContinued in tst.data_head.can_be_continued%type,
    pPartitionId in tst.data_head.partition_id%type
  ) return sys_refcursor;
  
  -- Now executing cursor and filling data_list, fill complete_flag
  procedure writeCursorData(
    pDataCursor in sys_refcursor
  );

end data_utils;
/


-- Utility package for jobs
create or replace package body tst.data_utils
as

  procedure logs(pMsg in varchar2) is
  begin
    if vTestFlag = 1 then
      dbms_output.put_line(pMsg);
    end if;
  end logs;
  
  procedure processTasks
  is
    vTaskKey tst.data_head.task_key%type;
    vQueryText tst.data_head.query_text%type;
    vCanBeContinued tst.data_head.can_be_continued%type;
    vPartitionId tst.data_head.partition_id%type;
    vCursor sys_refcursor;
  begin
  
    vTaskKey := getNextTask;
    
    -- If don't find any work - exit
    if vTaskKey is null then
      return;
    end if;
    
    -- set session action = task_key
    dbms_application_info.set_action(vTaskKey);
    
    select query_text, can_be_continued, partition_id
    into vQueryText, vCanBeContinued, vPartitionId
    from tst.data_head
    where task_key = vTaskKey;
    
    vCursor := getDataCursor(vQueryText, vCanBeContinued, vPartitionId);
    
    -- Write and close cur
    writeCursorData(vCursor);
    
    update tst.data_head
    set complete_flag = 1,
    processing_now = 0
    where task_key = vTaskKey;
    
    -- clear session action
    dbms_application_info.set_action(null);
  
  exception
    when no_data_found then
      -- Log error here
      logs('Error: '||sqlerrm||'; '||dbms_utility.format_error_backtrace);
      null;
  end processTasks;
  
  -- Get 1 when there is active session with action = pTaskKey and 0 when not
  function isSessionActive(pTaskKey in tst.data_head.task_key%type)
  return number
  is
    vResult number;
  begin
    select sign(count(1))
    into vResult
    from v$session
    where status in ('ACTIVE')
    and action = pTaskKey
    and module = cModuleName;
    
    return vResult;
  end isSessionActive;
  
  -- Set processing_now = 0 for inactive not finished tasks
  procedure returnUnfinishedTasks
  is
  begin
    update tst.data_head
    set processing_now = 0
    where processing_now = 1
    and isSessionActive(task_key) = 0
    and complete_flag = 0;
  end returnUnfinishedTasks;
  
  -- Drop partition with expired data
  procedure dropExpiredPartition
  is
  begin
    for r in (
      with xmlform as
      (
      select dbms_xmlgen.getxmltype(q'[select table_name,partition_name,high_value from all_tab_partitions where table_name='DATA_LIST' and table_owner = 'TST']') as x
        from dual
      ),
      tab_part as (
        select xmltab.partition_name, to_number(xmltab.high_value) high_value
        from xmlform
            ,xmltable('/ROWSET/ROW'
                passing xmlform.x
                columns table_name varchar2(20) path 'TABLE_NAME'
                       ,partition_name  varchar2(20) path 'PARTITION_NAME'
                       ,high_value varchar2(85) path 'HIGH_VALUE'
            ) xmltab
      ) 
      select p.partition_name, h.partition_id, h.task_key 
      from tab_part p, tst.data_head h 
      where h.partition_id+1 = p.high_value
      and h.due_date < sysdate
      and h.complete_flag = 1
    ) loop
      delete from tst.data_head where partition_id = r.partition_id;
      execute immediate 'alter table tst.data_list drop partition '||r.partition_name;
    end loop;
  end dropExpiredPartition;
  
  -- Get next task by select for update skip locked because with simple update we can't see locks
  function getNextTask
  return tst.data_head.task_key%type
  is
    vTaskKey tst.data_head.task_key%type;
    vCur sys_refcursor;
  begin
    -- open cursor with free rows and locks them
    open vCur for
      select task_key
      from tst.data_head
      where processing_now = 0
      order by partition_id
      for update skip locked;
      
    -- fetch locked row and close cursor
    fetch vCur into vTaskKey;
    close vCur;
    
    -- set processing flag for task
    update tst.data_head
    set processing_now = 1
    where task_key = vTaskKey;
    
    -- release lock
    commit;
    
    return vTaskKey;
  exception 
    when others then
      -- Log here
      logs('Error: '||sqlerrm||'; '||dbms_utility.format_error_backtrace);
      rollback;
  end getNextTask;
  
  
  -- Get cursor with left join with partition data for continuable tasks and plain cursor for others
  -- We can use bind variables inside query_text but not for these example
  function getDataCursor(
    pQueryText in tst.data_head.query_text%type,
    pCanBeContinued in tst.data_head.can_be_continued%type,
    pPartitionId in tst.data_head.partition_id%type
  ) return sys_refcursor is
    vCursor sys_refcursor;
    vSQL tst.data_head.query_text%type;
  begin
    -- If query can be continued then it had PK and we can use it for left join and query only nonexistent rows
    if pCanBeContinued = 1 then
      vSQL := q'[select :pPartitionId partition_id, x.num_1, x.num_2, x.num_3, x.var_1, x.var_2, x.var_3, x.date_1 
        from (]'||pQueryText||q'[) x 
        left join (select num_1 from tst.data_list where partition_id = :pPartitionId) l on (l.num_1 = x.num_1) 
        where l.num_1 is null]';
      open vCursor for vSQL using pPartitionId, pPartitionId;
    else
      -- If can_be_continued = 0 then delete data from partition
      delete from tst.data_list where partition_id = pPartitionId;
      vSQL := 'select 
          :pPartitionId partition_id, x.num_1, x.num_2, x.num_3, x.var_1, x.var_2, x.var_3, x.date_1 
        from ('||pQueryText||') x';
      open vCursor for vSQL using pPartitionId;
    end if;
    logs(vSQL);
    return vCursor;
  end getDataCursor;
  
  
  -- Now executing cursor and filling data_list, fill complete_flag
  procedure writeCursorData(
    pDataCursor in sys_refcursor
  ) is
    vDataListTab tst.data_api.tDataList;
  begin
    -- Fetch by limit
    loop
      fetch pDataCursor bulk collect into vDataListTab limit cLimit;
      exit when vDataListTab.count = 0;
      
      forall i in 1..vDataListTab.count
        insert into tst.data_list
        values(
          vDataListTab(i).partition_id, 
          vDataListTab(i).num_1, 
          vDataListTab(i).num_2, 
          vDataListTab(i).num_3, 
          vDataListTab(i).var_1, 
          vDataListTab(i).var_2, 
          vDataListTab(i).var_3,
          vDataListTab(i).date_1
        );
    end loop;
    close pDataCursor;
    commit;
  end writeCursorData;
  

end data_utils;
/





create or replace package tst.data_test as
    type tDataHeadTab is table of tst.data_head%rowtype;
    type tDataListTab is table of tst.data_list%rowtype;

    procedure runReturnUnfinishedTasks;
    procedure runDropExpiredPartition;
    procedure runGetNextTask;
    procedure runGetDataCursor;
    procedure runWriteCursorData;
end data_test;
/



create or replace package body tst.data_test as

    procedure createTestData is
        vPartitionId number;
    begin
        tst.data_utils.vTestFlag := 1;
    
        insert into tst.data_head(task_key, due_date, query_text, complete_flag, can_be_continued, processing_now)
        values('task_key1', sysdate-1, 'select 1 from dual', 1, 1, 1)
        returning partition_id into vPartitionId;
        insert into tst.data_list
        values(vPartitionId, 1, 1, 1, '1', null, null, sysdate);
        insert into tst.data_list
        values(vPartitionId, 2, 2, 2, '2', null, null, sysdate);
        
        insert into tst.data_head(task_key, due_date, query_text, complete_flag, can_be_continued, processing_now)
        values('task_key2', sysdate+1, 'select 2 from dual', 1, 0, 1)
        returning partition_id into vPartitionId;
        insert into tst.data_list
        values(vPartitionId, 3, 3, 3, '3', null, null, sysdate);
        
        
        insert into tst.data_head(task_key, due_date, query_text, complete_flag, can_be_continued, processing_now)
        values('task_key3', sysdate+1, 'select 3 from dual', 0, 0, 1)
        returning partition_id into vPartitionId;
        insert into tst.data_list
        values(vPartitionId, 3, 3, 3, 'task_key31', null, null, sysdate);
        insert into tst.data_list
        values(vPartitionId, 3, 3, 3, 'task_key34', null, null, sysdate);
        insert into tst.data_list
        values(vPartitionId, 3, 3, 3, 'task_key35', null, null, sysdate);
        
        insert into tst.data_head(task_key, due_date, query_text, complete_flag, can_be_continued, processing_now)
        values('task_key4', sysdate+3, 'select object_id num_1, null num_2, null num_3, object_name var_1, null var_2, null var_3, null date_1 from all_objects where rownum <= 10000', 0, 0, 0)
        returning partition_id into vPartitionId;
        insert into tst.data_list
        values(vPartitionId, 16, 3, 3, 'task_key31', null, null, sysdate);
        
        commit;
    end createTestData;
    
    procedure deleteTestData is
    begin
        delete from tst.data_list;
        delete from tst.data_head;
        
        tst.data_utils.vTestFlag := 0;
        commit;
    end deleteTestData;

    procedure runReturnUnfinishedTasks is
      vDataTab tDataHeadTab;
      vErrorMsg varchar2(256);
    begin
        deleteTestData;
        createTestData;
        select *
        bulk collect into vDataTab
        from tst.data_head
        where processing_now = 1
        and complete_flag = 0;
        tst.data_utils.returnUnfinishedTasks;
        for r in (
          select h.*
          from table(vDataTab) v, tst.data_head h
          where h.task_key = v.task_key
          and h.processing_now = 1
        ) loop
          vErrorMsg := vErrorMsg||'task_key with still processing_now = 1 - '||r.task_key||chr(10);
        end loop;
        if vErrorMsg is null then
          dbms_output.put_line('All is fine');
        else
          dbms_output.put_line(vErrorMsg);
        end if;
        deleteTestData;
    end runReturnUnfinishedTasks;
    
    procedure runDropExpiredPartition is
      vPartitionsCount number;
      vExpiredHeaderCount number;
      vPartitionCountAfter number;
      vExpiredHeaderCountAfter number;
    begin
        deleteTestData;
        createTestData;
        select count(1) 
        into vPartitionsCount
        from all_tab_partitions 
        where table_name='DATA_LIST' 
        and table_owner = 'TST';
        select count(1)
        into vExpiredHeaderCount
        from tst.data_head
        where complete_flag = 1
        and due_date < sysdate;
        dbms_output.put_line('Partitions count = '||vPartitionsCount);
        dbms_output.put_line('Expired tasks count = '||vExpiredHeaderCount);
        
        tst.data_utils.dropExpiredPartition;
        
        select count(1) 
        into vPartitionCountAfter
        from all_tab_partitions 
        where table_name='DATA_LIST' 
        and table_owner = 'TST';
        dbms_output.put_line('Partitions count after drop= '||vPartitionCountAfter);
        if vPartitionCountAfter = vPartitionsCount - vExpiredHeaderCount then
          dbms_output.put_line('All good');
        else
          dbms_output.put_line('Error - drop partition ended not right. vPartitionCountAfter = '||vPartitionCountAfter);
          select count(1)
          into vExpiredHeaderCountAfter
          from tst.data_head
          where complete_flag = 1
          and due_date < sysdate;
          dbms_output.put_line('vExpiredHeaderCountAfter = '||vExpiredHeaderCountAfter);
        end if;
        
        deleteTestData;
    end runDropExpiredPartition;
    
    procedure runGetNextTask is
      vTaskKeyBefore tst.data_head.task_key%type;
      vTaskKeyAfter tst.data_head.task_key%type;
      vRes number;
    begin
        deleteTestData;
        createTestData;
        
        select task_key
        into vTaskKeyBefore
        from tst.data_head
        where processing_now = 0
        order by partition_id
        fetch first rows only;
        
        vTaskKeyAfter := tst.data_utils.getNextTask;
        
        if vTaskKeyAfter = vTaskKeyBefore then
          select count(1)
          into vRes
          from tst.data_head
          where task_key = vTaskKeyAfter
          and processing_now = 1;
          if vRes = 1 then
            dbms_output.put_line('All good');
          else
            dbms_output.put_line('Error - processing_now != 1');
          end if;
        else
          dbms_output.put_line('Error - getNextTask returns unexpected task_key = '||vTaskKeyAfter||'; Expect task_key = '||vTaskKeyBefore);
        end if;
        deleteTestData;
    end runGetNextTask;
    
    procedure runGetDataCursor is
      vCursor sys_refcursor;
      vQueryText tst.data_head.query_text%type;
      vCanBeContinued tst.data_head.can_be_continued%type;
      vPartitionId tst.data_head.partition_id%type;
      vTab tDataListTab;
    begin
        deleteTestData;
        createTestData;
        
        select query_text, can_be_continued, partition_id
        into vQueryText, vCanBeContinued, vPartitionId
        from tst.data_head
        where task_key = 'task_key4';
        
        vCursor := tst.data_utils.getDataCursor(vQueryText, vCanBeContinued, vPartitionId);
        
        fetch vCursor bulk collect into vTab;
        close vCursor;
        
        if vTab.count between 9999 and 10000 then
          dbms_output.put_line('All good');
        else
          dbms_output.put_line('Error - we''ve got incorrect count in dataset, vCount = '||vTab.count);
        end if;
        
        deleteTestData;
    end runGetDataCursor;
    
    procedure runWriteCursorData is
      vCursor sys_refcursor;
      vQueryText tst.data_head.query_text%type;
      vCanBeContinued tst.data_head.can_be_continued%type;
      vPartitionId tst.data_head.partition_id%type;
      vCnt number;
    begin
        deleteTestData;
        createTestData;
        select query_text, can_be_continued, partition_id
        into vQueryText, vCanBeContinued, vPartitionId
        from tst.data_head
        where task_key = 'task_key4';
        
        vCursor := tst.data_utils.getDataCursor(vQueryText, vCanBeContinued, vPartitionId);
        tst.data_utils.writeCursorData(vCursor);
        
        select count(1)
        into vCnt
        from tst.data_list
        where partition_id = vPartitionId;
        
        if vCnt between 9999 and 10000 then
          dbms_output.put_line('All good. vCnt = '||vCnt);
        else
          dbms_output.put_line('Error - we''ve got incorrect count in table, vCount = '||vCnt);
        end if;
        
        deleteTestData;
    end runWriteCursorData;    

end data_test;
/


begin
  tst.data_test.runReturnUnfinishedTasks;
end;
begin
  tst.data_test.runDropExpiredPartition;
end;
begin
  tst.data_test.runGetNextTask;
end;
begin
  tst.data_test.runGetDataCursor;
end;
begin
  tst.data_test.runWriteCursorData;
end;

begin
  tst.data_api.createTask(
    pTaskKey => 'TaskKey_test',
    pDueDate => sysdate + 2,
    pQueryText => 'select object_id num_1, data_object_id num_2, namespace num_3, object_name var_1, owner var_2, object_type var_3, last_ddl_time date_1 from all_objects where rownum <= 10000',
    pCanBeContinued => 1
  );
end;

select * from tst.data_head;
select * from tst.data_list;

begin
  tst.data_utils.processTasks;
exception
  when others then
    tst.data_utils.returnUnfinishedTasks;
end;

select * from table(tst.data_api.getTaskResult('TaskKey_test'));



begin
  dbms_scheduler.drop_job('tst.RenewProcessingNow, tst.DropExpiredPartitions, tst.processTasks_1, tst.processTasks_2');
end;

-- Create job that sets processing_now = 0 for tasks without active session and not finished
begin
  DBMS_SCHEDULER.create_job (
    job_name => 'tst.RenewProcessingNow',
    job_type => 'PLSQL_BLOCK',
    job_action => 'begin tst.data_utils.returnUnfinishedTasks; end;',
    start_date => sysdate,
    repeat_interval => 'freq=minutely;interval=1',
    enabled => true,
    comments => 'Job that sets tst.data_head.processing_now = 0 for unfinished tasks without active working session'
  );
end;
/

-- Create job for partitions drop when result are expire
begin
  DBMS_SCHEDULER.create_job (
    job_name => 'tst.DropExpiredPartitions',
    job_type => 'PLSQL_BLOCK',
    job_action => 'begin tst.data_utils.dropExpiredPartition; end;',
    start_date => sysdate,
    repeat_interval => 'freq=hourly;interval=1',
    enabled => true,
    comments => 'Job that drops tst.data_list partitions with expired due_date'
  );
end;
/


-- Create jobs for each partitions for process tasks 
begin
  DBMS_SCHEDULER.create_job (
    job_name => 'tst.processTasks_1',
    job_type => 'PLSQL_BLOCK',
    job_action => '
    begin 
      dbms_application_info.set_module(tst.data_utils.cModuleName, null);
      tst.data_utils.processTasks; 
      commit;
    end;',
    start_date => sysdate,
    repeat_interval => 'freq=minutely;interval=1',
    enabled => true,
    comments => 'Job that processes tst.data_head tasks with processing_now = 0'
  );
end;
/


begin
  DBMS_SCHEDULER.create_job (
    job_name => 'tst.processTasks_2',
    job_type => 'PLSQL_BLOCK',
    job_action => '
    begin 
      dbms_application_info.set_module(tst.data_utils.cModuleName, null);
      tst.data_utils.processTasks; 
      commit;
    end;',
    start_date => sysdate,
    repeat_interval => 'freq=minutely;interval=1',
    enabled => true,
    comments => 'Job that processes tst.data_head tasks with processing_now = 0'
  );
end;
/

select * from dba_scheduler_jobs where owner = 'TST';

select * from v$session where upper(action) like '%TASKKEY%' and upper(module) = upper('tst.data_utils');

-- Potential improvements
-- We can replace jobs with more flexible mechanism without constant number of sessions
-- We can move assign partition in async process and user only will create task without getting error


