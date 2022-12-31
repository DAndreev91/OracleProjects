/*
* This is short example for decreasing OLTP tables size.
* We just divide table by some logic on small operational table and bigger "archive" table.
* Smaller table stays on OLTP DB and bigger part moves to warehouse.
* That method can be used for tables with big volumes where we need to support indexes.
* In case of more then few indexes for big tables that's a very practical approach.
*
* In that case I'll use that approach for some history table with only two indexes.    
*/

set serveroutput on;
drop user tst cascade;
create user tst identified by tst;
grant connect, resource to tst;
alter user tst quota 1024M on users;

-- Let's create some table with data (without CTAS for readability)
create table tst.data_table(
    id number generated always as identity,
    some_val varchar2(256),
    other_val number,
    created date default sysdate,
    updated date not null,
    primary key (id)
);
/

-- Adding history table for data_table (contains all changes within data_table)
create table tst.data_table_history(
    -- All data_table columns
    id number not null,
    some_val varchar2(256),
    other_val number,
    created date,
    updated date,
    -- And history columns
    history_id number generated always as identity,
    history_row_created date default sysdate,
    primary key (history_id)
);
/

-- Index in history
create index tst.x_data_tab_history_id on tst.data_table_history(id, history_id);

-- Adding before trigger
create or replace trigger tst.x_data_table_biud
before insert or update or delete
on tst.data_table 
for each row
begin
  if inserting or updating then
    :new.updated := sysdate;
  end if;
end;
/

-- Adding after trigger writing history
create or replace trigger tst.x_data_table_aiud
after insert or update or delete
on tst.data_table 
for each row
begin
  -- Writing history (all old values for changing and deleting rows and only id for new rows)
  if deleting or updating then
    insert into tst.data_table_history(id, some_val, other_val, created, updated)
    values(:old.id, :old.some_val, :old.other_val, :old.created, :old.updated);
  elsif inserting then
    insert into tst.data_table_history(id)
    values(:new.id);
  end if;
end;
/

-- Populating with data

delete from tst.data_table;
/

insert into tst.data_table(some_val, other_val)
select owner||'.'||object_name||' ('||object_type||')' some_val, object_id other_val
from dba_objects;
commit;
/

ALTER SESSION SET NLS_DATE_FORMAT = 'dd.mm.yyyy hh24:mi:ss';
select * from tst.data_table_history h order by history_id desc;

-- Changing data (writing history)
begin
  dbms_random.seed(to_char(sysdate,'dd.mm.yyyy hh24:mi:ss'));
  for i in 1..3 loop
    update tst.data_table
    set other_val = other_val + 1
    where mod(id, round(dbms_random.value(0, 10))) = 0;
  end loop;
  commit;
end;
/

/*
* Now we have history table with various data and want to move it to warehouse.
* We can move all history to WH but we suppose that our clients query history data with high frequence.
* In that case we assume that more recent data have higher change of being queryed.
* So we split history table in two pieces - 1) operational layer (small and within OLTP) 2) WH layer (big and within WH)
*/

-- Alter table to partitioned state (without dbms_redefenition in our case)
alter table tst.data_table_history modify 
  partition by range (history_id) interval (50000)
  (
    partition p1 values less than (50000),
    partition p2 values less than (100000),
    partition p3 values less than (150000),
    partition p4 values less than (200000),
    partition p5 values less than (250000)
  ) online;
/

-- Check results
select * from dba_part_tables where owner = 'TST';
select * from dba_part_indexes where owner = 'TST';
select * from dba_tab_partitions where table_owner = 'TST';

-- Creating partitioned table in WH (for our case we creating in neighbor schema)
drop user history cascade;
create user history identified by history;
grant connect, resource to history;
alter user history quota 1024M on users;

create table history.data_table_history(
  -- All data_table columns
  id number not null,
  some_val varchar2(256),
  other_val number,
  created date,
  updated date,
  -- And history columns
  history_id number generated always as identity,
  history_row_created date default sysdate,
  primary key (history_id)
)
partition by range (history_id) interval (50000)
(
  partition p1 values less than (50000),
  partition p2 values less than (100000),
  partition p3 values less than (150000),
  partition p4 values less than (200000),
  partition p5 values less than (250000)
);
/

create index history.x_data_table_history_id on history.data_table_history(id, history_id) local;
/

-- Exchange all partitions except last one (for enterprise solution we can use datapump between servers)
-- Create temp table
create table tst.exch_temp 
for exchange with table tst.data_table_history;
alter table tst.exch_temp add constraint x_pk primary key (history_id);

-- Change partition to temp and to WH table
alter table tst.data_table_history
  exchange partition p1
  with table tst.exch_temp;
  
select count(1) from tst.data_table_history partition (p1); -- 0
select count(1) from tst.exch_temp; -- 49999

alter table history.data_table_history
  exchange partition p1
  with table tst.exch_temp
  without validation;

select count(1) from tst.exch_temp; -- 0
select count(1) from history.data_table_history partition (p1); -- 49999

-- For different DBs we can create db_link between them
grant select on history.data_table_history to tst;

-- Create history view (union all for excluding doubles checks and therefore perfomance).

create or replace view tst.data_table_h as
select id, some_val, other_val, created, updated, history_id, history_row_created
from tst.data_table_history
union all
select id, some_val, other_val, created, updated, history_id, history_row_created
from history.data_table_history;

select * from tst.data_table_h where id = 530;

/*
* With that approach we'll get some problems when we need to shutdown WH server. 
* But that's would only need to change db_links to OLTP servers and that's all.
*/

