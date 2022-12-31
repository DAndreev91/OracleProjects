set serveroutput on;
create user tst identified by tst;
alter user tst quota 1024M on users;

grant select on v_$result_cache_objects to tst;
grant execute on dbms_result_cache to tst;

/*
* As is: 
* tst.object_attribs - table with objects attributes
* tst.object_attribs_utl - DML package for object_attribs table
* 
* To be:
* Add cache mechanism with result cache api for all(!) db users
* 
* Problem in rational usage of result cache blocks and invalidating mechanism (as always)
* Now we assume that we caching all table for simplicity and table data almost doesn't change (store aggregate values in one block)
* In big tables (over 100 000 000 rows) we choose data part (segment or logical) for caching 
*
* I'm suppose that result cache is enabled in DB:
* DBMS_RESULT_CACHE.STATUS = 'ENABLED'
*/

-- Logger package
create or replace package tst.timer as
  
  procedure startTimer;
  
  procedure restartTimer;
  
  procedure stopTimer;
end timer;
/
create or replace package body tst.timer as
  t timestamp;
  
  procedure startTimer is
  begin
    t := systimestamp;
  end startTimer;
  
  procedure restartTimer is
  begin
    dbms_output.put_line(systimestamp - t);
    t := systimestamp;
  end restartTimer;
  
  procedure stopTimer is
  begin
    dbms_output.put_line(systimestamp - t);
    t := null;
  end stopTimer;
end timer;


-- Create attributes table
drop table tst.object_attribs;
create table tst.object_attribs(
  object_id number,
  attr_key number,
  attr_value varchar2(4000)
)
tablespace users;

/*
* I'm intentionally don't create indexes for such small table
*/

-- Populate attribute table
insert into tst.object_attribs
select 
  o.object_id,
  attrs.attr_key,
  attrs.attr_value
from dba_objects o,
lateral(
  select 
    o.object_id,
    round(dbms_random.value(0, 1000)) attr_key, 
    dbms_random.string('x', 10) attr_value
  from dual 
  where o.object_id = o.object_id
  connect by level <= dbms_random.value(1, 5)
) attrs
where attrs.object_id = o.object_id
-- for simplicity
fetch first 100000 rows only;

commit;

-- UTL package as is
create or replace package tst.object_attribs_utl as
  /*
  * Attribute getter
  */
  function getAttrValue(
    pObjectId in number, 
    pAttrKey in number
  ) return varchar2;
    
  /*
  * Attribute setter
  */ 
  procedure setAttrValue(
    pObjectId in number,
    pAttrKey in number,
    pAttrValue in varchar2
  );
end object_attribs_utl;
/


create or replace package body tst.object_attribs_utl as
  /*
  * Attribute getter
  */
  function getAttrValue(
    pObjectId in number, 
    pAttrKey in number
  ) return varchar2 is
    vAttrValue tst.object_attribs.attr_value%type;
  begin
    <<getVal>>
    begin
      select attr_value
      into vAttrValue
      from tst.object_attribs
      where object_id = pObjectId
      and attr_key = pAttrKey;
    exception
      when no_data_found then
        vAttrValue := null;
      when too_many_rows then
        vAttrValue := 'DoubleDataError';
    end getVal;
    
    return vAttrValue;
  end getAttrValue;
    
  /*
  * Attribute setter
  */ 
  procedure setAttrValue(
    pObjectId in number,
    pAttrKey in number,
    pAttrValue in varchar2
  ) is
  begin
    merge into tst.object_attribs oa
    using (select pAttrValue from dual) p
    on (oa.object_id = pObjectId and oa.attr_key = pAttrKey)
    when matched then 
      update set
      oa.attr_value = pAttrValue
    when not matched then 
      insert(object_id, attr_key, attr_value)
      values(pObjectId, pAttrKey, pAttrValue);
  end setAttrValue;
  
end object_attribs_utl;
/

-- Cache package to be

create or replace package tst.object_attribs_cache as

  -- Cache blocks count
  cHashCnt number := 1000;
  /*
  * Setter for value in attr cache
  */
  procedure setAttrVal(
    pObjectId in number,
    pAttrKey in number,
    pAttrValue in varchar2
  );
  /*
  * Getter for value in attr cache
  */
  function getAttrVal(
    pObjectId in number,
    pAttrKey in number
  ) return varchar2;
  
  /*
  * Initialize attr cache
  */
  procedure initAttrCache;
  
  /*
  * Get raw string for one attr
  */
  function getAttrRawString(
    pObjectId in number,
    pAttrKey in number,
    pAttrValue in varchar2
  ) return raw;
  
  /*
  * Get hash key for key
  */
  function getHashKey(
    pObjectId in number
  ) return varchar2;
  
end object_attribs_cache;
/


create or replace package body tst.object_attribs_cache as

  /*
  * We place all values in result cache by api package in form of raw string with dividers (null byte - 00)
  * All key-value pairs would store in 32Kb blocks for used space reasons (in result cache it can be expensive)
  * For equal distribution of pairs in blocks we need to randomize keys so hash functions is our best friend
  */
  
  /*
  * Get hash key for key
  */
  function getHashKey(
    pObjectId in number
  ) return varchar2
  is
    vHash varchar2(32);
  begin
    select ora_hash(pObjectId, cHashCnt)
    into vHash
    from dual;
    return vHash;
  end getHashKey;
  
  /*
  * Invalidate all cache
  */
  procedure clearCacheAll
  is
  begin
    for r in (
      select id
      from v$result_cache_objects
      where namespace = 'API'
      and status != 'Invalid'
    ) loop
      dbms_result_cache.invalidate_object(r.id);
    end loop;
  end clearCacheAll;
  
  /*
  * Invalidate cache by key
  */
  procedure clearCacheByKey(pKey in varchar2)
  is
    vKey varchar2(32) := upper(pKey);
  begin
    for r in (
      select id 
      from v$result_cache_objects
      where name = pKey
      and namespace = 'API'
      and status != 'Invalid'
    ) loop
      dbms_result_cache.invalidate_object(r.id);
    end loop;
  end clearCacheByKey;
  
  /*
  * Set cache block
  */
  procedure setCacheBlockByKey(pKey in varchar2, pValue in raw) 
  is
    cc number;
    vRaw raw(32767);
  begin
    -- without isPublic another users cannot see our cache
    cc := dbms_result_cache_api.get(key => upper(pKey), value => vRaw, isPublic => 1);
    -- Not exists
    if cc = 2 then
      cc := dbms_result_cache_api.set(value => pValue);
    -- Already exists cache with this key
    elsif cc = 1 then
      ClearCacheByKey(pKey);
      setCacheBlockByKey(pKey, pValue);
    end if;
  end setCacheBlockByKey;
  
  /*
  * Get cache block
  */
  function getCacheBlockByKey(pKey in varchar2) 
  return raw 
  is
    cc number;
    vRaw raw(32767);
  begin
    -- without isPublic another users cannot see our cache
    cc := dbms_result_cache_api.get(key => upper(pKey), value => vRaw, isPublic => 1, noCreate => 1);
    return vRaw;
  end getCacheBlockByKey;
  
  /*
  * Get raw string for one attr
  */
  function getAttrRawString(
    pObjectId in number,
    pAttrKey in number,
    pAttrValue in varchar2
  ) return raw
  is
    vRaw raw(32767);
  begin
    -- raw string ending with 00
    vRaw := utl_raw.cast_from_number(pObjectID)||
      utl_raw.cast_from_number(pAttrKey)||
      case when pAttrValue is not null then 
        utl_raw.cast_to_raw(pAttrValue)||hextoraw('00')
      else
        null
      end;
    return vRaw;
  end getAttrRawString;
  
  /*
  * Get upper and lower bounds for key_value pair inside cache block
  */
  procedure getAttrBoundsInCacheBlock(
    pRaw in raw, 
    pObjectId in number, 
    pAttrKey in number,
    pLowerPos out pls_integer,
    pUpperPos out pls_integer,
    pAttrValuePos out pls_integer
  )
  is
    vRaw raw(32767) := pRaw;
    vCacheKey raw(32);
    vTmpBlob blob;
  begin
    vCacheKey := getAttrRawString(pObjectId, pAttrKey, null);
    vTmpBlob := to_blob(vRaw);
    pUpperPos := 0;
    pLowerPos := dbms_lob.instr(vTmpBlob, vCacheKey, 1, 1);
    if pLowerPos > 0 then
      -- Looking 00 position before next attr value
      pUpperPos := dbms_lob.instr(vTmpBlob, hextoraw('00'), pLowerPos, 1);
      -- Find position where attr_value starts
      pAttrValuePos := pLowerPos + utl_raw.length(vCacheKey);
    end if;
  end getAttrBoundsInCacheBlock;
  
  /*
  * Delete key-val pair from raw string and return new value
  */
  function delOldValueInCacheBlock(
    pRaw in raw, 
    pObjectId in number, 
    pAttrKey in number
  ) return raw 
  is
    vRaw raw(32767) := pRaw;
    vTmpBlob blob;
    vCachePos pls_integer;
    vDividerPos pls_integer;
    vValuePos pls_integer;
    vPartRaw raw(32767);
  begin
    getAttrBoundsInCacheBlock(
      vRaw,
      pObjectId,
      pAttrKey,
      vCachePos,
      vDividerPos,
      vValuePos
    );
    if vCachePos > 0 then
      vTmpBlob := to_blob(vRaw);
      -- Substr second part after 00 divider
      vPartRaw := dbms_lob.substr(vTmpBlob, dbms_lob.getlength(vTmpBlob)-vDividerPos, vDividerPos+1);
      -- Substr first part of blob before vCacheKey
      vRaw := dbms_lob.substr(vTmpBlob, vCachePos-1, 1);
      -- Merge parts
      vRaw := vRaw||vPartRaw;
    end if;
    return vRaw;
  end delOldValueInCacheBlock;
  
  /*
  * Append key-val pair to raw string
  */
  function insNewValueInCacheBlock(
    pRaw in raw, 
    pObjectId in number, 
    pAttrKey in number,
    pAttrValue in varchar2
  )
  return raw 
  is
    vRaw raw(32767) := pRaw;
    vCachePair raw(128);
  begin
    vCachePair := getAttrRawString(pObjectId, pAttrKey, pAttrValue);
    vRaw := vRaw||vCachePair;
    return vRaw;
  end insNewValueInCacheBlock;
  
  /*
  * Setter for value in attr cache
  */
  procedure setAttrVal(
    pObjectId in number,
    pAttrKey in number,
    pAttrValue in varchar2
  ) is
    vKey varchar2(32);
    vRaw raw(32767);
  begin
    -- get hash (key)
    vKey := getHashKey(pObjectId);
    -- get all block raw value
    vRaw := getCacheBlockByKey(vKey);
    -- invalidate this cache block
    clearCacheByKey(vKey);
    -- delete from block old key-value pair
    vRaw := delOldValueInCacheBlock(vRaw, pObjectId, pAttrKey);
    -- append new pair
    vRaw := insNewValueInCacheBlock(vRaw, pObjectId, pAttrKey, pAttrValue);
    -- set new block by this key
    setCacheBlockByKey(vKey, vRaw);
  end setAttrVal;
  
  /*
  * Get value from key-val pair from raw
  */
  function getAttrValueFromCacheBlock(
    pRaw in raw, 
    pObjectId in number, 
    pAttrKey in number
  )
  return varchar2 
  is
    vRaw raw(32767) := pRaw;
    vCachePos pls_integer;
    vDividerPos pls_integer;
    vValuePos pls_integer;
    vTmpBlob blob;
    vAttrValueRaw raw(128);
    vAttrValue varchar2(128);
  begin
    getAttrBoundsInCacheBlock(
      vRaw,
      pObjectId,
      pAttrKey,
      vCachePos,
      vDividerPos,
      vValuePos
    );
    if vCachePos > 0 then
      vTmpBlob := to_blob(vRaw);
      -- Substr second part after 00 divider
      vAttrValueRaw := dbms_lob.substr(vTmpBlob, vDividerPos-vValuePos, vValuePos);
      vAttrValue := utl_raw.cast_to_varchar2(vAttrValueRaw);
    end if;
    return vAttrValue;
  end getAttrValueFromCacheBlock;
  
  /*
  * Getter for value in attr cache
  */
  function getAttrVal(
    pObjectId in number,
    pAttrKey in number
  ) return varchar2
  is
    vKey varchar2(32);
    vRaw raw(32767);
    vAttrValue varchar2(128);
  begin
    -- get hash (key)
    vKey := getHashKey(pObjectId);
    -- get all block raw value
    vRaw := getCacheBlockByKey(vKey);
    --
    vAttrValue := getAttrValueFromCacheBlock(
      vRaw,
      pObjectId,
      pAttrKey
    );
    return vAttrValue;
  end getAttrVal;
  
  /*
  * Initialize attr cache
  */
  procedure initAttrCache 
  is
    vHashIter varchar2(32);
    vRaw raw(32767);
  begin
    -- Clear all cache
    clearCacheAll;
    
    for r in (
      select
        getHashKey(object_id) vHash,
        getAttrRawString(object_id, attr_key, attr_value) rawStr
      from tst.object_attribs
      order by 1
    ) loop
      -- For next hash values (new block) write in block
      if nvl(vHashIter, '-') != nvl(r.vHash, '-') then
        -- Don't try to write null var
        if utl_raw.length(vRaw) > 0 then
          setCacheBlockByKey(vHashIter, vRaw);
        end if;
        vHashIter := r.vHash;
        vRaw := r.rawStr;
      -- Or append in raw var
      else
        vRaw := vRaw||r.rawStr;
      end if;
    end loop;
    -- Write last block
    setCacheBlockByKey(vHashIter, vRaw);
  end initAttrCache;
  
end object_attribs_cache;
/


-- UTL package to be
create or replace package tst.object_attribs_utl_new as
  /*
  * Attribute getter
  */
  function getAttrValue(
    pObjectId in number, 
    pAttrKey in number
  ) return varchar2;
    
  /*
  * Attribute setter
  */ 
  procedure setAttrValue(
    pObjectId in number,
    pAttrKey in number,
    pAttrValue in varchar2
  );
end object_attribs_utl_new;
/



create or replace package body tst.object_attribs_utl_new as
  /*
  * Attribute getter
  */
  function getAttrValue(
    pObjectId in number, 
    pAttrKey in number
  ) return varchar2 is
    vAttrValue tst.object_attribs.attr_value%type;
  begin
    vAttrValue := tst.object_attribs_cache.getAttrVal(pObjectId, pAttrKey);
    if vAttrValue is null then
      <<getVal>>
      begin
        select attr_value
        into vAttrValue
        from tst.object_attribs
        where object_id = pObjectId
        and attr_key = pAttrKey;
      exception
        when no_data_found then
          vAttrValue := null;
        when too_many_rows then
          vAttrValue := 'DoubleDataError';
      end getVal;
    end if;
    return vAttrValue;
  end getAttrValue;
    
  /*
  * Attribute setter
  */ 
  procedure setAttrValue(
    pObjectId in number,
    pAttrKey in number,
    pAttrValue in varchar2
  ) is
  begin
    merge into tst.object_attribs oa
    using (select pAttrValue from dual) p
    on (oa.object_id = pObjectId and oa.attr_key = pAttrKey)
    when matched then 
      update set
      oa.attr_value = pAttrValue
    when not matched then 
      insert(object_id, attr_key, attr_value)
      values(pObjectId, pAttrKey, pAttrValue);
      
    /*
    * Important note - we change cache value here before commiting transaction and these changes can be seen by other users
    * When we need to update cache is question to discuss
    */
    tst.object_attribs_cache.setAttrVal(
      pObjectId,
      pAttrKey,
      pAttrValue
    );
  end setAttrValue;
  
begin
  -- Init cache
  tst.object_attribs_cache.initAttrCache;
end object_attribs_utl_new;
/

/*
* As we see we got problem with changing logic for doubles attr_key in one object_id
* In first version of UTL package we raise error when found more then one value
* In last version we just grab first value and return it
* That's worth noting that I'm do it intentionally
*/

-- Perfomance test
set serveroutput on;
declare
  type tAttrTable is table of tst.object_attribs%rowtype;
  vAttrTable tAttrTable;
  vValue tst.object_attribs.attr_value%type;
begin
  tst.object_attribs_cache.initAttrCache;
  /*
  * Moving all rows in memory because we need to take out of the picture physical I/O
  */
  select object_id, attr_key, null
  bulk collect into vAttrTable
  from tst.object_attribs;
  tst.timer.startTimer;
  for i in 1..vAttrTable.count loop
    vValue := tst.object_attribs_cache.getAttrVal(
      vAttrTable(i).object_id,
      vAttrTable(i).attr_key
    );
  end loop;
  tst.timer.restartTimer;
  for i in 1..vAttrTable.count loop
    select attr_value
    into vValue
    from tst.object_attribs
    where object_id = vAttrTable(i).object_id
    and attr_key = vAttrTable(i).attr_key
    fetch first 1 rows only;
  end loop;
  tst.timer.stopTimer;
end;
/*
* Results:
* Cache: +000000000 00:00:10.149000000
* SQL without index: +000000000 00:02:16.870000000
*/
-- With index 
create index tst.x_object_attribs_object_id_attr_key on tst.object_attribs(object_id, attr_key) online;
/
/*
drop index x_object_attribs_object_id_attr_key;
/
*/

set serveroutput on;
declare
  type tAttrTable is table of tst.object_attribs%rowtype;
  vAttrTable tAttrTable;
  vValue tst.object_attribs.attr_value%type;
begin
  tst.object_attribs_cache.initAttrCache;
  /*
  * Moving all rows in memory because we need to take out of the picture physical I/O
  */
  select object_id, attr_key, null
  bulk collect into vAttrTable
  from tst.object_attribs;
  tst.timer.startTimer;
  for i in 1..vAttrTable.count loop
    vValue := tst.object_attribs_utl_new.getAttrValue(
      vAttrTable(i).object_id,
      vAttrTable(i).attr_key
    );
  end loop;
  tst.timer.restartTimer;
  for i in 1..vAttrTable.count loop
    vValue := tst.object_attribs_utl.getAttrValue(
      vAttrTable(i).object_id,
      vAttrTable(i).attr_key
    );
  end loop;
  tst.timer.stopTimer;
end;

/*
* Results:
* Cache: +000000000 00:00:09.849000000
* SQL with index: +000000000 00:00:01.289000000
*/

/*
* Conclusion: 
* For some configuration using indexes is unifficeint in terms space (index on big tables).
* For that reasons you can use Result Cache that can store anyting in memory with that approach.
* These method can exchange your server CPU usage for memory usage.
*/
