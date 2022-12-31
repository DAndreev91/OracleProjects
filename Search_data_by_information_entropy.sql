/*
* Let's review some string data search problem. 
* Suppose we've got a table with some movie heroes catch phrases and someone trying to find phrase with only few words they remember.
* 
* For this task I'll use information content presentation from information theory entropy: 
*   E = -log(p(E)),
* where p(E) - probability of event E
*
* So let's try to write the function that returns best assumption on phrases.
*/

set serveroutput on;
create user tsts identified by tsts;
alter user tsts quota 1024M on users;

/*
* Create table with data
*/
CREATE TABLE "TSTS"."CATCH_PHRASES" 
   (	"ID" NUMBER(5,0), 
	"CATCH_PHRASE" VARCHAR2(1024 BYTE), 
	"CHARACTER" VARCHAR2(128 BYTE), 
	"ACTOR_ACTRESS" VARCHAR2(128 BYTE), 
	"FILM" VARCHAR2(128 BYTE), 
	"YEAR" NUMBER(6,0)
   );
REM INSERTING into TSTS.CATCH_PHRASES
SET DEFINE OFF;

/*
* Then we need to "weight" our phrases. So we use nice oracle feature - context policy, divide data by words and measure them.
* Let's create temp table with just words from catch phrases. And another with weight for each word.
*/

create table tsts.catch_phrases_words(
  catch_phrase_id number,
  catch_phrase_word varchar2(128)
);
/

create table tsts.catch_phrases_words_weight(
  catch_phrase_word varchar2(128),
  catch_phrase_word_weight number
);
/

/*
* Create context policy for normilize/prepare data
*/

begin
  ctxsys.ctx_ddl.create_preference('TSTS.WORD_LEXER', 'BASIC_LEXER');
  ctxsys.ctx_ddl.set_attribute('TSTS.WORD_LEXER', 'printjoins', q'[/\]');
  ctxsys.ctx_ddl.set_attribute('TSTS.WORD_LEXER', 'punctuations', q'[.?!,-]');
  
  ctxsys.ctx_ddl.create_stoplist('TSTS.WORD_STOPLIST', 'BASIC_STOPLIST');
  
  ctxsys.ctx_ddl.create_preference('TSTS.WORD_WORDLIST', 'BASIC_WORDLIST');
  
  ctxsys.ctx_ddl.create_policy(
    'TSTS.WORD_POLICY',
    null,
    null,
    'TSTS.WORD_LEXER',
    'TSTS.WORD_STOPLIST',
    'TSTS.WORD_WORDLIST'
  );
end;

select * from CTXSYS.CTX_INDEXES where idx_name = 'WORD_POLICY';
/*
* Now create dml package for catch_phrases
*/

create or replace package tsts.catch_phrases_dml 
as
  type catch_phrase_tab is table of tsts.catch_phrases%rowtype;
  type words_tab is table of tsts.catch_phrases_words.catch_phrase_word%type;
  type id_tab is table of pls_integer;
  
  -- setters
  procedure insert_batch(pBatch in catch_phrase_tab);
  procedure delete_batch(pBatch in catch_phrase_tab);
  procedure update_batch(pBatch in catch_phrase_tab);
  
  -- getter only for completeness (doesn't need for our little project)
  function get_batch(pID in id_tab) return catch_phrase_tab;
  
end catch_phrases_dml;
/



create or replace package body tsts.catch_phrases_dml
as
  /*
  * Variable for storing all changed words in one transaction
  */
  type tChangedWords is table of pls_integer index by varchar2(128);
  vChangedWords tChangedWords;
  vAllPhraseCnt number;

  
  function getNewId return pls_integer is
    vNewId pls_integer;
  begin
    -- We can get new id through logical call or sequence
    select max(id)+1
    into vNewId
    from tsts.catch_phrases;
    return vNewId;
  end getNewId;
  
  
  function getTokens(pString in tsts.catch_phrases.catch_phrase%type) return ctxsys.ctx_doc.token_tab is
    vTokens ctxsys.ctx_doc.token_tab;
  begin
    /*
    * Dividing catch phrase to tokens with policy
    */
    ctxsys.ctx_doc.policy_tokens('TSTS.WORD_POLICY', pString, vTokens);
    return vTokens;
  end getTokens;
  
  
  procedure insert_words(
    pId in tsts.catch_phrases.id%type,
    pCatchPhrase in tsts.catch_phrases.catch_phrase%type
  ) is
    vCatchPhraseWords ctxsys.ctx_doc.token_tab;
  begin
    /*
    * Get tokens
    */
    vCatchPhraseWords := getTokens(pCatchPhrase);
    /*
    * Place tokens into collection if not already placed and write to table
    */
    for i in 1..vCatchPhraseWords.count loop
      if not vChangedWords.exists(vCatchPhraseWords(i).token) then
        vChangedWords(vCatchPhraseWords(i).token) := 1;
      end if;
      insert into tsts.catch_phrases_words
      values (pId, vCatchPhraseWords(i).token);
    end loop;
  end insert_words;
  
  
  procedure delete_words(
    pId in tsts.catch_phrases.id%type,
    pCatchPhrase in tsts.catch_phrases.catch_phrase%type
  ) is
  begin
    /*
    * Place old words into collection if not already placed and delete them
    */
    for r in (
      select catch_phrase_word word
      from tsts.catch_phrases_words
      where catch_phrase_id = pId
    ) loop
      if not vChangedWords.exists(r.word) then
        vChangedWords(r.word) := 1;
      end if;
    end loop;
    delete from tsts.catch_phrases_words
    where catch_phrase_id = pId;
  end delete_words;
  
  
  procedure update_words(
    pId in tsts.catch_phrases.id%type,
    pCatchPhrase in tsts.catch_phrases.catch_phrase%type
  ) is
    vWordsOld words_tab;
    vWordsNew words_tab;
    vCatchPhraseWords ctxsys.ctx_doc.token_tab;
    vWordsDiff words_tab := words_tab();
  begin
    select catch_phrase_word word
    bulk collect into vWordsOld
    from tsts.catch_phrases_words
    where catch_phrase_id = pId;
    
    vCatchPhraseWords := getTokens(pCatchPhrase);
    
    -- cast token_tab to words_tab for multiset operations
    select w.token
    bulk collect into vWordsNew
    from table(vCatchPhraseWords) w;
    
    -- Words that present only in old collection (without duplicates) must be deleted
    vWordsDiff := vWordsOld multiset except all vWordsNew;
    forall i in 1..vWordsDiff.count
      delete from tsts.catch_phrases_words w
      where w.rowid = (
        select rowid 
        from tsts.catch_phrases_words 
        where catch_phrase_id = pId
        and catch_phrase_word = vWordsDiff(i)
        and rownum <= 1
      );
    -- Place old words into collection if not already placed
    for i in 1..vWordsDiff.count loop
      if not vChangedWords.exists(vWordsDiff(i)) then
        vChangedWords(vWordsDiff(i)) := 1;
      end if;
    end loop;
      
    -- Words that present only in new collection (without duplicates) must be inserted
    vWordsDiff := vWordsNew multiset except all vWordsOld;
    forall i in 1..vWordsDiff.count
      insert into tsts.catch_phrases_words w
      values(pId, vWordsDiff(i));
    -- Place new words into collection if not already placed
    for i in 1..vWordsDiff.count loop
      if not vChangedWords.exists(vWordsDiff(i)) then
        vChangedWords(vWordsDiff(i)) := 1;
      end if;
    end loop;
  end update_words;
  
  
  procedure calculateChangedWordsWeight is
    vWord tsts.catch_phrases.catch_phrase%type;
    vWordsCnt number;
  begin
    vWord := vChangedWords.first;
    while vWord is not null loop
      -- We counting words group by phrases (without duplicates within one phrase)
      select count(1)
      into vWordsCnt
      from (
        select catch_phrase_id
        from tsts.catch_phrases_words
        where catch_phrase_word = vWord
        group by catch_phrase_id
      );
      
      -- After that we change weight of words as log(2, 1/(vWordsCnt/vAllPhraseCnt)) 
      if vWordsCnt = 0 then
        delete from tsts.catch_phrases_words_weight
        where catch_phrase_word = vWord;
      else
        merge into tsts.catch_phrases_words_weight w
        using (select vWord as vWord from dual) v
        on (v.vWord = w.catch_phrase_word)
        when matched then
          update 
          set catch_phrase_word_weight = round(-log(2, vWordsCnt/vAllPhraseCnt), 2)
        when not matched then
          insert(w.catch_phrase_word, w.catch_phrase_word_weight)
          values(v.vWord, round(-log(2, vWordsCnt/vAllPhraseCnt), 2));
      end if;
      
      vWord := vChangedWords.next(vWord);
    end loop;
  end calculateChangedWordsWeight;
  
  
  procedure insert_row(pRow in tsts.catch_phrases%rowtype) is
    vId pls_integer;
  begin
    vId := case when pRow.id is null then getNewId else pRow.id end;
    
    insert into tsts.catch_phrases
    values(vId, pRow.catch_phrase, pRow.character, pRow.actor_actress, pRow.film, pRow.year);
    
    insert_words(vId, pRow.catch_phrase);
  end insert_row;
  
  
  procedure delete_row(pRow in tsts.catch_phrases%rowtype) is
  begin
    delete from tsts.catch_phrases
    where id = pRow.id;
    
    delete_words(pRow.id, pRow.catch_phrase);
  end delete_row;
  
  
  procedure update_row(pRow in tsts.catch_phrases%rowtype) is
    vOldCatchPhrase tsts.catch_phrases.catch_phrase%type;
  begin
    select catch_phrase
    into vOldCatchPhrase
    from tsts.catch_phrases
    where id = pRow.id;
  
    update tsts.catch_phrases
    set row = pRow
    where id = pRow.id;
    
    /*
    * If catchphrase changes then we need to recalc weight for words
    */
    if nvl(vOldCatchPhrase, '-') != nvl(pRow.catch_phrase, '-') then
      update_words(pRow.id, pRow.catch_phrase);
    end if;
  exception
    when no_data_found then
      /*
      * Log here and throw user exception
      */
      raise;
  end update_row;
  
  
  procedure insert_batch(pBatch in catch_phrase_tab) is
  begin
  
    vAllPhraseCnt := vAllPhraseCnt + pBatch.count;
  
    /*
    * Supposse that we doesn't need here top perfomance so we don't use forall
    */ 
    for i in 1..pBatch.count loop
      insert_row(pBatch(i));
    end loop;
    
    calculateChangedWordsWeight;
      
  end insert_batch;
  
  
  procedure delete_batch(pBatch in catch_phrase_tab) is
  begin
  
    vAllPhraseCnt := vAllPhraseCnt - pBatch.count;
  
    for i in 1..pBatch.count loop
      delete_row(pBatch(i));
    end loop;
    
    calculateChangedWordsWeight;
      
  end delete_batch;
  
  
  procedure update_batch(pBatch in catch_phrase_tab) is
  begin
  
    for i in 1..pBatch.count loop
      update_row(pBatch(i));
    end loop;
      
    calculateChangedWordsWeight;
      
  end update_batch;
  
  
  -- Dummy function
  function get_batch(pID in id_tab) return catch_phrase_tab
  is
  begin
    return null;
  end get_batch;
  
begin
  select count(1)
  into vAllPhraseCnt
  from tsts.catch_phrases;
end catch_phrases_dml;
/

-- Replace data (task completed in 0,354 seconds)
declare
  vPhraseCollection tsts.catch_phrases_dml.catch_phrase_tab;
begin
  delete from tsts.catch_phrases;
  delete from tsts.catch_phrases_words;
  delete from tsts.catch_phrases_words_weight;
  with tmp as (
    select '1' id,'"Frankly, my dear, I don''t give a damn."' catch_phrase,'Rhett Butler' character,'Clark Gable' actor_actress,'Gone with the Wind' film,'1939' year from dual union
    select '2','"I''m gonna make him an offer he can''t refuse."','Vito Corleone','Marlon Brando','The Godfather','1972' from dual union
    select '3','"You don''t understand! I coulda had class. I coulda been a contender. I could''ve been somebody, instead of a bum, which is what I am."[4]','Terry Malloy','Marlon Brando','On the Waterfront','1954' from dual union
    select '4','"Toto, I''ve a feeling we''re not in Kansas anymore."[5]','Dorothy Gale','Judy Garland','The Wizard of Oz','1939' from dual union
    select '5','"Here''s looking at you, kid."','Rick Blaine','Humphrey Bogart','Casablanca','1942' from dual union
    select '6','"Go ahead, make my day."','Harry Callahan','Clint Eastwood','Sudden Impact[6]','1983' from dual union
    select '7','"All right, Mr. DeMille, I''m ready for my close-up."','Norma Desmond','Gloria Swanson','Sunset Boulevard','1950' from dual union
    select '8','"May the Force be with you."','Han Solo','Harrison Ford','Star Wars','1977' from dual union
    select '9','"Fasten your seatbelts. It''s going to be a bumpy night."','Margo Channing','Bette Davis','All About Eve','1950' from dual union
    select '10','"You talkin'' to me?"','Travis Bickle','Robert De Niro','Taxi Driver','1976' from dual union
    select '11','"What we''ve got here is failure to communicate."[7]','Captain','Strother Martin','Cool Hand Luke','1967' from dual union
    select '12','"I love the smell of napalm in the morning."','Lt. Col. Bill Kilgore','Robert Duvall','Apocalypse Now','1979' from dual union
    select '13','"Love means never having to say you''re sorry."','Jennifer Cavalleri, Oliver Barrett IV','Ali MacGraw Ryan O''Neal','Love Story','1970' from dual union
    select '14','"The stuff that dreams are made of."[8]','Sam Spade','Humphrey Bogart','The Maltese Falcon','1941' from dual union
    select '15','"E.T. phone home."','E.T.','Pat Welsh','E.T. the Extra-Terrestrial','1982' from dual union
    select '16','"They call me Mister Tibbs!"','Virgil Tibbs','Sidney Poitier','In the Heat of the Night','1967' from dual union
    select '17','"Rosebud."','Charles Foster Kane','Orson Welles','Citizen Kane','1941' from dual union
    select '18','"Made it, Ma! Top of the world!"','Arthur "Cody" Jarrett','James Cagney','White Heat','1949' from dual union
    select '19','"I''m as mad as hell, and I''m not going to take this anymore!"','Howard Beale','Peter Finch','Network','1976' from dual union
    select '20','"Louis, I think this is the beginning of a beautiful friendship."','Rick Blaine','Humphrey Bogart','Casablanca','1942' from dual union
    select '21','"A census taker once tried to test me. I ate his liver with some fava beans and a nice Chianti."','Hannibal Lecter','Anthony Hopkins','The Silence of the Lambs','1991' from dual union
    select '22','"Bond. James Bond."','James Bond','Sean Connery[9]','Dr. No[10]','1962' from dual union
    select '23','"There''s no place like home."[11]','Dorothy Gale','Judy Garland','The Wizard of Oz','1939' from dual union
    select '24','"I am big! It''s the pictures that got small."','Norma Desmond','Gloria Swanson','Sunset Boulevard','1950' from dual union
    select '25','"Show me the money!"','Rod Tidwell','Cuba Gooding Jr.','Jerry Maguire','1996' from dual union
    select '26','"Why don''t you come up sometime and see me?"[12]','Lady Lou','Mae West','She Done Him Wrong','1933' from dual union
    select '27','"I''m walkin'' here! I''m walkin'' here!"[13]','"Ratso" Rizzo','Dustin Hoffman','Midnight Cowboy','1969' from dual union
    select '28','"Play it, Sam. Play ''As Time Goes By.''"[14]','Ilsa Lund','Ingrid Bergman','Casablanca','1942' from dual union
    select '29','"You can''t handle the truth!"','Col. Nathan R. Jessup','Jack Nicholson','A Few Good Men','1992' from dual union
    select '30','"I want to be alone."','Grusinskaya','Greta Garbo','Grand Hotel','1932' from dual union
    select '31','"After all, tomorrow is another day!"','Scarlett O''Hara','Vivien Leigh','Gone with the Wind','1939' from dual union
    select '32','"Round up the usual suspects."','Capt. Louis Renault','Claude Rains','Casablanca','1942' from dual union
    select '33','"I''ll have what she''s having."','Customer','Estelle Reiner','When Harry Met Sally...','1989' from dual union
    select '34','"You know how to whistle, don''t you, Steve? You just put your lips together and blow."','Marie "Slim" Browning','Lauren Bacall','To Have and Have Not','1944' from dual union
    select '35','"You''re gonna need a bigger boat."[15]','Martin Brody','Roy Scheider','Jaws','1975' from dual union
    select '36','"Badges? We ain''t got no badges! We don''t need no badges! I don''t have to show you any stinking badges!"[16]','Gold Hat','Alfonso Bedoya','The Treasure of the Sierra Madre','1948' from dual union
    select '37','"I''ll be back"','The Terminator','Arnold Schwarzenegger','The Terminator','1984' from dual union
    select '38','"Today, I consider myself the luckiest man on the face of the Earth."[17]','Lou Gehrig','Gary Cooper','The Pride of the Yankees','1942' from dual union
    select '39','"If you build it, he will come."','Shoeless Joe Jackson','Ray Liotta (voice)','Field of Dreams','1989' from dual union
    select '40','"Mama always said life was like a box of chocolates. You never know what you''re gonna get."','Forrest Gump','Tom Hanks','Forrest Gump','1994' from dual union
    select '41','"We rob banks."','Clyde Barrow','Warren Beatty','Bonnie and Clyde','1967' from dual union
    select '42','"Plastics."','Mr. Maguire','Walter Brooke','The Graduate','1967' from dual union
    select '43','"We''ll always have Paris."','Rick Blaine','Humphrey Bogart','Casablanca','1942' from dual union
    select '44','"I see dead people."','Cole Sear','Haley Joel Osment','The Sixth Sense','1999' from dual union
    select '45','"Stella! Hey, Stella!"','Stanley Kowalski','Marlon Brando','A Streetcar Named Desire','1951' from dual union
    select '46','"Oh, Jerry, don''t let''s ask for the moon. We have the stars."','Charlotte Vale','Bette Davis','Now, Voyager','1942' from dual union
    select '47','"Shane. Shane. Come back!"','Joey Starrett','Brandon De Wilde','Shane','1953' from dual union
    select '48','"Well, nobody''s perfect."','Osgood Fielding III','Joe E. Brown','Some Like It Hot','1959' from dual union
    select '49','"It''s alive! It''s alive!"','Henry Frankenstein','Colin Clive','Frankenstein','1931' from dual union
    select '50','"Houston, we have a problem."[18]','Jim Lovell','Tom Hanks','Apollo 13','1995' from dual union
    select '51','"You''ve got to ask yourself one question: ''Do I feel lucky?'' Well, do ya, punk?"','Harry Callahan','Clint Eastwood','Dirty Harry','1971' from dual union
    select '52','"You had me at ''hello.''"','Dorothy Boyd','Ren?e Zellweger','Jerry Maguire','1996' from dual union
    select '53','"One morning I shot an elephant in my pajamas. How he got in my pajamas, I don''t know."[19]','Capt. Geoffrey T. Spaulding','Groucho Marx','Animal Crackers','1930' from dual union
    select '54','"There''s no crying in baseball!"','Jimmy Dugan','Tom Hanks','A League of Their Own','1992' from dual union
    select '55','"La-dee-da, la-dee-da."','Annie Hall','Diane Keaton','Annie Hall','1977' from dual union
    select '56','"A boy''s best friend is his mother."','Norman Bates','Anthony Perkins','Psycho','1960' from dual union
    select '57','"Greed, for lack of a better word, is good."[20]','Gordon Gekko','Michael Douglas','Wall Street','1987' from dual union
    select '58','"Keep your friends close, but your enemies closer."[21]','Michael Corleone','Al Pacino','The Godfather Part II','1974' from dual union
    select '59','"As God is my witness, I''ll never be hungry again."','Scarlett O''Hara','Vivien Leigh','Gone with the Wind','1939' from dual union
    select '60','"Well, here''s another nice mess you''ve gotten me into!"[22]','Oliver','Oliver Hardy','Sons of the Desert','1933' from dual union
    select '61','"Say ''hello'' to my little friend!"','Tony Montana','Al Pacino','Scarface','1983' from dual union
    select '62','"What a dump."[23]','Rosa Moline','Bette Davis','Beyond the Forest','1949' from dual union
    select '63','"Mrs. Robinson, you''re trying to seduce me. Aren''t you?"','Benjamin Braddock','Dustin Hoffman','The Graduate','1967' from dual union
    select '64','"Gentlemen, you can''t fight in here! This is the War Room!"','President Merkin Muffley','Peter Sellers','Dr. Strangelove','1964' from dual union
    select '65','"Elementary, my dear Watson."[24]','Sherlock Holmes','Basil Rathbone','The Adventures of Sherlock Holmes','1939' from dual union
    select '66','"Take your stinking paws off me, you damned dirty ape."','George Taylor','Charlton Heston','Planet of the Apes','1968' from dual union
    select '67','"Of all the gin joints in all the towns in all the world, she walks into mine."','Rick Blaine','Humphrey Bogart','Casablanca','1942' from dual union
    select '68','"Here''s Johnny!"[25]','Jack Torrance','Jack Nicholson','The Shining','1980' from dual union
    select '69','"They''re here!"','Carol Anne Freeling','Heather O''Rourke','Poltergeist','1982' from dual union
    select '70','"Is it safe?"','Dr. Christian Szell','Laurence Olivier','Marathon Man','1976' from dual union
    select '71','"Wait a minute, wait a minute. You ain''t heard nothin'' yet!"[26]','Jakie Rabinowitz/Jack Robin','Al Jolson','The Jazz Singer','1927' from dual union
    select '72','"No wire hangers, ever!"[27]','Joan Crawford','Faye Dunaway','Mommie Dearest','1981' from dual union
    select '73','"Mother of mercy, is this the end of Rico?"','Rico Bandello','Edward G. Robinson','Little Caesar','1931' from dual union
    select '74','"Forget it, Jake, it''s Chinatown."','Lawrence Walsh','Joe Mantell','Chinatown','1974' from dual union
    select '75','"I have always depended on the kindness of strangers."','Blanche DuBois','Vivien Leigh','A Streetcar Named Desire','1951' from dual union
    select '76','"Hasta la vista, baby."','The Terminator','Arnold Schwarzenegger','Terminator 2: Judgment Day','1991' from dual union
    select '77','"Soylent Green is people!"','Det. Robert Thorn','Charlton Heston','Soylent Green','1973' from dual union
    select '78','"Open the pod bay doors, HAL."','Dave Bowman','Keir Dullea','2001: A Space Odyssey','1968' from dual union
    select '79','Striker: "Surely you can''t be serious." Rumack: "I am serious … and don''t call me Shirley."','Ted Striker and Dr. Rumack','Robert Hays and Leslie Nielsen','Airplane!','1980' from dual union
    select '80','"Yo, Adrian!"','Rocky Balboa','Sylvester Stallone','Rocky','1976' from dual union
    select '81','"Hello, gorgeous."','Fanny Brice','Barbra Streisand','Funny Girl','1968' from dual union
    select '82','"Toga! Toga!"','John "Bluto" Blutarsky','John Belushi','National Lampoon''s Animal House','1978' from dual union
    select '83','"Listen to them. Children of the night. What music they make."','Count Dracula','Bela Lugosi','Dracula','1931' from dual union
    select '84','"Oh, no, it wasn''t the airplanes. It was Beauty killed the Beast."[28]','Carl Denham','Robert Armstrong','King Kong','1933' from dual union
    select '85','"My precious."','Gollum','Andy Serkis','The Lord of the Rings: The Two Towers','2002' from dual union
    select '86','"Attica! Attica!"','Sonny Wortzik','Al Pacino','Dog Day Afternoon','1975' from dual union
    select '87','"Sawyer, you''re going out a youngster, but you''ve got to come back a star!"','Julian Marsh','Warner Baxter','42nd Street','1933' from dual union
    select '88','"Listen to me, mister. You''re my knight in shining armor. Don''t you forget it. You''re going to get back on that horse, and I''m going to be right behind you, holding on tight, and away we''re gonna go, go, go!"','Ethel Thayer','Katharine Hepburn','On Golden Pond','1981' from dual union
    select '89','"Tell ''em to go out there with all they got and win just one for the Gipper."','George Gipp','Ronald Reagan','Knute Rockne, All American','1940' from dual union
    select '90','"A martini. Shaken, not stirred."[29]','James Bond','Sean Connery[9]','Goldfinger[30]','1964' from dual union
    select '91','"Who''s on first."[31]','Dexter','Bud Abbott','The Naughty Nineties','1945' from dual union
    select '92','"Cinderella story. Outta nowhere. A former greenskeeper, now, about to become the Masters champion. It looks like a mirac...It''s in the hole! It''s in the hole! It''s in the hole!"','Carl Spackler','Bill Murray','Caddyshack','1980' from dual union
    select '93','"Life is a banquet, and most poor suckers are starving to death!"','Mame Dennis','Rosalind Russell','Auntie Mame','1958' from dual union
    select '94','"I feel the need—the need for speed!"','Pete Mitchell and Nick Bradshaw','Tom Cruise and Anthony Edwards','Top Gun','1986' from dual union
    select '95','"Carpe diem. Seize the day, boys. Make your lives extraordinary."','John Keating','Robin Williams','Dead Poets Society','1989' from dual union
    select '96','"Snap out of it!"','Loretta Castorini','Cher','Moonstruck','1987' from dual union
    select '97','"My mother thanks you. My father thanks you. My sister thanks you. And I thank you."[32]','George M. Cohan','James Cagney','Yankee Doodle Dandy','1942' from dual union
    select '98','"Nobody puts Baby in a corner."','Johnny Castle','Patrick Swayze','Dirty Dancing','1987' from dual union
    select '99','"I''ll get you, my pretty, and your little dog too!"','Wicked Witch of the West','Margaret Hamilton','The Wizard of Oz','1939' from dual union
    select '100','"I''m the king of the world!"[33]','Jack Dawson','Leonardo DiCaprio','Titanic','1997' from dual
  )
  select * 
  bulk collect into vPhraseCollection
  from tmp 
  order by to_number(id);
  
  tsts.catch_phrases_dml.insert_batch(vPhraseCollection);
  commit;
end;
/


/*
* Now we need to create function that search phrases by summary weight of information
*/
create or replace function tsts.findSimilarPhrases(pPhrase in varchar2, pResultCount in number default 10) 
return catch_phrases_dml.catch_phrase_tab pipelined
is
  vPhraseToken ctxsys.ctx_doc.token_tab;
  vPhrase tsts.catch_phrases%rowtype;
begin
  -- Get tokens within phrase
  ctxsys.ctx_doc.policy_tokens('TSTS.WORD_POLICY', pPhrase, vPhraseToken);
  
  for r in (
    with token as (
      select t.token, w.catch_phrase_word_weight weight
      from table(vPhraseToken) t, tsts.catch_phrases_words_weight w
      where t.token = w.catch_phrase_word
    ),
    weights as (
      select p.catch_phrase_id id, p.catch_phrase_word word, t.weight 
      from token t, tsts.catch_phrases_words p
      where t.token = p.catch_phrase_word
      group by p.catch_phrase_id, p.catch_phrase_word, t.weight 
    )
    select w.id, sum(w.weight) all_weight
    from weights w
    group by w.id
    order by 2 desc
    fetch first pResultCount rows only
  ) loop
    select *
    into vPhrase
    from tsts.catch_phrases
    where id = r.id;
    pipe row (vPhrase);
  end loop;
  return;
end findSimilarPhrases;
/

-- Tests

select * from table(tsts.findSimilarPhrases('he will come'));
select * from table(tsts.findSimilarPhrases('Johnny'));

/*
* Of course in case of only 100 phrases we don't need these method. We'll use something like "matches" function with CTXRULE index.
* But in case of phrases over 1 000 000 "matches" function doesn't work very well. It misses some entryies even if they was placed in index.
*/
