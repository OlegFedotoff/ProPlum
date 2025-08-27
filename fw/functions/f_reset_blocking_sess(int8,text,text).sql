-- DROP FUNCTION fw.f_reset_blocking_sess(int8, text, text);

CREATE OR REPLACE FUNCTION fw.f_reset_blocking_sess(p_load_id int8 DEFAULT 0, p_db_name text DEFAULT 'ADWH'::text, p_role_name text DEFAULT 'airflow'::text)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	
	
	
-- Создана для освобождения заблокированных объектов захваченных сессиями других пользователей 
-- p_db_name    - name DB ( ADWH )
-- p_role_name  - логин для которого нужно освободить заблокированные объекты

declare
    rec              RECORD;
    v_location       text := 'fw.f_reset_blocking_sess';
    v_load_id        int8 := coalesce(p_load_id,0);
begin
	
	perform fw.f_write_log(p_log_type := 'WARN', 
       p_log_message := 'START reset blocking session load_id = ' || v_load_id, 
       p_location    := v_location,
       p_load_id     := v_load_id); --log function call
	
	for rec in
	     
select kl.pid as blocking_pid,ka.usename as blocking_user,ka.query as blocking_query,bl.pid as blocked_pid,a.usename as blocked_user
	,a.query as blocked_query,to_char(age(now(), a.query_start),'HH24h:MIm:SSs') as age
from pg_catalog.pg_locks bl
join pg_catalog.pg_stat_activity a
	on bl.pid = a.pid
join pg_catalog.pg_locks kl
	on bl.locktype = kl.locktype
	and bl.database is not distinct from kl.database
	and bl.relation is not distinct from kl.relation
	and bl.page is not distinct from kl.page
	and bl.tuple is not distinct from kl.tuple
	and bl.transactionid is not distinct from kl.transactionid
	and bl.classid is not distinct from kl.classid
	and bl.objid is not distinct from kl.objid
	and bl.objsubid is not distinct from kl.objsubid
	and bl.pid <> kl.pid
   join pg_catalog.pg_stat_activity ka
	 on kl.pid = ka.pid
  where kl.granted and not bl.granted
    and ka.usename <> p_role_name and a.usename = p_role_name
    and a.datname = p_db_name
    
 loop
	  perform fw.f_write_log(p_log_type := 'WARN', 
        p_log_message := 'Reset blocking pid: '||rec.blocking_pid||' blocking user: '||rec.blocking_user||' blocking query: '||rec.blocking_query||' load_id = ' || v_load_id, 
        p_location    := v_location,
        p_load_id     := v_load_id); --log function call
	 
    	execute 'SELECT pg_cancel_backend('||rec.blocking_pid||')';
    	execute 'SELECT pg_terminate_backend('||rec.blocking_pid||')';
end loop;

	perform fw.f_write_log(p_log_type := 'WARN', 
       p_log_message := 'End reset blocking session load_id = ' || v_load_id, 
       p_location    := v_location,
       p_load_id     := v_load_id); --log function call

end;






$$
EXECUTE ON ANY;