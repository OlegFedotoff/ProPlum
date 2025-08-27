-- DROP FUNCTION fw.f_drop_tab_partition_date_range(text, timestamp, timestamp, bool);

CREATE OR REPLACE FUNCTION fw.f_drop_tab_partition_date_range(p_table_name text, p_partition_start timestamp, p_partition_end timestamp, flag_drop_trunc bool DEFAULT false)
	RETURNS bool
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	

DECLARE
    rec                 record;
    v_location text := 'fw.f_drop_tab_partition_date_range';
   
/*Function drop partition of the table by date range*/
BEGIN
       perform fw.f_write_log(
        p_log_type    := 'SERVICE', 
        p_log_message := 'Start drop partitions in table ' || p_table_name ||' range from '||p_partition_start||' to '||p_partition_end, 
        p_location    := v_location); --log function call
        
  for rec in         
    select  
         partitionschemaname||'.'||partitiontablename::text as partitiontablename, 
		 split_part(partitionrangestart, '::', 1)::timestamp as partitionrangestart,
		 split_part(partitionrangeend, '::', 1)::timestamp as partitionrangeend
	from pg_partitions
    where lower(schemaname||'.'||tablename) = lower(p_table_name)
	  and partitionisdefault = false
	  and ((split_part(partitionrangestart, '::', 1)::timestamp between to_timestamp(to_char(p_partition_start, 'YYYYMMDDHH24MISS'), 'YYYYMMDDHH24MISS') and to_timestamp(to_char(p_partition_end, 'YYYYMMDDHH24MISS'), 'YYYYMMDDHH24MISS')) 
	   or ( split_part(partitionrangeend, '::', 1)::timestamp - interval '1 second' between to_timestamp(to_char(p_partition_start, 'YYYYMMDDHH24MISS'), 'YYYYMMDDHH24MISS') and to_timestamp(to_char(p_partition_end, 'YYYYMMDDHH24MISS'), 'YYYYMMDDHH24MISS')) 
	   or ( to_timestamp(to_char(p_partition_start, 'YYYYMMDDHH24MISS'), 'YYYYMMDDHH24MISS') between split_part(partitionrangestart, '::', 1)::timestamp and split_part(partitionrangeend, '::', 1)::timestamp - interval '1 second') 
	   or ( to_timestamp(to_char(p_partition_end, 'YYYYMMDDHH24MISS'), 'YYYYMMDDHH24MISS') between split_part(partitionrangestart, '::', 1)::timestamp and split_part(partitionrangeend, '::', 1)::timestamp - interval '1 second'))
    order by partitionposition
 
  -- Drop partition cycle
   loop
	 if flag_drop_trunc is true
	  then
        execute 'alter table '||p_table_name||' drop partition for ('''||rec.partitionrangestart||'''::date)';
        perform fw.f_write_log(
        p_log_type    := 'SERVICE', 
        p_log_message := 'Drop partitions ' || rec.partitiontablename ||' in table ' || p_table_name ||' range from '||rec.partitionrangestart||' to '||rec.partitionrangeend, 
        p_location    := v_location); --log function call
       else 
        execute 'truncate table '||rec.partitiontablename;
        perform fw.f_write_log(
        p_log_type    := 'SERVICE', 
        p_log_message := 'Truncate partitions ' || rec.partitiontablename ||' in table ' || p_table_name ||' range from '||rec.partitionrangestart||' to '||rec.partitionrangeend, 
        p_location    := v_location); --log function call
       end if;
   end loop;  

       perform fw.f_write_log(
        p_log_type    := 'SERVICE', 
        p_log_message := 'End drop partitions in table ' || p_table_name ||' range from '||p_partition_start||' to '||p_partition_end, 
        p_location    := v_location); --log function call
        
  return true;
 
 exception when others then 
     raise notice 'ERROR drop partition %: %',p_table_name,SQLERRM;
     return false;
END;
 



$$
EXECUTE ON ANY;