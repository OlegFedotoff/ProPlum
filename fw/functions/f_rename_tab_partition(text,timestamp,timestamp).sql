-- DROP FUNCTION fw.f_rename_tab_partition(text, timestamp, timestamp);

CREATE OR REPLACE FUNCTION fw.f_rename_tab_partition(p_schema_name text, p_partition_start timestamp, p_partition_end timestamp)
	RETURNS bool
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	

DECLARE
    rec                 record;
    v_location text := 'fw.f_rename_tab_partition';
   
/* Function rename partition of the schema */
BEGIN
       perform fw.f_write_log(
        p_log_type    := 'SERVICE', 
        p_log_message := 'Start rename partitions in schema ' || p_schema_name ||' range from '||p_partition_start||' to '||p_partition_end, 
        p_location    := v_location); --log function call
        
  for rec in         

           select schemaname||'.'||tablename as  tablename, partitionname, split_part(partitionrangestart, '::', 1)::timestamp as partitionrangestart, 
                  split_part(partitionrangeend, '::', 1)::timestamp as partitionrangeend,
                  split_part(partitionrangeend, '::', 1)::timestamp  - split_part(partitionrangestart, '::', 1)::timestamp intt,
                case when split_part(partitionrangeend, '::', 1)::timestamp  - split_part(partitionrangestart, '::', 1)::timestamp between '28 days'::interval and '31 days'::interval 
                  then 'm_'|| to_char(split_part(partitionrangestart, '::', 1)::timestamp,'mm_yyyy')
                 when split_part(partitionrangeend, '::', 1)::timestamp  - split_part(partitionrangestart, '::', 1)::timestamp = '1 days'::interval 
                  then 'd_'|| to_char(split_part(partitionrangestart, '::', 1)::timestamp,'dd_mm_yyyy')
                 when split_part(partitionrangeend, '::', 1)::timestamp  - split_part(partitionrangestart, '::', 1)::timestamp between '365 days'::interval and '366 days'::interval 
                  then 'y_'||to_char(split_part(partitionrangestart, '::', 1)::timestamp,'yyyy')
                 when split_part(partitionrangeend, '::', 1)::timestamp  - split_part(partitionrangestart, '::', 1)::timestamp = '7 days'::interval 
                  then 'w_'||to_char(split_part(partitionrangestart, '::', 1)::timestamp,'www_yyyy')
                else '' end part_name
           from pg_partitions
           where schemaname = lower(p_schema_name)
           -- Rename partition only ( day, week, month, year ) interval
           and split_part(partitionrangeend, '::', 1)::timestamp  - 
               split_part(partitionrangestart, '::', 1)::timestamp in 
               ('1 days'::interval,'7 days'::interval,'28 days'::interval,'29 days'::interval,'30 days'::interval,'31 days'::interval,'365 days'::interval,'366 days'::interval)
               and partitionname = ''           
      		   and partitionisdefault = false 
      	  	   and ((split_part(partitionrangestart, '::', 1)::timestamp between to_timestamp(to_char(p_partition_start, 'YYYYMMDDHH24MISS'), 'YYYYMMDDHH24MISS') and to_timestamp(to_char(p_partition_end, 'YYYYMMDDHH24MISS'), 'YYYYMMDDHH24MISS')) 
	  		   or ( split_part(partitionrangeend, '::', 1)::timestamp - interval '1 second' between to_timestamp(to_char(p_partition_start, 'YYYYMMDDHH24MISS'), 'YYYYMMDDHH24MISS') and to_timestamp(to_char(p_partition_end, 'YYYYMMDDHH24MISS'), 'YYYYMMDDHH24MISS')) 
	   		   or ( to_timestamp(to_char(p_partition_start, 'YYYYMMDDHH24MISS'), 'YYYYMMDDHH24MISS') between split_part(partitionrangestart, '::', 1)::timestamp and split_part(partitionrangeend, '::', 1)::timestamp - interval '1 second') 
	   		   or ( to_timestamp(to_char(p_partition_end, 'YYYYMMDDHH24MISS'), 'YYYYMMDDHH24MISS') between split_part(partitionrangestart, '::', 1)::timestamp and split_part(partitionrangeend, '::', 1)::timestamp - interval '1 second'))
           order by tablename, partitionposition
 
  -- Rename partition cycle
   loop
      execute 'alter table '||rec.tablename||' rename partition for ('''||rec.partitionrangestart||'''::date) to '|| rec.part_name;
       perform fw.f_write_log(
        p_log_type    := 'SERVICE', 
        p_log_message := 'Rename partitions ' || rec.part_name ||' in table ' || rec.tablename ||' range from '||rec.partitionrangestart||' to '||rec.partitionrangeend, 
        p_location    := v_location); --log function call
   end loop;  

       perform fw.f_write_log(
        p_log_type    := 'SERVICE', 
        p_log_message := 'End rename partitions in schema ' || p_schema_name ||' range from '||p_partition_start||' to '||p_partition_end, 
        p_location    := v_location); --log function call
        
  return true;
 
 exception when others then 
     raise notice 'ERROR rename partition %: %',p_schema_name,SQLERRM;
     return false;
END;
 



$$
EXECUTE ON ANY;