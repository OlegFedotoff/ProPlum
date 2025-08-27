-- fw.load_status_today исходный текст

CREATE OR REPLACE VIEW fw.load_status_today
AS SELECT o.load_group,
    o.object_id AS "Object Id",
    o.object_name AS "Object name",
    o.object_desc AS "Description",
    ls.load_status AS "Load status",
    li.row_cnt AS "Rows affected",
    timezone('Europe/Moscow'::text, timezone('UTC'::text, li.updated_dttm)) AS "Last update"
   FROM fw.objects o
     LEFT JOIN fw.load_info li ON o.object_id = li.object_id AND li.updated_dttm::date = 'now'::text::date
     LEFT JOIN fw.d_load_status ls ON li.load_status = ls.load_status
  WHERE (o.load_group IN ( SELECT d_load_group.load_group
           FROM fw.d_load_group)) AND o.active
  ORDER BY o.object_id;


