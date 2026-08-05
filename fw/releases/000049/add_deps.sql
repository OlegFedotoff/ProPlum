delete from fw.dependencies d where object_id = 741 and d.object_id_depend = 691;

INSERT INTO fw.dependencies
(object_id, object_id_depend)
VALUES(755, 691);