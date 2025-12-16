UPDATE fw.objects
set transform_mapping = '{"p_products": "null"}'::jsonb
where object_name ilike '%QUERYRECOMMENDEDPRODUCTS%'; 