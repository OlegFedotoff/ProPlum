UPDATE fw.objects
set transform_mapping = '{"p_products": "null"}'::jsonb
where object_name = 'src_hybris.queryrecommendedproducts';
UPDATE fw.objects
set transform_mapping = null
where object_name = 'hybris.queryrecommendedproducts';