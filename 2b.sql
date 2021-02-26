INSERT /*+direct*/ into ${dwh4dm_scm}.${trnst_60days_opers_step2}
(
    rpobarcode_ccode,
    msk_dts,
    local_dts,
    local_timezone,
    opertype_ncode,
	operattr_ncode,
    country_ncode,
    objecttype_ncode,
    rule_ncode,
	operindex_ccode, --атрибут нужен для сверки
    objectborder_index_ccode,
	object_barcode,
    next_index,
    next_border,
	indexnext_border,
    lastGroupedOper_rank,
    lostOperation_flg,
    lastEnterOper_flg,
	tmp_oper_flg
)
select 
	b.rpobarcode_ccode,
	b.msk_dts,
	b.local_dts,
	b.local_timezone,
	b.opertype_ncode,
	b.operattr_ncode,
	b.country_ncode,
	b.objecttype_ncode,
	b.rule_ncode,
	b.operindex_ccode,  
	b.objectborder_index_ccode,
	b.object_barcode,
	case 
		when b.objecttype_ncode = 1 and b.indexnext_ccode is not null then b.indexnext_ccode
		else b.indexto_ccode 
	end as next_index,
	case 
		when b.objecttype_ncode = 1 and b.indexnext_ccode is not null then indexnext_border		
		else coalesce(po.objectborder_index_ccode, b.indexto_ccode)
	end as next_border,
	indexnext_border, 
case 
		when (first_value(msk_dts) 
		over (partition by b.rpobarcode_ccode, objecttype_ncode order by msk_dts DESC, decode(rule_ncode, 3, 2, 1, 3, 2, 4, 4, 5, 1) DESC)) = msk_dts 
		and (first_value(rule_ncode) 
		over (partition by b.rpobarcode_ccode, objecttype_ncode order by local_dts DESC, decode(rule_ncode, 3, 2, 1, 3, 2, 4, 4, 5, 1) DESC)) = rule_ncode
		and objecttype_ncode = 3
		then 2 
		when (first_value(msk_dts) 
		over (partition by b.rpobarcode_ccode, objecttype_ncode order by msk_dts DESC, decode(rule_ncode, 3, 2, 1, 3, 2, 4, 4, 5, 1) DESC)) = msk_dts 
		and (first_value(rule_ncode) 
		over (partition by b.rpobarcode_ccode, objecttype_ncode order by local_dts DESC, decode(rule_ncode, 3, 2, 1, 3, 2, 4, 4, 5, 1) DESC)) = rule_ncode 
		and objecttype_ncode = 2
		then 3
		when (first_value(msk_dts) 
		over (partition by b.rpobarcode_ccode, objecttype_ncode order by msk_dts DESC, decode(rule_ncode, 3, 2, 1, 3, 2, 4, 4, 5, 1) DESC)) = msk_dts 
		and (first_value(rule_ncode) 
		over (partition by b.rpobarcode_ccode, objecttype_ncode order by local_dts DESC, decode(rule_ncode, 3, 2, 1, 3, 2, 4, 4, 5, 1) DESC)) = rule_ncode
		and objecttype_ncode = 1
		then 1		
		else 0
	end as lastGroupedOper_rank,
    case 
		when (first_value(opertype_ncode) 
		over (partition by b.rpobarcode_ccode order by msk_dts DESC)) in (18, 1027) -- расширить условие с проверкой на тип операнда
		then 1 else 0
	end as lostOperation_flg,
	case
		when (first_value(msk_dts) 
		over (partition by b.rpobarcode_ccode order by rule_ncode, msk_dts DESC)) = msk_dts 
		and rule_ncode = 1
		then 1 else 0
	end as lastEnterOper_flg, 
	null as tmp_oper_flg
from ${dwh4dm_scm}.${trnst_60days_opers_step1} b
left join ${dict_scm}.${postobject_sdim_tbl} po
on b.indexto_ccode = po.postobject_index_ccode;
select analyze_statistics ('${dwh4dm_scm}.${trnst_60days_opers_step2}');