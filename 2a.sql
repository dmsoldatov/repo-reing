INSERT /*+direct*/ into ${dwh4dm_scm}.${trnst_first_opers_tbl}
(	rpobarcode_ccode,
	f_local_dts,
	f_msk_dts,
	f_country,
	f_objectborder_index_ccode,
	f_local_timezone,
	tmpstorage_flag,
	return_flag,
	forwarding_flag,
	shortage_flag,
	exitnet_flag,
	enternet_msk_dts,
	enternet_local_dts,
	enternet_timezone
)
select
	rpobarcode_ccode,
	max(f_local_dts) as f_local_dts,
	max(f_msk_dts) as f_msk_dts,
	max(f_country) as f_country,
	max(f_objectborder_index_ccode) as f_objectborder_index_ccode,
	max(f_local_timezone) as f_local_timezone,
	max(tmpstorage_flag) as tmpstorage_flag,
	max(decode(return_flag,TRUE,1,FALSE,0)) as return_flag,
	max(decode(forwarding_flag,TRUE,1,FALSE,0)) as forwarding_flag,
	max(decode(shortage_flag,TRUE,1,FALSE,0)) as shortage_flag,
	max(decode(exitnet_flag,TRUE,1,FALSE,0)) as exitnet_flag,
	max(enternet_msk_dts) as enternet_msk_dts,
	max(enternet_local_dts) as enternet_local_dts,
	max(enternet_timezone) as enternet_timezone
from (
select 
	rpobarcode_ccode,
	first_value(local_dts) over (partition by rpobarcode_ccode order by firstborderoper_flg DESC, msk_dts DESC, priority DESC) as f_local_dts,
	first_value(msk_dts) over (partition by rpobarcode_ccode order by firstborderoper_flg DESC, msk_dts DESC, priority DESC) as f_msk_dts,
	first_value(country_ncode) over (partition by rpobarcode_ccode order by firstborderoper_flg DESC, msk_dts DESC, priority DESC) as f_country,
	first_value(objectborder_index_ccode) over (partition by rpobarcode_ccode order by firstborderoper_flg DESC, msk_dts DESC, priority DESC) as f_objectborder_index_ccode,
	first_value(local_timezone) over (partition by rpobarcode_ccode order by firstborderoper_flg DESC, msk_dts DESC, priority DESC) as f_local_timezone,
	tmpstorage_flag,	
	return_flag,
    forwarding_flag,
    shortage_flag,
	exitnet_flag,
    enternet_msk_dts,
    enternet_local_dts,
    enternet_timezone
from (
select 
	rpobarcode_ccode,
	msk_dts,
	local_dts,
	local_timezone,
	country_ncode,
	objectborder_index_ccode,
	decode(rule_ncode, 3, 2, 1, 3, 2, 4, 4, 5, 1) as priority,
	case 
		when nvl((lag(objectborder_index_ccode, 1, 0) 
		over (partition by rpobarcode_ccode order by msk_dts, decode(rule_ncode, 3, 2, 1, 3, 2, 4, 4, 5, 1), decode(indexnext_ccode, '', 1, 0))), 'no_Value') != nvl(objectborder_index_ccode, 'no_Value') 
		then 1 else 0
	end as firstborderoper_flg,
	BOOL_OR (rule_ncode = 3)
    	OVER(PARTITION BY b.rpobarcode_ccode) as tmpstorage_flag,
    BOOL_OR (opertype_ncode = 3)
    	OVER(PARTITION BY b.rpobarcode_ccode) as return_flag,
	BOOL_OR (opertype_ncode = 4)
    	OVER(PARTITION BY b.rpobarcode_ccode) as forwarding_flag,
	BOOL_OR ((opertype_ncode = 18 and objecttype_ncode in (1,2)) 
		OR (opertype_ncode = 1027 and objecttype_ncode in (1,2,3)) )
    	OVER(PARTITION BY b.rpobarcode_ccode) as shortage_flag,
	BOOL_OR (rule_ncode = 5)
    	OVER(PARTITION BY b.rpobarcode_ccode) as exitnet_flag,
	CASE 
    	when BOOL_OR (rule_ncode = 3) 
     		OVER(PARTITION BY b.rpobarcode_ccode) is True
     	THEN FIRST_value(msk_dts) OVER(PARTITION BY b.rpobarcode_ccode ORDER BY decode(rule_ncode,3,1,0) DESC, msk_dts)
     	else null 
    end as enternet_msk_dts,  
    CASE 
    	when BOOL_OR (rule_ncode = 3) 
     		OVER(PARTITION BY b.rpobarcode_ccode) is True
     	THEN FIRST_value(local_dts) OVER(PARTITION BY b.rpobarcode_ccode ORDER BY decode(rule_ncode,3,1,0) DESC, msk_dts)
     	else null 
    end as enternet_local_dts, 
    CASE 
    	when BOOL_OR (rule_ncode = 3) 
     		OVER(PARTITION BY b.rpobarcode_ccode) is True
     	THEN FIRST_value(local_timezone) OVER(PARTITION BY b.rpobarcode_ccode ORDER BY decode(rule_ncode,3,1,0) DESC, msk_dts)
     	else null 
    end as enternet_timezone
from ${dwh4dm_scm}.${trnst_60days_opers_step1} b
) t1
) t2
group by rpobarcode_ccode;
select analyze_statistics ('${dwh4dm_scm}.${trnst_first_opers_tbl}');