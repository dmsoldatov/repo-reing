INSERT /*+direct*/ into ${dwh4dm_scm}.${trnst_60days_opers_step3}
(
	rpobarcode_ccode,
	msk_dts,
	local_dts,
	local_timezone,
    country_ncode,
    objecttype_ncode,
    operindex_ccode,
    objectborder_index_ccode,
    next_index,
    next_border,
    indexnext_border,
    lastGroupedOper_rank,
    lostOperation_flg,
    lastEnterOper_flg,
	tmp_oper_flg,
	isValidExit_flg,
	isExitNowhere_flg,
	isValidAllOpers_flg,
	EnterNetInBorderOper_flg,
	last_oper_date, --дата последней значимой операции
	f_local_dts,
	f_msk_dts,
	f_country,
	f_objectborder_index_ccode,
	f_local_timezone,
	operWithIndexInWay_rank
)
SELECT 
	firstlvl.rpobarcode_ccode,
	firstlvl.msk_dts,
    firstlvl.local_dts,
    firstlvl.local_timezone,
	firstlvl.country_ncode,
    firstlvl.objecttype_ncode,
	firstlvl.operindex_ccode,
    firstlvl.objectborder_index_ccode,
    firstlvl.next_index,
    firstlvl.next_border,
    firstlvl.indexnext_border,
    firstlvl.lastGroupedOper_rank,
    firstlvl.lostOperation_flg,
    firstlvl.lastEnterOper_flg,
	firstlvl.tmp_oper_flg,
	firstlvl.isValidExit_flg,
	firstlvl.isExitNowhere_flg,
	firstlvl.isValidAllOpers_flg,
	firstlvl.EnterNetInBorderOper_flg,
	firstlvl.last_oper_date, --дата последней значимой операции
	firstlvl.f_local_dts,
	firstlvl.f_msk_dts,
	firstlvl.f_country,
	firstlvl.f_objectborder_index_ccode,
	firstlvl.f_local_timezone,	
	case 
		when isValidExit_flg = 1 and objecttype_ncode = 3
		and first_value(msk_dts) over (partition by rpobarcode_ccode order by isValidExit_flg DESC, objecttype_ncode DESC, msk_dts DESC) = msk_dts
		then 6
		when isValidAllOpers_flg = 1 and objecttype_ncode = 3
		and first_value(msk_dts) over (partition by rpobarcode_ccode order by isValidAllOpers_flg DESC, objecttype_ncode DESC, msk_dts DESC,
			decode(formtype_ccode, 'F23A', 1, 'F16B', 1, 'F23', 2, 'F16A', 2, 3 )) = msk_dts
		then 5
		when isValidExit_flg = 1 and objecttype_ncode = 2
		and first_value(msk_dts) over (partition by rpobarcode_ccode order by isValidExit_flg DESC, objecttype_ncode DESC, msk_dts DESC) = msk_dts
		then 4
		when isValidAllOpers_flg = 1 and objecttype_ncode = 2
		and first_value(msk_dts) over (partition by rpobarcode_ccode order by isValidAllOpers_flg DESC, objecttype_ncode DESC, msk_dts DESC) = msk_dts
		then 3
		when isValidExit_flg = 1 and objecttype_ncode = 1
		and first_value(msk_dts) over (partition by rpobarcode_ccode order by isValidExit_flg DESC, objecttype_ncode DESC, msk_dts DESC) = msk_dts
		then 2
		when isValidAllOpers_flg = 1 and objecttype_ncode = 1
		and first_value(msk_dts) over (partition by rpobarcode_ccode order by isValidAllOpers_flg DESC, objecttype_ncode DESC, msk_dts DESC) = msk_dts
		then 1
		else 0
	end as operWithIndexInWay_rank
from (
select
	b.rpobarcode_ccode,
	msk_dts,
    local_dts,
    local_timezone,
    country_ncode,
    objecttype_ncode,
    b.operindex_ccode,
    b.objectborder_index_ccode,
	b.object_barcode,
    next_index,
    next_border,
    indexnext_border,
	lastGroupedOper_rank,
	lostOperation_flg,
	lastEnterOper_flg,
    tmp_oper_flg,
    case 
		when next_index is not null and next_index <> '0' and substring(next_index,1,3) <> '901' and next_border <> f_objectborder_index_ccode 
		and msk_dts >= f_msk_dts and rule_ncode = 4
		then 1 else 0 
	end as isValidExit_flg,
	case 
		when (next_index is null or next_index = '0' or SUBSTRING(next_index,1,3) = '901') 
		and msk_dts >= f_msk_dts and rule_ncode = 4
		then 1 else 0
	end as isExitNowhere_flg,
    case 
		when next_index is not null and next_index <> '0' and substring(next_index,1,3) <> '901'
		and next_border <> f_objectborder_index_ccode and msk_dts >= f_msk_dts
		then 1 else 0 
	end as isValidAllOpers_flg,	
	case
		when rule_ncode = 3 and msk_dts >= f_msk_dts
		then 1 else 0
	end as EnterNetInBorderOper_flg,
--  дата последней значимой операции
	date(first_value(msk_dts) over (partition by b.rpobarcode_ccode order by msk_dts DESC))::varchar(20) as last_oper_date,
--	
	f_local_dts,
	f_msk_dts,
	f_country,
	f_objectborder_index_ccode,
	f_local_timezone
from ${dwh4dm_scm}.${trnst_60days_opers_step2} b 
left join ${dwh4dm_scm}.${trnst_first_opers_tbl} f 
on f.rpobarcode_ccode = b.rpobarcode_ccode
where f.exitnet_flag = 0 
) firstlvl
left join ${dwh4dm_scm}.${docbase_sdim} doc
on doc.barcode_ccode = firstlvl.object_barcode
and firstlvl.objecttype_ncode = 3
where (lostOperation_flg+lastEnterOper_flg+isValidExit_flg+isExitNowhere_flg+isValidAllOpers_flg+EnterNetInBorderOper_flg) > 0;
select analyze_statistics ('${dwh4dm_scm}.${trnst_60days_opers_step3}');