insert into ${dwh4dm_scm}.${trnst_rpostate_tbl}
(
	rpobarcode_ccode,
	rpostate_ncode,
	status_msk_date,
    status_change_msk_dts,
    status_change_local_dts,
    status_change_local_timezone,
    opsindex_ccode,
    country_ccode,
 	scenario,
	last_oper_date,
	report_date
)
select 
	rpobarcode_ccode,
	case 
		when scenario in ('inObj_noExitOpers', 'inObj_lastOper-Enter', 'inObj_ExitSameBorder') then 2
		when scenario in ('LostRpo', 'inWay_noValidExit', 'inWay_ValidExit') then 1
		when scenario = 'unknown' then -1
		else 0
	end as rpostate_ncode,
	case
		when scenario in ('inObj_noExitOpers', 'inObj_lastOper-Enter', 'inObj_ExitSameBorder') then date(f_msk_dts)
		when scenario in ('inWay_noValidExit', 'inWay_ValidExit') then date(isValidExit_msk_dts)
		when scenario = 'LostRpo' then date(LostOperation_msk_dts)
		when scenario = 'unknown' then date(LastExNowhOp_msk_dts)
		else null
	end as status_msk_date,
	case
		when scenario in ('inObj_noExitOpers', 'inObj_lastOper-Enter', 'inObj_ExitSameBorder') then f_msk_dts
		when scenario in ('inWay_noValidExit', 'inWay_ValidExit') then operWithIndexInWay_msk_dts
		when scenario = 'LostRpo' then Null
		when scenario = 'unknown' then LastExNowhOp_msk_dts 
		else null
	end as status_change_msk_dts,
	case
		when scenario in ('inObj_noExitOpers', 'inObj_lastOper-Enter', 'inObj_ExitSameBorder') then f_local_dts
		when scenario in ('inWay_noValidExit', 'inWay_ValidExit') then operWithIndexInWay_local_dts
		when scenario = 'LostRpo' then Null
		when scenario = 'unknown' then LastExNowhOp_local_dts 
		else null
	end as status_change_local_dts,
	case
		when scenario in ('inObj_noExitOpers', 'inObj_lastOper-Enter', 'inObj_ExitSameBorder') then f_local_timezone
		when scenario in ('inWay_noValidExit', 'inWay_ValidExit') then operWithIndexInWay_local_timezone
		when scenario = 'LostRpo' then Null
		when scenario = 'unknown' then LastExNowhOp_local_timezone 
		else null
	end as status_change_timezone,
	case 
		when scenario in ('inObj_noExitOpers', 'inObj_lastOper-Enter', 'inObj_ExitSameBorder') then f_objectborder_index_ccode
		when scenario in ('inWay_noValidExit', 'inWay_ValidExit') then operWithIndexInWay_border_index
		when scenario = 'LostRpo' then lostrpo_border_index
		when scenario = 'unknown' then Null 
		else null
	end as opsindex_ccode, 
	case
		when scenario in ('inObj_noExitOpers', 'inObj_lastOper-Enter', 'inObj_ExitSameBorder') then f_country
		when scenario in ('inWay_noValidExit', 'inWay_ValidExit') then operWithIndexInWay_country
		when scenario = 'LostRpo' then lostrpo_country
		when scenario = 'unknown' then Null 
		else null
	end as country_ccode,
	scenario,
	last_oper_date,
	'${report_date}'::date as report_date
from (
select 
	rpobarcode_ccode,
	case 
		when max_lastEnterOper_flg = 0 and max_isExitNowhere_flg = 0 and max_isValidExit_flg = 0 and max_EnterNetInBorderOper_flg = 0
		then 'None'
		when max_lostOperation_flg = 1 
			then 'LostRpo'
		when max_isValidExit_flg = 0 and max_isExitNowhere_flg = 0
			then 'inObj_noExitOpers'
		when max_isValidExit_flg = 0 and max_isExitNowhere_flg = 1
			and (max_lastEnterOper_flg = 0 or (lastEnterOper_msk_dts <= isExitNowhere_msk_dts))
			and max_operWithIndexInWay_rank > 0
			then 'inWay_noValidExit'
		when max_isValidExit_flg = 0 and max_isExitNowhere_flg = 1
			and (max_lastEnterOper_flg = 0 or (lastEnterOper_msk_dts <= isExitNowhere_msk_dts))
			and max_operWithIndexInWay_rank = 0
			then 'unknown'
		when max_isValidExit_flg = 0 and max_isExitNowhere_flg = 1
			and (max_lastEnterOper_flg = 1 and (lastEnterOper_msk_dts > isExitNowhere_msk_dts))
			then 'inObj_lastOper-Enter'
		when max_isValidExit_flg = 1 and max_isExitNowhere_flg = 0
			and (max_lastEnterOper_flg = 0 or (lastEnterOper_msk_dts <= isValidExit_msk_dts))
			and isValidExit_next_index is not NULL 
			and isValidExit_next_index <> isValidExit_objectborder
			then 'inWay_ValidExit'
		when max_isValidExit_flg = 1 and max_isExitNowhere_flg = 1
			and (max_lastEnterOper_flg = 0 or (lastEnterOper_msk_dts <= isValidExit_msk_dts) or (lastEnterOper_msk_dts <= isExitNowhere_msk_dts))
			and isValidExit_next_index is not NULL 
			and isValidExit_next_index <> isValidExit_objectborder
			then 'inWay_ValidExit'
		when max_isValidExit_flg = 1
			then 'inObj_ExitSameBorder'
	end as scenario,	
--  f_opers
	f_local_dts,
	f_msk_dts,
	f_country,
	f_objectborder_index_ccode,
	f_local_timezone,	
-- 	operWithIndexInWay
	case 
		when max_operWithIndexInWay_rank > 0
		then first_value(local_dts) over (partition by rpobarcode_ccode order by operWithIndexInWay_rank DESC, msk_dts DESC)
		else null
	end as operWithIndexInWay_local_dts,
	case 
		when max_operWithIndexInWay_rank > 0
		then first_value(msk_dts) over (partition by rpobarcode_ccode order by operWithIndexInWay_rank DESC, msk_dts DESC)
		else null
	end as operWithIndexInWay_msk_dts,
	case 
		when max_operWithIndexInWay_rank > 0
		then first_value(local_timezone) over (partition by rpobarcode_ccode order by operWithIndexInWay_rank DESC, msk_dts DESC)
		else null
	end as operWithIndexInWay_local_timezone,
	case 
		when max_operWithIndexInWay_rank > 0
		then first_value(next_border) over (partition by rpobarcode_ccode order by operWithIndexInWay_rank DESC, msk_dts DESC)
		else null
	end as operWithIndexInWay_border_index,	
	case 
		when max_operWithIndexInWay_rank > 0
		then first_value(country_ncode) over (partition by rpobarcode_ccode order by operWithIndexInWay_rank DESC, msk_dts DESC)
		else null
	end as operWithIndexInWay_country,
--  LastExNowhOp
	case 
		when max_isExitNowhere_flg = 1 
		then first_value(local_dts) over (partition by rpobarcode_ccode order by isExitNowhere_flg DESC, msk_dts DESC)
		else null
	end as LastExNowhOp_local_dts,
	case 
		when max_isExitNowhere_flg = 1 
		then first_value(msk_dts) over (partition by rpobarcode_ccode order by isExitNowhere_flg DESC, msk_dts DESC)
		else null
	end as LastExNowhOp_msk_dts,
	case 
		when max_isExitNowhere_flg = 1 
		then first_value(local_timezone) over (partition by rpobarcode_ccode order by isExitNowhere_flg DESC, msk_dts DESC)
		else null
	end as LastExNowhOp_local_timezone,
	case 
		when max_lostOperation_flg = 1
		then first_value(msk_dts) over (partition by rpobarcode_ccode order by lostOperation_flg DESC, msk_dts DESC)
		else null
	end as LostOperation_msk_dts,
	case 
		when max_lastGroupedOper_rank > 0
		then first_value(country_ncode) over (partition by rpobarcode_ccode order by lastGroupedOper_rank DESC)
		when max_operWithIndexInWay_rank > 0
		then first_value(country_ncode) over (partition by rpobarcode_ccode order by operWithIndexInWay_rank DESC, msk_dts DESC)
		else null
	end as lostrpo_country,
	case 
		when max_lastGroupedOper_rank > 1
		then first_value(objectborder_index_ccode) over (partition by rpobarcode_ccode order by lastGroupedOper_rank DESC)
		when max_lastGroupedOper_rank = 1 and indexnext_border is not null
		then first_value(indexnext_border) over (partition by rpobarcode_ccode order by lastGroupedOper_rank DESC)
		when max_operWithIndexInWay_rank > 0
		then first_value(next_border) over (partition by rpobarcode_ccode order by operWithIndexInWay_rank DESC, msk_dts DESC)
		else NULL
	end as lostrpo_border_index,
	isValidExit_msk_dts,
	last_oper_date
from (
select *,
	max(lostOperation_flg) over (partition by rpobarcode_ccode) as max_lostOperation_flg,
	max(isValidExit_flg) over (partition by rpobarcode_ccode) as max_isValidExit_flg,
	max(isExitNowhere_flg) over (partition by rpobarcode_ccode) as max_isExitNowhere_flg,
	max(lastEnterOper_flg) over (partition by rpobarcode_ccode) as max_lastEnterOper_flg,
	max(EnterNetInBorderOper_flg) over (partition by rpobarcode_ccode) as max_EnterNetInBorderOper_flg,
	first_value(msk_dts) over (partition by rpobarcode_ccode order by lastEnterOper_flg DESC, msk_dts DESC) as lastEnterOper_msk_dts,
	first_value(msk_dts) over (partition by rpobarcode_ccode order by isExitNowhere_flg DESC, msk_dts DESC) as isExitNowhere_msk_dts,
	first_value(msk_dts) over (partition by rpobarcode_ccode order by isValidExit_flg DESC, msk_dts DESC) as isValidExit_msk_dts,
	first_value(objectborder_index_ccode) over (partition by rpobarcode_ccode order by isValidExit_flg DESC, msk_dts DESC) as isValidExit_objectborder,
	first_value(next_index) over (partition by rpobarcode_ccode order by isValidExit_flg DESC, msk_dts DESC) as isValidExit_next_index,
	max(operWithIndexInWay_rank) over (partition by rpobarcode_ccode) as max_operWithIndexInWay_rank,
	max(lastGroupedOper_rank) over (partition by rpobarcode_ccode) as max_lastGroupedOper_rank
from ${dwh4dm_scm}.${trnst_60days_opers_step3}
) firstlvl
) secondlvl
--GROUP BY rpobarcode_ccode
limit 1 over (partition by rpobarcode_ccode order by rpobarcode_ccode);