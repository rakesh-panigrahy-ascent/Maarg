select 
	base.n_distributor_id,
	base.dist_name,
	base.scan_Date,
	case 
		when base.scan_time >= (current_date||' 06:00AM')::datetime and base.scan_time < (current_date||' 07:00AM')::datetime then (date(base.scan_Date)||' 07:00AM')::datetime
		when base.scan_time >= (current_date||' 07:00AM')::datetime and base.scan_time < (current_date||' 08:00AM')::datetime then (date(base.scan_Date)||' 08:00AM')::datetime
		when base.scan_time >= (current_date||' 08:00AM')::datetime and base.scan_time < (current_date||' 09:00AM')::datetime then (date(base.scan_Date)||' 09:00AM')::datetime
		when base.scan_time >= (current_date||' 09:00AM')::datetime and base.scan_time < (current_date||' 10:00AM')::datetime then (date(base.scan_Date)||' 10:00AM')::datetime
		when base.scan_time >= (current_date||' 10:00AM')::datetime and base.scan_time < (current_date||' 11:00AM')::datetime then (date(base.scan_Date)||' 11:00AM')::datetime
		when base.scan_time >= (current_date||' 11:00AM')::datetime and base.scan_time < (current_date||' 12:00PM')::datetime then (date(base.scan_Date)||' 12:00PM')::datetime
		when base.scan_time >= (current_date||' 12:00PM')::datetime and base.scan_time < (current_date||' 01:00PM')::datetime then (date(base.scan_Date)||' 01:00PM')::datetime
		when base.scan_time >= (current_date||' 01:00PM')::datetime and base.scan_time < (current_date||' 02:00PM')::datetime then (date(base.scan_Date)||' 02:00PM')::datetime
		when base.scan_time >= (current_date||' 02:00PM')::datetime and base.scan_time < (current_date||' 03:00PM')::datetime then (date(base.scan_Date)||' 03:00PM')::datetime
		when base.scan_time >= (current_date||' 03:00PM')::datetime and base.scan_time < (current_date||' 04:00PM')::datetime then (date(base.scan_Date)||' 04:00PM')::datetime
		when base.scan_time >= (current_date||' 04:00PM')::datetime and base.scan_time < (current_date||' 05:00PM')::datetime then (date(base.scan_Date)||' 05:00PM')::datetime
		when base.scan_time >= (current_date||' 05:00PM')::datetime and base.scan_time < (current_date||' 06:00PM')::datetime then (date(base.scan_Date)||' 06:00PM')::datetime
		when base.scan_time >= (current_date||' 06:00PM')::datetime and base.scan_time < (current_date||' 07:00PM')::datetime then (date(base.scan_Date)||' 07:00PM')::datetime
		when base.scan_time >= (current_date||' 07:00PM')::datetime and base.scan_time < (current_date||' 08:00PM')::datetime then (date(base.scan_Date)||' 08:00PM')::datetime
		when base.scan_time >= (current_date||' 08:00PM')::datetime and base.scan_time < (current_date||' 09:00PM')::datetime then (date(base.scan_Date)||' 09:00PM')::datetime
		when base.scan_time >= (current_date||' 09:00PM')::datetime and base.scan_time < (current_date||' 10:00PM')::datetime then (date(base.scan_Date)||' 10:00PM')::datetime
		when base.scan_time >= (current_date||' 10:00PM')::datetime and base.scan_time < (current_date||' 11:00PM')::datetime then (date(base.scan_Date)||' 11:00PM')::datetime
		when base.scan_time >= (current_date||' 11:00PM')::datetime and base.scan_time < (current_date||' 11:59PM')::datetime then (date(base.scan_Date)||' 12:00PM')::datetime
		when base.scan_time >= (current_date||' 12:00AM')::datetime and base.scan_time < (current_date||' 01:00AM')::datetime then (date(base.scan_Date)||' 01:00AM')::datetime
		when base.scan_time >= (current_date||' 01:00AM')::datetime and base.scan_time < (current_date||' 02:00AM')::datetime then (date(base.scan_Date)||' 02:00AM')::datetime
		when base.scan_time >= (current_date||' 02:00AM')::datetime and base.scan_time < (current_date||' 03:00AM')::datetime then (date(base.scan_Date)||' 03:00AM')::datetime
		when base.scan_time >= (current_date||' 03:00AM')::datetime and base.scan_time < (current_date||' 04:00AM')::datetime then (date(base.scan_Date)||' 04:00AM')::datetime
		when base.scan_time >= (current_date||' 04:00AM')::datetime and base.scan_time < (current_date||' 05:00AM')::datetime then (date(base.scan_Date)||' 05:00AM')::datetime
		when base.scan_time >= (current_date||' 05:00AM')::datetime and base.scan_time < (current_date||' 06:00AM')::datetime then (date(base.scan_Date)||' 06:00AM')::datetime
		end time_slot,
	count(distinct base.challan_sb) challan_count,
	sum(line_item_count) total_line_items,
	sum(value) item_value,
    sum(quantity) quantity
from
(select 
    ndi.n_distributor_id,
    dn.name dist_name,
    date(po.odt) scan_Date, 
    po.mtime,
    (current_date||' '||po.mtime)::datetime scan_time,
    po.purvtype,
    po.purvno challan_sb,
    count(distinct po.ordno||''||date(po.odt)||''||po.itemc) as line_item_count,
    sum((po.qty+po.fqty)*po.rate) value,
	sum(po.qty+po.fqty) quantity
from ahwspl__ahwspl_de__easysol.porder po 
left join adhoc.namespace_distributor_id ndi 
    on ndi."namespace" = po.skull_namespace
left join adhoc.distributor_name dn 
    on ndi.n_distributor_id = dn.distributor_id 
left join level1.act_mst am 
	on am.n_distributor_id = ndi.n_distributor_id
	and am.c_code = po.acno 
where 
    date(po.odt) between '{start_date}' and '{end_date}'
    and po.purvtype in ('SB','SN')
    and po.skull_opcode <> 'D'
    and po."tag"  = 'Y' 
    and (am.consol_category_fin is null or am.consol_category_fin = 'RETAIL')
group by 1,2,3,4,5,6,7
)base
group by 1,2,3,4
order by 2,3