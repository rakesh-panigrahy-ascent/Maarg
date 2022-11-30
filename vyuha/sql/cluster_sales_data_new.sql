select 
	distinct 
	fa.distributor_id n_distributor_id,
	fa."name" dist_name,
	date(fs2.latest_ofd_time) tagdt,
	ds.acno,
	am.c_name,
	fs2.pincode Pincodes,
	case when fs2.latest_ofd_time <=  (date(fs2.latest_ofd_time)||' '||'02:00PM')::datetime then 'F' else 'S' end Shift,
	sum(sp1.amt01) amount
from ahwspl_analytics_data_model.flash_shipment fs2
left join adhoc.flash_asset fa on fs2.asset_id = fa.asset_id 
left join adhoc.flash_asset fa2 on fa.hub_asset_id = fa2.asset_id 
left join ahwspl__ahwspl_de__easysol.dispatchstmt ds
	on fa2.skull_namespace = ds.skull_namespace 
	and ds.vno::varchar = split_part(fs2.reference_id,'-',3)
	and (date(ds.tagdt) between date(fs2.dt) and date(fs2.dt) + 1 or ds.tagdt is null)
	and ds.reversed <> 'Y'
	and ds.skull_opcode <> 'D'
left join level1.act_mst am 
	on am.n_distributor_id = fa2.distributor_id 
	and am.c_code = ds.acno
left join ahwspl__ahwspl_de__easysol.salepurchase1 sp1
	on ds.skull_namespace = sp1.skull_namespace 
	and ds.vno = sp1.vno 
	and ds.vtype = sp1.vtyp 
	and ds.vdt = sp1.vdt 
	and sp1.skull_opcode <> 'D'
where  
	date(fs2.latest_ofd_time) between '{start_date}' and '{end_date}'
	and fs2.skull_opcode  <> 'D'
group by 1,2,3,4,5,6,7
order by 2,3,5