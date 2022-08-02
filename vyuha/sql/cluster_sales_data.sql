select 
	distinct 
	ndi.n_distributor_id,
	dn."name" dist_name,
	ds.tagdt,
	(ds.tagdt||' '||ds.finaltime)::datetime dispatch_dtime,
	(ds.tagdt||' '||'12:00PM')::datetime first_cutoff,
	(ds.tagdt||' '||'04:30PM')::datetime second_cutoff,
	case when dispatch_dtime <=  first_cutoff then 'F'
		else 'S' end cutoff,
	sp1.acno,
	am.c_name,
	sum(sp1.amt01) amount
from ahwspl__ahwspl_de__easysol.dispatchstmt ds
left join ahwspl__ahwspl_de__easysol.salepurchase1 sp1
	on ds.vno = sp1.vno 
	and ds.skull_namespace = sp1.skull_namespace 
	and ds.vdt = sp1.vdt 
	and ds.vtype = sp1.vtyp 
	and sp1.skull_opcode <> 'D'
left join adhoc.namespace_distributor_id ndi on ndi."namespace" = ds.skull_namespace 
left join adhoc.distributor_name dn on ndi.n_distributor_id = dn.distributor_id 
left join adhoc.flash_asset fa
	on fa.hub_distributor_id = ndi.n_distributor_id 
	and fa.asset_id = {asset_id}
left join level1.act_mst am 
	on am.n_distributor_id = ndi.n_distributor_id 
	and am.c_code = sp1.acno 
where 
	fa.asset_id = {asset_id}
	and ds.skull_opcode <> 'D'
	and ds.reversed <> 'Y'
	and date(ds.tagdt) between '{start_date}' and '{end_date}'	
group by 1,2,3,4,5,6,7,8,9
order by 2,3,4