(-- godown to store lineitems
select
    ndi.n_distributor_id dist_id,
    dn.name dist_name,
    date_trunc('month', gt.vdt)::date as dt,
    'Godown to Store Strips' kpi_name,
    sum(gt.qty) kpi_value
from ahwspl__ahwspl_de__easysol.godowntrn gt
left join adhoc.namespace_distributor_id ndi on ndi."namespace" = gt.skull_namespace 
left join adhoc.distributor_name dn on ndi.n_distributor_id = dn.distributor_id
where
	gt.vtype = 'G2'
    and gt.skull_opcode <> 'D'
    and gt.vdt between date_trunc('month', dateadd(month, -6, getdate())) and DATE(DATEADD(DAY, -1, GETDATE()))
group by 1,2,3,4
order by 2,3)
union
(--Billed Orders RETAIL
select 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp2.vdt)::date as dt,
    'Retail Billed Orders' kpi_name,
    count(distinct sp2.vno) kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase2 sp2
left join adhoc.namespace_distributor_id ndi 
    on sp2.skull_namespace = ndi."namespace"  
left join adhoc.distributor_name dn 
    on dn.distributor_id = ndi.n_distributor_id 
left join level1.act_mst am on 
	ndi.n_distributor_id = am.n_distributor_id 
	and sp2.acno = am.c_code 
where 
    date(sp2.vdt) between '2022-04-01' and DATE(DATEADD(DAY, -1, GETDATE()))
    and sp2.vtype = case when ndi.n_distributor_id in (select 
															distributor_id
															from adhoc.distributor_sale_challan
															where vtype = 'SB') then 'SB' else 'SN' end
    and sp2.skull_opcode <> 'D'
    and (am.consol_category_fin is null or am.consol_category_fin = 'RETAIL')
group by 1,2,3,4
order by 2,3)
union
(--Billed Orders SEMI
select 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp2.vdt)::date as dt,
    'Semi Billed Orders' kpi_name,
    count(distinct sp2.vno) kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase2 sp2
left join adhoc.namespace_distributor_id ndi 
    on sp2.skull_namespace = ndi."namespace"  
left join adhoc.distributor_name dn 
    on dn.distributor_id = ndi.n_distributor_id 
left join level1.act_mst am on 
	ndi.n_distributor_id = am.n_distributor_id 
	and sp2.acno = am.c_code 
where 
    date(sp2.vdt) between '2022-04-01' and DATE(DATEADD(DAY, -1, GETDATE()))
    and sp2.vtype = case when ndi.n_distributor_id in (select 
															distributor_id
															from adhoc.distributor_sale_challan
															where vtype = 'SB') then 'SB' else 'SN' end
    and sp2.skull_opcode <> 'D'
    and (am.consol_category_fin is not null and am.consol_category_fin <> 'RETAIL')
group by 1,2,3,4
order by 2,3
)
union
(--Billed Orders TOTAL
select 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp2.vdt)::date as dt,
    'Total Billed Orders' kpi_name,
    count(distinct sp2.vno) kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase2 sp2
left join adhoc.namespace_distributor_id ndi 
    on sp2.skull_namespace = ndi."namespace"  
left join adhoc.distributor_name dn 
    on dn.distributor_id = ndi.n_distributor_id 
left join level1.act_mst am on 
	ndi.n_distributor_id = am.n_distributor_id 
	and sp2.acno = am.c_code 
where 
    date(sp2.vdt) between '2022-04-01' and DATE(DATEADD(DAY, -1, GETDATE()))
    and sp2.vtype = case when ndi.n_distributor_id in (select 
															distributor_id
															from adhoc.distributor_sale_challan
															where vtype = 'SB') then 'SB' else 'SN' end
    and sp2.skull_opcode <> 'D'
group by 1,2,3,4
order by 2,3
)
union
(select 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp2.vdt)::date as dt,
    'Semi Picklist Line Items' kpi_name,
    count(distinct sp2.vno||'_'||sp2.vdt||'_'||sp2.itemc) kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase2 sp2
left join adhoc.namespace_distributor_id ndi 
    on sp2.skull_namespace = ndi."namespace"  
left join adhoc.distributor_name dn 
    on dn.distributor_id = ndi.n_distributor_id 
left join level1.act_mst am
    on am.n_distributor_id = ndi.n_distributor_id
    and am.c_code = sp2.acno
where 
    date(sp2.vdt) between '2022-04-01' and DATE(DATEADD(DAY, -1, GETDATE()))
    and sp2.skull_opcode <> 'D'
    and sp2.vtype = case when ndi.n_distributor_id in (select 
																distributor_id
															from adhoc.distributor_sale_challan
															where vtype = 'SB') then 'SB' else 'SN' end
    and (am.consol_category_fin not in ('RETAIL') and am.consol_category_fin is not null)
group by 1,2,3,4
order by 2,3)
union
(select 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp2.vdt)::date as dt,
    'Retail Picklist Line Items' kpi_name,
    count(distinct sp2.vno||'_'||sp2.vdt||'_'||sp2.itemc) kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase2 sp2
left join adhoc.namespace_distributor_id ndi 
    on sp2.skull_namespace = ndi."namespace"  
left join adhoc.distributor_name dn 
    on dn.distributor_id = ndi.n_distributor_id 
left join level1.act_mst am
    on am.n_distributor_id = ndi.n_distributor_id
    and am.c_code = sp2.acno
where 
    date(sp2.vdt) between '2022-04-01' and DATE(DATEADD(DAY, -1, GETDATE()))
    and sp2.skull_opcode <> 'D'
    and sp2.vtype = case when ndi.n_distributor_id in (select 
																distributor_id
															from adhoc.distributor_sale_challan
															where vtype = 'SB') then 'SB' else 'SN' end
    and (am.consol_category_fin in ('RETAIL') or am.consol_category_fin is null)
group by 1,2,3,4
order by 2,3)
union
(select 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp2.vdt)::date as dt,
    'Total Picklist Line Items' kpi_name,
    count(distinct sp2.vno||'_'||sp2.vdt||'_'||sp2.itemc) kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase2 sp2
left join adhoc.namespace_distributor_id ndi 
    on sp2.skull_namespace = ndi."namespace"  
left join adhoc.distributor_name dn 
    on dn.distributor_id = ndi.n_distributor_id 
left join level1.act_mst am
    on am.n_distributor_id = ndi.n_distributor_id
    and am.c_code = sp2.acno
where 
    date(sp2.vdt) between '2022-04-01' and DATE(DATEADD(DAY, -1, GETDATE()))
    and sp2.skull_opcode <> 'D'
    and sp2.vtype = case when ndi.n_distributor_id in (select 
																distributor_id
															from adhoc.distributor_sale_challan
															where vtype = 'SB') then 'SB' else 'SN' end
    -- and (am.consol_category_fin not in ('RETAIL') and am.consol_category_fin is not null)
group by 1,2,3,4
order by 2,3)
union
--Billed Strips
(--Semi Picklist Qty
select
	base.dist_id,
	base.dist_name,
	base.dt,
	base.kpi_name,
	base.kpi_value
from
(select 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp2.vdt)::date as dt,
    sp2.vtype,
    'Semi Picklist Qty' kpi_name,
    case when sp2.vtype = 'SB' then sum(sp2.qty+sp2.fqty) else sum(sp2.chlnqty+sp2.chlnfqty) end kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase2 sp2
left join adhoc.namespace_distributor_id ndi 
    on sp2.skull_namespace = ndi."namespace"  
left join adhoc.distributor_name dn 
    on dn.distributor_id = ndi.n_distributor_id 
left join level1.act_mst am
    on am.n_distributor_id = ndi.n_distributor_id
    and am.c_code = sp2.acno
where 
    date(sp2.vdt) between '2022-04-01' and DATE(DATEADD(DAY, -1, GETDATE()))
    and sp2.skull_opcode <> 'D'
    and sp2.vtype = case when ndi.n_distributor_id in (select 
																distributor_id
															from adhoc.distributor_sale_challan
															where vtype = 'SB') then 'SB' else 'SN' end
    and (am.consol_category_fin not in ('RETAIL') and am.consol_category_fin is not null)
group by 1,2,3,4,5
order by 2,3,4)base)
union
(--Retail Picklist Qty
select
	base.dist_id,
	base.dist_name,
	base.dt,
	base.kpi_name,
	base.kpi_value
from
(select 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp2.vdt)::date as dt,
    sp2.vtype,
    'Retail Picklist Qty' kpi_name,
    case when sp2.vtype = 'SB' then sum(sp2.qty+sp2.fqty) else sum(sp2.chlnqty+sp2.chlnfqty) end kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase2 sp2
left join adhoc.namespace_distributor_id ndi 
    on sp2.skull_namespace = ndi."namespace"  
left join adhoc.distributor_name dn 
    on dn.distributor_id = ndi.n_distributor_id 
left join level1.act_mst am
    on am.n_distributor_id = ndi.n_distributor_id
    and am.c_code = sp2.acno
where 
    date(sp2.vdt) between '2022-04-01' and DATE(DATEADD(DAY, -1, GETDATE()))
    and sp2.skull_opcode <> 'D'
    and sp2.vtype = case when ndi.n_distributor_id in (select 
																distributor_id
															from adhoc.distributor_sale_challan
															where vtype = 'SB') then 'SB' else 'SN' end
    and (am.consol_category_fin in ('RETAIL') or am.consol_category_fin is null)
group by 1,2,3,4,5
order by 2,3,4)base)
union
(--Semi Picklist Qty
select
	base.dist_id,
	base.dist_name,
	base.dt,
	base.kpi_name,
	base.kpi_value
from
(select 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp2.vdt)::date as dt,
    sp2.vtype,
    'Total Picklist Qty' kpi_name,
    case when sp2.vtype = 'SB' then sum(sp2.qty+sp2.fqty) else sum(sp2.chlnqty+sp2.chlnfqty) end kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase2 sp2
left join adhoc.namespace_distributor_id ndi 
    on sp2.skull_namespace = ndi."namespace"  
left join adhoc.distributor_name dn 
    on dn.distributor_id = ndi.n_distributor_id 
left join level1.act_mst am
    on am.n_distributor_id = ndi.n_distributor_id
    and am.c_code = sp2.acno
where 
    date(sp2.vdt) between '2022-04-01' and DATE(DATEADD(DAY, -1, GETDATE()))
    and sp2.skull_opcode <> 'D'
    and sp2.vtype = case when ndi.n_distributor_id in (select 
																distributor_id
															from adhoc.distributor_sale_challan
															where vtype = 'SB') then 'SB' else 'SN' end
    -- and (am.consol_category_fin not in ('RETAIL') and am.consol_category_fin is not null)
group by 1,2,3,4,5
order by 2,3,4)base)
union
(--Inward Strips
select 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp2.vdt)::date as dt,
    'Inward Strips' kpi_name,
    sum(sp2.qty+sp2.fqty) kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase2 sp2
left join adhoc.namespace_distributor_id ndi 
    on sp2.skull_namespace = ndi."namespace"  
left join adhoc.distributor_name dn 
    on dn.distributor_id = ndi.n_distributor_id 
left join level1.act_mst am
    on am.n_distributor_id = ndi.n_distributor_id
    and am.c_code = sp2.acno
where 
    date(sp2.vdt) between '2022-04-01' and DATE(DATEADD(DAY, -1, GETDATE()))
    and sp2.vtype in ('PB', 'BR')
    and sp2.skull_opcode <> 'D'
group by 1,2,3,4
order by 2,3)
union
(--Inward Strips
select 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp2.vdt)::date as dt,
    'Sale Return Strips' kpi_name,
    sum(sp2.qty+sp2.fqty) kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase2 sp2
left join adhoc.namespace_distributor_id ndi 
    on sp2.skull_namespace = ndi."namespace"  
left join adhoc.distributor_name dn 
    on dn.distributor_id = ndi.n_distributor_id 
left join level1.act_mst am
    on am.n_distributor_id = ndi.n_distributor_id
    and am.c_code = sp2.acno
where 
    date(sp2.vdt) between '2022-04-01' and DATE(DATEADD(DAY, -1, GETDATE()))
    and sp2.vtype in ('SR')
    and sp2.skull_opcode <> 'D'
group by 1,2,3,4
order by 2,3)
union
(--Inward Strips
select 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp2.vdt)::date as dt,
    'Expiry Return Strips' kpi_name,
    sum(sp2.qty+sp2.fqty) kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase2 sp2
left join adhoc.namespace_distributor_id ndi 
    on sp2.skull_namespace = ndi."namespace"  
left join adhoc.distributor_name dn 
    on dn.distributor_id = ndi.n_distributor_id 
left join level1.act_mst am
    on am.n_distributor_id = ndi.n_distributor_id
    and am.c_code = sp2.acno
where 
    date(sp2.vdt) between '2022-04-01' and DATE(DATEADD(DAY, -1, GETDATE()))
    and sp2.vtype in ('BC', 'BE')
    and sp2.betype = 'E'
    and sp2.skull_opcode <> 'D'
group by 1,2,3,4
order by 2,3)
union
(select
	distinct 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp1.vdt)::date as dt,
    'Sale Return Value' kpi_name,
    sum(sp1.amt01) kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase1 sp1
left join adhoc.namespace_distributor_id ndi 
	on ndi."namespace" = sp1.skull_namespace 
left join adhoc.distributor_name dn 
	on dn.distributor_id = ndi.n_distributor_id 
left join level1.act_mst am 
	on am.n_distributor_id = ndi.n_distributor_id 
	and am.c_code = sp1.acno 
where
	date(sp1.vdt) between '2022-04-01' and DATE(DATEADD(DAY, -1, GETDATE()))
	and sp1.vtyp = 'SR'
	and sp1.skull_opcode <> 'D'
	-- and (am.consol_category_fin in ('RETAIL') or am.consol_category_fin is null)
group by 1,2,3,4
order by 2,3)
union
(select
	distinct 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp1.vdt)::date as dt,
    'Inward Value' kpi_name,
    sum(sp1.amt01) kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase1 sp1
left join adhoc.namespace_distributor_id ndi 
	on ndi."namespace" = sp1.skull_namespace 
left join adhoc.distributor_name dn 
	on dn.distributor_id = ndi.n_distributor_id 
where
	date(sp1.vdt) between '2022-04-01' and DATE(DATEADD(DAY, -1, GETDATE()))
	and sp1.vtyp in ('PB', 'BR')
	and sp1.skull_opcode <> 'D'
group by 1,2,3,4
order by 2,3)
union
(select 
	esde.distributor_id dist_id,
	dn."name" dist_name,
	date_trunc('month', esde.sync_date)::date dt,  
	'Inventory Value at 15th' kpi_name,
	sum(esde.cost * esde.bqty) kpi_value
from adhoc.easysol_stock_day_end esde  
left join adhoc.distributor_name dn on esde.distributor_id = dn.distributor_id 
where 
	date(esde.sync_date) between '2022-04-01' and current_date - 1
--	and esde.distributor_id in (2,6,64,914,70,71,5)
	and date_part('d', esde.sync_date) = 15
group by 1,2,3,4
order by 2,3)
union
(select 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp2.vdt)::date as dt,
    'Expiry Return Amt' kpi_name,
    sum(sp2.netamt) kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase2 sp2
left join adhoc.namespace_distributor_id ndi 
    on sp2.skull_namespace = ndi."namespace"  
left join adhoc.distributor_name dn 
    on dn.distributor_id = ndi.n_distributor_id 
left join level1.act_mst am
    on am.n_distributor_id = ndi.n_distributor_id
    and am.c_code = sp2.acno
where 
    date(sp2.vdt) between '2022-04-01' and current_date - 1
    and sp2.vtype in ('BC', 'BE')
    and sp2.betype = 'E'
    and sp2.skull_opcode <> 'D'
group by 1,2,3,4
order by 2,3)
union
(select 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp2.vdt)::date as dt,
    'Sale Return Amt' kpi_name,
    sum(sp2.netamt) kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase2 sp2
left join adhoc.namespace_distributor_id ndi 
    on sp2.skull_namespace = ndi."namespace"  
left join adhoc.distributor_name dn 
    on dn.distributor_id = ndi.n_distributor_id 
left join level1.act_mst am
    on am.n_distributor_id = ndi.n_distributor_id
    and am.c_code = sp2.acno
where 
    date(sp2.vdt) between '2022-04-01' and current_date - 1
    and sp2.vtype in ('SR')
    and sp2.skull_opcode <> 'D'
group by 1,2,3,4
order by 2,3)
union
(--Sale Return Value from customer	
select
	ndi.n_distributor_id "dist_id",
	dn.name dist_name,
	date_trunc('month', sp2.vdt)::date as dt,
	'Sale Return Value from customer' kpi_name,
	sum(sp2.netamt) kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase2 sp2
left join adhoc.namespace_distributor_id ndi on ndi."namespace" = sp2.skull_namespace 
left join adhoc.distributor_name dn on ndi.n_distributor_id = dn.distributor_id 
where 
	date(sp2.vdt) between '2022-01-01' and current_date - 1
	and sp2.vtype in ('SR')
	and sp2.skull_opcode <> 'D'
--	and sp2.betype = 'E'
--	and ndi.n_distributor_id in (70, 71, 6)
group by 1,2,3
order by 1,2,3)
union
(--Expiry Return Value from customer
select
	ndi.n_distributor_id "dist_id",
	dn.name dist_name,
	date_trunc('month', sp2.vdt)::date as dt,
	'Expiry Return Value from customer' kpi_name,
	sum(sp2.netamt) kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase2 sp2
left join adhoc.namespace_distributor_id ndi on ndi."namespace" = sp2.skull_namespace 
left join adhoc.distributor_name dn on ndi.n_distributor_id = dn.distributor_id 
where 
	date(sp2.vdt) between '2022-01-01' and current_date - 1
	and sp2.vtype in ('BC','BE')
	and sp2.skull_opcode <> 'D'
	and sp2.betype = 'E'
--	and ndi.n_distributor_id in (70, 71, 6)
group by 1,2,3
order by 1,2,3)
union
(--Intransit breakage (retail)
select
	ndi.n_distributor_id "dist_id",
	dn.name dist_name,
	date_trunc('month', sp2.vdt)::date as dt,
	'Intransit breakage (Retail)' kpi_name,
	sum(sp2.netamt) kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase2 sp2
left join adhoc.namespace_distributor_id ndi on ndi."namespace" = sp2.skull_namespace 
left join adhoc.distributor_name dn on ndi.n_distributor_id = dn.distributor_id 
left join level1.act_mst am 
	on am.c_code = sp2.acno 
	and am.n_distributor_id = ndi.n_distributor_id 
where 
	date(sp2.vdt) between '2022-01-01' and current_date - 1
	and sp2.vtype in ('BC','BE')
	and sp2.skull_opcode <> 'D'
	and sp2.betype = 'B'
--	and ndi.n_distributor_id in (70, 71, 6)
	and (am.consol_category_fin is null or am.consol_category_fin = 'RETAIL')
group by 1,2,3
order by 1,2,3)
union
(--Expiry from godown
select
	ndi.n_distributor_id "dist_id",
	dn.name dist_name,
	date_trunc('month', sp2.vdt)::date as dt,
	'Expiry from godown' kpi_name,
	sum(sp2.netamt) kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase2 sp2
left join adhoc.namespace_distributor_id ndi on ndi."namespace" = sp2.skull_namespace 
left join adhoc.distributor_name dn on ndi.n_distributor_id = dn.distributor_id 
where 
	date(sp2.vdt) between '2022-01-01' and current_date - 1
	and sp2.vtype in ('GE')
	and sp2.skull_opcode <> 'D'
	and sp2.betype = 'E'
--	and ndi.n_distributor_id in (70, 71, 6)
group by 1,2,3
order by 1,2,3)
union 
(--Gowdown breakage
select
	ndi.n_distributor_id "dist_id",
	dn.name dist_name,
	date_trunc('month', sp2.vdt)::date as dt,
	'Gowdown breakage' kpi_name,
	sum(sp2.netamt) kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase2 sp2
left join adhoc.namespace_distributor_id ndi on ndi."namespace" = sp2.skull_namespace 
left join adhoc.distributor_name dn on ndi.n_distributor_id = dn.distributor_id 
where 
	date(sp2.vdt) between '2022-01-01' and current_date - 1
	and sp2.vtype in ('GE')
	and sp2.skull_opcode <> 'D'
	and sp2.betype = 'B'
--	and ndi.n_distributor_id in (70, 71, 6)
group by 1,2,3
order by 1,2,3)
union 
(--Expiry to supplier
select
	ndi.n_distributor_id "dist_id",
	dn.name dist_name,
	date_trunc('month', sp2.vdt)::date as dt,
	'Expiry to supplier' kpi_name,
	sum(sp2.netamt) kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase2 sp2
left join adhoc.namespace_distributor_id ndi on ndi."namespace" = sp2.skull_namespace 
left join adhoc.distributor_name dn on ndi.n_distributor_id = dn.distributor_id 
where 
	date(sp2.vdt) between '2022-01-01' and current_date - 1
	and sp2.vtype in ('PE')
	and sp2.skull_opcode <> 'D'
	and sp2.betype = 'E'
--	and ndi.n_distributor_id in (70, 71, 6)
group by 1,2,3
order by 1,2,3)
union
(--Expiry return strip to supplier
select
	ndi.n_distributor_id "dist_id",
	dn.name dist_name,
	date_trunc('month', sp2.vdt)::date as dt,
	'Expiry return strip to supplier' kpi_name,
	sum(sp2.qty + sp2.fqty) kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase2 sp2
left join adhoc.namespace_distributor_id ndi on ndi."namespace" = sp2.skull_namespace 
left join adhoc.distributor_name dn on ndi.n_distributor_id = dn.distributor_id 
where 
	date(sp2.vdt) between '2022-01-01' and current_date - 1
	and sp2.vtype in ('PE')
	and sp2.skull_opcode <> 'D'
	and sp2.betype = 'E'
--	and ndi.n_distributor_id in (70, 71, 6)
group by 1,2,3
order by 1,2,3)
union
(--Value of non-moving items
select 
		esde.distributor_id "dist_id",
		dn."name" "dist_name",
        date_trunc('month', esde.sync_date)::date as dt,
		'Value of Non-moving item' "kpi_name",
		sum(esde.cost * esde.bqty) day_end_stock
	from adhoc.easysol_stock_day_end esde  
	left join adhoc.distributor_name dn on esde.distributor_id = dn.distributor_id
	left join adhoc.category_abc_fms caf on (caf.c_code::varchar) = (esde.itemc::varchar) and caf.n_distributor_id = esde.distributor_id 
	where 
		date(esde.sync_date) between '2022-01-01' and current_date - 1
		and date_part('d', esde.sync_date) = 15
        and caf.category_fms ='N'
	group by 1,2,3,4
	order by 2,3)