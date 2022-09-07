select base.*, net_amt.net_amt from
((-- godown to store lineitems
select
    ndi.n_distributor_id dist_id,
    dn.name dist_name,
    date_trunc('month', gt.vdt) as dt,
    'Godown to Store Line Items' kpi_name,
    COUNT(DISTINCT gt.vno||'_'||gt.itemc) kpi_value
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
(--Picklist Printed
select 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp2.vdt) as dt,
    'Picklist Printed' kpi_name,
    count(distinct sp2.vno) kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase2 sp2
left join adhoc.namespace_distributor_id ndi 
    on sp2.skull_namespace = ndi."namespace"  
left join adhoc.distributor_name dn 
    on dn.distributor_id = ndi.n_distributor_id 
where 
    date(sp2.vdt) between date_trunc('month', dateadd(month, -6, getdate())) and DATE(DATEADD(DAY, -1, GETDATE()))
    and sp2.vtype = case when ndi.n_distributor_id in (select 
															distributor_id
															from adhoc.distributor_sale_challan
															where vtype = 'SB') then 'SB' else 'SN' end
    and sp2.skull_opcode <> 'D'
group by 1,2,3,4
order by 2,3)
union
(--Picklist Dispatched
select 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp2.vdt) as dt,
    'Picklist Dispatched' kpi_name,
    count(distinct sp2.vno) kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase2 sp2
left join adhoc.namespace_distributor_id ndi 
    on sp2.skull_namespace = ndi."namespace"  
left join adhoc.distributor_name dn 
    on dn.distributor_id = ndi.n_distributor_id 
left join level1.act_mst am
    on am.n_distributor_id = ndi.n_distributor_id
    and am.c_code = sp2.acno
where 
    date(sp2.vdt) between date_trunc('month', dateadd(month, -6, getdate())) and DATE(DATEADD(DAY, -1, GETDATE()))
    and sp2.vtype = case when ndi.n_distributor_id in (select 
															distributor_id
															from adhoc.distributor_sale_challan
															where vtype = 'SB') then 'SB' else 'SN' end
    and sp2.skull_opcode <> 'D'
    and (am.consol_category_fin not in ('RETAIL') and am.consol_category_fin is not null)
group by 1,2,3,4
order by 2,3)
union
(-- Dispatch-System Operator - Net Revenue
select
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp1.vdt) as dt,
    'Dispatch-System Operator - Net Revenue' kpi_name,
    sum(sp1.amt01) kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase1 sp1
left join adhoc.namespace_distributor_id ndi 
	on ndi."namespace" = sp1.skull_namespace 
left join adhoc.distributor_name dn 
	on dn.distributor_id = ndi.n_distributor_id 
left join ahwspl__ahwspl_de__easysol.dispatchstmt ds
	on sp1.skull_namespace = ds.skull_namespace
	AND sp1.vno = ds.vno
	and sp1.vdt = ds.vdt
	and sp1.vtyp = ds.vtype
	and ds.reversed <> 'Y'
	and ds.skull_opcode <> 'D'
left join level1.act_mst am 
	on am.n_distributor_id = ndi.n_distributor_id 
	and am.c_code = sp1.acno 
where
	date(ds.tagdt) between date_trunc('month', dateadd(month, -6, getdate())) and DATE(DATEADD(DAY, -1, GETDATE()))
	and sp1.vtyp = 'SB'
	and sp1.skull_opcode <> 'D'
	and (am.consol_category_fin in ('RETAIL') or am.consol_category_fin is null)
group by 1,2,3,4
order by 2,3)
union
(--Expiry return qty
select 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp2.vdt) as dt,
    'Expiry Return Qty' kpi_name,
    sum(sp2.qty+sp2.fqty) kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase2 sp2
left join adhoc.namespace_distributor_id ndi 
    on sp2.skull_namespace = ndi."namespace"  
left join adhoc.distributor_name dn 
    on dn.distributor_id = ndi.n_distributor_id 
where 
    date(sp2.vdt) between date_trunc('month', dateadd(month, -6, getdate())) and DATE(DATEADD(DAY, -1, GETDATE()))
    and sp2.skull_opcode <> 'D'
    and sp2.vtype in ('BC', 'BE')
    -- and sp2.betype = 'E'
group by 1,2,3,4
order by 2,3)
union
(--Expiry batch line items 
select 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp2.vdt) as dt,
    'Expiry Return Batch Line Items' kpi_name,
    count(distinct sp2.vno||'_'||sp2.vdt||'_'||sp2.itemc||'_'||sp2.batch) kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase2 sp2
left join adhoc.namespace_distributor_id ndi 
    on sp2.skull_namespace = ndi."namespace"  
left join adhoc.distributor_name dn 
    on dn.distributor_id = ndi.n_distributor_id 
where 
    date(sp2.vdt) between date_trunc('month', dateadd(month, -6, getdate())) and DATE(DATEADD(DAY, -1, GETDATE()))
    and sp2.skull_opcode <> 'D'
    and sp2.vtype in ('BC', 'BE')
    -- and sp2.betype = 'E'
group by 1,2,3,4
order by 2,3)
union
(--SR return qty
select 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp2.vdt) as dt,
    'Sale Return Qty' kpi_name,
    sum(sp2.qty+sp2.fqty) kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase2 sp2
left join adhoc.namespace_distributor_id ndi 
    on sp2.skull_namespace = ndi."namespace"  
left join adhoc.distributor_name dn 
    on dn.distributor_id = ndi.n_distributor_id 
where 
    date(sp2.vdt) between date_trunc('month', dateadd(month, -6, getdate())) and DATE(DATEADD(DAY, -1, GETDATE()))
    and sp2.skull_opcode <> 'D'
    and sp2.vtype in ('SR')
group by 1,2,3,4
order by 2,3)
union
(--SR batch line items 
select 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp2.vdt) as dt,
    'Sale Return Batch Line Items' kpi_name,
    count(distinct sp2.vno||'_'||sp2.vdt||'_'||sp2.itemc||'_'||sp2.batch) kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase2 sp2
left join adhoc.namespace_distributor_id ndi 
    on sp2.skull_namespace = ndi."namespace"  
left join adhoc.distributor_name dn 
    on dn.distributor_id = ndi.n_distributor_id 
where 
    date(sp2.vdt) between date_trunc('month', dateadd(month, -6, getdate())) and DATE(DATEADD(DAY, -1, GETDATE()))
    and sp2.skull_opcode <> 'D'
    and sp2.vtype in ('SR')
group by 1,2,3,4
order by 2,3)
union
(--Purchase Line Items
select 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp2.vdt) as dt,
    'Purchase Line Items' kpi_name,
    count(distinct sp2.vno||'_'||sp2.vdt||'_'||sp2.itemc) kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase2 sp2
left join adhoc.namespace_distributor_id ndi 
    on sp2.skull_namespace = ndi."namespace"  
left join adhoc.distributor_name dn 
    on dn.distributor_id = ndi.n_distributor_id 
where 
    date(sp2.vdt) between date_trunc('month', dateadd(month, -6, getdate())) and DATE(DATEADD(DAY, -1, GETDATE()))
    and sp2.skull_opcode <> 'D'
    and sp2.vtype in ('PB', 'BR')
group by 1,2,3,4
order by 2,3)
union
(--Purchase Qty
select 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp2.vdt) as dt,
    'Purchase Qty' kpi_name,
    sum(sp2.qty+sp2.fqty) kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase2 sp2
left join adhoc.namespace_distributor_id ndi 
    on sp2.skull_namespace = ndi."namespace"  
left join adhoc.distributor_name dn 
    on dn.distributor_id = ndi.n_distributor_id 
where 
    date(sp2.vdt) between date_trunc('month', dateadd(month, -6, getdate())) and DATE(DATEADD(DAY, -1, GETDATE()))
    and sp2.skull_opcode <> 'D'
    and sp2.vtype in ('PB', 'BR')
group by 1,2,3,4
order by 2,3)
union
(--Gatepass Boxes
select 
	ndi.n_distributor_id,
	dn."name" dist_name,
	date_trunc('month', ac.vdt) as dt,
	'Gatepass Boxes' kpi_name,
	sum(ac.noofcases) kpi_value
from ahwspl__ahwspl_de__easysol.acknow ac
left join adhoc.namespace_distributor_id ndi on ndi."namespace" = ac.skull_namespace 
left join adhoc.distributor_name dn on ndi.n_distributor_id = dn.distributor_id 
where 
	date(ac.vdt) between date_trunc('month', dateadd(month, -6, getdate())) and DATE(DATEADD(DAY, -1, GETDATE()))
    and ac.skull_opcode <> 'D'
    and ac.vtype in ('PB', 'BR')
group by 1,2,3,4
order by 2,3)
union
(--Picklist Line Items
select 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp2.vdt) as dt,
    'Picklist Line Items' kpi_name,
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
    date(sp2.vdt) between date_trunc('month', dateadd(month, -6, getdate())) and DATE(DATEADD(DAY, -1, GETDATE()))
    and sp2.skull_opcode <> 'D'
    and sp2.vtype = case when ndi.n_distributor_id in (select 
																distributor_id
															from adhoc.distributor_sale_challan
															where vtype = 'SB') then 'SB' else 'SN' end
    and (am.consol_category_fin not in ('RETAIL') and am.consol_category_fin is not null)
group by 1,2,3,4
order by 2,3)
union
(--Picklist Qty
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
    date_trunc('month', sp2.vdt) as dt,
    sp2.vtype,
    'Picklist Qty' kpi_name,
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
    date(sp2.vdt) between date_trunc('month', dateadd(month, -6, getdate())) and DATE(DATEADD(DAY, -1, GETDATE()))
    and sp2.skull_opcode <> 'D'
    and sp2.vtype = case when ndi.n_distributor_id in (select 
																distributor_id
															from adhoc.distributor_sale_challan
															where vtype = 'SB') then 'SB' else 'SN' end
    and (am.consol_category_fin not in ('RETAIL') and am.consol_category_fin is not null)
group by 1,2,3,4,5
order by 2,3,4)base)
union
(--Picklist Qty
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
    date_trunc('month', sp2.vdt) as dt,
    sp2.vtype,
    'Ratio Of Checker (1/10)' kpi_name,
    case when sp2.vtype = 'SB' then sum(sp2.qty+sp2.fqty) else sum(sp2.chlnqty+sp2.chlnfqty) end kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase2 sp2
left join adhoc.namespace_distributor_id ndi 
    on sp2.skull_namespace = ndi."namespace"  
left join adhoc.distributor_name dn 
    on dn.distributor_id = ndi.n_distributor_id 
where 
    date(sp2.vdt) between date_trunc('month', dateadd(month, -6, getdate())) and DATE(DATEADD(DAY, -1, GETDATE()))
    and sp2.skull_opcode <> 'D'
    and sp2.vtype = case when ndi.n_distributor_id in (select 
																distributor_id
															from adhoc.distributor_sale_challan
															where vtype = 'SB') then 'SB' else 'SN' end
group by 1,2,3,4,5
order by 2,3,4)base)
union
(select
	distinct 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp1.vdt) as dt,
    'Audit & Refilling-Lead - Not required' kpi_name,
    0 kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase1 sp1
left join adhoc.namespace_distributor_id ndi 
	on ndi."namespace" = sp1.skull_namespace 
left join adhoc.distributor_name dn 
	on dn.distributor_id = ndi.n_distributor_id 
left join level1.act_mst am 
	on am.n_distributor_id = ndi.n_distributor_id 
	and am.c_code = sp1.acno 
where
	date(sp1.vdt) between date_trunc('month', dateadd(month, -6, getdate())) and DATE(DATEADD(DAY, -1, GETDATE()))
	and sp1.vtyp = 'SB'
	and sp1.skull_opcode <> 'D'
	and (am.consol_category_fin in ('RETAIL') or am.consol_category_fin is null)
order by 2,3)
union
(select
	distinct 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp1.vdt) as dt,
    'Dispatch-Lead - Not required' kpi_name,
    0 kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase1 sp1
left join adhoc.namespace_distributor_id ndi 
	on ndi."namespace" = sp1.skull_namespace 
left join adhoc.distributor_name dn 
	on dn.distributor_id = ndi.n_distributor_id 
left join level1.act_mst am 
	on am.n_distributor_id = ndi.n_distributor_id 
	and am.c_code = sp1.acno 
where
	date(sp1.vdt) between date_trunc('month', dateadd(month, -6, getdate())) and DATE(DATEADD(DAY, -1, GETDATE()))
	and sp1.vtyp = 'SB'
	and sp1.skull_opcode <> 'D'
	and (am.consol_category_fin in ('RETAIL') or am.consol_category_fin is null)
order by 2,3)
union
(select
	distinct 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp1.vdt) as dt,
    'Expiry-Lead - Not required' kpi_name,
    0 kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase1 sp1
left join adhoc.namespace_distributor_id ndi 
	on ndi."namespace" = sp1.skull_namespace 
left join adhoc.distributor_name dn 
	on dn.distributor_id = ndi.n_distributor_id 
left join level1.act_mst am 
	on am.n_distributor_id = ndi.n_distributor_id 
	and am.c_code = sp1.acno 
where
	date(sp1.vdt) between date_trunc('month', dateadd(month, -6, getdate())) and DATE(DATEADD(DAY, -1, GETDATE()))
	and sp1.vtyp = 'SB'
	and sp1.skull_opcode <> 'D'
	and (am.consol_category_fin in ('RETAIL') or am.consol_category_fin is null)
order by 2,3)
union
(select
	distinct 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp1.vdt) as dt,
    'Manual' kpi_name,
    1 kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase1 sp1
left join adhoc.namespace_distributor_id ndi 
	on ndi."namespace" = sp1.skull_namespace 
left join adhoc.distributor_name dn 
	on dn.distributor_id = ndi.n_distributor_id 
left join level1.act_mst am 
	on am.n_distributor_id = ndi.n_distributor_id 
	and am.c_code = sp1.acno 
where
	date(sp1.vdt) between date_trunc('month', dateadd(month, -6, getdate())) and DATE(DATEADD(DAY, -1, GETDATE()))
	and sp1.vtyp = 'SB'
	and sp1.skull_opcode <> 'D'
	and (am.consol_category_fin in ('RETAIL') or am.consol_category_fin is null)
order by 2,3)
union
(select 
	base.dist_id,
	base.dist_name,
	base.dt,
	base.kpi_name,
	base.kpi_value
from
(select
	distinct 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', spa.vdt) as dt,
    spa.vtype,
    'Line Items Modified' kpi_name,
    count(distinct spa.vno||spa.vdt||spa.itemc)  kpi_value
from ahwspl__ahwspl_de__easysol.spalter spa
left join adhoc.namespace_distributor_id ndi 
	on ndi."namespace" = spa.skull_namespace 
left join adhoc.distributor_name dn 
	on dn.distributor_id = ndi.n_distributor_id 
left join level1.act_mst am
    on am.n_distributor_id = ndi.n_distributor_id
    and am.c_code = spa.acno
where
	date(spa.vdt) between date_trunc('month', dateadd(month, -6, getdate())) and DATE(DATEADD(DAY, -1, GETDATE()))
	and spa.vtype = case when ndi.n_distributor_id in (select 
																distributor_id
															from adhoc.distributor_sale_challan
															where vtype = 'SB') then 'SB' else 'SN' end
    and (am.consol_category_fin not in ('RETAIL') and am.consol_category_fin is not null)
	and spa.skull_opcode <> 'D'
	and spa.descp in ('ITEM DELETE', 'BATCH CHANGE')
group by 1,2,3,4,5
order by 2,3,4)base
order by 2,3)
union
(select
	distinct 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp1.vdt) as dt,
    'Checking-Lead,Support - Net Revenue' kpi_name,
    1 kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase1 sp1
left join adhoc.namespace_distributor_id ndi 
	on ndi."namespace" = sp1.skull_namespace 
left join adhoc.distributor_name dn 
	on dn.distributor_id = ndi.n_distributor_id 
left join level1.act_mst am 
	on am.n_distributor_id = ndi.n_distributor_id 
	and am.c_code = sp1.acno 
where
	date(sp1.vdt) between date_trunc('month', dateadd(month, -6, getdate())) and DATE(DATEADD(DAY, -1, GETDATE()))
	and sp1.vtyp = 'SB'
	and sp1.skull_opcode <> 'D'
	and (am.consol_category_fin in ('RETAIL') or am.consol_category_fin is null)
order by 2,3)
union 
(select
	distinct 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp1.vdt) as dt,
    'Inward-Lead,Overall - Net Revenue' kpi_name,
    1 kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase1 sp1
left join adhoc.namespace_distributor_id ndi 
	on ndi."namespace" = sp1.skull_namespace 
left join adhoc.distributor_name dn 
	on dn.distributor_id = ndi.n_distributor_id 
left join level1.act_mst am 
	on am.n_distributor_id = ndi.n_distributor_id 
	and am.c_code = sp1.acno 
where
	date(sp1.vdt) between date_trunc('month', dateadd(month, -6, getdate())) and DATE(DATEADD(DAY, -1, GETDATE()))
	and sp1.vtyp = 'SB'
	and sp1.skull_opcode <> 'D'
	and (am.consol_category_fin in ('RETAIL') or am.consol_category_fin is null)
order by 2,3)
union 
(select
	distinct 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp1.vdt) as dt,
    'Store-Lead,Support - Net Revenue' kpi_name,
    1 kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase1 sp1
left join adhoc.namespace_distributor_id ndi 
	on ndi."namespace" = sp1.skull_namespace 
left join adhoc.distributor_name dn 
	on dn.distributor_id = ndi.n_distributor_id 
left join level1.act_mst am 
	on am.n_distributor_id = ndi.n_distributor_id 
	and am.c_code = sp1.acno 
where
	date(sp1.vdt) between date_trunc('month', dateadd(month, -6, getdate())) and DATE(DATEADD(DAY, -1, GETDATE()))
	and sp1.vtyp = 'SB'
	and sp1.skull_opcode <> 'D'
	and (am.consol_category_fin in ('RETAIL') or am.consol_category_fin is null)
order by 2,3)
union 
(select
	distinct 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp1.vdt) as dt,
    'Overall Operation-Lead,Support - Net Revenue' kpi_name,
    1 kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase1 sp1
left join adhoc.namespace_distributor_id ndi 
	on ndi."namespace" = sp1.skull_namespace 
left join adhoc.distributor_name dn 
	on dn.distributor_id = ndi.n_distributor_id 
left join level1.act_mst am 
	on am.n_distributor_id = ndi.n_distributor_id 
	and am.c_code = sp1.acno 
where
	date(sp1.vdt) between date_trunc('month', dateadd(month, -6, getdate())) and DATE(DATEADD(DAY, -1, GETDATE()))
	and sp1.vtyp = 'SB'
	and sp1.skull_opcode <> 'D'
	and (am.consol_category_fin in ('RETAIL') or am.consol_category_fin is null)
order by 2,3)
union
(select
	distinct 
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp1.vdt) as dt,
    'Items' kpi_name,
    2600 kpi_value
from ahwspl__ahwspl_de__easysol.salepurchase1 sp1
left join adhoc.namespace_distributor_id ndi 
	on ndi."namespace" = sp1.skull_namespace 
left join adhoc.distributor_name dn 
	on dn.distributor_id = ndi.n_distributor_id 
left join level1.act_mst am 
	on am.n_distributor_id = ndi.n_distributor_id 
	and am.c_code = sp1.acno 
where
	date(sp1.vdt) between date_trunc('month', dateadd(month, -6, getdate())) and DATE(DATEADD(DAY, -1, GETDATE()))
	and sp1.vtyp = 'SB'
	and sp1.skull_opcode <> 'D'
	and (am.consol_category_fin in ('RETAIL') or am.consol_category_fin is null)
order by 2,3)
)base
left join 
(select
    ndi.n_distributor_id dist_id,
    dn."name" dist_name,
    date_trunc('month', sp1.vdt) as dt,
    'Net Revenue' kpi_name,
    sum(sp1.amt01) net_amt
from ahwspl__ahwspl_de__easysol.salepurchase1 sp1
left join adhoc.namespace_distributor_id ndi 
	on ndi."namespace" = sp1.skull_namespace 
left join adhoc.distributor_name dn 
	on dn.distributor_id = ndi.n_distributor_id 
left join ahwspl__ahwspl_de__easysol.dispatchstmt ds
	on sp1.skull_namespace = ds.skull_namespace
	AND sp1.vno = ds.vno
	and sp1.vdt = ds.vdt
	and sp1.vtyp = ds.vtype
	and ds.reversed <> 'Y'
	and ds.skull_opcode <> 'D'
left join level1.act_mst am 
	on am.n_distributor_id = ndi.n_distributor_id 
	and am.c_code = sp1.acno 
where
	date(ds.tagdt) between date_trunc('month', dateadd(month, -6, getdate())) and DATE(DATEADD(DAY, -1, GETDATE()))
	and sp1.vtyp = 'SB'
	and sp1.skull_opcode <> 'D'
	and (am.consol_category_fin in ('RETAIL') or am.consol_category_fin is null)
group by 1,2,3,4
order by 2,3)net_amt
	on base.dist_id = net_amt.dist_id
	and base.dt = net_amt.dt