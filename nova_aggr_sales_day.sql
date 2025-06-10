--8 минут считается в хадупе за 5 дней
--3 минуты в нове за месяц

CREATE TABLE dm.ttd_aggr_sales_day (
  zsaletype STRING,
  distr_chan STRING,
  plant STRING,
  lfl_ppy STRING,
  lfl_py STRING,
  lfl_cy STRING,
  zfrmttyp STRING,
  zfrmttyp_text STRING,
  zcomdir STRING,
  zcomdir_text STRING,
  zcity_code STRING,
  zcity_txtmd STRING,
  rt_promo STRING,
  rt_promoct STRING,
  rt_promoct_txt STRING,
  rt_promoth STRING,
  rt_promoth_txt STRING,
  material STRING,
  material_text STRING,
  base_uom STRING,
  apur_grp STRING,
  apur_grp_text STRING,
  rpa_wgh2 STRING,
  rpa_wgh2_text STRING,
  rpa_wgh3 STRING,
  rpa_wgh3_text STRING,
  rpa_wgh4 STRING,
  rpa_wgh4_text STRING,
  rtsaexcusv DECIMAL(17, 2),
  rtsaexcust DECIMAL(17, 2),
  zaltcost DECIMAL(17, 2),
  zrt_predr DECIMAL(17, 2),
  cpsaexcubu DECIMAL(17, 3),
  cpsaexcubu_pei DECIMAL(17, 3),
  vgo_margin DECIMAL(17, 2),
  calday DATE
)
PARTITIONED BY SPEC ( calday )
STORED AS ICEBERG
TBLPROPERTIES (
  'format-version'='2'
);



INSERT INTO TABLE dm.ttd_aggr_sales_day
select
  sales_tier2.zsaletype as zsaletype,
  sales_tier2.distr_chan as distr_chan,
  sales_tier2.plant as plant,
  lfl_ppy.rollinglfl as lfl_ppy,
  lfl_py.rollinglfl as lfl_py,
  lfl_cy.rollinglfl as lfl_cy,
  pl.zfrmttyp as zfrmttyp,
  pl.zfrmttyp_txt as zfrmttyp_text,
  pl.zcomdir as zcomdir,
  pl.zcomdir_txt as zcomdir_text,
  pl.city_code as zcity_code,
  pl.city_code_txt as zcity_txtmd,
  sales_tier2.rt_promo as rt_promo,
  pr.rt_promoct as rt_promoct,
  ct.rt_promoct_txt as rt_promoct_txt,
  pr.rt_promoth as rt_promoth,
  th.rt_promoth_txt as rt_promoth_txt,
  sales_tier2.material as material,
  sales_tier2.material_text as material_text,
  sales_tier2.base_uom as base_uom,
  rpa_wgh4_list.apur_grp as apur_grp,
  rpa_wgh4_list.pur_group_txt as apur_grp_text,
  rpa_wgh4_list.rpa_wgh2 as rpa_wgh2,
  rpa_wgh4_list.rpa_wgh2_txt as rpa_wgh2_text,
  rpa_wgh4_list.rpa_wgh3 as rpa_wgh3,
  rpa_wgh4_list.rpa_wgh3_txt as rpa_wgh3_text,
  sales_tier2.rpa_wgh4 as rpa_wgh4,
  rpa_wgh4_list.rpa_wgh4_txt as rpa_wgh4_text,
  cast(sales_tier2.rtsaexcusv as decimal(17, 2)) as rtsaexcusv,
	cast(sales_tier2.rtsaexcust as decimal(17, 2)) as rtsaexcust,
	cast(sales_tier2.zaltcost as decimal(17, 2)) as zaltcost,
	cast(sales_tier2.zrt_predr as decimal(17, 2)) as zrt_predr,
	cast(sales_tier2.cpsaexcubu as decimal(17, 3)) as cpsaexcubu,
	cast(sales_tier2.cpsaexcubu_pei as decimal(17, 3)) as cpsaexcubu_pei,
  cast(sales_tier2.vgo_margin as decimal(17, 2)) as vgo_margin,
	sales_tier2.calday as calday
from
  (select
    sales_tier1.calday,
    case
      when sales_tier1.calday between trunc(add_months(current_date(), -24), 'YEAR') and
                                      date_add(trunc(add_months(current_date(), -12), 'YEAR'), -1)
      then cast(year(sales_tier1.calday) * 100 + month(sales_tier1.calday) as int)
      when sales_tier1.calday between trunc(add_months(current_date(), -36), 'YEAR') and
                                      date_add(trunc(add_months(current_date(), - 24), 'YEAR'), -1) 
      then cast(year(years_add(sales_tier1.calday, 1)) * 100 + month(years_add(sales_tier1.calday, 1)) as int)   
      else 0
    end as m_lfl_ppy,
    case
      when sales_tier1.calday between trunc(add_months(current_date(), -12), 'YEAR') and
                                      date_add(trunc(current_date(), 'YEAR'), -1) 
      then cast(year(sales_tier1.calday) * 100 + month(sales_tier1.calday) as int)
      when sales_tier1.calday between trunc(add_months(current_date(), -24), 'YEAR') and
                                      date_add(trunc(add_months(current_date(), - 12), 'YEAR'), -1) 
      then cast(year(years_add(sales_tier1.calday, 1)) * 100 + month(years_add(sales_tier1.calday, 1)) as int)   
      else 0
    end as m_lfl_py,
    case
      when sales_tier1.calday between trunc(current_date(), 'YEAR') and
                                      date_add(trunc(add_months(current_date(), 12), 'YEAR'), -1)
      then cast(year(sales_tier1.calday) * 100 + month(sales_tier1.calday) as int)
      when sales_tier1.calday between trunc(add_months(current_date(), - 12), 'YEAR') and
                                      date_add(trunc(current_date(), 'YEAR'), -1) 
      then cast(year(years_add(sales_tier1.calday, 1)) * 100 + month(years_add(sales_tier1.calday, 1)) as int)   
      else 0
    end as m_lfl_cy,
    zsaletype,
    plant,
    distr_chan,
    rt_promo,
    sales_tier1.material,
    m.ctxt60_1 || m.ctxt60_2 as material_text,
    ifnull(sales_tier1.base_uom, m.base_uom) as base_uom,
    ifnull(sales_tier1.rpa_wgh4, m.rpa_wgh4) as rpa_wgh4,
    rtsaexcusv,
    cast(m.fc_taxrate as int) as taxrate,
    case
      when rtsaexcusv <> 0 then rtsaexcusv
      else round(rtsaexcusv * (1 + cast(m.fc_taxrate as int) / 100), 2)
    end as rtsaexcust,
    zaltcost,
    zrt_predr,
    cpsaexcubu,
    cpsaexcubu_pei,
    vgo_margin
  from
    (select
      calday,
      zsaletype,
      plant,
      distr_chan,
      rt_promo,
      material,
      base_uom,
      null as rpa_wgh4,
      round(sum(rtsaexcusv), 2) as rtsaexcusv,
      round(sum(rtsaexcust), 2) as rtsaexcust,
      round(sum(zaltcost), 2) as zaltcost,
      round(sum(zrt_predr), 2) as zrt_predr,
      round(sum(cpsaexcubu), 3) as cpsaexcubu,
      round(sum(
        case
          when base_uom = 'G' then 0.001
          else 1 
        end * cpsaexcubu
        ), 3) as cpsaexcubu_pei,
      round(sum(zicomargn), 2) as vgo_margin
    from dwh.ttd_pos_rec_itm
    where calday between '2025-01-01' and '2025-01-31'
    group by 
      calday,
      zsaletype,
      plant,
      distr_chan,
      rt_promo,
      material,
      base_uom
      
    union
    
    select
      calday,
      zsaletype,
      plant,
      distr_chan,
      rt_promo,
      material,
      base_uom,
      null as rpa_wgh4,
      sum(zval_inv),
      sum(rtsaexcust),
      sum(zaltcost),
      sum(0) as zrt_predr,
      sum(cpsaexcubu),
      round(sum(
        case
          when base_uom = 'G' then 0.001
          else 1
        end * cpsaexcubu
      ), 3) as cpsaexcubu_pei,
      round(sum(zicomargn), 2) as vgo_margin
    from dwh.ttd_zrtsdo05_t2
    where calday between '2025-01-01' and '2025-01-31'
      and distr_chan = '70'
      and zinstype in ('Y7', 'Y8')
    group by  calday,
      zsaletype,
      plant,
      distr_chan,
      rt_promo,
      material,
      base_uom) as sales_tier1
  join tmd_material as m
    on sales_tier1.material = m.material
  join tmd_calday as c
    on sales_tier1.calday = c.calday
  where m.apur_grp not in ('010','012','013','014','015','018','022','023','Z01','A00','S00','S01','S99', 'Z01')) as sales_tier2
  
join 
  (select distinct
    m.apur_grp,
    p.pur_group_txt,
    m.rpa_wgh2,
    r2.rpa_wgh2_txt,
    m.rpa_wgh3,
    r3.rpa_wgh3_txt,
    m.rpa_wgh4,
    r4.rpa_wgh4_txt
  from dwh.tmd_material as m
  join dwh.tmd_pur_group as p
    on m.apur_grp = p.pur_group
  join dwh.tmd_rpa_wgh2 as r2
    on m.rpa_wgh2 = r2.rpa_wgh2
  join dwh.tmd_rpa_wgh3 as r3
    on m.rpa_wgh3 = r3.rpa_wgh3
  join dwh.tmd_rpa_wgh4 as r4
    on m.rpa_wgh4 = r4.rpa_wgh4
  where zqvdata <> 'X') as rpa_wgh4_list
on sales_tier2.rpa_wgh4 = rpa_wgh4_list.rpa_wgh4

join vmd_plant_04 as pl
  on sales_tier2.plant = pl.plant

join tmd_rt_promo as pr
  on sales_tier2.rt_promo = pr.rt_promo
  
join tmd_rt_promoct as ct
  on pr.rt_promoct = ct.rt_promoct
  
join tmd_rt_promoth as th
  on pr.rt_promoth = th.rt_promoth
  
left outer join tmd_plant_lfl as lfl_cy
  on lfl_cy.calmonth = sales_tier2.m_lfl_cy
 and lfl_cy.plant = sales_tier2.plant
 
left outer join tmd_plant_lfl as lfl_py
  on lfl_py.calmonth = sales_tier2.m_lfl_py
 and lfl_py.plant = sales_tier2.plant
 
left outer join tmd_plant_lfl as lfl_ppy
  on lfl_ppy.calmonth = sales_tier2.m_lfl_ppy
 and lfl_ppy.plant = sales_tier2.plant
  
order by sales_tier2.calday, sales_tier2.material, sales_tier2.plant;