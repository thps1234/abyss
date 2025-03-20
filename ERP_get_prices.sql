/* 
Для работы нужно заполнить 2 STVARV переменные:
1) SQL_DATE_GET_PRIC (Параметр). Тут указываешь дату, на которую хочешь сделать выборку. Дата в формате YYYYMMDD.
2) SQL_LIST_PRIC_WERKS (Множественный выбор) - тут указываешь список ТК. 4 символа, если требуется, то ставишь 0 сначала.
*/
with material_data_r as 
(
select a.*,
	   row_number() over (partition by a.matnr, a.werks order by a.matnr,
									             a.werks,
                                                 a.mdatbi
										       ) as seqnum
from (
select   a.artnr as matnr,
         (case 
          when d.MEINS = 'G' then 'KG'
          else d.MEINS 
          end ) as MEINS,
		   c.werks,
		   c.PR_CLUSTER,
		   c.PR_CLUSTER_GR,
		   c.vkorg,
		   f.low as date_select,
		   c.datab,
		   c.datbi,
		   g.mdatab,
		   g.mdatbi
from wlk1 a 
inner join t001w b on a.filia = b.kunnr
inner join ZSD_PRICE_CLUSTR c on c.werks = b.werks
inner join mara d on d.matnr = a.artnr
inner join tvarvc e on e.low = b.werks
				   and e.NAME = 'SQL_LIST_PRIC_WERKS'
				   and e.TYPE = 'S'
inner join tvarvc f on f.name = 'SQL_DATE_GET_PRIC'
			       and f.type = 'P'
				   and f.low >= a.datab
				   and f.low <= a.datbi
left join ZSD_PRICE_CLUSMT g on g.id = c.id
							and g.matnr = a.artnr
where d.mtart in ( '1HAW', '2FER' )
/* and a.artnr = '000000000000455636' */
  and a.artnr = a.STRNR
  order by a.artnr asc, b.werks asc, g.mdatbi desc
) a
where ( mdatab <= date_select
  and mdatbi >= date_select )
 or (datab <= date_select
  and datbi >= date_select )
),

rawdata as ( 
select matnr,
	   meins,
	   werks,
	   pr_cluster,
	   pr_cluster_gr,
	   vkorg,
	   date_select
    from material_data_r
where seqnum = 1
),

rawdata_a071_reg as (
select matnr,
       werks,
       vrkme,
       max(kbetr_zc01) kbetr_zc01,
       max(kbetr_vkp0) kbetr_vkp0,
	   max(datab_zc01) datab_zc01,
	   max(datbi_zc01) datbi_zc01,
	   max(datab_vkp0) datab_vkp0,
	   max(datbi_vkp0) datbi_vkp0
from (
select a.matnr,
       a.werks, 
       a.vrkme,
    (
      case when a.kschl = 'ZC01' then b.kbetr end
    ) as kbetr_zc01, 
    (
      case when a.kschl = 'VKP0' then b.kbetr end
    ) as kbetr_vkp0,
	( case when a.kschl = 'ZC01' then a.datab end ) as datab_zc01,
	( case when a.kschl = 'ZC01' then a.datbi end ) as datbi_zc01,
	( case when a.kschl = 'VKP0' then a.datab end ) as datab_vkp0,
	( case when a.kschl = 'VKP0' then a.datbi end ) as datbi_vkp0
  from 
    a071 a 
    inner join konp b on b.knumh = a.knumh 
    inner join rawdata c on a.werks = c.werks
                        and a.vkorg = c.vkorg
                        and a.matnr = c.matnr
                        and a.vrkme = c.meins
						and a.datab <= c.date_select
						and a.datbi >= c.date_select
  where a.vtweg = '10' 
    and a.kappl = 'V'
    and a.kschl in ( 'ZC01', 'VKP0' ) 
)
group by 
       matnr,
       werks,
       vrkme
),
rawdata_a071_act as (
select a.matnr,
       a.werks, 
       a.vrkme,
	   b.aktnr,
	   b.kbetr as kbetr_vka0,
	   a.datab as datab_vka0,
	   a.datbi as datbi_vka0
  from 
    a071 a 
    inner join konp b on b.knumh = a.knumh 
    inner join rawdata c on a.werks = c.werks
                        and a.vkorg = c.vkorg
                        and a.matnr = c.matnr
                        and a.vrkme = c.meins
						and a.datab <= c.date_select
						and a.datbi >= c.date_select
  where a.vtweg = '10' 
    and a.kappl = 'V'
    and a.kschl = 'VKA0'
),

rawdata_a965_reg as (
select matnr,
       werks,
       vrkme,
       max(kbetr_zc01) kbetr_zc01,
       max(kbetr_vkp0) kbetr_vkp0,
	   	   max(datab_zc01) datab_zc01,
	   max(datbi_zc01) datbi_zc01,
	   max(datab_vkp0) datab_vkp0,
	   max(datbi_vkp0) datbi_vkp0
from (
select a.matnr,
       c.werks, 
       a.vrkme,
    (
      case when a.kschl = 'ZC01' then b.kbetr end
    ) as kbetr_zc01, 
    (
      case when a.kschl = 'VKP0' then b.kbetr end
    ) as kbetr_vkp0,
	( case when a.kschl = 'ZC01' then a.datab end ) as datab_zc01,
	( case when a.kschl = 'ZC01' then a.datbi end ) as datbi_zc01,
	( case when a.kschl = 'VKP0' then a.datab end ) as datab_vkp0,
	( case when a.kschl = 'VKP0' then a.datbi end ) as datbi_vkp0
  from 
    a965 a 
    inner join konp b on b.knumh = a.knumh 
    inner join rawdata c on a.pltyp = c.pr_cluster
                        and a.vkorg = c.vkorg
                        and a.matnr = c.matnr
                        and a.vrkme = c.meins
						and a.datab <= c.date_select
						and a.datbi >= c.date_select
  where a.vtweg = '10' 
    and a.kappl = 'V'
    and a.kschl in ( 'ZC01', 'VKP0' ) 
)
group by 
       matnr,
       werks,
       vrkme
),
rawdata_a960_reg as (
select matnr,
       werks,
       vrkme,
       max(kbetr_zc01) kbetr_zc01,
       max(kbetr_vkp0) kbetr_vkp0,
	   max(datab_zc01) datab_zc01,
	   max(datbi_zc01) datbi_zc01,
	   max(datab_vkp0) datab_vkp0,
	   max(datbi_vkp0) datbi_vkp0
from (
select a.matnr,
       c.werks, 
       a.vrkme,
    (
      case when a.kschl = 'ZC01' then b.kbetr end
    ) as kbetr_zc01, 
    (
      case when a.kschl = 'VKP0' then b.kbetr end
    ) as kbetr_vkp0,
	( case when a.kschl = 'ZC01' then a.datab end ) as datab_zc01,
	( case when a.kschl = 'ZC01' then a.datbi end ) as datbi_zc01,
	( case when a.kschl = 'VKP0' then a.datab end ) as datab_vkp0,
	( case when a.kschl = 'VKP0' then a.datbi end ) as datbi_vkp0
  from 
    a960 a 
    inner join konp b on b.knumh = a.knumh 
    inner join rawdata c on a.pltyp = c.pr_cluster_gr
                        and a.vkorg = c.vkorg
                        and a.matnr = c.matnr
                        and a.vrkme = c.meins
						and a.datab <= c.date_select
						and a.datbi >= c.date_select
  where a.vtweg = '10' 
    and a.kappl = 'V'
    and a.kschl in ( 'ZC01', 'VKP0' ) 
)
group by 
       matnr,
       werks,
       vrkme
),
rawdata_a073_reg as (
select matnr,
       werks,
       vrkme,
       max(kbetr_zc01) kbetr_zc01,
       max(kbetr_vkp0) kbetr_vkp0,
	   max(datab_zc01) datab_zc01,
	   max(datbi_zc01) datbi_zc01,
	   max(datab_vkp0) datab_vkp0,
	   max(datbi_vkp0) datbi_vkp0
from (
select a.matnr,
       c.werks, 
       a.vrkme,
    (
      case when a.kschl = 'ZC01' then b.kbetr end
    ) as kbetr_zc01, 
    (
      case when a.kschl = 'VKP0' then b.kbetr end
    ) as kbetr_vkp0,
	( case when a.kschl = 'ZC01' then a.datab end ) as datab_zc01,
	( case when a.kschl = 'ZC01' then a.datbi end ) as datbi_zc01,
	( case when a.kschl = 'VKP0' then a.datab end ) as datab_vkp0,
	( case when a.kschl = 'VKP0' then a.datbi end ) as datbi_vkp0
  from 
    a073 a 
    inner join konp b on b.knumh = a.knumh 
    inner join rawdata c on a.vkorg = c.vkorg
                        and a.matnr = c.matnr
                        and a.vrkme = c.meins
						and a.datab <= c.date_select
						and a.datbi >= c.date_select
  where a.vtweg = '10' 
    and a.kappl = 'V'
    and a.kschl in ( 'ZC01', 'VKP0' ) 
)
group by 
       matnr,
       werks,
       vrkme
),

rawdata_union_price as (

select a.matnr,
	   a.meins,
       a.werks,
       b.kbetr_zc01 as kbetr_zc01_071,
       b.kbetr_vkp0 as kbetr_vkp0_071,
       c.kbetr_zc01 as kbetr_zc01_965,
       c.kbetr_vkp0 as kbetr_vkp0_965,
       d.kbetr_zc01 as kbetr_zc01_960,
       d.kbetr_vkp0 as kbetr_vkp0_960,
       e.kbetr_zc01 as kbetr_zc01_073,
       e.kbetr_vkp0 as kbetr_vkp0_073,
	   f.kbetr_vka0,
	   f.aktnr,
	   b.datab_zc01 as datab_zc01_071,
	   b.datbi_zc01 as datbi_zc01_071,
	   b.datab_vkp0 as datab_vkp0_071,
	   b.datbi_vkp0 as datbi_vkp0_071,
	   c.datab_zc01 as datab_zc01_965,
	   c.datbi_zc01 as datbi_zc01_965,
	   c.datab_vkp0 as datab_vkp0_965,
	   c.datbi_vkp0 as datbi_vkp0_965,
	   d.datab_zc01 as datab_zc01_960,
	   d.datbi_zc01 as datbi_zc01_960,
	   d.datab_vkp0 as datab_vkp0_960,
	   d.datbi_vkp0 as datbi_vkp0_960,
	   e.datab_zc01 as datab_zc01_073,
	   e.datbi_zc01 as datbi_zc01_073,
	   e.datab_vkp0 as datab_vkp0_073,
	   e.datbi_vkp0 as datbi_vkp0_073,
	   f.datab_vka0,
	   f.datbi_vka0
from rawdata a
left join rawdata_a071_reg b on b.matnr = a.matnr
                            and b.werks = a.werks
left join rawdata_a965_reg c on c.matnr = a.matnr
                            and c.werks = a.werks
left join rawdata_a960_reg d on d.matnr = a.matnr
                            and d.werks = a.werks
left join rawdata_a073_reg e on e.matnr = a.matnr
                            and e.werks = a.werks
left join rawdata_a071_act f on f.matnr = a.matnr
							and f.werks = a.werks

)

select matnr,
	   meins,
	   (SUBSTR(matnr,13,6)||'_'||meins) as set_view,
	   werks,
	   kbetr_vka0 as action_price,
	   datab_vka0 as promo_date_b,
	   datbi_vka0 as promo_date_e,
	   aktnr      as action_number,
	   ( case when kbetr_zc01_071 is not null then kbetr_zc01_071
			  when kbetr_zc01_965 is not null then kbetr_zc01_965
			  when kbetr_zc01_960 is not null then kbetr_zc01_960
			  else kbetr_zc01_073 
		end
	   )  as regular_card_price,
	   ( case when kbetr_zc01_071 is not null then datab_zc01_071
			  when kbetr_zc01_965 is not null then datab_zc01_965
			  when kbetr_zc01_960 is not null then datab_zc01_960
			  else datab_zc01_073 
		end
	   )  as reg_card_date_b,
	   ( case when kbetr_zc01_071 is not null then datbi_zc01_071
			  when kbetr_zc01_965 is not null then datbi_zc01_965
			  when kbetr_zc01_960 is not null then datbi_zc01_960
			  else datbi_zc01_073 
		end
	   )  as reg_card_date_e,
	   ( case when kbetr_vkp0_071 is not null then kbetr_vkp0_071
			  when kbetr_vkp0_965 is not null then kbetr_vkp0_965
			  when kbetr_vkp0_960 is not null then kbetr_vkp0_960
			  else kbetr_vkp0_073 
		end
	   )  as regular_price,
	   ( case when kbetr_zc01_071 is not null then datab_vkp0_071
			  when kbetr_zc01_965 is not null then datab_vkp0_965
			  when kbetr_zc01_960 is not null then datab_vkp0_960
			  else datab_vkp0_073 
		end
	   )  as reg_date_b,
	   ( case when kbetr_zc01_071 is not null then datbi_vkp0_071
			  when kbetr_zc01_965 is not null then datbi_vkp0_965
			  when kbetr_zc01_960 is not null then datbi_vkp0_960
			  else datbi_vkp0_073 
		end
	   )  as reg_date_b
 from rawdata_union_price 