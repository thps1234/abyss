CREATE TABLE dwh.tmd_plant_lfl
(   
calmonth int,
plant string,
annuallfl string,
rollinglfl string
 ) 
STORED AS ICEBERG 
--LOCATION 's3a://nova1-bi/dwh/tmd_calday' 
TBLPROPERTIES (
'STATS_GENERATED'='TASK', 
'format'='iceberg/parquet', 
'impala.lastComputeStatsTime'='1743606043', 
'owner'='ext_user00', 
'table_type'='ICEBERG', 
'write.delete.mode'='merge-on-read', 
'write.distribution-mode'='hash', 
'write.merge.mode'='merge-on-read', 
'write.metadata.delete-after-commit.enabled'='true', 
'write.metadata.metrics.default'='full', 
'write.metadata.metrics.max-inferred-column-defaults'='100', 
'write.metadata.previous-versions-max'='10', 
'write.parquet.compression-codec'='zstd', 
'write.parquet.compression-level'='3', 
'write.update.mode'='merge-on-read')
;

insert into dwh.tmd_plant_lfl
select 
  cast(calmonth as int) as calmonth,
  plant as plant,
  case 
    when ZT_LOPDAT = '00000000' then
      case 
        when ZT_LCLDAT = '00000000' then 'NSO CY'
        else 'Closed'
      end
    else
      case 
        when ZT_LCLDAT <> '00000000' and left( ZT_LCLDAT, 4 ) <= left(calmonth, 4) then 'Closed'
        else
          case 
            when (cast(left(calmonth, 4) as int) - cast(left(ZT_LOPDAT, 4) as int)) <= 0 then 'NSO CY'
            when (cast(left(calmonth, 4) as int) - cast(left(ZT_LOPDAT, 4) as int)) = 1 then 'NSO PY'
            else 'LFL'
          end
      end
  end as annuallfl,

  case 
    when ZT_LOPDAT = '00000000' then
      case 
        when ZT_LCLDAT = '00000000' then 'NSO CY'
        else 'Closed'
      end
    else
    case 
      when ZT_LCLDAT <> '00000000' and left(ZT_LCLDAT, 6) <= calmonth then 'Closed'
      else
        case 
          when (cast(calmonth as int) - cast(left(ZT_LOPDAT, 6) as int)) <= 100 then
            case 
              when (cast(left(calmonth, 4) as int) - cast(left(ZT_LOPDAT, 4) as int)) <= 0 then 'NSO CY'
              when (cast(left(calmonth, 4) as int) - cast(left(ZT_LOPDAT, 4) as int)) = 1 then 'NSO PY'
              else 'LFL'
            end
          else 'LFL'
         end
      end
  end as rollinglfl
  from dwh.tmd_plant
  cross join (
    select distinct calmonth from dwh.tmd_calday
    where calyear between 2020 and 2025
  ) as mon
  ;
