--стейджинговая таблица на csv
CREATE TABLE stg.st_city_sber(
    st_id string, --список полей, все стринг
    st_city_name_sber string,
    st_city_name_sber_2 string
)
USING CSV
TBLPROPERTIES ('external.table.purge'='TRUE')
OPTIONS (delimiter "\u007E",
        header "true",
        quoteAll "false",
        quote "",
        emptyValue "");

--целевая таблица
CREATE EXTERNAL TABLE focus_data.st_city_media( --название таблицы
		etl_dttm timestamp,
	    type_metric_media string,
	    st_city_name string,
	    st_city_id int,
	    okhvat decimal(17, 2),
	    sov_dolya_golosa decimal(17, 2),
	    reyting_sredi_konkurentov int,
	    month date
)
stored as orc
 TBLPROPERTIES (  
 'TRANSLATED_TO_EXTERNAL'='TRUE', 
 'bucketing_version'='2', 
 'external.table.purge'='TRUE');

--хист-таблица с партицией
CREATE EXTERNAL TABLE stg.st_city_media_hist( --название таблицы
	    type_metric_media string,
	    st_city_name string,
	    st_city_id string,
	    okhvat string,
	    sov_dolya_golosa string,
	    reyting_sredi_konkurentov string,
	    month string
)
PARTITIONED BY (loadday string)
stored as orc
 TBLPROPERTIES (  
 'TRANSLATED_TO_EXTERNAL'='TRUE', 
 'bucketing_version'='2', 
 'external.table.purge'='TRUE')
 ;
