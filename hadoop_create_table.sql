CREATE TABLE stg.st_city_sber( --название таблицы
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
        emptyValue "")
