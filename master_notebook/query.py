ar_query = '''
with drivers_split as (
    {drivers_cte}),
s as (
    select s.ID, s.ID_DRIVER, s.DATE_SUGGEST, s.ID_ORDER, s."ACTION",
           row_number() over (partition by s.ID_DRIVER, s.ID_ORDER order by s.DATE_SUGGEST) rn
    from REPLICA.FAIRBOT_SUGGESTS_SUCCESS s
    where to_date(s.DATE_SUGGEST) between '{date_from}' and '{date_to}'),
c as (
    select c.ORDER_ID, c.DRIVER_ID, c.CONVENIENCE_TYPE, c.SPECIFICATION_NAME,
           row_number() over (partition by c.DRIVER_ID, c.ORDER_ID order by c.EVENT_TIME) rn
    from REPLICA_CH.DRIVER_POINTS_REQUEST_CONVENIENCE c
    where to_date(c.EVENT_TIME) between '{date_from}' and '{date_to}')
select
       ds.ID DRIVER_RK, o.LOCALITY_RK, o.LOCALITY_NM, to_date(s.DATE_SUGGEST) dt,
     ds.exp_group,
    s.ID fss_id,
    c.CONVENIENCE_TYPE, c.SPECIFICATION_NAME,
    case
        when o.EXP_DIST_KM between 0 and 3 then '0 - 3 km'
        when o.EXP_DIST_KM between 3 and 6 then '3 - 6 km'
        when o.EXP_DIST_KM between 6 and 10 then '6 - 10 km'
        when o.EXP_DIST_KM > 10 then '+10 km'
        else 'no data' end distance_category,
    case when s."ACTION" in (1,-2,3,4) then 1 else 0 end accept,
    case when s."ACTION" in (-1,-3) then 1 else 0 end "REJECT",
    case when s."ACTION" in (0,-4,-5) then 1 else 0 end fraud
from drivers_split ds
join s on s.ID_DRIVER = ds.ID
join emart."ORDER" o on o.ORDER_RK = s.ID_ORDER
left join c on c.ORDER_ID = s.ID_ORDER and c.DRIVER_ID = s.ID_DRIVER and c.rn = s.rn
'''

of2r_query = '''
with drivers_split as (
    {drivers_cte}),
s as (
    select s.ID_DRIVER, ds.exp_group,
           count(s.ID) offers
    from REPLICA.FAIRBOT_SUGGESTS_SUCCESS s
    join drivers_split ds on ds.ID = s.ID_DRIVER
    where to_date(s.DATE_SUGGEST) between '{date_from}' and '{date_to}'
    group by 1, 2),
o as (
    select o.DRIVER_RK, o.LOCALITY_RK, sum(o.STATUS_CD='CP') rides
    from EMART."ORDER" o
    WHERE to_date(o.LOCAL_ORDER_DTTM) between '{date_from}' and '{date_to}'
        and o.LOCALITY_RK in ({locality_id})
    group by 1, 2)
select o.LOCALITY_RK, l.SHORT_NAME LOCALITY_NM, s.ID_DRIVER DRIVER_RK,
     s.exp_group,
       s.offers, ZEROIFNULL(o.rides) rides
from s
left join o on s.ID_DRIVER = o.DRIVER_RK
join md.LOCALITY l on l.LOCALITY_RK = o.LOCALITY_RK
'''

mph_query = '''
    with b as (
        SELECT dcar.DRIVER_RK, max(dcar.BRAND_FLG) BRAND_FLG
        FROM EMART.DRIVER_CAR_ADS_REPORT dcar
        WHERE ORDER_ASSIGN_DT between '{date_from}' and '{date_to}'
        group by 1),
    rfm as (
        SELECT ds.DRIVER_ID DRIVER_RK, ds.SEGMENT_ID,
               case
                    when ds.SEGMENT_ID = 0 then 'BOTTOM'
                    when ds.SEGMENT_ID = 1 then 'LOW'
                    when ds.SEGMENT_ID = 2 then 'MIDDLE'
                    when ds.SEGMENT_ID = 3 then 'HIGH'
                    when ds.SEGMENT_ID = 4 then 'TOP'
                    end segment
        FROM REPLICA.DRIVER_SEGMENTATION_RFM ds
        where ds."DATE" = '{date_from}'),
    drivers_split as (
        {drivers_cte}),
    sh as (
        select DRIVER_RK, cast(ZEROIFNULL(SUM(DRIVER_STAY_DUR_SEC)/3600) as float) supply_hours,
               cast(ZEROIFNULL(SUM(case when state_tcd = 3 then DRIVER_STAY_DUR_SEC end)/3600) as float) on_trip
        from REPLICA_MART.DRIVER_MOVING_AGG_DAILY
        where STATE_TCD in (1,2,3)
            and LOCALITY_RK = {locality_id}
            and BUSINESS_DT between '{date_from}' and '{date_to}'
        group by 1),
    o as (
        select o.LOCALITY_RK, o.DRIVER_RK,
             ds.exp_group,

               max(CASE WHEN c.PARTNER_BUSINESS_TYPE in (1,3) then 'Park' else 'Not_park' end) park,

               cast(ZEROIFNULL(SUM(ic.driver_bill_amt +ic.di_mfg + ic.di_welcome_dxgy + ic.di_power_dxgy
                   + ic.di_other_dxgy + ic.di_main_dxgy+ ic.di_guaranteed_amt_per_hour)) as float) money,
               cast(ZEROIFNULL(SUM(ic.COMMISSION_TOTAL_AMT)) as float) commission,
               cast(ZEROIFNULL(SUM(di_mfg + di_welcome_dxgy + di_power_dxgy + di_other_dxgy + di_main_dxgy
                    + di_guaranteed_amt_per_hour + di_geo_minimal_amt + di_gold_minimal_amt
                    + di_base_minimal_amt + di_silver_minimal_amt +
                          di_other_incentives_amt)) as float) DI,
            cast(ZEROIFNULL(sum(case when o.STATUS_CD='CP' then 1 else 0 end)) as float) trips
        from EMART."ORDER" o
        join drivers_split ds on ds.ID = o.DRIVER_RK
        left join replica_mart.incentive_comission ic on o.order_rk = ic.ORDER_ID
        left join REPLICA.COMPANY c on c.id = o.DRIVER_COMPANY_ACCOUNT_RK
        WHERE to_date(o.LOCAL_ORDER_DTTM) between '2021-12-08' and '2021-12-14'
        group by 1, 2, 3)
    select o.LOCALITY_RK, l.short_name LOCALITY_NM, o.DRIVER_RK,
           o.park, case when b.BRAND_FLG = 1 then 'brand' else 'not_brand' end brand, r.segment,
           o.exp_group,

           o.money money_gross, o.money - o.commission money_net,
           o.money - o.DI organic_money_gross, o.money - o.commission - o.DI organic_money_net,

           sh.supply_hours,
           o.trips, sh.on_trip
    from o
    left join md.LOCALITY l on l.LOCALITY_RK = o.LOCALITY_RK
    left join sh on sh.DRIVER_RK = o.DRIVER_RK
    left join b on b.DRIVER_RK = o.DRIVER_RK
    left join rfm r on r.DRIVER_RK = o.DRIVER_RK
'''

copt_query = '''
with drivers_split as (
    {drivers_cte})
select o.LOCALITY_RK, o.LOCALITY_NM, o.DRIVER_RK,
         ds.exp_group,
       cast(count(o.ORDER_RK) as float) rides,
       cast(sum(ZEROIFNULL(ic.COMMISSION_TOTAL_AMT - ic.DI_TOTAL_AMT - ic.CI_TOTAL_AMT)) as float) contribution,
       cast(sum(ZEROIFNULL(ic.DI_TOTAL_AMT)) as float) DI,
       cast(sum(ZEROIFNULL(ic.COMMISSION_TOTAL_AMT)) as float) COMMISSION,
       cast(ZEROIFNULL(SUM(o.CLIENT_BILL_AMT)) as float) GMV,
       cast(ZEROIFNULL(SUM(ic.COMMISSION_TOTAL_AMT - ic.DI_TOTAL_AMT - ic.CI_TOTAL_AMT)/
            count(o.ORDER_RK)) as float) COPT,
       cast(ZEROIFNULL(SUM(ic.DI_TOTAL_AMT)/count(o.ORDER_RK)) as float) DIPT,
       cast(ZEROIFNULL(SUM(ic.COMMISSION_TOTAL_AMT)/count(o.ORDER_RK)) as float) COMPT,
       cast(ZEROIFNULL(SUM(o.CLIENT_BILL_AMT)/count(o.ORDER_RK)) as float) GMVPT
from EMART."ORDER" o
join drivers_split ds on ds.ID = o.DRIVER_RK
LEFT JOIN REPLICA_MART.INCENTIVE_COMISSION ic on ic.ORDER_ID = o.ORDER_RK and ic.DRIVER_ID = o.DRIVER_RK
WHERE to_date(o.LOCAL_ORDER_DTTM) between '{date_from}' and '{date_to}'
    AND o.STATUS_CD = 'CP'
group by 1, 2, 3, 4
'''

dist_query = '''
with drivers_split as (
    {drivers_cte})
select o.LOCALITY_RK, o.LOCALITY_NM, o.ORDER_RK, o.DRIVER_RK,
         ds.exp_group,
       o.EXP_DIST_KM
from EMART."ORDER" o
join drivers_split ds on o.DRIVER_RK = ds.ID
    and o.ORDER_DTTM between '{date_from}' and '{date_to}'
where o.STATUS_CD = 'CP'
'''

churn_query = '''
    with drivers_split as (
        {drivers_cte}),
    lo as (
        select o.LOCALITY_RK, o.DRIVER_RK, to_date(max(o.LOCAL_ORDER_DTTM)) last_order
        from EMART."ORDER" o
        where o.STATUS_CD = 'CP'
            and o.DRIVER_RK in (select distinct id from drivers_split)
        group by o.LOCALITY_RK, o.DRIVER_RK)
    select lo.LOCALITY_RK, l.SHORT_NAME LOCALITY_NM, ds.ID DRIVER_RK,
               ds.exp_group,
               case when lo.last_order between '{date_from}' and to_date('{date_from}') 
                                                                + INTERVAL '6' DAY then 1 else 0 end churn_7day,
               case when lo.last_order between '{date_from}' and to_date('{date_from}')
                                                                + INTERVAL '13' DAY then 1 else 0 end churn_14day,
               case when lo.last_order between '{date_from}' and to_date('{date_from}')
                                                                + INTERVAL '20' DAY then 1 else 0 end churn_21day
    from drivers_split ds
    join lo on lo.DRIVER_RK = ds.ID
    left join md.LOCALITY l on l.LOCALITY_RK = lo.LOCALITY_RK
'''

queries = [ar_query, of2r_query, mph_query, copt_query, dist_query, churn_query]