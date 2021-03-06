-- script to get touch possible pairs of touch on and touch off
-- note there's a two hour window which may need to be extended.
with all_touch_on as
(
   select dt.*,
   sl.location_type,
   sl.suburb,
   cast(sl.postcode as integer) as postcode,
   sl.city as city,
   sl.region,
   sl.region_type,
   sl.lat,
   sl.lng
   from default.datathon_transactions dt
   join default.datathon_stop_locations sl
   on dt.StopID = sl.StopID
   where 1=1
   --and dt.year = 2015
   --and dt.week = 29
   and scan_type = 'On'
), all_touch_off as
(
   select dt.*,
   sl.location_type,
   sl.suburb,
   cast(sl.postcode as integer) as postcode,
   sl.city as city,
   sl.region,
   sl.region_type,
   sl.lat,
   sl.lng
   from default.datathon_transactions dt
   join default.datathon_stop_locations sl
   on dt.StopID = sl.StopID
   where 1=1
   and dt.year = 2015
   and dt.week = 29
   and scan_type = 'Off'
)
select ton.mode,
  ton.datetime as touch_on_datetime,
  tof.datetime as touch_off_datetime,
  ton.cardid,
  ton.card_type_id,
  ton.vehicleid,
  ton.parentroute,
  ton.routeid,
  ton.stopid as touch_on_stopid,
  tof.stopid as touch_off_stopid,
  ton.location_type as touch_on_location_type,
  tof.location_type as touch_off_location_type,
  ton.suburb as touch_on_suburb,
  tof.suburb as touch_off_suburb,
  cast(ton.postcode as integer) as touch_on_postcode,
  cast(tof.postcode as integer) as touch_off_postcode,
  ton.city as touch_on_city,
  tof.city as touch_off_city,
  ton.region as touch_on_region,
  tof.region as touch_off_region,
  ton.region_type as touch_on_region_type,
  tof.region_type as touch_off_region_type,
  ton.lat as touch_on_lat,
  ton.lng as touch_on_lon,
  tof.lat as touch_off_lat,
  tof.lng as touch_off_lon,
  date_diff(
    'second',
    cast(ton.datetime as timestamp),
    cast(tof.datetime as timestamp)
  ) as time_diff_seconds
from all_touch_on ton
join all_touch_off tof
on ton.cardid = tof.cardid
and (ton.year = tof.year) -- doesn't count NYE
and (ton.week = tof.week or ton.week + 1 % 52 = tof.week or ton.week + 1 % 53 = tof.week)
and date_diff(
  'second',
  cast(ton.datetime as timestamp),
  cast(tof.datetime as timestamp)
) between 0 and 7200
limit 100
