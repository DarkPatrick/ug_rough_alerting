with dau_h as (
    select
        toStartOfHour(datetime) as hour,
        source,
        uniq(unified_id) as dau
    from
        default.ug_rt_events_app
    where
        date in (today(), today() - 7)
    and
        toHour(datetime) = toHour(now() - interval 1 hour)
    group by
        hour,
        source
),

events_h as (
    select
        toStartOfHour(datetime) as hour,
        source as source,
        event as event,
        uniq(unified_id) as unified_cnt
    from
        default.ug_rt_events_app
    where
        date in (today(), today() - 7)
    and
        hour < toStartOfHour(now())
    group by
        hour,
        source,
        event
),

data_h as (
    select
        toUnixTimestamp(hour) as hour,
        source,
        event,
        dau,
        unified_cnt,
        unified_cnt / dau as event_part
    from
        events_h
    inner join
        dau_h
    on
        events_h.hour = dau_h.hour
    and
        events_h.source = dau_h.source
)

select
    *
from
    data_h
order by
    hour
