with dau_d as (
    select
        date,
        source,
        uniq(unified_id) as dau
    from
        default.ug_rt_events_app
    where
        date in (today(), today() - 7)
    and
        -- toHour(datetime) < toHour(now())
        toHour(datetime) = toHour(now() - interval 1 hour)
    group by
        date,
        source
),

events_d as (
    select
        date,
        source as source,
        event as event,
        uniq(unified_id) as unified_cnt
    from
        default.ug_rt_events_app
    where
        date in (today(), today() - 7)
    and
        toHour(datetime) < toHour(now())
    group by
        date,
        source,
        event
),

data_d as (
    select
        toUnixTimestamp(date) as date,
        source,
        event,
        dau,
        unified_cnt,
        unified_cnt / dau as event_part
    from
        events_d
    inner join
        dau_d
    on
        events_d.date = dau_d.date
    and
        events_d.source = dau_d.source
)


select
    *
from
    data_d
order by
    date