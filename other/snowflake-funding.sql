-- adapted from postgresql code at 
-- https://github.com/Angel-Studios/chosen-hydra/blob/master/lib/chosen_hydra/jobs/global_season_funding.ex#L109-L156

drop table if exists orders;
create transient table orders (created date, price_paid number, user_id text);

insert into orders (created, price_paid, user_id)
  select p.created, (offer:price)::int as price_paid, o.user_id
    from STITCH__HYDRABIM__MARKET_PURCHASEDOFFER as p
    join STITCH__HYDRABIM__MARKET_ORDER as o on o.id = p.order_id
    where p.created > $season_three_completed_on
    AND refunded = FALSE
    AND (project_id = 'the-chosen' or project_id is null);

select * from orders limit 4;
select count(*) from orders;

    select created,
        sum(price_paid * case
            when created < $start_funding_slowdown_date then 0.3
            else 0.2
            end) over (order by created asc rows between unbounded preceding and current row)
        as running_total,
        user_id
    from orders

    select 
      s.a as episode, 
        (s.a - 1) * $episode_cost_in_pennies as ep_start,  
        s.a * $episode_cost_in_pennies as ep_end 
        from (select row_number() over (order by seq4()) from table(generator(rowcount => $number_of_episodes))) as s(a)


drop table if exists orders;
-------------------------------------

with orders as (
    select p.created, (offer:price)::int as price_paid, o.user_id
    from STITCH__HYDRABIM__MARKET_PURCHASEDOFFER as p
    join STITCH__HYDRABIM__MARKET_ORDER as o on o.id = p.order_id
    where p.created > $season_three_completed_on
    AND refunded = FALSE
    AND (project_id = 'the-chosen' or project_id is null)
    ), running_totals as (
    select created,
        sum(price_paid * case
            when created < $start_funding_slowdown_date then 0.3
            else 0.2
            end) over (order by created asc rows between unbounded preceding and current row)
        as running_total,
        user_id
    from orders
    ), episodes as (
    select s.a as episode, 
        (s.a - 1) * $episode_cost_in_pennies as ep_start,  
        s.a * $episode_cost_in_pennies as ep_end 
        from (select row_number() over (order by seq4()) from table(generator(rowcount => $number_of_episodes))) as s(a)
    ), funding_episodes as (
    select episode,
        episodes.ep_end,
        episodes.ep_start,
        count(distinct(user_id)) as num_of_users,
        max(running_total) as total_raised,
        max(running_total) - episodes.ep_start as raised,
        (max(running_total) - episodes.ep_start) / $episode_cost_in_pennies as percent_complete
        from episodes left join lateral (
        select user_id, running_total from running_totals
        where running_totals.running_total between episodes.ep_start and episodes.end
        ) counts on TRUE
        group by episode, episodes.ep_end, episodes.ep_start
        order by episode, episodes.ep_end, episodes.ep_start
    ) select
        episode,
        num_of_users,
        (raised / 100)::int as raised,
        (percent_complete * 100.0)::int,
        (total_raised / 100)::int as total_raised,

        ( select (sum (IFF(created > DATEADD('DAY',-7,CURRENT_DATE()), price_paid, 0 )) / 100 * .3)::int 
        from orders) as past_seven_days,

        --(select ((sum(price_paid) filter( where created > DATEADD('DAY',-7,CURRENT_DATE()) )) / 100 * .3)::int
        --  from orders) as past_seven_days,

        (select count(distinct(user_id)) from orders) as total_users
        from funding_episodes
        where raised is not null
        and percent_complete <= 0.99
        limit 1

/*
set market_purchasedoffer = 'STITCH__HYDRABIM__MARKET_PURCHASEDOFFER';
set market_order = 'STITCH__HYDRABIM__MARKET_ORDER';
set season_three_completed_on = '2021-12-22'::date;
set start_funding_slowdown_date = '2022-06-17'::date;
set season_required = 24000000;
set number_of_episodes = 8;
set episode_cost = $season_required / $number_of_episodes;
set episode_cost_in_pennies = $episode_cost * 100;
set season_currently_funding = 4;
show variables;

*/