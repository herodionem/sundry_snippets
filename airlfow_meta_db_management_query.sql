## Query to help manage dag runs for a process in Airflow to help speed up backfill time
update
    task_instance t join dag_run d using(dag_id, execution_date)
    join
        ## Give me an ordered list of dag_ids with a status other than (e.g. running) to focus on.
        ### This beast is necessitated by the fact that mysql 5.7 doesn't support window functions. Otherwise we could just use 5 or six lines of code here...
        (
            select tbl1.dag_id as kpi_id, tbl3.kpi_state, tbl3.action_id, tbl3.action_state, tbl1.execution_date, count(tbl2.execution_date) as `rank`, tbl1.running_count
            from
                (
                    select d.dag_id, d.execution_date, d.state, count(t.execution_date) as `rank`, runcnt.running_count
                    from dag_run d
                        left join dag_run t on d.dag_id = t.dag_id and d.execution_date < t.execution_date
                        ## Get count of dag_runs running before most recent 2 weeks
                        left join (select dag_id, count(*) as running_count from dag_run where dag_id like '%kpi%' and state = 'running' and execution_date < @wb - interval 2 week group by dag_id) as runcnt on d.dag_id = runcnt.dag_id
                    where d.dag_id like '%kpi%'
                      and d.state not in ('success', 'running')
                      and d.execution_date < @wb - interval 2 week
                    group by d.dag_id, d.execution_date
                    order by d.dag_id, d.execution_date desc
                ) tbl1
                LEFT JOIN
                (
                    select d.dag_id, d.execution_date, d.state, count(t.execution_date) as `rank`, runcnt.running_count
                    from dag_run d
                        left join dag_run t on d.dag_id = t.dag_id and d.execution_date < t.execution_date
                        ## Get count of dag_runs running before most recent 2 weeks
                        left join (select dag_id, count(*) as running_count from dag_run where dag_id like '%kpi%' and state = 'running' and execution_date < @wb - interval 2 week group by dag_id) as runcnt on d.dag_id = runcnt.dag_id
                    where d.dag_id like '%kpi%'
                      and d.state not in ('success', 'running')
                      and d.execution_date < @wb - interval 2 week
                    group by d.dag_id, d.execution_date
                    order by d.dag_id, d.execution_date desc
                ) tbl2 on tbl1.dag_id = tbl2.dag_id and tbl1.execution_date < tbl2.execution_date
                LEFT JOIN
                (
                    select d1.dag_id as kpi_id, d1.state as kpi_state, d2.dag_id as action_id, d2.state as action_state, d1.execution_date, count(*)
                    from dag_run d1 join dag_run d2
                        on d1.execution_date = d2.execution_date
                            and d1.dag_id like 'kpi%' and d2.dag_id like 'action%'
                            and substring_index(d1.dag_id, '_', -1) = substring_index(d2.dag_id, '_', -1)
                    group by kpi_id, action_id, d1.execution_date
                ) tbl3 on tbl1.execution_date = tbl3.execution_date and tbl1.dag_id = tbl3.kpi_id
            group by tbl1.dag_id, tbl1.execution_date
            having `rank` < 3 - ifnull(tbl1.running_count, 0)
            order by tbl1.dag_id, `rank`
        ) run_next
        on (t.dag_id = run_next.kpi_id or t.dag_id = run_next.action_id) and t.execution_date = run_next.execution_date
SET
    t.state = CASE
            #<!> action on `external_sensor_check` should always skip when set to reschedule
            #<!> `external_sensor_check` seems it should just skip if it's failed and tries exceed max tries... don't know WHY it won't soft fail yet...
            WHEN ((t.state = 'up_for_reschedule'
                      OR (t.state like '%fail%' and t.try_number >= t.max_tries))
                 and t.task_id like '%external_sensor_check%'
                 and t.dag_id like '%action%')
                THEN 'skipped'
            #<!> pretty much no good reason kpi task should fail outright (bc the only reasons it might seem to revolve around scheduler issues)
            WHEN (t.dag_id like '%kpi%' or t.dag_id like '%action%')
                 and (t.state like '%fail%')
                THEN NULL
            ELSE t.state
       END,
    #<!> pretty much no good reason kpi task should fail outright (bc the only reasons it might seem to revolve around scheduler issues)
    t.try_number = CASE
            WHEN (t.dag_id like '%kpi%' and t.state like '%fail%')
                THEN 0
            ELSE t.try_number
        END,
    d.state = CASE WHEN d.state = 'failed' THEN 'running' ELSE d.state END
where (d.dag_id like '%kpi%' and (t.state not in ('success', 'removed') or t.state is NULL))
   or (d.dag_id like '%action%' and d.state not in ('success', 'removed'))
;


