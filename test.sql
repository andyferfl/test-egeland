/*Написать SQL-запрос, который будет рассчитывать среднее время 
ответа для каждого менеджера/пары менеджеров. 
Расчёт должен учитывать следующее: 
• если в диалоге идут несколько сообщений подряд от 
  клиента или менеджера, то при расчёте времени ответа 
  надо учитывать только первое сообщение из каждого блока; 
• менеджеры работают с 09:30 до 00:00, поэтому нерабочее время 
  не должно учитываться в расчёте среднего времени ответа, т.е. 
  если клиент написал в 23:59, а менеджер ответил в 09:30 – время 
  ответа равно одной минуте; 
• ответы на сообщения, пришедшие ночью также нужно учитывать.*/


--Считать среднее время
with tmp as
(
select 
entity_id,
"type",
created_by,
created_at,
LAG("type") over (partition by entity_id order by created_at) as prev_type
from test.chat_messages
),
first_messages as
(
select
*,
LAG(created_at) over (partition by entity_id order by created_at) as prev_created_at
from tmp
where ("type" = 'incoming_chat_message' and (prev_type is null or prev_type <> 'incoming_chat_message')) or
("type" = 'outgoing_chat_message' and (prev_type is null or prev_type <> 'outgoing_chat_message'))
),
response_times as 
(
select
*,
case 
when (to_timestamp(prev_created_at) at time zone 'MSK')::time between '00:00:00'::time and '09:30:00'::time
then (created_at - prev_created_at) - 
	 ((9*3600)+(30*60)) + 
	 (extract(second from (to_timestamp(prev_created_at) at time zone 'MSK') - CONCAT((to_timestamp(prev_created_at) at time zone 'MSK')::date, ' 00:00:00')::timestamp)) +
	 (extract(minute from (to_timestamp(prev_created_at) at time zone 'MSK') - CONCAT((to_timestamp(prev_created_at) at time zone 'MSK')::date, ' 00:00:00')::timestamp)*60) +
	 (extract(hour from (to_timestamp(prev_created_at) at time zone 'MSK') - CONCAT((to_timestamp(prev_created_at) at time zone 'MSK')::date, ' 00:00:00')::timestamp)*3600)
when 
( (to_timestamp(created_at) at time zone 'MSK')::date - (to_timestamp(prev_created_at) at time zone 'MSK')::date) >= 1
then
(created_at - prev_created_at) - (((9*3600)+(30*60))*((to_timestamp(created_at) at time zone 'MSK')::date - (to_timestamp(prev_created_at) at time zone 'MSK')::date))
else (created_at - prev_created_at)
end response_time
from first_messages
where "type" = 'outgoing_chat_message' and prev_type is not null
)
select
m.mop_id manager_id,
m.name_mop manager_name,
r.rop_name responsible_name,
avg(rt.response_time) avg_response_time
from response_times rt
join test.managers m on m.mop_id = rt.created_by
join test.rops r on r.rop_id = m.rop_id::int
where rt.response_time > 0
group by 1,2,3;


-- Данные для визуализации
-- https://datalens.yandex/g1m63r29g8je3
with tmp as
(
select 
entity_id,
"type",
created_by,
created_at,
LAG("type") over (partition by entity_id order by created_at) as prev_type
from test.chat_messages
),
first_messages as
(
select
*,
LAG(created_at) over (partition by entity_id order by created_at) as prev_created_at
from tmp
where ("type" = 'incoming_chat_message' and (prev_type is null or prev_type <> 'incoming_chat_message')) or
("type" = 'outgoing_chat_message' and (prev_type is null or prev_type <> 'outgoing_chat_message'))
),
response_times as 
(
select
entity_id,
"type",
created_by,
(to_timestamp(created_at) at time zone 'MSK') outgoing_timestamp,
(to_timestamp(prev_created_at) at time zone 'MSK') incoming_timestamp,
case 
when (to_timestamp(prev_created_at) at time zone 'MSK')::time between '00:00:00'::time and '09:30:00'::time
then (created_at - prev_created_at) - 
	 ((9*3600)+(30*60)) + 
	 (extract(second from (to_timestamp(prev_created_at) at time zone 'MSK') - CONCAT((to_timestamp(prev_created_at) at time zone 'MSK')::date, ' 00:00:00')::timestamp)) +
	 (extract(minute from (to_timestamp(prev_created_at) at time zone 'MSK') - CONCAT((to_timestamp(prev_created_at) at time zone 'MSK')::date, ' 00:00:00')::timestamp)*60) +
	 (extract(hour from (to_timestamp(prev_created_at) at time zone 'MSK') - CONCAT((to_timestamp(prev_created_at) at time zone 'MSK')::date, ' 00:00:00')::timestamp)*3600)
when 
( (to_timestamp(created_at) at time zone 'MSK')::date - (to_timestamp(prev_created_at) at time zone 'MSK')::date) >= 1
then
(created_at - prev_created_at) - (((9*3600)+(30*60))*((to_timestamp(created_at) at time zone 'MSK')::date - (to_timestamp(prev_created_at) at time zone 'MSK')::date))
else (created_at - prev_created_at)
end response_time
from first_messages
where "type" = 'outgoing_chat_message' and prev_type is not null
)
select
m.mop_id manager_id,
m.name_mop manager_name,
r.rop_name responsible_name,
rt.*
from response_times rt
join test.managers m on m.mop_id = rt.created_by
join test.rops r on r.rop_id = m.rop_id::int
where rt.response_time > 0;


