-- через 3 верменные таблицы постепенно получаем итоговый отчет
with all_data as (
	select *
	from shds_dwh_dim_transactions sddt
	left join shds_dwh_dim_cards sddc on sddt.card_num = sddc.card_num
	left join shds_dwh_dim_accounts sdda on sddc.account = sdda.account
	left join shds_dwh_dim_clients sddc2 on sddc2.client_id = sdda.client
	left join shds_dwh_dim_terminals tm on sddt.terminal = tm.terminal_id),
transactionssss AS ( 
	SELECT 
		client_id,
		terminal_city,
		transaction_id,
		CAST(transaction_date AS timestamp) AS transaction_date,
		LEAD(CAST(transaction_date AS timestamp)) OVER (PARTITION BY client_id ORDER BY CAST(transaction_date AS timestamp)) AS next_transaction_date,
		LEAD(terminal_city) OVER (PARTITION BY client_id ORDER BY CAST(transaction_date AS timestamp)) as next_terminal_city
	FROM all_data),
rep_fraud_rd as (
	select 
		*,
		case when passport_num in (select passport from shds_dwh_dim_passport_blacklist where is_active = True) or passport_valid_to < CAST(transaction_date AS date) or passport_valid_to is null
		then 'Заблокированный или просроченный паспорт' else 
		case when valid_to < CAST(transaction_date AS date) then 'Недействующий договор' else
			case when transaction_id in (
			select transaction_id from transactionssss
			WHERE next_terminal_city IS NOT null
			AND terminal_city <> next_terminal_city
			AND next_transaction_date <= transaction_date + INTERVAL '1 hour'
			) then 'Совершение операций в разных городах за короткое время' else 'pusto'
		end
		end 
		end as event_type,
		CONCAT(last_name, ' ', first_name, ' ', patronymic) AS fio
	from all_data)
insert into public.shds_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
	select 
		transaction_date as event_dt,
		passport_num as passport,
		fio,
		phone,
		event_type,
		NOW()::timestamp as report_dt
	from rep_fraud_rd
	where event_type != 'pusto';