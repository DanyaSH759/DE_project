INSERT INTO public.shds_stg_accounts
	SELECT *
	from info.accounts;
INSERT INTO public.shds_stg_cards
	SELECT *
	from info.cards;
INSERT INTO public.shds_stg_clients 
	SELECT *
	from info.clients;
INSERT INTO public.shds_dwh_dim_accounts
	SELECT *
	from info.accounts;
INSERT INTO public.shds_dwh_dim_cards
	SELECT *
	from info.cards;
INSERT INTO public.shds_dwh_dim_clients 
	SELECT *
	from info.clients;