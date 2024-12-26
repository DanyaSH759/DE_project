--Скрипт для создания всех необходимых таблиц

CREATE table IF NOT EXISTS public.shds_stg_accounts (
    account bpchar(20) PRIMARY KEY,
    valid_to date,
    client varchar(10),
    create_dt timestamp(0),
    update_dt timestamp(0)
);

CREATE TABLE IF NOT exists public.shds_stg_cards (
    card_num bpchar(20) PRIMARY KEY,
    account bpchar(20),
    create_dt timestamp(0),
    update_dt timestamp(0)
);
 CREATE TABLE IF NOT exists public.shds_stg_clients (
client_id varchar(10) PRIMARY KEY,
last_name varchar(20),
first_name varchar(20),
patronymic varchar(20),
date_of_birth date,
passport_num varchar(15),
passport_valid_to date,
phone bpchar(16),
create_dt timestamp(0),
update_dt timestamp(0)
);

CREATE TABLE IF NOT exists public.shds_stg_passport_blacklist (
date date,
passport varchar(15) PRIMARY KEY
);

CREATE TABLE IF NOT exists public.shds_stg_terminals (
terminal_id varchar(10) PRIMARY KEY,
terminal_type varchar(10),
terminal_city varchar(30),
terminal_addres varchar
);

CREATE TABLE IF NOT exists public.shds_stg_transactions (
transaction_id varchar(15),
transaction_date varchar(20), 
amount decimal,
card_num bpchar(20),
oper_type varchar,
oper_result varchar,
terminal varchar(10)
);

CREATE table IF NOT EXISTS public.shds_dwh_dim_accounts (
    account bpchar(20) PRIMARY KEY,
    valid_to date,
    client varchar(10),
    create_dt timestamp(0),
    update_dt timestamp(0)
);

CREATE TABLE IF NOT exists public.shds_dwh_dim_cards (
    card_num bpchar(20) PRIMARY KEY,
    account bpchar(20),
    create_dt timestamp(0),
    update_dt timestamp(0)
);

CREATE TABLE IF NOT exists public.shds_dwh_dim_clients (
client_id varchar(10) PRIMARY KEY,
last_name varchar(20),
first_name varchar(20),
patronymic varchar(20),
date_of_birth date,
passport_num varchar(15),
passport_valid_to date,
phone bpchar(16),
create_dt timestamp(0),
update_dt timestamp(0)
);

CREATE TABLE IF NOT exists public.shds_dwh_dim_passport_blacklist (
date date,
passport varchar(15),
is_active bool,
load_date date
);

CREATE TABLE IF NOT exists public.shds_dwh_dim_terminals (
terminal_id varchar(10),
terminal_type varchar(10),
terminal_city varchar(30),
terminal_addres varchar,
is_active bool,
load_date date
);

CREATE TABLE IF NOT exists public.shds_dwh_dim_transactions (
transaction_id varchar(15),
transaction_date varchar(20), 
amount decimal,
card_num bpchar(20),
oper_type varchar,
oper_result varchar,
terminal varchar(10),
load_date date,
id SERIAL PRIMARY KEY
);

CREATE table IF NOT EXISTS public.shds_rep_fraud (
    id SERIAL PRIMARY KEY,
    event_dt varchar(20),
    passport varchar(20),
    phone varchar,
    fio varchar,
    event_type varchar,
    report_dt date
);

--после сооздания таблиц сразу их очистим кроме shds_rep_fraud!!!
truncate public.shds_stg_accounts, public.shds_stg_cards, public.shds_stg_clients, public.shds_stg_transactions, public.shds_stg_terminals, public.shds_stg_passport_blacklist;
truncate public.shds_dwh_dim_accounts, public.shds_dwh_dim_cards, public.shds_dwh_dim_clients, public.shds_dwh_dim_transactions, public.shds_dwh_dim_terminals, public.shds_dwh_dim_passport_blacklist;
