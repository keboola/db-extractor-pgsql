CREATE USER readonly_user WITH PASSWORD 'password' NOSUPERUSER NOINHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
ALTER USER readonly_user set default_transaction_read_only = on;