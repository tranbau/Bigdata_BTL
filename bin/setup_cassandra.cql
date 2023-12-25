CREATE KEYSPACE IF NOT EXISTS stock_data
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE stock_data;

CREATE TABLE IF NOT EXISTS stock_prices (
    date DATE PRIMARY KEY,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    adj_close DOUBLE,
    volume BIGINT
);
