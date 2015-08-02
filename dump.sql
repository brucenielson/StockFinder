BEGIN TRANSACTION;
CREATE TABLE dividend_history (id INTEGER PRIMARY KEY, stockid INTEGER, dividends REAL, date TEXT);
CREATE TABLE recent_stats (id INTEGER PRIMARY KEY, stockid INTEGER);
CREATE TABLE stock (
	id INTEGER NOT NULL, 
	symbol VARCHAR(5) NOT NULL, 
	company_name VARCHAR, 
	industry VARCHAR, 
	sector VARCHAR, 
	created DATETIME, 
	company_start DATETIME, 
	num_full_time_employees INTEGER, 
	company_data_last_updated DATETIME, 
	PRIMARY KEY (id), 
	UNIQUE (symbol)
);
INSERT INTO "stock" VALUES(1,'FOO','Company Name','Weasle Raising','','2015-07-26 14:10:57.467000',NULL,NULL,'2015-07-26 14:10:57.467000');
CREATE TABLE stock_list(id INTEGER PRIMARY KEY, symbol TEXT, company_name TEXT, industry TEXT,sector TEXT, start TEXT, full_time_employees INTEGER, has_dividends INTEGER, last_dividend_date TEXT, last_updated TEXT);
CREATE TABLE url(id INTEGER PRIMARY KEY, stockid INTEGER, url TEXT, description TEXT);
CREATE UNIQUE INDEX symbolx on stock_list(symbol);
CREATE INDEX datex on dividend_history(date);
CREATE INDEX stockx on dividend_history(stockid);
CREATE UNIQUE INDEX stockdatex on dividend_history(stockid, date);
COMMIT;
