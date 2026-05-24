CREATE TABLE [dbo].[gold_stock_monthly_summary ] (

	[ticker] varchar(8000) NULL, 
	[year] bigint NULL, 
	[month] bigint NULL, 
	[avg_close] float NULL, 
	[max_close] float NULL, 
	[min_close] float NULL, 
	[avg_volume] float NULL, 
	[avg_return_pct] float NULL, 
	[trading_days] bigint NULL
);