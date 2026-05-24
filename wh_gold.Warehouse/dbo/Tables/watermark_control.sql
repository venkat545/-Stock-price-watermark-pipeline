CREATE TABLE [dbo].[watermark_control] (

	[ticker] varchar(20) NOT NULL, 
	[last_load_date] date NOT NULL, 
	[last_load_ts] datetime2(6) NOT NULL, 
	[total_rows] bigint NULL, 
	[status] varchar(20) NULL
);