CREATE TABLE [dbo].[gold_stock_monthly_summary ] (
    [ticker]         VARCHAR (8000) NULL,
    [year]           BIGINT         NULL,
    [month]          BIGINT         NULL,
    [avg_close]      FLOAT (53)     NULL,
    [max_close]      FLOAT (53)     NULL,
    [min_close]      FLOAT (53)     NULL,
    [avg_volume]     FLOAT (53)     NULL,
    [avg_return_pct] FLOAT (53)     NULL,
    [trading_days]   BIGINT         NULL
);


GO