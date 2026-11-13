CREATE TABLE [dbo].[watermark_control] (
    [ticker]         VARCHAR (20)  NOT NULL,
    [last_load_date] DATE          NOT NULL,
    [last_load_ts]   DATETIME2 (6) NOT NULL,
    [total_rows]     BIGINT        NULL,
    [status]         VARCHAR (20)  NULL
);


GO