[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "silver_moving_averages_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "ticker", DestinationColumnName = "ticker"], [SourceColumnName = "year", DestinationColumnName = "year"], [SourceColumnName = "month", DestinationColumnName = "month"], [SourceColumnName = "avg_close", DestinationColumnName = "avg_close"], [SourceColumnName = "max_close", DestinationColumnName = "max_close"], [SourceColumnName = "min_close", DestinationColumnName = "min_close"], [SourceColumnName = "avg_volume", DestinationColumnName = "avg_volume"], [SourceColumnName = "avg_return_pct", DestinationColumnName = "avg_return_pct"], [SourceColumnName = "trading_days", DestinationColumnName = "trading_days"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared silver_moving_averages = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "4db246e1-0a39-4ba9-a437-cf43cb1ec668"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2eaddb7b-d98c-4e57-bcf0-3bec72762ee9"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "silver_moving_averages", ItemKind = "Table"]}[Data],

  // ── Filter to last 365 days (Gold is a rolling summary)
  FilteredRows = Table.SelectRows(#"Navigation 2", each [price_date] >= Date.AddDays(DateTime.Date(DateTime.LocalNow()), -365)),
  // ── Add week and month columns for aggregation
  AddedYear  = Table.AddColumn(FilteredRows, "year",  each Date.Year([price_date]),  Int64.Type),
  AddedMonth = Table.AddColumn(AddedYear,    "month", each Date.Month([price_date]), Int64.Type),

    // ── Group by ticker + month for Gold summary
  Grouped = Table.Group(AddedMonth,
      {"ticker", "year", "month"},
      {
          {"avg_close",      each List.Average([close_price]),    type number},
          {"max_close",      each List.Max([close_price]),        type number},
          {"min_close",      each List.Min([close_price]),        type number},
          {"avg_volume",     each List.Average([vol_ma_7d]),      type number},
          {"avg_return_pct", each List.Average([daily_return_pct]), type number},
          {"trading_days",   each Table.RowCount(_),              Int64.Type}
      }
  ),

    // ── Round numeric columns
  RoundedAvg = Table.TransformColumns(Grouped, {
      {"avg_close",      each Number.Round(_, 4), type number},
      {"avg_return_pct", each Number.Round(_, 4), type number}
  })

in
    RoundedAvg;
shared silver_moving_averages_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false, HierarchicalNavigation = true]),
  Navigation_1 = Pattern{[workspaceId = "."]}[Data],
  Navigation_2 = Navigation_1{[displayName = "wh_gold"]}[Data],
  Navigation_3 = Navigation_2{[Schema = "dbo"]}[Data],
  TableNavigation = Navigation_3{[Name = "gold_stock_monthly_summary "]}?[Data]?
in
  TableNavigation;
