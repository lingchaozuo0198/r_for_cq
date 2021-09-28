#' @export

print("Hello World")
print("test")


calc_nodal_basis  <- function(value_date,
                              years_back    = 5,
                              tbl_holiday   = c.tbl_holiday,
                              calendar_type = 'us_nerc',
                              db_params     = c.list_db_param_risk)
{
  end_date   <- as.Date(value_date)
  
  start_date <- end_date - lubridate::years(years_back)
  
  asset_crvs <- sql_query_select_star_from_table('vw_cq_gen_asset_unit_spec') %>%
                dplyr::distinct(Subportfolio, PowerPriceName, FuelPriceName, .keep_all=TRUE) %>%
                dplyr::filter(!grepl('rev_put',UnitName))  %>%
                dplyr::pull(PowerPriceName)
  
  hierarchy  <- sql_query_select_star_from_table('vw_price_curve_hierarchy_power') %>%
                dplyr::filter(curve_type == 'node' & curve_name %in% asset_crvs)  %>%
                dplyr::select(curve_name,parent_name)
  
  holidays   <- sql_query_select_star_from_table(tbl_holiday) %>% 
                dplyr::filter(calendar == calendar_type)      %>%
                dplyr::mutate(date = as.Date(date))  
  
  crv_detail <- sql_query_select_star_from_table('vw_price_curve_detail') %>%
                dplyr::filter(commodity == 'power')
  
  pk_prd_hrs <- sql_query_select_star_from_table('vw_cq_peak_period_definition') %>%
                dplyr::select(-PeakPeriodGroup)  %>%
                tidyr::pivot_longer(cols            = dplyr::matches("\\d"),
                                    names_to        = 'hour_ended',
                                    names_transform = list(hour_ended = as.integer),
                                    values_to       = 'in_period')
  
  px_history <- sql_query_select_star_from_table('vw_price_settle_history_hourly_OLD') %>%
                dplyr::mutate(settle_date = as.Date(settle_date)) %>%
                dplyr::filter(settle_date >= start_date & settle_date <= end_date) %>%
                dplyr::select(-description)   %>%
                tidyr::pivot_longer(cols            = dplyr::contains('he_'),
                                    names_to        = 'hour_ended',
                                    names_prefix    = 'he_',
                                    names_transform = list(hour_ended = as.integer),
                                    values_to       = 'price')  %>%
                dplyr::left_join(crv_detail, by = c('curve_name'='curve_display_name')) %>%
                dplyr::select(curve_name,settle_date,hour_ended,price,peak_period)  %>%
                dplyr::mutate(day = ifelse(settle_date %in% holidays$date,
                                           'holiday',
                                           tolower(weekdays(settle_date))))  %>%
                dplyr::left_join(pk_prd_hrs, by = c('hour_ended'='hour_ended', 
                                                    'peak_period'='PeakPeriod',
                                                    'day'='Day'))  %>%
                dplyr::mutate(month  = lubridate::month(settle_date),
                              year   = lubridate::year(settle_date),
                              pk_prd = dplyr::case_when(in_period == 1 ~ 'peak', 
                                                        TRUE ~ 'offpeak'))  
  
  child     <- dplyr::filter(px_history, curve_name %in% hierarchy$curve_name) %>%
               dplyr::left_join(hierarchy,by=c('curve_name'))

  parent    <- dplyr::filter(px_history, curve_name %in% hierarchy$parent_name)  
  
  pairs     <- dplyr::left_join(child,parent,
                                by= c('settle_date','hour_ended','parent_name'='curve_name'),
                                suffix=c('_child','_parent'))
  
  basis     <- dplyr::group_by(pairs,curve_name,month_child,pk_prd_child)  %>%
               dplyr::summarise(nodal_basis = mean(price_child - price_parent,na.rm=TRUE),
                                .groups = 'drop') %>%
               dplyr::ungroup() %>%
               dplyr::rename(month      = month_child,
                             price_band = pk_prd_child) 
               
  return(basis)
}