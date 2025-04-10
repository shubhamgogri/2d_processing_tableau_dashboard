STAY_DATE_LAG_FEATURES = [
    'pickup_total_revenue', 
    'pickup_adr', 
    'competitor_median_rate',
    'rate',
    'final_day_total_rooms_predicted',
    'final_day_total_revenue_predicted', 'final_recorded_pickup_adr_predictions',         
]

STAY_DATE_LAG_FEATURES_DAYS = [
    1,7,14,21,28
]



COLS = ['stay_date', 'report_date', 'lead_in', 
        'competitor_median_rate', 'competitor_median_rate_last_year',    'competitor_min_rate', 'competitor_max_rate',   
  'rate', 'optimal_rooms', 'pickup_total_rooms_-1', 'pickup_total_revenue_-1',
  'pickup_total_rooms','pickup_total_rooms_last_year', 
    'pickup_adr', 'pickup_adr_-1', 'pickup_adr_last_year',
    'pickup_total_revenue', 
    'pickup_total_revenue_last_year',
    'final_day_total_rooms_predicted', 'final_day_total_revenue_predicted', 'final_recorded_pickup_adr_predictions', 
    'final_day_pickup_adr_predicted',
    'final_recorded_pickup_total_rooms_predictions', 'final_recorded_pickup_total_revenue_predictions',

    'revenue_uplift', 'optimal_price', 
    'final_recorded_pickup_adr_last_year', 'final_recorded_pickup_adr', 
    'final_recorded_pickup_total_rooms_last_year', 'final_recorded_pickup_total_rooms',
    'final_recorded_pickup_total_revenue_last_year', 'final_recorded_pickup_total_revenue',
    'pickup_total_rooms_lag_-7_-7', 
    'pickup_total_rooms_lag_-14_-14',
    'pickup_total_rooms_lag_-21_-21',
    'pickup_total_rooms_lag_-28_-28', 
    
    'competitor_median_rate_lag_-7_-7', 
  'competitor_median_rate_lag_-14_-14', 
  'competitor_median_rate_lag_-21_-21', 
  'competitor_median_rate_lag_-28_-28',

  'rate_lag_-7_-7', 
  'rate_lag_-14_-14', 
  'rate_lag_-21_-21', 
  'rate_lag_-28_-28',

  'pickup_total_revenue_lag_-7_-7', 
  'pickup_total_revenue_lag_-14_-14', 
  'pickup_total_revenue_lag_-21_-21', 
  'pickup_total_revenue_lag_-28_-28', 

  'pickup_adr_lag_-7_-7', 
  'pickup_adr_lag_-14_-14', 
  'pickup_adr_lag_-21_-21', 
  'pickup_adr_lag_-28_-28', 

  'final_day_total_rooms_predicted_lag_-7_-7', 
  'final_day_total_rooms_predicted_lag_-14_-14', 
  'final_day_total_rooms_predicted_lag_-21_-21', 
  'final_day_total_rooms_predicted_lag_-28_-28', 

  'final_day_total_revenue_predicted_lag_-7_-7', 
  'final_day_total_revenue_predicted_lag_-14_-14', 
  'final_day_total_revenue_predicted_lag_-21_-21', 
  'final_day_total_revenue_predicted_lag_-28_-28', 

  'final_recorded_pickup_adr_predictions_lag_-7_-7', 
  'final_recorded_pickup_adr_predictions_lag_-14_-14', 
  'final_recorded_pickup_adr_predictions_lag_-21_-21', 
  'final_recorded_pickup_adr_predictions_lag_-28_-28', 

  'rate_last_year', 
  'final_day_total_rooms_predicted_last_year',
  'final_day_total_revenue_predicted_last_year', 'final_recorded_pickup_adr_predictions_last_year', 
  'optimal_price_last_year',

  'final_day_predicted_total_revenue_VAT_excluded', 'revenue_uplift_VAT_excluded', 'final_day_optimal_total_revenue_VAT_excluded',
  'final_day_predicted_total_revenue_VAT_excluded_last_year', 'revenue_uplift_VAT_excluded_last_year', 'final_day_optimal_total_revenue_VAT_excluded_last_year',
  'predicted_revenue_VAT_excluded', 
  ]

DICT_RENAME = {'pickup_total_rooms_lag_-7_-7': 'Rooms SPIT 1 week ago',
                    'pickup_total_rooms_lag_-14_-14' : 'Rooms SPIT 2 weeks ago',
                    'pickup_total_rooms_lag_-21_-21': 'Rooms SPIT 3 weeks ago',
                    'pickup_total_rooms_lag_-28_-28' : 'Rooms SPIT 4 weeks ago', 
                    
                    'pickup_total_revenue_lag_-7_-7': 'Revenue SPIT 1 week ago',
                    'pickup_total_revenue_lag_-14_-14' : 'Revenue SPIT 2 weeks ago',
                    'pickup_total_revenue_lag_-21_-21': 'Revenue SPIT 3 weeks ago',
                    'pickup_total_revenue_lag_-28_-28' : 'Revenue SPIT 4 weeks ago', 
                    
                    'pickup_adr_lag_-7_-7': 'ADR SPIT 1 week ago',
                    'pickup_adr_lag_-14_-14' : 'ADR SPIT 2 weeks ago',
                    'pickup_adr_lag_-21_-21': 'ADR SPIT 3 weeks ago',
                    'pickup_adr_lag_-28_-28' : 'ADR SPIT 4 weeks ago', 
                
                    'competitor_median_rate_lag_-7_-7': 'Comp rate SPIT 1 week ago', 
                    'competitor_median_rate_lag_-14_-14':'Comp rate SPIT 2 week ago', 
                    'competitor_median_rate_lag_-21_-21':'Comp rate SPIT 3 week ago',  
                    'competitor_median_rate_lag_-28_-28':'Comp rate SPIT 4 week ago', 
                
                    'rate_lag_-7_-7': 'Rate SPIT 1 week ago', 
                    'rate_lag_-14_-14':'Rate SPIT 2 week ago', 
                    'rate_lag_-21_-21':'Rate SPIT 3 week ago',  
                    'rate_lag_-28_-28':'Rate SPIT 4 week ago', 

                    'final_day_total_rooms_predicted_lag_-7_-7': 'Pred Rooms SPIT 1 week ago', 
                    'final_day_total_rooms_predicted_lag_-14_-14':'Pred Rooms SPIT 2 week ago', 
                    'final_day_total_rooms_predicted_lag_-21_-21':'Pred Rooms SPIT 3 week ago',  
                    'final_day_total_rooms_predicted_lag_-28_-28':'Pred Rooms SPIT 4 week ago', 

                    'final_day_total_revenue_predicted_lag_-7_-7': 'Pred Revenue SPIT 1 week ago', 
                    'final_day_total_revenue_predicted_lag_-14_-14':'Pred Revenue SPIT 2 week ago', 
                    'final_day_total_revenue_predicted_lag_-21_-21':'Pred Revenue SPIT 3 week ago',  
                    'final_day_total_revenue_predicted_lag_-28_-28':'Pred Revenue SPIT 4 week ago', 

                    'final_recorded_pickup_adr_predictions_lag_-7_-7': 'Pred ADR SPIT 1 week ago', 
                    'final_recorded_pickup_adr_predictions_lag_-14_-14':'Pred ADR SPIT 2 week ago', 
                    'final_recorded_pickup_adr_predictions_lag_-21_-21':'Pred ADR SPIT 3 week ago',  
                    'final_recorded_pickup_adr_predictions_lag_-28_-28':'Pred ADR SPIT 4 week ago', 

                    }
