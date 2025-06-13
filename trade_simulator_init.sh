#!/bin/bash

# script to start the trade simulator


python main.py  --data_source "eis_data" \
                --data_name "EIS DATA" \
                --underlying "BANKNIFTY" \
                --start_date_time "2020-01-01 09:16:00" \
                --end_date_time "2020-01-07 15:30:00" \
                --interval_type "minutes" \
                --interval_value 1 \
                --initial_cash 0 \
                --strategy_type "condor" \
                --otm_percentage 5 \
                --trade_interval 5 \
                --hedge_interval 2 \
                --unwind_time 45 \
                --unit_size 100 \
                --is_mkt_maker 1 \
                --expiry_type 'nearest_weekly'
