SELECT 
date,
country,
soe,
arrivals,
arrivals_male,
arrivals_female,
ingested_at
FROM `myfirstproject-364809.MY_FOREIGN_MONTHLY_ARR.mas_monthly_foriegn_arr`
WHERE date > '2021-12-31'
