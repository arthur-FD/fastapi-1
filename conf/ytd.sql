SELECT OEM_GROUP, BRAND, PROPULSION, SUM(VALUE) from EV_VOLUMES_TEST WHERE PERIOD_GRANULARITY='MONTH' and EXTRACT(YEAR FROM DATE)=%(LAST_YEAR_AVAILABLE)s-1 and EXTRACT(MONTH FROM DATE) 
IN (SELECT distinct EXTRACT(MONTH FROM DATE) FROM EV_VOLUMES_TEST where EXTRACT(YEAR FROM DATE)=%(LAST_YEAR_AVAILABLE)s and PERIOD_GRANULARITY='MONTH') GROUP BY OEM_GROUP, BRAND, PROPULSION