SELECT OEM_GROUP, BRAND, PROPULSION,DATE,PERIOD_GRANULRITY, SUM(VALUE) from EV_VOLUMES_TEST WHERE PERIOD_GRANULARITY='YEAR' GROUP BY  OEM_GROUP, BRAND, PROPULSION,DATE