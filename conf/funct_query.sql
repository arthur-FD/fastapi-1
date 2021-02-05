SELECT  EV_VOLUMES_TEST.OEM_GROUP, EV_VOLUMES_TEST.BRAND,EV_VOLUMES_TEST.PROPULSION,EV_VOLUMES_TEST.MODEL_ID,VEHICLE_SPEC_TEST.CATHODE, EV_VOLUMES_TEST.PERIOD_GRANULARITY,EV_VOLUMES_TEST.DATE,SUM(EV_VOLUMES_TEST.VALUE)
FROM EV_VOLUMES_TEST
INNER JOIN VEHICLE_SPEC_TEST ON EV_VOLUMES_TEST.MODEL_ID=VEHICLE_SPEC_TEST.MODEL_ID
GROUP BY EV_VOLUMES_TEST.OEM_GROUP, EV_VOLUMES_TEST.BRAND,EV_VOLUMES_TEST.PROPULSION,EV_VOLUMES_TEST.MODEL_ID,VEHICLE_SPEC_TEST.CATHODE,EV_VOLUMES_TEST.PERIOD_GRANULARITY,EV_VOLUMES_TEST.DATE
