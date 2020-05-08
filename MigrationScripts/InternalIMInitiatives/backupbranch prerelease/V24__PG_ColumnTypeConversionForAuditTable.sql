DROP VIEW iso21.v_claim_alert_req;
ALTER TABLE iso21.clt000m0 ALTER COLUMN t_rqst_cntt TYPE text;
ALTER TABLE iso21.clt000m0 ALTER COLUMN t_rspns_cntt TYPE text;


CREATE OR REPLACE VIEW iso21.v_claim_alert_req AS
 SELECT clt000m0.i_clm_alrt_rqst,
    clt000m0.c_rqst_stus,
    clt000m0.i_rqstr,
    clt000m0.h_rqst,
    clt000m0.h_rspns,
    clt000m0.t_rqst_cntt,
    clt000m0.t_rspns_cntt
   FROM iso21.clt000m0;

 

ALTER TABLE iso21.v_claim_alert_req OWNER TO attunitya;
GRANT ALL ON TABLE iso21.v_claim_alert_req TO attunitya;
GRANT SELECT ON TABLE iso21.v_claim_alert_req TO claims_dev_readonly;
GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE iso21.v_claim_alert_req TO pidpgvcsbcknda;
GRANT SELECT ON TABLE iso21.v_claim_alert_req TO pidqa;