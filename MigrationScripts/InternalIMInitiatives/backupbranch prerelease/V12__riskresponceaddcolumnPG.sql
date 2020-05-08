alter table iso21.risk_elect_rpt add column "version" character varying(5) DEFAULT '';

CREATE OR REPLACE VIEW iso21.v_risk_elect_rpt AS
 SELECT risk_elect_rpt.inscomp,
    risk_elect_rpt.regoff,
    risk_elect_rpt.r_response_mq_acctnum,
    risk_elect_rpt.r_indicator,
    risk_elect_rpt.type,
    risk_elect_rpt.t_risk_url,
    risk_elect_rpt.m_dom_url,
    risk_elect_rpt.i_usrid_url,
    risk_elect_rpt.t_pswd_url,
    risk_elect_rpt.c_trns_mode,
    risk_elect_rpt.m_soap_mthd,
    risk_elect_rpt.version
   FROM iso21.risk_elect_rpt; 

ALTER TABLE iso21.v_risk_elect_rpt
    OWNER TO attunitya;