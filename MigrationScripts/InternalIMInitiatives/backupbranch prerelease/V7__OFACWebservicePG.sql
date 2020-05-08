CREATE TABLE iso21.ofac_sys_inpt
(
 i_trns timestamp without time zone NOT NULL,
 i_usr character varying(5) NOT NULL,
 i_cust character varying(4) NOT NULL,
 i_regoff character varying(5) NOT NULL,
 i_cust_shp_to character varying(4) NOT NULL,
 i_regoff_shp_to character varying(5) NOT NULL,
 t_usr_rfrnc character varying(30) NOT NULL DEFAULT ''::character varying,
 c_nm_typ character varying(1) NOT NULL DEFAULT ''::character varying,
 m_ful_nm character varying(100) NOT NULL DEFAULT ''::character varying,
 d_brth character varying(8) NOT NULL DEFAULT ''::character varying,
 t_adr_ln1 character varying(50) NOT NULL DEFAULT ''::character varying,
 m_city character varying(25) NOT NULL DEFAULT ''::character varying,
 c_st character varying(2) NOT NULL DEFAULT ''::character varying,
 c_zip character varying(9) NOT NULL DEFAULT ''::character varying,
 c_cntry character varying(3) NOT NULL DEFAULT ''::character varying,
 n_psprt character varying(9) NOT NULL DEFAULT ''::character varying,
 n_ssn character varying(9) NOT NULL DEFAULT ''::character varying,
 f_mtch character varying(1) NOT NULL DEFAULT ''::character varying,
 f_mtch_cnt character varying(3) NOT NULL DEFAULT ''::character varying,
 i_allclm character varying(11) NOT NULL DEFAULT ''::character varying,
 c_iso_trns character varying(6) NOT NULL DEFAULT ''::character varying,
 score character varying(30) NOT NULL DEFAULT ''::character varying
);
create unique index ofac_inpt_index on iso21.ofac_sys_inpt (i_trns);
create index on iso21.ofac_sys_inpt (i_trns,i_cust,i_regoff);

CREATE TABLE iso21.ofac_sys_prd_lst
(
 c_iso_trns character varying(6) NOT NULL,
 t_iso_trns character varying(50) NOT NULL,
 a_iso_trns_list integer NOT NULL,
 comments character varying(30) NOT NULL DEFAULT ''::character varying
);
create unique index prd_lst_index on iso21.ofac_sys_inpt (c_iso_trns);


