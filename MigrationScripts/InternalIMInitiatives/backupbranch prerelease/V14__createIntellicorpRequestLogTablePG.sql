CREATE TABLE iso21.intellicorp_rqst_log
(
    i_allclm character varying(11) NOT NULL,
    d_proc timestamp without time zone NOT NULL,
	i_nm_adr character varying(35) NOT NULL ,
    datesearched timestamp without time zone NOT NULL,
    firstname character varying(30) NOT NULL,
	lastname character varying(30) NOT NULL,
	middlename character varying(20) NOT NULL,
    dob date NOT NULL,
    state character varying(2) NOT NULL
);