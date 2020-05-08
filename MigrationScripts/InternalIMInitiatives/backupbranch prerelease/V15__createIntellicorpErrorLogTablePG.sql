CREATE TABLE iso21.intellicorp_error_log
(
    i_allclm character varying(11) NOT NULL,
    d_proc timestamp without time zone NOT NULL,
    i_nm_adr character varying(35) NOT NULL ,
    datesearched timestamp without time zone NOT NULL,
    error character varying(255) NOT NULL,
	ufmessage character varying(255) 
);