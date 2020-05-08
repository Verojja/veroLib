CREATE TABLE flywaydb.SprintDemo
(
    i_frwrn_id integer,
    n_prof_med_lic character varying(20) COLLATE pg_catalog."default" NOT NULL DEFAULT ''::character varying
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;