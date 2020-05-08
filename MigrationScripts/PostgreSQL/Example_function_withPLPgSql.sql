--Function with input parameters
create or replace function pkg_audittrial_checkvalue(
	userid character varying,
	tablename character varying, pkv character varying,
       	columnname character varying,
       	newvalue character varying, oldvalue character varying
)
RETURNS void
LANGUAGE plpgsql

AS $BODY$
BEGIN

	-- TODO make default on the sequence
	-- TODO calculate current user here, not from argument
	-- TODO don't uppercase tablename and columnname

	if newvalue is distinct from oldvalue then

		INSERT INTO hciuser.audit_trail
		(audit_trail_no, "timestamp", modifying_user,
			table_name, pk_value, column_name, old_value,
			new_value)
		VALUES (NEXTVAL('hciuser.audit_trail_seq'), current_timestamp,
			userid,
			UPPER(tablename), pkv, UPPER(columnname), oldvalue,
			newvalue);
	END IF;
END;
$BODY$;

--Function with return type and input parameters
CREATE OR REPLACE FUNCTION public.count_rows(
	schema text,
	tablename text)
    RETURNS integer
    LANGUAGE 'plpgsql'

  
AS $BODY$

declare
  result integer;
  query varchar;
begin
   query := 'SELECT count(1) FROM ' || schema || '.' || tablename;
  query := 'SELECT count(1), '|| current_timestamp ||' FROM '|| schema || '.' || tablename;
  execute query into result;
  return result;
end;

$BODY$;

/*
BEGIN TRANSACTION;
--DROP function public.hsp_test_languagePostgreSQL
create or replace function public.hsp_test_languagePostgreSQL (
    columnname character varying,
	newvalue character varying,
	oldvalue character varying
)
RETURNS void
LANGUAGE plpgsql

AS $BODY$
BEGIN
	declare
	  result integer;
	  query varchar;
END;
$BODY$;

ROLLBACK TRANSACTION;

*/