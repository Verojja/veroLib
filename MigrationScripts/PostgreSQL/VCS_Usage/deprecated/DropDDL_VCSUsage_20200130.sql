BEGIN TRANSACTION;

DROP VIEW cs_dw.v_ipuaudit;
DROP VIEW cs_dw.v_officelocationkey;
DROP TABLE cs_dw.ipu_audit;
DROP TABLE cs_dw.ipauditactivitylog;
DROP FUNCTION cs_dw.pkg_hsp_updateipuseraudit;

--ROLLBACK TRANSACTION;
COMMIT TRANSACTION;