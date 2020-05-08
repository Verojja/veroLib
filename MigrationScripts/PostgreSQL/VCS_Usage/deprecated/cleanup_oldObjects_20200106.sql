BEGIN TRANSACTION;

DROP FUNCTION public.pkg_hsp_updateipuseraudit;
DROP VIEW public."V_InsuranceProviderUserAudit";
DROP VIEW public."V_OfficeLocationKey";
DROP TABLE public."IPAuditActivityLog";
DROP TABLE public."InsuranceProviderUserAudit";
	
COMMIT TRANSACTION;