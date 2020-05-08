DROP INDEX if exists iso21.prd_lst_index;
create unique index prd_lst_index on iso21.ofac_sys_prd_lst (c_iso_trns);
