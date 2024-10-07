-- MOYENNE DES TEMPERATURES CHAUFFAGE ET CLIM PAR COMMUNE


-- connexion à prisme pour import meteo
/*
create extension if not exists postgres_fdw;
CREATE SERVER fdw_prisme
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host '172.16.13.168', port '5432', dbname 'prisme', updatable 'false');
create user mapping for user
	server fdw_prisme
	options ( user 'emi', password 'Em1ss!');   -- le mapping est crée */
drop foreign table if exists prj_res_tps_reel.src_meteo_hist;
create foreign table prj_res_tps_reel.src_meteo_hist(
	id_station INT,
	date_tu timestamp,
	temperature FLOAT
	)
server fdw_prisme
options (schema_name 'meteo', table_name 'data_station_meteo');


-- COMMUNES
drop table if exists prj_res_tps_reel.w_bases_dj;
create table prj_res_tps_reel.w_bases_dj as
with chauffage AS (
    SELECT 
        id_station AS id_comm,
        AVG(Temperature) AS tempe_moy_chauff,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Temperature) AS tempe_med_chauff,
        17 - PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Temperature) AS base_DJ17
    FROM 
        prj_res_tps_reel.src_meteo_hist
    WHERE 
        id_station / 1000 IN (4, 5, 6, 13, 83, 84)
        and an between (select max(an)- 3 from total.bilan_comm_v11_diffusion) and (select max(an) from total.bilan_comm_v11_diffusion)  -- 2018-2021 inclus
        AND Temperature < 17 
    GROUP BY 
        id_comm
),
clim AS (
    SELECT 
        id_station AS id_comm,
        AVG(Temperature) AS tempe_moy_clim,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Temperature) AS tempe_med_clim,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Temperature) - 21 AS base_DJ21
    FROM 
        prj_res_tps_reel.src_meteo_hist
    WHERE 
        id_station / 1000 IN (4, 5, 6, 13, 83, 84)
        and an between (select max(an)- 3 from total.bilan_comm_v11_diffusion) and (select max(an) from total.bilan_comm_v11_diffusion)  -- 2018-2021 inclus
        AND Temperature > 21 
    GROUP BY 
        id_comm
)
SELECT 
    chauffage.id_comm, 
    chauffage.tempe_moy_chauff, 
    chauffage.tempe_med_chauff, 
    chauffage.base_DJ17, 
    clim.tempe_moy_clim, 
    clim.tempe_med_clim,
    CASE
        WHEN COALESCE(clim.base_DJ21, 0.3) < 0.3 THEN 0.3
        ELSE COALESCE(clim.base_DJ21, 0.3)
    END AS base_DJ21
FROM 
    total
INNER JOIN    
    chauffage USING (id_comm)
LEFT JOIN 
    clim USING (id_comm)
ORDER BY 
    chauffage.tempe_med_chauff;
comment on table prj_res_tps_reel.w_bases_dj is 'Températures de base (moy, med) de chauffage et clim  pour chaque commune. Utilisées pour corriger les consos selon la température';
select * from prj_res_tps_reel.w_bases_dj;







-- METROPOLES
drop table if exists prj_res_tps_reel.w_bases_dj_metropoles;
create table prj_res_tps_reel.w_bases_dj_metropoles as
select * from
	(select
		CASE 
	    	WHEN id_station/1000 = 6 THEN 6100
	    	WHEN id_station/1000 = 83 THEN 83100
	    	when id_station/1000 = 13 then 13000
			ELSE NULL
	    END AS id_metropole,
	    AVG(Temperature) as tempe_moy_chauff, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Temperature) AS tempe_med_chauff, 17 - PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Temperature) as base_DJ17
	from prj_res_tps_reel.src_meteo_hist
	where id_station in (06006, 06009, 06011, 06013, 06020, 06021, 06025, 06027, 06032, 06033, 06034, 06039, 06042, 06046, 06054, 06055, 06059, 06060, 06064, 06065, 06066, 06072, 06073, 06074, 06075, 06080, 06088, 06102, 06103, 06109, 06110, 06111, 06114, 06117, 06119, 06120, 06121, 06122, 06123, 06126, 06127, 06129, 06144, 06146, 06147, 06149, 06151, 06153, 06156, 06157, 06159,
/* Aix-Marseille */ 13001, 13002, 13003, 13005, 13007, 13008, 13009, 13012, 13013, 13014, 13015, 13016, 13019, 13020, 13119, 13021, 13022, 13023, 13024, 13025, 13026, 13028, 13029, 13118, 13030, 13031, 13032, 13033, 13035, 13037, 13039, 13040, 13041, 13042, 13043, 13044, 13046, 13047, 13048, 13049, 13050, 13051, 13053, 13054, 13055, 13056, 13059, 13060, 13062, 13063, 13069, 13070, 13071, 84089, 13072, 13073, 13074, 13075, 13077, 13078, 13080, 13079, 13081, 13082, 13084, 13085, 13086, 13087, 13088, 13090, 13091, 13092, 13093, 13095, 13098, 13099, 13101, 13102, 83120, 13103, 13104, 13105, 13106, 13107, 13109, 13110, 13111, 13112, 13113, 13114, 13115, 13117,
/* Toulon metropole */ 83034, 83047, 83062, 83069, 83090, 83098, 83103, 83126, 83129, 83137, 83144, 83153) 
		and EXTRACT(year from date_tu) = (SELECT DISTINCT EXTRACT(year FROM CURRENT_DATE) - 1)
		and temperature < 17 
	group by id_metropole) as chauff
natural join
	(select
		CASE 
	    	WHEN id_station/1000 = 6 THEN 6100
	    	WHEN id_station/1000 = 83 THEN 83100
	    	when id_station/1000 = 13 then 13000
			ELSE NULL
	    END AS id_metropole,
	    AVG(Temperature) as tempe_moy_clim, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Temperature) AS tempe_med_clim, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Temperature) - 21 as base_DJ21
	from prj_res_tps_reel.src_meteo_hist
	where id_station in (06006, 06009, 06011, 06013, 06020, 06021, 06025, 06027, 06032, 06033, 06034, 06039, 06042, 06046, 06054, 06055, 06059, 06060, 06064, 06065, 06066, 06072, 06073, 06074, 06075, 06080, 06088, 06102, 06103, 06109, 06110, 06111, 06114, 06117, 06119, 06120, 06121, 06122, 06123, 06126, 06127, 06129, 06144, 06146, 06147, 06149, 06151, 06153, 06156, 06157, 06159,
/* Aix-Marseille */ 13001, 13002, 13003, 13005, 13007, 13008, 13009, 13012, 13013, 13014, 13015, 13016, 13019, 13020, 13119, 13021, 13022, 13023, 13024, 13025, 13026, 13028, 13029, 13118, 13030, 13031, 13032, 13033, 13035, 13037, 13039, 13040, 13041, 13042, 13043, 13044, 13046, 13047, 13048, 13049, 13050, 13051, 13053, 13054, 13055, 13056, 13059, 13060, 13062, 13063, 13069, 13070, 13071, 84089, 13072, 13073, 13074, 13075, 13077, 13078, 13080, 13079, 13081, 13082, 13084, 13085, 13086, 13087, 13088, 13090, 13091, 13092, 13093, 13095, 13098, 13099, 13101, 13102, 83120, 13103, 13104, 13105, 13106, 13107, 13109, 13110, 13111, 13112, 13113, 13114, 13115, 13117,
/* Toulon metropole */ 83034, 83047, 83062, 83069, 83090, 83098, 83103, 83126, 83129, 83137, 83144, 83153) 
		and EXTRACT(year from date_tu) = (SELECT DISTINCT EXTRACT(year FROM CURRENT_DATE) - 1)
		and temperature > 21
	group by id_metropole) as clim;
	select * from prj_res_tps_reel.w_bases_dj_metropoles;