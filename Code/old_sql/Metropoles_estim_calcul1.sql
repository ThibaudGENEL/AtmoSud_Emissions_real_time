--inv


-- DONNEES CONSO A ESTIMER SELON °C


/*
create extension if not exists postgres_fdw;
CREATE SERVER fdw_cadastre
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host '172.16.13.249', port '5432', dbname 'cadastre', updatable 'false');
create user mapping for user
	server fdw_cadastre
	options ( user 'postgres', password 'postgres');   -- le mapping est crée */

-- import table meteo
drop foreign table if exists prj_res_tps_reel.src_meteo_tps_reel;
create foreign table prj_res_tps_reel.src_meteo_tps_reel(
	id_station_meteo INT,
	date_jour timestamp,
	temperature FLOAT,
	echeance INT
	)
server fdw_cadastre
options (schema_name 'airesv5', table_name 'meteo_routier_tps_reel');
-- group by JOUR et Calcul DJ17, DJ21. 									LONG (+1min)
drop table if exists prj_res_tps_reel.w_meteo_tps_reel;
create table prj_res_tps_reel.w_meteo_tps_reel as
WITH MinEcheance AS (
    SELECT
        id_station_meteo,
        CAST(date_jour AS DATE) AS date,
        MIN(echeance) AS min_echeance
    from prj_res_tps_reel.src_meteo_tps_reel
    GROUP by id_station_meteo, CAST(date_jour AS DATE)
)
select t.id_station_meteo, CAST(t.date_jour AS DATE) AS date, 
	 CASE
	    WHEN EXTRACT(DOW FROM date) = 0 THEN 'Dimanche'
	    WHEN EXTRACT(DOW FROM date) = 1 THEN 'Lundi'
	    WHEN EXTRACT(DOW FROM date) = 2 THEN 'Mardi'
	    WHEN EXTRACT(DOW FROM date) = 3 THEN 'Mercredi'
	    WHEN EXTRACT(DOW FROM date) = 4 THEN 'Jeudi'
	    WHEN EXTRACT(DOW FROM date) = 5 THEN 'Vendredi'
	    WHEN EXTRACT(DOW FROM date) = 6 THEN 'Samedi'
	end as Weekday,
    AVG(t.temperature) AS temperature,
    case
		when AVG(t.temperature) < 17 then 17 - AVG(t.temperature)
		else 0
	end as DJ17,
	case 
		when AVG(t.temperature) > 21 then AVG(t.temperature) - 21
		else 0
	end as DJ21
from prj_res_tps_reel.src_meteo_tps_reel t INNER join MinEcheance me
ON
    t.id_station_meteo = me.id_station_meteo
    AND CAST(t.date_jour AS DATE) = me.date
    AND t.echeance = me.min_echeance
GROUP by t.id_station_meteo, CAST(t.date_jour AS DATE), EXTRACT(DOW FROM date)
ORDER by date desc, t.id_station_meteo;
;	
comment on table prj_res_tps_reel.w_meteo_tps_reel is 'Température, DJ17 et DJ21 quotidiens pour chaque commune en Région Sud jusqu à aujourd hui';
select * from prj_res_tps_reel.w_meteo_tps_reel;
-- group by metropole
drop table if exists meteo_metropole;
create temp table meteo_metropole as
select Date, Weekday,
	CASE 
    	WHEN id_station_meteo/1000 = 6 THEN 6100
    	WHEN id_station_meteo/1000 = 83 THEN 83100
    	when id_station_meteo/1000 = 13 then 13000
		ELSE NULL
    END AS id_metropole, 
    SUM(temperature * psdc) / SUM(psdc) as Temperature,  -- Tempé Moyenne pondérée par la pop de la commune
    case
		when SUM(temperature * psdc) / SUM(psdc) < 17 then 17 - SUM(temperature * psdc) / SUM(psdc)
		else 0
	end as DJ17,
	case
		when SUM(temperature * psdc) / SUM(psdc) > 21 then SUM(temperature * psdc) / SUM(psdc) - 21
		else 0
	end as DJ21
from prj_res_tps_reel.w_meteo_tps_reel join commun.src_pop_comm pop on id_station_meteo = pop.id_comm
where id_station_meteo in (06006, 06009, 06011, 06013, 06020, 06021, 06025, 06027, 06032, 06033, 06034, 06039, 06042, 06046, 06054, 06055, 06059, 06060, 06064, 06065, 06066, 06072, 06073, 06074, 06075, 06080, 06088, 06102, 06103, 06109, 06110, 06111, 06114, 06117, 06119, 06120, 06121, 06122, 06123, 06126, 06127, 06129, 06144, 06146, 06147, 06149, 06151, 06153, 06156, 06157, 06159,
/* Aix-Marseille */ 13001, 13002, 13003, 13005, 13007, 13008, 13009, 13012, 13013, 13014, 13015, 13016, 13019, 13020, 13119, 13021, 13022, 13023, 13024, 13025, 13026, 13028, 13029, 13118, 13030, 13031, 13032, 13033, 13035, 13037, 13039, 13040, 13041, 13042, 13043, 13044, 13046, 13047, 13048, 13049, 13050, 13051, 13053, 13054, 13055, 13056, 13059, 13060, 13062, 13063, 13069, 13070, 13071, 84089, 13072, 13073, 13074, 13075, 13077, 13078, 13080, 13079, 13081, 13082, 13084, 13085, 13086, 13087, 13088, 13090, 13091, 13092, 13093, 13095, 13098, 13099, 13101, 13102, 83120, 13103, 13104, 13105, 13106, 13107, 13109, 13110, 13111, 13112, 13113, 13114, 13115, 13117,
/* Toulon metropole */ 83034, 83047, 83062, 83069, 83090, 83098, 83103, 83126, 83129, 83137, 83144, 83153) 
	and pop.id_version_pop = 16 and pop.an = (select max(an) from commun.src_pop_comm)   --2023
group by id_metropole, date, Weekday
order by date desc, id_metropole
;
select * from meteo_metropole;



-- Part de conso en PACA    TOUS SECTEURS
drop table if exists parts_conso_comm;
create temp table parts_conso_comm as
select *, Conso_annee / Conso_totale_annee as Part_Conso_comm from
(select id_comm, sum(val) as Conso_Annee
from total.bilan_comm_v11_diffusion bcvd natural join commun.tpk_polluants tp natural join total.tpk_cat_energie_color tcec 
where tp.nom_court_polluant = 'conso'
	and tcec.nom_court_cat_energie = 'Electricité'
	--and id_secteur_detail in (3, 4)
	and an = (select max(an) from total.bilan_comm_v11_diffusion bcvd2)
	and id_comm/1000 in (4, 5, 6, 13, 83, 84)
	and format_detaille_scope_2=1
group by id_comm) as consos
cross join (
select sum(val) as Conso_Totale_Annee
from total.bilan_comm_v11_diffusion bcvd natural join commun.tpk_polluants tp natural join total.tpk_cat_energie_color tcec 
where tp.nom_court_polluant = 'conso'
	and tcec.nom_court_cat_energie = 'Electricité'
	--and id_secteur_detail in (3, 4)
	and an = (select max(an) from total.bilan_comm_v11_diffusion bcvd2)
	and id_comm/1000 in (4, 5, 6, 13, 83, 84)) as conso
	and format_detaille_scope_2=1
;
drop table if exists parts_conso_metr;



-- conso
drop table if exists conso_comm_est;
create temp table conso_comm_est as
select date, weekday, id_comm, temperature, part_conso_comm,
	CASE
        WHEN WeekDay = 'Lundi' THEN 
		     CASE WHEN Temperature < 1 THEN (171016.46319417175 + (-4549.346755578752 * Temperature)) * part_conso_comm
		         WHEN Temperature BETWEEN 1 and 29 THEN (162833.69 + (-10.1829097165942 * POW(Temperature, 1))+ (-620.8913209596785 * POW(Temperature, 2))+ (15.173836774209013 * POW(Temperature, 3))+ (0.8361893500078418 * POW(Temperature, 4))+ (-0.023835039496863976 * POW(Temperature, 5))) * part_conso_comm 
		         ELSE (-6984.68089434749 + (4428.874027658534 * Temperature)) * part_conso_comm 
		     END
		WHEN WeekDay = 'Mardi' THEN 
		     CASE WHEN Temperature < 1 THEN (173429.3974370337 + (-4700.922648849064 * Temperature)) * part_conso_comm
		         WHEN Temperature BETWEEN 1 and 29 THEN (173528.40 + (-3207.1133922879685 * POW(Temperature, 1))+ (-941.5478321839163 * POW(Temperature, 2))+ (234.57825473993933 * POW(Temperature, 3))+ (-29.284900669928557 * POW(Temperature, 4))+ (1.8172842602197978 * POW(Temperature, 5))+ (-0.052995753141384855 * POW(Temperature, 6))+ (0.0005843030485579255 * POW(Temperature, 7))) * part_conso_comm 
		         ELSE (21872.717087945217 + (3348.206599453792 * Temperature)) * part_conso_comm 
		     END
		WHEN WeekDay = 'Mercredi' THEN 
		     CASE WHEN Temperature < 1 THEN (174519.3619348151 + (-5272.1089706884595 * Temperature)) * part_conso_comm
		         WHEN Temperature BETWEEN 1 and 29 THEN (190385.94 + (-20442.27801345836 * POW(Temperature, 1))+ (4484.063321619279 * POW(Temperature, 2))+ (-562.5690549541175 * POW(Temperature, 3))+ (33.565668740164334 * POW(Temperature, 4))+ (-0.9282875552265284 * POW(Temperature, 5))+ (0.00970246435555825 * POW(Temperature, 6))) * part_conso_comm 
		         ELSE (13222.77552227497 + (3691.3608935975444 * Temperature)) * part_conso_comm 
		     END
		WHEN WeekDay = 'Jeudi' THEN 
		     CASE WHEN Temperature < 1 THEN (170024.04608904762 + (-3976.251964331911 * Temperature)) * part_conso_comm
		         WHEN Temperature BETWEEN 1 and 29 THEN (176290.82 + (-10514.392911209909 * POW(Temperature, 1))+ (2150.0275130328046 * POW(Temperature, 2))+ (-309.52847609598325 * POW(Temperature, 3))+ (19.63143483948526 * POW(Temperature, 4))+ (-0.5496060531316305 * POW(Temperature, 5))+ (0.005672948625714985 * POW(Temperature, 6))) * part_conso_comm 
		         ELSE (44686.46006786286 + (2516.0520246227334 * Temperature)) * part_conso_comm 
		     END
		WHEN WeekDay = 'Vendredi' THEN 
		     CASE WHEN Temperature < 1 THEN (165766.95430801786 + (-2742.4274541469904 * Temperature)) * part_conso_comm
		         WHEN Temperature BETWEEN 1 and 29 THEN (174459.22 + (-10230.327292294094 * POW(Temperature, 1))+ (2280.0748352652013 * POW(Temperature, 2))+ (-340.2756419046232 * POW(Temperature, 3))+ (22.096694875348604 * POW(Temperature, 4))+ (-0.6346535536985819 * POW(Temperature, 5))+ (0.006741890733719544 * POW(Temperature, 6))) * part_conso_comm 
		         ELSE (51247.752265941606 + (2234.2968408029164 * Temperature)) * part_conso_comm 
		     END
		WHEN WeekDay = 'Samedi' THEN 
		     CASE WHEN Temperature < 1 THEN (159112.97025609785 + (-2585.738084304401 * Temperature)) * part_conso_comm
		         WHEN Temperature BETWEEN 1 and 29 THEN (150173.88 + (3967.629278444392 * POW(Temperature, 1))+ (-1286.6949439535015 * POW(Temperature, 2))+ (66.83366420315043 * POW(Temperature, 3))+ (-0.9931505483065018 * POW(Temperature, 4))) * part_conso_comm 
		         ELSE (42250.13522684381 + (2390.7431625204385 * Temperature)) * part_conso_comm 
		     END
		WHEN WeekDay = 'Dimanche' THEN 
		     CASE WHEN Temperature < 1 THEN (157312.30100110377 + (-3511.4468940078946 * Temperature)) * part_conso_comm
		         WHEN Temperature BETWEEN 1 and 29 THEN (141137.86 + (5665.319186016214 * POW(Temperature, 1))+ (-1466.4511788811192 * POW(Temperature, 2))+ (74.57353437778615 * POW(Temperature, 3))+ (-1.1116062932980189 * POW(Temperature, 4))) * part_conso_comm 
		         ELSE (24862.227730554136 + (2879.742715021911 * Temperature)) * part_conso_comm 
		     END
    END AS Consommation_est_mwh
from prj_res_tps_reel.w_meteo_tps_reel meteo inner join parts_conso_comm parts on meteo.id_station_meteo = parts.id_comm
order by date desc, id_comm;
select * from conso_comm_est;
-- conso metropoles
drop table if exists conso_est;
create temp table conso_est as
select date, weekday, 
	case 
		when id_comm/1000 = 6 then 6100
		when id_comm/1000 = 83 then 83100
		when id_comm/1000 = 13 then 13000
		else null
	end as id_metropole,
	SUM(temperature * psdc) / SUM(psdc) as Temperature, SUM(part_conso_comm) as part_conso_metropole,
	SUM(consommation_est_mwh) as consommation_est_mwh
from conso_comm_est  inner join commun.src_pop_comm pop using(id_comm)
where id_comm in (06006, 06009, 06011, 06013, 06020, 06021, 06025, 06027, 06032, 06033, 06034, 06039, 06042, 06046, 06054, 06055, 06059, 06060, 06064, 06065, 06066, 06072, 06073, 06074, 06075, 06080, 06088, 06102, 06103, 06109, 06110, 06111, 06114, 06117, 06119, 06120, 06121, 06122, 06123, 06126, 06127, 06129, 06144, 06146, 06147, 06149, 06151, 06153, 06156, 06157, 06159,
/* Aix-Marseille */ 13001, 13002, 13003, 13005, 13007, 13008, 13009, 13012, 13013, 13014, 13015, 13016, 13019, 13020, 13119, 13021, 13022, 13023, 13024, 13025, 13026, 13028, 13029, 13118, 13030, 13031, 13032, 13033, 13035, 13037, 13039, 13040, 13041, 13042, 13043, 13044, 13046, 13047, 13048, 13049, 13050, 13051, 13053, 13054, 13055, 13056, 13059, 13060, 13062, 13063, 13069, 13070, 13071, 84089, 13072, 13073, 13074, 13075, 13077, 13078, 13080, 13079, 13081, 13082, 13084, 13085, 13086, 13087, 13088, 13090, 13091, 13092, 13093, 13095, 13098, 13099, 13101, 13102, 83120, 13103, 13104, 13105, 13106, 13107, 13109, 13110, 13111, 13112, 13113, 13114, 13115, 13117,
/* Toulon metropole */ 83034, 83047, 83062, 83069, 83090, 83098, 83103, 83126, 83129, 83137, 83144, 83153) 
	and pop.id_version_pop = 16 
	and pop.an = (select MAX(an) from commun.src_pop_comm)   --2023
group by date, weekday, id_metropole
order by date desc;
select * from conso_est where id_metropole is not null;




-- CALCUL RATIO DE CONSO  : Conso_usage_energie / conso elec. Par commune
-- Voir Calcul_ratio_conso.sql pour refaire le calcul.
select * from prj_res_tps_reel.w_ratios_conso_metropoles;







-- correction, selon la meteo

-- Etape 1 : Energie = Electricité 
-- Données joined ratios_conso et météo
drop table if exists InitialData;
CREATE TEMPORARY TABLE InitialData AS
SELECT id_metropole, id_usage, code_cat_energie, consommation, consommation_elec, ratio_conso, date, meteo.temperature, dj17, base_dj17, dj21, base_dj21
FROM 
    prj_res_tps_reel.w_ratios_conso_metropoles ratios 
    NATURAL JOIN meteo_metropole meteo
    natural join prj_res_tps_reel.w_bases_dj_metropoles djbases
ORDER BY 
    meteo.Date DESC, id_metropole, id_usage, code_cat_energie;
--select * from InitialData;
-- Conso[chauffage] = Conso * (DJ17 / valeur_dj17_de_base)   exemple: 4. -> La conso pour chauffage ne change pas si DJ17 =  4 [13°C)]. Diminue si DJ17 < 4, augmente si > 4
drop table if exists AdjustedRatio;
CREATE TEMPORARY TABLE AdjustedRatio AS
SELECT 
    id_metropole, id_usage, code_cat_energie, consommation, consommation_elec, ratio_conso,
    date, temperature, DJ17, DJ21, base_dj17, base_dj21,
    CASE 
        WHEN id_usage = 0 and code_cat_energie = 8 THEN ratio_conso * LEAST((DJ17 / base_dj17), (sum1_barre / sum1) + 1 - 0.01)  -- SI CHAUFFAGE
        when id_usage = 21 and code_cat_energie = 8 then ratio_conso * LEAST((DJ21 / base_dj21), (sum1_barre / sum1) + 1 - 0.01)  -- SI CLIM
        ELSE ratio_conso 
    END AS adjusted_ratio
FROM 
    (select *, 
    	(SELECT SUM(ratio_conso) FROM InitialData AS sub WHERE sub.id_usage in (0, 21) and sub.code_cat_energie = 8 AND sub.date = InitialData.date AND sub.id_metropole = InitialData.id_metropole) AS sum1,
    	(SELECT SUM(ratio_conso) FROM InitialData AS sub WHERE sub.id_usage not in (0, 21) and sub.code_cat_energie = 8 AND sub.date = InitialData.date AND sub.id_metropole = InitialData.id_metropole) AS sum1_barre
    from InitialData) as InitialData2;
--select * from AdjustedRatio;
-- Diff_chauffage avant/après
DROP TABLE IF EXISTS HeatingDiff;
CREATE TEMPORARY TABLE HeatingDiff AS
SELECT 
    Date,
    id_metropole,
    SUM(CASE WHEN id_usage = 0 THEN adjusted_ratio - ratio_conso ELSE 0 END) AS diff_chauffage,
    SUM(CASE WHEN id_usage = 21 THEN adjusted_ratio - ratio_conso ELSE 0 END) AS diff_clim,
    SUM(CASE WHEN id_usage <> 0 and id_usage <> 21 and code_cat_energie = 8 THEN ratio_conso ELSE 0 END) AS sum2
FROM 
    AdjustedRatio
GROUP BY 
    Date, id_metropole;
--select * from heatingdiff;
DROP TABLE IF EXISTS correctionElec;
CREATE temp table correctionElec AS
SELECT 
    a.Date, 
    a.id_metropole, 
    a.id_usage, 
    a.code_cat_energie, 
    a.ratio_conso,
    a.temperature,
    a.DJ17, a.DJ21,
    CASE 
        WHEN a.id_usage = 0 and code_cat_energie = 8 THEN a.adjusted_ratio  -- chauffage deja corrigé
        when a.id_usage = 21 and code_cat_energie = 8 then a.adjusted_ratio  -- clim aussi
        when id_usage not in (0, 21) and code_cat_energie = 8 then 
        	a.ratio_conso - (a.ratio_conso / h.sum2) * (h.diff_chauffage + h.diff_clim)    -- correction hors-chauffage, pour garder la même conso totale
        else a.ratio_conso
    END AS ratio_corrigelec
FROM 
    AdjustedRatio a
    JOIN HeatingDiff h ON a.Date = h.Date AND a.id_metropole = h.id_metropole
order by Date DESC, id_metropole, id_usage, code_cat_energie ;
select * from correctionElec;
-- verif même conso totale pour 1 jour 1 ville
/*
select date, id_metropole, sum(ratio_conso) as Avant, sum(ratio_corrigelec) as Apres
from correctionElec
group by date, id_metropole
order by date desc, id_metropole; 
select date, id_metropole, sum(ratio_corrigelec) 
from correctionElec
where code_cat_energie = 8 
group by date, id_metropole*/


-- ETAPE 2 : Autres énergies
-- Données joined ratios_conso et météo
drop table if exists InitialData;
CREATE TEMPORARY TABLE InitialData AS
SELECT id_metropole, id_usage, code_cat_energie, ratio_conso, ratio_corrigelec, date, temperature, dj17, base_dj17, dj21, base_dj21
FROM 
    correctionElec
    natural join prj_res_tps_reel.w_bases_dj_metropoles djbases
ORDER BY 
    Date DESC, id_metropole, id_usage, code_cat_energie;
select * from InitialData;
-- Conso[chauffage] = Conso * (DJ17 / valeur_dj17_de_base)   exemple: 4. -> La conso pour chauffage ne change pas si DJ17 =  4 [13°C)]. Diminue si DJ17 < 4, augmente si > 4
drop table if exists AdjustedRatio;
CREATE TEMPORARY TABLE AdjustedRatio AS
SELECT 
    id_metropole, id_usage, code_cat_energie, ratio_conso, ratio_corrigelec, date, temperature, base_dj17, base_dj21,
    CASE 
        WHEN id_usage = 0 and code_cat_energie <> 8 THEN ratio_conso * LEAST((DJ17 / base_dj17), (sum1_barre / sum1) + 1 - 0.1)  -- SI CHAUFFAGE
        when id_usage = 21 and code_cat_energie <> 8 then ratio_conso * LEAST((DJ21 / base_dj21), (sum1_barre / sum1) + 1 - 0.1)  -- SI CLIM
        ELSE ratio_corrigelec 
    END AS adjusted_ratio
FROM 
    (select *, 
    	(SELECT SUM(ratio_conso) FROM InitialData AS sub WHERE sub.id_usage in (0, 21) and sub.code_cat_energie <> 8 AND sub.date = InitialData.date AND sub.id_metropole = InitialData.id_metropole) AS sum1,
    	(SELECT SUM(ratio_conso) FROM InitialData AS sub WHERE sub.id_usage not in (0, 21) and sub.code_cat_energie <> 8 AND sub.date = InitialData.date AND sub.id_metropole = InitialData.id_metropole) AS sum1_barre
    from InitialData) as InitialData2;
--select * from AdjustedRatio;
-- Diff_chauffage avant/après
DROP TABLE IF EXISTS HeatingDiff;
CREATE TEMPORARY TABLE HeatingDiff AS
SELECT 
    Date,
    id_metropole,
    SUM(CASE WHEN id_usage = 0 THEN adjusted_ratio - ratio_corrigelec ELSE 0 END) AS diff_chauffage,
    SUM(CASE WHEN id_usage = 21 THEN adjusted_ratio - ratio_corrigelec ELSE 0 END) AS diff_clim,
    SUM(CASE WHEN id_usage <> 0 and id_usage <> 21 and code_cat_energie <> 8 THEN ratio_corrigelec ELSE 0 END) AS sum2
FROM 
    AdjustedRatio
GROUP BY 
    Date, id_metropole;
--select * from heatingdiff;
DROP TABLE IF EXISTS prj_res_tps_reel.w_ratios_usage_energie_metropoles_corriges;
CREATE TABLE prj_res_tps_reel.w_ratios_usage_energie_metropoles_corriges AS
SELECT 
    a.Date, 
    a.id_metropole, 
    a.id_usage, 
    a.code_cat_energie, 
    a.ratio_conso,
    a.temperature,
    CASE 
        WHEN a.id_usage = 0 and code_cat_energie <> 8 THEN a.adjusted_ratio  -- chauffage deja corrigé
        when a.id_usage = 21 and code_cat_energie <> 8 then a.adjusted_ratio  -- clim aussi
        when id_usage not in (0, 21) and code_cat_energie <> 8 
        	then a.ratio_corrigelec - (a.ratio_corrigelec / h.sum2) * (h.diff_chauffage + h.diff_clim)    -- correction hors-chauffage, pour garder la même conso totale
        else a.ratio_corrigelec
    END AS ratio_corrige
FROM 
    AdjustedRatio a
    JOIN HeatingDiff h ON a.Date = h.Date AND a.id_metropole = h.id_metropole
order by Date DESC, id_metropole, id_usage, code_cat_energie ;
ALTER TABLE prj_res_tps_reel.w_ratios_usage_energie_metropoles_corriges  -- Cles etrangeères
ADD CONSTRAINT fk_id_usage1
FOREIGN KEY (id_usage)
REFERENCES commun.tpk_usages(id_usage),
ADD CONSTRAINT fk_code_cat_energie1
FOREIGN KEY (code_cat_energie)
REFERENCES total.tpk_cat_energie_color(code_cat_energie);
comment on table prj_res_tps_reel.w_ratios_usage_energie_metropoles_corriges is 'Consommations par usage et énergie corrigées selon la température, pour chaque jour et métropole';
select * from prj_res_tps_reel.w_ratios_usage_energie_metropoles_corriges;
-- verif même conso totale pour 1 jour 1 ville
/*
select date, id_metropole, sum(ratio_conso) as Avant, sum(ratio_corrige) as Apres
from prj_res_tps_reel.w_ratios_usage_energie_metropoles_corriges
group by date, id_metropole
order by date desc, id_metropole; 
select date, id_metropole, sum(ratio_corrige) 
from prj_res_tps_reel.w_ratios_usage_energie_metropoles_corriges
where code_cat_energie = 8 
group by date, id_metropole*/
select *  from prj_res_tps_reel.w_ratios_usage_energie_metropoles_corriges where temperature < 4;






-- APPLICATION CONSO TPS REEL * RATIOconso
drop table if exists prj_res_tps_reel.w_consos_usage_energie_test2;
CREATE table if not exists prj_res_tps_reel.w_consos_usage_energie_test2 AS
select date, id_metropole, consommation_est_mwh as conso_totale_mwh, id_usage, code_cat_energie, ratio_conso, ratios.temperature, ratio_corrige, consommation_est_mwh * ratio_corrige as consommation_usage_energie_mwh
from conso_est natural join prj_res_tps_reel.w_ratios_usage_energie_metropoles_corriges ratios
order by date desc, id_metropole, id_usage, code_cat_energie
;
ALTER TABLE prj_res_tps_reel.w_consos_usage_energie_test2  -- Cles etrangeères
ADD CONSTRAINT fk_id_usage5
FOREIGN KEY (id_usage)
REFERENCES commun.tpk_usages(id_usage),
ADD CONSTRAINT fk_code_cat_energie5
FOREIGN KEY (code_cat_energie)
REFERENCES total.tpk_cat_energie_color(code_cat_energie);
comment on table prj_res_tps_reel.w_consos_usage_energie_test2 is 'Consommations (estimations) par usage et énergie à partir des consos electriques en temps réel et des ratios de consommation. Pour chaque jour et métropole';

select * from prj_res_tps_reel.w_consos_usage_energie_test2;

-- Verif somme elec
/*
select date, id_metropole, avg(conso_totale_mwh), SUM(consommation_usage_energie_mwh) as sum_elec
from prj_res_tps_reel.w_consos_usage_energie_test2
where code_cat_energie = 8
group by date, id_metropole; */










-- CALCUL FACTEURS D'EMISSION  : Emission_usage_energie / conso_usage_energie. Par commune, par polluant
-- Voir Calcul_fe.sql pour refaire le calcul.
select * from prj_res_tps_reel.w_facteurs_emiss_metropoles ;





-- CALCUL EMISSIONS TEMPS REEL : Emission [jour, commune, polluant, usage, energie] = conso[jour, commune, usage, energie] * fe [commune, polluant, usage, energie]

-- tables à merger
--select * from prj_res_tps_reel.w_consos_usage_energie_test2 conso; select * from prj_res_tps_reel.w_facteurs_emiss_metropoles fe;

drop table if exists prj_res_tps_reel.w_emissions_tps_reel_test2;
create table prj_res_tps_reel.w_emissions_tps_reel_test2 as 
	select conso.date, conso.id_metropole, src.consommation_est_mwh as conso_elec_totale_mwh, id_polluant, id_usage, code_cat_energie, ratio_conso, src.consommation_est_mwh * ratio_conso as conso_provisoire, conso.temperature, ratio_corrige, conso.consommation_usage_energie_mwh, facteur_emission_kg_by_mwh, 
		consommation_usage_energie_mwh * facteur_emission_kg_by_mwh  as Emission_tps_reel_kg
	from prj_res_tps_reel.w_consos_usage_energie_test2 conso 
		natural join prj_res_tps_reel.w_facteurs_emiss_metropoles fe
		inner join conso_est src on (src.Date = conso.Date and src.id_metropole = conso.id_metropole)
	order by date desc, id_metropole, id_polluant, id_usage, code_cat_energie;
ALTER TABLE prj_res_tps_reel.w_emissions_tps_reel_test2  -- Cles etrangères
ADD CONSTRAINT fk_id_usage4
FOREIGN KEY (id_usage)
REFERENCES commun.tpk_usages(id_usage),
ADD CONSTRAINT fk_code_cat_energie4
FOREIGN KEY (code_cat_energie)
REFERENCES total.tpk_cat_energie_color(code_cat_energie),
add constraint fk_id_polluant4
foreign key (id_polluant)
references commun.tpk_polluants(id_polluant)
;
COMMENT ON TABLE prj_res_tps_reel.w_emissions_tps_reel_test2 is 'Calcul final (basé sur des estimations de conso) des émissions en temps réel sur les métropoles de Aix-Marseille Nice et Toulon. Emission = ConsoEstimee * FE';
select * from prj_res_tps_reel.w_emissions_tps_reel_test2;

-- Tout + noms
select date, id_metropole, conso_elec_totale_mwh, id_polluant, nom_court_polluant, id_usage, nom_usage, code_cat_energie, nom_court_cat_energie, 
ratio_conso, conso_provisoire, temperature, ratio_corrige, consommation_usage_energie_mwh, facteur_emission_kg_by_mwh, emission_tps_reel_kg 
from prj_res_tps_reel.w_emissions_tps_reel_test2 natural join commun.tpk_usages natural join total.tpk_cat_energie_color natural join commun.tpk_polluants;

--simple
select date, id_metropole, id_polluant, id_usage, code_cat_energie, consommation_usage_energie_mwh, facteur_emission_kg_by_mwh, emission_tps_reel_kg
from prj_res_tps_reel.w_emissions_tps_reel_test2
natural join commun.tpk_usages natural join total.tpk_cat_energie_color natural join commun.tpk_polluants;






-- Verif somme elec
select date, id_metropole, id_polluant, consommation_est_mwh, SUM(consommation_usage_energie_mwh) as sum_elec
from prj_res_tps_reel.w_emissions_tps_reel_test2 natural join conso_est
where code_cat_energie = 8  --elec
group by date, id_metropole, consommation_est_mwh, id_polluant
order by date desc, id_metropole;



