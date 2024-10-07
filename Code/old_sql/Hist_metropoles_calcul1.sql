-- AVEC HISTORIQUE DE CONSO


-- DONNEES CONSO A JOUR

select * from prj_res_tps_reel.src_conso_elec_jour_test_hist order by date desc , id_metropole ;


-- CALCUL RATIO DE CONSO  : Conso_usage_energie / conso elec. Par commune
-- Voir Calcul_ratio_conso.sql pour refaire le calcul.
select * from prj_res_tps_reel.w_ratios_conso_metropoles;


-- CORRECTION AVEC TEMPERATURE

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

-- group by JOUR et Calcul DJ17, DJ21
drop table if exists prj_res_tps_reel.w_meteo_hist;
create table prj_res_tps_reel.w_meteo_hist as
select t.id_station, CAST(t.date_tu AS DATE) AS date,
	CASE
	    WHEN EXTRACT(DOW FROM date_tu) = 0 THEN 'Dimanche'
	    WHEN EXTRACT(DOW FROM date_tu) = 1 THEN 'Lundi'
	    WHEN EXTRACT(DOW FROM date_tu) = 2 THEN 'Mardi'
	    WHEN EXTRACT(DOW FROM date_tu) = 3 THEN 'Mercredi'
	    WHEN EXTRACT(DOW FROM date_tu) = 4 THEN 'Jeudi'
	    WHEN EXTRACT(DOW FROM date_tu) = 5 THEN 'Vendredi'
	    WHEN EXTRACT(DOW FROM date_tu) = 6 THEN 'Samedi'
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
from prj_res_tps_reel.src_meteo_hist t
GROUP by t.id_station, CAST(t.date_tu AS DATE), EXTRACT(DOW FROM date_tu)
ORDER by date desc, t.id_station;
;	
comment on table prj_res_tps_reel.w_meteo_hist is 'Température, DJ17 et DJ21 quotidiens pour chaque commune en Région Sud jusqu à aujourd hui';
select * from prj_res_tps_reel.w_meteo_hist;
-- group by metropole
drop table if exists meteo_metropole;
create temp table meteo_metropole as
select Date, Weekday,
	CASE 
    	WHEN id_station/1000 = 6 THEN 6100
    	WHEN id_station/1000 = 83 THEN 83100
    	when id_station/1000 = 13 then 13000
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
from prj_res_tps_reel.w_meteo_hist join commun.src_pop_comm pop on id_station = pop.id_comm
where id_station in (06006, 06009, 06011, 06013, 06020, 06021, 06025, 06027, 06032, 06033, 06034, 06039, 06042, 06046, 06054, 06055, 06059, 06060, 06064, 06065, 06066, 06072, 06073, 06074, 06075, 06080, 06088, 06102, 06103, 06109, 06110, 06111, 06114, 06117, 06119, 06120, 06121, 06122, 06123, 06126, 06127, 06129, 06144, 06146, 06147, 06149, 06151, 06153, 06156, 06157, 06159,
/* Aix-Marseille */ 13001, 13002, 13003, 13005, 13007, 13008, 13009, 13012, 13013, 13014, 13015, 13016, 13019, 13020, 13119, 13021, 13022, 13023, 13024, 13025, 13026, 13028, 13029, 13118, 13030, 13031, 13032, 13033, 13035, 13037, 13039, 13040, 13041, 13042, 13043, 13044, 13046, 13047, 13048, 13049, 13050, 13051, 13053, 13054, 13055, 13056, 13059, 13060, 13062, 13063, 13069, 13070, 13071, 84089, 13072, 13073, 13074, 13075, 13077, 13078, 13080, 13079, 13081, 13082, 13084, 13085, 13086, 13087, 13088, 13090, 13091, 13092, 13093, 13095, 13098, 13099, 13101, 13102, 83120, 13103, 13104, 13105, 13106, 13107, 13109, 13110, 13111, 13112, 13113, 13114, 13115, 13117,
/* Toulon metropole */ 83034, 83047, 83062, 83069, 83090, 83098, 83103, 83126, 83129, 83137, 83144, 83153) 
	and pop.id_version_pop = 16 and pop.an = (select max(an) from commun.src_pop_comm)   --2023
group by id_metropole, date, Weekday
order by date desc, id_metropole
;
select * from meteo_metropole;
-- DJ de base par metropole :
select * from prj_res_tps_reel.w_bases_dj_metropoles;

--POP
select * from commun.src_pop_comm where id_comm/1000 in (4, 5, 6, 13, 83, 84) and id_version_pop = 16 and an = (select max(an) from commun.src_pop_comm) ;  --2023;

-- correction, selon la meteo

-- Etape 1 : Energie = Electricité 
-- Données joined ratios_conso et météo
drop table if exists InitialData;
CREATE TEMPORARY TABLE InitialData AS
SELECT id_metropole, id_usage, code_cat_energie, consommation, consommation_elec, ratio_conso, date, temperature, dj17, base_dj17, dj21, base_dj21
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
--select count(*) from AdjustedRatio;
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
DROP TABLE IF EXISTS w_ratios_usage_energie_metropoles_corriges;
CREATE temp TABLE w_ratios_usage_energie_metropoles_corriges AS
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
select * from w_ratios_usage_energie_metropoles_corriges;
-- verif même conso totale pour 1 jour 1 ville
/*
select date, id_metropole, sum(ratio_conso) as Avant, sum(ratio_corrige) as Apres
from w_ratios_usage_energie_metropoles_corriges
group by date, id_metropole
order by date desc, id_metropole; 
select date, id_metropole, sum(ratio_corrige) 
from w_ratios_usage_energie_metropoles_corriges
where code_cat_energie = 8 
group by date, id_metropole*/
select *  from w_ratios_usage_energie_metropoles_corriges where temperature < 4;




-- APPLICATION CONSO TPS REEL * RATIOconso   
drop table if exists prj_res_tps_reel.w_consos_u_e_test_hist;
CREATE table if not exists prj_res_tps_reel.w_consos_u_e_test_hist AS
select date, id_metropole, consommation_mwh as conso_totale_mwh, id_usage, code_cat_energie, ratio_conso, temperature, ratio_corrige, consommation_mwh * ratio_corrige as consommation_usage_energie_mwh
from prj_res_tps_reel.src_conso_elec_jour_test_hist natural join w_ratios_usage_energie_metropoles_corriges
order by date desc, id_metropole, id_usage, code_cat_energie
;
comment on table prj_res_tps_reel.w_consos_u_e_test_hist is 'Historique Consommations par usage et énergie à partir des consos electriques en temps réel et des ratios de consommation. Pour chaque jour et métropole';
select * from prj_res_tps_reel.w_consos_u_e_test_hist;

-- verif somme ok
/*
select id_metropole, date, sum(ratio_conso) as sum_ratios, avg(conso_totale_mwh), sum(consommation_usage_energie_mwh)
from prj_res_tps_reel.w_consos_u_e_test_hist cuet natural join total.tpk_cat_energie_color
where nom_court_cat_energie = 'Electricité'
group by id_metropole, date; */










-- CALCUL FACTEURS D'EMISSION  : Emission_usage_energie / conso_usage_energie. Par commune, par polluant
-- Voir Calcul_fe.sql pour refaire le calcul.
select * from prj_res_tps_reel.w_facteurs_emiss_metropoles ;





-- CALCUL EMISSIONS TEMPS REEL : Emission [jour, commune, polluant, usage, energie] = conso[jour, commune, usage, energie] * fe [commune, polluant, usage, energie]

-- tables à merger
--select * from prj_res_tps_reel.w_consos_u_e_test_hist conso; select * from prj_res_tps_reel.w_facteurs_emiss_metropoles fe;

drop table if exists prj_res_tps_reel.w_emissions_tps_reel_test_hist;
create table prj_res_tps_reel.w_emissions_tps_reel_test_hist as 
	select conso.date, conso.id_metropole, src.consommation_mwh as conso_elec_totale_mwh, id_polluant, id_usage, code_cat_energie, ratio_conso, src.consommation_mwh * ratio_conso as conso_provisoire, temperature, ratio_corrige, conso.consommation_usage_energie_mwh, facteur_emission_kg_by_mwh, 
		consommation_usage_energie_mwh * facteur_emission_kg_by_mwh  as Emission_tps_reel_kg
	from prj_res_tps_reel.w_consos_u_e_test_hist conso 
		natural join prj_res_tps_reel.w_facteurs_emiss_metropoles fe
		inner join prj_res_tps_reel.src_conso_elec_jour_test_hist src on (src.Date = conso.Date and src.id_metropole = conso.id_metropole)
	order by date desc, id_metropole, id_polluant, id_usage, code_cat_energie;
ALTER TABLE prj_res_tps_reel.w_emissions_tps_reel_test_hist  -- Cles etrangères
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
COMMENT ON TABLE prj_res_tps_reel.w_emissions_tps_reel_test_hist is ' V2 Calcul final des émissions en temps réel sur les métropoles de Nice et Toulon. Emission = Conso * FE';
select * from prj_res_tps_reel.w_emissions_tps_reel_test_hist;

-- Tout + noms
select date, id_metropole, conso_elec_totale_mwh, id_polluant, nom_court_polluant, id_usage, nom_usage, code_cat_energie, nom_court_cat_energie, 
ratio_conso, conso_provisoire, temperature, ratio_corrige, consommation_usage_energie_mwh, facteur_emission_kg_by_mwh, emission_tps_reel_kg 
from prj_res_tps_reel.w_emissions_tps_reel_test_hist natural join commun.tpk_usages natural join total.tpk_cat_energie_color natural join commun.tpk_polluants;

--simple
select date, id_metropole, id_polluant, id_usage, code_cat_energie, consommation_usage_energie_mwh, facteur_emission_kg_by_mwh, emission_tps_reel_kg
from prj_res_tps_reel.w_emissions_tps_reel_test_hist
natural join commun.tpk_usages natural join total.tpk_cat_energie_color natural join commun.tpk_polluants;



-- Verif somme elec
select date, id_metropole, id_polluant, consommation_mwh, SUM(consommation_usage_energie_mwh) as sum_elec
from prj_res_tps_reel.w_emissions_tps_reel_test_hist natural join prj_res_tps_reel.src_conso_elec_jour_test_hist
where code_cat_energie = 8
group by date, id_metropole, consommation_mwh, id_polluant
order by date desc, id_metropole;

