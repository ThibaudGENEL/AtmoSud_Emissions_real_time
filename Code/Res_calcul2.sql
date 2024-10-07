-- Emissions en temps reel du Residentiel.	Execution : ~7 minutes
-- METHODE NUMERO 2 - tps réel
--> on considère que toute variation de conso > [conso "de base" sans Chauff ni clim]  est dûe au chauffage ou à la clim.
-- permet de fixer toutes les consos autres, et on fait varier quotidiennement chauffage et clim en fonction de la température


-- connexion à cadastre pour import meteo
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
select t.id_station_meteo as id_comm, CAST(t.date_jour AS DATE) AS date, 
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


--CONSO ELEC ESTIMEE AVEC MODELE
-- conso   modele : entraînés sur des données de tmpératures PACA (moyenne brute)
drop table if exists prj_res_tps_reel.w_conso_res_estim_tr;
create table prj_res_tps_reel.w_conso_res_estim_tr as
select date,  id_comm, temperature, part_conso_comm,
	CASE WHEN Temperature < 3 THEN (66704.23588245558 + (-3902.887827704622 * Temperature)) * part_conso_comm
         WHEN Temperature BETWEEN 3 and 27 THEN (87430.11 + (-20095.08277786156 * POW(Temperature, 1))+ (4219.728678066393 * POW(Temperature, 2))+ (-482.5300092060183 * POW(Temperature, 3))+ (28.074054017762844 * POW(Temperature, 4))+ (-0.7877522906343015 * POW(Temperature, 5))+ (0.008504717233948255 * POW(Temperature, 6))) * part_conso_comm 
         ELSE (13295.935215642927 + (799.4012802720838 * Temperature)) * part_conso_comm 
     END AS Consommation_est_mwh
from prj_res_tps_reel.w_meteo_tps_reel meteo natural join prj_res_tps_reel.w2_parts_conso_res_comm parts
order by date desc, id_comm;
comment on table prj_res_tps_reel.w_conso_res_estim_tr is 'Estimation de conso électrique res en temps réel (jour) pour chaque commune, selon la température';


-- SEPARATION DE LA CONSO
-- sans utiliser l'inventaire : BASE_CONSO[commune] = Min(Estimation) - ecs_fixe
-- CONSO ELEC = BASE_CONSO + CONSO_CHAUFF + CONSO_CLIM. Identifions conso chauffage et conso clim, en regardant la diff entre la conso estimee totale et base_conso.
drop table if exists prj_res_tps_reel.w2_consos_sep_tr;
create table prj_res_tps_reel.w2_consos_sep_tr as
WITH min_avg_conso AS (
    SELECT 
        id_comm,
        min_consommation_est_mwh,
        conso_elec_ecs_fixe
    FROM (SELECT id_comm, MIN(consommation_est_mwh) AS min_consommation_est_mwh FROM prj_res_tps_reel.w_conso_res_estim_hist GROUP BY id_comm) as min_est
    	natural join (select id_comm, consommation as conso_elec_ecs_fixe from prj_res_tps_reel.w2_consos_fixes_usage_energie where id_usage = 1 and code_cat_energie = 8) as ecs  -- conso journalière d'elec pour ECS par commune
),
max_tempe_diff as (
	select id_comm, max(abs(tmoy.temperature_moy - temperature) / 0.28) as X    -- Pour modif à max +- 28% l'ECS
	from prj_res_tps_reel.w2_temperatures_moy tmoy natural join prj_res_tps_reel.w_conso_res_estim_tr
	group by id_comm)
SELECT 
    date, 
    w.id_comm, 
    mac.min_consommation_est_mwh - mac.conso_elec_ecs_fixe as base_consoelec_mwh, 
    temperature, 
    consommation_est_mwh,
    mac.conso_elec_ecs_fixe as conso_elec_ecs_fixe_mwh,
    mac.conso_elec_ecs_fixe * (1 + (tmoy.temperature_moy - temperature) / max_tempe_diff.X) as conso_elec_ecs_mwh,   -- Correction ecs selon meteo
    CASE -- Chauffage = Conso_est - base_conso - conso_ECS (corrigée)
        WHEN temperature < 17 THEN consommation_est_mwh - (mac.min_consommation_est_mwh - mac.conso_elec_ecs_fixe) - (mac.conso_elec_ecs_fixe * (1 + (tmoy.temperature_moy - temperature) / max_tempe_diff.X))
        ELSE 0         -- bornée à 0 en bas
    END AS conso_elec_chauffage_mwh,
    CASE 
        WHEN temperature > 21 THEN consommation_est_mwh - (mac.min_consommation_est_mwh - mac.conso_elec_ecs_fixe) - (mac.conso_elec_ecs_fixe * (1 + (tmoy.temperature_moy - temperature) / max_tempe_diff.X))
        ELSE 0
    END AS conso_elec_clim_mwh
FROM 
    prj_res_tps_reel.w_conso_res_estim_tr w
JOIN min_avg_conso mac using (id_comm)
join prj_res_tps_reel.w2_temperatures_moy  tmoy using (id_comm)
join max_tempe_diff using (id_comm)
order by date desc;
comment on table prj_res_tps_reel.w2_consos_sep_tr is 'Décomposition de la conso électrique (BASE_CONSO + ECS + CHAUFF/CLIM) par jour et commune';


-- RATIOS DE CONSO  : voir Calcul_ratios_conso.sql
--select * from prj_res_tps_reel.w2_ratios_conso_uvariables;



-- APPLICATION CONSO * RATIO_CONSO
-- provisoire : clim non incluse dans les lignes car pas de ratio
drop table if exists consos_u_e_estim_tr;
create temp table if not exists consos_u_e_estim_tr AS
select date, id_comm, base_consoelec_mwh, temperature, conso_elec_chauffage_mwh, conso_elec_clim_mwh, id_usage, ratios_bis.usage_old, code_cat_energie, ratio_conso_uvariable,
	case
		when id_usage not in (0, 1, 21) then consos_ue_inv.consommation    -- usages fixes : moyenne j des consos de cet usage cette énergie
		when id_usage = 0 then conso_elec_chauffage_mwh * ratio_conso_uvariable	--usages variables : conso elec * ratio [ratio = conso energie usage / conso elec usage]
		when id_usage = 1 then conso_elec_ecs_mwh * ratio_conso_uvariable
		when id_usage = 21 then conso_elec_clim_mwh
	end as conso_u_e_mwh
from prj_res_tps_reel.w2_consos_sep_tr
	inner join prj_res_tps_reel.w2_ratios_conso_uvariables ratios_bis using (id_comm)
	inner join prj_res_tps_reel.w2_consos_fixes_usage_energie consos_ue_inv using (id_comm, id_usage, code_cat_energie)
order by date desc, id_comm, id_usage, code_cat_energie
;
-- Inclusion lignes CLIM par force
drop table if exists prj_res_tps_reel.w2_consos_u_e_estim_tr;
CREATE table if not exists prj_res_tps_reel.w2_consos_u_e_estim_tr as
WITH climatisation AS (
    SELECT distinct
        date, id_comm, base_consoelec_mwh, temperature, conso_elec_chauffage_mwh, conso_elec_clim_mwh, 21 AS id_usage,'Climatisation' as usage_old,8 AS code_cat_energie, 
        1 AS ratio_conso_uvariable, conso_elec_clim_mwh AS conso_u_e_mwh
    FROM consos_u_e_estim_tr
    order by date desc
)
SELECT date, id_comm, base_consoelec_mwh, temperature, conso_elec_chauffage_mwh, conso_elec_clim_mwh, id_usage, usage_old, code_cat_energie, 
	ratio_conso_uvariable,conso_u_e_mwh
FROM consos_u_e_estim_tr
union ALL
SELECT * FROM climatisation
ORDER BY date DESC, id_comm, id_usage, code_cat_energie
;
-- 7,937% de AutreSpe2 est de la clim -> on les enlève
update prj_res_tps_reel.w2_consos_u_e_estim_tr
set conso_u_e_mwh = conso_u_e_mwh * (1 - 0.07937)
where id_usage = 34    -- AutreSpe2
;
-- Cles etrangeères
ALTER TABLE prj_res_tps_reel.w2_consos_u_e_estim_tr  
ADD CONSTRAINT fk_id_usage5
FOREIGN KEY (id_usage)
REFERENCES commun.tpk_usages(id_usage),
ADD CONSTRAINT fk_code_cat_energie5
FOREIGN KEY (code_cat_energie)
REFERENCES total.tpk_cat_energie_color(code_cat_energie);
comment on table prj_res_tps_reel.w2_consos_u_e_estim_tr is 'Consommations par usage et énergie en temps réel à partir des températures (modèle d estimation) et des ratios de consommation. Pour chaque jour et commune';



-- CALCUL FACTEURS D'EMISSION  : Emission_usage_energie / conso_usage_energie. Par commune, par polluant
-- Voir Calcul_fe.sql pour refaire le calcul.
--SELECT * FROM prj_res_tps_reel.w_facteurs_emiss_res;




-- -- CALCUL EMISSIONS TEMPS REEL : Emission [jour, commune, polluant, usage, energie] = conso[jour, commune, usage, energie] * fe [commune, polluant, usage, energie]
drop table if exists prj_res_tps_reel.emissions_estim_tr;
create table prj_res_tps_reel.emissions_estim_tr as 
	select conso.date, conso.id_comm, base_consoelec_mwh, temperature, id_polluant, id_usage, code_cat_energie, ratio_conso_uvariable, conso.conso_u_e_mwh, facteur_emission_kg_by_mwh, 
		conso_u_e_mwh * facteur_emission_kg_by_mwh  as Emission_tps_reel_kg
	from prj_res_tps_reel.w2_consos_u_e_estim_tr conso 
		natural join prj_res_tps_reel.w_facteurs_emiss_res fe
	-- where nom_court_polluant in('CO', 'COVNM', 'NOx', 'PM10', 'PM2.5', 'SOx', 'NH3', 'CH4.EQCO2', 'CO2.bio', 'CO2.nbio', 'N2O.EQCO2', 'PRG100_3GES', 'Benzene', 'BenzoAPyren', '16hap', 'BC', 'conso')
	where id_polluant in(3, 4, 11, 16, 36, 38, 48, 65, 108, 111, 121, 122, 123, 124, 128, 131, 187)
	order by date desc, id_comm, id_polluant, id_usage, code_cat_energie;
ALTER TABLE prj_res_tps_reel.emissions_estim_tr  -- Cles etrangères
ADD CONSTRAINT fk_id_usage5
FOREIGN KEY (id_usage)
REFERENCES commun.tpk_usages(id_usage),
ADD CONSTRAINT fk_code_cat_energie5
FOREIGN KEY (code_cat_energie)
REFERENCES total.tpk_cat_energie_color(code_cat_energie),
add constraint fk_id_polluant5
foreign key (id_polluant)
references commun.tpk_polluants(id_polluant)
;
COMMENT ON TABLE prj_res_tps_reel.emissions_estim_tr is 'Calcul final (basé sur des estimations de conso) des émissions temps réel sur les communes. Emission = ConsoEstimee * FE';



-- Export
drop table if exists prj_res_tps_reel.export_emissions_estim_tr;
create table prj_res_tps_reel.export_emissions_estim_tr as
select distinct date, id_comm, comm.nom_comm, comm.siren_epci, comm.nom_epci, id_comm/1000 as departement, temperature, id_polluant, 
	case 
		when id_polluant = 128 then 'Total_GES'    -- anciennement PRG100_3GES
		else nom_court_polluant
	end as nom_court_polluant,
	id_usage, 
	case -- AutreSpe2, Loisirs et Elec spécifique -> Autre
		when id_usage in (34, 35, 3) then 'Autre'    -- 'AutreSpe2', 'Loisirs', 'Electricité spécifique'
		when id_usage = 1 then 'Eau chaude'
		else nom_usage
	end as nom_usage, 
	code_cat_energie, nom_court_cat_energie, conso_u_e_mwh, emission_tps_reel_kg
from prj_res_tps_reel.emissions_estim_tr natural join commun.tpk_usages natural join total.tpk_cat_energie_color natural join commun.tpk_polluants 
natural join (select id_comm, nom_comm, siren_epci, nom_epci from commun.tpk_comm_evol comm1 where 
						an_ref = (select max(an_ref) from commun.tpk_comm_evol comm2 where comm1.id_comm = comm2.id_comm)) comm
	-- filtre date pour limiter la taille ? : and date BETWEEN CURRENT_DATE - interval '6 months' and CURRENT_DATE + interval '3 days'
order by date desc, id_comm, id_polluant, id_usage, code_cat_energie;
ALTER TABLE prj_res_tps_reel.export_emissions_estim_tr  -- Cles etrangères
ADD CONSTRAINT fk_id_usage6
FOREIGN KEY (id_usage)
REFERENCES commun.tpk_usages(id_usage),
ADD CONSTRAINT fk_code_cat_energie6
FOREIGN KEY (code_cat_energie)
REFERENCES total.tpk_cat_energie_color(code_cat_energie),
add constraint fk_id_polluant6
foreign key (id_polluant)
references commun.tpk_polluants(id_polluant);
COMMENT ON TABLE prj_res_tps_reel.export_emissions_estim_tr is 'Version Visualisation de emissions_estim_tr';
select * from prj_res_tps_reel.export_emissions_estim_tr;


-- DICOS pour optimisation viz
----drop table if exists prj_res_tps_reel.dico_polluants_u_e;
----drop table if exists prj_res_tps_reel.dico_communes;
create table if not exists prj_res_tps_reel.dico_communes as
select distinct departement, siren_epci, nom_epci, id_comm, nom_comm from prj_res_tps_reel.export_emissions_estim_tr;
create table if not exists prj_res_tps_reel.dico_polluants_u_e as
(select distinct id_polluant, 
	case 
		when id_polluant = 128 then 'Total_GES'
		else nom_court_polluant
	end as nom_court_polluant, 
	id_usage, nom_usage, code_cat_energie, nom_court_cat_energie from prj_res_tps_reel.export_emissions_estim_tr)
;

-- Autorisations extérieures
Grant select on all tables in schema prj_res_tps_reel to readonly;
