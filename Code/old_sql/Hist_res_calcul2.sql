-- METHODE NUMERO 2 - Historique pour tester.
--> on considère que toute variation de conso > [conso "de base" sans Chauff ni clim]  est dûe au chauffage ou à la clim.
-- permet de fixer toutes les consos autres, et on fait varier quotidiennement chauffage et clim en fonction de la température

-- ConsoMOY hors usages variables (chauff, clim..) sur 4 ans, issue de l'inventaire. -> Nos consos fixes si on prend base_conso_inv
drop table if exists prj_res_tps_reel.w2_bases_conso;
create table prj_res_tps_reel.w2_bases_conso as
select id_comm, SUM(val) / (365.25 * 4) as Base_consoelec_mwh   -- 365*3 + 366 est le nb de jours dans 4 ans 
from total.bilan_comm_v11_diffusion natural join commun.tpk_polluants natural join commun.tpk_usages natural join total.tpk_cat_energie_color tcec
where nom_court_polluant = 'conso'
and tcec.nom_court_cat_energie = 'Electricité'
	and id_usage not in (0, 1, 21)     --nom_usage not in ('Chauffage', 'ECS', 'Climatisation')
	and (an between (select max(an)- 3 from total.bilan_comm_v11_diffusion) and (select max(an) from total.bilan_comm_v11_diffusion))  -- 2018-2021 inclus
	and format_detaille_scope_2=1
	and id_comm/1000 in (4,5,6,13,83,84) 
	and id_secteur_detail in (3)  -- Residentiel
group by id_comm;
comment on table prj_res_tps_reel.w2_bases_conso is 'Consommation journalière moyenne des usages fixes (tous sauf Chauffage, Climatisation, ECS) pour chaque commune.';
select * from prj_res_tps_reel.w2_bases_conso;

select sum(Base_consoelec_mwh) from prj_res_tps_reel.w2_bases_conso;

-- TEMPERATURE -> Estimer conso Chauff/Clim
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

-- TEMPERATURE MOY de chaque commune
drop table if exists prj_res_tps_reel.w2_temperatures_moy;
create table prj_res_tps_reel.w2_temperatures_moy as
SELECT id_station AS id_comm, AVG(Temperature) AS temperature_moy 
FROM prj_res_tps_reel.src_meteo_hist 
WHERE id_station / 1000 IN (4, 5, 6, 13, 83, 84)
    and EXTRACT(year from date_tu) between (select max(an)- 3 from total.bilan_comm_v11_diffusion) and (select max(an) from total.bilan_comm_v11_diffusion)   -- 2018-2021 inclus
GROUP BY id_station;  
comment on table prj_res_tps_reel.w2_temperatures_moy is 'Températures moyennes de chaque commune en Région Sud, prise sur la période 2018-2021 (4 ans)';
select * from prj_res_tps_reel.w2_temperatures_moy;
-- group by JOUR et Calcul DJ17, DJ21
drop table if exists prj_res_tps_reel.w_meteo_hist;
create table prj_res_tps_reel.w_meteo_hist as
select t.id_station as id_comm, CAST(t.date_tu AS DATE) AS date,
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
GROUP by t.id_station, CAST(t.date_tu AS DATE)
ORDER by date desc, t.id_station;
;	
comment on table prj_res_tps_reel.w_meteo_hist is 'Température, DJ17 et DJ21 quotidiens pour chaque commune en Région Sud jusqu à aujourd hui';
select * from prj_res_tps_reel.w_meteo_hist;

-- Merge avec tps_reel
drop table if exists prj_res_tps_reel.w_meteo;
create table prj_res_tps_reel.w_meteo as
select id_comm, date, temperature, dj17, dj21 from prj_res_tps_reel.w_meteo_hist wmh where id_comm/1000 in (4, 5, 6, 13, 83, 84)
UNION
select id_comm, date, temperature, dj17, dj21 from prj_res_tps_reel.w_meteo_tps_reel where id_comm/1000 in (4, 5, 6, 13, 83, 84)
order by date desc, id_comm;
select * from prj_res_tps_reel.w_meteo;


--CONSO ELEC ESTIMEE AVEC MODELE
-- PART DE CONSO DE CHAQUE COMMUNE EN PACA   (RESIDENTIEL)
drop table if exists prj_res_tps_reel.w2_parts_conso_res_comm;
create table prj_res_tps_reel.w2_parts_conso_res_comm as
select *, Conso_annee / Conso_totale_annee as Part_Conso_comm from
(select id_comm, sum(val) as Conso_Annee
from total.bilan_comm_v11_diffusion bcvd natural join commun.tpk_polluants tp natural join total.tpk_cat_energie_color tcec 
where tp.nom_court_polluant = 'conso'
	and tcec.nom_court_cat_energie = 'Electricité'
	and id_secteur_detail in (3)  -- Résidentiel
	and an between (select max(an)- 3 from total.bilan_comm_v11_diffusion) and (select max(an) from total.bilan_comm_v11_diffusion)   -- 2018-2021 inclus
	and id_comm/1000 in (4, 5, 6, 13, 83, 84)
	and format_detaille_scope_2=1
group by id_comm) as consos
cross join (
select sum(val) as Conso_Totale_Annee
from total.bilan_comm_v11_diffusion bcvd natural join commun.tpk_polluants tp natural join total.tpk_cat_energie_color tcec 
where tp.nom_court_polluant = 'conso'
	and tcec.nom_court_cat_energie = 'Electricité'
	and id_secteur_detail in (3)    -- Résidentiel
	and an between (select max(an)- 3 from total.bilan_comm_v11_diffusion) and (select max(an) from total.bilan_comm_v11_diffusion)   -- 2018-2021 inclus
	and id_comm/1000 in (4, 5, 6, 13, 83, 84)
	and format_detaille_scope_2=1) as conso
;
comment on table prj_res_tps_reel.w2_parts_conso_res_comm is 'Part de consommation de chaque commune en région PACA, prise sur la période 2018-2021 (4 ans)';
select * from prj_res_tps_reel.w2_parts_conso_res_comm;
-- conso   modele : entraînés sur des données de températures PACA (moyenne brute)
drop table if exists prj_res_tps_reel.w_conso_res_estim_hist;
create table prj_res_tps_reel.w_conso_res_estim_hist as
select date,  id_comm, temperature, part_conso_comm,
	CASE WHEN Temperature < 3 THEN (66704.23588245558 + (-3902.887827704622 * Temperature)) * part_conso_comm
         WHEN Temperature BETWEEN 3 and 27 THEN (87430.11 + (-20095.08277786156 * POW(Temperature, 1))+ (4219.728678066393 * POW(Temperature, 2))+ (-482.5300092060183 * POW(Temperature, 3))+ (28.074054017762844 * POW(Temperature, 4))+ (-0.7877522906343015 * POW(Temperature, 5))+ (0.008504717233948255 * POW(Temperature, 6))) * part_conso_comm 
         ELSE (13295.935215642927 + (799.4012802720838 * Temperature)) * part_conso_comm 
     END AS Consommation_est_mwh
from prj_res_tps_reel.w_meteo_hist meteo natural join prj_res_tps_reel.w2_parts_conso_res_comm parts
order by date desc, id_comm;
comment on table prj_res_tps_reel.w_conso_res_estim_hist is 'Estimation de conso électrique res quotidienne historique pour chaque commune, selon la température';
select * from prj_res_tps_reel.w_conso_res_estim_hist;
/*
select sum(Consommation_est_mwh) as conso_estim_2021
from prj_res_tps_reel.w_conso_res_estim_hist
where EXTRACT(year from date) = 2021;
select an, sum(val) as Conso_inv_2021
from total.bilan_comm_v11_diffusion bcvd 
natural join transversal.tpk_secteur_emi_detail sect
natural join commun.tpk_polluants p
natural join total.tpk_cat_energie_color energ
where p.nom_court_polluant = 'conso' -- conso = COnsommation finale à climat réel
	and lib_secteur_detail in ('Résidentiel')
	and an between (select max(an) -3 from total.bilan_comm_v11_diffusion) and (select max(an) from total.bilan_comm_v11_diffusion)
	and format_detaille_scope_2=1
	and energ.nom_court_cat_energie = 'Electricité'
	and id_comm/1000 in (4,5,6,13,83,84) 
	group by an;*/
/*
select sum(Consommation_est_mwh) as conso_estim_2021
from prj_res_tps_reel.w_conso_res_estim_hist
where EXTRACT(year from date) = 2021;
select sum(val) as ConsoBase_inv_2021
from total.bilan_comm_v11_diffusion bcvd 
natural join transversal.tpk_secteur_emi_detail sect
natural join commun.tpk_polluants p
natural join total.tpk_cat_energie_color energ
where p.nom_court_polluant = 'conso' -- conso = COnsommation finale à climat réel
	and lib_secteur_detail in ('Résidentiel')
	and id_usage not in (0, 21)
	and an = (select max(an) from total.bilan_comm_v11_diffusion)
	and format_detaille_scope_2=1
	and energ.nom_court_cat_energie = 'Electricité';*/

-- CONSO USAGE ENERGIE POUR LES USAGES FIXES : moyenne journalière des 4 dernières années
drop table if exists prj_res_tps_reel.w2_consos_fixes_usage_energie;
create table prj_res_tps_reel.w2_consos_fixes_usage_energie as (
select id_comm, bcvd.id_usage, usages.nom_usage as usage_old, bcvd.code_cat_energie, energ.nom_court_cat_energie as energie_old, sum(val) / (4 * 365.25) as Consommation, id_unite
from total.bilan_comm_v11_diffusion bcvd 
natural join transversal.tpk_secteur_emi_detail sect
natural join commun.tpk_polluants p
natural join total.tpk_cat_energie_color energ
natural join commun.tpk_usages usages 
where p.nom_court_polluant = 'conso' -- conso = COnsommation finale à climat réel
	and lib_secteur_detail in ('Résidentiel')
	and an between (select max(an) - 3 from total.bilan_comm_v11_diffusion) and (select max(an) from total.bilan_comm_v11_diffusion)  --2018-2021 inclus -> moyenne
	and format_detaille_scope_2=1
	and id_comm/1000 in (4,5,6,13,83,84) 
group by id_comm, bcvd.id_usage, usages.nom_usage, bcvd.code_cat_energie, energ.nom_court_cat_energie, id_unite
order by id_comm, usage_old, energie_old
);
comment on table prj_res_tps_reel.w2_consos_fixes_usage_energie is 'Consommations fixes moyennes journalières par comunne, par usage (pour les usages non thermosensibles), et énergie, prise sur la période 2018-2021 (4 ans)';
select * from prj_res_tps_reel.w2_consos_fixes_usage_energie;

-- SEPARATION DE LA CONSO
-- CONSO ELEC = BASE_CONSO_inv + ECS + CONSO_CHAUFF + CONSO_CLIM. Identifions conso chauffage et conso clim, en regardant la diff entre la conso estimee totale et base_conso.
drop table if exists prj_res_tps_reel.w2_consos_sep_hist;
create table prj_res_tps_reel.w2_consos_sep_hist as
with max_tempe_diff as (
	select id_comm, max(abs(tmoy.temperature_moy - temperature) / 0.28) as X
	from prj_res_tps_reel.w2_temperatures_moy tmoy natural join prj_res_tps_reel.w_conso_res_estim_hist
	group by id_comm)
select date, id_comm, base_consoelec_mwh, temperature, part_conso_comm, consommation_est_mwh,
	cfue.Consommation as conso_fixe_elec_ecs_mwh,
	cfue.Consommation * (1 + (tmoy.temperature_moy - temperature) / max_tempe_diff.X) as conso_elec_ecs_mwh,			-- Correction ECS +-28% selon température
	case 
		when temperature < 17 then greatest(consommation_est_mwh - base_consoelec_mwh - cfue.Consommation * (1 + (tmoy.temperature_moy - temperature) / max_tempe_diff.X), 0)    -- bornée à 0 en bas
		else 0
	end as conso_elec_chauffage_mwh,
	case 
		when temperature > 21 then greatest(consommation_est_mwh - base_consoelec_mwh - cfue.Consommation * (1 + (tmoy.temperature_moy - temperature) / max_tempe_diff.X), 0)
		else 0
	end as conso_elec_clim_mwh
from prj_res_tps_reel.w2_bases_conso
	natural join prj_res_tps_reel.w_conso_res_estim_hist
	natural join prj_res_tps_reel.w2_temperatures_moy  tmoy
	natural join max_tempe_diff
	natural join  (select * from prj_res_tps_reel.w2_consos_fixes_usage_energie 	-- Conso fixe d'elec pour ECS
					where id_usage = 1 and code_cat_energie = 8) as cfue 
order by date desc;
comment on table prj_res_tps_reel.w2_consos_sep_hist is 'Décomposition de la conso électrique historique (BASE_CONSO + ECS + CHAUFF/CLIM) par jour et commune';
select * from prj_res_tps_reel.w2_consos_sep_hist;
-- sans utiliser l'inventaire
-- CONSO ELEC = ~Min(est) + CONSO_CHAUFF + CONSO_CLIM. Identifions conso chauffage et conso clim, en regardant la diff entre la conso estimee totale et base_conso.
drop table if exists prj_res_tps_reel.w2_consos_sep_hist;
create table prj_res_tps_reel.w2_consos_sep_hist as
WITH min_avg_conso AS (
    SELECT 
        id_comm,
        min_consommation_est_mwh,
        avg1721_consommation_mwh,
        (min_consommation_est_mwh + avg1721_consommation_mwh) / 2 as base_conso_milieu,   -- base_conso_enedis = Le milieu entre min(est) et conso moyenne lorsqu'il fait entre 17 et 21°C,
        conso_elec_ecs_fixe
    FROM (SELECT id_comm, MIN(consommation_est_mwh) AS min_consommation_est_mwh FROM prj_res_tps_reel.w_conso_res_estim_hist GROUP BY id_comm) as min_est
    natural join 
    	(select id_comm, avg(consommation_est_mwh) as avg1721_consommation_mwh from prj_res_tps_reel.w_conso_res_estim_hist where temperature between 17 and 21 group by id_comm) as avg1721
    natural join (select id_comm, consommation as conso_elec_ecs_fixe from prj_res_tps_reel.w2_consos_fixes_usage_energie where id_usage = 1 and code_cat_energie = 8) as ecs  -- conso journalière d'elec pour ECS par commune
),
max_tempe_diff as (
	select id_comm, max(abs(tmoy.temperature_moy - temperature) / 0.28) as X    -- 0.28 car +- 28%
	from prj_res_tps_reel.w2_temperatures_moy tmoy natural join prj_res_tps_reel.w_conso_res_estim_hist
	group by id_comm)
SELECT 
    date, 
    w.id_comm, 
    mac.min_consommation_est_mwh - mac.conso_elec_ecs_fixe as base_consoelec_mwh,   -- CHOIX DE BASE_CONSO : ici min de l'estimation
    temperature,
    part_conso_comm, 
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
FROM prj_res_tps_reel.w_conso_res_estim_hist w
JOIN min_avg_conso mac using (id_comm)
join prj_res_tps_reel.w2_temperatures_moy  tmoy using (id_comm)
join max_tempe_diff using (id_comm)
order by date desc;
comment on table prj_res_tps_reel.w2_consos_sep_hist is 'Décomposition de la conso électrique historique (BASE_CONSO + ECS + CHAUFF/CLIM) par jour et commune';
select * from prj_res_tps_reel.w2_consos_sep_hist ;


-- RATIOS DE CONSO
select * from prj_res_tps_reel.w2_ratios_conso_uvariables;

-- Où est la clim ? -> Pas présente différenciée dans l'invntaire, peut-être une fraction dans Electricité Spécifique
select distinct usages.nom_usage as usage_old
from total.bilan_comm_v11_diffusion bcvd 
natural join transversal.tpk_secteur_emi_detail sect
natural join commun.tpk_polluants p
natural join commun.tpk_usages usages 
where lib_secteur_detail in ('Résidentiel')
	and id_comm/1000 in (4,5,6,13,83,84) ;



-- APPLICATION CONSO * RATIO_CONSO
-- provisoire : clim non incluse dans les lignes car pas de ratio
drop table if exists consos_u_e_estim_hist;
create temp table if not exists consos_u_e_estim_hist AS
select date, id_comm, base_consoelec_mwh, temperature, conso_elec_chauffage_mwh, conso_elec_clim_mwh, id_usage, ratios_bis.usage_old, code_cat_energie, ratio_conso_uvariable,
	case
		when id_usage not in (0, 1, 21) then consos_ue_inv.consommation    -- usages fixes : moyenne j des consos de cet usage cette énergie
		when id_usage = 0 then conso_elec_chauffage_mwh * ratio_conso_uvariable	--usages variables : conso elec * ratio [ratio = conso energie usage / conso elec usage]
		when id_usage = 1 then conso_elec_ecs_mwh * ratio_conso_uvariable
		when id_usage = 21 then conso_elec_clim_mwh
	end as conso_u_e_mwh
from prj_res_tps_reel.w2_consos_sep_hist
	inner join prj_res_tps_reel.w2_ratios_conso_uvariables ratios_bis using (id_comm)
	inner join prj_res_tps_reel.w2_consos_fixes_usage_energie consos_ue_inv using (id_comm, id_usage, code_cat_energie)
order by date desc, id_comm, id_usage, code_cat_energie
;
-- Inclusion lignes CLIM par force
drop table if exists prj_res_tps_reel.w2_consos_u_e_estim_hist;
CREATE table if not exists prj_res_tps_reel.w2_consos_u_e_estim_hist as
WITH climatisation AS (
    SELECT distinct
        date, id_comm, base_consoelec_mwh, temperature, conso_elec_chauffage_mwh, conso_elec_clim_mwh, 21 AS id_usage,'Climatisation' as usage_old,8 AS code_cat_energie, 
        1 AS ratio_conso_uvariable, conso_elec_clim_mwh AS conso_u_e_mwh
    FROM consos_u_e_estim_hist
    order by date desc
)
SELECT date, id_comm, base_consoelec_mwh, temperature, conso_elec_chauffage_mwh, conso_elec_clim_mwh, id_usage, usage_old, code_cat_energie, 
	ratio_conso_uvariable,conso_u_e_mwh
FROM consos_u_e_estim_hist
union ALL
SELECT * FROM climatisation
ORDER BY date DESC, id_comm, id_usage, code_cat_energie
;
-- 7,937% de AutreSpe2 est de la clim -> on les enlève
update prj_res_tps_reel.w2_consos_u_e_estim_hist
set conso_u_e_mwh = conso_u_e_mwh * (1 - 0.07937)
where id_usage = 34    -- AutreSpe2
;
-- Cles etrangeères
ALTER TABLE prj_res_tps_reel.w2_consos_u_e_estim_hist  
ADD CONSTRAINT fk_id_usage5
FOREIGN KEY (id_usage)
REFERENCES commun.tpk_usages(id_usage),
ADD CONSTRAINT fk_code_cat_energie5
FOREIGN KEY (code_cat_energie)
REFERENCES total.tpk_cat_energie_color(code_cat_energie);
comment on table prj_res_tps_reel.w2_consos_u_e_estim_hist is 'Consommations par usage et énergie à partir des températures (modèle d estimation) et des ratios de consommation. Pour chaque jour et commune';
select * from prj_res_tps_reel.w2_consos_u_e_estim_hist order by date desc, id_comm, id_usage ;



/*
-- CALCUL FACTEURS D'EMISSION  : Emission_usage_energie / conso_usage_energie. Par commune, par polluant
-- Voir Calcul_fe.sql pour refaire le calcul.
SELECT * FROM prj_res_tps_reel.w_facteurs_emiss_res;

-- -- CALCUL EMISSIONS TEMPS REEL : Emission [jour, commune, polluant, usage, energie] = conso[jour, commune, usage, energie] * fe [commune, polluant, usage, energie]
drop table if exists prj_res_tps_reel.w2_emissions_estim_hist;
create table prj_res_tps_reel.w2_emissions_estim_hist as 
	select conso.date, conso.id_comm, base_consoelec_mwh, temperature, id_polluant, id_usage, code_cat_energie, ratio_conso_uvariable, conso.conso_u_e_mwh, facteur_emission_kg_by_mwh, 
		conso_u_e_mwh * facteur_emission_kg_by_mwh  as Emission_tps_reel_kg
	from prj_res_tps_reel.w2_consos_u_e_estim_hist conso 
		natural join prj_res_tps_reel.w_facteurs_emiss_res fe
	where id_comm in (4012, 5065, 6136, 13001, 83069, 84088)	-- ATTENTION filtre (trop long sinon)
	order by date desc, id_comm, id_polluant, id_usage, code_cat_energie;
ALTER TABLE prj_res_tps_reel.w2_emissions_estim_hist  -- Cles etrangères
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
COMMENT ON TABLE prj_res_tps_reel.w2_emissions_estim_hist is 'Calcul final V2 (basé sur des estimations de conso) des émissions temps réel (historique 2020-2023) sur les communes. Emission = ConsoEstimee * FE';
select * from prj_res_tps_reel.w2_emissions_estim_hist;

*/
