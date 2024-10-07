-- SCRIPT A METTRE A JOUR POUR CHANGER LA VERSION DE L'INVENTAIRE
--> Toutes ces tables sont les tables issues de l'inventaire qui sont utilisées dans le calcul temps réel.


-- RATIOS DE CONSO utilisés pour les usages variables
-- Conso par usage energie, pour chaque commune  RESIDENTIEL
drop table if exists w_consos_usage_energie;
create temp table w_consos_usage_energie as (
select id_comm, bcvd.id_usage, usages.nom_usage as usage_old, bcvd.code_cat_energie, energ.nom_court_cat_energie as energie_old, sum(val) as Consommation, id_unite
from total.bilan_comm_v11_diffusion bcvd 
natural join transversal.tpk_secteur_emi_detail sect
natural join commun.tpk_polluants p
natural join total.tpk_cat_energie_color energ
natural join commun.tpk_usages usages 
where p.nom_court_polluant = 'conso' -- conso = COnsommation finale à climat réel
	and lib_secteur_detail in ('Résidentiel')
	and an = (select max(an) from total.bilan_comm_v11_diffusion)
	and format_detaille_scope_2=1
	and id_comm/1000 in (4,5,6,13,83,84) 
group by id_comm, bcvd.id_usage, usages.nom_usage, bcvd.code_cat_energie, energ.nom_court_cat_energie, id_unite
order by id_comm, usage_old, energie_old
);
--Conso elec totale PAR USAGE (bis), pour chaque commune
drop table if exists w_conso_elec;
create temp table w_conso_elec as (
select id_comm, id_usage, sum(val) as Consommation_elec, id_unite as id_unite_elec
from total.bilan_comm_v11_diffusion bcvd 
natural join transversal.tpk_secteur_emi_detail sect
natural join commun.tpk_polluants p
natural join total.tpk_cat_energie_color energ
where p.nom_court_polluant = 'conso' -- conso = COnsommation finale à climat réel
	and lib_secteur_detail in ('Résidentiel')
	and an = (select max(an) from total.bilan_comm_v11_diffusion)
	and format_detaille_scope_2=1
	and id_comm/1000 in (4,5,6,13,83,84) 
	and energ.nom_court_cat_energie = 'Electricité'
group by id_comm, id_usage, id_unite
order by Consommation_elec desc
);
select * from w_conso_elec;
-- calcul
drop table if exists prj_res_tps_reel.w2_ratios_conso_uvariables;
create table prj_res_tps_reel.w2_ratios_conso_uvariables as(
select id_comm, id_usage, usage_old, code_cat_energie, energie_old, consommation, id_unite, consommation_elec, 
	case 
		when consommation_elec > 0 then consommation / consommation_elec
		else NULL
	end AS ratio_conso_uvariable
from w_consos_usage_energie consos natural join w_conso_elec
order by id_comm, id_usage, code_cat_energie
); 
ALTER TABLE prj_res_tps_reel.w2_ratios_conso_uvariables  -- Cles etrangeères
ADD CONSTRAINT fk_id_usage
FOREIGN KEY (id_usage)
REFERENCES commun.tpk_usages(id_usage),
ADD CONSTRAINT fk_code_cat_energie
FOREIGN KEY (code_cat_energie)
REFERENCES total.tpk_cat_energie_color(code_cat_energie),
add constraint fk_id_unite
foreign key (id_unite)
references commun.tpk_unite(id_unite)
;
COMMENT ON TABLE prj_res_tps_reel.w2_ratios_conso_uvariables  -- Description de la table
IS 'Ratios de consommation (residentiel) pour les usages variables. par commune, usage et énergie. Formule : Conso[N-2] par usage et énergie / Conso electrique PAR USAGE [N-2]';
select * from prj_res_tps_reel.w2_ratios_conso_uvariables;



-- FACTEURS D'EMISSION TOUS SECTEURS (utile pour clim)
-- Conso par energie
drop table if exists consos;
create temp table consos as (
select id_comm, bcvd.id_usage, bcvd.code_cat_energie, sum(val) as Consommation_mwh
from total.bilan_comm_v11_diffusion bcvd 
natural join transversal.tpk_secteur_emi_detail sect
natural join commun.tpk_polluants p
where p.nom_court_polluant = 'conso' -- conso = COnsommation finale à climat réel
	--and lib_secteur_detail in ('Résidentiel','Tertiaire')
	and an = (select max(an) from total.bilan_comm_v11_diffusion)
	and format_detaille_scope_2=1
	and id_comm/1000 in (4,5,6,13,83,84) 
group by id_comm, bcvd.id_usage,bcvd.code_cat_energie) ;
select * from consos;

-- Emission par energie
drop table if exists emissions;
create temp table emissions as (
select id_polluant, nom_court_polluant, id_comm, bcvd.id_usage, bcvd.code_cat_energie, sum(val) as Emission_kg
from total.bilan_comm_v11_diffusion bcvd 
natural join transversal.tpk_secteur_emi_detail sect
natural join commun.tpk_polluants p
where 	--and lib_secteur_detail in ('Résidentiel','Tertiaire')
	an = (select max(an) from total.bilan_comm_v11_diffusion)
	and format_detaille_scope_2=1
	and id_comm/1000 in (4,5,6,13,83,84) 
group by id_polluant, nom_court_polluant, id_comm, bcvd.id_usage,bcvd.code_cat_energie) ;
select * from emissions;
--FE
DROP TABLE IF EXISTS prj_res_tps_reel.w_facteurs_emiss;
CREATE TABLE prj_res_tps_reel.w_facteurs_emiss AS (
    SELECT id_comm, id_polluant, nom_court_polluant, id_usage, code_cat_energie, Emission_kg, Consommation_mwh, 
	    case 
	    	when Consommation_mwh > 0 then Emission_kg / Consommation_mwh
	    	else NULL
	    end AS facteur_emission_kg_by_mwh
    FROM consos
    NATURAL JOIN emissions
    order by id_comm, id_polluant, id_usage, code_cat_energie
);
ALTER TABLE prj_res_tps_reel.w_facteurs_emiss  -- Cles etrangeères
ADD CONSTRAINT fk_id_usage2
FOREIGN KEY (id_usage)
REFERENCES commun.tpk_usages(id_usage),
ADD CONSTRAINT fk_code_cat_energie2
FOREIGN KEY (code_cat_energie)
REFERENCES total.tpk_cat_energie_color(code_cat_energie),
add constraint fk_id_polluant2
foreign key (id_polluant)
references commun.tpk_polluants(id_polluant)
;
COMMENT ON TABLE prj_res_tps_reel.w_facteurs_emiss is 'Facteurs d Emission (tous secteurs) pour chaque polluant selon commune, usage, énergie.';  -- Description de la table
SELECT * FROM prj_res_tps_reel.w_facteurs_emiss;

-- FACTEURS D'EMISSIONS du secteur résidentiel
-- Conso par energie
drop table if exists consos;
create temp table consos as (
select id_comm, bcvd.id_usage, bcvd.code_cat_energie, sum(val) as Consommation_mwh
from total.bilan_comm_v11_diffusion bcvd 
natural join transversal.tpk_secteur_emi_detail sect
natural join commun.tpk_polluants p
where p.nom_court_polluant = 'conso' -- conso = COnsommation finale à climat réel
	and lib_secteur_detail in ('Résidentiel')
	and an = (select max(an) from total.bilan_comm_v11_diffusion)
	and format_detaille_scope_2=1
	and id_comm/1000 in (4,5,6,13,83,84) 
group by id_comm, bcvd.id_usage,bcvd.code_cat_energie) ;
select * from consos;
-- Emission par energie
drop table if exists emissions;
create temp table emissions as (
select id_polluant, nom_court_polluant, id_comm, bcvd.id_usage, bcvd.code_cat_energie, sum(val) as Emission_kg
from total.bilan_comm_v11_diffusion bcvd 
natural join transversal.tpk_secteur_emi_detail sect
natural join commun.tpk_polluants p
where lib_secteur_detail in ('Résidentiel') 
	and an = (select max(an) from total.bilan_comm_v11_diffusion)
	and format_detaille_scope_2=1
	and id_comm/1000 in (4,5,6,13,83,84) 
group by id_polluant, nom_court_polluant, id_comm, bcvd.id_usage,bcvd.code_cat_energie) ;
select * from emissions;
--FE
DROP TABLE IF EXISTS prj_res_tps_reel.w_facteurs_emiss_res;
CREATE TABLE prj_res_tps_reel.w_facteurs_emiss_res AS (
    SELECT id_comm, id_polluant, nom_court_polluant, id_usage, code_cat_energie, Emission_kg, Consommation_mwh, 
	    case 
	    	when Consommation_mwh > 0 then Emission_kg / Consommation_mwh
	    	else NULL
	    end AS facteur_emission_kg_by_mwh
    FROM consos
    NATURAL JOIN emissions
UNION 
	(select * from prj_res_tps_reel.w_facteurs_emiss where id_usage = 21)    -- ajout lignes clim issues de facteurs tous secteurs
order by id_comm, id_polluant, id_usage, code_cat_energie
);
ALTER TABLE prj_res_tps_reel.w_facteurs_emiss_res  -- Cles etrangeères
ADD CONSTRAINT fk_id_usage2
FOREIGN KEY (id_usage)
REFERENCES commun.tpk_usages(id_usage),
ADD CONSTRAINT fk_code_cat_energie2
FOREIGN KEY (code_cat_energie)
REFERENCES total.tpk_cat_energie_color(code_cat_energie),
add constraint fk_id_polluant2
foreign key (id_polluant)
references commun.tpk_polluants(id_polluant)
;
COMMENT ON TABLE prj_res_tps_reel.w_facteurs_emiss_res is 'Facteurs d Emission (résidentiel) pour chaque polluant selon commune, usage, énergie.';  -- Description de la table
SELECT * FROM prj_res_tps_reel.w_facteurs_emiss_res;



-- TEMPERATURE MOY HISTORIQUE DE CHAQUE COMMUNE - utile pour correction ECS.	nécessite src_meteo_hist. Existe dans schema prj_res_tps_reel. Code dans Hist_res_calcul2.sql si besoin.
drop table if exists prj_res_tps_reel.w2_temperatures_moy;
create table prj_res_tps_reel.w2_temperatures_moy as
SELECT id_station AS id_comm, AVG(Temperature) AS temperature_moy 
FROM prj_res_tps_reel.src_meteo_hist 
WHERE id_station / 1000 IN (4, 5, 6, 13, 83, 84)
    and EXTRACT(year from date_tu) BETWEEN EXTRACT(year from CURRENT_DATE) - 3 AND EXTRACT(year from CURRENT_DATE) - 1        -- moy sur 2021-2023
GROUP BY id_station;  
comment on table prj_res_tps_reel.w2_temperatures_moy is 'Températures moyennes de chaque commune en Région Sud, prise sur les 3 dernières années complètes';
select * from prj_res_tps_reel.w2_temperatures_moy;


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
comment on table prj_res_tps_reel.w2_parts_conso_res_comm is 'Part de consommation de chaque commune en région PACA, prise sur les 4 dernières années de l inventaire';
select * from prj_res_tps_reel.w2_parts_conso_res_comm; 



-- HISTORIQUE DES CONSOS (estimées)   modele : entraîné sur des données de températures PACA (moyenne brute)
-- METEO group by JOUR et Calcul DJ17, DJ21
/*drop table if exists prj_res_tps_reel.w_meteo_hist;
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
comment on table prj_res_tps_reel.w_meteo_hist is 'Température, DJ17 et DJ21 quotidiens pour chaque commune en Région Sud jusqu à fin 2023';
select * from prj_res_tps_reel.w_meteo_hist; */
-- CONSO VIA MODELE
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


-- CONSO USAGE ENERGIE POUR LES USAGES FIXES : moyenne journalière des 4 dernières années
drop table if exists prj_res_tps_reel.w2_consos_fixes_usage_energie;
create table prj_res_tps_reel.w2_consos_fixes_usage_energie as (
select id_comm, bcvd.id_usage, usages.nom_usage as usage_old, bcvd.code_cat_energie, energ.nom_court_cat_energie as energie_old, sum(val) / (4 * 365.25) as Consommation, id_unite
from total.bilan_comm_v11_diffusion bcvd 
natural join total.tpk_secteur_emi_detail sect
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
comment on table prj_res_tps_reel.w2_consos_fixes_usage_energie is 'Consommations fixes moyennes journalières par comunne, par usage (pour les usages non thermosensibles), et énergie, prise sur les 4 dernières années de linventaire';
select * from prj_res_tps_reel.w2_consos_fixes_usage_energie; 

