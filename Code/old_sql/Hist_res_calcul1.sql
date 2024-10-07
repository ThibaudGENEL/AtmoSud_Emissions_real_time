--inv


-- DONNEES CONSO A ESTIMER SELON °C


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
select t.id_station as id_comm, CAST(t.date_tu AS DATE) AS date,
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
comment on table prj_res_tps_reel.w_meteo_hist is 'Température, DJ17 et DJ21 quotidiens pour chaque commune en Région Sud jusqu à fin 2023';
select * from prj_res_tps_reel.w_meteo_hist;




-- Part de conso en PACA   RES
drop table if exists prj_res_tps_reel.w_parts_conso_res_comm;
create table prj_res_tps_reel.w_parts_conso_res_comm as
select *, Conso_annee / Conso_totale_annee as Part_Conso_comm from
(select id_comm, sum(val) as Conso_Annee
from total.bilan_comm_v11_diffusion bcvd natural join commun.tpk_polluants tp natural join total.tpk_cat_energie_color tcec 
where tp.nom_court_polluant = 'conso'
	and tcec.nom_court_cat_energie = 'Electricité'
	and id_secteur_detail in (3, 4)
	and an = (select max(an) from total.bilan_comm_v11_diffusion bcvd2)
	and id_comm/1000 in (4, 5, 6, 13, 83, 84)
	and format_detaille_scope_2=1
group by id_comm) as consos
cross join (
select sum(val) as Conso_Totale_Annee
from total.bilan_comm_v11_diffusion bcvd natural join commun.tpk_polluants tp natural join total.tpk_cat_energie_color tcec 
where tp.nom_court_polluant = 'conso'
	and tcec.nom_court_cat_energie = 'Electricité'
	and id_secteur_detail in (3, 4)
	and an = (select max(an) from total.bilan_comm_v11_diffusion bcvd2)
	and id_comm/1000 in (4, 5, 6, 13, 83, 84)
	and format_detaille_scope_2=1) as conso
;
select * from prj_res_tps_reel.w_parts_conso_res_comm;


-- conso
drop table if exists prj_res_tps_reel.w_conso_res_estim_hist;
create table prj_res_tps_reel.w_conso_res_estim_hist as
select date, weekday, id_comm, temperature, part_conso_comm,
	CASE WHEN Temperature < 1 THEN (146099.74289894194 + (-5816.140256418537 * Temperature)) * part_conso_comm
         WHEN Temperature BETWEEN 1 and 29 THEN (137552.45 + (-121.14083370349684 * POW(Temperature, 1))+ (-987.5848768754704 * POW(Temperature, 2))+ (56.47743482706349 * POW(Temperature, 3))+ (-0.8652169466900932 * POW(Temperature, 4))) * part_conso_comm 
         ELSE (13606.041105252298 + (1934.9302643135015 * Temperature)) * part_conso_comm 
     END AS Consommation_est_mwh
from prj_res_tps_reel.w_meteo_hist meteo natural join prj_res_tps_reel.w_parts_conso_res_comm parts
order by date desc, id_comm;
select * from prj_res_tps_reel.w_conso_res_estim_hist;




-- CALCUL RATIO DE CONSO  : Conso_usage_energie / conso elec. Par commune
-- Voir Calcul_ratio_conso.sql pour refaire le calcul.
select * from prj_res_tps_reel.w_ratios_conso_res;
select id_comm, sum(ratio_conso) 
from prj_res_tps_reel.w_ratios_conso_res 
where code_cat_energie = 8    --Elec
group by id_comm;







-- correction, selon la meteo

-- Etape 1 : Energie = Electricité 
-- Données joined ratios_conso et météo
drop table if exists InitialData;
CREATE TEMPORARY TABLE InitialData AS
SELECT id_comm, id_usage, code_cat_energie, consommation, consommation_elec, ratio_conso, date, meteo.temperature, dj17, base_dj17, dj21, base_dj21
FROM 
    prj_res_tps_reel.w_ratios_conso_res ratios 
    inner join prj_res_tps_reel.w_meteo_hist meteo using (id_comm)
    inner join prj_res_tps_reel.w_bases_dj djbases using (id_comm)
where date > '2020-12-31'
	and id_comm in (13001, 83069, 84088, 06136, 05065, 04012)  -- FILTRE POUR RAPIDITE, qq communes
ORDER BY Date, id_comm, id_usage, code_cat_energie;
--select * from InitialData;
-- Conso[chauffage] = Conso * (DJ17 / valeur_dj17_de_base)   exemple: 4. -> La conso pour chauffage ne change pas si DJ17 =  4 [13°C)]. Diminue si DJ17 < 4, augmente si > 4
drop table if exists AdjustedRatio;
CREATE TEMPORARY TABLE AdjustedRatio AS
SELECT 
    id_comm, id_usage, code_cat_energie, consommation, consommation_elec, ratio_conso,
    date, temperature, DJ17, DJ21, base_dj17, base_dj21,
    CASE 
        WHEN id_usage = 0 and code_cat_energie = 8 THEN ratio_conso * LEAST((DJ17 / base_dj17), (sum1_barre / sum1) + 1 - 0.01)  -- SI CHAUFFAGE
        when id_usage = 21 and code_cat_energie = 8 then ratio_conso * LEAST((DJ21 / base_dj21), (sum1_barre / sum1) + 1 - 0.01)  -- SI CLIM
        ELSE ratio_conso 
    END AS adjusted_ratio
FROM 
    (select *, 
    	(SELECT SUM(ratio_conso) FROM InitialData AS sub WHERE sub.id_usage in (0, 21) and sub.code_cat_energie = 8 AND sub.date = InitialData.date AND sub.id_comm = InitialData.id_comm) AS sum1,
    	(SELECT SUM(ratio_conso) FROM InitialData AS sub WHERE sub.id_usage not in (0, 21) and sub.code_cat_energie = 8 AND sub.date = InitialData.date AND sub.id_comm = InitialData.id_comm) AS sum1_barre
    from InitialData) as InitialData2;
--select * from AdjustedRatio;
-- Diff_chauffage avant/après
DROP TABLE IF EXISTS HeatingDiff;
CREATE TEMPORARY TABLE HeatingDiff AS
SELECT 
    Date,
    id_comm,
    SUM(CASE WHEN id_usage = 0 THEN adjusted_ratio - ratio_conso ELSE 0 END) AS diff_chauffage,
    SUM(CASE WHEN id_usage = 21 THEN adjusted_ratio - ratio_conso ELSE 0 END) AS diff_clim,
    SUM(CASE WHEN id_usage <> 0 and id_usage <> 21 and code_cat_energie = 8 THEN ratio_conso ELSE 0 END) AS sum2
FROM 
    AdjustedRatio
GROUP BY 
    Date, id_comm;
--select * from heatingdiff;
DROP TABLE IF EXISTS correctionElec;
CREATE temp table correctionElec AS
SELECT 
    a.Date, 
    a.id_comm, 
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
    JOIN HeatingDiff h ON a.Date = h.Date AND a.id_comm = h.id_comm
order by Date DESC, id_comm, id_usage, code_cat_energie ;
select * from correctionElec;
-- verif même conso totale pour 1 jour 1 ville
/*
select date, id_comm, sum(ratio_conso) as Avant, sum(ratio_corrigelec) as Apres
from correctionElec
group by date, id_comm
order by date desc, id_comm; 
select date, id_comm, sum(ratio_corrigelec) 
from correctionElec
where code_cat_energie = 8 
group by date, id_comm*/


-- ETAPE 2 : Autres énergies
-- Données joined ratios_conso et météo
drop table if exists InitialData;
CREATE TEMPORARY TABLE InitialData AS
SELECT id_comm, id_usage, code_cat_energie, ratio_conso, ratio_corrigelec, date, temperature, dj17, base_dj17, dj21, base_dj21
FROM 
    correctionElec
    natural join prj_res_tps_reel.w_bases_dj djbases
ORDER BY 
    Date DESC, id_comm, id_usage, code_cat_energie;
select * from InitialData;
-- Conso[chauffage] = Conso * (DJ17 / valeur_dj17_de_base)   exemple: 4. -> La conso pour chauffage ne change pas si DJ17 =  4 [13°C)]. Diminue si DJ17 < 4, augmente si > 4
drop table if exists AdjustedRatio;
CREATE TEMPORARY TABLE AdjustedRatio AS
SELECT 
    id_comm, id_usage, code_cat_energie, ratio_conso, ratio_corrigelec, date, temperature, base_dj17, base_dj21,
    CASE 
        WHEN id_usage = 0 and code_cat_energie <> 8 THEN ratio_conso * LEAST((DJ17 / base_dj17), (sum1_barre / sum1) + 1 - 0.01)  -- SI CHAUFFAGE
        when id_usage = 21 and code_cat_energie <> 8 then ratio_conso * LEAST((DJ21 / base_dj21), (sum1_barre / sum1) + 1 - 0.01)  -- SI CLIM
        ELSE ratio_corrigelec 
    END AS adjusted_ratio
FROM 
    (select *, 
    	(SELECT SUM(ratio_conso) FROM InitialData AS sub WHERE sub.id_usage in (0, 21) and sub.code_cat_energie <> 8 AND sub.date = InitialData.date AND sub.id_comm = InitialData.id_comm) AS sum1,
    	(SELECT SUM(ratio_conso) FROM InitialData AS sub WHERE sub.id_usage not in (0, 21) and sub.code_cat_energie <> 8 AND sub.date = InitialData.date AND sub.id_comm = InitialData.id_comm) AS sum1_barre
    from InitialData) as InitialData2;
--select * from AdjustedRatio;
-- Diff_chauffage avant/après
DROP TABLE IF EXISTS HeatingDiff;
CREATE TEMPORARY TABLE HeatingDiff AS
SELECT 
    Date,
    id_comm,
    SUM(CASE WHEN id_usage = 0 THEN adjusted_ratio - ratio_corrigelec ELSE 0 END) AS diff_chauffage,
    SUM(CASE WHEN id_usage = 21 THEN adjusted_ratio - ratio_corrigelec ELSE 0 END) AS diff_clim,
    SUM(CASE WHEN id_usage <> 0 and id_usage <> 21 and code_cat_energie <> 8 THEN ratio_corrigelec ELSE 0 END) AS sum2
FROM 
    AdjustedRatio
GROUP BY 
    Date, id_comm;
--select * from heatingdiff;
DROP TABLE IF EXISTS prj_res_tps_reel.w_ratios_conso_res_corriges;
CREATE TABLE prj_res_tps_reel.w_ratios_conso_res_corriges AS
SELECT 
    a.Date, 
    a.id_comm, 
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
    JOIN HeatingDiff h ON a.Date = h.Date AND a.id_comm = h.id_comm
order by Date DESC, id_comm, id_usage, code_cat_energie ;
ALTER TABLE prj_res_tps_reel.w_ratios_conso_res_corriges  -- Cles etrangeères
ADD CONSTRAINT fk_id_usage1
FOREIGN KEY (id_usage)
REFERENCES commun.tpk_usages(id_usage),
ADD CONSTRAINT fk_code_cat_energie1
FOREIGN KEY (code_cat_energie)
REFERENCES total.tpk_cat_energie_color(code_cat_energie);
comment on table prj_res_tps_reel.w_ratios_conso_res_corriges is 'Consommations par usage et énergie corrigées selon la température, pour chaque jour et métropole';
select * from prj_res_tps_reel.w_ratios_conso_res_corriges;
-- verif même conso totale pour 1 jour 1 ville
/*
select date, id_comm, sum(ratio_conso) as Avant, sum(ratio_corrige) as Apres
from prj_res_tps_reel.w_ratios_conso_res_corriges
group by date, id_comm
order by date desc, id_comm; 
select date, id_comm, sum(ratio_corrige) 
from prj_res_tps_reel.w_ratios_conso_res_corriges
where code_cat_energie = 8 
group by date, id_comm*/
select *  from prj_res_tps_reel.w_ratios_conso_res_corriges where temperature < 4;






-- APPLICATION CONSO TPS REEL * RATIOconso
drop table if exists prj_res_tps_reel.w_consos_u_e_estim_hist;
CREATE table if not exists prj_res_tps_reel.w_consos_u_e_estim_hist AS
select date, id_comm, consommation_est_mwh as conso_totale_mwh, id_usage, code_cat_energie, ratio_conso, 
	consommation_est_mwh*ratio_conso as conso_provisoire, ratios.temperature, ratio_corrige, consommation_est_mwh * ratio_corrige as consommation_usage_energie_mwh
from prj_res_tps_reel.w_conso_res_estim_hist natural join prj_res_tps_reel.w_ratios_conso_res_corriges ratios
order by date desc, id_comm, id_usage, code_cat_energie
;
ALTER TABLE prj_res_tps_reel.w_consos_u_e_estim_hist  -- Cles etrangeères
ADD CONSTRAINT fk_id_usage5
FOREIGN KEY (id_usage)
REFERENCES commun.tpk_usages(id_usage),
ADD CONSTRAINT fk_code_cat_energie5
FOREIGN KEY (code_cat_energie)
REFERENCES total.tpk_cat_energie_color(code_cat_energie);
comment on table prj_res_tps_reel.w_consos_u_e_estim_hist is 'Consommations par usage et énergie à partir des températures (modèle d estimation) et des ratios de consommation. Pour chaque jour et commune';

select * from prj_res_tps_reel.w_consos_u_e_estim_hist;

-- Verif somme elec
/*
select date, id_comm, avg(conso_totale_mwh), SUM(consommation_usage_energie_mwh) as sum_elec
from prj_res_tps_reel.w_consos_u_e_estim_hist
where code_cat_energie = 8
group by date, id_comm; */










-- CALCUL FACTEURS D'EMISSION  : Emission_usage_energie / conso_usage_energie. Par commune, par polluant
-- Voir Calcul_fe.sql pour refaire le calcul.
select * from prj_res_tps_reel.w_facteurs_emiss_res ;





-- CALCUL EMISSIONS TEMPS REEL : Emission [jour, commune, polluant, usage, energie] = conso[jour, commune, usage, energie] * fe [commune, polluant, usage, energie]

-- tables à merger
--select * from prj_res_tps_reel.w_consos_u_e_estim_hist conso; select * from prj_res_tps_reel.w_facteurs_emiss_res fe;

drop table if exists prj_res_tps_reel.w_emissions_estim_hist;
create table prj_res_tps_reel.w_emissions_estim_hist as 
	select conso.date, conso.id_comm, src.consommation_est_mwh as conso_elec_totale_mwh, id_polluant, id_usage, code_cat_energie, ratio_conso, src.consommation_est_mwh * ratio_conso as conso_provisoire, conso.temperature, ratio_corrige, conso.consommation_usage_energie_mwh, facteur_emission_kg_by_mwh, 
		consommation_usage_energie_mwh * facteur_emission_kg_by_mwh  as Emission_tps_reel_kg
	from prj_res_tps_reel.w_consos_u_e_estim_hist conso 
		natural join prj_res_tps_reel.w_facteurs_emiss_res fe
		inner join prj_res_tps_reel.w_conso_res_estim_hist src on (src.Date = conso.Date and src.id_comm = conso.id_comm)
	order by date desc, id_comm, id_polluant, id_usage, code_cat_energie;
ALTER TABLE prj_res_tps_reel.w_emissions_estim_hist  -- Cles etrangères
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
COMMENT ON TABLE prj_res_tps_reel.w_emissions_estim_hist is 'Calcul final (basé sur des estimations de conso) des émissions temps réel (historique 2020-2023) sur les communes. Emission = ConsoEstimee * FE';
select * from prj_res_tps_reel.w_emissions_estim_hist;

-- Tout + noms
select date, id_comm, conso_elec_totale_mwh, id_polluant, nom_court_polluant, id_usage, nom_usage, code_cat_energie, nom_court_cat_energie, 
ratio_conso, conso_provisoire, temperature, ratio_corrige, consommation_usage_energie_mwh, facteur_emission_kg_by_mwh, emission_tps_reel_kg 
from prj_res_tps_reel.w_emissions_estim_hist natural join commun.tpk_usages natural join total.tpk_cat_energie_color natural join commun.tpk_polluants;

--simple
select date, id_comm, id_polluant, id_usage, code_cat_energie, consommation_usage_energie_mwh, facteur_emission_kg_by_mwh, emission_tps_reel_kg
from prj_res_tps_reel.w_emissions_estim_hist
natural join commun.tpk_usages natural join total.tpk_cat_energie_color natural join commun.tpk_polluants;






-- Verif somme elec
select date, id_comm, id_polluant, consommation_est_mwh, SUM(consommation_usage_energie_mwh) as sum_elec
from prj_res_tps_reel.w_emissions_estim_hist natural join prj_res_tps_reel.w_conso_res_estim_hist
where code_cat_energie = 8  --elec
group by date, id_comm, consommation_est_mwh, id_polluant
order by date desc, id_comm;



