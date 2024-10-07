-- TABLES FIXES
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