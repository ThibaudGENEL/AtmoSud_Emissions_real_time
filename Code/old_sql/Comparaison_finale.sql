-- COMPARAISON FINALE AVEC INV : utilisé pour la validation des méthodes


create temp table comparaison2023 as
select id_metropole, id_polluant, nom_court_polluant, id_usage, nom_usage, code_cat_energie, nom_court_cat_energie, emission_tps_reel_ann, emission_inventaire_ann,  emission_tps_reel_ann / emission_inventaire_ann as Rapport from
	(select id_metropole, id_polluant, id_usage, code_cat_energie, sum(emission_tps_reel_kg) as emission_tps_reel_ann
	from prj_res_tps_reel.w_emissions_tps_reel_test_hist
	where EXTRACT(year from date) = 2023
	group by id_metropole, id_polluant, id_usage, code_cat_energie) as tps_reel
natural join
	(select CASE 
	    	WHEN id_comm/1000 = 6 THEN 6100
	    	WHEN id_comm/1000 = 83 THEN 83100
			ELSE NULL
	    END AS id_metropole,
	    id_polluant, id_usage, code_cat_energie, sum(val) as Emission_inventaire_ann
	from total.bilan_comm_v11_diffusion bcvd 
	where an = (select max(an) from total.bilan_comm_v11_diffusion)
		and format_detaille_scope_2=1
		--and id_secteur_detail in (3, 4)
		and id_comm in (06006, 06009, 06011, 06013, 06020, 06021, 06025, 06027, 06032, 06033, 06034, 06039, 06042, 06046, 06054, 06055, 06059, 06060, 06064, 06065, 06066, 06072, 06073, 06074, 06075, 06080, 06088, 06102, 06103, 06109, 06110, 06111, 06114, 06117, 06119, 06120, 06121, 06122, 06123, 06126, 06127, 06129, 06144, 06146, 06147, 06149, 06151, 06153, 06156, 06157, 06159,
	/* Toulon metropole */ 83034, 83047, 83062, 83069, 83090, 83098, 83103, 83126, 83129, 83137, 83144, 83153)
	group by id_metropole, id_polluant, id_usage, code_cat_energie) as inv
natural join commun.tpk_polluants natural join total.tpk_cat_energie_color tcec natural join commun.tpk_usages tu 
	;
select * from comparaison2023;	

--SUM
select sum(emission_tps_reel_ann) as tps_reel, sum(emission_inventaire_ann) as inv
from comparaison2023



-- COMPARER SOMME CONSO source et inventaire
select sum(val) as Somme_conso_Elec_Inventaire_2021_NiceToulon, id_unite
from total.bilan_comm_v11_diffusion bcvd natural join commun.tpk_polluants tp natural join total.tpk_cat_energie_color tcec 
where an = (select max(an) from total.bilan_comm_v11_diffusion)
	and tp.nom_court_polluant = 'conso'
	and tcec.nom_court_cat_energie = 'Electricité'
	and id_comm in (--06006, 06009, 06011, 06013, 06020, 06021, 06025, 06027, 06032, 06033, 06034, 06039, 06042, 06046, 06054, 06055, 06059, 06060, 06064, 06065, 06066, 06072, 06073, 06074, 06075, 06080, 06088, 06102, 06103, 06109, 06110, 06111, 06114, 06117, 06119, 06120, 06121, 06122, 06123, 06126, 06127, 06129, 06144, 06146, 06147, 06149, 06151, 06153, 06156, 06157, 06159
	/* Toulon metropole */83034, 83047, 83062, 83069, 83090, 83098, 83103, 83126, 83129, 83137, 83144, 83153)
group by id_unite;
select sum(consommation_mwh) as Somme_conso_Elec_RTE_2023_NiceToulon
from prj_res_tps_reel.src_conso_elec_jour_test_hist
where EXTRACT(year from date) = 2021
and id_metropole in (6100, 83100);



-- validation consos usage energie. Comparaison 2021
select * from prj_res_tps_reel.w_consos_u_e_test_hist;
drop table if exists comparaison2021;
create temp table comparaison2021 as
select id_metropole, id_usage, nom_usage, code_cat_energie, nom_court_cat_energie, conso_tps_reel_ann, conso_inventaire_ann,  
	case 
		when conso_inventaire_ann = 0 then null
		else conso_tps_reel_ann / conso_inventaire_ann
	end as Rapport 
from
	(select id_metropole, id_usage, code_cat_energie, sum(consommation_usage_energie_mwh) as conso_tps_reel_ann
	from prj_res_tps_reel.w_consos_u_e_test_hist
	where EXTRACT(year from date) = 2021
	group by id_metropole, id_usage, code_cat_energie) as tps_reel
natural join
	(select CASE 
	    	WHEN id_comm/1000 = 6 THEN 6100
	    	WHEN id_comm/1000 = 83 THEN 83100
	    	when id_comm/1000 = 13 then 13000
			ELSE NULL
	    END AS id_metropole,
	    id_usage, code_cat_energie, sum(val) as conso_inventaire_ann
	from total.bilan_comm_v11_diffusion bcvd natural join commun.tpk_polluants tp
	where an = 2021
		and tp.nom_court_polluant = 'conso'
		and format_detaille_scope_2=1
		--and id_secteur_detail in (3, 4)
		and id_comm in (06006, 06009, 06011, 06013, 06020, 06021, 06025, 06027, 06032, 06033, 06034, 06039, 06042, 06046, 06054, 06055, 06059, 06060, 06064, 06065, 06066, 06072, 06073, 06074, 06075, 06080, 06088, 06102, 06103, 06109, 06110, 06111, 06114, 06117, 06119, 06120, 06121, 06122, 06123, 06126, 06127, 06129, 06144, 06146, 06147, 06149, 06151, 06153, 06156, 06157, 06159,
/* Aix-Marseille */ 13001, 13002, 13003, 13005, 13007, 13008, 13009, 13012, 13013, 13014, 13015, 13016, 13019, 13020, 13119, 13021, 13022, 13023, 13024, 13025, 13026, 13028, 13029, 13118, 13030, 13031, 13032, 13033, 13035, 13037, 13039, 13040, 13041, 13042, 13043, 13044, 13046, 13047, 13048, 13049, 13050, 13051, 13053, 13054, 13055, 13056, 13059, 13060, 13062, 13063, 13069, 13070, 13071, 84089, 13072, 13073, 13074, 13075, 13077, 13078, 13080, 13079, 13081, 13082, 13084, 13085, 13086, 13087, 13088, 13090, 13091, 13092, 13093, 13095, 13098, 13099, 13101, 13102, 83120, 13103, 13104, 13105, 13106, 13107, 13109, 13110, 13111, 13112, 13113, 13114, 13115, 13117,
/* Toulon metropole */ 83034, 83047, 83062, 83069, 83090, 83098, 83103, 83126, 83129, 83137, 83144, 83153) 
	group by id_metropole, id_usage, code_cat_energie) as inv
 natural join total.tpk_cat_energie_color tcec natural join commun.tpk_usages tu 
	;
select * from comparaison2021;	



-- validation consos usage energie RESIDENTIEL COMMUNES. Comparaison 2021
select * from prj_res_tps_reel.w_consos_u_e_estim_hist;
drop table if exists comparaison2021;
create temp table comparaison2021 as
select id_comm, id_usage, nom_usage, code_cat_energie, nom_court_cat_energie, conso_tps_reel_ann, conso_inventaire_ann,  
	case 
		when conso_inventaire_ann = 0 then null
		else conso_tps_reel_ann / conso_inventaire_ann
	end as Rapport 
from
	(select id_comm, id_usage, code_cat_energie, sum(consommation_usage_energie_mwh) as conso_tps_reel_ann
	from prj_res_tps_reel.w_consos_u_e_estim_hist
	where EXTRACT(year from date) = 2021
	group by id_comm, id_usage, code_cat_energie) as tps_reel
natural join
	(select id_comm,
	    id_usage, code_cat_energie, sum(val) as conso_inventaire_ann
	from total.bilan_comm_v11_diffusion bcvd natural join commun.tpk_polluants tp
	where an = 2021
		and tp.nom_court_polluant = 'conso'
		and format_detaille_scope_2=1
		and id_secteur_detail in (3, 4)
		and id_comm in (13001, 83069, 84088, 06136, 05065, 04012)
	group by id_comm, id_usage, code_cat_energie) as inv
 natural join total.tpk_cat_energie_color tcec natural join commun.tpk_usages tu 
	;
select * from comparaison2021;	



-- validation consos usage energie RESIDENTIEL COMMUNES. Comparaison 2021
select * from prj_res_tps_reel.w_consos_u_e_estim_hist;
drop table if exists comparaison2021;
create temp table comparaison2021 as
select id_comm, id_usage, nom_usage, code_cat_energie, nom_court_cat_energie, conso_tps_reel_ann, conso_inventaire_ann,  
	case 
		when conso_inventaire_ann = 0 then null
		else conso_tps_reel_ann / conso_inventaire_ann
	end as Rapport 
from
	(select id_comm, id_usage, code_cat_energie, sum(consommation_usage_energie_mwh) as conso_tps_reel_ann     --conso_provisoire pour voir sans correction, consommation_usage_energie_mwh avec
	from prj_res_tps_reel.w_consos_u_e_estim_hist
	where EXTRACT(year from date) = 2021
	group by id_comm, id_usage, code_cat_energie) as tps_reel
natural join
	(select id_comm,
	    id_usage, code_cat_energie, sum(val) as conso_inventaire_ann
	from total.bilan_comm_v11_diffusion bcvd natural join commun.tpk_polluants tp
	where an = 2021
		and tp.nom_court_polluant = 'conso'
		and format_detaille_scope_2=1
		and id_secteur_detail in (3, 4)
		and id_comm in (13001, 83069, 84088, 06136, 05065, 04012)
	group by id_comm, id_usage, code_cat_energie) as inv
 natural join total.tpk_cat_energie_color tcec natural join commun.tpk_usages tu 
	;
select * from comparaison2021;	
-- SOMME DES CONSOS tous usages confondus SIMILAIRE ?
select id_comm, sum(conso_tps_reel_ann), sum(conso_inventaire_ann), sum(conso_tps_reel_ann) /  sum(conso_inventaire_ann) as rapport_sum
from comparaison2021
group by id_comm;


-- validation consos usage energie Meth2 RESIDENTIEL COMMUNES. Comparaison 2021
select * from prj_res_tps_reel.w2_consos_u_e_estim_hist;
drop table if exists comparaison2021;
create temp table comparaison2021 as
select id_comm, id_usage, nom_usage, code_cat_energie, nom_court_cat_energie, conso_tps_reel_ann, conso_inventaire_ann,  
	case 
		when conso_inventaire_ann = 0 then null
		else conso_tps_reel_ann / conso_inventaire_ann
	end as Rapport 
from
	(select id_comm, id_usage, code_cat_energie, sum(conso_u_e_mwh) as conso_tps_reel_ann  
	from prj_res_tps_reel.w2_consos_u_e_estim_hist
	where EXTRACT(year from date) = 2021
	group by id_comm, id_usage, code_cat_energie) as tps_reel
left join
	(select id_comm,
	    id_usage, code_cat_energie, sum(val) as conso_inventaire_ann
	from total.bilan_comm_v11_diffusion bcvd natural join commun.tpk_polluants tp
	where an = 2021
		and tp.nom_court_polluant = 'conso'
		and format_detaille_scope_2=1
		and id_secteur_detail in (3)
		and id_comm/1000 in (4, 5, 6, 13, 83, 84)
	group by id_comm, id_usage, code_cat_energie) as inv
using (id_comm, id_usage, code_cat_energie)
natural join total.tpk_cat_energie_color tcec natural join commun.tpk_usages tu 
;
select * from comparaison2021;
--RMSE
SELECT SQRT(AVG(POWER(conso_tps_reel_ann - conso_inventaire_ann, 2))) AS rmse, AVG(ABS(conso_tps_reel_ann - conso_inventaire_ann)) AS mae
FROM comparaison2021;

-- SOMME DES CONSOS tous usages confondus SIMILAIRE ?
select id_comm, sum(conso_tps_reel_ann), sum(conso_inventaire_ann), sum(conso_tps_reel_ann) / sum(conso_inventaire_ann) as rapport_sum
from comparaison2021
group by id_comm;

-- COMPARAISON JOUR
select * from prj_res_tps_reel.w2_consos_u_e_estim_hist;
drop table if exists comparaison_jour;
create temp table comparaison_jour as
select id_comm, id_usage, nom_usage, code_cat_energie, nom_court_cat_energie, conso_tps_reel_j, conso_inventaire_j,  
	case 
		when conso_inventaire_j = 0 then null
		else conso_tps_reel_j / conso_inventaire_j
	end as Rapport 
from
	(select id_comm, id_usage, code_cat_energie, sum(conso_u_e_mwh) as conso_tps_reel_j 
	from prj_res_tps_reel.w2_consos_u_e_estim_hist
	where date = '2023-11-01'
	group by id_comm, id_usage, code_cat_energie) as tps_reel
LEFT JOIN (
    SELECT id_comm,
           id_usage, 
           code_cat_energie, 
           SUM(val) / 365 AS conso_inventaire_j
    FROM (
        SELECT id_comm,
               id_usage,
               code_cat_energie, 
               CASE 
                   WHEN id_usage = 34 THEN 0.92063 * val  -- Diminution de AutreSpe pour elnever la clim
                   ELSE val 
               END AS val
        FROM total.bilan_comm_v11_diffusion bcvd 
        NATURAL JOIN commun.tpk_polluants tp
        WHERE an = 2021
          AND tp.nom_court_polluant = 'conso'
          AND format_detaille_scope_2 = 1
          AND id_secteur_detail IN (3)
          AND id_comm / 1000 IN (4, 5, 6, 13, 83, 84)
        
        UNION ALL
        
        SELECT id_comm,
               21 AS id_usage,
               8 AS code_cat_energie,
               (0.07937 * val) AS val   -- clim par rapport à autrespe
        FROM total.bilan_comm_v11_diffusion bcvd 
        NATURAL JOIN commun.tpk_polluants tp
        WHERE an = 2021
          AND tp.nom_court_polluant = 'conso'
          AND format_detaille_scope_2 = 1
          AND id_secteur_detail IN (3)
          AND id_comm / 1000 IN (4, 5, 6, 13, 83, 84)
          AND id_usage = 34  -- autrespe
    ) AS subquery
    GROUP BY id_comm, id_usage, code_cat_energie
) AS inv
using (id_comm, id_usage, code_cat_energie)
natural join total.tpk_cat_energie_color tcec natural join commun.tpk_usages tu 
;
select * from comparaison_jour;


-- validation 2019 AVEC CLIM (8% AutreSpe) consos usage energie Meth2 RESIDENTIEL COMMUNES. 
select * from prj_res_tps_reel.w2_consos_u_e_estim_hist;
drop table if exists comparaison2019;
create temp table comparaison2019 as
select id_comm, id_usage, nom_usage, code_cat_energie, nom_court_cat_energie, conso_tps_reel_ann, conso_inventaire_ann,  
	case 
		when conso_inventaire_ann = 0 then null
		else conso_tps_reel_ann / conso_inventaire_ann
	end as Rapport 
from
	(select id_comm, id_usage, code_cat_energie, sum(conso_u_e_mwh) as conso_tps_reel_ann  
	from prj_res_tps_reel.w2_consos_u_e_estim_hist
	where EXTRACT(year from date) = 2019
	group by id_comm, id_usage, code_cat_energie) as tps_reel
LEFT JOIN (
    SELECT id_comm,
           id_usage, 
           code_cat_energie, 
           SUM(val) AS conso_inventaire_ann
    FROM (
        SELECT id_comm,
               id_usage,
               code_cat_energie, 
               CASE 
                   WHEN id_usage = 34 THEN 0.92063 * val  -- Diminution de AutreSpe pour elnever la clim
                   ELSE val 
               END AS val
        FROM total.bilan_comm_v11_diffusion bcvd 
        NATURAL JOIN commun.tpk_polluants tp
        WHERE an = 2021
          AND tp.nom_court_polluant = 'conso'
          AND format_detaille_scope_2 = 1
          AND id_secteur_detail IN (3)
          AND id_comm / 1000 IN (4, 5, 6, 13, 83, 84)
        
        UNION ALL
        
        SELECT id_comm,
               21 AS id_usage,
               8 AS code_cat_energie,
               0.07937 * val AS val   -- clim par rapport à autrespe
        FROM total.bilan_comm_v11_diffusion bcvd 
        NATURAL JOIN commun.tpk_polluants tp
        WHERE an = 2019
          AND tp.nom_court_polluant = 'conso'
          AND format_detaille_scope_2 = 1
          AND id_secteur_detail IN (3)
          AND id_comm / 1000 IN (4, 5, 6, 13, 83, 84)
          AND id_usage = 34  -- autrespe
    ) AS subquery
    GROUP BY id_comm, id_usage, code_cat_energie
) AS inv
using (id_comm, id_usage, code_cat_energie)
natural join total.tpk_cat_energie_color tcec natural join commun.tpk_usages tu 
;
select * from comparaison2019;
--RMSE
SELECT SQRT(AVG(POWER(conso_tps_reel_ann - conso_inventaire_ann, 2))) AS rmse, AVG(ABS(conso_tps_reel_ann - conso_inventaire_ann)) AS mae
FROM comparaison2019;
-- SOMME DES CONSOS tous usages confondus SIMILAIRE ?
select id_comm, sum(conso_tps_reel_ann), sum(conso_inventaire_ann), sum(conso_tps_reel_ann) / sum(conso_inventaire_ann) as rapport_sum
from comparaison2019
group by id_comm
order by rapport_sum asc;


-- validation AVEC CLIM (8% AutreSpe) consos usage energie Meth2 RESIDENTIEL COMMUNES. Comparaison 2021
select * from prj_res_tps_reel.w2_consos_u_e_estim_hist;
drop table if exists prj_res_tps_reel.comparaison2022;
create table prj_res_tps_reel.comparaison2022 as
select id_comm, id_usage, nom_usage, code_cat_energie, nom_court_cat_energie, conso_tps_reel_ann, conso_inventaire_ann,  
	case 
		when conso_inventaire_ann = 0 then null
		else conso_tps_reel_ann / conso_inventaire_ann
	end as Rapport 
from
	(select id_comm, id_usage, code_cat_energie, sum(conso_u_e_mwh) as conso_tps_reel_ann  
	from prj_res_tps_reel.w2_consos_u_e_estim_hist
	where EXTRACT(year from date) = 2022
	group by id_comm, id_usage, code_cat_energie) as tps_reel
LEFT JOIN (
    SELECT id_comm,
           id_usage, 
           code_cat_energie, 
           SUM(val) AS conso_inventaire_ann
    FROM (
        SELECT id_comm,
               id_usage,
               code_cat_energie, 
               CASE 
                   WHEN id_usage = 34 THEN 0.92063 * val  -- Diminution de AutreSpe pour elnever la clim
                   ELSE val 
               END AS val
        FROM total.bilan_comm_v11_diffusion bcvd 
        NATURAL JOIN commun.tpk_polluants tp
        WHERE an = 2022
          AND tp.nom_court_polluant = 'conso'
          AND format_detaille_scope_2 = 1
          AND id_secteur_detail IN (3)
          AND id_comm / 1000 IN (4, 5, 6, 13, 83, 84)
        
        UNION ALL
        
        SELECT id_comm,
               21 AS id_usage,
               8 AS code_cat_energie,
               0.07937 * val AS val   -- clim par rapport à autrespe
        FROM total.bilan_comm_v11_diffusion bcvd 
        NATURAL JOIN commun.tpk_polluants tp
        WHERE an = 2022
          AND tp.nom_court_polluant = 'conso'
          AND format_detaille_scope_2 = 1
          AND id_secteur_detail IN (3)
          AND id_comm / 1000 IN (4, 5, 6, 13, 83, 84)
          AND id_usage = 34  -- autrespe
    ) AS subquery
    GROUP BY id_comm, id_usage, code_cat_energie
) AS inv
using (id_comm, id_usage, code_cat_energie)
natural join total.tpk_cat_energie_color tcec natural join commun.tpk_usages tu 
;
select * from prj_res_tps_reel.comparaison2022;
--RMSE
SELECT SQRT(AVG(POWER(conso_tps_reel_ann - conso_inventaire_ann, 2))) AS rmse, AVG(ABS(conso_tps_reel_ann - conso_inventaire_ann)) AS mae
FROM prj_res_tps_reel.comparaison2022;
-- SOMME DES CONSOS tous usages confondus SIMILAIRE ?
select id_comm, sum(conso_tps_reel_ann), sum(conso_inventaire_ann), sum(conso_tps_reel_ann) / sum(conso_inventaire_ann) as rapport_sum
from prj_res_tps_reel.comparaison2022
group by id_comm
order by rapport_sum asc;




