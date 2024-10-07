-- CALCUL FACTEUR D'EMISSION  TOUS SECTEURS

-- "polluants" à prendre
--select * from commun.tpk_polluants tp;


-- RESIDENTIEL+TERTIAIRE
-- Conso par energie
drop table if exists consos;
create temp table consos as (
select id_comm, bcvd.id_usage, bcvd.code_cat_energie, sum(val) as Consommation_mwh
from total.bilan_comm_v11_diffusion bcvd 
natural join transversal.tpk_secteur_emi_detail sect
natural join commun.tpk_polluants p
where p.nom_court_polluant = 'conso' -- conso = COnsommation finale à climat réel
	and lib_secteur_detail in ('Résidentiel','Tertiaire')
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
where lib_secteur_detail in ('Résidentiel','Tertiaire')
	and an = (select max(an) from total.bilan_comm_v11_diffusion)
	and format_detaille_scope_2=1
	and id_comm/1000 in (4,5,6,13,83,84) 
group by id_polluant, nom_court_polluant, id_comm, bcvd.id_usage,bcvd.code_cat_energie) ;
select * from emissions;

--FACTEURS D'EMISSION
DROP TABLE IF EXISTS prj_res_tps_reel.w_facteurs_emiss_rester;
CREATE TABLE prj_res_tps_reel.w_facteurs_emiss_rester AS (
    SELECT id_comm, id_polluant, nom_court_polluant, id_usage, code_cat_energie, Emission_kg, Consommation_mwh, 
	    case 
	    	when Consommation_mwh > 0 then Emission_kg / Consommation_mwh
	    	else NULL
	    end AS facteur_emission_kg_by_mwh
    FROM consos
    NATURAL JOIN emissions
    order by id_comm, id_polluant, id_usage, code_cat_energie
);
ALTER TABLE prj_res_tps_reel.w_facteurs_emiss_rester  -- Cles etrangeères
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
COMMENT ON TABLE prj_res_tps_reel.w_facteurs_emiss_rester is 'Facteurs d Emission (résidentiel+tertiaire) pour chaque polluant selon commune, usage, énergie.';  -- Description de la table
SELECT * FROM prj_res_tps_reel.w_facteurs_emiss_rester;




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

select * from prj_res_tps_reel.w_facteurs_emiss where facteur_emission_kg_by_mwh > 0 order by facteur_emission_kg_by_mwh desc ;

-- nombre de facteurs d'emission differents (étalés par commune)
select id_polluant, id_usage, code_cat_energie, count(distinct facteur_emission_kg_by_mwh) from prj_res_tps_reel.w_facteurs_emiss group by id_polluant, id_usage, code_cat_energie;


-- RESIDENTIEL
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

--FACTEURS D'EMISSION
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



-- metropoles
drop table if exists prj_res_tps_reel.w_facteurs_emiss_metropoles;
create table prj_res_tps_reel.w_facteurs_emiss_metropoles as
	select CASE 
	        WHEN id_comm / 1000 = 6 THEN 6100
	        WHEN id_comm / 1000 = 83 THEN 83100
	        when id_comm/1000 = 13 then 13000
	        ELSE NULL
	    END AS id_metropole, id_polluant, nom_court_polluant, id_usage, code_cat_energie,
	    sum(Emission_kg) as Emission_kg, sum(Consommation_mwh) as Consommation_mwh,
	    case 
	    	when sum(Consommation_mwh) > 0 then sum(Emission_kg) / sum(Consommation_mwh)
	    	else null
	    end  as facteur_emission_kg_by_mwh	
	from prj_res_tps_reel.w_facteurs_emiss
	where id_comm in (06006, 06009, 06011, 06013, 06020, 06021, 06025, 06027, 06032, 06033, 06034, 06039, 06042, 06046, 06054, 06055, 06059, 06060, 06064, 06065, 06066, 06072, 06073, 06074, 06075, 06080, 06088, 06102, 06103, 06109, 06110, 06111, 06114, 06117, 06119, 06120, 06121, 06122, 06123, 06126, 06127, 06129, 06144, 06146, 06147, 06149, 06151, 06153, 06156, 06157, 06159,
/* Aix-Marseille */ 13001, 13002, 13003, 13005, 13007, 13008, 13009, 13012, 13013, 13014, 13015, 13016, 13019, 13020, 13119, 13021, 13022, 13023, 13024, 13025, 13026, 13028, 13029, 13118, 13030, 13031, 13032, 13033, 13035, 13037, 13039, 13040, 13041, 13042, 13043, 13044, 13046, 13047, 13048, 13049, 13050, 13051, 13053, 13054, 13055, 13056, 13059, 13060, 13062, 13063, 13069, 13070, 13071, 84089, 13072, 13073, 13074, 13075, 13077, 13078, 13080, 13079, 13081, 13082, 13084, 13085, 13086, 13087, 13088, 13090, 13091, 13092, 13093, 13095, 13098, 13099, 13101, 13102, 83120, 13103, 13104, 13105, 13106, 13107, 13109, 13110, 13111, 13112, 13113, 13114, 13115, 13117,
/* Toulon metropole */ 83034, 83047, 83062, 83069, 83090, 83098, 83103, 83126, 83129, 83137, 83144, 83153) 
	group by id_metropole, id_polluant, nom_court_polluant, id_usage, code_cat_energie
	order by id_metropole, id_polluant, id_usage, code_cat_energie;
ALTER TABLE prj_res_tps_reel.w_facteurs_emiss_metropoles  -- Cles etrangeères
ADD CONSTRAINT fk_id_usage3
FOREIGN KEY (id_usage)
REFERENCES commun.tpk_usages(id_usage),
ADD CONSTRAINT fk_code_cat_energie3
FOREIGN KEY (code_cat_energie)
REFERENCES total.tpk_cat_energie_color(code_cat_energie),
add constraint fk_id_polluant3
foreign key (id_polluant)
references commun.tpk_polluants(id_polluant)
;
COMMENT ON TABLE prj_res_tps_reel.w_facteurs_emiss_metropoles is 'Facteurs d Emission pour chaque polluant selon Metropole, usage, énergie.';  -- Description de la table
select * from prj_res_tps_reel.w_facteurs_emiss_metropoles;
