-- CALCUL RATIO DE CONSO  : Conso_usage_energie / conso elec. Par commune



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
--Conso elec totale, pour chaque commune
drop table if exists w_conso_elec;
create temp table w_conso_elec as (
select id_comm,  sum(val) as Consommation_elec, id_unite as id_unite_elec
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
group by id_comm, id_unite
order by Consommation_elec desc
);
-- calcul
drop table if exists prj_res_tps_reel.w_ratios_conso_res;
create table prj_res_tps_reel.w_ratios_conso_res as(
select id_comm, id_usage, usage_old, code_cat_energie, energie_old, consommation, id_unite, consommation_elec, consommation / consommation_elec AS ratio_conso
from w_consos_usage_energie consos full join w_conso_elec using (id_comm)
order by id_comm, id_usage, code_cat_energie
); 
ALTER TABLE prj_res_tps_reel.w_ratios_conso_res  -- Cles etrangeères
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
COMMENT ON TABLE prj_res_tps_reel.w_ratios_conso_res  -- Description de la table
IS 'Ratios de consommation (residentiel) par commune, usage et énergie. Formule : Conso[N-2] par usage et énergie / Conso electrique tous usages confondus[N-2]';

select * from prj_res_tps_reel.w_ratios_conso_res;


-- Conso par usage energie, pour chaque commune  RESIDENTIEL+TERTIAIRE
drop table if exists w_consos_usage_energie;
create temp table w_consos_usage_energie as (
select id_comm, bcvd.id_usage, usages.nom_usage as usage_old, bcvd.code_cat_energie, energ.nom_court_cat_energie as energie_old, sum(val) as Consommation, id_unite
from total.bilan_comm_v11_diffusion bcvd 
natural join transversal.tpk_secteur_emi_detail sect
natural join commun.tpk_polluants p
natural join total.tpk_cat_energie_color energ
natural join commun.tpk_usages usages 
where p.nom_court_polluant = 'conso' -- conso = COnsommation finale à climat réel
	and lib_secteur_detail in ('Résidentiel','Tertiaire')
	and an = (select max(an) from total.bilan_comm_v11_diffusion)
	and format_detaille_scope_2=1
	and id_comm/1000 in (4,5,6,13,83,84) 
group by id_comm, bcvd.id_usage, usages.nom_usage, bcvd.code_cat_energie, energ.nom_court_cat_energie, id_unite
order by id_comm, usage_old, energie_old
);
--Conso elec totale, pour chaque commune
drop table if exists w_conso_elec;
create temp table w_conso_elec as (
select id_comm,  sum(val) as Consommation_elec, id_unite as id_unite_elec
from total.bilan_comm_v11_diffusion bcvd 
natural join transversal.tpk_secteur_emi_detail sect
natural join commun.tpk_polluants p
natural join total.tpk_cat_energie_color energ
where p.nom_court_polluant = 'conso' -- conso = COnsommation finale à climat réel
	and lib_secteur_detail in ('Résidentiel','Tertiaire')
	and an = (select max(an) from total.bilan_comm_v11_diffusion)
	and format_detaille_scope_2=1
	and id_comm/1000 in (4,5,6,13,83,84) 
	and energ.nom_court_cat_energie = 'Electricité'
group by id_comm, id_unite
order by Consommation_elec desc
);
-- calcul
drop table if exists prj_res_tps_reel.w_ratios_conso_res_ter;
create table prj_res_tps_reel.w_ratios_conso_res_ter as(
select id_comm, id_usage, usage_old, code_cat_energie, energie_old, consommation, id_unite, consommation_elec, consommation / consommation_elec AS ratio_conso
from w_consos_usage_energie consos full join w_conso_elec using (id_comm)
order by id_comm, id_usage, code_cat_energie
); 
ALTER TABLE prj_res_tps_reel.w_ratios_conso_res_ter  -- Cles etrangeères
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
COMMENT ON TABLE prj_res_tps_reel.w_ratios_conso_res_ter  -- Description de la table
IS 'Ratios de consommation (residentiel)) par commune, usage et énergie. Formule : Conso[N-2] par usage et énergie / Conso electrique tous usages confondus[N-2]';

select * from prj_res_tps_reel.w_ratios_conso_res_ter;





-- Conso par usage energie, pour chaque commune  TOUS SECTEURS
drop table if exists w_consos_usage_energie;
create temp table w_consos_usage_energie as (
select id_comm, bcvd.id_usage, usages.nom_usage as usage_old, bcvd.code_cat_energie, energ.nom_court_cat_energie as energie_old, sum(val) as Consommation, id_unite
from total.bilan_comm_v11_diffusion bcvd 
natural join transversal.tpk_secteur_emi_detail sect
natural join commun.tpk_polluants p
natural join total.tpk_cat_energie_color energ
natural join commun.tpk_usages usages 
where p.nom_court_polluant = 'conso' -- conso = COnsommation finale à climat réel
	--and lib_secteur_detail in ('Résidentiel','Tertiaire')
	and an = (select max(an) from total.bilan_comm_v11_diffusion)
	and format_detaille_scope_2=1
	and id_comm/1000 in (4,5,6,13,83,84) 
group by id_comm, bcvd.id_usage, usages.nom_usage, bcvd.code_cat_energie, energ.nom_court_cat_energie, id_unite
order by id_comm, usage_old, energie_old
);

--Conso elec totale, pour chaque commune
drop table if exists w_conso_elec;
create temp table w_conso_elec as (
select id_comm,  sum(val) as Consommation_elec, id_unite as id_unite_elec
from total.bilan_comm_v11_diffusion bcvd 
natural join transversal.tpk_secteur_emi_detail sect
natural join commun.tpk_polluants p
natural join total.tpk_cat_energie_color energ
where p.nom_court_polluant = 'conso' -- conso = COnsommation finale à climat réel
	--and lib_secteur_detail in ('Résidentiel','Tertiaire')
	and an = (select max(an) from total.bilan_comm_v11_diffusion)
	and format_detaille_scope_2=1
	and id_comm/1000 in (4,5,6,13,83,84) 
	and energ.nom_court_cat_energie = 'Electricité'
group by id_comm, id_unite
order by Consommation_elec desc
);

-- calcul
drop table if exists prj_res_tps_reel.w_ratios_conso;
create table prj_res_tps_reel.w_ratios_conso as(
select id_comm, id_usage, usage_old, code_cat_energie, energie_old, consommation, id_unite, consommation_elec, consommation / consommation_elec AS ratio_conso
from w_consos_usage_energie consos full join w_conso_elec using (id_comm)
order by id_comm, id_usage, code_cat_energie
); 
ALTER TABLE prj_res_tps_reel.w_ratios_conso  -- Cles etrangeères
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
COMMENT ON TABLE prj_res_tps_reel.w_ratios_conso  -- Description de la table
IS 'Ratios de consommation par commune, usage et énergie. Formule : Conso[N-2] par usage et énergie / Conso electrique tous usages confondus[N-2]';

select * from prj_res_tps_reel.w_ratios_conso;







 -- Pour les métropoles (test)

-- Conso par usage energie, pour chaque métropole
drop table if exists w_consos_usage_energie_metropole;
create temp table w_consos_usage_energie_metropole as (
select CASE 
        WHEN id_comm / 1000 = 6 THEN 6100
        WHEN id_comm / 1000 = 83 THEN 83100
        when id_comm/1000 = 13 then 13000
        ELSE NULL
    END AS id_metropole,
    id_usage, usage_old, code_cat_energie, energie_old,
    sum(consommation) as consommation, id_unite
from w_consos_usage_energie
where id_comm in (06006, 06009, 06011, 06013, 06020, 06021, 06025, 06027, 06032, 06033, 06034, 06039, 06042, 06046, 06054, 06055, 06059, 06060, 06064, 06065, 06066, 06072, 06073, 06074, 06075, 06080, 06088, 06102, 06103, 06109, 06110, 06111, 06114, 06117, 06119, 06120, 06121, 06122, 06123, 06126, 06127, 06129, 06144, 06146, 06147, 06149, 06151, 06153, 06156, 06157, 06159,
/* Aix-Marseille */ 13001, 13002, 13003, 13005, 13007, 13008, 13009, 13012, 13013, 13014, 13015, 13016, 13019, 13020, 13119, 13021, 13022, 13023, 13024, 13025, 13026, 13028, 13029, 13118, 13030, 13031, 13032, 13033, 13035, 13037, 13039, 13040, 13041, 13042, 13043, 13044, 13046, 13047, 13048, 13049, 13050, 13051, 13053, 13054, 13055, 13056, 13059, 13060, 13062, 13063, 13069, 13070, 13071, 84089, 13072, 13073, 13074, 13075, 13077, 13078, 13080, 13079, 13081, 13082, 13084, 13085, 13086, 13087, 13088, 13090, 13091, 13092, 13093, 13095, 13098, 13099, 13101, 13102, 83120, 13103, 13104, 13105, 13106, 13107, 13109, 13110, 13111, 13112, 13113, 13114, 13115, 13117,
/* Toulon metropole */ 83034, 83047, 83062, 83069, 83090, 83098, 83103, 83126, 83129, 83137, 83144, 83153) 
group by id_metropole, id_usage, usage_old, code_cat_energie, energie_old, id_unite
order by id_metropole, usage_old, energie_old
);

--Conso elec totale, pour chaque métropole
drop table if exists w_conso_elec_metropole;
create temp table w_conso_elec_metropole as (
select CASE 
        WHEN id_comm / 1000 = 6 THEN 6100
        WHEN id_comm / 1000 = 83 THEN 83100
        when id_comm/1000 = 13 then 13000
        ELSE NULL
    END AS id_metropole,  -- colonne metropole pour après
    sum(consommation_elec) as Consommation_elec, id_unite_elec
from w_conso_elec
where id_comm in (06006, 06009, 06011, 06013, 06020, 06021, 06025, 06027, 06032, 06033, 06034, 06039, 06042, 06046, 06054, 06055, 06059, 06060, 06064, 06065, 06066, 06072, 06073, 06074, 06075, 06080, 06088, 06102, 06103, 06109, 06110, 06111, 06114, 06117, 06119, 06120, 06121, 06122, 06123, 06126, 06127, 06129, 06144, 06146, 06147, 06149, 06151, 06153, 06156, 06157, 06159,
/* Aix-Marseille */ 13001, 13002, 13003, 13005, 13007, 13008, 13009, 13012, 13013, 13014, 13015, 13016, 13019, 13020, 13119, 13021, 13022, 13023, 13024, 13025, 13026, 13028, 13029, 13118, 13030, 13031, 13032, 13033, 13035, 13037, 13039, 13040, 13041, 13042, 13043, 13044, 13046, 13047, 13048, 13049, 13050, 13051, 13053, 13054, 13055, 13056, 13059, 13060, 13062, 13063, 13069, 13070, 13071, 84089, 13072, 13073, 13074, 13075, 13077, 13078, 13080, 13079, 13081, 13082, 13084, 13085, 13086, 13087, 13088, 13090, 13091, 13092, 13093, 13095, 13098, 13099, 13101, 13102, 83120, 13103, 13104, 13105, 13106, 13107, 13109, 13110, 13111, 13112, 13113, 13114, 13115, 13117,
/* Toulon metropole */ 83034, 83047, 83062, 83069, 83090, 83098, 83103, 83126, 83129, 83137, 83144, 83153) 
group by id_metropole, id_unite_elec
);

-- calcul metropole
drop table if exists prj_res_tps_reel.w_ratios_conso_metropoles;
create table prj_res_tps_reel.w_ratios_conso_metropoles as(
select id_metropole, id_usage, usage_old, code_cat_energie, energie_old, consommation, id_unite, consommation_elec, consommation / consommation_elec AS ratio_conso
from w_consos_usage_energie_metropole consos full join w_conso_elec_metropole using (id_metropole)
order by id_metropole, id_usage, code_cat_energie
);
ALTER TABLE prj_res_tps_reel.w_ratios_conso_metropoles  -- Cles etrangeères
ADD CONSTRAINT fk_id_usage
FOREIGN KEY (id_usage)
REFERENCES commun.tpk_usages(id_usage),
ADD CONSTRAINT fk_code_cat_energie
FOREIGN KEY (code_cat_energie)
REFERENCES total.tpk_cat_energie_color(code_cat_energie),
add constraint fk_id_unite
foreign key (id_unite)
references commun.tpk_unite(id_unite);
COMMENT ON TABLE prj_res_tps_reel.w_ratios_conso_metropoles  -- Description de la table
IS 'Ratios de consommation par métropole, usage et énergie. Formule : Conso[N-2] par usage et énergie / Conso electrique tous usages confondus[N-2]';

select * from prj_res_tps_reel.w_ratios_conso_metropoles;






-- METHODE 2 



-- RATIOS DE CONSO BIS utilisés pour les usages variables
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