-- Part de conso des départements. Utilisé pour l'entraînement des 6 modèles meteo_conso
-- consos dep
select *, Conso_annee / Conso_totale_annee as Part_Conso_dep from
(select id_comm/1000 as num_dep, sum(val) as Conso_Annee
from total.bilan_comm_v11_diffusion bcvd natural join commun.tpk_polluants tp natural join total.tpk_cat_energie_color tcec 
where tp.nom_court_polluant = 'conso'
	and tcec.nom_court_cat_energie = 'Electricité'
	and id_secteur_detail in (3)  -- Résidentiel
	and an between (select max(an)- 3 from total.bilan_comm_v11_diffusion) and (select max(an) from total.bilan_comm_v11_diffusion)   -- 2018-2021 inclus
	and id_comm/1000 in (4, 5, 6, 13, 83, 84)
	and format_detaille_scope_2=1
group by num_dep) as consos
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
