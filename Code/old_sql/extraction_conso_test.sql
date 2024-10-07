-- base inv

-- MAJ via Python ; from_api_to_emi_server.py

-- TABLE PRINCIPALE PAR QUART D'HEURE  ; MAJ via Python
select * from prj_res_tps_reel.src_conso_elec_4_test ;        --  order by date_heure desc, libelle_metropole;

-- JOUR
/*
drop table if exists prj_res_tps_reel.src_conso_elec_jour_test;
CREATE TABLE IF NOT EXISTS prj_res_tps_reel.src_conso_elec_jour_test (
    date DATE,
    libelle_metropole VARCHAR(255),
    id_metropole INT,
    Consommation_mwh INT,
    missing_percentage FLOAT,
    PRIMARY KEY (date, libelle_metropole)
);*/
INSERT INTO prj_res_tps_reel.src_conso_elec_jour_test (date, libelle_metropole, id_metropole, Consommation_mwh, missing_percentage)
SELECT 
    date, 
    libelle_metropole, 
    CASE 
	    WHEN libelle_metropole like 'Métropole Nice%' THEN 6100
	    WHEN libelle_metropole like 'Métropole Toulon%' THEN 83100
    	ELSE NULL
    END AS id_metropole,  -- colonne metropole pour après
    SUM(consommation_mwh) AS Consommation_mwh, 
    ROUND(100 - COUNT(CASE WHEN consommation_mwh IS NOT NULL AND consommation_mwh > 0 THEN consommation_mwh END) / 0.96, 2) AS missing_percentage
FROM prj_res_tps_reel.src_conso_elec_4_test 
GROUP BY date, libelle_metropole
ORDER BY date DESC, libelle_metropole
ON CONFLICT (date, libelle_metropole) DO UPDATE SET 
    Consommation_mwh = EXCLUDED.Consommation_mwh,
    missing_percentage = EXCLUDED.missing_percentage;
select * from prj_res_tps_reel.src_conso_elec_jour_test;












-- HISTORIQUE PROFOND JOURS
select * from prj_res_tps_reel.conso_elec_jour_test_v0 ;
select * from prj_res_tps_reel.conso_elec_jour_test_hist order by date desc, libelle_metropole ;
-- HEURES
select * from prj_res_tps_reel.conso_elec_heure_test_v0 ;
select * from prj_res_tps_reel.conso_elec_heure_test_hist order by date_h desc, libelle_metropole ;


--drop table prj_res_tps_reel.conso_elec_4_test ;