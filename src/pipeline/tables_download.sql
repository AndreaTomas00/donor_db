-- Create the table with all columns from the CSV
CREATE TABLE DONANT (
    id_donant CHAR(15) PRIMARY KEY,
    data_donacio DATE,
    codi_centre VARCHAR(50),
    tipus_donant VARCHAR(20) CHECK (tipus_donant IN ('MORT ENCEFÀLICA', 'MORT ASISTÒLIA')),
    grup_sanguini VARCHAR(5) CHECK (grup_sanguini IN ('A', 'B', 'AB', 'O')),
    rh VARCHAR(15),
    edat SMALLINT,
    talla SMALLINT,
    pes SMALLINT,
    sexe CHAR(4) CHECK (sexe in ('Home', 'Dona')),
    cip CHAR(14),
    document_identificatiu VARCHAR(20),
    pais_origen VARCHAR(50),
    ccaa_residencia VARCHAR(50),
    organització_procedencia VARCHAR(20),
    pais_procedencia VARCHAR(50),
    provincia_procedencia VARCHAR(50),
    hospital_donant TEXT,
    resultat_organs TEXT,
    data_hora_ingres_hospital TIMESTAMP,
    unitat_procedencia VARCHAR(50),
    motiu_ingres VARCHAR(20),
    motiu_ingres_especificacio TEXT,
    ingres_CIOD CHAR(2) CHECK (ingres_CIOD IN ('SI', 'NO')) ,
    iot_CIOD CHAR(2) CHECK (iot_CIOD IN ('SI', 'NO')),
    servei_procedencia VARCHAR(50),
    hospital_procedencia VARCHAR(50),
    servei_estada VARCHAR(50),
    tipus_uci VARCHAR(50),
    uci_especificacio VARCHAR(50),
    data_hora_ingres_uci TIMESTAMP,
    iot CHAR(2) CHECK (iot IN ('SI', 'NO')),
    data_hora_iot TIMESTAMP,
    lloc_iot VARCHAR(50),
    lesió_cerebral_catastrofica CHAR(2) CHECK (lesió_cerebral_catastrofica IN ('SI', 'NO')),
    data_hora_lesio_cerebral TIMESTAMP,
    patologia_cim_10 VARCHAR(100),
    donacio_post_pram CHAR(2) CHECK (donacio_post_pram IN ('SI', 'NO')),
    data_hora_exitus TIMESTAMP,
    causa_primaria_exitus TEXT,
    tabaquisme CHAR(2) CHECK (tabaquisme IN ('SI', 'NO')),
    any_inici_tabaquisme VARCHAR(10),
    estat_habit_tabaquic VARCHAR(50),
    severitat_habit_tabaquic VARCHAR(50),
    quantitat_cigarretes_dia SMALLINT,
    enolisme CHAR(2) CHECK (enolisme IN ('SI', 'NO')),
    any_inici_enolisme VARCHAR(10),
    grau_enolisme VARCHAR(50),
    quantitat_enol_g_dia SMALLINT,
    drogues_abus CHAR(2) CHECK (drogues_abus IN ('SI', 'NO')),
    drogues_abus_especificacio VARCHAR(100),
    intoxicacio_aguda CHAR(2) CHECK (intoxicacio_aguda IN ('SI', 'NO')),
    toxics_especificacio VARCHAR(100),
    hipertensio_arterial CHAR(2) CHECK (hipertensio_arterial IN ('SI', 'NO')),
    any_inici_hta VARCHAR(10),
    tractament_hta VARCHAR(100),
    diabetes_mellitus CHAR(2) CHECK (diabetes_mellitus IN ('SI', 'NO')),
    tipus_diabetes VARCHAR(50),
    any_inici_dm VARCHAR(10),
    antecedents_familiars_dm CHAR(2) CHECK (antecedents_familiars_dm IN ('SI', 'NO')),
    tractament_dm VARCHAR(100),
    dislipemia CHAR(2) CHECK (dislipemia IN ('SI', 'NO')),
    any_inici_dislipemia VARCHAR(10),
    tractament_dislipemia VARCHAR(100),
    cirurgies_previes CHAR(2) CHECK (cirurgies_previes IN ('SI', 'NO')),
    cirurgies_previes_especificacio TEXT,
    h_nefropatia_urologia CHAR(2) CHECK (h_nefropatia_urologia IN ('SI', 'NO')),
    h_cardiopatia CHAR(2) CHECK (h_cardiopatia IN ('SI', 'NO')),
    h_patologia_digestiva CHAR(2) CHECK (h_patologia_digestiva IN ('SI', 'NO')),
    h_patologia_respiratoria CHAR(2) CHECK (h_patologia_respiratoria IN ('SI', 'NO')),
    h_patologia_neurologica CHAR(2) CHECK (h_patologia_neurologica IN ('SI', 'NO')),
    h_ginecologia_obstetricia CHAR(2) CHECK (h_ginecologia_obstetricia IN ('SI', 'NO')),
    h_malaltia_autoinmune CHAR(2) CHECK (h_malaltia_autoinmune IN ('SI', 'NO')),
    h_malaltia_hematologica CHAR(2) CHECK (h_malaltia_hematologica IN ('SI', 'NO')),
    h_neoplasia CHAR(2) CHECK (h_neoplasia IN ('SI', 'NO')),
    h_infeccio_cronica CHAR(2) CHECK (h_infeccio_cronica IN ('SI', 'NO')),
    altres_patologies TEXT,
    tractaments_cronics TEXT,
    observacions_antecedents TEXT,
    tipus_asistolia VARCHAR(50),
    causa_mort_dac TEXT,
    lloc_acr VARCHAR(50),
    lloc_extubacio VARCHAR(50),
    canulacio VARCHAR(10) CHECK (canulacio IN ('Premortem', 'Postmortem')),
    dac_data_hora_inici_canulacio TIMESTAMP,
    dac_data_hora_inici_ltsv TIMESTAMP,
    dac_data_hora_hipoperfusio TIMESTAMP,
    dac_data_hora_acr TIMESTAMP,
    dac_data_hora_mort TIMESTAMP,
    dac_data_hora_preservacio TIMESTAMP,
    dac_data_hora_perfusio TIMESTAMP,
    temps_isquemia_calenta_total SMALLINT,
    temps_isquemia_calenta_funcional SMALLINT,
    danc_data_hora_acr TIMESTAMP,
    danc_data_hora_inici_svb TIMESTAMP,
    danc_data_hora_inici_svad TIMESTAMP,
    danc_cardiocompresio_extrahospitalaria CHAR(2) CHECK (danc_cardiocompresio_extrahospitalaria IN ('SI', 'NO')),
    danc_data_hora_activacio_codi_303 TIMESTAMP,
    danc_data_hora_ingres_hospital TIMESTAMP,
    danc_servei_estada VARCHAR(50),
    danc_cardiocompresio_intrahospitalaria CHAR(2) CHECK (danc_cardiocompresio_intrahospitalaria IN ('SI', 'NO')),
    danc_especifiqueu_cardiocompresion VARCHAR(100),
    danc_data_hora_diagnostic_mort TIMESTAMP,
    danc_data_hora_heparinitzacio TIMESTAMP,
    danc_data_hora_mostres_sang TIMESTAMP,
    danc_data_hora_inicio_canulacion TIMESTAMP,
    danc_data_hora_inicio_preservacio_abdominal TIMESTAMP,
    danc_data_hora_clampatge TIMESTAMP,
    danc_temps_isquemia_calenta_absolut SMALLINT,
    danc_temps_isquemia_calenta_total SMALLINT,
    danc_temps_preparacio_canulacio SMALLINT,
    danc_temps_total_preservacio SMALLINT,
    preservacio_abdominal CHAR(2) CHECK (preservacio_abdominal IN ('SI', 'NO')),
    preservacio_renal CHAR(2) CHECK (preservacio_renal IN ('SI', 'NO')),
    preservacio_hepatica CHAR(2) CHECK (preservacio_hepatica IN ('SI', 'NO')),
    preservacio_pancreatica CHAR(2) CHECK (preservacio_pancreatica IN ('SI', 'NO')),
    tipus_preservacio_extraccio_abdominal VARCHAR(100),
    preservacio_toracica CHAR(2) CHECK (preservacio_toracica IN ('SI', 'NO')),
    preservacio_pulmonar CHAR(2) CHECK (preservacio_pulmonar IN ('SI', 'NO')),
    preservacio_cardiaca CHAR(2) CHECK (preservacio_cardiaca IN ('SI', 'NO')),
    incisio_pell_abdominal TIMESTAMP,
    extraccio_primer_organ_abdominal TIMESTAMP,
    causa_me VARCHAR(100),
    data_hora_dx_mort_me TIMESTAMP,
    dx_me VARCHAR(100),
    pacient_rep_transfusions CHAR(2) CHECK (pacient_rep_transfusions IN ('SI', 'NO')),
    data_hora_extraccio_sang TIMESTAMP,
    concentrats_hematies_48h SMALLINT,
    sang_total_48h SMALLINT,
    sang_reconstituida SMALLINT,
    total_a SMALLINT,
    dextra SMALLINT,
    plasma SMALLINT,
    plaquetes SMALLINT,
    albumina SMALLINT,
    altres_coloides SMALLINT,
    total_b SMALLINT,
    serum_sali SMALLINT,
    dextrosa SMALLINT,
    ringer SMALLINT,
    altres_cristaloides SMALLINT,
    total_c SMALLINT,
    hemodilucio CHAR(2) CHECK (hemodilucio IN ('SI', 'NO')),
    tensio_arterial_sistolica SMALLINT,
    tensio_arterial_diastolica SMALLINT,
    presio_venosa_central SMALLINT,
    temperatura DECIMAL(4,1),
    frequencia_cardiaca SMALLINT,
    hipotensio CHAR(2) CHECK (hipotensio IN ('SI', 'NO')),
    temps_hipotensio SMALLINT,
    tensio_minima SMALLINT,
    hipertensio CHAR(2) CHECK (hipertensio IN ('SI', 'NO')),
    temps_hipertensio SMALLINT,
    tensio_maxima SMALLINT,
    aturada_cardio_respiratoria CHAR(2) CHECK (aturada_cardio_respiratoria IN ('SI', 'NO')),
    temps_acr SMALLINT,
    temps_acr_sense_rcp SMALLINT,
    maniobres_rcp VARCHAR(50),
    temps_rcp_basica SMALLINT,
    temps_rcp_avançada SMALLINT,
    dopamina DECIMAL(4,2),
    temps_dopamina DECIMAL(4,2),
    noradrenalina DECIMAL(4,2),
    temps_noradrenalina SMALLINT,
    dobutamina DECIMAL(4,2),
    temps_dobutamina SMALLINT,
    adrenalina DECIMAL(4,2),
    temps_adrenalina SMALLINT,
    otras_drogas DECIMAL(4,2),
    diuresi_24h SMALLINT,
    diuresi_mmitjana_hora SMALLINT,
    diabetis_insipida CHAR(2) CHECK (diabetis_insipida IN ('SI', 'NO')),
    tractament_diabetis_insipida VARCHAR(100),
    antidiuretics CHAR(2) CHECK (antidiuretics IN ('SI', 'NO')),
    diuretics CHAR(2) CHECK (diuretics IN ('SI', 'NO')),
    insulina CHAR(2) CHECK (insulina IN ('SI', 'NO')),
    unitats_insulina SMALLINT,
    detalls_insulina VARCHAR(100),
    descripcio_drogas TEXT,
    hla_a_1 VARCHAR(20),
    hla_a_2 VARCHAR(20),
    hla_b_1 VARCHAR(20),
    hla_b_2 VARCHAR(20),
    hla_c_1 VARCHAR(20),
    hla_c_2 VARCHAR(20),
    hla_drb1_1 VARCHAR(20),
    hla_drb1_2 VARCHAR(20),
    hla_drb345_1 VARCHAR(20),
    hla_drb345_2 VARCHAR(20),
    hla_dqb1_1 VARCHAR(20),
    hla_dqb1_2 VARCHAR(20),
    hla_dqa1_1 VARCHAR(20),
    hla_dqa1_2 VARCHAR(20),
    hla_dpb1_1 VARCHAR(20),
    hla_dpb1_2 VARCHAR(20),
    hla_dpa1_1 VARCHAR(20),
    hla_dpa1_2 VARCHAR(20),
    altres_alels TEXT,
    observacions_hla TEXT,
    hiv VARCHAR(15) CHECK (hiv IN ('Positiva', 'Negativa', 'Pendent', 'No determinada')),
    hiv_p24 VARCHAR(15) CHECK (hiv_p24 IN ('Positiva', 'Negativa', 'Pendent', 'No determinada')),
    hiv_pcr VARCHAR(15) CHECK (hiv_pcr IN ('Positiva', 'Negativa', 'Pendent', 'No determinada')),
    ag_vhbs VARCHAR(15) CHECK (ag_vhbs IN ('Positiva', 'Negativa', 'Pendent', 'No determinada')),
    ab_vhbc VARCHAR(15) CHECK (ab_vhbc IN ('Positiva', 'Negativa', 'Pendent', 'No determinada')),
    ab_vhbs VARCHAR(15) CHECK (ab_vhbs IN ('Positiva', 'Negativa', 'Pendent', 'No determinada')),
    hbs_ab_quant VARCHAR(15),
    vhb_pcr VARCHAR(15) CHECK (vhb_pcr IN ('Positiva', 'Negativa', 'Pendent', 'No determinada')),
    virus_delta VARCHAR(15) CHECK (virus_delta IN ('Positiva', 'Negativa', 'Pendent', 'No determinada')),
    ab_vhc VARCHAR(15) CHECK (ab_vhc IN ('Positiva', 'Negativa', 'Pendent', 'No determinada')),
    vhc_pcr VARCHAR(15) CHECK (vhc_pcr IN ('Positiva', 'Negativa', 'Pendent', 'No determinada')),
    cmv_igg VARCHAR(15) CHECK (cmv_igg IN ('Positiva', 'Negativa', 'Pendent', 'No determinada')),
    cmv_igm VARCHAR(15) CHECK (cmv_igm IN ('Positiva', 'Negativa', 'Pendent', 'No determinada')),
    cmv_igg_quant VARCHAR(15),
    sifilis_rpr VARCHAR(15) CHECK (sifilis_rpr IN ('Positiva', 'Negativa', 'Pendent', 'No determinada')),
    sifilis_hatp VARCHAR(15) CHECK (sifilis_hatp IN ('Positiva', 'Negativa', 'Pendent', 'No determinada')),
    toxoplasmosis_igg VARCHAR(15) CHECK (toxoplasmosis_igg IN ('Positiva', 'Negativa', 'Pendent', 'No determinada')),
    toxoplasmosis_igg_quant VARCHAR(15),
    toxoplasmosis_igm VARCHAR(15) CHECK (toxoplasmosis_igm IN ('Positiva', 'Negativa', 'Pendent', 'No determinada')),
    ebv VARCHAR(15) CHECK (ebv IN ('Positiva', 'Negativa', 'Pendent', 'No determinada')),
    htlv_ab VARCHAR(15) CHECK (htlv_ab IN ('Positiva', 'Negativa', 'Pendent', 'No determinada')),
    vhs VARCHAR(15) CHECK (vhs IN ('Positiva', 'Negativa', 'Pendent', 'No determinada')),
    pais_naixement_donant VARCHAR(50),
    zones_viatges VARCHAR(100),
    tripanosoma_cruzi VARCHAR(15) CHECK (tripanosoma_cruzi IN ('Positiva', 'Negativa', 'Pendent', 'No determinada')),
    covid_19 VARCHAR(15) CHECK (covid_19 IN ('Positiva', 'Negativa', 'Pendent', 'No determinada')),
    estrongiloides VARCHAR(15) CHECK (estrongiloides IN ('Positiva', 'Negativa', 'Pendent', 'No determinada')),
    malaria VARCHAR(15) CHECK (malaria IN ('Positiva', 'Negativa', 'Pendent', 'No determinada')),
    esquistosoma VARCHAR(15) CHECK (esquistosoma IN ('Positiva', 'Negativa', 'Pendent', 'No determinada')),
    altres_serologies VARCHAR(100),
    observacions_serologies TEXT,
    viatges_fora_europa CHAR(2) CHECK (viatges_fora_europa IN ('SI', 'NO')),
    febre_durant_ingres CHAR(2) CHECK (febre_durant_ingres IN ('Sí', 'No')),
    temperatura_maxima DECIMAL(4,1),
    durada_febre VARCHAR(20),
    secrecions_purulentes CHAR(2) CHECK (secrecions_purulentes IN ('SI', 'NO')),
    observacions_secrecions TEXT,
    sepsia CHAR(2) CHECK (sepsia IN ('SI', 'NO')),
    antibiotics CHAR(2) CHECK (antibiotics IN ('SI', 'NO')),
    antibiotics_descripcion TEXT
);

CREATE TABLE ANALITICA (
    id_analitica SERIAL PRIMARY KEY,
    data_hora_analitica TIMESTAMP NOT NULL,
    hemoglobina DECIMAL(5,2),
    hematocrit DECIMAL(5,2),
    vcm DECIMAL(5,2),
    leucocits DECIMAL(6,2),
    neutrofils DECIMAL(5,2),
    limfocits DECIMAL(5,2),
    monocits DECIMAL(5,2),
    basofils DECIMAL(5,2),
    eosinofils DECIMAL(5,2),
    plaquetes DECIMAL(6,2),
    temps_protombina DECIMAL(5,2),
    temps_protombina_inr_ratio DECIMAL(5,2),
    temps_protombina_s DECIMAL(5,2),
    temps_quick_ratio DECIMAL(5,2),
    attp_ratio DECIMAL(5,2),
    attp_s DECIMAL(5,2),
    fibrinogen DECIMAL(5,2),
    dimer_d DECIMAL(6,2),
    glucosa DECIMAL(6,2),
    urea DECIMAL(6,2),
    creatinina DECIMAL(6,2),
    bilirrubina_total DECIMAL(6,2),
    bilirrubina_directa DECIMAL(6,2),
    na DECIMAL(5,2),
    k DECIMAL(5,2),
    cl DECIMAL(5,2),
    p DECIMAL(5,2),
    ca DECIMAL(5,2),
    colesterol DECIMAL(6,2),
    triglicerids DECIMAL(6,2),
    proteina DECIMAL(6,2),
    albumina DECIMAL(6,2),
    got DECIMAL(6,2),
    gpt DECIMAL(6,2),
    ggt DECIMAL(6,2),
    fosfatasa_alcalina DECIMAL(6,2),
    ldh DECIMAL(6,2),
    troponina DECIMAL(6,2),
    cpk DECIMAL(6,2),
    cpk_mb DECIMAL(6,2),
    amilasa DECIMAL(6,2),
    lipasa DECIMAL(6,2),
    beta_hcg DECIMAL(6,2),
    psa DECIMAL(6,2),
    cea DECIMAL(6,2),
    alfa_fp DECIMAL(6,2),
    id_donant CHAR(15),
    FOREIGN KEY (id_donant) REFERENCES DONANT(id_donant)
);


CREATE TABLE ORINA (
    id_orina VARCHAR(15) PRIMARY KEY,
    data_hora_orina TIMESTAMP,
    proteinuria_resultat_1 VARCHAR(8) CHECK (proteinuria_resultat_1 IN ('Positiu', 'Negatiu', NULL)),
    proteinuria_resultat_2 VARCHAR(12) CHECK (proteinuria_resultat_2 IN ('No realitzat', 'Normal', 'Patologic', 'No informat', NULL)),
    proteinuria_tira VARCHAR(5),
    proteinuria_mg_dl DECIMAL(6,2),
    proteinuria_g_dia DECIMAL(6,2),
    hematuria_resultat_1 VARCHAR(8) CHECK (hematuria_resultat_1 IN ('Positiu', 'Negatiu', NULL)),
    hematuria_resultat_2 VARCHAR(12) CHECK (hematuria_resultat_2 IN ('No realitzat', 'Normal', 'Patologic', 'No informat', NULL)),
    hematuria_tira VARCHAR(5),
    hematuria_mg_dl DECIMAL(6,2),
    sediment_resultat_1 VARCHAR(8) CHECK (sediment_resultat_1 IN ('Positiu', 'Negatiu', NULL)),
    sediment_resultat_2 VARCHAR(12) CHECK (sediment_resultat_2 IN ('No realitzat', 'Normal', 'Patologic', 'No informat', NULL)),
    sediment_hematies_camp VARCHAR(10),
    sediment_leucocits_camp VARCHAR(10),
    altres_sediments VARCHAR(100),
    clearance_creatinina_ml_min DECIMAL(6,2),
    id_donant CHAR(15),
    FOREIGN KEY (id_donant) REFERENCES DONANT(id_donant)
);
CREATE TABLE MICRO (
    id_micro VARCHAR(15),
    tipus_cultiu VARCHAR(20) CHECK (tipus_cultiu IN ('Urocultiu', 'Hemocultiu', 'Aspirat traqueal', 'Altres cultius', NULL)),
    data_hora_cultiu TIMESTAMP,
    resultat_cultius VARCHAR(15) CHECK (resultat_cultius IN ('Pendent', 'Positiu', 'Negatiu', 'No realitzat', NULL)),
    resultats_cultiu_descripció VARCHAR(1000),
    germens VARCHAR(300),
    tincio_gram VARCHAR(300),
    id_donant CHAR(15),
    primary key (id_micro, tipus_cultiu),
    FOREIGN KEY (id_donant) REFERENCES DONANT(id_donant)
);

CREATE TABLE GSA (
    id_gsa VARCHAR(15) PRIMARY KEY,
    data_hora_gsa TIMESTAMP,
    fio2 SMALLINT,
    peep SMALLINT,
    ph DECIMAL(4,2),
    po2 SMALLINT,
    pco2 SMALLINT,
    hco3 DECIMAL(4,2),
    eb DECIMAL(4,2),
    saturacio_o2 DECIMAL(4,1),
    id_donant CHAR(15),
    FOREIGN KEY (id_donant) REFERENCES DONANT(id_donant)
);


CREATE TABLE IMATGE (
    id_donant CHAR(15),
    tipus_prova VARCHAR(50),
    estat_prova VARCHAR(50),
    resultat VARCHAR(1000),
    PRIMARY KEY (id_donant, tipus_prova),
    FOREIGN KEY (id_donant) REFERENCES DONANT(id_donant)
);

-- CREATE TABLE RECEPTOR (
--     id_receptor SERIAL PRIMARY KEY,
--     receptor VARCHAR(50) UNIQUE,
--     data_naixement DATE,
--     edat INT,
--     sexe VARCHAR(1) CHECK (sexe IN ('M', 'F')),
--     grup_sanguini VARCHAR(5) CHECK (grup_sanguini IN ('A', 'B', 'AB', '0')),
--     rh CHAR(2) CHECK (rh IN ('+', '-')),
--     cip VARCHAR(50) UNIQUE,
--     document_identificatiu VARCHAR(50) UNIQUE,
--     patologia VARCHAR(50),
--     trasplantat CHAR(2) CHECK (trasplantat IN ('SI', 'NO'))
-- );

CREATE TABLE OFERTA (
    id_oferta SERIAL PRIMARY KEY,
    Tipus_oferta VARCHAR(50) ,
    organ_1 VARCHAR(100),
    organ_2 VARCHAR(100),
    urgencia_oferta VARCHAR(100),
    organització_trasplantadora VARCHAR(100),
    hospital_trasplantador VARCHAR(100),
    acceptació CHAR(2) CHECK (acceptació IN ('SI', 'NO')),
    causa_no_acceptació VARCHAR(100),
    causa_no_acceptació_observacions VARCHAR(200),
    equip_extractor VARCHAR(100),
    extracció CHAR(2) CHECK (extracció IN ('SI', 'NO')),
    causa_no_extracció VARCHAR(100),
    trasplantament CHAR(2) CHECK (trasplantament IN ('SI', 'NO')),
    causa_no_trasplantament VARCHAR(100),
    receptor VARCHAR(100),
    observacions_receptor VARCHAR(200),
    split CHAR(2) CHECK (split IN ('SI', 'NO')),
    hospital_trasplantament_split VARCHAR(100),
    id_donant CHAR(15) NOT NULL,
    FOREIGN KEY (id_donant) REFERENCES DONANT(id_donant)
    -- FOREIGN KEY (receptor) REFERENCES RECEPTOR(receptor)
);

