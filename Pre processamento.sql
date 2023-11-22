CREATE VIEW NovaVersaoEventLogPaisJanelaId AS (
SELECT
      CountryWindows AS Instancia,
      CASE WHEN  (NomeEvento LIKE 'E3%' OR NomeEvento LIKE 'H4%'OR NomeEvento LIKE 'H5%') THEN  NomeEvento
		ELSE concat(NomeEvento,'_',valor_novo) 
	  END AS NomeEvento,
      [Date],
      LAG([Date]) OVER (PARTITION BY CountryName, NomeEvento, Valor ORDER BY [Date]) AS prev_date,
	  LEAD([Date]) OVER (PARTITION BY CountryName, NomeEvento, Valor ORDER BY [Date]) AS post_date
FROM (
	SELECT *,
		CASE WHEN NOMEEVENTO IN ('grocery_and_pharmacy_percent_change_from_baseline',
								 'parks_percent_change_from_baseline',
								 'residential_percent_change_from_baseline',
								 'retail_and_recreation_percent_change_from_baseline',
								 'transit_stations_percent_change_from_baseline',
								 'workplaces_percent_change_from_baseline') THEN (CASE WHEN CAST(VALOR AS INT) > 0   AND  CAST(VALOR  AS INT)  < 33 THEN 'Baixa variação positiva'
																					   WHEN CAST(VALOR AS INT) >= 33  AND CAST(VALOR  AS INT)  < 66  THEN 'Média variação positiva'
																					   WHEN CAST(VALOR AS INT) >= 66                                 THEN 'Alta variação positiva'
																					   WHEN CAST(VALOR AS INT) < 0   AND CAST(VALOR  AS INT)  > -33 THEN 'Baixa variação negativa'
																					   WHEN CAST(VALOR AS INT) <= -33 AND CAST(VALOR  AS INT)  > -66 THEN 'Média variação negativa'
																					   WHEN CAST(VALOR AS INT) <= -66 THEN 'Alta variação negativa'
																				   END) 
			ELSE VALOR
		END AS VALOR_novo
	FROM 
		TCC.[dbo].[DatasetCompletoTransposto]
	WHERE
		1=1
		AND NOT (NOMEEVENTO IN ('grocery_and_pharmacy_percent_change_from_baseline',
								 'parks_percent_change_from_baseline',
								 'residential_percent_change_from_baseline',
								 'retail_and_recreation_percent_change_from_baseline',
								 'transit_stations_percent_change_from_baseline',
								 'workplaces_percent_change_from_baseline') AND VALOR LIKE '0')
		AND NOT (NomeEvento IN ('C1_School closing', 
							'C1_School closing_Agrup',
							'C1_School closing_Flag',
							'C2_Workplace closing',
							'C2_Workplace closing_Agrup',
							'C2_Workplace closing_flag'
							,'C3_Cancel public events'
							,'C4_Restrictions on gatherings'
							,'C4_Restrictions on gatherings_Agrup'
							,'C4_Restrictions on gatherings_Flag'
							,'C5_Close public transport'
							,'C6_Stay at home requirements'
							,'C6_Stay at home requirements_Agrup'
							,'C6_Stay at home requirements_Flag'
							,'C7_Restrictions on internal movement'
							,'C8_International travel controls'
							,'E1_Income support'
							,'H6_Facial Coverings_Flag'
							,'H6_Facial Coverings_Agrup'
						    ,'H6_Facial Coverings'
							,'H7_Vaccination policy'
							,'H8_Protection of elderly people'
							,'H8_Protection of elderly people_Agrup'
							,'H8_Protection of elderly people_Flag'

							))
		--Removendo países com base na literatura que diz ma qualidade dos dados
		AND	CountryName NOT IN ('VENEZUELA', 'CUBA')
		--Removento o que é evento de abrangencia regional
		AND Valor NOT IN  ('0_NA','0_0','1_0','2_0','3_0','4_0')
		--Removendo os evento que acontecem depois de 15/10/2022 (LIMITE GOOGLE MOBILITY)
		AND [DATE]<'2022-10-15 00:00:000'
		--Tirando classificações 0
	    AND NOT( (NomeEvento='C1_School closing_Agrup_Flag' AND Valor='0_1')
			 OR (NomeEvento='C2_Workplace closing_Agrup_Flag' AND Valor='0_1')
			 OR (NomeEvento='C3_Cancel public events_Flag' AND Valor='0_1')
			 OR (NomeEvento='C5_Close public transport' AND Valor='0')
			 OR (NomeEvento='C6_Stay at home requirements_Agrup_Flag' AND Valor='0_1')
			 OR (NomeEvento='C7_Restrictions on internal movement_Flag' AND Valor='0_1')
			 OR (NomeEvento='E3_Fiscal measures' AND Valor='0')
			 OR (NomeEvento='E4_International support' AND Valor='0')
			 OR (NomeEvento='H4_Emergency investment in healthcare' AND Valor='0')
			 OR (NomeEvento='H5_Investment in vaccines' AND Valor='0')
			 OR (NomeEvento='H7_Vaccination policy_Flag' AND Valor='0_1')
			 OR (NomeEvento='H8_Protection of elderly people_Agrup_Flag' AND Valor='0_1')
			 OR (NomeEvento='M1_Wildcard' AND Valor='0')
			 OR (NomeEvento='V1_Vaccine Prioritisation (summary)' AND Valor='0')
			 OR (NomeEvento='V3_Vaccine Financial Support (summary)' AND Valor='0')
		)
		--Tirando os registros de eventos de vacinação que ocorreram antes da vacinação
		AND NOT (([Pré ou pós vacinação]='Antes da Vacinação') AND (NomeEvento IN ( 'H7_Vaccination policy'
																			   ,'H7_Vaccination policy_Flag'
																			   ,'Primieira Vacina aplicada'
																			   ,'V1_Vaccine Prioritisation (summary)'
																			   ,'V2A_Vaccine Availability (summary)'
																			   ,'V2B_Vaccine age eligibility/availability age floor (general popu'
																			   ,'V2C_Vaccine age eligibility/availability age floor (at risk summ'
																			   ,'V2D_Medically/ clinically vulnerable (Non-elderly)'
																			   ,'V2E_Education'
																			   ,'V2F_Frontline workers  (non healthcare)'
																			   ,'V2G_Frontline workers  (healthcare)'
																			   ,'V3_Vaccine Financial Support (summary)'
																			   ,'V4_Mandatory Vaccination (summary)')))
		--Tirando o que aconteceu antes do primeiro caso dos países
		AND IDX NOT IN (SELECT DISTINCT IDX FROM TCC.[dbo].[DatasetCompletoTransposto] WHERE (NomeEvento='Registro primeiro caso') AND (VALOR LIKE 'NA')))p)


		SELECT * FROM TCC.[dbo].[DatasetCompletoTransposto] WHERE CountryName='BRAZIL' AND NomeEvento='C1_School closing_Agrup_Flag'