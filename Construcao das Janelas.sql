CREATE VIEW TratadaNovaVersaoEnventLogPaisJanelaIdStartEndDate AS (

SELECT
    Instancia,
    NomeEvento,
    DATEADD(DAY,-1,[START_DATE]) AS [START_DATE],
    END_DATE
FROM (
		SELECT
			Instancia,
			NomeEvento,
			MIN(DATE) AS [START_DATE],
			MAX(DATE) AS END_DATE
		FROM (
				SELECT
					Instancia,
					NomeEvento,
					Date,
					new_group as Flag,
					ROW_NUMBER() OVER (PARTITION BY Instancia, NomeEvento ORDER BY DATE) - 
					ROW_NUMBER() OVER (PARTITION BY Instancia, NomeEvento, new_group ORDER BY DATE) AS grp
				FROM 
					(
						SELECT
							Instancia,
							NomeEvento,
							[Date],
							prev_date,
							CASE
								WHEN ((prev_date IS NULL) OR (NomeEvento <> LAG(NomeEvento) OVER (PARTITION BY Instancia, NomeEvento ORDER BY [Date])) OR ((DATEDIFF(day, prev_date, [Date])> 1))) THEN 1
								ELSE 0
							END AS new_group
						FROM
							tcc.[dbo].TratadaNovaVersaoEventLogPaisJanelaId
						) AS GroupedData
				)L
		--WHERE 
			--FLAG = 1
		GROUP BY 
			Instancia, NomeEvento, grp
		)m
WHERE 
	(([START_DATE]<>END_DATE))
--ORDER BY Instancia, NomeEvento, [START_DATE]
)
