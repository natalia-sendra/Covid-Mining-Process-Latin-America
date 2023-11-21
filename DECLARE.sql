
----------------------------------------------------------
------------------event log-------------------------------
----------------------------------------------------------
IF OBJECT_ID ('TEMPDB..#EVENTLOG') IS NOT NULL DROP TABLE #EVENTLOG 
SELECT 
      Instancia AS INSTANCE,
      NomeEvento AS TASK,
      [Start_date] AS [TIME],
	  [end_date] AS [TIME_end]
INTO #EVENTLOG
FROM 
	tcc.dbo.[TratadaNovaVersaoEnventLogPaisJanelaIdStartEndDate]
--Filtros
WHERE 
	RIGHT(Instancia,2) not IN ('t2','T4','T6','T8')
	AND SUBSTRING(instancia,1,len(instancia)-2) IN ('CostaRica')



----------------------------------------------------------
------------------Pares de evento-------------------------
----------------------------------------------------------
IF OBJECT_ID ('TEMPDB..#TUPLAS') IS NOT NULL DROP TABLE #TUPLAS 
SELECT a.Task AS TaskA , b.Task AS TaskB 
INTO #TUPLAS
FROM #EventLog a , #EventLog b 
WHERE a.Task != b.Task 
GROUP BY a.Task , b.Task 


SELECT * FROM  (
----------------------------------------------------------
-------------------------Regra 1--------------------------
----------------------------------------------------------
SELECT regra='response' , 
	   TaskA , 
	   TaskB,
       (CAST(COUNT( *) AS FLOAT) / CAST( (SELECT count(*)
										  FROM #EventLog 
										  WHERE trim(Task) = TaskA) AS FLOAT) ) AS Support, 
	   ((CAST(COUNT( *) AS FLOAT) /CAST( (SELECT COUNT( *) 
										  FROM #EventLog 
										  WHERE trim(Task) = TaskA) AS FLOAT) ) * (CAST( (SELECT COUNT( *) 
																						  FROM (SELECT Instance 
																								FROM #EventLog 
																								WHERE trim(Task) = TaskA
																								GROUP BY Instance ) t2 ) AS FLOAT) /CAST( (SELECT COUNT( *) 
																																			FROM (SELECT Instance 
																																				  FROM #EventLog 
																																				  GROUP BY Instance ) t ) AS FLOAT) ) ) AS Confidence
FROM #EventLog a , #tuplas x
WHERE a.Task = x.TaskA AND EXISTS (SELECT * 
								   FROM #EventLog b 
								   WHERE b.Task = x.TaskB AND b.Instance = a.Instance AND b.Time  > a.Time  )
GROUP BY x.TaskA , x.TaskB
--HAVING (CAST(COUNT(*) AS FLOAT) /CAST( (SELECT COUNT( *) 
--										FROM #EventLog
--										WHERE trim(Task) = TaskA) AS FLOAT) ) > 0.7
--	  AND ( (CAST(COUNT( *) AS FLOAT) /CAST( (SELECT COUNT( *) 
--											  FROM #EventLog
--											  WHERE trim(Task) = TaskA) AS FLOAT) ) * (CAST( (SELECT COUNT( *) 
--																							  FROM(SELECT Instance 
--																								   FROM #EventLog 
--																								   WHERE trim(Task) = TaskA
--																								   GROUP BY Instance ) t2 ) AS FLOAT) / CAST( (SELECT COUNT( *)
--																																			   FROM(SELECT Instance 
--																																					FROM #EventLog 
--																																					GROUP BY Instance ) t ) AS FLOAT) ) ) > 0.5
--																																					
--	
union
----------------------------------------------------------
-------------------------Regra 2--------------------------
----------------------------------------------------------								
SELECT regra='Alternative Response' , 
		TaskA , 
		TaskB ,
		(CAST(COUNT( *) AS FLOAT) /CAST( (SELECT COUNT( *) 
										  FROM #EventLog 
										  WHERE trim(Task) = TaskA) AS FLOAT) ) AS Support ,
	    ( (CAST(COUNT( *) AS FLOAT) /CAST( (SELECT COUNT( *) 
											FROM #EventLog 
											WHERE trim(Task) = TaskA) AS FLOAT) ) * (CAST( (SELECT COUNT( *) 
																							FROM (SELECT Instance 
																								  FROM #EventLog 
																								  WHERE trim(Task) = TaskA
																								  GROUP BY Instance ) t2 ) AS FLOAT) /CAST( (SELECT COUNT( *) 
																																			 FROM (SELECT Instance 
																																				  FROM #EventLog 
																																				  GROUP BY Instance ) t ) AS FLOAT) ) ) AS Confidence
FROM #EventLog a , #tuplas x
WHERE a.Task = x.TaskA AND EXISTS (SELECT * 
								   FROM #EventLog b 
								   WHERE b.Task = x.TaskB 
										 AND b.Instance = a.Instance 
										 AND b.Time > a. Time )
AND NOT EXISTS(SELECT * 
			   FROM #EventLog B, #EventLog c 
			   WHERE c .Instance = a.Instance 
					AND c.Task = x.TaskA 
					AND b.Instance = a.Instance 
					AND b.Task = x.TaskB 
					AND c.Time > a.Time 
					AND c.Time < b.Time )
GROUP BY x.TaskA , x.TaskB
--HAVING (CAST(COUNT( *) AS FLOAT) /CAST( (SELECT COUNT( *) 
--										 FROM #EventLog
--										 WHERE trim(Task) =  TaskA) AS FLOAT) ) > 0.7 AND ( (CAST(COUNT( *) AS FLOAT) /CAST( (SELECT COUNT( *) 
--																															FROM #EventLog
--																															WHERE trim(Task) =  TaskA) AS FLOAT) ) * (CAST( (SELECT COUNT( *) 
--																																										 FROM(SELECT Instance 
--																																											  FROM #EventLog 
--																																											  WHERE trim(Task) =  TaskA 
--																																											  GROUP BY Instance ) t2 ) AS FLOAT) / CAST( (SELECT COUNT( *)
--																																																						  FROM(SELECT Instance 
--																																																							   FROM #EventLog 
--																																																							   GROUP BY Instance ) t ) AS FLOAT) ) ) > 0.5								
--
--
union
----------------------------------------------------------
-------------------------Regra 3--------------------------
----------------------------------------------------------
SELECT regra='ChainResponse' , 
		TaskA , 
		TaskB ,
		(CAST(COUNT( *) AS FLOAT) /CAST( (SELECT COUNT( *) 
										  FROM #EventLog 
										  WHERE trim(Task) =  TaskA) AS FLOAT) ) AS Support ,
		((CAST(COUNT( *) AS FLOAT) /CAST( (SELECT COUNT( *) 
										   FROM #EventLog 
										   WHERE trim(Task) =  TaskA) AS FLOAT) ) * (CAST( (SELECT COUNT( *) 
																							FROM(SELECT Instance 
																								 FROM #EventLog 
																								 WHERE trim(Task) =  TaskA 
																								 GROUP BY Instance ) t2 ) AS FLOAT) /CAST( (SELECT COUNT( *) 
																																			FROM     (SELECT Instance 
																																					  FROM #EventLog 
																																					  GROUP BY Instance ) t ) AS FLOAT) ) ) AS Confidence
FROM #EventLog a ,  #tuplas x
WHERE a.Task = x.TaskA 
	  AND EXISTS     (SELECT * 
				      FROM #EventLog b 
				      WHERE b.Task = x.TaskB 
							AND b.Instance = a.Instance 
							AND b.Time > a.Time ) 
	  AND NOT EXISTS (SELECT * 
					  FROM #EventLog B, #EventLog c 
					  WHERE c.Instance= a.Instance 
					     AND b.Instance = a.Instance 
					  	 AND b.Task =x.TaskB 
					  	 AND c.Time > a.Time 
						 AND c.Time < b.Time )
GROUP BY x.TaskA , x.TaskB
--HAVING (CAST(COUNT( *) AS FLOAT) /CAST( (SELECT COUNT( *) 
--										 FROM #EventLog
--										 WHERE trim(Task) =  TaskA) AS FLOAT) ) > 0.7 
--        AND( (CAST(COUNT( *) AS FLOAT) /CAST( (SELECT COUNT( *) 
--												FROM #EventLog 
--												WHERE trim(Task) =  TaskA) AS FLOAT) ) * (CAST( (SELECT COUNT ( *) 
--																								  FROM(SELECT Instance 
--																								  			FROM #EventLog 
--																								  			WHERE trim(Task) =  TaskA
--																											GROUP BY Instance ) t2 ) AS FLOAT) / CAST( (SELECT COUNT( *)
--																																					    FROM(SELECT Instance 
--																																							 FROM #EventLog 
--																																							 GROUP BY Instance ) t ) AS FLOAT) ) ) > 0.5
--
--
union
----------------------------------------------------------
-------------------------Regra 4--------------------------
----------------------------------------------------------	
SELECT regra='Precedence' , 
		TaskA , 
		TaskB ,
	    (CAST(COUNT( *) AS FLOAT) /CAST( (SELECT COUNT( *) 
										  FROM #EventLog 
										  WHERE trim(Task) =  TaskB) AS FLOAT) ) AS Support ,
        ( (CAST(COUNT( *) AS FLOAT) /CAST( (SELECT COUNT( *) 
											FROM #EventLog 
											WHERE trim(Task) =  TaskB) AS FLOAT) ) * (CAST( (SELECT COUNT( *) 
																							 FROM(SELECT Instance 
																								  FROM #EventLog 
																								  WHERE trim(Task) =  TaskB 
																								  GROUP BY Instance ) t2 ) AS FLOAT) /CAST( (SELECT COUNT( *) 
																																			 FROM (SELECT Instance 
																																					  FROM #EventLog 
																																					  GROUP BY Instance ) t ) AS FLOAT) ) ) AS Confidence
FROM #EventLog a ,  #tuplas x
WHERE a.Task = x.TaskB AND EXISTS (SELECT * 
								   FROM #EventLog b 
								   WHERE b.Task = x.TaskA 
								   		 AND b.Instance = a.Instance 
								   		 AND b.Time < a.Time )
GROUP BY x.TaskA , x.TaskB
--HAVING (CAST(COUNT( *) AS FLOAT) /CAST( (SELECT COUNT( *) 
--										 FROM #EventLog 
--										 WHERE trim(Task) =  TaskB) AS FLOAT) ) > 0.7 
--		AND ( (CAST(COUNT( *) AS FLOAT) /CAST( (SELECT COUNT( *) 
--												FROM #EventLog
--												WHERE trim(Task) =  TaskB) AS FLOAT) ) * (CAST( (SELECT COUNT( *) 
--																							     FROM(SELECT Instance 
--																							     	  FROM #EventLog 
--																							     	  WHERE trim(Task) =  TaskB
--																									  GROUP BY Instance ) t2 ) AS FLOAT) / CAST( (SELECT COUNT( *)
--																									  											   FROM(SELECT Instance 
--																									  													FROM #EventLog 
--																									  													GROUP BY Instance ) t ) AS FLOAT) ) ) > 0.5
--
--
union
----------------------------------------------------------
-------------------------Regra 5--------------------------
----------------------------------------------------------	
SELECT regra='alternatePrecedence' , 
		TaskA , 
		TaskB ,
		(CAST(COUNT( *) AS FLOAT) /CAST( (SELECT COUNT( *) 
										  FROM #EventLog 
										  WHERE trim(Task) =  TaskB) AS FLOAT) ) AS Support ,
		( (CAST(COUNT( *) AS FLOAT) /CAST( (SELECT COUNT( *) 
										    FROM #EventLog 
											WHERE trim(Task) =  TaskB) AS FLOAT) ) * (CAST( (SELECT COUNT( *) 
																						 FROM (SELECT Instance 
																								FROM #EventLog 
																								WHERE trim(Task) =  TaskB 
																								GROUP BY Instance ) t2 ) AS FLOAT) /CAST( (SELECT COUNT( *) 
																																			  FROM (SELECT Instance 
																																					FROM #EventLog 
																																					GROUP BY Instance ) t ) AS FLOAT) ) ) AS Confidence
FROM #EventLog a ,  #tuplas x
WHERE a.Task = x.TaskB AND EXISTS (SELECT * 
									  FROM #EventLog b 
									  WHERE b.Task = x.TaskA 
											AND b.Instance = a.Instance 
											AND b.Time < a.Time )
AND NOT EXISTS(SELECT * 
			   FROM #EventLog B, #EventLog c 
			   WHERE c.Instance = a.Instance 
					 AND c.Task = x.TaskB 
					 AND b.Instance = a.Instance 
					 AND b.Task = x.TaskA 
					 AND c.Time< a.Time 
					 AND c.Time > b.Time )
GROUP BY x.TaskA , x.TaskB
--HAVING (CAST(COUNT( *) AS FLOAT) /CAST( (SELECT COUNT( *) 
--										 FROM #EventLog
--										 WHERE trim(Task) =  TaskB) AS FLOAT) ) > 0.7 
--											   AND ( (CAST(COUNT( *) AS FLOAT) /CAST( (SELECT COUNT( *) 
--																					   FROM #EventLog
--																					   WHERE trim(Task) =  TaskB) AS FLOAT) ) * (CAST( (SELECT COUNT ( *) 
--																																		FROM(SELECT Instance 
--																																			 FROM #EventLog 
--																																			 WHERE trim(Task) =  TaskB
--																																		GROUP BY Instance ) t2 ) AS FLOAT) / CAST( (SELECT COUNT( *)
--																																														FROM(SELECT Instance 
--																																															 FROM #EventLog 
--																																															 GROUP BY Instance ) t ) AS FLOAT) ) ) > 0.5
--																																		
--	
union
----------------------------------------------------------
-------------------------Regra 6--------------------------
----------------------------------------------------------	
SELECT regra='chainPrecedence' , 
		TaskA , 
		TaskB ,
		(CAST(COUNT( *) AS FLOAT) /CAST( (SELECT COUNT( *) 
										  FROM #EventLog 
										  WHERE trim(Task) =  TaskB) AS FLOAT) ) AS Support ,
		( (CAST(COUNT( *) AS FLOAT) /CAST( (SELECT COUNT( *) 
											FROM #EventLog 
											WHERE trim(Task) =  TaskB) AS FLOAT) ) * (CAST( (SELECT COUNT( *) 
																							 FROM (SELECT Instance 
																								   FROM #EventLog 
																								   WHERE trim(Task) =  TaskB 
																								   GROUP BY Instance ) t2 ) AS FLOAT) /CAST( (SELECT COUNT( *) 
																																			  FROM (SELECT Instance 
																																			     FROM #EventLog 
																																			     GROUP BY Instance ) t ) AS FLOAT) ) ) AS Confidence
FROM #EventLog a ,  #tuplas x
WHERE a.Task = x.TaskB 
	  AND EXISTS (SELECT * 
				  FROM #EventLog b 
				  WHERE b.Task = x.TaskA 
				  	AND b.Instance = a.Instance 
				  	AND b.Time < a.Time )
     AND NOT EXISTS(SELECT * 
					FROM #EventLog B, #EventLog c 
					WHERE c.Instance = a.Instance 
						 AND b.Instance = a.Instance 
						 AND b.Task = x.TaskA 
						 AND c.Time < a.Time 
						 AND c.Time > b.Time )
GROUP BY x.TaskA , x.TaskB
--HAVING (CAST(COUNT( *) AS FLOAT) /CAST( (SELECT COUNT( *) 
--										 FROM #EventLog
--										 WHERE trim(Task) =  TaskB) AS FLOAT) ) > 0.7 
--		AND ( (CAST(COUNT( *) AS FLOAT) /CAST( (SELECT COUNT( *) 
--												FROM #EventLog
--												WHERE trim(Task) =  TaskB) AS FLOAT) ) * (CAST( (SELECT COUNT ( *) 
--																								 FROM(SELECT Instance 
--																									  FROM #EventLog 
--																									  WHERE trim(Task) =  TaskB
--																									  GROUP BY Instance ) t2 ) AS FLOAT) / CAST( (SELECT COUNT( *)
--																																					 FROM(SELECT Instance 
--																																						  FROM #EventLog 
--																																						  GROUP BY Instance ) t ) AS FLOAT) ) ) > 0.5
--
--
union
----------------------------------------------------------
-------------------------Regra 7--------------------------
----------------------------------------------------------	
SELECT regra='respondedExistence' , 
		TaskA , 
		TaskB ,
		(CAST(COUNT( *) AS FLOAT) /CAST( (SELECT COUNT( *) 
										  FROM #EventLog 
										  WHERE trim(Task) =  TaskA or  trim(Task) =  TaskB ) AS FLOAT) ) AS Support ,
		( (CAST(COUNT( *) AS FLOAT) /CAST( (SELECT COUNT( *) 
											FROM #EventLog 
											WHERE trim(Task) =  TaskA  or  trim(Task) =  TaskB) AS FLOAT) ) * (CAST( (SELECT COUNT( *) 
																						 FROM (SELECT Instance 
																							   FROM #EventLog 
																							   WHERE trim(Task) =  TaskA GROUP BY Instance ) t2 ) AS FLOAT) /CAST( (SELECT COUNT( *) 
																																									FROM (SELECT Instance 
																																										  FROM #EventLog 
																																										  GROUP BY Instance ) t ) AS FLOAT) ) ) AS Confidence
FROM #EventLog a ,  #tuplas x
WHERE a.Task = x.TaskB AND EXISTS (SELECT * 
								   FROM #EventLog b 
								   WHERE b.Task = x.TaskA 
										 AND b.Instance = a.Instance )
GROUP BY x.TaskA , x.TaskB
--HAVING (CAST(COUNT( *) AS FLOAT) /CAST( (SELECT COUNT( *) 
--										 FROM #EventLog
--										 WHERE trim(Task) =  TaskA) AS FLOAT) ) > 0.7 
--	  AND ( (CAST(COUNT( *) AS FLOAT) /CAST( (SELECT COUNT( *) 
--											  FROM #EventLog
--											  WHERE trim(Task) =  TaskA) AS FLOAT) ) * (CAST( (SELECT COUNT ( *) 
--																							FROM(SELECT Instance 
--																								 FROM #EventLog 
--																								 WHERE trim(Task) =  TaskA 
--																								 GROUP BY Instance ) t2 ) AS FLOAT) / CAST( (SELECT COUNT( *) 
--																																				FROM(SELECT Instance 
--																																					FROM #EventLog 
--																																					GROUP BY Instance ) t ) AS FLOAT) ) ) > 0.5
--
--
union
----------------------------------------------------------
-------------------------Regra 8--------------------------
----------------------------------------------------------	
SELECT regra='notSuccession' , 
		TaskA , 
		TaskB ,
	    (CAST(COUNT( *) AS FLOAT) /CAST( (SELECT COUNT( *) 
										  FROM #EventLog 
										  WHERE trim(Task) =  TaskA) AS FLOAT) ) AS Support ,
        ( (CAST(COUNT( *) AS FLOAT) /CAST( (SELECT COUNT( *) 
											FROM #EventLog 
											WHERE trim(Task) =  TaskA) AS FLOAT) ) * (CAST( (SELECT COUNT( *) 
																						  FROM (SELECT Instance 
																								FROM #EventLog 
																								WHERE trim(Task) =  TaskA 
																								GROUP BY Instance ) t2 ) AS FLOAT) /CAST( (SELECT COUNT( *) 
																																			  FROM (SELECT Instance 
																																					FROM #EventLog GROUP BY Instance ) t ) AS FLOAT) ) ) AS Confidence
FROM #EventLog a ,  #tuplas x 
WHERE a.Task = x.TaskB 
	 AND a.Time < ALL (SELECT TIME
					   FROM #EventLog b 
					   WHERE b.Task = x.TaskA 
					   		AND b.Instance = a.Instance )
	 AND EXISTS (SELECT * 
				 FROM #EventLog b 
				 WHERE b.Task = x.TaskA 
					  AND b.Instance = a.Instance )
	 AND a.Time > ALL(SELECT Time 
					  FROM #EventLog b 
					  WHERE b.Task = x.TaskB 
							  AND b.Instance = a.Instance )
GROUP BY x.TaskA , x.TaskB
--HAVING (CAST(COUNT( *) AS FLOAT) /CAST( (SELECT COUNT( *) 
--										 FROM #EventLog 
--										 WHERE trim(Task) =  TaskA) AS FLOAT) ) > 0.7 
--		AND ( (CAST(COUNT( *) AS FLOAT) /CAST( (SELECT COUNT( *) 
--												FROM #EventLog
--												WHERE trim(Task) =  TaskA) AS FLOAT) ) * (CAST( (SELECT COUNT ( *) 
--																							 FROM(SELECT Instance 
--																							 FROM #EventLog WHERE trim(Task) =  TaskA
--																							 GROUP BY Instance ) t2 ) AS FLOAT) / CAST( (SELECT COUNT( *)
--																																			FROM(SELECT Instance 
--																																				 FROM #EventLog 
--																																				 GROUP BY Instance ) t ) AS FLOAT) ) ) > 0.5
--
--
--
--
--
--
)h