WITH departures AS (
	SELECT origin AS faa, 
			COUNT(DISTINCT dest) AS nunique_to,
			COUNT(sched_dep_time) AS dep_planned,
			SUM(cancelled) AS dep_cancelled,
			SUM(diverted) AS dep_diverted,
			COUNT(arr_time) AS act_dep_n_flights
	FROM {{ref('prep_flights')}}
	GROUP BY origin
),
arrivals AS (
	SELECT dest AS faa, 
			COUNT(DISTINCT origin) AS nunique_from, 
			COUNT(sched_dep_time) AS arr_planned,
			SUM(cancelled) AS arr_cancelled,
			SUM(diverted) AS arr_diverted,
			COUNT(arr_time) AS act_arr_n_flights
	FROM {{ref('prep_flights')}}
	GROUP BY dest
),
total_stats AS (
	SELECT faa,
			nunique_to,
			nunique_from,
			d.dep_planned + a.arr_planned AS total_planned,
			d.dep_cancelled + a.arr_cancelled AS total_cancelled,
			d.dep_diverted + a.arr_diverted AS total_diverted,
			d.act_dep_n_flights + a.act_arr_n_flights AS total_act_n_flights
	FROM departures d
	JOIN arrivals a
	USING (faa)
)
SELECT ap.city,
		ap.country,
		ap.name,
		ts.* 
FROM total_stats ts
LEFT JOIN {{ref('prep_airports')}} ap
USING (faa)