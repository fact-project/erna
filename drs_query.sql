SELECT
	r.fNight AS night,
	r.fRunID AS run_id,
	r.fRunStart AS run_start,
	r.fRunStop AS run_stop,
	s.fSourceName AS source_name,
	r.fZenithDistanceMean AS mean_zenith,
	r.fHumidityMean as mean_humidity,
	r.fTNGDust as tng_dust,
	r.fTriggerRateMedian as median_trigger_rate,
	r.fCurrentsMedMean as mean_current,
	TIMESTAMPDIFF(SECOND, r.fRunStart, fRunStop) * r.fEffectiveON as ontime,
	(
		SELECT d.fRunID
		FROM RunInfo d
		WHERE
			d.fDrsStep = 2
			AND d.fRunTypeKey = 2
			AND d.fNight = r.fNight
			AND ABS(TIMESTAMPDIFF(MINUTE, d.fRunStart, r.fRunStart)) < 120
		ORDER BY ABS(TIMESTAMPDIFF(MINUTE, d.fRunStart, r.fRunStart))
		LIMIT 1
	) AS drs_run
FROM RunInfo r
LEFT JOIN RunType rt
ON r.fRunTypeKey = rt.fRunTypeKey
LEFT JOIN Source s
ON r.fSourceKey = s.fSourceKey
WHERE
	rt.fRunTypeName = "data"
	AND s.fSourceTypeKey = 1
	AND r.fNight >= 20120810
	AND r.fNight <= 20180630
	AND r.fCurrentsMedMean <= 10
	AND r.fRunStart IS NOT NULL
	AND r.fRunStop IS NOT NULL
	AND r.fEffectiveON IS NOT NULL
