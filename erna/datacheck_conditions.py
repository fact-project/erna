conditions = dict()

conditions['standard'] = [
    'fRunTypeName = "data"',
    'fZenithDistanceMean < 30',
    'fTriggerRateMedian > 40',
    'fTriggerRateMedian < 85',
    'fEffectiveOn > 0.95',
]

conditions['darknight'] = [
    'fCurrentsMedMean < 20',
    'fMoonZenithDistance > 100',
] + conditions['standard']


conditions['no_moonlight'] = [
    'fCurrentsMedMeanBeg < 8',
    'fZenithDistanceMax < 30',
    'fMoonZenithDistance > 100',
    'fThresholdMinSet < 350',
    'fEffectiveOn > 0.95',
    'fTriggerRateMedian > 40',
    'fTriggerRateMedian < 85',
    'fThresholdMinSet < (14 * fCurrentsMedMeanBeg + 265)'
]

conditions['low_moonlight'] = [
    'fTriggerRateMedian < 85',
    'fZenithDistanceMax < 30',
    'fThresholdMinSet < (14 * fCurrentsMedMeanBeg + 265)',
    'fCurrentsMedMeanBeg > 8',
    'fCurrentsMedMeanBeg <= 16',
]


conditions['moderate_moonlight'] = [
    'fTriggerRateMedian < 85',
    'fZenithDistanceMax < 30',
    'fThresholdMinSet < (14 * fCurrentsMedMeanBeg + 265)',
    'fCurrentsMedMeanBeg > 32',
    'fCurrentsMedMeanBeg <= 48',
]

conditions=['strong_moonlight'] = [
    'fTriggerRateMedian < 85',
    'fZenithDistanceMax < 30',
    'fThresholdMinSet < (14 * fCurrentsMedMeanBeg + 265)',
    'fCurrentsMedMeanBeg > 64',
    'fCurrentsMedMeanBeg <= 96',
]
