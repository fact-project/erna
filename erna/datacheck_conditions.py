conditions = dict()

conditions[''] = []
# ------------------------------------------------------------------------------

conditions['std'] = [
    'fRunTypeKey == 1',  # Data Events
    'fMoonZenithDistance > 100',
    'fROI == 300',
    'fZenithDistanceMean < 30',
    'fTriggerRateMedian > 40',
    'fTriggerRateMedian < 85',
    'fOnTime > 0.95',
    'fThresholdMinSet < 350'
]

# ------------------------------------------------------------------------------

conditions['onlyOnTime'] = [
    'fRunTypeKey == 1',  # Data Events
    'fROI == 300',
    'fZenithDistanceMean < 30',
    #'fTriggerRateMedian > 40',
    #'fTriggerRateMedian < 85',
    'fOnTime > 0.95',
    #'fThresholdMinSet < 350'
]

# ------------------------------------------------------------------------------

# zdparamStr = 'pow(0.753833 * cos(Radians(fZenithDistanceMean)), 7.647435) * exp(-5.753686*pow(Radians(fZenithDistanceMean),2.089609))'
# thparamStr = 'pow((if(isnull(fThresholdMinSet),fThresholdMedian,fThresholdMinSet)-329.4203),2) * (-0.0000002044803)'
# paramStr = '(fNumEvtsAfterBgCuts/5-fNumSigEvts)/fOnTimeAfterCuts - {0} - {1}'.format(zdparamStr, thparamStr)
# conditions['dorner'] = [
#     'fNight > 20120420',  # the currentfeedback starts 20120420
#     'not fNight in [20120406, 20120410, 20120503]',  # different bias voltage
#     'not 20121206 < fNight < 20130110',  # broken bias channel
#     # '-0.085 < {0} AND {0} < 0.25'.format(paramStr)
#     # datacheck parameter condition
#     # (complex parameter which depends on the results
#     # of the std analysis at the isdc)
# ]

# ------------------------------------------------------------------------------

conditions['dorner'] = [
    'fRunTypeKey == 1', # datafiles
    'fNight > 20120420',  # the currentfeedback starts 20120420
    'not fNight in [20120406, 20120410, 20120503]',  # different bias voltage
    'not 20121206 < fNight < 20130110',  # broken bias channel
    # '-0.085 < {0} AND {0} < 0.25'.format(paramStr)
    # datacheck parameter condition
    # (complex parameter which depends on the results
    # of the std analysis at the isdc)
]

# ------------------------------------------------------------------------------

conditions['darknight'] = [
    'fRunTypeKey == 1', # datafiles
    'fCurrentsMedMeanBeg < 20',  # light condition cut
    'fThresholdMinSet < 400',  # light condition cut
    'fThresholdMinSet < (14 * fCurrentsMedMeanBeg + 265)'  # katjas 'blob' cut
]

# ------------------------------------------------------------------------------

conditions['Crab20142015'] = [
'fSourceKEY == 5', # Crab
'fRunTypeKey == 1', # datafiles
'fZenithDistanceMax < 30', # zenith cut
'fZenithDistanceMin > 6', # zenith cut
'20140630 < fNight < 20150630', # used time range for the set
'fTriggerRateMedian < 85',
'fTriggerRateMedian > 40',
'fMoonZenithDistance > 100',
'fThresholdMinSet < 350',
'fEffectiveOn > 0.95',
]
conditions['Crab20142015'] += conditions['darknight']
conditions['Crab20142015'] += conditions['dorner']

# ------------------------------------------------------------------------------

conditions['Crab20132014'] = [
'fSourceKEY == 5', # Crab
'fRunTypeKey == 1', # datafiles
'fZenithDistanceMax < 30', # zenith cut
'fZenithDistanceMin > 6', # zenith cut
'20130630 < fNight < 20140205', # used time range for the set
'fTriggerRateMedian < 85',
'fTriggerRateMedian > 40',
'fMoonZenithDistance > 100',
'fThresholdMinSet < 350',
'fEffectiveOn > 0.95',
]
conditions['Crab20132014'] += conditions['darknight']
conditions['Crab20132014'] += conditions['dorner']

# ------------------------------------------------------------------------------
