from sklearn import ensemble

n_trees = 150
n_jobs = 2
max_depth=100
#the classifier to be used.
learner = ensemble.ExtraTreesRegressor(n_estimators=n_trees, max_features="sqrt", n_jobs=n_jobs, max_depth=max_depth)


#randomly sample the data if you dont want to use

#query to apply to the data before separation
query = 'Leakage < 0.2'

# define th number of cross validations to perform
num_cross_validations = 5

training_variables = ['Size', 'Length', 'Width', 'numIslands', 'Leakage',
        'm3l', 'm3t', 'Disp', 'Timespread', 'arrivalTimeMean',
       'ConcCore']
#specify the variables to train on
# training_variables = ['ConcCore',
#  'Concentration_onePixel',
#  'Concentration_twoPixel',
#  'Leakage',
#  'Leakage2',
#  'Size',
#  'Slope_long',
#  'Slope_spread',
#  'Slope_spread_weighted',
#  'Slope_trans',
#  'Distance',
#  'Theta',
#  'Timespread',
#  'Timespread_weighted',
#  'Width',
#  'arrTimePosShower_kurtosis',
#  'arrTimePosShower_max',
#  'arrTimePosShower_mean',
#  'arrTimePosShower_min',
#  'arrTimePosShower_skewness',
#  'arrTimePosShower_variance',
#  'arrTimeShower_kurtosis',
#  'arrTimeShower_max',
#  'arrTimeShower_mean',
#  'arrTimeShower_min',
#  'arrTimeShower_skewness',
#  'arrTimeShower_variance',
#  'concCOG',
#  'm3l',
#  'm3t',
#  'maxPosShower_kurtosis',
#  'maxPosShower_max',
#  'maxPosShower_mean',
#  'maxPosShower_min',
#  'maxPosShower_skewness',
#  'maxPosShower_variance',
#  'maxSlopesPosShower_kurtosis',
#  'maxSlopesPosShower_max',
#  'maxSlopesPosShower_mean',
#  'maxSlopesPosShower_min',
#  'maxSlopesPosShower_skewness',
#  'maxSlopesPosShower_variance',
#  'maxSlopesShower_kurtosis',
#  'maxSlopesShower_max',
#  'maxSlopesShower_mean',
#  'maxSlopesShower_min',
#  'maxSlopesShower_skewness',
#  'maxSlopesShower_variance',
#  'numIslands',
#  'numPixelInShower',
#  'phChargeShower_kurtosis',
#  'phChargeShower_max',
#  'phChargeShower_mean',
#  'phChargeShower_min',
#  'phChargeShower_skewness',
#  'phChargeShower_variance',
#  'photonchargeMean'
#  ]
