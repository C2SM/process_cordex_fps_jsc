#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
File Name : find_files_jsc.py
Author: Ruth Lorenz (ruth.lorenz@c2sm.ethz.ch)
Created: 19-08-2022
Modified:
Purpose: find all files for CORDEX-FPSCONV data


'''
import time
import os
import logging
import glob
from cdo import *
cdo = Cdo()

import filefinder
from filefinder.filters import priority_filter

####################
### Define input ###
####################
INPUT_PATH = '/home/rlorenz/fpscpcm/CORDEX-FPSCONV/output'

DOMAIN = 'ALP-3'
SCENARIOS = ['historical', 'rcp85', 'evaluation']

VARIABLES = ['pr', "orog"]

TIME_RES = ['1hr', "fx"]
TRES_VALID = ['1hr', '3hr', '6hr', 'day']

def main():
    '''
    Loop over all files found and cut to smaller domain,
    resample if necessary
    '''
    for varn in VARIABLES:

        path_pattern = '/home/rlorenz/fpscpcm/CORDEX-FPSCONV/output/ALP-3/{institut}/{gcm}/{scenario}/{ensemble}/{rcm}/{nesting}/{t_freq}/{variable}/'
        file_pattern = '{variable}_ALP-3_{gcm}_{scenario}_{ensemble}_{rcm}_{nesting}_{t_freq}_*.nc'

        ff = filefinder.FileFinder(path_pattern, file_pattern)
        files = ff.find_paths(variable=varn)
        files_prioritized = priority_filter(files, "t_freq", TRES_VALID)

        logger.info('All files found are %s.', files_prioritized)

if __name__ == '__main__':
    main()
