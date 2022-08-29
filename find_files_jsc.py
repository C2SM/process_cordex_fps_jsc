#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
File Name : find_files_jsc.py
Author: Ruth Lorenz (ruth.lorenz@c2sm.ethz.ch)
Created: 19-08-2022
Modified:
Purpose: find all files for CORDEX-FPSCONV data


'''
import os
import logging
import glob

from cdo import Cdo

import filefinder
from filefinder.filters import priority_filter

cdo = Cdo()

### Define logger
logging.basicConfig(format='%(levelname)s %(asctime)s: %(message)s',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

####################
### Define input ###
####################
INPUT_PATH = '/home/rlorenz/fpscpcm/CORDEX-FPSCONV/output'

DOMAIN = 'ALP-3'
SCENARIOS = ['historical', 'rcp85', 'evaluation']

VARIABLES = ['orog', 'lsm', 'sftls']

TIME_RES = ['fx', 'fx', 'fx']


def find_files(path_pattern, file_pattern, varn, v_ind):
    '''
    Find files to rsync using filefinder class
    '''
    ff = filefinder.FileFinder(path_pattern, file_pattern)
    files = ff.find_paths(variable=varn)

    logger.info('All files found are %s.', files)
    for path, meta in files:
        logger.info(meta)
        filelist = sorted(glob.glob(path))
        for ifile in filelist:
            # check if file:
            if os.path.isfile(ifile):
                os.system(f'rsync -av {ifile} '
                          f'daint:/store/c2sm/c2sme/CH202X/CORDEX-FPSCONV/'
                          f'{DOMAIN}/{TIME_RES[v_ind]}/{varn}/')
            else:
                logger.warning('Not file but %s found', ifile)

def main():
    '''
    Find files based on different path patterns and rsync to CSCS
    '''
    for v_ind, varn in enumerate(VARIABLES):
        file_pattern = '{variable}_ALP-3_{gcm}_{scenario}_{ensemble}_{rcm}_{nesting}_{t_freq}*.nc'

        path_pattern1 = '/home/rlorenz/fpscpcm/CORDEX-FPSCONV/output/ALP-3/{institut}/{gcm}/{scenario}/{ensemble}/{rcm}/{nesting}/{t_freq}/{variable}/'
        find_files(path_pattern1, file_pattern, varn, v_ind)

        path_pattern2 = '/home/rlorenz/fpscpcm/CORDEX-FPSCONV/output/ALP-3/{institut}/{gcm}/{t_freq}/'
        find_files(path_pattern2, file_pattern, varn, v_ind)

        path_pattern3 = '/home/rlorenz/fpscpcm/CORDEX-FPSCONV/output/ALP-3/{institut}/{gcm}/{scenario}/{ensemble}/{rcm}/{nesting}/{t_freq}/'
        find_files(path_pattern3, file_pattern, varn, v_ind)

        path_pattern4 = '/home/rlorenz/fpscpcm/CORDEX-FPSCONV/output/ALP-3/{institut}/{gcm}/{scenario}/{ensemble}/{rcm}/{nesting}/{t_freq}/{variable}/latest/'
        find_files(path_pattern4, file_pattern, varn, v_ind)

if __name__ == '__main__':
    main()
