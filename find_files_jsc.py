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

cdo = Cdo()

### Define logger
logging.basicConfig(format='%(levelname)s %(asctime)s: %(message)s',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

####################
### Define input ###
####################
INPUT_PATH = '/home/rlorenz/fpscpcm/CORDEX-FPSCONV/output'

DOMAIN = 'EUR-11'
SCENARIOS = ['historical', 'rcp85', 'evaluation']

VARIABLES = ['orog', 'sftls']

TIME_RES = ['fx', 'fx']

OUTPUT_PATH = f'/home/rlorenz/fpscpcm/tmp/rlorenz/data/{DOMAIN}'

def find_files(path_pattern, file_pattern, varn, t_freq):
    '''
    Find files and copy/rename using filefinder class
    '''
    outpath_varn = f'{OUTPUT_PATH}/{t_freq}/{varn}'
    if not os.access(outpath_varn, os.F_OK):
        os.makedirs(outpath_varn)

    ff = filefinder.FileFinder(path_pattern, file_pattern)
    try:
        files = ff.find_paths(variable=varn, t_freq=t_freq)
    except ValueError:
        logger.warning('No files found for path %s', path_pattern)
        return

    logger.info('All files found are %s.', files)
    for path, meta in files:
        logger.info(meta)
        filelist = sorted(glob.glob(path))
        for ifile in filelist:
            # check if file:
            if os.path.isfile(ifile):
                if ifile.endswith('.nc'):
                    new_name = (f'{varn}_{DOMAIN}_{meta["gcm"]}_{meta["scenario"]}_'
                                f'{meta["ensemble"]}_{meta["rcm"]}_{meta["nesting"]}_'
                                f'{t_freq}.nc')
                    logger.info('New filename is %s', new_name)
                    os.system(f'cp {ifile} {outpath_varn}/{new_name}')
                else:
                    logger.warning('File found is not netcdf but %s', ifile)
            else:
                logger.warning('Not file but %s found', ifile)

def main():
    '''
    Find files based on different path patterns and copy/rename to output folder
    '''
    for v_ind, varn in enumerate(VARIABLES):

        file_pattern = '{variable}_%s_{gcm}_{scenario}_{ensemble}_{rcm}_{nesting}_{t_freq}*.nc' %(DOMAIN)

        path_pattern1 = '%s/%s/{institut}/{gcm}/{scenario}/{ensemble}/{rcm}/{nesting}/{t_freq}/{variable}/' %(INPUT_PATH, DOMAIN)
        find_files(path_pattern1, file_pattern, varn, TIME_RES[v_ind])

        path_pattern2 = '%s/%s/{institut}/{gcm}/{t_freq}/' %(INPUT_PATH, DOMAIN)
        find_files(path_pattern2, file_pattern, varn, TIME_RES[v_ind])

        path_pattern3 = '%s/%s/{institut}/{gcm}/{scenario}/{ensemble}/{rcm}/{nesting}/{t_freq}/' %(INPUT_PATH, DOMAIN)
        find_files(path_pattern3, file_pattern, varn, TIME_RES[v_ind])

        path_pattern4 = '%s/%s/{institut}/{gcm}/{scenario}/{ensemble}/{rcm}/{nesting}/{t_freq}/{variable}/latest/' %(INPUT_PATH, DOMAIN)
        find_files(path_pattern4, file_pattern, varn, TIME_RES[v_ind])


if __name__ == '__main__':
    main()
