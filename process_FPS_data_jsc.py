#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
File Name : process_FPS_data_jsc.py
Author: Ruth Lorenz (ruth.lorenz@c2sm.ethz.ch)
Created: 23-06-2022
Modified:
Purpose: process high resolution FPS data at jsc to decrease data amount
         to copy to CSCS
 steps:
 * process variables:
 Some of the variables might exist in different frequency
    - 1hr: pr, tas
    - 3hr: hus850
    - 6hr: psl, zg500, zg850
    - day: tasmax, tasmin, snd
    - fx: orog
 * cut DOMAIN to allAlps region

'''
import time
import os
import logging
import glob
from cdo import *
cdo = Cdo()

import filefinder
from filefinder.filters import priority_filter

import time_functions as tf

# Define logfile and logger
seconds = time.time()
local_time = time.localtime(seconds)
LOG_FILENAME = (f'logfiles/logging_{local_time.tm_year}{local_time.tm_mon}'
                f'{local_time.tm_mday}{local_time.tm_hour}{local_time.tm_min}'
                f'{local_time.tm_sec}.out')

logging.basicConfig(filename=LOG_FILENAME,
                    filemode='w',
                    format='%(levelname)s %(asctime)s: %(message)s',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

####################
### Define input ###
####################
INPUT_PATH = '/home/rlorenz/fpscpcm/CORDEX-FPSCONV/output'

DOMAIN = 'ALP-3'
SCENARIOS = ['historical', 'rcp85', 'evaluation']

#VARIABLES = ["tas", "pr", "hus850", "psl", "zg500", "zg850",
#             "tasmax", "tasmin", "snd", "orog"]
VARIABLES = ['pr']
# time resolutions we want for each variable
#TIME_RES = ["1hr", "1hr", "6hr", "6hr", "6hr", "6hr", "day", "day", "day", "fx"]
TIME_RES = ['1hr']
# valid time resolutions to look in if the one we want is not available
TRES_VALID = ['1hr', '3hr', '6hr', 'day']

SUBDOMAIN = 'allAlps'
LON1 = 5.3
LON2 = 16.3
LAT1 = 43.3
LAT2 = 48.5

OVERWRITE = False # Flag to trigger overwriting of Files
OUTPUT_PATH = f'/home/rlorenz/fpscpcm/tmp/rlorenz/data/{SUBDOMAIN}'
WORKDIR = '/home/rlorenz/fpscpcm/tmp/rlorenz/data/work'


def get_folders(path):
    '''
    Get all folder names in path

    Parameters
    ----------
    path: string
        path to look for folder names

    Returns
    -------
    res: list
        list with all folder names
    '''
    res = []
    for folder in os.listdir(path):
        if os.path.isdir(os.path.join(path, folder)):
            res.append(folder)

    return res

def remove_item_from_list(orig_list, item):
    '''
    Remove a known item from a list

    Parameters
    ----------
    orig_list: list
        original list
    item: string
        item to be removed

    Returns
    -------
    orig_list: list
        list without item
    '''
    try:
        orig_list.remove(item)
    except ValueError:
        infomsg = ('No %s folder to remove from list.', item)
        logger.info(infomsg)

    return orig_list

def main():
    '''
    Loop over all files found and cut to smaller domain,
    resample if necessary
    '''

    # Create directories if needed
    if not os.access(OUTPUT_PATH, os.F_OK):
        os.makedirs(OUTPUT_PATH)
    if not os.access(WORKDIR, os.F_OK):
        os.makedirs(WORKDIR)


    if len(TIME_RES) != len(VARIABLES):
        errormsg = ('Lists TIME_RES and VARIABLES do not have equal length, '
                    'please check those lists!')
        logger.error(errormsg)
        os.exit()

    for v_ind, varn in enumerate(VARIABLES):
        outpath_varn = f'{OUTPUT_PATH}/{varn}'
        if not os.access(outpath_varn, os.F_OK):
            os.makedirs(outpath_varn)

        path_pattern = '/home/rlorenz/fpscpcm/CORDEX-FPSCONV/output/ALP-3/{institut}/{gcm}/{scenario}/{ensemble}/{rcm}/{nesting}/{t_freq}/{variable}/'
        file_pattern = '{variable}_ALP-3_{gcm}_{scenario}_{ensemble}_{rcm}_{nesting}_{t_freq}_*.nc'

        ff = filefinder.FileFinder(path_pattern, file_pattern)
        files = ff.find_paths(variable=varn)
        files_prioritized = priority_filter(files, "t_freq", TRES_VALID)

        for path, meta in files_prioritized:
            print(f"{path} = ")
            print(f"{meta} = ")
            print()

            if meta['rcm'] in ('BCCR-WRF381BF', 'BCCR-WRF381CF'):
                continue

            derived = False
            filelist = sorted(glob.glob(path))

            if TIME_RES[v_ind] != meta['t_freq']
                derived = True


            logger.info('%s files found, start processing:', len(filelist))
            # Loop over all files found in file_path
            for ifile in filelist:
                # find time range included in file for output filename
                # cdo showdate returns list with one string incl. all dates
                # also checks if file can be read by cdo
                try:
                    dates = cdo.showdate(input=ifile)[0]
                except PermissionError:
                    errmsg = f'PermissionError for file {ifile}, continuing.'
                    logger.error(errmsg)
                    continue
                except Exception:
                    errmsg = (f'Unknown error on file {ifile} using '
                              f' cdo.showdate, continuing.')
                    logger.error(errmsg)
                    continue
                firstdate = dates.split(' ')[0]
                firstdate_str = ''.join(firstdate.split('-'))
                lastdate = dates.split(' ')[-1]
                lastdate_str = ''.join(lastdate.split('-'))
                time_range = f'{firstdate_str}-{lastdate_str}'

                # find nesting info from path for output filename
                split_ifile = ifile.split('/')
                nesting = split_ifile[12]

                filename = (f'{varn}_{SUBDOMAIN}_{meta['gcm']}_{meta['scenario']}_'
                            f'{meta['ensemble']}_{meta['rcm']}_{meta['nesting']}_'
                            f'{TIME_RES[v_ind]}_{time_range}')
                if derived:
                    tmp_file = (f'{WORKDIR}/{varn}_{SUBDOMAIN}_{meta['gcm']}'
                                f'_{meta['scenario']}_{meta['ensemble']}_{meta['rcm']}_{meta['nesting']}'
                                f'_{meta['t_freq']}_{time_range}.nc')
                    if (
                        TIME_RES[v_ind] == '1hr' and
                        meta['t_freq'] in ['3hr', '6hr', 'day'] or
                        (TIME_RES[v_ind] == '6hr' and
                        meta['t_freq'] == 'day')
                    ):
                        infomsg=(f'TIME_RES[v_ind]: {TIME_RES[v_ind]}'
                                 f' and new_time_res: {meta['t_freq']}.'
                                 f'We do not upsample, native time'
                                 f' frequency will be processed!')
                        logger.info(infomsg)
                        filename = (f'{varn}_{SUBDOMAIN}_{meta['gcm']}_{meta['scenario']}_'
                                    f'{meta['ensemble']}_{meta['rcm']}_{meta['nesting']}_'
                                    f'{meta['t_freq']}_{time_range}')

                ofile = f'{outpath_varn}/{filename}.nc'

                # Check if ofile already exists, create if does not exist
                # yet or OVERWRITE=True
                if os.path.isfile(ofile) and not OVERWRITE:
                    logger.info('File %s already exists.', ofile)
                else:
                    if derived and not (
                    (TIME_RES[v_ind] == '1hr' and
                    meta['t_freq'] in ['3hr', '6hr', 'day']) or
                    (TIME_RES[v_ind] == '6hr' and
                    meta['t_freq'] == 'day')
                    ):

                        cdo.sellonlatbox(f'{LON1},{LON2},{LAT1},{LAT2}',
                                         input=ifile, output=tmp_file)
                        # Variable needs to be derived for the
                        # required time frequency
                        if TIME_RES[v_ind] == 'day':
                            tf.calc_to_day(varn, tmp_file, ofile)
                        elif TIME_RES[v_ind] == '6hr' and new_time_res == '1hr':
                            tf.calc_1h_to_6h(varn, tmp_file, ofile)
                        elif TIME_RES[v_ind] == '3hr' and new_time_res == '1hr':
                            tf.calc_1h_to_3h(varn, tmp_file, ofile)
                        elif TIME_RES[v_ind] == '6hr' and new_time_res == '3hr':
                            tf.calc_3h_to_6h(varn, tmp_file, ofile)
                        else:
                            errormsg = ('Not implemented error!'
                                        ' TIME_RES[v_ind]: %s,'
                                        ' meta["t_freq"]: %s',
                                        TIME_RES[v_ind],
                                        new_time_res)
                            logger.error(errormsg)
                        # clean up WORKDIR
                        os.system(f'rm {WORKDIR}/*')
                    else:
                        # All we need to do is cut the SUBDOMAIN
                        try:
                            cdo.sellonlatbox(f'{LON1},{LON2},{LAT1},{LAT2}',
                                             input=ifile, output=ofile)
                            logger.info('File written to %s', ofile)
                        except:
                            logger.error('File %s not written', ofile)
                            logger.error('Something wrong with input file')
                            continue

    logger.info('All files found that could be processed were processed.')

if __name__ == '__main__':
    main()
