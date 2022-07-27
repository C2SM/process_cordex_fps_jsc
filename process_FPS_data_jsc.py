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
VARIABLES = ['hus850']
# time resolutions we want for each variable
#TIME_RES = ["1hr", "1hr", "6hr", "6hr", "6hr", "6hr", "day", "day", "day", "fx"]
TIME_RES = ['6hr']
# valid time resolutions to look in if the one we want is not available
TRES_VALID = ['1hr', '3hr', '6hr', 'day']

SUBDOMAIN = 'allAlps'
LON1 = 5.3
LON2 = 16.3
LAT1 = 43.3
LAT2 = 48.5

OVERWRITE = True # Flag to trigger overwriting of Files
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

    # Find all institutes, models etc.
    institutes = get_folders(f'{INPUT_PATH}/{DOMAIN}/')
    # remove ETHz from list because we only need ETHZ-2
    institutes.remove('ETHZ')
    logger.info('Institute folders found are: %s', institutes)

    for inst in institutes:
        # Loop over variables, SCENARIOS, models
        for scen in SCENARIOS:
            if scen == 'evaluation':
                gcm = 'ECMWF-ERAINT'
            else:
                gcms = get_folders(f'{INPUT_PATH}/{DOMAIN}/{inst}')
                logger.info('gcms list is %s', gcms)
                remove_item_from_list(gcms, 'ECMWF-ERAINT')

                # check if one gcm name found:
                if len(gcms) > 1:
                    errormsg = ('More than one gcm folder found! %s', gcms)
                    logger.error(errormsg)
                    exit()
                if len(gcms) == 1:
                    logger.info('One gcm folder found: %s', gcms[0])
                else:
                    logger.warning('No gcm folder found, continuing')
                    continue
                gcm = gcms[0]

                # find ensemble
                try:
                    ensembles = get_folders(f'{INPUT_PATH}/{DOMAIN}/{inst}/{gcm}/{scen}')
                except FileNotFoundError:
                    warnmsg = ('No folder found for %s', scen)
                    logger.warning(warnmsg)
                    continue
                remove_item_from_list(ensembles, 'r0i0p0')
                ensemble = ensembles[0]
                # find rcm names
                rcms = get_folders(f'{INPUT_PATH}/{DOMAIN}/{inst}/{gcm}/{scen}/{ensemble}')

                # loop over rcms
                for rcm in rcms:
                    logger.info('RCM is %s', rcm)
                    for v_ind, varn in enumerate(VARIABLES):
                        file_path = (f'{INPUT_PATH}/{DOMAIN}/{inst}/{gcm}/{scen}'
                                     f'/{ensemble}/{rcm}/*/{TIME_RES[v_ind]}/{varn}/*.nc')
                        derived = False
                        # Find all files matching pattern file_path
                        filelist = glob.glob(file_path)
                        if len(filelist) == 0:
                            warnmsg = ('Filelist is empty, no files for path %s,'
                                       ' keep looking in other frequencies.',
                                       file_path)
                            logger.warning(warnmsg)
                            # Variables can be in different frequencies than the
                            # required ones, check other folders
                            tres_valid_rm = [n for n in TRES_VALID if n != TIME_RES[v_ind]]
                            logger.info(tres_valid_rm)
                            new_time_res = None
                            for new_time_res in tres_valid_rm:
                                check_path = (f'{INPUT_PATH}/{DOMAIN}/{inst}/{gcm}/'
                                              f'{scen}/{ensemble}/{rcm}/*/{new_time_res}/'
                                              f'{varn}/*.nc')
                                filelist = glob.glob(check_path)
                                if len(filelist) != 0:
                                    infomsg=('Variable %s found in frequency %s',
                                             varn, new_time_res)
                                    logger.info(infomsg)
                                    derived = True
                                    break
                            if len(filelist) == 0:
                                warnmsg = ('No files found for %s', file_path)
                                logger.warning(warnmsg)
                                continue

                        logger.info('%s files found, start processing:', len(filelist))
                        # Loop over all files found in file_path
                        for ifile in filelist:
                            split_ifile = ifile.split('/')

                            # find nesting info from path for output filename
                            nesting = split_ifile[12]

                            # find time range included in file for output filename
                            # cdo showdate returns list with one string incl. all dates
                            dates = cdo.showdate(input=ifile)[0]
                            firstdate = dates.split(' ')[0]
                            firstdate_str = ''.join(firstdate.split('-'))
                            lastdate = dates.split(' ')[-1]
                            lastdate_str = ''.join(lastdate.split('-'))
                            time_range = f'{firstdate_str}-{lastdate_str}'

                            filename = (f'{varn}_{SUBDOMAIN}_{gcm}_{scen}_'
                                        f'{ensemble}_{rcm}_{nesting}_'
                                        f'{TIME_RES[v_ind]}_{time_range}')
                            if derived:
                                tmp_file = (f'{WORKDIR}/{varn}_{SUBDOMAIN}_{gcm}'
                                            f'_{scen}_{ensemble}_{rcm}_{nesting}'
                                            f'_{new_time_res}_{time_range}.nc')
                                if (
                                    TIME_RES[v_ind] == '1hr' and
                                    new_time_res in ['3hr', '6hr', 'day'] or
                                    (TIME_RES[v_ind] == '6hr' and
                                    new_time_res == 'day')
                                ):
                                    infomsg=(f'TIME_RES[v_ind]: {TIME_RES[v_ind]}'
                                             f' and new_time_res: {new_time_res}.'
                                             f'We do not upsample, native time'
                                             f' frequency will be processed!')
                                    logger.info(infomsg)
                                    filename = (f'{varn}_{SUBDOMAIN}_{gcm}_{scen}_'
                                                f'{ensemble}_{rcm}_{nesting}_'
                                                f'{new_time_res}_{time_range}')


                            logger.info('Filename is %s', filename)
                            ofile = f'{OUTPUT_PATH}/{filename}.nc'

                            # Check if ofile already exists, create if does not exist
                            # yet or OVERWRITE=True
                            if os.path.isfile(ofile) and not OVERWRITE:
                                logger.info('File %s already exists.', ofile)
                            else:
                                if derived:
                                    if (
                                        (TIME_RES[v_ind] == '1hr' and
                                        new_time_res in ['3hr', '6hr', 'day']) or
                                        (TIME_RES[v_ind] == '6hr' and
                                        new_time_res == 'day')
                                    ):
                                        # process native frequency, do not increase frequency
                                        cdo.sellonlatbox(f'{LON1},{LON2},{LAT1},{LAT2}',
                                                         input=ifile, output=ofile)
                                    else:
                                        cdo.sellonlatbox(f'{LON1},{LON2},{LAT1},{LAT2}',
                                                         input=ifile, output=tmp_file)
                                        # Variable needs to be derived for the required time frequency
                                        if TIME_RES[v_ind] == 'day':
                                            tf.calc_to_day(varn, tmp_file, ofile)
                                        elif TIME_RES[v_ind] == '6hr' and new_time_res == '1hr':
                                            logger.info(f'Calculating from new_time_res: {new_time_res}'
                                                        f' to TIME_RES[v_ind]: {TIME_RES[v_ind]}')
                                            tf.calc_1h_to_6h(varn, tmp_file, ofile)
                                        elif TIME_RES[v_ind] == '3hr' and new_time_res == '1hr':
                                            tf.calc_1h_to_3h(varn, tmp_file, ofile)
                                        elif TIME_RES[v_ind] == '6hr' and new_time_res == '3hr':
                                            tf.calc_3h_to_6h(varn, tmp_file, ofile)
                                        else:
                                            errormsg = ('Not implemented error!'
                                                        ' TIME_RES[v_ind]: %s,'
                                                        ' new_time_res: %s',
                                                        TIME_RES[v_ind],
                                                        new_time_res)
                                            logger.error(errormsg)
                                    # clean up WORKDIR
                                    os.system(f'rm {WORKDIR}/*')
                                else:
                                    # All we need to do is cut the SUBDOMAIN
                                    cdo.sellonlatbox(f'{LON1},{LON2},{LAT1},{LAT2}',
                                                     input=ifile, output=ofile)


if __name__ == '__main__':
    main()
