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
    - fx: orog
    - 1hr: pr, tas
    - 6hr: evspsbl/ hfls, rlds, rlus, rsds, rsus, hurs, prsn,
           psl, hus850, zg500, zg850
    - day: tasmax, tasmin, snw (snd), mrro


'''
import glob
import logging
import os, sys
import time
import shutil

from cdo import Cdo

import filefinder
from filefinder.filters import priority_filter

import time_functions as tf

cdo = Cdo()

# Define logfile and logger
seconds = time.time()
local_time = time.localtime(seconds)
LOG_FILENAME = (f'logfiles/logging_proc_{local_time.tm_year}{local_time.tm_mon}'
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
DOMAIN = 'ALP-3'
INPUT_PATH = f'/home/rlorenz/fpscpcm/CORDEX-FPSCONV/output/{DOMAIN}'

SCENARIOS = ['historical', 'rcp85', 'evaluation']
VARIABLES = ['hurs', 'hus850']
TIME_RES = ['6hr', '6hr']

# valid time resolutions to look in if the one we want is not available
TRES_VALID = ['1hr', '3hr', '6hr']

OVERWRITE = False # Flag to trigger overwriting of Files
OUTPUT_PATH = f'/home/rlorenz/fpscpcm/tmp/rlorenz/data/{DOMAIN}'
WORKDIR = '/home/rlorenz/fpscpcm/tmp/rlorenz/data/work'



def find_dates_in_file(input_file):
    '''
    find time range included in file for output filename
    cdo showdate returns list with one string incl. all dates
    '''
    dates = cdo.showdate(input=input_file)[0]

    firstdate = dates.split(' ')[0]
    firstdate_str = ''.join(firstdate.split('-'))
    lastdate = dates.split(' ')[-1]
    lastdate_str = ''.join(lastdate.split('-'))
    return f'{firstdate_str}-{lastdate_str}'


def process_file(meta, ifile, ofile, time_res_in,
                 derived=False, varnamech=False):
    '''
    Process input file, change varname if necessary,
    Resample if necessary (derived=True)
    create link if nothing needs to be done
    '''
    if derived:
        # Variable needs to be derived for the
        # required time frequency
        if time_res_in == 'day':
            tf.calc_to_day(meta["variable"], ifile, ofile)
        elif time_res_in == '6hr' and meta["t_freq"] == '1hr':
            tf.calc_1h_to_6h(meta["variable"], ifile, ofile)
        elif time_res_in == '3hr' and meta["t_freq"] == '1hr':
            tf.calc_1h_to_3h(meta["variable"], ifile, ofile)
        elif time_res_in == '6hr' and meta["t_freq"] == '3hr':
            tf.calc_3h_to_6h(meta["variable"], ifile, ofile)
        else:
            errormsg = ('Not implemented error! TIME_RES[v_ind]: %s,'
                        ' meta["t_freq"]: %s', time_res_in, meta["t_freq"])
            logger.error(errormsg)
        logger.info('File written to %s', ofile)
    elif varnamech:
        # All we need to do is copy/rename the file
        shutil.copy2(f'{ifile}', f'{ofile}')
        logger.info('File copied to %s', ofile)
    else:
        # All we need to do is link file
        try:
            os.link(f'{ifile}', f'{ofile}')
            logger.info('File linked to %s', ofile)
        except PermissionError:
            shutil.copy2(f'{ifile}', f'{ofile}')
            logger.info('File copied to %s', ofile)



def main():
    '''
    Loop over all files found and copy, rename or link file to new folder,
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
        outpath_varn = f'{OUTPUT_PATH}/{TIME_RES[v_ind]}/{varn}'
        if not os.access(outpath_varn, os.F_OK):
            os.makedirs(outpath_varn)

        path_pattern = '%s/{institut}/{gcm}/{scenario}/{ensemble}/{rcm}/{nesting}/{t_freq}/{variable}/' %(INPUT_PATH)
        file_pattern = '{variable}_%s_{gcm}_{scenario}_{ensemble}_{rcm}_{nesting}_{t_freq}_*.nc' %(DOMAIN)

        ff = filefinder.FileFinder(path_pattern, file_pattern)
        files = ff.find_paths(variable=varn)
        files_prioritized = priority_filter(files, "t_freq", TRES_VALID)

        for path, meta in files_prioritized:

            if meta['rcm'] in ('BCCR-WRF381BF', 'BCCR-WRF381CF'):
                continue

            filelist = sorted(glob.glob(path))

            derived = bool(TIME_RES[v_ind] != meta['t_freq'])

            logger.info('%s files found, start processing:', len(filelist))
            # Loop over all files found in path
            for ifile in filelist:
                # Test if file can be read by cdo and if variable name in file is correct:
                try:
                    varname_file = cdo.showname(input=ifile)[0]
                    if varname_file != varn:
                        logger.warning('Variable name %s in file is not equal to %s!',
                                       varname_file, varn)
                        varnamech=True
                    else:
                        varnamech=False
                except PermissionError:
                    errmsg = f'PermissionError for file {ifile}, continuing.'
                    logger.error(errmsg)
                    continue
                except Exception:
                    errmsg = (f'Unknown error on file {ifile} using '
                              f' cdo.showname, continuing.')
                    logger.error(errmsg)
                    continue

                time_range = find_dates_in_file(ifile)

                metainfo = (f'{meta["gcm"]}_{meta["scenario"]}_'
                            f'{meta["ensemble"]}_{meta["rcm"]}_'
                            f'{meta["nesting"]}')

                filename = (f'{varn}_{DOMAIN}_{metainfo}_'
                            f'{TIME_RES[v_ind]}_{time_range}')

                if derived and (
                    TIME_RES[v_ind] == '1hr' and
                    meta['t_freq'] in ['3hr', '6hr', 'day'] or
                    (TIME_RES[v_ind] == '6hr' and
                    meta['t_freq'] == 'day')
                ):
                    infomsg=(f'TIME_RES[v_ind]: {TIME_RES[v_ind]}'
                             f' and meta["t_freq"]: {meta["t_freq"]}.'
                             f'We do not upsample, native time'
                             f' frequency will be processed!')
                    logger.info(infomsg)
                    filename = (f'{varn}_{DOMAIN}_{metainfo}_'
                                f'{meta["t_freq"]}_{time_range}')
                    derived=False

                ofile = f'{outpath_varn}/{meta["scenario"]}/{filename}.nc'
                # create output directory if does not exist yet
                final_out = f'{outpath_varn}/{meta["scenario"]}'
                if not os.access(final_out, os.F_OK):
                    os.makedirs(final_out)

                # Check if ofile already exists, create if does not exist
                # yet or OVERWRITE=True
                if os.path.isfile(ofile) and not OVERWRITE:
                    logger.info('File %s already exists.', ofile)
                else:
                    if varnamech:
                        corrnamefile=(f'{WORKDIR}/{varn}_correct_{metainfo}_'
                                      f'{TIME_RES[v_ind]}_{time_range}.nc')
                        cdo.chname(f'"{varname_file}"', varn,
                                   input=ifile, output=corrnamefile)
                        ifile=corrnamefile
                        logger.warning('varname corrected in file %s!', ifile)
                    try:
                        process_file(meta=meta, ifile=ifile, ofile=ofile,
                                     time_res_in=TIME_RES[v_ind],
                                     derived=derived, varnamech=varnamech)
                    except:
                        logger.error('Unexpected error:', sys.exc_info()[0])
                        raise
                        logger.error('File %s not written', ofile)
                    continue

    logger.info('All files found that could be processed were processed.')

if __name__ == '__main__':
    main()
