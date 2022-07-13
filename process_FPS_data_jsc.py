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
LOG_FILENAME = 'logfiles/logging_%s%s%s%s%s.out' %(
    local_time.tm_year, local_time.tm_mon, local_time.tm_hour,
    local_time.tm_min, local_time.tm_sec)
logging.basicConfig(filename=LOG_FILENAME,
                    filemode='w',
                    format='%(levelname)s %(asctime)s: %(message)s',
                    level=logging.INFO)

####################
### Define input ###
####################
INPUT_PATH = "/home/rlorenz/fpscpcm/CORDEX-FPSCONV/output"

DOMAIN = "ALP-3"
SCENARIOS = ["historical", "rcp85", "evaluation"]

#VARIABLES = ["tas", "pr", "hus850", "psl", "zg500", "zg850",
#             "tasmax", "tasmin", "snd", "orog"]
VARIABLES = ["tas"]
# time resolutions we want for each variable
#TIME_RES = ["1hr", "1hr", "6hr", "6hr", "6hr", "6hr", "day", "day", "day", "fx"]
TIME_RES = ["1hr"]
# valid time resolutions to look in if the one we want is not available
TRES_VALID = ["1hr", "3hr", "6hr", "day"]

SUBDOMAIN="allAlps"
LON1=5.3
LON2=16.3
LAT1=43.3
LAT2=48.5

OVERWRITE = False # Flag to trigger overwriting of Files
OUTPUT_PATH = "/home/rlorenz/fpscpcm/tmp/rlorenz/data/%s" %(SUBDOMAIN)
WORKDIR = "/home/rlorenz/fpscpcm/tmp/rlorenz/data/work"

# Create directories if needed
if not os.access(OUTPUT_PATH, os.F_OK):
    os.makedirs(OUTPUT_PATH)
if not os.access(WORKDIR, os.F_OK):
    os.makedirs(WORKDIR)


def get_folders(path):
    """
    Function to get all folder names in path

    input: path
    returns: list with all folder names
    """
    res=[]
    for folder in os.listdir(path):
        if os.path.isdir(os.path.join(path, folder)):
            yield folder
        res.append(folder)
    return res

def main():
    """
    Loop over all files found and cut to smaller domain,
    resample if necessary
    """
    if len(TIME_RES) != len(VARIABLES):
        errormsg = ('Lists TIME_RES and variables do not have equal length, '
                    'please check those lists!')
        logging.error(errormsg)
        os.exit()

    # Find all institutes, models etc.
    institutes = get_folders("%s/%s/" %(INPUT_PATH, DOMAIN))
    # remove ETHz from list because we only need ETHZ-2
    institutes.remove("ETHZ")
    logging.info('Institute folders found are: %s', institutes)

    for inst in institutes:
        # Loop over variables, SCENARIOS, models
        for scen in SCENARIOS:
            if scen == "evaluation":
                gcm = "ECMWF-ERAINT"
            else:
                gcms = get_folders("%s/%s/%s/" %(INPUT_PATH, DOMAIN, inst))
                try:
                    gcms.remove("ECMWF-ERAINT")
                except ValueError:
                    infomsg = ('No ECMWF-ERAINT folder to remove from list.')
                    logging.info(infomsg)
                # check if one gcm name found:
                if len(gcms) >= 1:
                    errormsg = ('More than one gcm folder found! %s', gcms)
                    logging.error(errormsg)
                elif len(gcms) == 1:
                    gcm = gcms[0]
                    logging.info('One gcm folder found: %s', gcm)
                else:
                    logging.warning('No gcm folder found, continuing')
                    continue

                # find rcm names
                rcms = glob.glob("%s/%s/%s/%s/%s/r*/" %(
                    INPUT_PATH, DOMAIN, inst, gcm, scen))
                # loop over rcms
                for rcm in rcms:
                    for v, varn in enumerate(VARIABLES):
                        file_path = ("%s/%s/%s/%s/%s/r*/%s/*/%s/%s/*.nc"
                            %(INPUT_PATH, DOMAIN, inst, gcm, scen, rcm,
                              TIME_RES[v], varn))
                        derived = False
                        # Find all files matching pattern file_path
                        filelist = glob.glob(file_path)
                        if len(filelist) == 0:
                            warnmsg = ('Filelist is empty, no files for path %s,'
                                       ' keep looking in other frequencies.',
                                       file_path)
                            logging.warning(warnmsg)
                            # Variables can be in different frequencies than the
                            # required ones, check other folders
                            tres_valid_rm = [n for n in TRES_VALID if n != TIME_RES[v]]
                            logging.info(tres_valid_rm)
                            for new_time_res in tres_valid_rm:
                                check_path = ("%s/%s/%s/%s/%s/r*/%s/*/%s/%s/*.nc"
                                    %(INPUT_PATH, DOMAIN, inst, gcm, scen, rcm,
                                      new_time_res, varn))
                                filelist = glob.glob(check_path)
                                if len(filelist) != 0:
                                    infomsg=('Variable %s found in frequency %s',
                                             varn, new_time_res)
                                    logging.info(infomsg)
                                    derived = True
                                    break

                            warnmsg = ('No files found for %s', file_path)
                            logging.warning(warnmsg)
                            continue

                        logging.info('%s files found, start processing:', len(filelist))
                        # Loop over all files found in file_path
                        for ifile in filelist:
                            split_ifile = ifile.split('/')

                            # find ensemble and nesting info from path for output filename
                            ensemble = split_ifile[10]
                            nesting = split_ifile[12]

                            # find time range included in file for output filename
                            # cdo showdate returns list with one string incl. all dates
                            dates = cdo.showdate(input=ifile)[0]
                            firstdate = dates.split(" ")[0]
                            firstdate_str = "".join(firstdate.split("-"))
                            lastdate = dates.split(" ")[-1]
                            lastdate_str = "".join(lastdate.split("-"))
                            time_range = "%s-%s" %(firstdate_str, lastdate_str)

                            filename = ("%s_%s_%s_%s_%s_%s_%s_%s_%s"
                                        %(varn, SUBDOMAIN, gcm, scen, ensemble,
                                          rcm, nesting, TIME_RES[v], time_range))
                            if derived:
                                if TIME_RES[v] == '1h' and new_time_res in ['3h', '6h', 'day']:
                                    # we do not upsample, native time frequency will be processed
                                    filename = ("%s_%s_%s_%s_%s_%s_%s_%s_%s"
                                                %(varn, SUBDOMAIN, gcm, scen, ensemble,
                                                  rcm, nesting, new_time_res, time_range))
                                tmp_file = ("%s/%s_%s_%s_%s_%s_%s_%s_%s_%s.nc"
                                            %(WORKDIR, varn, SUBDOMAIN, gcm, scen, ensemble,
                                              rcm, nesting, new_time_res, time_range))

                            logging.info('Filename is %s', filename)
                            ofile = "%s/%s.nc" %(OUTPUT_PATH, filename)

                            # Check if ofile already exists, create if does not exist
                            # yet or OVERWRITE=True
                            if os.path.isfile(ofile) and not OVERWRITE:
                                logging.info("File %s already exists.", ofile)
                            else:
                                if derived:
                                    cdo.sellonlatbox('%s,%s,%s,%s' %(LON1,LON2,LAT1,LAT2),
                                                     input=ifile, output=tmp_file)
                                    # Variable needs to be DERIVED for the required time frequency
                                    if TIME_RES[v] == '6h' and new_time_res == '1h':
                                        tf.calc_1h_to_6h(varn, tmp_file, ofile)
                                    if TIME_RES[v] == 'day':
                                        tf.calc_to_day(varn, tmp_file, ofile)

                                    if ((TIME_RES[v] == '1h' and new_time_res in
                                        ['3h', '6h', 'day']) or
                                        (TIME_RES[v] == '6h' and new_time_res == 'day')):
                                        # process native frequency, cannot increase frequency
                                        cdo.sellonlatbox('%s,%s,%s,%s' %(LON1,LON2,LAT1,LAT2),
                                                         input=ifile, output=ofile)
                                else:
                                    # All we need to do is cut the SUBDOMAIN
                                    cdo.sellonlatbox('%s,%s,%s,%s' %(LON1,LON2,LAT1,LAT2),
                                                     input=ifile, output=ofile)


if __name__ == '__main__':
    main()
