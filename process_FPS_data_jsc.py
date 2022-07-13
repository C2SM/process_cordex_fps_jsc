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
 * cut domain to allAlps region

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
input_path = "/home/rlorenz/fpscpcm/CORDEX-FPSCONV/output"

domain = "ALP-3"
scenarios = ["historical", "rcp85", "evaluation"]

#variables = ["tas", "pr", "hus850", "psl", "zg500", "zg850",
#             "tasmax", "tasmin", "snd", "orog"]
variables = ["tas"]
# time resolutions we want for each variable
#time_res = ["1hr", "1hr", "6hr", "6hr", "6hr", "6hr", "day", "day", "day", "fx"]
time_res = ["1hr"]
# valid time resolutions to look in if the one we want is not available
tres_valid = ["1hr", "3hr", "6hr", "day"]
# Flag for variables that need to be calculated to required frequency
derived = False

subdomain="allAlps"
lon1=5.3
lon2=16.3
lat1=43.3
lat2=48.5

overwrite = False # Flag to trigger overwriting of Files
output_path = "/home/rlorenz/fpscpcm/tmp/rlorenz/data/%s" %(subdomain)
workdir = "/home/rlorenz/fpscpcm/tmp/rlorenz/data/work"

# Create directories if needed
if (os.access(output_path, os.F_OK)==False):
    os.makedirs(output_path)
if (os.access(workdir, os.F_OK)==False):
    os.makedirs(workdir)


def get_folders(path):
    res=list()
    for folder in os.listdir(path):
        if os.path.isdir(os.path.join(path, folder)):
            yield folder
        res.append(folder)
    return res

def main():
    if (len(time_res) != len(variables)):
        errormsg = ('Lists time_res and variables do not have equal length, '
                    'please check those lists!')
        logging.error(errormsg)
        os.exit()

    # Find all institutes, models etc.
    institutes = get_folders("%s/%s/" %(input_path, domain))
    # remove ETHz from list because we only need ETHZ-2
    institutes.remove("ETHZ")
    logger.info('Institute folders found are:' %institutes)

    for inst in institutes:
        # Loop over variables, scenarios, models
        for scen in scenarios:
            if scen == "evaluation":
                gcm = "ECMWF-ERAINT"
            else:
                gcms = get_folders("%s/%s/%s/" %(input_path, domain, inst))
                try:
                    gcms.remove("ECMWF-ERAINT")
                except ValueError:
                    infomsg = ('No ECMWF-ERAINT folder to remove from list.')
                    logging.info(infomsg)
                # check if one gcm name found:
                if len(gcms) >= 1:
                    errormsg = ('More than one gcm folder found! %s' %(gcms))
                    logger.error(errormsg)
                else if len(gcms) == 1:
                    gcm=gcms[0]
                    logger.info('One gcm folder found: %s' %(gcm))
                else:
                    logger.warning('No gcm folder found, continuing')
                    continue

                # find rcm names
                rcms = glob.glob("%s/%s/%s/%s/%s/r*/" %(input_path, domain, inst, gcm, scen))
                # loop over rcms
                for rcm in rcms:
                    for v, varn in enumerate(variables):
                        file_path = ("%s/%s/%s/%s/%s/r*/%s/*/%s/%s/*.nc"
                            %(input_path, domain, inst, gcm, scen, rcm,
                              time_res[v], varn))
                        # Find all files matching pattern file_path
                        filelist = glob.glob(file_path)
                        if len(filelist) == 0:
                            infomsg = ('Filelist is empty, no files for path %s'
                                       %(file_path))
                            logging.warning(infomsg)
                            # Variables can be in different frequencies than the
                            # required ones, check other folders
                            for new_time_res in tres_valid:
                                check_path = ("%s/%s/%s/%s/%s/r*/%s/*/%s/%s/*.nc"
                                    %(input_path, domain, inst, gcm, scen, rcm,
                                      new_time_res, varn))
                                filelist = glob.glob(check_path)
                                if len(filelist) != 0:
                                    infomsg=('Variable %s found in frequency %s'
                                             %(varn, new_time_res))
                                    logger.info(infomsg)
                                    derived = True
                                    break
                                else:
                                    infomsg = ('Filelist still empty, no files for path %s'
                                               %(check_path))
                                    logging.info(infomsg)
                            warnmsg = ('No files found for %s' %(file_path))
                            logging.warning(warnmsg)
                            continue
                        else:
                            logging.info('%s files found, start processing:'
                                         %(len(filelist)))

                        # Loop over all files found in file_path
                        for ifile in filelist:
                            # extract name of file only without path
                            split_ifile = ifile.split('/')
                            fname = split_ifile[-1]

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
                                        %(varn, subdomain, gcm, scen, ensemble,
                                          rcm, nesting, time_res[v], time_range))
                            if derived:
                                if time_res[v] == '1h' and new_time_res in ['3h', '6h', 'day']:
                                    # we do not upsample, native time frequency will be processed
                                    filename = ("%s_%s_%s_%s_%s_%s_%s_%s_%s"
                                                %(varn, subdomain, gcm, scen, ensemble,
                                                  rcm, nesting, new_time_res, time_range))
                                tmp_file = ("%s/%s_%s_%s_%s_%s_%s_%s_%s_%s.nc"
                                            %(workdir, varn, subdomain, gcm, scen, ensemble,
                                              rcm, nesting, new_time_res, time_range))

                            logging.info('Filename is %s' %(filename))
                            ofile = "%s/%s.nc" %(output_path, filename)

                            # Check if ofile already exists, create if does not exist
                            # yet or overwrite=True
                            if os.path.isfile(ofile) and overwrite==False:
                                logging.info("File %s already exists." %ofile)
                            else:
                                if derived:
                                    cdo.sellonlatbox('%s,%s,%s,%s' %(lon1,lon2,lat1,lat2),
                                                     input=ifile, output=tmp_file)
                                    # Variable needs to be derived for the required time frequency
                                    if time_res[v] == '6h' and new_time_res == '1h':
                                        tf.calc_1h_to_6h(varn, tmp_file, ofile)
                                    if time_res[v] == 'day':
                                        tf.calc_to_day(varn, tmp_file, ofile)

                                    if (time_res[v] == '1h' and new_time_res in ['3h', '6h', 'day']) or
                                        (time_res[v] == '6h' and new_time_res == 'day'):
                                        # process native frequency, cannot increase frequency
                                        cdo.sellonlatbox('%s,%s,%s,%s' %(lon1,lon2,lat1,lat2),
                                                         input=ifile, output=ofile)
                                else:
                                    # All we need to do is cut the subdomain
                                    cdo.sellonlatbox('%s,%s,%s,%s' %(lon1,lon2,lat1,lat2),
                                                     input=ifile, output=ofile)


if __name__ == '__main__':
    main()
