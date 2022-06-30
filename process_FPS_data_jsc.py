#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
File Name : process_FPS_data_jsc.py
Author: Ruth Lorenz (ruth.lorenz@c2sm.ethz.ch)
Created: 23-06-2022
Modified:
Purpose: process high resolution FPS data at jsc to decrease data amount
 to copy to ethz
 steps:
 * cut domain to
 * process variables: pr, tas, orog

'''
import os
import logging
import glob
from cdo import *
cdo = Cdo()

logging.basicConfig(format='%(levelname)s %(asctime)s: %(message)s', level=logging.INFO)

####################
### Define input ###
####################
input_path = "/home/rlorenz/fpscpcm/CORDEX-FPSCONV/output"
#workdir = "fpscpcm/tmp/rlorenz/data/work"

domain = "ALP-3"
time_res = ["1hr", "1hr"]
variables = ["tas", "pr"]
scenarios = ["historical", "rcp85", "evaluation"]
#time_period_hist = ["1996-2005"]
#time_periods_scen = ["2041-2050", "2090-2099"]

subdomain="allAlps"
lon1=5.3
lon2=16.3
lat1=43.3
lat2=48.5

output_path = "/home/rlorenz/fpscpcm/tmp/rlorenz/data/%s" %(subdomain)

# Create directories if needed
if (os.access(output_path, os.F_OK)==False):
    os.makedirs(output_path)

#if (os.access(workdir, os.F_OK)==False):
#    os.makedirs(workdir)

institutes_gcm = ["AUTH-MC/NorESM1-ME", # rcp only, 2090-2099 only
                  # "BCCR/NorESM1-ME", # BCCR-WRF381CF historical only
                  # "BCCR/NorESM1-ME", # BCCR-WRF381DA rcp 1 year only
                  # "CICERO/", # evaluation only
                  # "CLMCom-CMCC/ICHEC-EC-EARTH",
                  # "CLMCom-WEGC/MPI-M-MPI-ESM-LR", #2090-2099 only, filenames tot_prec_209???.nc, evaluation missing
                  # "CLMcom-BTU/CNRM-CERFACS-CNRM-CM5",
                  # "CLMcom-DWD/MOHC-HadGEM2-ES", # evaluation missing
                  # "CLMcom-JLU/MPI-M-MPI-ESM-LR",
                  # "CLMcom-KIT/MPI-M-MPI-ESM-LR",
                  # "CNRM/CNRM-CERFACS-CNRM-CM5",
                  # "ETHZ-2/MPI-M-MPI-ESM-LR", # evaluation contains concatenated file
                  # "FZJ-IBG3/SMHI-EC-EARTH", #historical only
                  # "FZJ-IDL/SMHI-EC-Earth", # 2090-2099 only, evaluation missing
                  # "GERICS/MPI-M-MPI-ESM-LR", # 2041-2050 only
                  # "HCLIMcom/ICHEC-EC-EARTH",
                  # "ICTP/MOHC-HadGEM2-ES",
                  # "IDL/SMHI-EC-Earth", #rcm: FZJ-IDL-WRF381DA, 2089-2099 only, historical missing
                  # "IDL/SMHI-EC-Earth", #rcm: IDL-WRF381CA, 2089-2099 only, historical missing
                  # "IPSL/IPSL-CM5A-MR", # 2041-2050 only
                  # "IPSL-WEGC/IPSL-CM5A-MR", #historical only
                  # "KNMI/KNMI-EC-EARTH", # r04i1p1 -> 2090-2099, r13i1p1 -> 2041-2015
                  # "MOHC/HadGEM3-GC3.1-N512",
                  # "UCAN/", # evaluation only, rcm: UCAN-WRF381BI
                  # "UHOH/",# evaluation only, rcm: UHOH-WRF381BD
                  # "WEGC/IPSL-CM5-MR" #rcp only, 2090-2099 only
                  ]

rcms = ["AUTH-MC-WRF381D",
        # "BCCR-WRF381CF", "BCCR-WRF381DA", "CICERO-WRF381BJ",
        # "CLMcom-CMCC-CCLM5-0-9", "CLMCom-WEGC-CCLM5-0-09", "CLMcom-BTU-CCLM5-0-14",
        # "CLMcom-DWD-CCLM5-0-15", "CLMcom-JLU-CCLM5-0-15", "CLMcom-KIT-CCLM5-0-15",
        # "CNRM-AROME41t1", "COSMO-pompa", "FZJ-IBG3-WRF381CA",
        # "FZJ-IDL-WRF381DA", "GERICS-REMO2015", "HCLIMcom-HCLIM38-AROME",
        # "ICTP-RegCM4-7", "FZJ-IDL-WRF381DA", "IDL-WRF381CA", "IPSL-WRF381CE",
        # "IPSL-WEGC-WRF381DA", "HCLIM38h1-AROME", "HadREM3-RA-UM10.1",
        # "UCAN-WRF381BI", "UHOH-WRF381BD", "WEGC-WRF381DA"
        ]

def main():
    if (len(institutes_gcm) != len(rcms)):
        logging.error('institutes_gcm and rcms lists do not have equal length, please check those lists!')
        os.exit()

    for v, varn in enumerate(variables):
        for scen in scenarios:
            for r, inst_gcm in enumerate(institutes_gcm):
                if scen == "evaluation":
                    inst = inst_gcm.split("/")[0]
                    gcm = "ECMWF-ERAINT"
                else:
                    inst = inst_gcm.split("/")[0]
                    gcm = inst_gcm.split("/")[1]
                file_path = "%s/%s/%s/%s/%s/r*/%s/*/%s/%s/*.nc" %(input_path, domain, inst, gcm, scen, rcms[r], time_res[v], varn)

                filelist = glob.glob(file_path)
                if len(filelist) == 0:
                    logging.warning('Filelist is empty, no files for path %s' %(file_path))
                    if scen == "evaluation":
                        # try different rcm names, WRF runs have different rcm name for evaluation runs
                        file_path = "%s/%s/%s/%s/%s/r*/*/*/%s/%s/*.nc" %(input_path, domain, inst, gcm, scen, time_res[v], varn)
                        filelist = glob.glob(file_path)
                        if len(filelist) != 0:
                            #extract rcm names
                            rcm_path=filelist[0].split('/')[8]
                            logging.warning('RCM name extracted from path is %s which is not equal to %s' %(rcm_path, rcms[r]))
                            logging.info('%s files found, start processing:' %(len(filelist)))
                        else:
                            logging.warning('Filelist is still empty, no files for path %s' %(file_path))
                            continue
                    else:
                        continue
                else:
                    logging.info('%s files found, start processing:' %(len(filelist)))

                for ifile in filelist:
                    split_ifile = ifile.split('/')
                    fname = split_ifile[-1]

                    gcm_path = split_ifile[5]
                    if gcm_path != gcm:
                        errormsg = ('GCM name given in input (%s) is not equal'
                                    'GCM name extracted from path %s!'
                                    %(gcm, gcm_path))
                        logging.error(errormsg)

                    ensemble = split_ifile[7]
                    nesting = split_ifile[9]

                    dates = cdo.showdate(input=ifile)[0] # returns list with one string all dates
                    firstdate = dates.split(" ")[0]
                    firstdate_str = "".join(firstdate.split("-"))
                    lastdate = dates.split(" ")[-1]
                    lastdate_str = "".join(lastdate.split("-"))
                    time_range = "%s-%s" %(firstdate_str, lastdate_str)

                    if rcm_path:
                        filename = "%s_%s_%s_%s_%s_%s_%s_%s_%s" %(varn, subdomain, gcm, scen, ensemble, rcm_path, nesting, time_res, time_range)
                    else:
                        filename = "%s_%s_%s_%s_%s_%s_%s_%s_%s" %(varn, subdomain, gcm, scen, ensemble, rcms[r], nesting, time_res, time_range)
                    logger.info('Filename is %s' %(filename))
                    ofile = "%s/%s.nc" %(output_path, filename)

                    cdo.sellonlatbox('%s,%s,%s,%s' %(lon1,lon2,lat1,lat2), input=ifile, output=ofile)


if __name__ == '__main__':
    main()
