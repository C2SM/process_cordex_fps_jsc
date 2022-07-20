#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Time-stamp: <2022-07-13 13:44:55 rlorenz>

(c) 2022 under a MIT License (https://mit-license.org)

Authors:
- Ruth Lorenz || ruth.lorenz@c2sm.ethz.ch

Abstract: Functions to calculate variable into different frequencies,
          e.g hourly to 6-hourly or daily

"""
import logging
import xarray as xr
import re

logger = logging.getLogger(__name__)

def calc_1h_to_3h(varn, infile, threehour_file):
    """
    Function to resample data from hourly to 3-hourly
    3-hourly variables cell_methods should all be "time: point"

    Parameters
    ----------
    varn: string
        variable name
    infile: string
        path to input file
    threehour_file: string
        path to new netcdf with 3-hourly values

    Returns
    -------
    Nothing, netcdf written to disk
    """
    logger.info(f'Calculating 3-hourly values from hourly')
    ds_in = xr.open_dataset(infile)
    var = ds_in[varn]
    if var.attrs['cell_methods'] == "time: point":
        ds_3h = ds_in.resample(time='3h').asfreq()
        ds_3h.to_netcdf(threehour_file, format='NETCDF4_CLASSIC')

    else:
        errormsg = (f'Wrong cell_method, should be point but is '
                    f'{var.attrs["cell_methods"]}')
        logger.error(errormsg)
    ds_in.close()


def calc_1h_to_6h(varn, infile, sixhour_file):
    """
    Function to resample data from hourly to 6-hourly
    6-hourly variables cell_methods should all be "time: point"

    Parameters
    ----------
    varn: string
        variable name
    infile: string
        path to input file
    sixhour_file: string
        path to new netcdf with 6-hourly values

    Returns
    -------
    Nothing, netcdf written to disk
    """
    logger.info(f'Calculating 6-hourly values from hourly')
    with xr.open_dataset(infile) as ds_in:
        try:
            var = ds_in[varn]
        except KeyError:
            try:
                new_key = re.split('(\d+)', varn)[0]
                var = ds_in[new_key]
                # ensure variable name is varn
                ds_in.rename({new_key: varn})

        if var.attrs['cell_methods'] == 'time: point':
            #ds_6h = ds_in.resample(time='6h').asfreq()
            ds_in.resample(time='6h').asfreq()
            #ds_6h.to_netcdf(sixhour_file, format='NETCDF4_CLASSIC')
            ds_in.to_netcdf(sixhour_file, format='NETCDF4_CLASSIC')
            logger.info(f'6-hourly file {sixhour_file} written.')
            #ds_6h.close()
        else:
            errormsg = (f'Wrong cell_method, should be point but is '
                        f'{var.attrs["cell_methods"]}')
            logger.error(errormsg)



def calc_to_day(varn, infile, day_file):
    """
    Function to calculate daily frequency VARIABLES
    either as mean, maximum, or minimum depending on cell_methods

    Parameters
    ----------
    varn: string
        variable name
    infile: string
        path to input file
    day_file: string
        path to new netcdf with daily values

    Returns
    -------
    Nothing, netcdf written to disk
    """
    logger.info(f'Calculating daily values')
    ds_in = xr.open_dataset(infile)
    if varn == 'snd':
        # snow depth should be mean over time
        # check cell_methods
        snd = ds_in['snd']
        if snd.attrs['cell_methods'] == "time: mean":
            ds_day = ds_in.resample(time='1D').mean()
        else:
            errormsg = (f'Wrong cell_method, should be mean but is '
                        f'{snd.attrs["cell_methods"]}')
            logger.error(errormsg)

    elif varn == 'tasmin':
        # daily minimum
        tasmin = ds_in['tasmin']
        if tasmin.attrs['cell_methods'] == "time: minimum":
            ds_day = ds_in.resample(time='1D').min()
        else:
            errormsg = (f'Wrong cell_method, should be minimum but is '
                        f'{tasmin.attrs["cell_methods"]}')
            logger.error(errormsg)
    elif varn == 'tasmax':
        # daily maximum
        tasmax = ds_in['tasmax']
        if tasmax.attrs['cell_methods'] == "time: maximum":
            ds_day = ds_in.resample(time='1D').max()
        else:
            errormsg = (f'Wrong cell_method, should be maximum but is '
                        f'{tasmax.attrs["cell_methods"]}')
            logger.error(errormsg)
    else:
        errormsg = ('Not implemented! variable needs to be snd, tasmax, or tasmin.')
        logger.error(errormsg)

    ds_day.to_netcdf(day_file, format='NETCDF4_CLASSIC')
    ds_in.close()
