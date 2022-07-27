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
import re
import xarray as xr

logger = logging.getLogger(__name__)

def calc_1h_to_3h(varn, infile, threehour_file):
    """
    Function to resample data from hourly to 3-hourly
    3-hourly variables cell_methods should all be "time: point" or "time: mean"
    Process as freq or mean, depending on cell_methods

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
    logger.info('Calculating 3-hourly values from hourly')
    with xr.open_dataset(infile) as ds_in:
        try:
            var = ds_in[varn]
        except KeyError:
            try:
                new_key = re.split('(\d+)', varn)[0]
                var = ds_in[new_key]
                # ensure variable name is varn
                ds_in.rename({new_key: varn})
            except KeyError:
                logger.error('The variable name in the file is not known')
                logger.error(ds_in)

        if (var.attrs['cell_methods'] == 'time: point' or
            var.attrs['cell_methods'] == 'lev: mean'):
            ds_in.resample(time='3H', keep_attrs=True).asfreq()
        elif (var.attrs['cell_methods'] == 'time: mean'):
            ds_in.resample(time='3H', keep_attrs=True).mean()
        else:
            errormsg = ('Wrong cell_method, should be time: point (or lev: mean)'
                        f' or time: mean but is {var.attrs["cell_methods"]}')
            logger.error(errormsg)
        ds_in.to_netcdf(threehour_file, format='NETCDF4_CLASSIC')
        logger.info(f'3-hourly file {threehour_file} written.')

def calc_1h_to_6h(varn, infile, sixhour_file):
    """
    Function to resample data from hourly to 6-hourly
    6-hourly variables cell_methods should all be "time: point" or "time: mean"
    Process as freq or mean, depending on cell_methods

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
    logger.info('Calculating 6-hourly values from hourly')
    with xr.open_dataset(infile) as ds_in:
        try:
            var = ds_in[varn]
        except KeyError:
            try:
                new_key = re.split('(\d+)', varn)[0]
                var = ds_in[new_key]
                # ensure variable name is varn
                ds_in.rename({new_key: varn})
            except KeyError:
                logger.error('The variable name in the file is not known')
                logger.error(ds_in)

        if (var.attrs['cell_methods'] == 'time: point' or
            var.attrs['cell_methods'] == 'lev: mean'):
            ds_in.resample(time='6H', keep_attrs=True).asfreq()
        elif (var.attrs['cell_methods'] == 'time: mean'):
            ds_in.resample(time='6H', keep_attrs=True).mean()
        else:
            errormsg = ('Wrong cell_method, should be time: point (or lev: mean)'
                        f' or time: mean but is {var.attrs["cell_methods"]}')
            logger.error(errormsg)
        ds_in.to_netcdf(sixhour_file, format='NETCDF4_CLASSIC')
        logger.info(f'6-hourly file {sixhour_file} written.')

def calc_3h_to_6h(varn, infile, sixhour_file):
    """
    Function to resample data from 3-hourly to 6-hourly
    6-hourly variables cell_methods should all be "time: point" or "time: mean"
    Process as freq or mean, depending on cell_methods

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
    logger.info('Calculating 6-hourly values from 3-hourly')
    with xr.open_dataset(infile) as ds_in:
        try:
            var = ds_in[varn]
        except KeyError:
            try:
                new_key = re.split('(\d+)', varn)[0]
                var = ds_in[new_key]
                # ensure variable name is varn
                ds_in.rename({new_key: varn})
            except KeyError:
                logger.error('The variable name in the file is not known')
                logger.error(ds_in)

        if (var.attrs['cell_methods'] == 'time: point' or
            var.attrs['cell_methods'] == 'lev: mean'):
            ds_in.resample(time='6H', keep_attrs=True).asfreq()
        elif (var.attrs['cell_methods'] == 'time: mean'):
            ds_in.resample(time='6H', keep_attrs=True).mean()
        else:
            errormsg = ('Wrong cell_method, should be time: point (or lev: mean)'
                        f' or time: mean but is {var.attrs["cell_methods"]}')
            logger.error(errormsg)
        ds_in.to_netcdf(sixhour_file, format='NETCDF4_CLASSIC')
        logger.info(f'6-hourly file {sixhour_file} written.')

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
    logger.info('Calculating daily values')
    with xr.open_dataset(infile) as ds_in:
        if varn == 'snd':
            # snow depth should be mean over time
            # check cell_methods
            snd = ds_in['snd']
            if snd.attrs['cell_methods'] == "time: mean":
                ds_in.resample(time='1D', keep_attrs=True).mean()
            else:
                errormsg = ('Wrong cell_method, should be mean but is '
                            f'{snd.attrs["cell_methods"]}')
                logger.error(errormsg)

        elif varn == 'tasmin':
            # daily minimum
            tasmin = ds_in['tasmin']
            if tasmin.attrs['cell_methods'] == "time: minimum":
                ds_in.resample(time='1D', keep_attrs=True).min()
            else:
                errormsg = ('Wrong cell_method, should be minimum but is '
                            f'{tasmin.attrs["cell_methods"]}')
                logger.error(errormsg)
        elif varn == 'tasmax':
            # daily maximum
            tasmax = ds_in['tasmax']
            if tasmax.attrs['cell_methods'] == "time: maximum":
                ds_in.resample(time='1D', keep_attrs=True).max()
            else:
                errormsg = ('Wrong cell_method, should be maximum but is '
                            f'{tasmax.attrs["cell_methods"]}')
                logger.error(errormsg)
        else:
            errormsg = ('Not implemented! variable needs to be snd, tasmax, or tasmin.')
            logger.error(errormsg)

        ds_in.to_netcdf(day_file, format='NETCDF4_CLASSIC')
