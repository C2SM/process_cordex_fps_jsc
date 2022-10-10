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

# set global option to keep attributes
xr.set_options(keep_attrs=True)

def ensure_no_fill_value_in_coords(varn, ds):
    """
    Function to create encoding dictionary for writing netcdfs which have no added
    _FillValue = NaN in coordinate variables but identical _FillValue to original
    dataset

    Parameters
    ----------
    varn: string
        variable name
    ds: xarray dataset
        dataset to be written to netcdf

    Return:
    encoding: dict
        dict holding all _FillValues
    """

    encoding = {}
    try:
        fill_value = ds[varn].encoding['_FillValue']
    except KeyError:
        fill_value = 1.e20
    all_keys=list(ds.keys())
    all_keys.remove(varn)
    for k in all_keys:
        encoding[k] = {'_FillValue': None}
    for c in list(ds.coords):
        encoding[c] = {'_FillValue': None}
    encoding[varn] = {'_FillValue': fill_value}
    return encoding


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
                ds_in = ds_in.rename({new_key: varn})
            except KeyError:
                logger.error('The variable name in the file is not known')
                logger.error(ds_in)

        if (var.attrs['cell_methods'] == 'time: point' or
            var.attrs['cell_methods'] == 'time:point' or
            var.attrs['cell_methods'] == 'lev: mean'):
            ds_3h = ds_in.resample(time='3H').asfreq()
        elif var.attrs['cell_methods'] == 'time: mean':
            ds_3h = ds_in.resample(time='3H').mean()
        else:
            errormsg = ('Wrong cell_method, should be time: point (or lev: mean)'
                        f' or time: mean but is {var.attrs["cell_methods"]}')
            logger.error(errormsg)

        # _FillValue for variable should be same as before, all other (coordinate)
        # variables should have no _FillValue
        encoding = ensure_no_fill_value_in_coords(varn, ds_3h)
        ds_3h.attrs['frequency'] = '3hr'
        ds_3h.to_netcdf(threehour_file, format='NETCDF4_CLASSIC', encoding=encoding)
        logger.info('3-hourly file %s written.', threehour_file)

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
                ds_in = ds_in.rename({new_key: varn})
            except KeyError:
                logger.error('The variable name in the file is not known')
                logger.error(ds_in)

        if (var.attrs['cell_methods'] == 'time: point' or
            var.attrs['cell_methods'] == 'time:point' or
            var.attrs['cell_methods'] == 'lev: mean'):
            ds_6h = ds_in.resample(time='6H').asfreq()
        elif var.attrs['cell_methods'] == 'time: mean':
            ds_6h = ds_in.resample(time='6H').mean()
        else:
            errormsg = ('Wrong cell_method, should be time: point (or lev: mean)'
                        f' or time: mean but is {var.attrs["cell_methods"]}')
            logger.error(errormsg)

        encoding = ensure_no_fill_value_in_coords(varn, ds_6h)
        ds_6h.attrs['frequency'] = '6hr'
        ds_6h.to_netcdf(sixhour_file, format='NETCDF4_CLASSIC', encoding=encoding)
        logger.info('6-hourly file %s written.', sixhour_file)

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
                ds_in = ds_in.rename({new_key: varn})
            except KeyError:
                logger.error('The variable name in the file is not known')
                logger.error(ds_in)

        if (var.attrs['cell_methods'] == 'time: point' or
            var.attrs['cell_methods'] == 'time:point' or
            var.attrs['cell_methods'] == 'lev: mean'):
            ds_6h = ds_in.resample(time='6H').asfreq()
        elif var.attrs['cell_methods'] == 'time: mean':
            ds_6h = ds_in.resample(time='6H').mean()
        else:
            errormsg = ('Wrong cell_method, should be time: point (or lev: mean)'
                        f' or time: mean but is {var.attrs["cell_methods"]}')
            logger.error(errormsg)

        encoding = ensure_no_fill_value_in_coords(varn, ds_6h)
        ds_6h.attrs['frequency'] = '6hr'
        ds_6h.to_netcdf(sixhour_file, format='NETCDF4_CLASSIC', encoding=encoding)
        logger.info('6-hourly file %s written.', sixhour_file)

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
            snd = ds_in['snd']
            ds_day = ds_in.resample(time='1D').mean()
        elif varn == 'snw':
            snw = ds_in['snw']
            ds_day = ds_in.resample(time='1D').mean()
        elif varn == 'tasmin':
            # daily minimum
            tasmin = ds_in['tasmin']
            ds_day = ds_in.resample(time='1D').min()
            if tasmin.attrs['cell_methods'] not "time: minimum":
                logger.warning('Wrong cell_method, should be minimum but is '
                               f'{tasmin.attrs["cell_methods"]} in {infile}.')
        elif varn == 'tasmax':
            # daily maximum
            tasmax = ds_in['tasmax']
            ds_day = ds_in.resample(time='1D').max()
            if tasmax.attrs['cell_methods'] not "time: maximum":
                logger.warning('Wrong cell_method, should be maximum but is '
                               f'{tasmax.attrs["cell_methods"]} in {infile}'.)
        elif varn == 'mrro':
            # runoff should be mean over time
            # check cell_methods
            mrro = ds_in['mrro']
            ds_day = ds_in.resample(time='1D').mean()
            if mrro.attrs['cell_methods'] not "time: mean":
                logger.warning('Wrong cell_method, should be mean but is '
                               f'{mrro.attrs["cell_methods"]} in {file}')
        else:
            errormsg = ('Not implemented! variable needs to be snd, snw, mrro, tasmax, or tasmin.')
            logger.error(errormsg)

        # _FillValue for variable should be same as before, all other (coordinate)
        # variables should have no _FillValue
        encoding = ensure_no_fill_value_in_coords(varn, ds_day)
        ds_day.attrs['frequency'] = 'day'
        ds_day.to_netcdf(day_file, format='NETCDF4_CLASSIC', encoding=encoding)
        logger.info('daily file %s written.', day_file)
