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


def calc_1h_to_6h(varn, infile, sixhour_file):
    """
    Function to resample data from hourly to 6-hourly
    6-hourly variables cell_methods should all be "time: point"
    """
    ds_in = xr.open_dataset(infile)
    var = ds_in[varn]
    if var.attrs['cell_methods'] == "time: point":
        ds_6h = ds_in.resample(time='6h')
        ds_6h.to_netcdf(sixhour_file, format='NETCDF4_CLASSIC')

    else:
        errormsg = (f'Wrong cell_method, should be point but is '
                    f'{var.attrs["cell_methods"]}')
        logging.error(errormsg)
    ds_in.close()


def calc_to_day(varn, infile, day_file):
    """
    Function to calculate daily frequency VARIABLES
    either as mean, maximum, or minimum depending on cell_methods
    """

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
            logging.error(errormsg)

    if varn == 'tasmin':
        # daily minimum
        tasmin = ds_in['tasmin']
        if tasmin.attrs['cell_methods'] == "time: minimum":
            ds_day = ds_in.resample(time='1D').min()
        else:
            errormsg = (f'Wrong cell_method, should be minimum but is '
                        f'{tasmin.attrs["cell_methods"]}')
            logging.error(errormsg)
    if varn == 'tasmax':
        # daily maximum
        tasmax = ds_in['tasmax']
        if tasmax.attrs['cell_methods'] == "time: maximum":
            ds_day = ds_in.resample(time='1D').min()
        else:
            errormsg = (f'Wrong cell_method, should be maximum but is '
                        f'{tasmax.attrs["cell_methods"]}')
            logging.error(errormsg)

    ds_day.to_netcdf(day_file, format='NETCDF4_CLASSIC')
    ds_in.close()
