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
    # 6-hourly variables should all be "time: point"
    ds = xr.open_dataset(infile)
    var = ds[varn]
    if var.attrs['cell_methods'] == "time: point":
        ds_6h = ds.resample(time='6h')
        ds_6h.to_netcdf(sixhour_file, format='NETCDF4_CLASSIC')

    else:
        errormsg = ('Wrong cell_method, should be point but is %s'
                    %(var.attrs['cell_methods']))
        logger.error(errormsg)
    ds.close()

def calc_to_day(varn, infile, day_file):
    ds = xr.open_dataset(infile)
    if varn == 'snd':
        # snow depth should be mean over time
        # check cell_methods
        snd = ds['snd']
        if snd.attrs['cell_methods'] == "time: mean":
            ds_day = ds.resample(time='1D').mean()
        else:
            errormsg = ('Wrong cell_method, should be mean but is %s'
                %(snd.attrs['cell_methods']))
            logger.error(errormsg)

    if varn == 'tasmin':
        # daily minimum
        tasmin = ds['tasmin']
        if tasmin.attrs['cell_methods'] == "time: minimum":
            ds_day = ds.resample(time='1D').min()
        else
            errormsg = ('Wrong cell_method, should be minimum but is %s'
                %(tasmin.attrs['cell_methods']))
            logger.error(errormsg)
    if varn == 'tasmax':
        # daily maximum
        tasmax = ds['tasmax']
        if tasmax.attrs['cell_methods'] == "time: maximum":
            ds_day = ds.resample(time='1D').min()
        else
            errormsg = ('Wrong cell_method, should be maximum but is %s'
                %(tasmax.attrs['cell_methods']))
            logger.error(errormsg)

    ds.to_netcdf(day_file, format='NETCDF4_CLASSIC')
    ds.close()
