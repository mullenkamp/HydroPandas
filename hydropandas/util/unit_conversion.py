# -*- coding: utf-8 -*-
"""
Created on Mon Jan  8 08:02:26 2018

@author: MichaelEK
"""
import pint

ureg = pint.UnitRegistry()


def to_units(self, hydro_id_units, inplace=False):
    """
    Function to convert the tsdata in a hpd from the stored units to new units. It modifies the values in place.

    Parameters
    ----------
    hydro_id_units: dict
        A dict of hydro_id to the proposed units as a str or a ureg.Quantity.
    inplace: bool
        Should the original object be changed?

    Returns
    -------
    Hydro if inplace is False, else None
    """
    if not isinstance(hydro_id_units, dict):
        raise TypeError('hydro_id_units must be a dict of hydro_id to units')

    ## Prepare input units
    exist_units = self.units
    val1 = hydro_id_units[list(hydro_id_units.keys())[0]]
    if isinstance(val1, ureg.Quantity):
        hydro_id_units1 = {i: hydro_id_units[i] for i in hydro_id_units}
    elif isinstance(val1, str):
        hydro_id_units1 = {i: ureg(hydro_id_units[i]) for i in hydro_id_units}
    else:
        raise TypeError('The values of the hydro_id_dict must be a str or ureg.Quantity')
    hydro_id_units2 = {i: hydro_id_units1[i] for i in hydro_id_units1 if i in self.hydro_id}
    hydro_id_units3 = {i: hydro_id_units2[i] for i in hydro_id_units2 if hydro_id_units2[i] != exist_units[i]}

    if inplace:
        hydro = self
    else:
        hydro = self.copy()

    ## Convert data
    for hi in hydro_id_units3:
        val = hydro.tsdata.loc[hi, 'value']
        val1 = val.values * exist_units[hi]
        val1.ito(hydro_id_units3[hi])
        hydro.tsdata.loc[hi, 'value'] = val1

    ## Update units dict
    hydro.units.update(hydro_id_units3)
    if hasattr(hydro, '_base_stats'):
        delattr(hydro, '_base_stats')

    if not inplace:
        return hydro

