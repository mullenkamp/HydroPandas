# -*- coding: utf-8 -*-
"""
Author: matth
Date Created: 8/09/2017 8:39 AM
"""

from __future__ import division
from core import env
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.model_run_tools import mod_gns_model
from users.MH.Waimak_modeling.models.extended_boundry.model_runs.model_run_tools import get_forward_wells, get_forward_rch

def setup_run_forward_run_mp (kwargs):
    setup_run_forward_run(**kwargs)

def setup_run_forward_run(model_id, name, base_dir, cc_inputs=None, pc5=False, wil_eff=1, naturalised=False,
                          full_abs=False, pumping_well_scale=1, full_allo=False):
    """
    sets up and runs a forward run with a number of options
    :param model_id: which NSMC version to user (see mod_gns_model)
    :param name: the name for the model (see mod_gns_model)
    :param base_dir: the folder for the model (see mod_gns_modle)
    :param cc_inputs: a dictionary of:  rcm: regional Climate model identifier,
                                        rcp: representetive carbon pathway identifier,
                                        period: e.g. 2010, 2020, ect,
                                        amag_type: the amalgamation type one of:
                                                        tym: ten year mean,
                                                        min: minimum annual average,
                                                        low_3_m: average of the 3 lowest consecutive years
    :param pc5: boolean if true use assumed PC5 efficency
    :param wil_eff: a factor (>0) which will be applied to the WIL scheme losses (e.g. .2 = 20% of optimised model value)
    :param naturalised: boolean if true run with a naturalised system no pumping, irrigation LSR impacts, WIL losses etc.
    :param full_abs: boolean, if true include full abstration for the wells
    :param pumping_well_scale: as factor that is applied to the pumping wells (e.g. .2 = 20%)
                               this factor is separate to any scaling due to additional demand under climate
                               change senarios (this is handled when getting the well data)
                               this factor applies to all wells
    :param full_allo: boolean if true use the full allocation of pumping
    :return:
    """
    # cc inputs are a dict
    if cc_inputs is None:
        cc_inputs = {'rcm':None, 'rcp':None, 'period':None, 'amag_type':None}
    if not isinstance(cc_inputs,dict):
        raise ValueError('incorrect type for cc_inputs {} expected dict or None'.format(type(cc_inputs)))

    well_data = get_forward_wells(full_abstraction=full_abs, cc_inputs=cc_inputs, naturalised=naturalised, full_allo=full_allo)
    well_data.loc[well_data.type=='well','flux'] *= pumping_well_scale
    well_data.loc[(well_data.type=='race') & (well_data.zone == 'n_wai'),'flux'] *= wil_eff

    rch = get_forward_rch(naturalised, pc5, **cc_inputs)

    # I'm assuming that the stream package will not change
    m = mod_gns_model(model_id, name, base_dir, well=well_data, recharge=rch)
    m.write_name_file()
    m.write_inputs()
    m.run_model()

