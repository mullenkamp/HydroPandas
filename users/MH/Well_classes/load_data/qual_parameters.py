"""
Author: matth
Date Created: 7/02/2017 2:35 PM
"""

from __future__ import division

qual_parameters = {
    # where there are multiple squalarc names they are added in order of their list
    'ecoli': {'standard_name': 'E. Coli',
              'unit': 'MPN/100mL',
              'sig_fig': 0,
              'squalarc_name': ('E coli',),
              'mol_mass': None,
              'charge': 0},

    'tcoli': {'standard_name': 'Total coliforms',
              'unit': 'MPN/100mL',
              'sig_fig': 0,
              'squalarc_name': ('Total Coliforms',),
              'mol_mass': None,
              'charge': 0},

    'NO3_N': {'standard_name': 'Nitrate nitrogen',
              'unit': 'mg/L',
              'sig_fig': 2,
              'squalarc_name': ('Nitrate Nitrogen',),
              'mol_mass': 14.0007,
              'charge': -1},

    'HCO3': {'standard_name': 'Bicarbonate (as HCO3-)',
             'unit': 'mg/L',
             'sig_fig': 1,
             'squalarc_name': ('Alkalinity as HCO3',),
             'mol_mass': 61.019,
             'charge': -1},

    'Cl': {'standard_name': 'Chloride',
           'unit': 'mg/L',
           'sig_fig': 1,
           'squalarc_name': ('Chloride',),
           'mol_mass': 35.45,
           'charge': -1},

    'SO4': {'standard_name': 'Sulphate',
            'unit': 'mg/L',
            'sig_fig': 1,
            'squalarc_name': ('Sulphate',),
            'mol_mass': 96.06,
            'charge': -2},

    'Ca': {'standard_name': 'Calcium',
           'unit': 'mg/L',
           'sig_fig': 2,
           'squalarc_name': ('Calcium',),
           'mol_mass': 40.078,
           'charge': 2},

    'Mg': {'standard_name': 'Magnesium',
           'sig_fig': 2,
           'unit': 'mg/L',
           'squalarc_name': ('Magnesium',),
           'mol_mass': 24.305,
           'charge': 2},

    'Na': {'standard_name': 'Sodium',
           'unit': 'mg/L',
           'sig_fig': 2,
           'squalarc_name': ('Sodium',),
           'mol_mass': 22.990,
           'charge': 1},

    'K': {'standard_name': 'Potassium',
          'unit': 'mg/L',
          'sig_fig': 2,
          'squalarc_name': ('Potassium',),
          'mol_mass': 39.098,
          'charge': 1},

    'NH4_N': {'standard_name': 'Ammonium Nitrogen',
              'unit': 'mg/L',
              'sig_fig': 2,
              'squalarc_name': ('Ammonia Nitrogen',),
              'mol_mass': 14.007,
              'charge': 1},

    'Fe': {'standard_name': 'Iron',
           'unit': 'mg/L',
           'sig_fig': 2,
           'squalarc_name': ('Iron',),
           'mol_mass': 55.845,
           'charge': 2},

    'Mn': {'standard_name': 'Manganese',
           'unit': 'mg/L',
           'sig_fig': 4,
           'squalarc_name': ('Manganese',),
           'mol_mass': None, #try not worrying about as it is quite small
           'charge': 0},

    'SiO2': {'standard_name': 'Silica (as SiO2)',
             'unit': 'mg/L',
             'sig_fig': 1,
             'squalarc_name': ('Silica',),
             'mol_mass': None,
             'charge': 0},

    'drp': {'standard_name': 'Dissolved reactive phosphorus',
            'unit': 'mg/L',
            'sig_fig': 4,
            'squalarc_name': ('Dissolved Reactive Phosphorus',),
            'mol_mass': None, # not worrying about as it is quite small
            'charge': 0},

    'hardness': {'standard_name': 'Total Hardness (as CaCO3)',
                 'unit': 'mg/L',
                 'sig_fig': 1,
                 'squalarc_name': ('Total Hardness as CaCO3',),
                 'mol_mass': None,
                 'charge': 0},

    'pH_field': {'standard_name': 'pH field',
                 'unit': '',
                 'sig_fig': 1,
                 'squalarc_name': ('pH Field',),
                 'mol_mass': None,
                 'charge': 0},

    'pH_lab': {'standard_name': 'pH lab',
               'unit': '',
               'sig_fig': 1,
               'squalarc_name': ('pH',),
               'mol_mass': None,
               'charge': 0},

    'conduct': {'standard_name': 'Conductivity (lab)',
                'unit': 'mS/m',
                'sig_fig': 1,
                'squalarc_name': ('Conductivity',),
                'mol_mass': None,
                'charge': 0},

    'DO': {'standard_name': 'Dissolved oxygen (field)',
           'unit': 'mg/L',
           'sig_fig': 2,
           'squalarc_name': ('Dissolved Oxygen',),
           'mol_mass': None,
           'charge': 0},

    'temp': {'standard_name': 'Temperature',
             'unit': 'degrees C',
             'sig_fig': 1,
             'squalarc_name': ('Water Temperature',),
             'mol_mass': None,
             'charge': 0},

    # other parameters
    'samp_id': {'standard_name': 'Sample ID',
                'unit': '',
                'squalarc_name': ('SampleID',),
                'mol_mass': None},

    'proj_id': {'standard_name': 'Project ID',
                'unit': '',
                'squalarc_name': ('Project ID',),
                'mol_mass': None}
}

squalarc_to_standard_names = {}
for key in qual_parameters.keys():
    squalarc_to_standard_names[qual_parameters[key]['squalarc_name'][0]] = key
squalarc_to_standard_names['datetime'] = 'qual_time'

expected_squalarc_unit = {
    'Ammonia Nitrogen': 'mg/L',
    'Ammonium Nitrogen': 'mg/L',
    'Alkalinity as HCO3': 'mg/L',
    'Alkalinity as CaCO3': 'mg/L',
    'Calcium': 'mg/L',
    'Chloride': 'mg/L',
    'Conductivity': 'mS/m',
    'Dissolved Oxygen': 'mg/L',
    'Dissolved Reactive Phosphorus': 'mg/L',
    'E coli': 'MPN/100mL',
    'Iron': 'mg/L',
    'Magnesium': 'mg/L',
    'Manganese': 'mg/L',
    'Nitrate Nitrogen': 'mg/L',
    'Nitrate Nitrogen Calc': 'mg/L',
    'Potassium': 'mg/L',
    'Silica': 'mg/L',
    'Sodium': 'mg/L',
    'Sulphate': 'mg/L',
    'Total Coliforms': 'MPN/100mL',
    'Total Hardness as CaCO3': 'mg/L',
    'Water Temperature': 'degrees C',
    'pH': '',
    'pH Field': ''}


def convert_qual_units(x, unit_in, unit_out):
    if unit_in == '' or unit_out == '':
        return x
    if unit_in == unit_out:
        return x
    try:
        if conv_mult[unit_out][unit_in] == 1 and conv_add[unit_out][unit_in] == 0:
            return x
        out = conv_mult[unit_out][unit_in] * x.astype(float) + conv_add[unit_out][unit_in]
    except KeyError as val:
        raise KeyError("{}: unit likely not added to conversion dictionary (qual_parameters.py)".format(val))
    return out


conv_mult = {
    'MPN/100mL': {'MPN / 100mL': 1},
    'mg/L': {'mg/L CaCO3': 1, 'mg/L HCO3': 1, 'mg/L P': 1, 'g/m3': 1, 'g/m3 as SiO2': 1, 'g/m3 as CaCO3': 1,
             'g/m3 at 25\xb0C': 1},
    'degrees C': {'\xb0C': 1},
    'mS/m': {}
}
conv_add = {
    'MPN/100mL': {'MPN / 100mL': 0},
    'mg/L': {'mg/L CaCO3': 0, 'mg/L HCO3': 0, 'mg/L P': 0, 'g/m3': 0, 'g/m3 as SiO2': 0, 'g/m3 as CaCO3': 0,
             'g/m3 at 25\xb0C': 0},
    'degrees C': {'\xb0C': 0},
    'mS/m': {}
}
