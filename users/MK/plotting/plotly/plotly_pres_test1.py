# -*- coding: utf-8 -*-
import plotly.plotly as py
import plotly.presentation_objs as pres

filename = 'simple-pres'
markdown_string = """
# slide 1
There is only one slide.

---
# slide 2
Again, another slide on this page.

"""

my_pres = pres.Presentation(markdown_string)
pres_url_0 = py.presentation_ops.upload(my_pres, filename)


