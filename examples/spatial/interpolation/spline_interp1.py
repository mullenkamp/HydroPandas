# -*- coding: utf-8 -*-

import numpy as np
from scipy import interpolate as interp
import matplotlib.pyplot as plt

# Define function over sparse 20x20 grid

x, y = np.mgrid[-1:1:20j, -1:1:20j]
z = (x+y) * np.exp(-6.0*(x*x+y*y))

plt.figure()
plt.pcolor(x, y, z)
plt.colorbar()
plt.title("Sparsely sampled function.")
plt.show()


xnew, ynew = np.mgrid[-1:1:70j, -1:1:70j]
tck = interp.bisplrep(x, y, z, s=0)
znew = interp.bisplev(xnew[:,0], ynew[0,:], tck)

plt.figure()
plt.pcolor(xnew, ynew, znew)
plt.colorbar()
plt.title("Interpolated function.")
plt.show()
