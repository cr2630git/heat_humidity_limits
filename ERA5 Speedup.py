#!/usr/bin/env python
# coding: utf-8

# In[1]:


#2 sec
import dask, os, xarray as xr, numpy as np
import dask.array as da
import math


# In[2]:


#1 sec
f='/Volumes/ExternalDriveZ/ERA5_Hourly_Data/ttd_world_2010.grib'


# In[3]:


#2 sec
chunk=10
d=xr.open_dataset(f,chunks={"time":chunk},engine='cfgrib')


# In[4]:


#1 sec
@dask.delayed
def dewptcelsius(dewptk):
    result=dewptk-273.15
    return result


# In[ ]:


#5 min
d2m_temp=d.d2m
d2m=dask.compute(dewptcelsius(d2m_temp));
t2m=d.t2m


# In[ ]:


#Calculate q from Td
#Follows Matlab script calcqfromTd
@dask.delayed
def division1(d2m):
    numerator=np.multiply(17.67,d2m);
    denominator=d2m+243.5;
    vp=6.112*math.exp(np.divide(numerator,denominator))
    return vp

@dask.delayed
def division2(vp,sfcP):
    numerator=0.622*vp;
    denominator=sfcP-(0.378*vp);
    q=1000*np.divide(numerator,denominator);
    return q

sfcP=1013;
vp=dask.compute(division1(d2m));
q=dask.compute(division2(vp,sfcP));


# In[ ]:





# In[ ]:





# In[ ]:




