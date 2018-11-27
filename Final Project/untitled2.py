# -*- coding: utf-8 -*-
"""
Created on Mon Nov 26 23:09:12 2018

@author: Po Adrich
"""
import matplotlib
import matplotlib.pyplot as plt
import mpl_toolkits
from mpl_toolkits.basemap import Basemap
#sc = SparkContext()
    
#    mydata = sc.textFile('C:\Users\Po Adrich\Desktop\427\Final Project\lat_longs\lat_longs')
#    mydata = mydata.filter(lambda line : len(line) > 0)
#    mydata = mydata.map(lambda line : line.split(' '))
#    mydata = mydata.map(lambda xy: (xy[0], xy[1]))
#    filepath = 'Iliad.txt'  
with open('C:\\Users\\Po Adrich\\Desktop\\427\\Final Project\\lat_longs\\lat_longs') as fp:   
   line = fp.readline()
   cnt = 1
   while cnt <100:
       xy = line.split()
       plt.plot(xy[0], xy[1])
       
       line = fp.readline()
       cnt=cnt+1
#    latitude = mydata.map(lambda xy: xy[0]).collect()
#    latitude = [float(i) for i in latitude];
#    longitude = mydata.map(lambda xy: xy[1]).collect()
#    longitude = [float(i) for i in longitude];
#    
#    
#    
#    margin = 2
#    lat_min = min(latitude) - margin
#    lat_max = max(latitude) + margin
#    lon_min = min(longitude) - margin
#    lon_max = max(longitude) + margin
#	
#     create map using BASEMAP
#    m = Basemap(llcrnrlon=lon_min,
#                llcrnrlat=lat_min,
#                urcrnrlon=lon_max,
#                urcrnrlat=lat_max,
#                lat_0=(lat_max - lat_min)/2,
#                lon_0=(lon_max-lon_min)/2 + 90,
#                projection='cyl',
#                resolution = 'f',
#                area_thresh=0.001,
#                )
#
#
#    m.fillcontinents(color = 'grey',lake_color='grey')
#    # convert lat and lon to map projection coordinates
#    lons, lats = m(longitude, latitude)
#    # plot points as red dots
#    m.scatter(lons, lats, s=0.001, marker = 'o', color='r', zorder=5)
#    plt.show()
#    
#    #comma.saveAsTextFile("/loudacre/devicestatus_etl")
#    sc.stop()