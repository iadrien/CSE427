import matplotlib.pyplot as plt
import numpy as np


points= [[(1,2), (4,3)],    #group 1
         [(4,5),(9,9),(10,10)],     #group 2
         [(3,3), (3,4)]]        #group 3
c=[(1,2),(2,3), (4,4), (1,9)]   # centroid for group 1-3


#groups: all the clusters
#centroid: all the centroids
#random k <= 9
# need to add us map to visualizing points
def visualize_kmap(groups, centroid, map_skeleton=[]):

    #groups
    assert (len(groups)<=9)
    '''
    
    '''
    c_color=0
    for xi in range(len(groups)):
        for yi in range(len(groups[xi])):
            c_color=xi
            plt.plot(groups[xi][yi], groups[xi][yi], color='C'+str(c_color), marker='.', linestyle='', markersize=9)



    #centroid point will cover the previous plotted point if overlap
    for c in centroid:
        plt.plot(c[0], c[1], color='C'+str(c_color+1), marker='x', linestyle='', markersize=12)

    plt.xlabel('altitude')
    plt.ylabel('latitude')
    plt.show()

visualize_kmap(groups=points, centroid=c)