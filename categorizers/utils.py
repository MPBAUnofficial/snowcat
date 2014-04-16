import utm
import numpy

def distance_2d(lon1, lat1, lon2, lat2):
    ym1, xm1, _, _ = utm.from_latlon(lat1, lon1)
    ym2, xm2, _, _ = utm.from_latlon(lat2, lon2)

    arr1 = numpy.array((xm1, ym1))
    arr2 = numpy.array((xm2, ym2))

    dist = numpy.linalg.norm(arr1-arr2)
    return dist


def distance_3d(lon1, lat1, h1, lon2, lat2, h2):
    ym1, xm1, _, _ = utm.from_latlon(lat1, lon1)
    ym2, xm2, _, _ = utm.from_latlon(lat2, lon2)

    arr1 = numpy.array((xm1, ym1, h1))
    arr2 = numpy.array((xm2, ym2, h2))

    dist = numpy.linalg.norm(arr1-arr2)
    return dist
