from random import random
from pyspark import SparkContext

def f(p):
	x = random() * 2 - 1
	y = random() * 2 - 1

	return 1 if x**2 + y**2 < 1 else 0

sc = SparkContext(appName="myPi")
n = 500000 
count = sc.parallelize(xrange(1,n+1)).map(f).reduce(lambda x,y: x+y)
print "Pi is roughly %f" % (4.0 * count / n)
sc.stop()
