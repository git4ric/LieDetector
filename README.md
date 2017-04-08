# LieDetector
Detect differential functional dependency violations by storing minimal states in streaming data

# Prerequisites
pip install numpy, scipy, scikit-learn, jep

Edit /etc/environment to add:
LD_PRELOAD= /path/to/libpython2.7.so
LD_LIBRARY_PATH=/path/to/where/libjep.so/is
