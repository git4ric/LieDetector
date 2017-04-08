# LieDetector
Detect differential functional dependency violations by storing minimal states in streaming data

# Prerequisites
Get Python 3.5.2
Execute pip3 install numpy, scipy, scikit-learn, pandas, nltk, jep
Get stopwords corpora from nltk

Edit /etc/environment to add:
LD_PRELOAD= /path/to/libpython3.5.so
LD_LIBRARY_PATH=/path/to/where/libjep.so/is
