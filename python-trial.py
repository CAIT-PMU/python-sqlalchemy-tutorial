# Really simple code to make sure ide is set up correctly.

import numpy as np
import pandas as pd

zebras = ['1','2','3','4','5']

for i in zebras:
    a = np.random.random_integers(1,12,4)
    print (a)

df = pd.DataFrame(data=a)

df
