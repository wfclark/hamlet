import pandas as pd    

with open('/tmp/hurricane.dat','r') as f:
    next(f) # skip first row
    df = pd.DataFrame(l.rstrip().split() for l in f)

print(df) 
