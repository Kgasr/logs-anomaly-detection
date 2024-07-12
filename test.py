import pandas as pd

df = pd.DataFrame(columns=['A','B'])
dict = {'A':'a','B':'b'}
print(df)
df = df.\
    _append([df,pd.DataFrame([dict])], ignore_index=True)
print(df)