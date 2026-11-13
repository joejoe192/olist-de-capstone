# once: pip install pyarrow
import pandas as pd
df = pd.DataFrame({
  "id":     [1, 2, 3, 4, 5],
  "state":  ["SP", "SP", "RJ", "MG", "RJ"],
  "amount": [25, 40, 15, 60, 30],
})
 
df.to_parquet("orders",
              partition_cols=["state"])
 
rj= pd.read_parquet("orders",
  filters=[("state", "==", "RJ")])
print(rj)
