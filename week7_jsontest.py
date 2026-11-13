#Array list in python/access by position: x[0] 
#Object │ {"a": 1}   │ dict   │ key: x["a"]  
import json

with open("week7test.json") as f:
      orders = json.load(f)

print(orders[1]["order_id"])
print(orders[1]["items"][0]["price"])

for order in orders:
      total = sum(item["price"] for item in order["items"])
      print(order["order_id"], order["status"], total)