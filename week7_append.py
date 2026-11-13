import json

new_order = {"order_id": "aa11bb22", "status": "pending"}

with open("week7test.jsonl", "a") as f:
      f.write(json.dumps(new_order) + "\n")

with open("week7test.json") as f:
      orders = json.load(f)
  
orders.append({"order_id": "aa11bb22", "status": "pending", "items": []})

with open("week7test.json", "w") as f:
      json.dump(orders, f, indent=2)