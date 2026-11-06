from datetime import datetime


def calc_delivery_days(purchase, delivered):
    """Return days between purchase and delivery."""
    days = (delivered - purchase).days
    return days


# example dates
start = datetime(2018, 1, 1)
end = datetime(2018, 1, 8)

# call the function
result = calc_delivery_days(start, end)
print(result)   # 7