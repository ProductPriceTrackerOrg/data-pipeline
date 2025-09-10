from fact_product_price import parse_price

# Test the price parsing function
test_prices = [
    "229,900.00",
    "174,900.00", 
    "1,400,000.00",
    "99,000.00",
    "1629.00",
    "",
    None,
    "LKR 25,900.00",
    "Rs 15,000",
    "$100.50"
]

print("Testing price parsing function:")
for price in test_prices:
    result = parse_price(price)
    print(f"'{price}' -> {result}")
