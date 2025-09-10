def parse_price(price_str):
    if not price_str or price_str == '':
        return None
    price_str = str(price_str).strip()
    price_str = price_str.replace(',', '')
    price_str = price_str.replace('LKR', '').replace('Rs', '').replace('$', '').strip()
    try:
        return float(price_str)
    except (ValueError, TypeError):
        return None

# Test the function
test_prices = ['229,900.00', '174,900.00', '', '1,400,000.00', '99,000.00']
for price in test_prices:
    result = parse_price(price)
    print(f"'{price}' -> {result}")
