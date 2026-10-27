ORDERS = [
    {"order_id": "ORD-001", "customer_id": "CUST-42", "customer_name": "Alice Chen",
     "restaurant": "Tokyo Ramen House", "items": [{"name": "Tonkotsu Ramen", "price": 16.50, "qty": 1},
       {"name": "Gyoza (6pc)", "price": 8.00, "qty": 2}], "address": "123 Main St, Apt 4B",
     "payment_method": "credit_card", "simulate_rejection": False},

    {"order_id": "ORD-002", "customer_id": "CUST-77", "customer_name": "Bob Martinez",
     "restaurant": "Bella Italia", "items": [{"name": "Margherita Pizza", "price": 14.00, "qty": 1},
       {"name": "Caesar Salad", "price": 10.00, "qty": 1}, {"name": "Tiramisu", "price": 9.00, "qty": 1}],
     "address": "456 Oak Ave, Suite 200", "payment_method": "credit_card", "simulate_rejection": False},

    {"order_id": "ORD-003", "customer_id": "CUST-42", "customer_name": "Alice Chen",
     "restaurant": "Tokyo Ramen House", "items": [{"name": "Spicy Miso Ramen", "price": 17.50, "qty": 1}],
     "address": "123 Main St, Apt 4B", "payment_method": "credit_card", "simulate_rejection": True},
]

AVAILABLE_DRIVERS = [
    {"driver_id": "DRV-301", "name": "Carlos Rivera", "rating": 4.9, "vehicle": "Honda Civic"},

    {"driver_id": "DRV-302", "name": "Maria Santos", "rating": 4.85, "vehicle": "Toyota Corolla"},
    
    {"driver_id": "DRV-303", "name": "James Wilson", "rating": 4.7, "vehicle": "Ford Focus"},
]