INSERT INTO public.customers (id, name, city) VALUES
    (1, 'Ana Demo', 'Bucharest'),
    (2, 'Mihai Demo', 'Cluj-Napoca'),
    (3, 'Elena Demo', 'Iasi')
ON CONFLICT (id) DO NOTHING;

INSERT INTO public.orders (id, customer_id, amount, status) VALUES
    (101, 1, 45.90, 'paid'),
    (102, 2, 99.50, 'paid'),
    (103, 1, 12.00, 'pending'),
    (104, 3, 29.00, 'cancelled')
ON CONFLICT (id) DO NOTHING;
