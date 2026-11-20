INSERT INTO public.customers AS existing (id, name, city) VALUES
    (1, 'Ana Demo', 'Brasov'),
    (4, 'Vlad Demo', 'Timisoara')
ON CONFLICT (id) DO UPDATE SET city = EXCLUDED.city
WHERE existing.city IS DISTINCT FROM EXCLUDED.city;
