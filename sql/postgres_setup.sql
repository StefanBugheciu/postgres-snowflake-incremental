CREATE TABLE IF NOT EXISTS public.customers (
    id bigint PRIMARY KEY,
    name text NOT NULL,
    city text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT statement_timestamp()
);

CREATE TABLE IF NOT EXISTS public.orders (
    id bigint PRIMARY KEY,
    customer_id bigint NOT NULL REFERENCES public.customers(id),
    amount numeric(12,2) NOT NULL CHECK (amount >= 0),
    status text NOT NULL CHECK (status IN ('paid', 'pending', 'cancelled')),
    updated_at timestamptz NOT NULL DEFAULT statement_timestamp()
);

CREATE INDEX IF NOT EXISTS customers_updated_idx ON public.customers (updated_at, id);
CREATE INDEX IF NOT EXISTS orders_updated_idx ON public.orders (updated_at, id);

CREATE OR REPLACE FUNCTION public.stamp_pipeline_row()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    write_time timestamptz := clock_timestamp();
BEGIN
    -- One timestamp for the demo statement, assigned after the table write lock.
    -- Use a transaction-local setting to reuse it across rows in this statement.
    IF current_setting('pipeline.statement_start', true) IS DISTINCT FROM statement_timestamp()::text THEN
        PERFORM set_config('pipeline.statement_start', statement_timestamp()::text, true);
        PERFORM set_config('pipeline.write_time', write_time::text, true);
    END IF;
    NEW.updated_at := current_setting('pipeline.write_time')::timestamptz;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER customers_stamp
BEFORE INSERT OR UPDATE ON public.customers
FOR EACH ROW EXECUTE FUNCTION public.stamp_pipeline_row();

CREATE OR REPLACE TRIGGER orders_stamp
BEFORE INSERT OR UPDATE ON public.orders
FOR EACH ROW EXECUTE FUNCTION public.stamp_pipeline_row();
