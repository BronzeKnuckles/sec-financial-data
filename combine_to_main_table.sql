-- Modify the fy column to be varchar(20)
ALTER TABLE main ALTER COLUMN fy TYPE character varying(20);

-- Insert data from each table with type casting
DO $$ 
DECLARE 
    table_name text;
    column_list text;
BEGIN 
    -- Get the column list from one of the tables using an alias for the table
    SELECT string_agg(c.column_name, ', ')
    INTO column_list
    FROM information_schema.columns c  -- Added alias 'c'
    WHERE c.table_schema = 'public' AND c.table_name = '_cu_bancorp';  -- Used alias 'c' to reference columns

    -- Replace 'fy' with 'fy::character varying(20)' in the column list
    column_list := REPLACE(column_list, 'fy', 'fy::character varying(20)');

    FOR table_name IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename NOT LIKE 'main') 
    LOOP
        EXECUTE 'INSERT INTO main SELECT ' || column_list || ' FROM ' || table_name;
        COMMIT;  -- Commit after each insert DOESNT WORK!
    END LOOP;
END $$;