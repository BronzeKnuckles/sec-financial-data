DO $$ 
DECLARE 
    table_name text;
BEGIN 
    FOR table_name IN (SELECT tablename 
                       FROM pg_tables 
                       WHERE schemaname = 'public' AND tablename NOT LIKE 'main') 
    LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || table_name;
    END LOOP;
END $$;
