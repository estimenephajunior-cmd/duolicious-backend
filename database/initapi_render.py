from pathlib import Path
import re


def create_dbs():
    # Render Postgres starts with a service-specific bootstrap database,
    # so we connect to that explicitly before creating duo_api.
    import os
    import psycopg
    import time

    DB_HOST = os.environ['DUO_DB_HOST']
    DB_PORT = os.environ['DUO_DB_PORT']
    DB_USER = os.environ['DUO_DB_USER']
    DB_PASS = os.environ['DUO_DB_PASS']
    DB_BOOTSTRAP_NAME = (
        os.environ.get('PGDATABASE')
        or os.environ.get('DUO_DB_NAME')
        or 'postgres'
    )

    _conninfo = psycopg.conninfo.make_conninfo(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        dbname=DB_BOOTSTRAP_NAME,
    )

    def create_db(name):
        for _ in range(10):
            try:
                with psycopg.connect(_conninfo, autocommit=True) as conn:
                    with conn.cursor() as cur:
                        cur.execute(f"CREATE DATABASE {name}")
                print(f'Created database: {name}')
                break
            except (
                psycopg.errors.DuplicateDatabase,
                psycopg.errors.UniqueViolation,
            ):
                print(f'Database already exists: {name}')
                break
            except psycopg.errors.OperationalError as e:
                print(
                    'Creating database(s) failed; waiting and trying again:',
                    e
                )
                time.sleep(1)

    create_db('duo_api')


def _safe_create_extension_statement(extension_name):
    return f"""
DO $render$
BEGIN
    CREATE EXTENSION IF NOT EXISTS {extension_name};
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Skipping unavailable extension {extension_name}: %', SQLERRM;
END
$render$;
""".strip()


def _render_safe_compute_personality_vectors_sql():
    return """
CREATE OR REPLACE FUNCTION compute_personality_vectors(
    new_presence_score INT[],
    new_absence_score INT[],
    old_presence_score INT[],
    old_absence_score INT[],
    cur_presence_score INT[],
    cur_absence_score INT[],
    cur_count_answers SMALLINT
)
RETURNS personality_vectors AS $$
DECLARE
    presence_score INT[] := cur_presence_score;
    absence_score INT[] := cur_absence_score;
    count_answers SMALLINT := cur_count_answers;
    dimensions INT := COALESCE(array_length(cur_presence_score, 1), 0);
    personality_raw FLOAT8[] := ARRAY[]::FLOAT8[];
    personality_out FLOAT4[] := ARRAY[]::FLOAT4[];
    new_excess INT;
    old_excess INT;
    numerator FLOAT8;
    denominator FLOAT8;
    trait_percentage FLOAT8;
    personality_value FLOAT8;
    personality_weight FLOAT8;
    normalizer FLOAT8 := 0;
    i INT;
BEGIN
    IF new_presence_score IS NOT NULL AND new_absence_score IS NOT NULL THEN
        FOR i IN 1..dimensions LOOP
            new_excess := LEAST(new_presence_score[i], new_absence_score[i]);
            presence_score[i] := COALESCE(presence_score[i], 0) + new_presence_score[i] - new_excess;
            absence_score[i] := COALESCE(absence_score[i], 0) + new_absence_score[i] - new_excess;
        END LOOP;

        count_answers := count_answers + 1;
    END IF;

    IF old_presence_score IS NOT NULL AND old_absence_score IS NOT NULL THEN
        FOR i IN 1..dimensions LOOP
            old_excess := LEAST(old_presence_score[i], old_absence_score[i]);
            presence_score[i] := COALESCE(presence_score[i], 0) - old_presence_score[i] + old_excess;
            absence_score[i] := COALESCE(absence_score[i], 0) - old_absence_score[i] + old_excess;
        END LOOP;

        count_answers := count_answers - 1;
    END IF;

    personality_weight := LN(LN(count_answers + 1) + 1) / LN(LN(251) + 1);
    personality_weight := GREATEST(0, LEAST(1, personality_weight));

    FOR i IN 1..dimensions LOOP
        numerator := COALESCE(presence_score[i], 0);
        denominator := COALESCE(presence_score[i], 0) + COALESCE(absence_score[i], 0);

        IF denominator <> 0 THEN
            trait_percentage := numerator / denominator;
        ELSE
            trait_percentage := 0.5;
        END IF;

        personality_value := (2 * trait_percentage - 1) * personality_weight;
        personality_raw := array_append(personality_raw, personality_value);
        normalizer := normalizer + power(personality_value, 2);
    END LOOP;

    personality_raw := array_append(personality_raw, 1e-5);
    normalizer := sqrt(normalizer + power(1e-5, 2));

    FOR i IN 1..array_length(personality_raw, 1) LOOP
        personality_out := array_append(
            personality_out,
            (personality_raw[i] / normalizer)::FLOAT4
        );
    END LOOP;

    RETURN (
        personality_out,
        presence_score,
        absence_score,
        count_answers
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
""".strip()


def _render_safe_init_sql():
    base_sql = (
        Path(__file__).resolve().parent.parent / 'init-api.sql'
    ).read_text()

    extension_replacements = {
        'pg_trgm': _safe_create_extension_statement('pg_trgm'),
        'plpython3u': _safe_create_extension_statement('plpython3u'),
        'postgis': _safe_create_extension_statement('postgis'),
        'vector': _safe_create_extension_statement('vector'),
        'btree_gist': _safe_create_extension_statement('btree_gist'),
        '"uuid-ossp"': _safe_create_extension_statement('"uuid-ossp"'),
    }

    for extension_name, replacement in extension_replacements.items():
        base_sql = base_sql.replace(
            f'CREATE EXTENSION IF NOT EXISTS {extension_name};',
            replacement,
        )

    base_sql = re.sub(
        r"CREATE OR REPLACE FUNCTION compute_personality_vectors\([\s\S]*?\$\$ LANGUAGE plpython3u IMMUTABLE LEAKPROOF PARALLEL SAFE;",
        _render_safe_compute_personality_vectors_sql(),
        base_sql,
        count=1,
    )

    # Render-managed Postgres does not permit non-superusers to define
    # LEAKPROOF functions, so strip that attribute from the bootstrap SQL.
    base_sql = re.sub(r"\s+LEAKPROOF\b", "", base_sql)

    return base_sql


def init_db():
    from database import api_tx
    from service import (
        api,
        location,
        person,
        question,
    )

    with api_tx() as tx:
        row = tx.execute("SELECT to_regclass('person')").fetchone()

    if row['to_regclass'] is None:
        print('Initializing api DB with Render-safe bootstrap...')
        with api_tx() as tx:
            tx.execute('SET LOCAL statement_timeout = 0')
            tx.execute(_render_safe_init_sql())
    else:
        print('Database already initialized')

    with open(api._migrations_sql_file, 'r') as f:
        migrations_sql_file = f.read()

    with open(api._email_domains_bad_file, 'r') as f:
        email_domains_bad_file = f.read()

    with open(api._email_domains_good_file, 'r') as f:
        email_domains_good_file = f.read()

    with open(api._banned_club_file, 'r') as f:
        banned_club_file = f.read()

    with api_tx() as tx:
        tx.execute('SET LOCAL statement_timeout = 300000')
        tx.execute(migrations_sql_file)

    with api_tx() as tx:
        tx.execute(email_domains_bad_file)

    with api_tx() as tx:
        tx.execute(email_domains_good_file)

    with api_tx() as tx:
        tx.execute('SET LOCAL statement_timeout = 300000')
        tx.execute(banned_club_file)

    api.migrate_unnormalized_emails()

    for init_func in [
        location.init_db,
        person.init_db,
        question.init_db,
    ]:
        init_func()

    print('Finished initializing api DB')


create_dbs()
init_db()
