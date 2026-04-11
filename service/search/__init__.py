import psycopg
import duotypes as t
from database import api_tx
from typing import Tuple
from service.search.sql import (
    Q_CACHED_SEARCH,
    Q_QUIZ_SEARCH,
    Q_SEARCH_PREFERENCE,
    Q_UNCACHED_SEARCH_1,
    Q_UNCACHED_SEARCH_2,
    Q_FEED,
)
from datetime import datetime

Q_SEARCH_DIAGNOSTICS = """
WITH searcher AS (
    SELECT
        person.id,
        person.gender_id,
        person.verification_level_id,
        person.date_of_birth,
        person.coordinates,
        COALESCE(person.roles, ARRAY[]::TEXT[]) && ARRAY['admin', 'bot'] AS searcher_is_admin,
        COALESCE(
            (
                SELECT
                    1000 * distance
                FROM
                    search_preference_distance
                WHERE
                    person_id = %(searcher_person_id)s
            ),
            1e9
        ) AS distance_preference,
        COALESCE(
            (
                SELECT
                    min_age
                FROM
                    search_preference_age
                WHERE
                    person_id = %(searcher_person_id)s
            ),
            0
        ) AS min_age,
        COALESCE(
            (
                SELECT
                    max_age
                FROM
                    search_preference_age
                WHERE
                    person_id = %(searcher_person_id)s
            ),
            999
        ) AS max_age
    FROM
        person
    WHERE
        person.id = %(searcher_person_id)s
), prospect_flags AS (
    SELECT
        prospect.id,
        (
            NOT searcher.searcher_is_admin
        AND
            prospect.privacy_verification_level_id > searcher.verification_level_id
        ) AS hidden_by_their_privacy_settings,
        (
            (
                prospect.verification_required
            AND
                COALESCE(prospect.verification_level_id, 0) <= 1
            )
        OR
            COALESCE(prospect.waitlist_status, 'active') = 'pending'
        ) AS awaiting_review,
        (
            NOT searcher.searcher_is_admin
        AND NOT (
                prospect.date_of_birth <= (
                    CURRENT_DATE - INTERVAL '1 year' * searcher.min_age
                )::DATE
            AND
                prospect.date_of_birth > (
                    CURRENT_DATE - INTERVAL '1 year' * (searcher.max_age + 1)
                )::DATE
            )
        ) AS outside_your_age_range,
        (
            COALESCE(prospect.profile_status, 'active') <> 'active'
        OR
            COALESCE(prospect.waitlist_status, 'active') = 'blocked'
        ) AS profile_paused,
        (
            NOT searcher.searcher_is_admin
        AND (
                prospect.gender_id <> ALL(%(gender_preference)s::SMALLINT[])
            OR
                NOT ST_DWithin(
                    prospect.coordinates,
                    searcher.coordinates,
                    searcher.distance_preference
                )
            OR
                prospect.ethnicity_id NOT IN (
                    SELECT ethnicity_id
                    FROM search_preference_ethnicity
                    WHERE person_id = %(searcher_person_id)s
                )
            OR (
                EXISTS (
                    SELECT 1
                    FROM search_preference_city
                    WHERE person_id = %(searcher_person_id)s
                )
                AND LOWER(BTRIM(SPLIT_PART(COALESCE(prospect.location_long_friendly, ''), ',', 1))) <> LOWER(
                    (
                        SELECT city
                        FROM search_preference_city
                        WHERE person_id = %(searcher_person_id)s
                    )
                )
            )
            OR (
                EXISTS (
                    SELECT 1
                    FROM search_preference_state
                    WHERE person_id = %(searcher_person_id)s
                )
                AND LOWER(BTRIM(SPLIT_PART(COALESCE(prospect.location_long_friendly, ''), ',', 2))) <> LOWER(
                    (
                        SELECT state
                        FROM search_preference_state
                        WHERE person_id = %(searcher_person_id)s
                    )
                )
            )
            OR
                COALESCE(prospect.height_cm, 0) < COALESCE(
                    (
                        SELECT min_height_cm
                        FROM search_preference_height_cm
                        WHERE person_id = %(searcher_person_id)s
                    ),
                    0
                )
            OR
                COALESCE(prospect.height_cm, 999) > COALESCE(
                    (
                        SELECT max_height_cm
                        FROM search_preference_height_cm
                        WHERE person_id = %(searcher_person_id)s
                    ),
                    999
                )
            OR
                prospect.has_profile_picture_id NOT IN (
                    SELECT has_profile_picture_id
                    FROM search_preference_has_profile_picture
                    WHERE person_id = %(searcher_person_id)s
                )
            OR
                prospect.drinking_id NOT IN (
                    SELECT drinking_id
                    FROM search_preference_drinking
                    WHERE person_id = %(searcher_person_id)s
                )
            OR
                prospect.long_distance_id NOT IN (
                    SELECT long_distance_id
                    FROM search_preference_long_distance
                    WHERE person_id = %(searcher_person_id)s
                )
            OR
                prospect.relationship_status_id NOT IN (
                    SELECT relationship_status_id
                    FROM search_preference_relationship_status
                    WHERE person_id = %(searcher_person_id)s
                )
            OR (
                EXISTS (
                    SELECT 1
                    FROM search_preference_pioneer_status
                    WHERE person_id = %(searcher_person_id)s
                )
                AND COALESCE(prospect.pioneer_status, 'Unanswered') NOT IN (
                    SELECT status
                    FROM search_preference_pioneer_status
                    WHERE person_id = %(searcher_person_id)s
                )
            )
            OR
                prospect.has_kids_id NOT IN (
                    SELECT has_kids_id
                    FROM search_preference_has_kids
                    WHERE person_id = %(searcher_person_id)s
                )
            OR
                prospect.wants_kids_id NOT IN (
                    SELECT wants_kids_id
                    FROM search_preference_wants_kids
                    WHERE person_id = %(searcher_person_id)s
                )
            OR
                prospect.exercise_id NOT IN (
                    SELECT exercise_id
                    FROM search_preference_exercise
                    WHERE person_id = %(searcher_person_id)s
                )
            OR
                prospect.baptism_date > CURRENT_DATE - (
                    COALESCE(
                        (
                            SELECT min_baptism_years
                            FROM search_preference_baptism_years
                            WHERE person_id = %(searcher_person_id)s
                        ),
                        2
                    ) || ' years'
                )::INTERVAL
            OR (
                NOT EXISTS (
                    SELECT 1
                    FROM messaged
                    WHERE
                        subject_person_id = prospect.id
                    AND
                        object_person_id = %(searcher_person_id)s
                )
                AND prospect.hide_me_from_strangers
            )
            OR EXISTS (
                SELECT 1
                FROM skipped
                WHERE
                    subject_person_id = prospect.id
                AND
                    object_person_id = %(searcher_person_id)s
            )
            OR (
                EXISTS (
                    SELECT 1
                    FROM skipped
                    WHERE
                        subject_person_id = %(searcher_person_id)s
                    AND
                        object_person_id = prospect.id
                )
                AND NOT EXISTS (
                    SELECT 1
                    FROM search_preference_skipped
                    WHERE
                        person_id = %(searcher_person_id)s
                    AND skipped_id = 1
                )
            )
            OR (
                EXISTS (
                    SELECT 1
                    FROM messaged
                    WHERE
                        subject_person_id = %(searcher_person_id)s
                    AND
                        object_person_id = prospect.id
                )
                AND NOT EXISTS (
                    SELECT 1
                    FROM search_preference_messaged
                    WHERE
                        person_id = %(searcher_person_id)s
                    AND messaged_id = 1
                )
            )
            OR EXISTS (
                SELECT 1
                FROM (
                    SELECT *
                    FROM search_preference_answer
                    WHERE person_id = %(searcher_person_id)s
                ) AS pref
                LEFT JOIN answer ans
                ON ans.person_id = prospect.id
                AND ans.question_id = pref.question_id
                WHERE
                    (ans.answer IS NOT NULL AND ans.answer != pref.answer)
                OR
                    (ans.answer IS NULL AND pref.accept_unanswered = FALSE)
            )
        )
        ) AS hidden_by_your_filters
    FROM
        person AS prospect
    CROSS JOIN
        searcher
    WHERE
        prospect.id <> %(searcher_person_id)s
    AND
        prospect.activated
)
SELECT json_build_object(
    'admin_bypass_active',
    (SELECT searcher_is_admin FROM searcher),
    'hidden_by_your_filters',
    COUNT(*) FILTER (WHERE hidden_by_your_filters),
    'hidden_by_their_privacy_settings',
    COUNT(*) FILTER (WHERE hidden_by_their_privacy_settings),
    'awaiting_review',
    COUNT(*) FILTER (WHERE awaiting_review),
    'outside_your_age_range',
    COUNT(*) FILTER (WHERE outside_your_age_range),
    'profile_paused',
    COUNT(*) FILTER (WHERE profile_paused)
) AS j
FROM
    prospect_flags
"""


def _quiz_search_results(tx, searcher_person_id: int):
    params = dict(
        searcher_person_id=searcher_person_id,
    )

    return tx.execute(Q_QUIZ_SEARCH, params).fetchall()


def _uncached_search_results(
    tx,
    searcher_person_id: int,
    no: Tuple[int, int],
    gender_preference: list[int],
):
    n, o = no

    params = dict(
        searcher_person_id=searcher_person_id,
        n=n,
        o=o,
        gender_preference=gender_preference,
    )

    try:
        tx.execute(Q_UNCACHED_SEARCH_1, params)
        tx.execute(Q_UNCACHED_SEARCH_2, params)
        tx.execute(Q_CACHED_SEARCH, params)
        return tx.fetchall()
    except psycopg.errors.QueryCanceled:
        # The query probably timed-out because it was too specific
        return []


def _cached_search_results(tx, searcher_person_id: int, no: Tuple[int, int]):
    n, o = no

    params = dict(
        searcher_person_id=searcher_person_id,
        n=n,
        o=o
    )

    return tx.execute(Q_CACHED_SEARCH, params).fetchall()


def get_search_type(n: str | None, o: str | None):
    n_: int | None = n if n is None else int(n)
    o_: int | None = o if o is None else int(o)

    if n_ is not None and not n_ >= 0:
        raise ValueError('n must be >= 0')
    if o_ is not None and not o_ >= 0:
        raise ValueError('o must be >= 0')

    no = None if (n_ is None or o_ is None) else (n_, o_)

    if no is None:
        return 'quiz-search', no
    elif no[1] == 0:
        return 'uncached-search', no
    else:
        return 'cached-search', no


def get_search(
    s: t.SessionInfo,
    n: str | None,
    o: str | None,
):
    search_type, no = get_search_type(n, o)

    if no is not None and no[0] > 10:
        return 'n must be less than or equal to 10', 400

    if s.person_id is None:
        return '', 500

    params = dict(
        person_id=s.person_id,
        do_modify=False,
    )

    with api_tx('READ COMMITTED') as tx:
        tx.execute('SET LOCAL statement_timeout = 10000') # 10 seconds

        rows = tx.execute(Q_SEARCH_PREFERENCE, params).fetchall()

        gender_preference = [row['gender_id'] for row in rows]


        if search_type == 'quiz-search':
            return _quiz_search_results(
                tx=tx,
                searcher_person_id=s.person_id)

        elif search_type == 'uncached-search':
            return _uncached_search_results(
                tx=tx,
                searcher_person_id=s.person_id,
                no=no,
                gender_preference=gender_preference)

        elif search_type == 'cached-search':
            return _cached_search_results(
                tx=tx,
                searcher_person_id=s.person_id, no=no)

        else:
            raise Exception('Unexpected quiz type')


def get_feed(s: t.SessionInfo, before: datetime):
    params = dict(
        searcher_person_id=s.person_id,
        before=before,
    )

    with api_tx('READ COMMITTED') as tx:
        rows = tx.execute(Q_FEED, params).fetchall()

    return [row['j'] for row in rows]


def get_search_diagnostics(s: t.SessionInfo):
    params = dict(
        person_id=s.person_id,
        do_modify=False,
    )

    with api_tx('READ COMMITTED') as tx:
        rows = tx.execute(Q_SEARCH_PREFERENCE, params).fetchall()
        gender_preference = [row['gender_id'] for row in rows]

        diagnostics = tx.execute(
            Q_SEARCH_DIAGNOSTICS,
            dict(
                searcher_person_id=s.person_id,
                gender_preference=gender_preference,
            ),
        ).fetchone()

    return diagnostics['j']
