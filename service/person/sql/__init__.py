import constants
from commonsql import Q_COMPUTED_FLAIR

MAX_CLUB_SEARCH_RESULTS = 20

# All users get top-tier access without payment.
CLUB_QUOTA = 100

# Payment and donation model removed; no funding table required.

Q_UPDATE_ANSWER = """
WITH
old_answer AS (
    SELECT question_id, answer
    FROM answer
    WHERE
        person_id = %(person_id)s AND
        question_id = COALESCE(
            %(question_id_to_insert)s,
            %(question_id_to_delete)s
        )
), deleted_answer AS (
    DELETE FROM answer
    WHERE
        person_id = %(person_id)s AND
        question_id = %(question_id_to_delete)s
), new_answer AS (
    INSERT INTO answer (
        person_id,
        question_id,
        answer,
        public_
    )
    SELECT
        %(person_id)s,
        %(question_id_to_insert)s,
        %(answer)s,
        %(public)s
    WHERE %(question_id_to_insert)s::SMALLINT IS NOT NULL
    ON CONFLICT (person_id, question_id) DO UPDATE SET
        answer  = EXCLUDED.answer,
        public_ = EXCLUDED.public_
    RETURNING
        question_id,
        answer
), updated_personality_vectors AS (
    SELECT
        (compute_personality_vectors(
            new_vectors.presence_score,
            new_vectors.absence_score,
            old_vectors.presence_score,
            old_vectors.absence_score,
            cur_vectors.presence_score,
            cur_vectors.absence_score,
            cur_vectors.count_answers
        )).*
    FROM (
        SELECT (answer_score_vectors(question_id, answer)).*
        FROM new_answer
        LIMIT 1
    ) AS new_vectors FULL OUTER JOIN (
        SELECT (answer_score_vectors(question_id, answer)).*
        FROM old_answer
        LIMIT 1
    ) AS old_vectors ON TRUE FULL OUTER JOIN (
        SELECT presence_score, absence_score, count_answers
        FROM person where id = %(person_id)s
        LIMIT 1
    ) AS cur_vectors ON TRUE
)
UPDATE person
SET
    personality    = updated_personality_vectors.personality,
    presence_score = updated_personality_vectors.presence_score,
    absence_score  = updated_personality_vectors.absence_score,
    count_answers  = updated_personality_vectors.count_answers
FROM updated_personality_vectors
WHERE person.id = %(person_id)s
"""

Q_ADD_YES_NO_COUNT = """
UPDATE question
SET
    count_yes = count_yes + %(add_yes)s,
    count_no  = count_no  + %(add_no)s
WHERE
    id = %(question_id)s
"""

Q_SELECT_PERSONALITY = """
SELECT
    CASE
        WHEN %(topic)s::TEXT = 'Big 5' AND trait.name = 'Introversion/Extraversion'
        THEN 'Extraversion'
        ELSE trait.name
    END                               AS trait_name,
    CASE
        WHEN %(topic)s::TEXT = 'Big 5' AND trait.name = 'Introversion/Extraversion'
        THEN NULL
        ELSE trait.min_label
    END                               AS trait_min_label,
    CASE
        WHEN %(topic)s::TEXT = 'Big 5' AND trait.name = 'Introversion/Extraversion'
        THEN NULL
        ELSE trait.max_label
    END                               AS trait_max_label,
    trait.description                 AS trait_description,
    person_trait.name                 AS person_name,
    person_trait.tiny_id              AS person_id,
    ROUND(100 * person_trait.ratio)   AS person_percentage,
    prospect_trait.name               AS prospect_name,
    ROUND(100 * prospect_trait.ratio) AS prospect_percentage,
    CASE
        WHEN %(prospect_person_id)s IS NOT NULL
        THEN COALESCE(prospect_trait.ratio, 0)
        ELSE COALESCE(person_trait.ratio, 0)
    END AS position
FROM
    trait
LEFT JOIN
    (
        SELECT
            id,
            tiny_id,
            name,
            (trait_ratio(presence_score, absence_score, 5000)).*
        FROM
            person
        WHERE
            id = %(person_id_as_int)s::INT
        OR
            tiny_id = %(person_id_as_str)s::TEXT
    ) AS person_trait
ON
    person_trait.trait_id = trait.id
LEFT JOIN
    (
        SELECT
            id,
            name,
            (trait_ratio(presence_score, absence_score, 5000)).*
        FROM person
        WHERE id = %(prospect_person_id)s
    ) AS prospect_trait
ON
    prospect_trait.trait_id = trait.id
WHERE
    trait.id IN (
        SELECT trait_id
        FROM trait_topic
        WHERE
            trait_topic.name = %(topic)s OR
            %(topic)s::TEXT IS NULL
    )
ORDER BY
    position DESC,
    trait_name ASC
"""

_OTP_CTE = """
WITH random_otp AS (
    SELECT LPAD(FLOOR(RANDOM() * (10e5 + 1))::TEXT, 6, '0') AS otp
), zero_otp AS (
    SELECT '000000' AS otp
), is_registered AS (
    SELECT 1 WHERE     EXISTS (SELECT 1 FROM person WHERE normalized_email = %(normalized_email)s)
), is_unregistered AS (
    SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM person WHERE normalized_email = %(normalized_email)s)
), domain AS (
    SELECT
        SUBSTRING(%(email)s FROM POSITION('@' IN %(email)s) + 1) AS domain
), otp AS (
    SELECT
        CASE
        WHEN
                EXISTS (SELECT 1 FROM domain WHERE domain = 'example.com')
            AND
                EXISTS (SELECT 1 FROM is_registered)
            OR
                EXISTS (SELECT 1 FROM domain WHERE domain = 'example.com')
            AND
                %(is_dev)s
        THEN
            (SELECT otp FROM zero_otp)
        ELSE
            (SELECT otp FROM random_otp)
        END AS otp
    WHERE
        NOT EXISTS (
            SELECT
                1
            FROM
                banned_person
            WHERE
                normalized_email = %(normalized_email)s
            AND
                expires_at > NOW()
            OR
                ip_address = %(ip_address)s
            AND
                expires_at > NOW()
        )
    AND
        NOT EXISTS (
            SELECT
                1
            FROM
                bad_email_domain
            JOIN
                domain
            ON
                domain.domain = bad_email_domain.domain
            JOIN
                is_unregistered
            ON
                TRUE
        )
)
"""

Q_INSERT_DUO_SESSION = f"""
{_OTP_CTE}
INSERT INTO duo_session (
    session_token_hash,
    person_id,
    email,
    referral_code_id,
    otp,
    ip_address,
    device_fingerprint
)
SELECT
    %(session_token_hash)s,
    (
        SELECT
            id
        FROM
            person
        WHERE
            normalized_email = %(normalized_email)s
        ORDER BY
            email = %(email)s DESC,
            email
        LIMIT 1
    ),
    %(email)s,
    %(referral_code_id)s,
    otp,
    %(ip_address)s,
    %(device_fingerprint)s
FROM
    otp
RETURNING
    otp
"""

Q_UPDATE_OTP = f"""
{_OTP_CTE}
UPDATE
    duo_session
SET
    otp = otp.otp,
    otp_expiry = NOW() + INTERVAL '10 minutes'
FROM
    otp
WHERE
    session_token_hash = %(session_token_hash)s
RETURNING
    otp.otp
"""

Q_MAYBE_DELETE_ONBOARDEE = """
WITH valid_session AS (
    UPDATE duo_session
    SET signed_in = TRUE
    WHERE
        session_token_hash = %(session_token_hash)s AND
        otp = %(otp)s AND
        otp_expiry > NOW()
    RETURNING email
)
DELETE FROM onboardee
WHERE email IN (SELECT email FROM valid_session)
RETURNING email
"""

Q_MAYBE_SIGN_IN = f"""
WITH valid_session AS (
    UPDATE
        duo_session
    SET
        signed_in = TRUE
    WHERE
        session_token_hash = %(session_token_hash)s
    AND
        otp = %(otp)s
    AND
        otp_expiry > NOW()
    RETURNING
        person_id,
        email
), existing_person AS (
    UPDATE
        person
    SET
        activated = TRUE,
        sign_in_count = sign_in_count + 1,
        sign_in_time = NOW()
    FROM
        valid_session
    WHERE
        person.id = person_id
    RETURNING
        person.id,
        person.uuid AS person_uuid,
        person.unit_id,
        person.name,
        person.last_nag_time,
        person.sign_up_time,
        person.count_answers
), new_onboardee AS (
    INSERT INTO onboardee (
        email
    )
    SELECT
        email
    FROM
        valid_session
    WHERE NOT EXISTS (SELECT 1 FROM existing_person)
)
SELECT
    person_id,
    person_uuid,
    TRUE AS has_gold,
    (SELECT name FROM unit WHERE id = existing_person.unit_id) AS units,
    FALSE AS do_show_donation_nag,
    NULL::TEXT AS estimated_end_date,
    existing_person.name AS name
FROM
    valid_session
LEFT JOIN
    existing_person
ON
    valid_session.person_id = existing_person.id
"""

Q_DELETE_ONBOARDEE_PHOTO = """
WITH deleted_uuid AS (
    DELETE FROM
        onboardee_photo
    WHERE
        email = %(email)s AND
        position = %(position)s
    RETURNING
        uuid
)
INSERT INTO undeleted_photo (
    uuid
)
SELECT
    uuid
FROM
    deleted_uuid
"""

Q_DELETE_DUO_SESSION = """
DELETE FROM duo_session
WHERE session_token_hash = %(session_token_hash)s
"""

Q_FINISH_ONBOARDING = f"""
WITH valid_session AS (
    SELECT referral_code_id
    FROM duo_session
    WHERE session_token_hash = %(session_token_hash)s
), onboardee_location AS (
    SELECT
        short_friendly,
        long_friendly,
        country,
        verification_required
    FROM
        location
    ORDER BY coordinates <-> (
        SELECT coordinates
        FROM onboardee
        WHERE email = %(email)s
    )
    LIMIT 1
), new_person AS (
    INSERT INTO person (
        email,
        normalized_email,
        name,
        date_of_birth,
        baptism_date,
        coordinates,
        gender_id,
        about,
        congregation_id,
        has_profile_picture_id,
        unit_id,
        intros_notification,
        privacy_verification_level_id,
        verification_required,
        location_short_friendly,
        location_long_friendly,
        roles,
        referred_by_code_id
    ) SELECT
        email,
        %(normalized_email)s,
        name,
        date_of_birth,
        baptism_date,
        coordinates,
        gender_id,
        COALESCE(about, ''),
        congregation_id,
        CASE
            WHEN EXISTS (SELECT 1 FROM onboardee_photo WHERE email = %(email)s)
            THEN 1
            ELSE 2
        END AS has_profile_picture_id,
        (
            SELECT id
            FROM unit
            WHERE name IN (
                CASE
                    WHEN country IN ('United States', 'United Kingdom')
                    THEN 'Imperial'
                    ELSE 'Metric'
                END
            )
        ) AS unit_id,
        2 AS intros_notification,
        CASE
            WHEN RANDOM() < 0.5 THEN 1
            ELSE 3
        END AS privacy_verification_level_id,
        TRUE AS verification_required,
        short_friendly,
        long_friendly,
        CASE
            WHEN NOT EXISTS (SELECT 1 FROM person)
            THEN ARRAY['bot']::TEXT[]
            ELSE ARRAY[]::TEXT[]
        END AS roles,
        valid_session.referral_code_id
    FROM
        onboardee,
        valid_session,
        onboardee_location
    WHERE email = %(email)s
    RETURNING
        id,
        person.uuid,
        email,
        unit_id,
        coordinates,
        date_of_birth,
        referred_by_code_id,
        person.name
), best_age AS (
    WITH new_person_age AS (
        SELECT
            EXTRACT(YEAR FROM AGE(date_of_birth)) AS age
        FROM
            new_person
    ), unbounded_age_preference AS (
        SELECT
            round(age - 2 - (age - 18) / 5.0) AS min_age,
            round(age + 2 + (age - 18) / 5.0) AS max_age
        FROM
            new_person_age
    )
    SELECT
        CASE WHEN min_age <= 18 THEN NULL ELSE min_age END AS min_age,
        CASE WHEN max_age >= 99 THEN NULL ELSE max_age END AS max_age
    FROM
        unbounded_age_preference
), best_distance AS (
    -- Use a binary search to compute the "furthest distance" search preference
    -- which causes search results to contain as close as possible to 2000 users
    WITH RECURSIVE t(dist, cnt, iters) AS (
        VALUES
            (    0.0,    0.0, 0),
            (10000.0,  1.0e9, 0)
        UNION ALL (
            WITH two_closest AS (
                SELECT
                    dist,
                    cnt,
                    iters
                FROM
                    t
                ORDER BY
                    iters DESC,
                    ABS(cnt - 2000),
                    dist
                LIMIT 2
            ), midpoint AS (
                SELECT
                    AVG(dist) AS dist,
                    MAX(iters) AS iters
                FROM
                    two_closest
            ), limited_search_results AS (
                SELECT
                    midpoint.dist AS dist,
                    midpoint.iters AS iters
                FROM
                    person AS prospect, midpoint
                WHERE
                    activated
                AND
                    -- The prospect meets the new_person's gender preference
                    prospect.gender_id IN (
                        SELECT gender_id
                        FROM onboardee_search_preference_gender AS preference
                        WHERE preference.email = (SELECT email FROM new_person)
                    )
                AND
                    -- The prospect meets the new_person's location preference
                    ST_DWithin(
                        prospect.coordinates,
                        (SELECT coordinates FROM new_person),
                        midpoint.dist * 1000
                    )
                AND
                    -- The new_person meets the prospect's gender preference
                    EXISTS (
                        SELECT 1
                        FROM search_preference_gender AS preference
                        WHERE
                            preference.person_id = prospect.id AND
                            preference.gender_id = (SELECT gender_id FROM new_person)
                    )
                AND
                    -- The new_person meets the prospect's location preference
                    ST_DWithin(
                        prospect.coordinates,
                        (SELECT coordinates FROM new_person),
                        (
                            SELECT
                                COALESCE(
                                    (
                                        SELECT
                                            1000 * distance
                                        FROM
                                            person
                                        JOIN
                                            search_preference_distance
                                        ON
                                            person.id = person_id
                                        WHERE
                                            person.id = prospect.id
                                    ),
                                    1e9
                                )
                        )
                    )
                AND
                   -- The prospect meets the new_person's age preference
                   EXISTS (
                        SELECT 1
                        FROM best_age AS preference
                        WHERE
                            prospect.date_of_birth <= (
                                CURRENT_DATE -
                                INTERVAL '1 year' *
                                COALESCE(preference.min_age, 0)
                            )
                        AND
                            prospect.date_of_birth > (
                                CURRENT_DATE -
                                INTERVAL '1 year' *
                                (COALESCE(preference.max_age, 999) + 1)
                            )
                    )
                AND
                   -- The new_person meets the prospect's age preference
                   EXISTS (
                        SELECT 1
                        FROM search_preference_age AS preference
                        WHERE
                            preference.person_id = prospect.id
                        AND
                            (SELECT date_of_birth FROM new_person) <= (
                                CURRENT_DATE -
                                INTERVAL '1 year' *
                                COALESCE(preference.min_age, 0)
                            )
                        AND
                            (SELECT date_of_birth FROM new_person) > (
                                CURRENT_DATE -
                                INTERVAL '1 year' *
                                (COALESCE(preference.max_age, 999) + 1)
                            )
                    )
                LIMIT
                    2000 * 2
            ), evaluated_midpoint AS (
                SELECT
                    MAX(dist) AS dist,
                    COUNT(*) AS cnt,
                    MAX(iters) AS iters
                FROM
                    limited_search_results
            ), points AS (
                SELECT dist, cnt, iters FROM evaluated_midpoint
                UNION
                SELECT dist, cnt, iters FROM two_closest
            )
            SELECT dist, cnt, iters + 1 FROM points WHERE iters < 5
        )
    )
    SELECT
        LEAST(dist, 10000) AS dist,
        cnt
    FROM
        t
    ORDER BY
        iters DESC,
        dist
    LIMIT
        1
    OFFSET
        1
), new_photo AS (
    INSERT INTO photo (
        person_id,
        position,
        uuid,
        blurhash,
        hash
    )
    SELECT
        new_person.id,
        position,
        onboardee_photo.uuid,
        onboardee_photo.blurhash,
        onboardee_photo.hash
    FROM onboardee_photo
    JOIN new_person
    ON onboardee_photo.email = new_person.email
), updated_session AS (
    UPDATE duo_session
    SET person_id = new_person.id
    FROM new_person
    WHERE duo_session.email = new_person.email
), p1 AS (
    INSERT INTO search_preference_gender (person_id, gender_id)
    SELECT new_person.id, gender_id
    FROM onboardee_search_preference_gender
    JOIN new_person
    ON new_person.email = onboardee_search_preference_gender.email
), p2 AS (
    INSERT INTO search_preference_orientation (person_id, orientation_id)
    SELECT new_person.id, orientation.id
    FROM new_person, orientation
), p3 AS (
    INSERT INTO search_preference_age (person_id, min_age, max_age)
    SELECT new_person.id, min_age, max_age
    FROM new_person, best_age
), p4 AS (
    INSERT INTO search_preference_distance (person_id, distance)
    SELECT
        new_person.id,
        CASE
            WHEN best_distance.cnt < 500
            THEN NULL

            WHEN best_distance.dist > 9000  -- It's over 9000
            THEN 9000

            ELSE best_distance.dist
        END AS distance
    FROM new_person, best_distance
), p5 AS (
    INSERT INTO search_preference_height_cm (person_id, min_height_cm, max_height_cm)
    SELECT new_person.id, NULL, NULL
    FROM new_person
), p6 AS (
    INSERT INTO search_preference_has_profile_picture (person_id, has_profile_picture_id)
    SELECT new_person.id, yes_no.id
    FROM new_person, yes_no
), p7 AS (
    INSERT INTO search_preference_looking_for (person_id, looking_for_id)
    SELECT new_person.id, looking_for.id
    FROM new_person, looking_for
), p8 AS (
    INSERT INTO search_preference_smoking (person_id, smoking_id)
    SELECT new_person.id, yes_no_optional.id
    FROM new_person, yes_no_optional
), p9 AS (
    INSERT INTO search_preference_drinking (person_id, drinking_id)
    SELECT new_person.id, frequency.id
    FROM new_person, frequency
), p10 AS (
    INSERT INTO search_preference_drugs (person_id, drugs_id)
    SELECT new_person.id, yes_no_optional.id
    FROM new_person, yes_no_optional
), p11 AS (
    INSERT INTO search_preference_long_distance (person_id, long_distance_id)
    SELECT new_person.id, yes_no_optional.id
    FROM new_person, yes_no_optional
), p12 AS (
    INSERT INTO search_preference_relationship_status (person_id, relationship_status_id)
    SELECT new_person.id, relationship_status.id
    FROM new_person, relationship_status
), p13 AS (
    INSERT INTO search_preference_has_kids (person_id, has_kids_id)
    SELECT new_person.id, yes_no_optional.id
    FROM new_person, yes_no_optional
), p14 AS (
    INSERT INTO search_preference_wants_kids (person_id, wants_kids_id)
    SELECT new_person.id, yes_no_maybe.id
    FROM new_person, yes_no_maybe
), p15 AS (
    INSERT INTO search_preference_exercise (person_id, exercise_id)
    SELECT new_person.id, frequency.id
    FROM new_person, frequency
), p16 AS (
    INSERT INTO search_preference_religion (person_id, religion_id)
    SELECT new_person.id, religion.id
    FROM new_person, religion
), p17 AS (
    INSERT INTO search_preference_star_sign (person_id, star_sign_id)
    SELECT new_person.id, star_sign.id
    FROM new_person, star_sign
), p18 AS (
    INSERT INTO search_preference_messaged (person_id, messaged_id)
    SELECT new_person.id, yes_no.id
    FROM new_person, yes_no
    WHERE yes_no.name = 'Yes'
), p19 AS (
    INSERT INTO search_preference_skipped (person_id, skipped_id)
    SELECT new_person.id, yes_no.id
    FROM new_person, yes_no
    WHERE yes_no.name = 'No'
), p20 AS (
    INSERT INTO search_preference_ethnicity (person_id, ethnicity_id)
    SELECT new_person.id, ethnicity.id
    FROM new_person, ethnicity
), deleted_onboardee AS (
    DELETE FROM onboardee
    WHERE email = %(email)s
)
SELECT
    new_person.id AS person_id,
    new_person.uuid AS person_uuid,
    (SELECT name FROM unit WHERE unit.id = new_person.unit_id) AS units,
    false AS do_show_donation_nag,
    NULL::TEXT AS estimated_end_date,
    new_person.name AS name
FROM
    new_person
"""

Q_SELECT_PROSPECT_PROFILE = f"""
WITH prospect AS (
    SELECT
        *,

        (
            SELECT EXTRACT(YEAR FROM AGE(prospect.date_of_birth))::SMALLINT
            WHERE prospect.show_my_age
        ) AS age,

        (
            SELECT prospect.location_short_friendly
            WHERE prospect.show_my_location
        ) AS location,

        (
            ROUND(EXTRACT(EPOCH FROM NOW() - last_online_time))
        ) AS seconds_since_last_online,

        (
            ROUND(EXTRACT(EPOCH FROM NOW() - sign_up_time))
        ) AS seconds_since_sign_up
    FROM
        person AS prospect
    LEFT JOIN LATERAL (
        SELECT
            EXISTS (
                SELECT
                    1
                FROM
                    messaged
                WHERE
                    messaged.subject_person_id = prospect.id
                AND
                    messaged.object_person_id = %(person_id)s
            ) AS prospect_has_messaged_person
    ) AS prospect_has_messaged_person
    ON
        TRUE
    WHERE
        uuid = uuid_or_null(%(prospect_uuid)s::TEXT)
    AND
        activated
    AND (
            NOT prospect.hide_me_from_strangers
        OR
            prospect_has_messaged_person
    )
    AND (
        prospect.privacy_verification_level_id <= (
            SELECT
                verification_level_id
            FROM
                person
            WHERE
                id = %(person_id)s
        )
        OR
            EXISTS (
                SELECT 1
                FROM messaged
                WHERE
                    messaged.subject_person_id = prospect.id
                AND
                    messaged.object_person_id = %(person_id)s
            )
    )
    AND
        NOT EXISTS (
            SELECT 1
            FROM skipped
            WHERE
                subject_person_id = prospect.id AND
                object_person_id  = %(person_id)s
        )
    OR

    -- User is viewing their own profile
        uuid = uuid_or_null(%(prospect_uuid)s::TEXT)
    AND
        prospect.id = %(person_id)s
), updated_visited AS (
    INSERT INTO visited (
        subject_person_id,
        object_person_id,
        updated_at,
        invisible
    )
    SELECT
        %(person_id)s AS subject_person_id,
        prospect.id AS object_person_id,
        now() AS updated_at,
        (
            SELECT
                person.browse_invisibly OR
                person.hide_me_from_strangers AND
                NOT prospect_has_messaged_person
            FROM
                person
            WHERE
                id = %(person_id)s
        ) AS invisible
    FROM
        prospect
    ON CONFLICT (subject_person_id, object_person_id) DO UPDATE SET
        updated_at = now(),
        invisible = EXCLUDED.invisible
), negative_dot_prod AS (
    SELECT (
        SELECT personality FROM person WHERE id = %(person_id)s
    ) <#> (
        SELECT personality FROM prospect
    ) AS negative_dot_prod
), match_percentage AS (
    SELECT
        CLAMP(
            0,
            99,
            100 * (1 - negative_dot_prod.negative_dot_prod) / 2
        )::SMALLINT AS j
    FROM
        negative_dot_prod
), photo_uuids AS (
    SELECT COALESCE(json_agg(photo.uuid ORDER BY position), '[]'::json) AS j
    FROM photo
    JOIN prospect
    ON   prospect.id = photo.person_id
), photo_extra_exts AS (
    SELECT COALESCE(json_agg(photo.extra_exts ORDER BY position), '[]'::json) AS j
    FROM photo
    JOIN prospect
    ON   prospect.id = photo.person_id
), photo_blurhashes AS (
    SELECT COALESCE(json_agg(photo.blurhash ORDER BY position), '[]'::json) AS j
    FROM photo
    JOIN prospect
    ON   prospect.id = photo.person_id
), photo_verifications AS (
    SELECT COALESCE(json_agg(photo.verified ORDER BY position), '[]'::json) AS j
    FROM photo
    JOIN prospect
    ON   prospect.id = photo.person_id
), audio_bio_uuid AS (
    SELECT audio.uuid AS j
    FROM   audio
    JOIN   prospect
    ON     prospect.id = audio.person_id
    WHERE  audio.position = -1
), gender AS (
    SELECT gender.name AS j
    FROM gender JOIN prospect ON gender_id = gender.id
    WHERE gender.name != 'Unanswered'
), orientation AS (
    SELECT orientation.name AS j
    FROM orientation JOIN prospect ON orientation_id = orientation.id
    WHERE orientation.name != 'Unanswered'
), ethnicity AS (
    SELECT ethnicity.name AS j
    FROM ethnicity JOIN prospect ON ethnicity_id = ethnicity.id
    WHERE ethnicity.name != 'Unanswered'
), looking_for AS (
    SELECT looking_for.name AS j
    FROM looking_for JOIN prospect ON looking_for_id = looking_for.id
    WHERE looking_for.name != 'Unanswered'
), smoking AS (
    SELECT yes_no_optional.name AS j
    FROM yes_no_optional JOIN prospect ON smoking_id = yes_no_optional.id
    WHERE yes_no_optional.name != 'Unanswered'
), drinking AS (
    SELECT frequency.name AS j
    FROM frequency JOIN prospect ON drinking_id = frequency.id
    WHERE frequency.name != 'Unanswered'
), drugs AS (
    SELECT yes_no_optional.name AS j
    FROM yes_no_optional JOIN prospect ON drugs_id = yes_no_optional.id
    WHERE yes_no_optional.name != 'Unanswered'
), long_distance AS (
    SELECT yes_no_optional.name AS j
    FROM yes_no_optional JOIN prospect ON long_distance_id = yes_no_optional.id
    WHERE yes_no_optional.name != 'Unanswered'
), relationship_status AS (
    SELECT relationship_status.name AS j
    FROM relationship_status JOIN prospect ON relationship_status_id = relationship_status.id
    WHERE relationship_status.name != 'Unanswered'
), has_kids AS (
    SELECT yes_no_maybe.name AS j
    FROM yes_no_maybe JOIN prospect ON has_kids_id = yes_no_maybe.id
    WHERE yes_no_maybe.name != 'Unanswered'
), wants_kids AS (
    SELECT yes_no_maybe.name AS j
    FROM yes_no_maybe JOIN prospect ON wants_kids_id = yes_no_maybe.id
    WHERE yes_no_maybe.name != 'Unanswered'
), exercise AS (
    SELECT frequency.name AS j
    FROM frequency JOIN prospect ON exercise_id = frequency.id
    WHERE frequency.name != 'Unanswered'
), religion AS (
    SELECT religion.name AS j
    FROM religion JOIN prospect ON religion_id = religion.id
    WHERE religion.name != 'Unanswered'
), star_sign AS (
    SELECT star_sign.name AS j
    FROM star_sign JOIN prospect ON star_sign_id = star_sign.id
    WHERE star_sign.name != 'Unanswered'
), is_skipped AS (
    SELECT
        EXISTS (
            SELECT 1
            FROM skipped
            WHERE
                subject_person_id = %(person_id)s AND
                object_person_id  = (SELECT id FROM prospect)
        ) AS j
), public_answer_count AS (
    SELECT
        COUNT(*)::BIGINT AS j
    FROM
        answer
    WHERE
        person_id = (SELECT id FROM prospect)
    AND
        public_ = TRUE
    AND
        answer IS NOT NULL
), flair AS (
    SELECT
        ({Q_COMPUTED_FLAIR}) AS computed_flair
    FROM
        prospect
)
SELECT
    jsonb_build_object(
        'person_id',                 (SELECT id                        FROM prospect),
        'photo_uuids',               (SELECT j                         FROM photo_uuids),
        'photo_extra_exts',          (SELECT j                         FROM photo_extra_exts),
        'photo_blurhashes',          (SELECT j                         FROM photo_blurhashes),
        'photo_verifications',       (SELECT j                         FROM photo_verifications),
        'audio_bio_uuid',            (SELECT j                         FROM audio_bio_uuid),
        'name',                      (SELECT name                      FROM prospect),
        'age',                       (SELECT age                       FROM prospect),
        'location',                  (SELECT location                  FROM prospect),
        'match_percentage',          (SELECT j                         FROM match_percentage),
        'about',                     (SELECT about                     FROM prospect),
        'count_answers',             (SELECT j                         FROM public_answer_count),
        'is_skipped',                (SELECT j                         FROM is_skipped),
        'seconds_since_last_online', (SELECT seconds_since_last_online FROM prospect),
        'seconds_since_sign_up',     (SELECT seconds_since_sign_up     FROM prospect),
        'flair',                     (SELECT computed_flair            FROM flair),

        -- Basics
        'occupation',             (SELECT occupation    FROM prospect),
        'education',              (SELECT education     FROM prospect),
        'height_cm',              (SELECT height_cm     FROM prospect),
        'gender',                 (SELECT j             FROM gender),
        'orientation',            (SELECT j             FROM orientation),
        'ethnicity',              (SELECT j             FROM ethnicity),
        'looking_for',            (SELECT j             FROM looking_for),
        'smoking',                (SELECT j             FROM smoking),
        'drinking',               (SELECT j             FROM drinking),
        'drugs',                  (SELECT j             FROM drugs),
        'long_distance',          (SELECT j             FROM long_distance),
        'relationship_status',    (SELECT j             FROM relationship_status)
    ) || jsonb_build_object(
        'service_goals',          (SELECT service_goals FROM prospect),
        'willingness_to_relocate',(SELECT willingness_to_relocate FROM prospect),
        'family_worship_habit',   (SELECT family_worship_habit FROM prospect),
        'spiritual_routine',      (SELECT spiritual_routine FROM prospect),
        'willing_to_involve_family_early', (SELECT willing_to_involve_family_early FROM prospect),
        'open_to_chaperoned_video_calls',  (SELECT open_to_chaperoned_video_calls FROM prospect),
        'congregation_compatibility',      (SELECT congregation_compatibility FROM prospect),
        'service_lifestyle',               (SELECT service_lifestyle FROM prospect),
        'life_stage',                      (SELECT life_stage FROM prospect),
        'emotional_temperament',           (SELECT emotional_temperament FROM prospect),
        'communication_style',             (SELECT communication_style FROM prospect),
        'who_can_contact_me',              (SELECT who_can_contact_me FROM prospect),
        'request_format_preference',       (SELECT request_format_preference FROM prospect),
        'message_pace_preference',         (SELECT message_pace_preference FROM prospect)
    ) || jsonb_build_object(
        'has_kids',               (SELECT j             FROM has_kids),
        'wants_kids',             (SELECT j             FROM wants_kids),
        'exercise',               (SELECT j             FROM exercise),
        'religion',               (SELECT j             FROM religion),
        'star_sign',              (SELECT j             FROM star_sign),
        'congregation_name',      (
            SELECT congregation.name
            FROM congregation
            WHERE congregation.id = (SELECT congregation_id FROM prospect)
        ),
        'baptism_date',           (
            SELECT to_char(baptism_date, 'YYYY-MM-DD')
            FROM prospect
        ),
        'verification_level_id',  (SELECT verification_level_id FROM prospect),
        'verification_required',  (SELECT verification_required FROM prospect),
        'profile_status',         (SELECT profile_status FROM prospect),
        'waitlist_status',        (SELECT waitlist_status FROM prospect),

        -- Verifications
        'verified_age',           (SELECT verified_age       FROM prospect),
        'verified_gender',        (SELECT verified_gender    FROM prospect),
        'verified_ethnicity',     (SELECT verified_ethnicity FROM prospect),
        'theme', json_build_object(
            'title_color',         (SELECT title_color      FROM prospect),
            'body_color',          (SELECT body_color       FROM prospect),
            'background_color',    (SELECT background_color FROM prospect)
        )
    ) AS j
WHERE
    EXISTS (SELECT 1 FROM prospect)
"""

Q_CHECK_SESSION_TOKEN = f"""
SELECT
    name,
    (TRUE) AS has_gold,
    (FALSE) AS do_show_donation_nag,
    NULL::TEXT AS estimated_end_date,
    (SELECT name FROM unit WHERE unit.id = person.unit_id) AS units,
    verification_required,
    verification_level_id,
    profile_status,
    waitlist_status
FROM
    person
WHERE
    id = %(person_id)s
"""

Q_DELETE_SKIPPED = """
DELETE FROM skipped
WHERE
    subject_person_id = %(subject_person_id)s AND
    object_person_id = %(object_person_id)s
"""

Q_DELETE_SKIPPED_BY_UUID = """
DELETE FROM
    skipped
USING
    person
WHERE
    person.id = skipped.object_person_id
AND
    person.uuid = uuid_or_null(%(prospect_uuid)s)
AND
    subject_person_id = %(subject_person_id)s
"""

Q_ANSWER_COMPARISON = """
WITH prospect_name AS(
    SELECT name FROM person WHERE id = %(prospect_person_id)s
), person_name AS(
    SELECT name FROM person WHERE id = %(person_id)s
)
SELECT
    prospect_answer.person_id AS prospect_person_id,
    (SELECT name FROM prospect_name) AS prospect_name,
    prospect_answer.answer AS prospect_answer,
    person_answer.person_id AS person_id,
    (SELECT name FROM person_name) AS person_name,
    person_answer.answer AS person_answer,
    person_answer.public_ AS person_public_,
    question.id AS question_id,
    question.question AS question,
    question.topic AS topic
FROM (
    SELECT
        person_id,
        question_id,
        answer.answer
    FROM
        answer
    JOIN
        question ON
        question.id = answer.question_id AND
        (question.topic = %(topic)s OR %(topic)s = 'All') AND
        answer.person_id = %(prospect_person_id)s AND
        answer.public_ = TRUE AND
        answer.answer IS NOT NULL
) AS prospect_answer
JOIN
    question ON
    question.id = prospect_answer.question_id
LEFT JOIN
    answer AS person_answer ON
    person_answer.person_id = %(person_id)s AND
    person_answer.question_id = prospect_answer.question_id
WHERE
    (
        %(agreement)s != 'Agree' OR
        person_answer.answer IS NOT NULL AND
        prospect_answer.answer IS NOT NULL AND
        person_answer.answer = prospect_answer.answer
    ) AND (
        %(agreement)s != 'Disagree' OR
        person_answer.answer IS NOT NULL AND
        prospect_answer.answer IS NOT NULL AND
        person_answer.answer != prospect_answer.answer
    ) AND (
        %(agreement)s != 'Unanswered' OR
        person_answer.answer IS NULL
    )
ORDER BY
    question.id
LIMIT %(n)s
OFFSET %(o)s
"""

Q_INBOX_INFO = """
WITH config AS (
    SELECT COALESCE(
        MAX(CASE WHEN key = 'system_conversation_auto_close_days' THEN NULLIF(value, '')::INT END),
        10
    ) AS auto_close_days
    FROM admin_setting
), person_info AS (
    SELECT
        id_table.id AS person_id,
        id_table.uuid AS person_uuid,
        prospect.id IS NULL AS is_prospect_deleted,
        COALESCE(prospect.activated, FALSE) AS is_prospect_activated,
        prospect.name AS name,
        prospect.personality AS personality,
        prospect.verification_level_id > 1 AS verified,
        EXISTS (
            SELECT
                1
            FROM
                messaged
            WHERE
                subject_person_id = %(person_id)s
            AND
                object_person_id = id_table.id
        ) AS person_messaged_prospect,
        EXISTS (
            SELECT
                1
            FROM
                messaged
            WHERE
                subject_person_id = id_table.id
            AND
                object_person_id = %(person_id)s
        ) AS prospect_messaged_person,
        EXISTS (
            SELECT
                1
            FROM
                skipped
            WHERE
                subject_person_id = %(person_id)s
            AND
                object_person_id = id_table.id
        ) AS person_skipped_prospect,
        EXISTS (
            SELECT
                1
            FROM
                skipped
            WHERE
                subject_person_id = id_table.id
            AND
                object_person_id = %(person_id)s
        ) AS prospect_skipped_person
    FROM
        (
            SELECT DISTINCT
                id,
                uuid
            FROM
                person
            JOIN
                messaged
            ON
                messaged.subject_person_id = %(person_id)s
            AND
                messaged.object_person_id = person.id
            OR
                messaged.subject_person_id = person.id
            AND
                messaged.object_person_id = %(person_id)s
        ) AS id_table
    LEFT JOIN
        person AS prospect
    ON
        prospect.id = id_table.id
    LEFT JOIN
        skipped
    ON
        subject_person_id = prospect.id
    AND
        object_person_id = %(person_id)s
), inbox_state AS (
    SELECT
        inbox.remote_bare_jid,
        inbox.box,
        inbox.timestamp
    FROM inbox
    CROSS JOIN config
    WHERE
        inbox.luser = (SELECT uuid::TEXT FROM person WHERE id = %(person_id)s)
    AND (
        inbox.box <> 'chats'
        OR inbox.timestamp >= ((EXTRACT(EPOCH FROM NOW() - (config.auto_close_days || ' days')::interval) * 1e6)::BIGINT)
    )
)
SELECT
    person_id,
    person_uuid,
    CASE
        WHEN is_prospect_activated AND NOT prospect_skipped_person
        THEN
            verified
        ELSE
            FALSE
    END AS verified,
    CASE
        WHEN is_prospect_activated AND NOT prospect_skipped_person
        THEN
            name
        ELSE
            NULL
    END AS name,
    CASE
        WHEN is_prospect_activated AND NOT prospect_skipped_person
        THEN
            CLAMP(
                0,
                99,
                100 * (
                    1 - (
                        SELECT (
                            SELECT personality FROM person WHERE id = %(person_id)s
                        ) <#> (
                            prospect.personality
                        )
                    )
                ) / 2
            )::SMALLINT
        ELSE
            NULL
    END AS match_percentage,
    CASE
        WHEN is_prospect_activated AND NOT prospect_skipped_person
        THEN
            (
                SELECT
                    uuid
                FROM
                    photo
                WHERE
                    person_id = prospect.person_id
                ORDER BY
                    position
                LIMIT 1
            )
        ELSE
            NULL
    END AS image_uuid,
    CASE
        WHEN is_prospect_activated AND NOT prospect_skipped_person
        THEN
            (
                SELECT
                    blurhash
                FROM
                    photo
                WHERE
                    person_id = prospect.person_id
                ORDER BY
                    position
                LIMIT 1
            )
        ELSE
            NULL
    END AS image_blurhash,
    CASE
        WHEN inbox_state.box = 'archive'
        THEN 'archive'
        WHEN inbox_state.box = 'inbox'
        THEN 'intros'
        WHEN
                inbox_state.box = 'chats'
            AND
                prospect_messaged_person
            AND
                person_messaged_prospect
            AND
                NOT prospect_skipped_person
            AND
                NOT person_skipped_prospect
        THEN 'chats'
        WHEN
                NOT is_prospect_deleted
            AND
                NOT prospect_messaged_person
        THEN 'nowhere'
        WHEN
                is_prospect_activated
            AND
                NOT prospect_skipped_person
            AND
                NOT person_skipped_prospect
            AND
                prospect_messaged_person
            AND
                person_messaged_prospect
        THEN 'chats'
        WHEN
                is_prospect_activated
            AND
                NOT prospect_skipped_person
            AND
                NOT person_skipped_prospect
            AND
                prospect_messaged_person
            AND
                NOT person_messaged_prospect
        THEN 'intros'
        ELSE 'archive'
    END AS conversation_location
FROM
    person_info AS prospect
LEFT JOIN
    inbox_state
ON
    inbox_state.remote_bare_jid = prospect.person_uuid || '@duolicious.app'
ORDER BY
    person_id
"""

Q_DELETE_ACCOUNT = """
WITH deleted_inbox AS (
    DELETE FROM inbox
    WHERE luser = %(person_uuid)s
), target_referral_codes AS (
    SELECT
        id
    FROM
        referral_code
    WHERE
        person_id = %(person_id)s
), cleared_referred_people AS (
    UPDATE
        person
    SET
        referred_by_code_id = NULL
    WHERE
        referred_by_code_id IN (SELECT id FROM target_referral_codes)
), deleted_duo_session AS (
    DELETE FROM
        duo_session
    WHERE
        person_id = %(person_id)s
    OR
        referral_code_id IN (SELECT id FROM target_referral_codes)
), deleted_mam_message AS (
    DELETE FROM mam_message
    WHERE person_id = %(person_id)s
    RETURNING audio_uuid
), deleted_photo AS (
    SELECT
        uuid
    FROM
        photo
    WHERE
        person_id = %(person_id)s
), deleted_audio AS (
    SELECT
        uuid
    FROM
        audio
    WHERE
        person_id = %(person_id)s
), deleted_verification_photo AS (
    SELECT
        photo_uuid AS uuid
    FROM
        verification_job
    WHERE
        person_id = %(person_id)s
), every_deleted_photo_uuid AS (
    SELECT uuid FROM deleted_photo
    UNION
    SELECT uuid FROM deleted_verification_photo
), every_deleted_audio_uuid AS (
    SELECT uuid FROM deleted_audio

    UNION

    SELECT
        deleted_mam_message.audio_uuid AS uuid
    FROM
        deleted_mam_message
    LEFT JOIN
        mam_message
    ON
        mam_message.audio_uuid = deleted_mam_message.audio_uuid
    AND
        mam_message.person_id <> %(person_id)s
    WHERE
        deleted_mam_message.audio_uuid IS NOT NULL
    AND
        mam_message.audio_uuid IS NULL
), deleted_person AS (
    DELETE FROM
        person
    WHERE
        id = %(person_id)s
    RETURNING
        activated
), undeleted_photo_insertion AS (
    INSERT INTO undeleted_photo (
        uuid
    )
    SELECT
        uuid
    FROM
        every_deleted_photo_uuid
), undeleted_audio_insertion AS (
    INSERT INTO undeleted_audio (
        uuid
    )
    SELECT
        uuid
    FROM
        every_deleted_audio_uuid
)
SELECT 1
"""

Q_POST_DEACTIVATE = """
WITH updated_person AS (
    UPDATE
        person
    SET
        activated = FALSE
    WHERE
        activated = TRUE
    AND
        id = %(person_id)s
    RETURNING
        id
)
SELECT 1
"""

Q_GET_PROFILE_INFO = f"""
WITH photo_ AS (
    SELECT json_object_agg(position, uuid) AS j
    FROM photo
    WHERE person_id = %(person_id)s
), photo_extra_exts AS (
    SELECT json_object_agg(position, extra_exts) AS j
    FROM photo
    WHERE person_id = %(person_id)s
), photo_blurhash AS (
    SELECT json_object_agg(position, blurhash) AS j
    FROM photo
    WHERE person_id = %(person_id)s
), photo_verification AS (
    SELECT json_object_agg(position, verified) AS j
    FROM photo
    WHERE person_id = %(person_id)s
), audio_bio AS (
    SELECT uuid AS j FROM audio WHERE person_id = %(person_id)s AND position = -1
), name AS (
    SELECT name AS j FROM person WHERE id = %(person_id)s
), about AS (
    SELECT about AS j FROM person WHERE id = %(person_id)s
), gender AS (
    SELECT gender.name AS j
    FROM gender JOIN person ON gender_id = gender.id
    WHERE person.id = %(person_id)s
), orientation AS (
    SELECT orientation.name AS j
    FROM orientation JOIN person ON orientation_id = orientation.id
    WHERE person.id = %(person_id)s
), ethnicity AS (
    SELECT ethnicity.name AS j
    FROM ethnicity JOIN person ON ethnicity_id = ethnicity.id
    WHERE person.id = %(person_id)s
), location AS (
    SELECT location_long_friendly AS j
    FROM person
    WHERE id = %(person_id)s
), congregation_name AS (
    SELECT congregation.name AS j
    FROM person
    LEFT JOIN congregation
    ON congregation.id = person.congregation_id
    WHERE person.id = %(person_id)s
), congregation_address_text AS (
    SELECT congregation.address AS j
    FROM person
    LEFT JOIN congregation
    ON congregation.id = person.congregation_id
    WHERE person.id = %(person_id)s
), congregation_language_name AS (
    SELECT meeting_language.name AS j
    FROM person
    LEFT JOIN congregation
    ON congregation.id = person.congregation_id
    LEFT JOIN meeting_language
    ON meeting_language.language_guid = congregation.language_guid
    WHERE person.id = %(person_id)s
), occupation AS (
    SELECT occupation AS j FROM person WHERE id = %(person_id)s
), education AS (
    SELECT education AS j FROM person WHERE id = %(person_id)s
), height AS (
    SELECT height_cm AS j FROM person WHERE id = %(person_id)s
), looking_for AS (
    SELECT looking_for.name AS j
    FROM looking_for JOIN person ON looking_for_id = looking_for.id
    WHERE person.id = %(person_id)s
), smoking AS (
    SELECT yes_no_optional.name AS j
    FROM yes_no_optional JOIN person ON smoking_id = yes_no_optional.id
    WHERE person.id = %(person_id)s
), drinking AS (
    SELECT frequency.name AS j
    FROM frequency JOIN person ON drinking_id = frequency.id
    WHERE person.id = %(person_id)s
), drugs AS (
    SELECT yes_no_optional.name AS j
    FROM yes_no_optional JOIN person ON drugs_id = yes_no_optional.id
    WHERE person.id = %(person_id)s
), long_distance AS (
    SELECT yes_no_optional.name AS j
    FROM yes_no_optional JOIN person ON long_distance_id = yes_no_optional.id
    WHERE person.id = %(person_id)s
), relationship_status AS (
    SELECT relationship_status.name AS j
    FROM relationship_status JOIN person ON relationship_status_id = relationship_status.id
    WHERE person.id = %(person_id)s
), has_kids AS (
    SELECT yes_no_maybe.name AS j
    FROM yes_no_maybe JOIN person ON has_kids_id = yes_no_maybe.id
    WHERE person.id = %(person_id)s
), wants_kids AS (
    SELECT yes_no_maybe.name AS j
    FROM yes_no_maybe JOIN person ON wants_kids_id = yes_no_maybe.id
    WHERE person.id = %(person_id)s
), exercise AS (
    SELECT frequency.name AS j
    FROM frequency JOIN person ON exercise_id = frequency.id
    WHERE person.id = %(person_id)s
), religion AS (
    SELECT religion.name AS j
    FROM religion JOIN person ON religion_id = religion.id
    WHERE person.id = %(person_id)s
), star_sign AS (
    SELECT star_sign.name AS j
    FROM star_sign JOIN person ON star_sign_id = star_sign.id
    WHERE person.id = %(person_id)s

), unit AS (
    SELECT unit.name AS j
    FROM unit JOIN person ON unit_id = unit.id
    WHERE person.id = %(person_id)s

), chat AS (
    SELECT immediacy.name AS j
    FROM immediacy JOIN person ON chats_notification = immediacy.id
    WHERE person.id = %(person_id)s
), intro AS (
    SELECT immediacy.name AS j
    FROM immediacy JOIN person ON intros_notification = immediacy.id
    WHERE person.id = %(person_id)s

), privacy_verification_level AS (
    SELECT
        verification_level.name AS j
    FROM person
    JOIN verification_level
    ON verification_level.id = person.privacy_verification_level_id
    WHERE person.id = %(person_id)s
), show_my_location AS (
    SELECT
        CASE WHEN show_my_location THEN 'Yes' ELSE 'No' END AS j
    FROM person
    WHERE id = %(person_id)s
), show_my_age AS (
    SELECT
        CASE WHEN show_my_age THEN 'Yes' ELSE 'No' END AS j
    FROM person
    WHERE id = %(person_id)s
), hide_me_from_strangers AS (
    SELECT
        CASE WHEN hide_me_from_strangers THEN 'Yes' ELSE 'No' END AS j
    FROM person
    WHERE id = %(person_id)s
), browse_invisibly AS (
    SELECT
        CASE WHEN browse_invisibly THEN 'Yes' ELSE 'No' END AS j
    FROM person
    WHERE id = %(person_id)s
), verified_gender AS (
    SELECT verified_gender AS j FROM person WHERE id = %(person_id)s
), verified_age AS (
    SELECT verified_age AS j FROM person WHERE id = %(person_id)s
), verified_ethnicity AS (
    SELECT verified_ethnicity AS j FROM person WHERE id = %(person_id)s
), title_color AS (
    SELECT title_color AS j FROM person WHERE id = %(person_id)s
), body_color AS (
    SELECT body_color AS j FROM person WHERE id = %(person_id)s
), background_color AS (
    SELECT background_color AS j FROM person WHERE id = %(person_id)s
), flair AS (
    SELECT ({Q_COMPUTED_FLAIR}) AS j FROM person WHERE id = %(person_id)s
), profile_status AS (
    SELECT profile_status AS j FROM person WHERE id = %(person_id)s
), waitlist_status AS (
    SELECT waitlist_status AS j FROM person WHERE id = %(person_id)s
), date_of_birth AS (
    SELECT to_char(date_of_birth, 'YYYY-MM-DD') AS j FROM person WHERE id = %(person_id)s
), baptism_date AS (
    SELECT to_char(baptism_date, 'YYYY-MM-DD') AS j FROM person WHERE id = %(person_id)s
), country_of_birth AS (
    SELECT country_of_birth AS j FROM person WHERE id = %(person_id)s
), pioneer_status AS (
    SELECT pioneer_status AS j FROM person WHERE id = %(person_id)s
), verification_required AS (
    SELECT verification_required AS j FROM person WHERE id = %(person_id)s
), verification_level_id AS (
    SELECT verification_level_id AS j FROM person WHERE id = %(person_id)s
)
SELECT
    jsonb_build_object(
        'photo',                  (SELECT j FROM photo_),
        'photo_extra_exts',       (SELECT j FROM photo_extra_exts),
        'photo_blurhash',         (SELECT j FROM photo_blurhash),
        'photo_verification',     (SELECT j FROM photo_verification),
        'audio_bio_max_seconds',  {constants.MAX_AUDIO_SECONDS},
        'audio_bio',              (SELECT j FROM audio_bio),
        'name',                   (SELECT j FROM name),
        'about',                  (SELECT j FROM about),
        'gender',                 (SELECT j FROM gender),
        'orientation',            (SELECT j FROM orientation),
        'ethnicity',              (SELECT j FROM ethnicity),
        'location',               (SELECT j FROM location),
        'congregation',           (SELECT j FROM congregation_name),
        'congregation_address',   (SELECT j FROM congregation_address_text),
        'congregation_language',  (SELECT j FROM congregation_language_name),
        'occupation',             (SELECT j FROM occupation),
        'education',              (SELECT j FROM education),
        'height',                 (SELECT j FROM height),
        'looking for',            (SELECT j FROM looking_for),
        'smoking',                (SELECT j FROM smoking),
        'drinking',               (SELECT j FROM drinking),
        'drugs',                  (SELECT j FROM drugs),
        'long distance',          (SELECT j FROM long_distance),
        'relationship status',    (SELECT j FROM relationship_status)
    ) || jsonb_build_object(
        'service goals',          (SELECT service_goals FROM person WHERE id = %(person_id)s),
        'willing to relocate',    (SELECT willingness_to_relocate FROM person WHERE id = %(person_id)s),
        'family worship',         (SELECT family_worship_habit FROM person WHERE id = %(person_id)s),
        'spiritual routine',      (SELECT spiritual_routine FROM person WHERE id = %(person_id)s),
        'willing to involve family early', (SELECT willing_to_involve_family_early FROM person WHERE id = %(person_id)s),
        'open to chaperoned video calls',  (SELECT open_to_chaperoned_video_calls FROM person WHERE id = %(person_id)s),
        'congregation compatibility',      (SELECT congregation_compatibility FROM person WHERE id = %(person_id)s),
        'service lifestyle',      (SELECT service_lifestyle FROM person WHERE id = %(person_id)s),
        'life stage',             (SELECT life_stage FROM person WHERE id = %(person_id)s),
        'emotional temperament',  (SELECT emotional_temperament FROM person WHERE id = %(person_id)s),
        'communication style',    (SELECT communication_style FROM person WHERE id = %(person_id)s),
        'has kids',               (SELECT j FROM has_kids),
        'wants kids',             (SELECT j FROM wants_kids),
        'exercise',               (SELECT j FROM exercise),
        'religion',               (SELECT j FROM religion),
        'star sign',              (SELECT j FROM star_sign),

        'units',                  (SELECT j FROM unit),

        'chats',                  (SELECT j FROM chat),
        'intros',                 (SELECT j FROM intro),

        'verification level',     (SELECT j FROM privacy_verification_level),
        'show my location',       (SELECT j FROM show_my_location),
        'show my age',            (SELECT j FROM show_my_age),
        'hide me from strangers', (SELECT j FROM hide_me_from_strangers),
        'browse invisibly',       (SELECT j FROM browse_invisibly)
    ) || jsonb_build_object(
        'who can contact me',     (SELECT who_can_contact_me FROM person WHERE id = %(person_id)s),
        'interest request format',(SELECT request_format_preference FROM person WHERE id = %(person_id)s),
        'message pace',           (SELECT message_pace_preference FROM person WHERE id = %(person_id)s),
        'verified_gender',        (SELECT j FROM verified_gender),
        'verified_age',           (SELECT j FROM verified_age),
        'verified_ethnicity',     (SELECT j FROM verified_ethnicity),
        'verification_required',  (SELECT j FROM verification_required),
        'verification_level_id',  (SELECT j FROM verification_level_id),
        'profile_status',         (SELECT j FROM profile_status),
        'waitlist_status',        (SELECT j FROM waitlist_status),
        'theme', json_build_object(
            'title_color',            (SELECT j FROM title_color),
            'body_color',             (SELECT j FROM body_color),
            'background_color',       (SELECT j FROM background_color)
        ),
        'flair',                  (SELECT j FROM flair)
    ) || jsonb_build_object(
        'date of birth',          (SELECT j FROM date_of_birth),
        'baptism date',           (SELECT j FROM baptism_date),
        'country of birth',       (SELECT j FROM country_of_birth),
        'pioneer',                (SELECT j FROM pioneer_status)
    ) AS j
"""

Q_DELETE_PROFILE_INFO_PHOTO = """
WITH deleted_photo AS (
    DELETE FROM
        photo
    WHERE
        person_id = %(person_id)s
    AND
        position = %(position)s
    RETURNING
        uuid
), deleted_person_event AS (
    UPDATE
        person
    SET
        last_event_time = sign_up_time,
        last_event_name = 'joined',
        last_event_data = '{}'
    WHERE
        id = %(person_id)s
    AND
        (last_event_data->>'added_photo_uuid')::TEXT = (
            SELECT
                uuid
            FROM
                deleted_photo
        )
)
INSERT INTO undeleted_photo (
    uuid
)
SELECT
    uuid
FROM
    deleted_photo
"""

Q_DELETE_PROFILE_INFO_AUDIO = """
WITH deleted_audio AS (
    DELETE FROM
        audio
    WHERE
        person_id = %(person_id)s
    AND
        position = %(position)s
    RETURNING
        uuid
), deleted_person_event AS (
    UPDATE
        person
    SET
        last_event_time = sign_up_time,
        last_event_name = 'joined',
        last_event_data = '{}'
    WHERE
        id = %(person_id)s
    AND
        (last_event_data->>'added_audio_uuid')::TEXT = (
            SELECT
                uuid
            FROM
                deleted_audio
        )
)
INSERT INTO undeleted_audio (
    uuid
)
SELECT
    uuid
FROM
    deleted_audio
"""

Q_GET_SEARCH_FILTERS = """
WITH answer AS (
    SELECT COALESCE(
        array_agg(
            json_build_object(
                'question_id', question_id,
                'question', question,
                'topic', topic,
                'answer', answer,
                'accept_unanswered', accept_unanswered
            )
            ORDER BY question_id
        ),
        ARRAY[]::JSON[]
    ) AS j
    FROM search_preference_answer
    LEFT JOIN question
    ON question.id = question_id
    WHERE person_id = %(person_id)s
), gender AS (
    SELECT ARRAY[
        CASE
            WHEN (
                SELECT gender.name
                FROM person
                JOIN gender
                ON gender.id = person.gender_id
                WHERE person.id = %(person_id)s
            ) = 'Man'
            THEN 'Woman'
            ELSE 'Man'
        END
    ]::TEXT[] AS j
), ethnicity AS (
    SELECT COALESCE(array_agg(name ORDER BY name), ARRAY[]::TEXT[]) AS j
    FROM search_preference_ethnicity JOIN ethnicity
    ON ethnicity_id = ethnicity.id
    WHERE person_id = %(person_id)s
), city AS (
    SELECT city AS j
    FROM search_preference_city
    WHERE person_id = %(person_id)s
), state AS (
    SELECT state AS j
    FROM search_preference_state
    WHERE person_id = %(person_id)s
), age AS (
    SELECT json_build_object(
        'min_age', min_age,
        'max_age', max_age
    ) AS j
    FROM search_preference_age
    WHERE person_id = %(person_id)s
), baptism_years AS (
    SELECT COALESCE(min_baptism_years, 2) AS j
    FROM search_preference_baptism_years
    WHERE person_id = %(person_id)s
), furthest_distance AS (
    SELECT distance AS j
    FROM search_preference_distance
    WHERE person_id = %(person_id)s
), height AS (
    SELECT json_build_object(
        'min_height_cm', min_height_cm,
        'max_height_cm', max_height_cm
    ) AS j
    FROM search_preference_height_cm
    WHERE person_id = %(person_id)s
), has_a_profile_picture AS (
    SELECT COALESCE(array_agg(name ORDER BY name), ARRAY[]::TEXT[]) AS j
    FROM search_preference_has_profile_picture JOIN yes_no
    ON has_profile_picture_id = yes_no.id
    WHERE person_id = %(person_id)s
), looking_for AS (
    SELECT COALESCE(array_agg(name ORDER BY name), ARRAY[]::TEXT[]) AS j
    FROM search_preference_looking_for JOIN looking_for
    ON looking_for_id = looking_for.id
    WHERE person_id = %(person_id)s
), drinking AS (
    SELECT COALESCE(array_agg(name ORDER BY name), ARRAY[]::TEXT[]) AS j
    FROM search_preference_drinking JOIN frequency
    ON drinking_id = frequency.id
    WHERE person_id = %(person_id)s
), long_distance AS (
    SELECT COALESCE(array_agg(name ORDER BY name), ARRAY[]::TEXT[]) AS j
    FROM search_preference_long_distance JOIN yes_no_optional
    ON long_distance_id = yes_no_optional.id
    WHERE person_id = %(person_id)s
), relationship_status AS (
    SELECT COALESCE(array_agg(name ORDER BY name), ARRAY[]::TEXT[]) AS j
    FROM search_preference_relationship_status JOIN relationship_status
    ON relationship_status_id = relationship_status.id
    WHERE person_id = %(person_id)s
), pioneer_status AS (
    SELECT COALESCE(array_agg(status ORDER BY status), ARRAY[]::TEXT[]) AS j
    FROM search_preference_pioneer_status
    WHERE person_id = %(person_id)s
), has_kids AS (
    SELECT COALESCE(array_agg(name ORDER BY name), ARRAY[]::TEXT[]) AS j
    FROM search_preference_has_kids JOIN yes_no_optional
    ON has_kids_id = yes_no_optional.id
    WHERE person_id = %(person_id)s
), wants_kids AS (
    SELECT COALESCE(array_agg(name ORDER BY name), ARRAY[]::TEXT[]) AS j
    FROM search_preference_wants_kids JOIN yes_no_maybe
    ON wants_kids_id = yes_no_maybe.id
    WHERE person_id = %(person_id)s
), exercise AS (
    SELECT COALESCE(array_agg(name ORDER BY name), ARRAY[]::TEXT[]) AS j
    FROM search_preference_exercise JOIN frequency
    ON exercise_id = frequency.id
    WHERE person_id = %(person_id)s
), religion AS (
    SELECT COALESCE(array_agg(name ORDER BY name), ARRAY[]::TEXT[]) AS j
    FROM search_preference_religion JOIN religion
    ON religion_id = religion.id
    WHERE person_id = %(person_id)s
), star_sign AS (
    SELECT COALESCE(array_agg(name ORDER BY name), ARRAY[]::TEXT[]) AS j
    FROM search_preference_star_sign JOIN star_sign
    ON star_sign_id = star_sign.id
    WHERE person_id = %(person_id)s
), people_you_messaged AS (
    SELECT name AS j
    FROM search_preference_messaged JOIN yes_no
    ON messaged_id = yes_no.id
    WHERE person_id = %(person_id)s
), people_you_skipped AS (
    SELECT name AS j
    FROM search_preference_skipped JOIN yes_no
    ON skipped_id = yes_no.id
    WHERE person_id = %(person_id)s
)
SELECT
    json_build_object(
        'answer',                 (SELECT j FROM answer),

        'gender',                 (SELECT j FROM gender),
        'ethnicity',              (SELECT j FROM ethnicity),
        'city',                   (SELECT j FROM city),
        'state',                  (SELECT j FROM state),
        'age',                    (SELECT j FROM age),
        'baptism_years',          COALESCE((SELECT j FROM baptism_years), 2),
        'furthest_distance',      (SELECT j FROM furthest_distance),
        'height',                 (SELECT j FROM height),
        'has_a_profile_picture',  (SELECT j FROM has_a_profile_picture),
        'looking_for',            (SELECT j FROM looking_for),
        'drinking',               (SELECT j FROM drinking),
        'long_distance',          (SELECT j FROM long_distance),
        'relationship_status',    (SELECT j FROM relationship_status),
        'pioneer_status',         (SELECT j FROM pioneer_status),
        'has_kids',               (SELECT j FROM has_kids),
        'wants_kids',             (SELECT j FROM wants_kids),
        'exercise',               (SELECT j FROM exercise),
        'religion',               (SELECT j FROM religion),
        'star_sign',              (SELECT j FROM star_sign),

        'people_you_messaged',    (SELECT j FROM people_you_messaged),
        'people_you_skipped',     (SELECT j FROM people_you_skipped)
    ) AS j
"""

Q_UPDATE_CHATS_NOTIFICATIONS = """
WITH updated_rows AS (
    UPDATE
        person
    SET
        chats_notification = immediacy.id
    FROM
        immediacy
    WHERE
        person.email = %(email)s
    AND
        immediacy.name = %(frequency)s
    RETURNING 1
)
SELECT
    count(*) > 0 AS ok
FROM
    updated_rows
"""

Q_UPDATE_INTROS_NOTIFICATIONS = """
WITH updated_rows AS (
    UPDATE
        person
    SET
        intros_notification = immediacy.id
    FROM
        immediacy
    WHERE
        person.email = %(email)s
    AND
        immediacy.name = %(frequency)s
    RETURNING 1
)
SELECT
    count(*) > 0 AS ok
FROM
    updated_rows
"""

Q_CHECK_ADMIN_BAN_TOKEN = """
SELECT 1 FROM banned_person_admin_token WHERE token = %(token)s
"""

Q_ADMIN_BAN = """
WITH deleted_token AS (
    DELETE FROM
        banned_person_admin_token
    WHERE
        token = %(token)s
    AND
        expires_at > NOW()
    RETURNING
        person_id
), this_banned_person AS (
    SELECT
        normalized_email,
        uuid AS person_uuid,
        id AS person_id
    FROM
        person
    JOIN
        deleted_token
    ON
        deleted_token.person_id = person.id
), report_reason AS (
    SELECT
        COALESCE(array_agg(report_reason), ARRAY[]::text[]) AS report_reasons
    FROM
        skipped
    WHERE
        reported
    AND
        object_person_id = (SELECT person_id FROM deleted_token)
    AND
        report_reason <> ''
), _duo_session AS (
    SELECT
        this_banned_person.normalized_email AS normalized_email,
        COALESCE(duo_session.ip_address, '127.0.0.1') AS ip_address
    FROM
        this_banned_person
    LEFT JOIN
        duo_session
    ON
        duo_session.person_id = this_banned_person.person_id
), banned_person_insertion AS (
    INSERT INTO banned_person (
        normalized_email,
        ip_address,
        report_reasons
    )
    SELECT
        normalized_email,
        ip_address,
        report_reasons
    FROM
        _duo_session,
        report_reason
    ON CONFLICT DO NOTHING
)
SELECT
    person_id,
    person_uuid::TEXT
FROM
    this_banned_person
"""

Q_CHECK_ADMIN_DELETE_PHOTO_TOKEN = """
SELECT 1 FROM deleted_photo_admin_token WHERE token = %(token)s
"""

Q_ADMIN_DELETE_PHOTO = """
WITH deleted_token AS (
    DELETE FROM
        deleted_photo_admin_token
    WHERE
        token = %(token)s
    AND
        expires_at > NOW()
    RETURNING
        photo_uuid
), deleted_photo AS (
    DELETE FROM
        photo
    USING
        deleted_token
    WHERE
        photo.uuid = deleted_token.photo_uuid
    RETURNING
        photo.uuid,
        photo.hash,
        photo.person_id
), deleted_person_event AS (
    UPDATE
        person
    SET
        last_event_time = sign_up_time,
        last_event_name = 'joined',
        last_event_data = '{}'
    WHERE
        id = (SELECT person_id FROM deleted_photo)
    AND
        (last_event_data->>'added_photo_uuid')::TEXT = (
            SELECT
                uuid
            FROM
                deleted_photo
        )
), inserted_undeleted_photo AS (
    INSERT INTO undeleted_photo (
        uuid
    )
    SELECT
        uuid
    FROM
        deleted_photo
    RETURNING
        uuid
), inserted_banned_photo_hash AS (
    INSERT INTO banned_photo_hash (
        hash
    )
    SELECT
        hash
    FROM
        deleted_photo
    WHERE
        hash <> ''
    ON CONFLICT DO NOTHING
)
SELECT
    inserted_undeleted_photo.uuid,
    deleted_photo.person_id
FROM
    inserted_undeleted_photo
JOIN
    deleted_photo
ON
    inserted_undeleted_photo.uuid = deleted_photo.uuid
"""

Q_STATS = """
SELECT
    count(*) AS num_active_users
FROM
    person
WHERE
    activated
"""

Q_GENDER_STATS = """
SELECT
    count(*) FILTER (WHERE activated AND gender_id = 1)::real /
    count(*) FILTER (WHERE activated AND gender_id = 2)::real
    AS gender_ratio,

    count(*) FILTER (WHERE activated AND gender_id NOT IN (1, 2))::real /
    count(*) FILTER (WHERE activated)::real *
    100.0
    AS non_binary_percentage
FROM
    person
"""

Q_PERSON_ID_TO_UUID = """
SELECT
    uuid::text
FROM
    person
WHERE
    id = %(person_id)s
"""

Q_ADMIN_TOKEN_TO_UUID = """
SELECT
    person.uuid::text AS person_uuid
FROM
    person
JOIN
    banned_person_admin_token
ON
    banned_person_admin_token.person_id = person.id
WHERE
    banned_person_admin_token.token = %(token)s
"""

Q_ADMIN_LIST_USERS = """
SELECT
    id,
    uuid::text AS uuid,
    email,
    normalized_email,
    name,
    about,
    congregation_id,
    (
        SELECT congregation.name
        FROM congregation
        WHERE congregation.id = person.congregation_id
    ) AS congregation_name,
    (
        SELECT gender.name
        FROM gender
        WHERE gender.id = person.gender_id
    ) AS gender,
    location_short_friendly AS location,
    to_char(date_of_birth, 'YYYY-MM-DD') AS date_of_birth,
    to_char(baptism_date, 'YYYY-MM-DD') AS baptism_date,
    country_of_birth,
    activated,
    profile_status,
    waitlist_status,
    invite_unlocked_at,
    verification_level_id,
    verified_gender,
    verified_age,
    verified_ethnicity,
    (
        SELECT COUNT(*)
        FROM photo
        WHERE photo.person_id = person.id
    ) AS photo_count,
    (
        SELECT COUNT(*)
        FROM photo
        WHERE
            photo.person_id = person.id
        AND
            photo.verified = FALSE
    ) AS pending_photo_review_count,
    EXISTS (
        SELECT 1
        FROM verification_review
        WHERE
            verification_review.person_id = person.id
        AND
            verification_review.admin_status = 'pending'
        AND
            verification_review.selfie_photo_uuid IS NOT NULL
    ) AS has_pending_selfie_review,
    (
        SELECT COUNT(*)
        FROM verification_review_asset
        JOIN verification_review
        ON verification_review.id = verification_review_asset.review_id
        WHERE
            verification_review.person_id = person.id
        AND
            verification_review.admin_status = 'pending'
    ) AS pending_verification_asset_count,
    (
        SELECT COALESCE(array_agg(DISTINCT skipped.report_reason), ARRAY[]::TEXT[])
        FROM skipped
        WHERE
            skipped.object_person_id = person.id AND
            skipped.reported AND
            skipped.report_reason <> ''
    ) AS report_reasons,
    sign_up_time,
    sign_in_time,
    sign_in_count
FROM
    person
ORDER BY sign_up_time DESC
LIMIT 500
"""

Q_ADMIN_GET_USER = """
SELECT
    id,
    uuid::text AS uuid,
    email,
    normalized_email,
    name,
    about,
    activated,
    profile_status,
    waitlist_status,
    waitlist_note,
    invite_unlocked_at,
    sign_up_time,
    sign_in_time,
    sign_in_count,
    location_short_friendly AS location,
    to_char(date_of_birth, 'YYYY-MM-DD') AS date_of_birth,
    to_char(baptism_date, 'YYYY-MM-DD') AS baptism_date,
    country_of_birth,
    congregation_id,
    (
        SELECT congregation.name
        FROM congregation
        WHERE congregation.id = person.congregation_id
    ) AS congregation_name,
    (
        SELECT gender.name
        FROM gender
        WHERE gender.id = person.gender_id
    ) AS gender,
    gender_id,
    orientation_id,
    ethnicity_id,
    pioneer_status,
    verification_level_id,
    verified_gender,
    verified_age,
    verified_ethnicity,
    has_profile_picture_id,
    (
        SELECT COUNT(*)
        FROM photo
        WHERE
            photo.person_id = person.id
        AND
            photo.verified = FALSE
    ) AS pending_photo_review_count,
    EXISTS (
        SELECT 1
        FROM verification_review
        WHERE
            verification_review.person_id = person.id
        AND
            verification_review.admin_status = 'pending'
        AND
            verification_review.selfie_photo_uuid IS NOT NULL
    ) AS has_pending_selfie_review,
    (
        SELECT COUNT(*)
        FROM verification_review_asset
        JOIN verification_review
        ON verification_review.id = verification_review_asset.review_id
        WHERE
            verification_review.person_id = person.id
        AND
            verification_review.admin_status = 'pending'
    ) AS pending_verification_asset_count,
    (
        SELECT COALESCE(array_agg(DISTINCT skipped.report_reason), ARRAY[]::TEXT[])
        FROM skipped
        WHERE
            skipped.object_person_id = person.id AND
            skipped.reported AND
            skipped.report_reason <> ''
    ) AS report_reasons,
    title_color,
    body_color,
    background_color
FROM
    person
WHERE
    id = %(person_id)s
"""

Q_ADMIN_CREATE_USER = """
INSERT INTO person (
    email,
    normalized_email,
    name,
    about,
    date_of_birth,
    coordinates,
    gender_id,
    unit_id,
    location_short_friendly,
    location_long_friendly
) VALUES (
    %(email)s,
    %(normalized_email)s,
    %(name)s,
    %(about)s,
    %(date_of_birth)s,
    ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326),
    %(gender_id)s,
    %(unit_id)s,
    %(location_short_friendly)s,
    %(location_long_friendly)s
)
RETURNING
    id,
    uuid::text AS uuid,
    email,
    normalized_email,
    name,
    about,
    activated,
    sign_up_time
"""

Q_ADMIN_UPDATE_USER = """
UPDATE person
SET
    name = COALESCE(NULLIF(%(name)s, ''), name),
    email = COALESCE(NULLIF(%(email)s, ''), email),
    normalized_email = COALESCE(NULLIF(%(normalized_email)s, ''), normalized_email),
    about = COALESCE(%(about)s, about),
    activated = COALESCE(%(activated)s, activated),
    congregation_id = COALESCE(%(congregation_id)s, congregation_id),
    gender_id = COALESCE((SELECT id FROM gender WHERE name = NULLIF(%(gender)s, '')), gender_id),
    date_of_birth = COALESCE(%(date_of_birth)s::date, date_of_birth),
    baptism_date = COALESCE(%(baptism_date)s::date, baptism_date),
    country_of_birth = CASE
        WHEN %(country_of_birth)s = '' THEN NULL
        ELSE COALESCE(%(country_of_birth)s, country_of_birth)
    END,
    profile_status = COALESCE(NULLIF(%(profile_status)s, ''), profile_status),
    waitlist_status = COALESCE(NULLIF(%(waitlist_status)s, ''), waitlist_status),
    waitlist_note = COALESCE(%(waitlist_note)s, waitlist_note),
    invite_unlocked_at = COALESCE(%(invite_unlocked_at)s::timestamp, invite_unlocked_at),
    pioneer_status = CASE
        WHEN %(pioneer_status)s = '' THEN NULL
        ELSE COALESCE(%(pioneer_status)s, pioneer_status)
    END,
    verified_gender = COALESCE(%(verified_gender)s, verified_gender),
    verified_age = COALESCE(%(verified_age)s, verified_age),
    verified_ethnicity = COALESCE(%(verified_ethnicity)s, verified_ethnicity),
    title_color = COALESCE(NULLIF(%(title_color)s, ''), title_color),
    body_color = COALESCE(NULLIF(%(body_color)s, ''), body_color),
    background_color = COALESCE(NULLIF(%(background_color)s, ''), background_color)
WHERE
    id = %(person_id)s
RETURNING
    id,
    uuid::text AS uuid,
    email,
    normalized_email,
    name,
    about,
    congregation_id,
    to_char(date_of_birth, 'YYYY-MM-DD') AS date_of_birth,
    to_char(baptism_date, 'YYYY-MM-DD') AS baptism_date,
    country_of_birth,
    (
        SELECT congregation.name
        FROM congregation
        WHERE congregation.id = person.congregation_id
    ) AS congregation_name,
    (
        SELECT gender.name
        FROM gender
        WHERE gender.id = person.gender_id
    ) AS gender,
    activated,
    pioneer_status,
    profile_status,
    waitlist_status,
    waitlist_note,
    invite_unlocked_at,
    verification_level_id,
    verified_gender,
    verified_age,
    verified_ethnicity,
    (
        SELECT COALESCE(array_agg(DISTINCT skipped.report_reason), ARRAY[]::TEXT[])
        FROM skipped
        WHERE
            skipped.object_person_id = person.id AND
            skipped.reported AND
            skipped.report_reason <> ''
    ) AS report_reasons,
    sign_up_time
"""

Q_ADMIN_DEACTIVATE_USER = """
UPDATE person
SET activated = FALSE
WHERE id = %(person_id)s
RETURNING id
"""

Q_ADMIN_BAN_USER = """
WITH target_person AS (
    SELECT
        id,
        uuid::TEXT AS person_uuid,
        normalized_email
    FROM
        person
    WHERE
        id = %(person_id)s
), deactivated_person AS (
    UPDATE
        person
    SET
        activated = FALSE
    WHERE
        id = %(person_id)s
    RETURNING
        id
), inserted_bans AS (
    INSERT INTO banned_person (
        normalized_email,
        ip_address,
        report_reasons
    )
    SELECT
        target_person.normalized_email,
        COALESCE(duo_session.ip_address, '127.0.0.1'::inet),
        ARRAY['admin-hard-ban']::TEXT[]
    FROM
        target_person
    LEFT JOIN
        duo_session
    ON
        duo_session.person_id = target_person.id
    ON CONFLICT DO NOTHING
), deleted_sessions AS (
    DELETE FROM
        duo_session
    WHERE
        person_id = %(person_id)s
)
SELECT
    id,
    person_uuid
FROM
    target_person
"""

Q_ADMIN_SYSTEM_STATS = """
SELECT
    COUNT(*) AS total_users,
    COUNT(*) FILTER (WHERE activated) AS active_users,
    COUNT(*) FILTER (WHERE verification_level_id >= 3) AS verified_users
FROM person
"""

Q_ADMIN_ONBOARDING_STEPS = """
SELECT id, step_name, is_required, ordinal
FROM admin_onboarding_step
ORDER BY ordinal ASC
"""

Q_ADMIN_CREATE_ONBOARDING_STEP = """
INSERT INTO admin_onboarding_step (step_name, is_required, ordinal)
VALUES (%(step_name)s, %(is_required)s, %(ordinal)s)
RETURNING id, step_name, is_required, ordinal
"""

Q_ADMIN_UPDATE_ONBOARDING_STEP = """
UPDATE admin_onboarding_step
SET
    step_name = COALESCE(NULLIF(%(step_name)s, ''), step_name),
    is_required = COALESCE(%(is_required)s, is_required),
    ordinal = COALESCE(%(ordinal)s, ordinal)
WHERE id = %(id)s
RETURNING id, step_name, is_required, ordinal
"""

Q_ADMIN_DELETE_ONBOARDING_STEP = """
DELETE FROM admin_onboarding_step
WHERE id = %(id)s
RETURNING id
"""

Q_ADMIN_LIST_SETTINGS = """
SELECT key, value
FROM admin_setting
ORDER BY key ASC
"""

Q_ADMIN_LIST_SETTINGS_BY_PREFIX = """
SELECT
    key,
    value
FROM
    admin_setting
WHERE
    key LIKE %(prefix)s || '%%'
ORDER BY
    key ASC
"""

Q_ADMIN_UPSERT_SETTING = """
INSERT INTO admin_setting (key, value)
VALUES (%(key)s, %(value)s)
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
RETURNING key, value
"""

Q_ADMIN_DELETE_SETTING = """
DELETE FROM admin_setting
WHERE key = %(key)s
RETURNING key
"""

Q_ADMIN_LIST_USER_PHOTOS = """
SELECT
    position,
    uuid,
    blurhash,
    verified,
    extra_exts,
    %(images_url)s || '/450-' || uuid || '.jpg' AS photo_url
FROM
    photo
WHERE
    person_id = %(person_id)s
ORDER BY
    position ASC
"""

Q_ADMIN_DELETE_USER_PHOTO = """
WITH deleted_photo AS (
    DELETE FROM
        photo
    WHERE
        person_id = %(person_id)s
    AND
        uuid = %(photo_uuid)s
    RETURNING
        uuid,
        hash,
        person_id
), deleted_person_event AS (
    UPDATE
        person
    SET
        last_event_time = sign_up_time,
        last_event_name = 'joined',
        last_event_data = '{}'
    WHERE
        id = (SELECT person_id FROM deleted_photo)
    AND
        (last_event_data->>'added_photo_uuid')::TEXT = (
            SELECT
                uuid
            FROM
                deleted_photo
        )
), inserted_undeleted_photo AS (
    INSERT INTO undeleted_photo (
        uuid
    )
    SELECT
        uuid
    FROM
        deleted_photo
    RETURNING
        uuid
), inserted_banned_photo_hash AS (
    INSERT INTO banned_photo_hash (
        hash
    )
    SELECT
        hash
    FROM
        deleted_photo
    WHERE
        hash <> ''
    ON CONFLICT DO NOTHING
)
SELECT
    inserted_undeleted_photo.uuid,
    deleted_photo.person_id
FROM
    inserted_undeleted_photo
JOIN
    deleted_photo
ON
    inserted_undeleted_photo.uuid = deleted_photo.uuid
"""

Q_ADMIN_VERIFY_USER_PHOTO = """
UPDATE
    photo
SET
    verified = TRUE
WHERE
    person_id = %(person_id)s
AND
    uuid = %(photo_uuid)s
RETURNING
    uuid,
    person_id
"""

Q_DELETE_VERIFICATION_JOB = """
WITH deleted_job AS (
    DELETE FROM
        verification_job
    WHERE
        person_id = %(person_id)s
    RETURNING
        photo_uuid AS uuid
)
INSERT INTO undeleted_photo (
    uuid
)
SELECT
    uuid
FROM
    deleted_job
"""

Q_INSERT_VERIFICATION_JOB = """
INSERT INTO verification_job (
    person_id,
    photo_uuid
) VALUES (
    %(person_id)s,
    %(photo_uuid)s
)
"""

Q_INSERT_VERIFICATION_PHOTO_HASH = """
INSERT INTO verification_photo_hash (
    hash
)
VALUES
    (%(photo_hash)s)
ON CONFLICT DO NOTHING
RETURNING
    1
"""

Q_UPDATE_VERIFICATION_JOB = """
UPDATE
    verification_job
SET
    status = %(status)s,
    message = %(message)s
WHERE
    person_id = %(person_id)s
AND (
        status = %(expected_previous_status)s
    OR
        %(expected_previous_status)s::verification_job_status IS NULL
)
"""

Q_CHECK_VERIFICATION = """
SELECT
    person.verified_gender,
    person.verified_age,
    person.verified_ethnicity,
    (
        SELECT json_object_agg(position, verified) AS j
        FROM photo
        WHERE person_id = %(person_id)s
    ) AS verified_photos,
    verification_job.status,
    verification_job.message
FROM
    person
LEFT JOIN
    verification_job
ON
    verification_job.person_id = person.id
WHERE
    person.id = %(person_id)s
"""

Q_UPSERT_VERIFICATION_REVIEW_SELFIE = """
INSERT INTO verification_review (
    person_id,
    selfie_photo_uuid,
    ai_status,
    ai_message,
    admin_status,
    admin_message,
    reviewed_by_person_id,
    updated_at
)
VALUES (
    %(person_id)s,
    %(photo_uuid)s,
    'uploading-photo',
    'Uploading photo',
    'pending',
    '',
    NULL,
    NOW()
)
ON CONFLICT (person_id) DO UPDATE
SET
    selfie_photo_uuid = EXCLUDED.selfie_photo_uuid,
    ai_status = EXCLUDED.ai_status,
    ai_message = EXCLUDED.ai_message,
    admin_status = EXCLUDED.admin_status,
    admin_message = EXCLUDED.admin_message,
    reviewed_by_person_id = NULL,
    updated_at = NOW()
RETURNING id
"""

Q_UPSERT_VERIFICATION_REVIEW_AI_STATUS = """
INSERT INTO verification_review (
    person_id,
    ai_status,
    ai_message,
    updated_at
)
VALUES (
    %(person_id)s,
    %(ai_status)s,
    %(ai_message)s,
    NOW()
)
ON CONFLICT (person_id) DO UPDATE
SET
    ai_status = EXCLUDED.ai_status,
    ai_message = EXCLUDED.ai_message,
    updated_at = NOW()
RETURNING id
"""

Q_INSERT_VERIFICATION_REVIEW_ASSET = """
WITH ensured_review AS (
    INSERT INTO verification_review (
        person_id,
        updated_at
    )
    VALUES (
        %(person_id)s,
        NOW()
    )
    ON CONFLICT (person_id) DO UPDATE
    SET
        updated_at = NOW()
    RETURNING id
), existing_review AS (
    SELECT id FROM ensured_review
    UNION ALL
    SELECT id FROM verification_review WHERE person_id = %(person_id)s
    LIMIT 1
)
INSERT INTO verification_review_asset (
    review_id,
    kind,
    label,
    photo_uuid,
    verified
)
SELECT
    id,
    %(kind)s,
    NULLIF(%(label)s, ''),
    %(photo_uuid)s,
    FALSE
FROM
    existing_review
RETURNING id
"""

Q_DELETE_VERIFICATION_REVIEW_ASSET = """
WITH deleted_asset AS (
    DELETE FROM
        verification_review_asset
    WHERE
        id = %(asset_id)s
    AND
        review_id IN (
            SELECT id FROM verification_review WHERE person_id = %(person_id)s
        )
    RETURNING
        photo_uuid
), inserted_undeleted_photo AS (
    INSERT INTO undeleted_photo (
        uuid
    )
    SELECT
        photo_uuid
    FROM
        deleted_asset
    RETURNING
        uuid
)
SELECT
    uuid
FROM
    inserted_undeleted_photo
"""

Q_GET_VERIFICATION_REVIEW = """
SELECT
    verification_review.id,
    verification_review.person_id,
    verification_review.selfie_photo_uuid,
    verification_review.ai_status,
    verification_review.ai_message,
    verification_review.admin_status,
    verification_review.admin_message,
    iso8601_utc(verification_review.created_at) AS created_at,
    iso8601_utc(verification_review.updated_at) AS updated_at,
    (
        SELECT
            COALESCE(
                json_agg(
                    json_build_object(
                        'id', verification_review_asset.id,
                        'kind', verification_review_asset.kind,
                        'label', verification_review_asset.label,
                        'photo_uuid', verification_review_asset.photo_uuid,
                        'verified', verification_review_asset.verified,
                        'photo_url', 'https://user-images.duolicious.app/450-' || verification_review_asset.photo_uuid || '.jpg',
                        'created_at', iso8601_utc(verification_review_asset.created_at)
                    )
                    ORDER BY verification_review_asset.created_at DESC
                ),
                '[]'::json
            )
        FROM
            verification_review_asset
        WHERE
            verification_review_asset.review_id = verification_review.id
    ) AS assets
FROM
    verification_review
WHERE
    person_id = %(person_id)s
"""

Q_ADMIN_LIST_VERIFICATION_REVIEWS = """
SELECT
    verification_review.id,
    verification_review.person_id,
    person.uuid,
    person.name,
    person.email,
    person.verification_required,
    person.verification_level_id,
    verification_review.ai_status,
    verification_review.ai_message,
    verification_review.admin_status,
    verification_review.admin_message,
    verification_review.selfie_photo_uuid,
    'https://user-images.duolicious.app/450-' || verification_review.selfie_photo_uuid || '.jpg' AS selfie_photo_url,
    iso8601_utc(verification_review.created_at) AS created_at,
    iso8601_utc(verification_review.updated_at) AS updated_at,
    (
        SELECT COUNT(*)
        FROM verification_review_asset
        WHERE verification_review_asset.review_id = verification_review.id
    ) AS asset_count
FROM
    verification_review
JOIN
    person
ON
    person.id = verification_review.person_id
ORDER BY
    verification_review.updated_at DESC,
    verification_review.id DESC
"""

Q_ADMIN_GET_VERIFICATION_REVIEW = """
SELECT
    verification_review.id,
    verification_review.person_id,
    person.uuid,
    person.name,
    person.email,
    person.about,
    person.location_long_friendly AS location,
    person.verification_required,
    person.verification_level_id,
    person.verified_gender,
    person.verified_age,
    person.verified_ethnicity,
    verification_review.ai_status,
    verification_review.ai_message,
    verification_review.admin_status,
    verification_review.admin_message,
    verification_review.selfie_photo_uuid,
    'https://user-images.duolicious.app/450-' || verification_review.selfie_photo_uuid || '.jpg' AS selfie_photo_url,
    iso8601_utc(verification_review.created_at) AS created_at,
    iso8601_utc(verification_review.updated_at) AS updated_at,
    (
        SELECT
            COALESCE(
                json_agg(
                    json_build_object(
                        'id', verification_review_asset.id,
                        'kind', verification_review_asset.kind,
                        'label', verification_review_asset.label,
                        'photo_uuid', verification_review_asset.photo_uuid,
                        'verified', verification_review_asset.verified,
                        'photo_url', 'https://user-images.duolicious.app/450-' || verification_review_asset.photo_uuid || '.jpg',
                        'created_at', iso8601_utc(verification_review_asset.created_at)
                    )
                    ORDER BY verification_review_asset.created_at DESC
                ),
                '[]'::json
            )
        FROM
            verification_review_asset
        WHERE
            verification_review_asset.review_id = verification_review.id
    ) AS assets
FROM
    verification_review
JOIN
    person
ON
    person.id = verification_review.person_id
WHERE
    verification_review.id = %(review_id)s
"""

Q_ADMIN_APPROVE_VERIFICATION_REVIEW = """
WITH updated_review AS (
    UPDATE
        verification_review
    SET
        admin_status = 'approved',
        admin_message = COALESCE(%(admin_message)s, ''),
        reviewed_by_person_id = %(reviewed_by_person_id)s,
        updated_at = NOW()
    WHERE
        id = %(review_id)s
    AND admin_status = 'pending'
    RETURNING
        person_id
), updated_person AS (
    UPDATE
        person
    SET
        verification_required = FALSE,
        verification_level_id = (
            SELECT id FROM verification_level WHERE name = 'Photos'
        ),
        verified_gender = TRUE,
        verified_age = TRUE,
        verified_ethnicity = TRUE
    WHERE
        id IN (SELECT person_id FROM updated_review)
    RETURNING
        id
), updated_photos AS (
    UPDATE
        photo
    SET
        verified = TRUE
    WHERE
        person_id IN (SELECT id FROM updated_person)
), updated_assets AS (
    UPDATE
        verification_review_asset
    SET
        verified = TRUE
    WHERE
        review_id = %(review_id)s
)
SELECT id FROM updated_person
"""

Q_ADMIN_REJECT_VERIFICATION_REVIEW = """
UPDATE
    verification_review
SET
    admin_status = 'rejected',
    admin_message = COALESCE(%(admin_message)s, ''),
    reviewed_by_person_id = %(reviewed_by_person_id)s,
    updated_at = NOW()
WHERE
    id = %(review_id)s
AND admin_status = 'pending'
RETURNING
    person_id
"""

Q_ADMIN_DELETE_VERIFICATION_REVIEW_SELFIE = """
WITH target_review AS (
    SELECT
        id,
        person_id,
        selfie_photo_uuid
    FROM
        verification_review
    WHERE
        id = %(review_id)s
    AND
        admin_status = 'pending'
), deleted_job AS (
    DELETE FROM
        verification_job
    WHERE
        person_id IN (SELECT person_id FROM target_review)
), inserted_undeleted_photo AS (
    INSERT INTO undeleted_photo (
        uuid
    )
    SELECT
        selfie_photo_uuid
    FROM
        target_review
    WHERE
        selfie_photo_uuid IS NOT NULL
), updated_review AS (
    UPDATE
        verification_review
    SET
        selfie_photo_uuid = NULL,
        ai_status = 'pending-selfie',
        ai_message = '',
        admin_status = 'pending',
        admin_message = '',
        reviewed_by_person_id = NULL,
        updated_at = NOW()
    WHERE
        id = %(review_id)s
    AND
        admin_status = 'pending'
    RETURNING
        person_id
)
SELECT
    person_id
FROM
    updated_review
"""

Q_ADMIN_DELETE_VERIFICATION_REVIEW_ASSET = """
WITH target_review AS (
    SELECT
        id,
        person_id
    FROM
        verification_review
    WHERE
        id = %(review_id)s
    AND
        admin_status = 'pending'
), deleted_asset AS (
    DELETE FROM
        verification_review_asset
    WHERE
        id = %(asset_id)s
    AND
        review_id = %(review_id)s
    RETURNING
        photo_uuid
), inserted_undeleted_photo AS (
    INSERT INTO undeleted_photo (
        uuid
    )
    SELECT
        photo_uuid
    FROM
        deleted_asset
), updated_review AS (
    UPDATE
        verification_review
    SET
        admin_status = 'pending',
        reviewed_by_person_id = NULL,
        updated_at = NOW()
    WHERE
        id = %(review_id)s
    AND
        admin_status = 'pending'
    RETURNING
        person_id
)
SELECT
    person_id
FROM
    updated_review
WHERE
    EXISTS (SELECT 1 FROM deleted_asset)
"""

Q_ADMIN_VERIFY_VERIFICATION_REVIEW_ASSET = """
WITH updated_asset AS (
    UPDATE
        verification_review_asset
    SET
        verified = TRUE
    WHERE
        id = %(asset_id)s
    AND
        review_id = %(review_id)s
    AND
        review_id IN (
            SELECT id
            FROM verification_review
            WHERE
                id = %(review_id)s
            AND
                admin_status = 'pending'
        )
    RETURNING
        id
), updated_review AS (
    UPDATE
        verification_review
    SET
        updated_at = NOW()
    WHERE
        id = %(review_id)s
    AND
        admin_status = 'pending'
    RETURNING
        person_id
)
SELECT
    person_id
FROM
    updated_review
WHERE
    EXISTS (SELECT 1 FROM updated_asset)
"""

Q_DISMISS_DONATION = """
UPDATE
    person
SET
    last_nag_time = NOW()
WHERE
    id = %(person_id)s
"""

Q_INSERT_EXPORT_DATA_TOKEN = """
INSERT INTO export_data_token (
    person_id
)
VALUES
    (%(person_id)s)
RETURNING
    token
"""

Q_CHECK_EXPORT_DATA_TOKEN = """
WITH deleted_export_data_token AS (
    DELETE FROM
        export_data_token
    WHERE
        token = %(token)s
    AND
        expires_at > NOW()
    RETURNING
        person_id
)
SELECT
    person.id AS person_id,
    person.uuid AS person_uuid
FROM
    person
JOIN
    deleted_export_data_token
ON
    deleted_export_data_token.person_id = person.id
"""

Q_EXPORT_API_DATA = """
SELECT json_build_object(
    'person', (
        SELECT
            json_agg(row_to_json(t))
        FROM (
            SELECT
                person.*,
                gender.name AS gender_name,
                has_profile_picture.name AS has_profile_picture_name,
                verification_level.name AS verification_level_name,
                orientation.name AS orientation_name,
                ethnicity.name AS ethnicity_name,
                looking_for.name AS looking_for_name,
                smoking.name AS smoking_name,
                drinking.name AS drinking_name,
                drugs.name AS drugs_name,
                long_distance.name AS long_distance_name,
                relationship_status.name AS relationship_status_name,
                has_kids.name AS has_kids_name,
                wants_kids.name AS wants_kids_name,
                exercise.name AS exercise_name,
                religion.name AS religion_name,
                star_sign.name AS star_sign_name,
                unit.name AS unit_name,
                chats_notification.name AS chats_notification_name,
                intros_notification.name AS intros_notification_name,
                privacy_verification_level.name AS privacy_verification_level_name
            FROM
                person
            LEFT JOIN
                gender ON
                gender.id = gender_id
            LEFT JOIN
                yes_no AS
                has_profile_picture ON
                has_profile_picture.id = has_profile_picture_id
            LEFT JOIN
                verification_level ON
                verification_level.id = verification_level_id
            LEFT JOIN
                orientation ON
                orientation.id = person.id
            LEFT JOIN
                ethnicity ON
                ethnicity.id = ethnicity_id
            LEFT JOIN
                looking_for ON
                looking_for.id = looking_for_id
            LEFT JOIN
                yes_no_optional AS
                smoking ON
                smoking.id = smoking_id
            LEFT JOIN
                frequency AS
                drinking ON
                drinking.id = drinking_id
            LEFT JOIN
                yes_no_optional AS
                drugs ON
                drugs.id = drugs_id
            LEFT JOIN
                yes_no_optional AS
                long_distance ON
                long_distance.id = long_distance_id
            LEFT JOIN
                yes_no_optional AS
                relationship_status ON
                relationship_status.id = relationship_status_id
            LEFT JOIN
                yes_no_optional AS
                has_kids ON
                has_kids.id = has_kids_id
            LEFT JOIN
                yes_no_maybe AS
                wants_kids ON
                wants_kids.id = wants_kids_id
            LEFT JOIN
                frequency AS
                exercise ON
                exercise.id = exercise_id
            LEFT JOIN
                religion ON
                religion.id = religion_id
            LEFT JOIN
                star_sign ON
                star_sign.id = star_sign_id
            LEFT JOIN
                unit ON
                unit.id = unit_id
            LEFT JOIN
                immediacy AS
                chats_notification ON
                chats_notification.id = person.chats_notification
            LEFT JOIN
                immediacy AS
                intros_notification ON
                intros_notification.id = person.intros_notification
            LEFT JOIN
                verification_level AS
                privacy_verification_level ON
                privacy_verification_level.id = person.privacy_verification_level_id

            WHERE
                person.id = %(person_id)s
        ) AS t
    ),

    'photo', (
        SELECT
            json_agg(row_to_json(t))
        FROM (
            SELECT
                *,

                'https://user-images.duolicious.app/original-' ||
                    uuid ||
                    '.jpg' AS photo_url

            FROM
                photo
            WHERE
                person_id = %(person_id)s
            ORDER BY
                position
        ) AS t
    ),

    'answer', (
        SELECT
            json_agg(row_to_json(t))
        FROM (
            SELECT
                question.id,
                question.question,
                answer.answer,
                answer.public_
            FROM
                answer
            JOIN
                question
            ON
                question.id = question_id
            WHERE
                person_id = %(person_id)s
        ) AS t
    ),

    'verification_job', (
        SELECT
            json_agg(row_to_json(t))
        FROM (
            SELECT
                *,

                'https://user-images.duolicious.app/450-' ||
                    photo_uuid ||
                    '.jpg' AS photo_url
            FROM
                verification_job
            WHERE
                person_id = %(person_id)s
        ) AS t
    ),

    'person_club', (
        SELECT
            json_agg(row_to_json(person_club))
        FROM
            person_club
        WHERE
            person_id = %(person_id)s
    ),

    'skipped', (
        SELECT
            json_agg(row_to_json(skipped))
        FROM
            skipped
        WHERE
            subject_person_id = %(person_id)s
    ),

    'messaged', (
        SELECT
            json_agg(row_to_json(messaged))
        FROM
            messaged
        WHERE
            subject_person_id = %(person_id)s
    ),

    'mam_message', (
        SELECT
            json_agg(row_to_json(mam_message))
        FROM
            mam_message
        WHERE
            person_id = %(person_id)s
    ),

    'presence_histogram', (
        SELECT
            json_agg(row_to_json(presence_histogram))
        FROM
            presence_histogram
        WHERE
            person_id = %(person_id)s
    )

) AS j
"""

Q_MESSAGE_STATS = """
WITH message_sent AS (
    SELECT
        object_person_id AS other_person_id,
        created_at AS message_sent_at
    FROM
        messaged
    JOIN
        person
    ON
        person.id = messaged.subject_person_id
    WHERE
        person.uuid = %(prospect_uuid)s
), message_received AS (
    SELECT
        subject_person_id AS other_person_id,
        created_at AS message_received_at
    FROM
        messaged
    JOIN
        person
    ON
        person.id = messaged.object_person_id
    WHERE
        person.uuid = %(prospect_uuid)s
), conversation AS (
    SELECT
        message_sent_at,
        message_received_at
    FROM
        message_sent
    FULL OUTER JOIN
        message_received USING (other_person_id)
), absolute_numbers AS (
    SELECT
        count(*) FILTER (
            WHERE message_sent_at <= message_received_at)::real
            AS num_intros_sent_with_reply,

        count(*) FILTER (
            WHERE message_sent_at <= message_received_at
            OR message_received_at IS NULL)::real
            AS num_intros_sent,

        count(*) FILTER (
            WHERE message_received_at <= message_sent_at)::real
            AS num_intros_received_with_reply,

        count(*) FILTER (
            WHERE message_received_at <= message_sent_at
            OR message_sent_at IS NULL)::real
            AS num_intros_received
    FROM
        conversation
), is_account_new AS (
    SELECT
        sign_up_time > now() - interval '1 day' AS value
    FROM
        person
    WHERE
        person.uuid = %(prospect_uuid)s
)
SELECT
    CASE
        WHEN (SELECT value FROM is_account_new)
        THEN NULL

        WHEN num_intros_sent < 5
        THEN NULL

        ELSE ROUND(num_intros_sent_with_reply / num_intros_sent * 100)
    END AS gets_reply_percentage,

    CASE
        WHEN (SELECT value FROM is_account_new)
        THEN NULL

        WHEN num_intros_received < 5
        THEN NULL

        ELSE ROUND(num_intros_received_with_reply / num_intros_received * 100)
    END AS gives_reply_percentage
FROM
    absolute_numbers
"""


Q_VISITORS = """
WITH checker AS (
    SELECT
        id,
        personality,
        last_visitor_check_time,
        verification_level_id
    FROM
        person
    WHERE
        id = %(person_id)s
), visited_pass_1 AS (
    (
        SELECT
            *
        FROM
            visited
        WHERE
            subject_person_id = %(person_id)s
        ORDER BY
            updated_at DESC
        LIMIT
            150
    )
    UNION ALL
    (
        SELECT
            *
        FROM
            visited
        WHERE
            object_person_id = %(person_id)s
        ORDER BY
            updated_at DESC
        LIMIT
            150
    )
), visited_pass_2 AS (
    SELECT
        *,
        CASE
            WHEN subject_person_id = %(person_id)s
            THEN object_person_id
            ELSE subject_person_id
        END AS other_person_id
    FROM
        visited_pass_1
), visited_pass_3 AS (
    SELECT
        direction.kind AS direction_kind,

        prospect.uuid AS person_uuid,

        visitor_photo.blurhash AS photo_blurhash,

        visitor_photo.uuid AS photo_uuid,

        iso8601_utc(visited_pass_2.updated_at) AS time,

        prospect.name AS name,

        (
            SELECT EXTRACT(YEAR FROM AGE(prospect.date_of_birth))
            WHERE prospect.show_my_age
        ) AS age,

        gender.name AS gender,

        (
            SELECT prospect.location_short_friendly
            WHERE prospect.show_my_location
        ) AS location,

        prospect.verification_level_id > 1 AS is_verified,

        CLAMP(
            0,
            99,
            100 * (1 - (prospect.personality <#> checker.personality)) / 2
        )::SMALLINT AS match_percentage,

        CASE
            WHEN direction.kind = 'visited_you'
            THEN visited_pass_2.updated_at > checker.last_visitor_check_time
            ELSE FALSE
        END AS is_new,

        verification_required_to_view,

        visited_pass_2.updated_at AS order_time,

        visited_pass_2.invisible AS was_invisible
    FROM
        visited_pass_2
    JOIN
        person AS prospect
    ON
        prospect.id = visited_pass_2.other_person_id
    LEFT JOIN
        gender
    ON
        gender.id = prospect.gender_id
    LEFT JOIN
        checker
    ON
        TRUE
    LEFT JOIN LATERAL (
        SELECT
            CASE
                WHEN visited_pass_2.subject_person_id = %(person_id)s
                THEN 'you_visited'
                ELSE 'visited_you'
            END AS kind
    ) AS direction
    ON
        TRUE
    LEFT JOIN LATERAL (
        SELECT
            CASE
                WHEN
                    checker.verification_level_id >=
                    prospect.privacy_verification_level_id
                THEN NULL
                WHEN
                    prospect.privacy_verification_level_id = 2
                THEN 'basics'
                WHEN
                    prospect.privacy_verification_level_id = 3
                THEN 'photos'
            END AS verification_required_to_view
        FROM
            checker
    ) AS verification_required_to_view
    ON
        TRUE
    LEFT JOIN LATERAL (
        SELECT
            CASE
                WHEN verification_required_to_view IS NULL
                THEN photo.uuid
                ELSE NULL
            END AS uuid,
            photo.blurhash
        FROM
            photo
        WHERE
            photo.person_id = prospect.id
        ORDER BY
            photo.position
        LIMIT 1
    ) AS visitor_photo
    ON
        TRUE
    WHERE
        prospect.activated
    AND
        prospect.id <> %(person_id)s
    AND
        -- The prospect did not skip the checker
        NOT EXISTS (
            SELECT
                1
            FROM
                skipped
            WHERE
                subject_person_id = prospect.id
            AND
                object_person_id = %(person_id)s
        )
    AND
        -- The checker did not skip the prospect, or wishes to view skipped prospects
        (
            NOT EXISTS (
                SELECT
                    1
                FROM
                    skipped
                WHERE
                    subject_person_id = %(person_id)s
                AND
                    object_person_id = prospect.id
            )
        OR
            1 = (
                SELECT
                    skipped_id
                FROM
                    search_preference_skipped
                WHERE
                    person_id = %(person_id)s
            )
        )
    AND
        -- The prospect wants to be shown to strangers or isn't a stranger
        (
            EXISTS (
                SELECT
                    subject_person_id
                FROM
                    messaged
                WHERE
                    subject_person_id = prospect.id
                AND
                    object_person_id = %(person_id)s
            )
        OR
            NOT prospect.hide_me_from_strangers
        )
    AND
        (
            direction.kind = 'you_visited'
        OR
            NOT visited_pass_2.invisible
        )
    AND
        (
            NOT prospect.verification_required
        OR
            prospect.verification_level_id > 1
        )
)
SELECT
    json_build_object(
        'visited_you',
        COALESCE(
            jsonb_agg(
                (
                    to_jsonb(visited_pass_3)
                    - 'order_time'
                    - 'direction_kind'
                ) ORDER BY visited_pass_3.order_time DESC
            ) FILTER (WHERE direction_kind = 'visited_you'),
            '[]'::jsonb
        ),

        'you_visited',
        COALESCE(
            jsonb_agg(
                (
                    to_jsonb(visited_pass_3)
                    - 'order_time'
                    - 'direction_kind'
                ) ORDER BY visited_pass_3.order_time DESC
            ) FILTER (WHERE direction_kind = 'you_visited'),
            '[]'::jsonb
        ),

        'last_visited_at',
        iso8601_utc(MAX(visited_pass_3.order_time))
    ) AS j
FROM
    visited_pass_3
"""

Q_MARK_VISITORS_CHECKED = """
UPDATE
    person
SET
    last_visitor_check_time = GREATEST(
        LEAST(
            COALESCE(%(when)s::timestamp, NOW()),
            NOW()
        ),
        last_visitor_check_time
    )
WHERE
    id = %(person_id)s
"""
