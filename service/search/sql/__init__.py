from constants import ONLINE_RECENTLY_SECONDS
from commonsql import Q_COMPUTED_FLAIR

# How many feed results to send to the client per request
FEED_RESULTS_PER_PAGE = 50

# The inverse of the proportion of feed results to discard.
FEED_SELECTIVITY = 2


Q_SEARCH_PREFERENCE = f"""
SELECT
    id AS gender_id
FROM
    gender
WHERE
    name = CASE
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
"""



Q_UNCACHED_SEARCH_1 = """
DELETE FROM
    search_cache
WHERE
    searcher_person_id = %(searcher_person_id)s
"""



Q_UNCACHED_SEARCH_2 = """
WITH searcher AS (
    SELECT
        coordinates,
        personality,
        gender_id,
        looking_for_id,
        long_distance_id,
        relationship_status_id,
        pioneer_status,
        service_goals,
        willingness_to_relocate,
        family_worship_habit,
        spiritual_routine,
        willing_to_involve_family_early,
        open_to_chaperoned_video_calls,
        congregation_compatibility,
        service_lifestyle,
        life_stage,
        emotional_temperament,
        communication_style,
        has_kids_id,
        wants_kids_id,
        congregation_id,
        roles,
        COALESCE(roles, ARRAY[]::TEXT[]) && ARRAY['admin', 'bot'] AS searcher_is_admin,
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
        date_of_birth,
        count_answers
    FROM
        person
    WHERE
        person.id = %(searcher_person_id)s
), prospects_first_pass AS (
    SELECT
        id
    FROM
        person AS prospect
    CROSS JOIN
        searcher
    WHERE
        prospect.activated
    AND
        (
            searcher.searcher_is_admin
            OR
            searcher.gender_id NOT IN (1, 2)
            OR prospect.gender_id <> searcher.gender_id
        )
    AND
        -- The prospect meets the searcher's gender preference
        (
            searcher.searcher_is_admin
            OR prospect.gender_id = ANY(%(gender_preference)s::SMALLINT[])
        )
    AND
        -- The prospect meets the searcher's location preference
        (
            searcher.searcher_is_admin
            OR ST_DWithin(
                prospect.coordinates,
                searcher.coordinates,
                searcher.distance_preference
            )
        )

    LIMIT
        30000
), prospects_second_pass AS (
    SELECT id FROM prospects_first_pass
), prospects_third_pass AS (
    SELECT
        prospect.id
    FROM
        person AS prospect
    JOIN
        prospects_second_pass
    ON
        prospects_second_pass.id = prospect.id
    CROSS JOIN
        searcher
    ORDER BY
        prospect.personality <#> searcher.personality
    LIMIT
        10000
), prospects_fourth_pass AS (
    SELECT
        prospect.id AS prospect_person_id,

        uuid AS prospect_uuid,

        name,

        prospect.personality,

        verification_level_id > 1 AS verified,

        (
            SELECT
                uuid
            FROM
                photo
            WHERE
                person_id = prospect.id
            ORDER BY
                position
            LIMIT 1
        ) AS profile_photo_uuid,

        CASE
            WHEN show_my_age
            THEN EXTRACT(YEAR FROM AGE(prospect.date_of_birth))
            ELSE NULL
        END AS age,

        CLAMP(
            0,
            99,
            (
                (
                    100 * (1 - (prospect.personality <#> searcher.personality)) / 2
                ) * 0.45
                + COALESCE(
                    (
                        100.0 * values_alignment.matched_traits
                        / NULLIF(values_alignment.compared_traits, 0)
                    ),
                    100 * (1 - (prospect.personality <#> searcher.personality)) / 2
                ) * 0.55
            )
        ) AS match_percentage,

        CLAMP(
            0,
            100,
            45
            + CASE WHEN prospect.verification_level_id > 1 THEN 20 ELSE 0 END
            + CASE WHEN (
                SELECT COUNT(*)
                FROM photo
                WHERE photo.person_id = prospect.id
            ) >= 3 THEN 15 ELSE 0 END
            + CASE WHEN EXISTS (
                SELECT 1
                FROM audio
                WHERE audio.person_id = prospect.id
            ) THEN 5 ELSE 0 END
            + CASE WHEN COALESCE(prospect.count_answers, 0) >= 25 THEN 5 ELSE 0 END
            + CASE WHEN prospect.referred_by_code_id IS NOT NULL THEN 5 ELSE 0 END
            + CASE WHEN prospect.sign_up_time < NOW() - INTERVAL '30 days' THEN 5 ELSE 0 END
            - CASE
                WHEN COALESCE(prospect.profile_status, 'active') = 'serious' THEN 10
                WHEN COALESCE(prospect.profile_status, 'active') = 'paused' THEN 5
                ELSE 0
            END
            - CASE
                WHEN COALESCE(prospect.waitlist_status, 'active') <> 'active' THEN 15
                ELSE 0
            END
            - LEAST(
                30,
                12 * (
                    SELECT COUNT(*)
                    FROM antiabuse_flag
                    WHERE
                        antiabuse_flag.person_id = prospect.id
                    AND antiabuse_flag.status = 'resolved'
                    AND antiabuse_flag.resolution IN ('warning', 'temporary_restriction', 'permanent_ban')
                )
            )
            - LEAST(
                20,
                8 * (
                    SELECT COUNT(*)
                    FROM antiabuse_flag
                    WHERE
                        antiabuse_flag.person_id = prospect.id
                    AND antiabuse_flag.status IN ('open', 'reviewing')
                )
            )
        ) AS trust_score,

        prospect.roles

    FROM
        person AS prospect
    JOIN
        prospects_third_pass
    ON
        prospects_third_pass.id = prospect.id
    CROSS JOIN
        searcher
    LEFT JOIN LATERAL (
        SELECT
            (
                CASE WHEN prospect.spiritual_routine IS NOT NULL AND searcher.spiritual_routine IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN prospect.family_worship_habit IS NOT NULL AND searcher.family_worship_habit IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN prospect.looking_for_id IS NOT NULL AND searcher.looking_for_id IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN prospect.relationship_status_id IS NOT NULL AND searcher.relationship_status_id IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN prospect.pioneer_status IS NOT NULL AND searcher.pioneer_status IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN prospect.service_lifestyle IS NOT NULL AND searcher.service_lifestyle IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN prospect.life_stage IS NOT NULL AND searcher.life_stage IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN prospect.emotional_temperament IS NOT NULL AND searcher.emotional_temperament IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN prospect.communication_style IS NOT NULL AND searcher.communication_style IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN prospect.long_distance_id IS NOT NULL AND searcher.long_distance_id IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN prospect.willingness_to_relocate IS NOT NULL AND searcher.willingness_to_relocate IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN prospect.willing_to_involve_family_early IS NOT NULL AND searcher.willing_to_involve_family_early IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN prospect.open_to_chaperoned_video_calls IS NOT NULL AND searcher.open_to_chaperoned_video_calls IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN prospect.congregation_compatibility IS NOT NULL AND searcher.congregation_compatibility IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN prospect.has_kids_id IS NOT NULL AND searcher.has_kids_id IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN prospect.wants_kids_id IS NOT NULL AND searcher.wants_kids_id IS NOT NULL THEN 1 ELSE 0 END +
                CASE WHEN prospect.congregation_id IS NOT NULL AND searcher.congregation_id IS NOT NULL THEN 1 ELSE 0 END
            ) AS compared_traits,
            (
                CASE WHEN prospect.spiritual_routine IS NOT NULL AND searcher.spiritual_routine IS NOT NULL AND prospect.spiritual_routine = searcher.spiritual_routine THEN 1 ELSE 0 END +
                CASE WHEN prospect.family_worship_habit IS NOT NULL AND searcher.family_worship_habit IS NOT NULL AND prospect.family_worship_habit = searcher.family_worship_habit THEN 1 ELSE 0 END +
                CASE WHEN prospect.looking_for_id IS NOT NULL AND searcher.looking_for_id IS NOT NULL AND prospect.looking_for_id = searcher.looking_for_id THEN 1 ELSE 0 END +
                CASE WHEN prospect.relationship_status_id IS NOT NULL AND searcher.relationship_status_id IS NOT NULL AND prospect.relationship_status_id = searcher.relationship_status_id THEN 1 ELSE 0 END +
                CASE WHEN prospect.pioneer_status IS NOT NULL AND searcher.pioneer_status IS NOT NULL AND prospect.pioneer_status = searcher.pioneer_status THEN 1 ELSE 0 END +
                CASE WHEN prospect.service_lifestyle IS NOT NULL AND searcher.service_lifestyle IS NOT NULL AND prospect.service_lifestyle = searcher.service_lifestyle THEN 1 ELSE 0 END +
                CASE WHEN prospect.life_stage IS NOT NULL AND searcher.life_stage IS NOT NULL AND prospect.life_stage = searcher.life_stage THEN 1 ELSE 0 END +
                CASE WHEN prospect.emotional_temperament IS NOT NULL AND searcher.emotional_temperament IS NOT NULL AND prospect.emotional_temperament = searcher.emotional_temperament THEN 1 ELSE 0 END +
                CASE WHEN prospect.communication_style IS NOT NULL AND searcher.communication_style IS NOT NULL AND prospect.communication_style = searcher.communication_style THEN 1 ELSE 0 END +
                CASE WHEN prospect.long_distance_id IS NOT NULL AND searcher.long_distance_id IS NOT NULL AND prospect.long_distance_id = searcher.long_distance_id THEN 1 ELSE 0 END +
                CASE WHEN prospect.willingness_to_relocate IS NOT NULL AND searcher.willingness_to_relocate IS NOT NULL AND prospect.willingness_to_relocate = searcher.willingness_to_relocate THEN 1 ELSE 0 END +
                CASE WHEN prospect.willing_to_involve_family_early IS NOT NULL AND searcher.willing_to_involve_family_early IS NOT NULL AND prospect.willing_to_involve_family_early = searcher.willing_to_involve_family_early THEN 1 ELSE 0 END +
                CASE WHEN prospect.open_to_chaperoned_video_calls IS NOT NULL AND searcher.open_to_chaperoned_video_calls IS NOT NULL AND prospect.open_to_chaperoned_video_calls = searcher.open_to_chaperoned_video_calls THEN 1 ELSE 0 END +
                CASE WHEN prospect.congregation_compatibility IS NOT NULL AND searcher.congregation_compatibility IS NOT NULL AND prospect.congregation_compatibility = searcher.congregation_compatibility THEN 1 ELSE 0 END +
                CASE WHEN prospect.has_kids_id IS NOT NULL AND searcher.has_kids_id IS NOT NULL AND prospect.has_kids_id = searcher.has_kids_id THEN 1 ELSE 0 END +
                CASE WHEN prospect.wants_kids_id IS NOT NULL AND searcher.wants_kids_id IS NOT NULL AND prospect.wants_kids_id = searcher.wants_kids_id THEN 1 ELSE 0 END +
                CASE WHEN prospect.congregation_id IS NOT NULL AND searcher.congregation_id IS NOT NULL AND prospect.congregation_id = searcher.congregation_id THEN 1 ELSE 0 END
            ) AS matched_traits
    ) AS values_alignment
    ON TRUE

    WHERE (
        -- The searcher meets the prospect's gender preference or
        -- the searcher is searching with in a club
        searcher.searcher_is_admin
        OR
        EXISTS (
            SELECT
                1
            FROM
                search_preference_gender AS preference
            WHERE
                preference.person_id = prospect.id
            AND
                preference.gender_id = searcher.gender_id
        )
    )

    AND (
        -- The searcher meets the prospect's location preference or
        -- the searcher is an admin
        searcher.searcher_is_admin
        OR ST_DWithin(
            prospect.coordinates,
            searcher.coordinates,
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
    )

    AND (
        searcher.searcher_is_admin
        OR (
       -- The prospect meets the searcher's age preference
            prospect.date_of_birth <= (
            SELECT
                CURRENT_DATE -
                INTERVAL '1 year' *
                COALESCE(min_age, 0)
            FROM
                search_preference_age
            WHERE
                person_id = %(searcher_person_id)s
        )::DATE
    AND
        prospect.date_of_birth > (
            SELECT
                CURRENT_DATE -
                INTERVAL '1 year' *
                (COALESCE(max_age, 999) + 1)
            FROM
                search_preference_age
            WHERE
                person_id = %(searcher_person_id)s
        )::DATE

    -- The searcher meets the prospect's age preference or
    -- the searcher is searching within a club
    AND (
       EXISTS (
            SELECT 1
            FROM search_preference_age AS preference
            WHERE
                preference.person_id = prospect.id
            AND
                searcher.date_of_birth <= (
                    CURRENT_DATE -
                    INTERVAL '1 year' *
                    COALESCE(preference.min_age, 0)
                )
            AND
                searcher.date_of_birth > (
                    CURRENT_DATE -
                    INTERVAL '1 year' *
                    (COALESCE(preference.max_age, 999) + 1)
                )
        )
    )

    -- The users have at least a 50%% match
    AND (
        (prospect.personality <#> searcher.personality) < 1e-5
    )

    -- One-way filters
    AND
        prospect.ethnicity_id IN (
            SELECT
                ethnicity_id
            FROM
                search_preference_ethnicity
            WHERE
                person_id = %(searcher_person_id)s
        )
    AND (
        NOT EXISTS (
            SELECT 1
            FROM search_preference_city
            WHERE person_id = %(searcher_person_id)s
        )
        OR LOWER(BTRIM(SPLIT_PART(COALESCE(prospect.location_long_friendly, ''), ',', 1))) = LOWER(
            (
                SELECT city
                FROM search_preference_city
                WHERE person_id = %(searcher_person_id)s
            )
        )
    )
    AND (
        NOT EXISTS (
            SELECT 1
            FROM search_preference_state
            WHERE person_id = %(searcher_person_id)s
        )
        OR LOWER(BTRIM(SPLIT_PART(COALESCE(prospect.location_long_friendly, ''), ',', 2))) = LOWER(
            (
                SELECT state
                FROM search_preference_state
                WHERE person_id = %(searcher_person_id)s
            )
        )
    )
    AND
        COALESCE(prospect.height_cm, 0) >= COALESCE(
            (
                SELECT
                    min_height_cm
                FROM
                    search_preference_height_cm
                WHERE
                    person_id = %(searcher_person_id)s
            ),
            0
        )
    AND
        COALESCE(prospect.height_cm, 999) <= COALESCE(
            (
                SELECT
                    max_height_cm
                FROM
                    search_preference_height_cm
                WHERE
                    person_id = %(searcher_person_id)s
            ),
            999
        )
    AND
        prospect.has_profile_picture_id IN (
            SELECT
                has_profile_picture_id
            FROM
                search_preference_has_profile_picture
            WHERE
                person_id = %(searcher_person_id)s
        )
    AND
        prospect.drinking_id IN (
            SELECT
                drinking_id
            FROM
                search_preference_drinking
            WHERE
                person_id = %(searcher_person_id)s
        )
    AND
        prospect.long_distance_id IN (
            SELECT
                long_distance_id
            FROM
                search_preference_long_distance
            WHERE
                person_id = %(searcher_person_id)s
        )
    AND
        prospect.relationship_status_id IN (
            SELECT
                relationship_status_id
            FROM
                search_preference_relationship_status
            WHERE
                person_id = %(searcher_person_id)s
        )
    AND (
        NOT EXISTS (
            SELECT 1
            FROM search_preference_pioneer_status
            WHERE person_id = %(searcher_person_id)s
        )
        OR COALESCE(prospect.pioneer_status, 'Unanswered') IN (
            SELECT status
            FROM search_preference_pioneer_status
            WHERE person_id = %(searcher_person_id)s
        )
    )
    AND
        prospect.has_kids_id IN (
            SELECT
                has_kids_id
            FROM
                search_preference_has_kids
            WHERE
                person_id = %(searcher_person_id)s
        )
    AND
        prospect.wants_kids_id IN (
            SELECT
                wants_kids_id
            FROM
                search_preference_wants_kids
            WHERE
                person_id = %(searcher_person_id)s
        )
    AND
        prospect.exercise_id IN (
            SELECT
                exercise_id
            FROM
                search_preference_exercise
            WHERE
                person_id = %(searcher_person_id)s
        )
    AND
        prospect.baptism_date <= CURRENT_DATE - (
            COALESCE(
                (
                    SELECT min_baptism_years
                    FROM search_preference_baptism_years
                    WHERE person_id = %(searcher_person_id)s
                ),
                2
            ) || ' years'
        )::INTERVAL
    AND
        -- The prospect wants to be shown to strangers or isn't a stranger
        (
            prospect.id IN (
                SELECT
                    subject_person_id
                FROM
                    messaged
                WHERE
                    object_person_id = %(searcher_person_id)s
            )
        OR
            NOT prospect.hide_me_from_strangers
        )
    AND
        -- The prospect did not skip the searcher
        prospect.id NOT IN (
            SELECT
                subject_person_id
            FROM
                skipped
            WHERE
                object_person_id = %(searcher_person_id)s
        )
    AND
        -- The searcher did not skip the prospect, or the searcher wishes to
        -- view skipped prospects
        (
            prospect.id NOT IN (
                SELECT
                    object_person_id
                FROM
                    skipped
                WHERE
                    subject_person_id = %(searcher_person_id)s
            )
        OR
            1 IN (
                SELECT
                    skipped_id
                FROM
                    search_preference_skipped
                WHERE
                    person_id = %(searcher_person_id)s
            )
        )
    AND
        -- The searcher did not message the prospect, or the searcher wishes to
        -- view messaged prospects
        (
            prospect.id NOT IN (
                SELECT
                    object_person_id
                FROM
                    messaged
                WHERE
                    subject_person_id = %(searcher_person_id)s
            )
        OR
            1 IN (
                SELECT
                    messaged_id
                FROM
                    search_preference_messaged
                WHERE
                    person_id = %(searcher_person_id)s
            )
        )
    AND
        -- NOT EXISTS an answer contrary to the searcher's preference...
        NOT EXISTS (
            SELECT 1
            FROM (
                SELECT *
                FROM search_preference_answer
                WHERE person_id = %(searcher_person_id)s
            ) AS pref
            LEFT JOIN
                answer ans
            ON
                ans.person_id = prospect.id AND
                ans.question_id = pref.question_id
            WHERE
                -- Contrary because the answer exists and is wrong
                ans.answer IS NOT NULL AND
                ans.answer != pref.answer
            OR
                -- Contrary because the answer doesn't exist but should
                ans.answer IS NULL AND
                pref.accept_unanswered = FALSE
        )
        )
    )

    -- Exclude users who should be verified but aren't
    AND (
            prospect.verification_level_id > 1
        OR
            NOT prospect.verification_required
    )
    AND COALESCE(prospect.profile_status, 'active') = 'active'
    AND COALESCE(prospect.waitlist_status, 'active') = 'active'
    AND (
        SELECT COUNT(*)
        FROM antiabuse_flag
        WHERE
            antiabuse_flag.person_id = prospect.id
        AND antiabuse_flag.status IN ('open', 'reviewing')
    ) < 2
    AND (
        SELECT COUNT(*)
        FROM skipped
        WHERE
            skipped.object_person_id = prospect.id
        AND skipped.reported
        AND skipped.report_reason <> ''
        AND skipped.created_at > NOW() - INTERVAL '30 days'
    ) < COALESCE(
        (SELECT NULLIF(value, '')::INT FROM admin_setting WHERE key = 'system_temporary_ban_report_threshold'),
        3
    )
    AND prospect.sign_in_time > NOW() - (
        COALESCE(
            (SELECT NULLIF(value, '')::INT FROM admin_setting WHERE key = 'system_inactive_hide_days'),
            45
        ) || ' days'
    )::interval

    ORDER BY
        -- If this is changed, other subqueries will need changing too
        verified DESC,
        trust_score DESC,
        match_percentage DESC

    LIMIT
        -- 500 + 2. The two extra records are the searcher and the moderation
        -- bot, which we'll filter out later so that we have 500 records to show
        -- the user. We don't filer them here to reduce the number of checks we
        -- need to do for 'bot' or 'self' status.
        502
), do_promote_verified AS (
    SELECT
        count(*) >= 250 AS x
    FROM
        prospects_fourth_pass
    WHERE
        profile_photo_uuid IS NOT NULL
    AND
        verified
    AND
        (SELECT count_answers > 0 FROM searcher)
)
INSERT INTO search_cache (
    searcher_person_id,
    position,
    prospect_person_id,
    prospect_uuid,
    profile_photo_uuid,
    name,
    age,
    match_percentage,
    personality,
    verified
)
SELECT
    %(searcher_person_id)s,
    ROW_NUMBER() OVER (
        ORDER BY
            -- If this is changed, other subqueries will need changing too
            CASE
                WHEN (SELECT x FROM do_promote_verified)
                THEN
                    profile_photo_uuid IS NOT NULL AND verified
                ELSE
                    profile_photo_uuid IS NOT NULL
            END DESC,

            trust_score DESC,
            match_percentage DESC
    ) AS position,
    prospect_person_id,
    prospect_uuid,
    profile_photo_uuid,
    name,
    age,
    match_percentage,
    personality,
    verified
FROM
    prospects_fourth_pass
WHERE
    prospects_fourth_pass.prospect_person_id != %(searcher_person_id)s
AND
    'bot' <> ALL(prospects_fourth_pass.roles)
ORDER BY
    position
LIMIT
    500
ON CONFLICT (searcher_person_id, position) DO UPDATE SET
    searcher_person_id = EXCLUDED.searcher_person_id,
    position = EXCLUDED.position,
    prospect_person_id = EXCLUDED.prospect_person_id,
    prospect_uuid = EXCLUDED.prospect_uuid,
    profile_photo_uuid = EXCLUDED.profile_photo_uuid,
    name = EXCLUDED.name,
    age = EXCLUDED.age,
    match_percentage = EXCLUDED.match_percentage,
    personality = EXCLUDED.personality,
    verified = EXCLUDED.verified
"""



Q_CACHED_SEARCH = """
WITH page AS (
    SELECT
        prospect_person_id,
        prospect_uuid,
        profile_photo_uuid,
        (
            SELECT blurhash
            FROM photo
            WHERE profile_photo_uuid = photo.uuid
            LIMIT 1
        ) AS profile_photo_blurhash,
        name,
        age,
        match_percentage,
        EXISTS (
            SELECT
                1
            FROM
                messaged
            WHERE
                subject_person_id = %(searcher_person_id)s
            AND
                object_person_id = prospect_person_id
        ) AS person_messaged_prospect,
        EXISTS (
            SELECT
                1
            FROM
                messaged
            WHERE
                subject_person_id = prospect_person_id
            AND
                object_person_id = %(searcher_person_id)s
        ) AS prospect_messaged_person,
        verified,
        (
            SELECT
                CASE
                    WHEN COALESCE(person.roles, ARRAY[]::TEXT[]) && ARRAY['admin', 'bot']
                    THEN 999
                    ELSE verification_level_id
                END
            FROM
                person
            WHERE
                id = %(searcher_person_id)s
        ) AS searcher_verification_level_id,
        (
            SELECT
                privacy_verification_level_id
            FROM
                person
            WHERE
                id = prospect_person_id
        ) AS privacy_verification_level_id
    FROM
        search_cache
    WHERE
        searcher_person_id = %(searcher_person_id)s AND
        position >  %(o)s AND
        position <= %(o)s + %(n)s
    ORDER BY
        position
)
SELECT
    public_page.profile_photo_blurhash,
    public_page.name,
    public_page.age,
    public_page.match_percentage,
    public_page.person_messaged_prospect,
    public_page.prospect_messaged_person,
    public_page.verified,
    public_page.verification_required_to_view,

    private_page.prospect_person_id,
    private_page.prospect_uuid,
    private_page.profile_photo_uuid
FROM
    (
        SELECT
            *,

            CASE
                WHEN
                    searcher_verification_level_id >=
                    privacy_verification_level_id
                THEN NULL
                WHEN
                    privacy_verification_level_id = 2
                THEN 'basics'
                WHEN
                    privacy_verification_level_id = 3
                THEN 'photos'
            END AS verification_required_to_view
        FROM
            page
    ) AS public_page
LEFT JOIN
    (
        SELECT
            *
        FROM
            page
        WHERE
            searcher_verification_level_id >= privacy_verification_level_id
    ) AS private_page
ON
    private_page.prospect_person_id = public_page.prospect_person_id
"""

Q_QUIZ_SEARCH = f"""
WITH searcher AS (
    SELECT
        personality,
        count_answers
    FROM
        person
    WHERE
        person.id = %(searcher_person_id)s
), do_promote_verified AS (
    SELECT
        count(*) >= 250 AS x
    FROM
        search_cache,
        searcher
    WHERE
        searcher_person_id = %(searcher_person_id)s
    AND
        profile_photo_uuid IS NOT NULL
    AND
        verified
    AND
        (SELECT count_answers > 0 FROM searcher)
), page AS (
    SELECT
        prospect_person_id,
        prospect_uuid,
        profile_photo_uuid,
        (
            SELECT blurhash
            FROM photo
            WHERE profile_photo_uuid = photo.uuid
            LIMIT 1
        ) AS profile_photo_blurhash,
        name,
        age,
        CLAMP(
            0,
            99,
            100 * (1 - (personality <#> (SELECT personality FROM searcher))) / 2
        )::SMALLINT AS match_percentage,
        (
            SELECT
                CASE
                    WHEN COALESCE(person.roles, ARRAY[]::TEXT[]) && ARRAY['admin', 'bot']
                    THEN 999
                    ELSE verification_level_id
                END
            FROM
                person
            WHERE
                id = %(searcher_person_id)s
        ) AS searcher_verification_level_id,
        (
            SELECT
                privacy_verification_level_id
            FROM
                person
            WHERE
                id = prospect_person_id
        ) AS privacy_verification_level_id
    FROM
        search_cache
    WHERE
        searcher_person_id = %(searcher_person_id)s
    ORDER BY
        -- If this is changed, other subqueries will need changing too
        CASE
            WHEN (SELECT x FROM do_promote_verified)
            THEN
                profile_photo_uuid IS NOT NULL AND verified
            ELSE
                profile_photo_uuid IS NOT NULL
        END DESC,

        match_percentage DESC
    LIMIT
        1
)
SELECT
    public_page.profile_photo_blurhash,
    public_page.name,
    public_page.age,
    public_page.match_percentage,
    public_page.verification_required_to_view,

    private_page.prospect_person_id,
    private_page.prospect_uuid,
    private_page.profile_photo_uuid
FROM
    (
        SELECT
            *,

            CASE
                WHEN
                    searcher_verification_level_id >=
                    privacy_verification_level_id
                THEN NULL
                WHEN
                    privacy_verification_level_id = 2
                THEN 'basics'
                WHEN
                    privacy_verification_level_id = 3
                THEN 'photos'
            END AS verification_required_to_view
        FROM
            page
    ) AS public_page
LEFT JOIN
    (
        SELECT
            *
        FROM
            page
        WHERE
            searcher_verification_level_id >= privacy_verification_level_id
    ) AS private_page
ON
    private_page.prospect_person_id = public_page.prospect_person_id
"""

Q_FEED = f"""
WITH searcher AS (
    SELECT
        id as searcher_id,
        gender_id,
        date_of_birth,
        personality,
        looking_for_id,
        long_distance_id,
        relationship_status_id,
        pioneer_status,
        service_goals,
        willingness_to_relocate,
        family_worship_habit,
        spiritual_routine,
        willing_to_involve_family_early,
        open_to_chaperoned_video_calls,
        congregation_compatibility,
        service_lifestyle,
        life_stage,
        emotional_temperament,
        communication_style,
        has_kids_id,
        wants_kids_id,
        congregation_id,
        verification_level_id,
        roles AS searcher_roles
    FROM
        person
    WHERE
        person.id = %(searcher_person_id)s
), recent_person AS (
    (
        SELECT
            *
        FROM
            person
        WHERE
            last_online_time < %(before)s
        ORDER BY
            last_online_time DESC
        LIMIT
            5000
    )

    UNION DISTINCT

    (
        SELECT
            *
        FROM
            person
        WHERE
            last_event_time < %(before)s
        ORDER BY
            last_event_time DESC
        LIMIT
            5000
    )
), person_data AS (
    SELECT
        prospect.id,
        prospect.uuid AS person_uuid,
        prospect.name,
        prospect.gender_id,
        photo_data.blurhash AS photo_blurhash,
        photo_data.uuid AS photo_uuid,
        prospect.verification_level_id > 1 AS is_verified,
        mapped_last_online_time,
        mapped_last_event_name,
        mapped_last_event_data,
        CLAMP(
            0,
            99,
            0.45 * (
                100 * (
                    1 - (prospect.personality <#> searcher.personality)
                ) / 2
            ) + 0.55 * COALESCE(
                (
                    SELECT
                        100.0 * values_alignment.matched_traits
                        / NULLIF(values_alignment.compared_traits, 0)
                ),
                50
            )
        )::SMALLINT AS match_percentage,
        flair,
        prospect.email,
        prospect.roles,
        TRUE AS has_gold,
        sign_up_time,
        prospect.referred_by_code_id,
        count_answers,
        about,
        (
            SELECT EXTRACT(YEAR FROM AGE(prospect.date_of_birth))
            WHERE prospect.show_my_age
        ) AS age,
        gender.name AS gender,
        (
            SELECT prospect.location_short_friendly
            WHERE prospect.show_my_location
        ) AS location
    FROM
        recent_person AS prospect
    JOIN
        gender
    ON
        gender.id = prospect.gender_id
    LEFT JOIN LATERAL (
        SELECT
            photo.uuid,
            photo.blurhash,
            photo.nsfw_score
        FROM
            photo
        WHERE
            photo.person_id = prospect.id
        ORDER BY
            photo.position
        LIMIT 1
    ) AS photo_data
    ON TRUE
    LEFT JOIN LATERAL (
        SELECT
            photo.uuid,
            photo.blurhash,
            photo.nsfw_score,
            photo.extra_exts
        FROM
            photo
        WHERE
            photo.person_id = prospect.id
        ORDER BY
            '{{}}'::TEXT[] = extra_exts,
            photo.uuid = photo_data.uuid,
            random()
        LIMIT 1
    ) AS added_photo_data
    ON TRUE
    LEFT JOIN LATERAL (
        SELECT
            prospect.last_online_time
            > now() - interval '{ONLINE_RECENTLY_SECONDS} seconds'
            AS was_recently_online
    )
    ON TRUE
    LEFT JOIN LATERAL (
        SELECT
            CASE

            WHEN was_recently_online AND last_event_name = 'added-photo'
            THEN 'recently-online-with-photo'

            WHEN was_recently_online AND last_event_name = 'added-voice-bio'
            THEN 'recently-online-with-voice-bio'

            WHEN was_recently_online AND last_event_name = 'updated-bio'
            THEN 'recently-online-with-bio'

            WHEN was_recently_online AND added_photo_data.uuid IS NOT NULL
            THEN 'recently-online-with-photo'

            WHEN last_event_name = 'recently-online-with-photo'
            THEN 'added-photo'

            WHEN last_event_name = 'recently-online-with-voice-bio'
            THEN 'added-voice-bio'

            WHEN last_event_name = 'recently-online-with-bio'
            THEN 'updated-bio'

            ELSE last_event_name

            END::person_event AS mapped_last_event_name
    ) AS mapped_last_event_name
    ON TRUE
    LEFT JOIN LATERAL (
        SELECT
            CASE
                WHEN
                    was_recently_online AND mapped_last_event_name <> 'joined'
                THEN
                    prospect.last_online_time
                ELSE
                    prospect.last_event_time
            END AS mapped_last_online_time
    ) AS mapped_last_online_time
    ON TRUE
    LEFT JOIN LATERAL (
        SELECT
            CASE

            WHEN was_recently_online AND last_event_name = 'added-photo'
            THEN last_event_data

            WHEN was_recently_online AND last_event_name = 'added-voice-bio'
            THEN last_event_data

            WHEN was_recently_online AND last_event_name = 'updated-bio'
            THEN last_event_data

            WHEN was_recently_online AND added_photo_data.uuid IS NOT NULL
            THEN jsonb_build_object(
                'added_photo_uuid', added_photo_data.uuid,
                'added_photo_blurhash', added_photo_data.blurhash,
                'added_photo_extra_exts', added_photo_data.extra_exts
            )

            ELSE last_event_data

            END::JSONB AS mapped_last_event_data
    ) AS mapped_last_event_data
    ON TRUE
    CROSS JOIN
        searcher
    LEFT JOIN LATERAL (
        SELECT
            (
                CASE WHEN searcher.spiritual_routine IS NOT NULL
                       AND prospect.spiritual_routine IS NOT NULL THEN 1 ELSE 0 END
                + CASE WHEN searcher.family_worship_habit IS NOT NULL
                       AND prospect.family_worship_habit IS NOT NULL THEN 1 ELSE 0 END
                + CASE WHEN searcher.looking_for_id IS NOT NULL
                       AND prospect.looking_for_id IS NOT NULL THEN 1 ELSE 0 END
                + CASE WHEN searcher.relationship_status_id IS NOT NULL
                       AND prospect.relationship_status_id IS NOT NULL THEN 1 ELSE 0 END
                + CASE WHEN searcher.pioneer_status IS NOT NULL
                       AND prospect.pioneer_status IS NOT NULL THEN 1 ELSE 0 END
                + CASE WHEN searcher.service_lifestyle IS NOT NULL
                       AND prospect.service_lifestyle IS NOT NULL THEN 1 ELSE 0 END
                + CASE WHEN searcher.life_stage IS NOT NULL
                       AND prospect.life_stage IS NOT NULL THEN 1 ELSE 0 END
                + CASE WHEN searcher.emotional_temperament IS NOT NULL
                       AND prospect.emotional_temperament IS NOT NULL THEN 1 ELSE 0 END
                + CASE WHEN searcher.communication_style IS NOT NULL
                       AND prospect.communication_style IS NOT NULL THEN 1 ELSE 0 END
                + CASE WHEN searcher.long_distance_id IS NOT NULL
                       AND prospect.long_distance_id IS NOT NULL THEN 1 ELSE 0 END
                + CASE WHEN searcher.willingness_to_relocate IS NOT NULL
                       AND prospect.willingness_to_relocate IS NOT NULL THEN 1 ELSE 0 END
                + CASE WHEN searcher.willing_to_involve_family_early IS NOT NULL
                       AND prospect.willing_to_involve_family_early IS NOT NULL THEN 1 ELSE 0 END
                + CASE WHEN searcher.open_to_chaperoned_video_calls IS NOT NULL
                       AND prospect.open_to_chaperoned_video_calls IS NOT NULL THEN 1 ELSE 0 END
                + CASE WHEN searcher.congregation_compatibility IS NOT NULL
                       AND prospect.congregation_compatibility IS NOT NULL THEN 1 ELSE 0 END
                + CASE WHEN searcher.has_kids_id IS NOT NULL
                       AND prospect.has_kids_id IS NOT NULL THEN 1 ELSE 0 END
                + CASE WHEN searcher.wants_kids_id IS NOT NULL
                       AND prospect.wants_kids_id IS NOT NULL THEN 1 ELSE 0 END
                + CASE WHEN searcher.congregation_id IS NOT NULL
                       AND prospect.congregation_id IS NOT NULL THEN 1 ELSE 0 END
            ) AS compared_traits,
            (
                CASE WHEN searcher.spiritual_routine IS NOT NULL
                       AND prospect.spiritual_routine IS NOT NULL
                       AND searcher.spiritual_routine = prospect.spiritual_routine THEN 1 ELSE 0 END
                + CASE WHEN searcher.family_worship_habit IS NOT NULL
                       AND prospect.family_worship_habit IS NOT NULL
                       AND searcher.family_worship_habit = prospect.family_worship_habit THEN 1 ELSE 0 END
                + CASE WHEN searcher.looking_for_id IS NOT NULL
                       AND prospect.looking_for_id IS NOT NULL
                       AND searcher.looking_for_id = prospect.looking_for_id THEN 1 ELSE 0 END
                + CASE WHEN searcher.relationship_status_id IS NOT NULL
                       AND prospect.relationship_status_id IS NOT NULL
                       AND searcher.relationship_status_id = prospect.relationship_status_id THEN 1 ELSE 0 END
                + CASE WHEN searcher.pioneer_status IS NOT NULL
                       AND prospect.pioneer_status IS NOT NULL
                       AND searcher.pioneer_status = prospect.pioneer_status THEN 1 ELSE 0 END
                + CASE WHEN searcher.service_lifestyle IS NOT NULL
                       AND prospect.service_lifestyle IS NOT NULL
                       AND searcher.service_lifestyle = prospect.service_lifestyle THEN 1 ELSE 0 END
                + CASE WHEN searcher.life_stage IS NOT NULL
                       AND prospect.life_stage IS NOT NULL
                       AND searcher.life_stage = prospect.life_stage THEN 1 ELSE 0 END
                + CASE WHEN searcher.emotional_temperament IS NOT NULL
                       AND prospect.emotional_temperament IS NOT NULL
                       AND searcher.emotional_temperament = prospect.emotional_temperament THEN 1 ELSE 0 END
                + CASE WHEN searcher.communication_style IS NOT NULL
                       AND prospect.communication_style IS NOT NULL
                       AND searcher.communication_style = prospect.communication_style THEN 1 ELSE 0 END
                + CASE WHEN searcher.long_distance_id IS NOT NULL
                       AND prospect.long_distance_id IS NOT NULL
                       AND searcher.long_distance_id = prospect.long_distance_id THEN 1 ELSE 0 END
                + CASE WHEN searcher.willingness_to_relocate IS NOT NULL
                       AND prospect.willingness_to_relocate IS NOT NULL
                       AND searcher.willingness_to_relocate = prospect.willingness_to_relocate THEN 1 ELSE 0 END
                + CASE WHEN searcher.willing_to_involve_family_early IS NOT NULL
                       AND prospect.willing_to_involve_family_early IS NOT NULL
                       AND searcher.willing_to_involve_family_early = prospect.willing_to_involve_family_early THEN 1 ELSE 0 END
                + CASE WHEN searcher.open_to_chaperoned_video_calls IS NOT NULL
                       AND prospect.open_to_chaperoned_video_calls IS NOT NULL
                       AND searcher.open_to_chaperoned_video_calls = prospect.open_to_chaperoned_video_calls THEN 1 ELSE 0 END
                + CASE WHEN searcher.congregation_compatibility IS NOT NULL
                       AND prospect.congregation_compatibility IS NOT NULL
                       AND searcher.congregation_compatibility = prospect.congregation_compatibility THEN 1 ELSE 0 END
                + CASE WHEN searcher.has_kids_id IS NOT NULL
                       AND prospect.has_kids_id IS NOT NULL
                       AND searcher.has_kids_id = prospect.has_kids_id THEN 1 ELSE 0 END
                + CASE WHEN searcher.wants_kids_id IS NOT NULL
                       AND prospect.wants_kids_id IS NOT NULL
                       AND searcher.wants_kids_id = prospect.wants_kids_id THEN 1 ELSE 0 END
                + CASE WHEN searcher.congregation_id IS NOT NULL
                       AND prospect.congregation_id IS NOT NULL
                       AND searcher.congregation_id = prospect.congregation_id THEN 1 ELSE 0 END
            ) AS matched_traits
    ) AS values_alignment
    ON TRUE
    WHERE
        mapped_last_online_time < %(before)s
    AND
        last_event_time > now() - interval '1 month'
    AND
        activated
    AND
        -- The searcher meets the prospects privacy_verification_level_id
        -- requirement
        (
            prospect.privacy_verification_level_id <=
                searcher.verification_level_id
            OR COALESCE(searcher.searcher_roles, ARRAY[]::TEXT[]) && ARRAY['admin', 'bot']
        )
    AND
        -- The prospect wants to be shown to strangers or isn't a stranger
        (
            prospect.id IN (
                SELECT
                    subject_person_id
                FROM
                    messaged
                WHERE
                    object_person_id = %(searcher_person_id)s
            )
        OR
            NOT prospect.hide_me_from_strangers
        )
    AND
        -- The prospect did not skip the searcher
        prospect.id NOT IN (
            SELECT
                subject_person_id
            FROM
                skipped
            WHERE
                object_person_id = %(searcher_person_id)s
        )
    AND
        -- The searcher did not skip the prospect, or the searcher wishes to
        -- view skipped prospects
        (
            prospect.id NOT IN (
                SELECT
                    object_person_id
                FROM
                    skipped
                WHERE
                    subject_person_id = %(searcher_person_id)s
            )
        OR
            1 IN (
                SELECT
                    skipped_id
                FROM
                    search_preference_skipped
                WHERE
                    person_id = %(searcher_person_id)s
            )
        )
    AND
        -- The searcher did not message the prospect, or the searcher wishes to
        -- view messaged prospects
        (
            prospect.id NOT IN (
                SELECT
                    object_person_id
                FROM
                    messaged
                WHERE
                    subject_person_id = %(searcher_person_id)s
            )
        OR
            1 IN (
                SELECT
                    messaged_id
                FROM
                    search_preference_messaged
                WHERE
                    person_id = %(searcher_person_id)s
            )
        )
    -- Decrease users' odds of appearing in the feed if they're already getting
    -- lots of messages
    AND random() < (
        SELECT
            1.0 / (1.0 + count(*)::real) ^ 1.5
        FROM
            messaged
        WHERE
            object_person_id = prospect.id
        AND
            created_at > now() - interval '1 day'
    )
    -- Decrease users' odds of appearing in the feed as the age gap between them
    -- and the searcher grows
    AND random() < age_gap_acceptability_odds(
        EXTRACT(YEAR FROM AGE(searcher.date_of_birth)),
        EXTRACT(YEAR FROM AGE(prospect.date_of_birth))
    )
    -- The searcher meets the prospect's gender preference
    AND (
        COALESCE(searcher.searcher_roles, ARRAY[]::TEXT[]) && ARRAY['admin', 'bot']
        OR EXISTS (
            SELECT
                1
            FROM
                search_preference_gender
            WHERE
                search_preference_gender.person_id = prospect.id
            AND
                search_preference_gender.gender_id = searcher.gender_id
        )
    )
    AND (
        COALESCE(searcher.searcher_roles, ARRAY[]::TEXT[]) && ARRAY['admin', 'bot']
        OR
        searcher.gender_id NOT IN (1, 2)
        OR prospect.gender_id <> searcher.gender_id
    )
    -- Exclude photos that might be NSFW
    AND NOT EXISTS (
        SELECT
            1
        FROM
            photo
        WHERE
            uuid = mapped_last_event_data->>'added_photo_uuid'
        AND
            photo.nsfw_score > 0.2
    )
    -- Exclude users who were reported two or more times in the past day
    AND (
        SELECT
            count(*)
        FROM
            skipped
        WHERE
            object_person_id = prospect.id
        AND
            created_at > now() - interval '2 days'
        AND
            reported
    ) < 2
    -- Exclude users who aren't verified but are required to be
    AND (
            prospect.verification_level_id > 1
        OR
            NOT prospect.verification_required
    )
    AND COALESCE(prospect.profile_status, 'active') = 'active'
    AND COALESCE(prospect.waitlist_status, 'active') = 'active'
    AND (
        SELECT COUNT(*)
        FROM antiabuse_flag
        WHERE
            antiabuse_flag.person_id = prospect.id
        AND antiabuse_flag.status IN ('open', 'reviewing')
    ) < 2
    AND (
        SELECT COUNT(*)
        FROM skipped
        WHERE
            skipped.object_person_id = prospect.id
        AND skipped.reported
        AND skipped.report_reason <> ''
        AND skipped.created_at > NOW() - INTERVAL '30 days'
    ) < COALESCE(
        (SELECT NULLIF(value, '')::INT FROM admin_setting WHERE key = 'system_temporary_ban_report_threshold'),
        3
    )
    AND prospect.sign_in_time > NOW() - (
        COALESCE(
            (SELECT NULLIF(value, '')::INT FROM admin_setting WHERE key = 'system_inactive_hide_days'),
            45
        ) || ' days'
    )::interval
    -- Exclude users who don't seem human. A user seems human if:
    --   * They're verified; or
    --   * Their account is more than a month old; or
    --   * They've customized their account's color scheme
    --   * They've got an audio bio
    --   * They've got an otherwise well-completed profile
    --   * They've got Gold
    AND (
            prospect.verification_level_id > 1

        OR
            prospect.sign_up_time < now() - interval '1 month'

        OR
            lower(prospect.title_color) <> '#000000'
        OR
            lower(prospect.body_color) <> '#000000'
        OR
            lower(prospect.background_color) <> '#ffffff'

        OR EXISTS (
            SELECT 1 FROM audio WHERE person_id = prospect.id
        )

        OR
            prospect.count_answers >= 25
        AND
            length(prospect.about) > 0
        AND EXISTS (
            SELECT 1 FROM person_club WHERE person_id = prospect.id
        )

        OR TRUE
    )
    -- Exclude the searcher from their own feed results
    AND
        searcher_id <> prospect.id
    ORDER BY
        mapped_last_online_time DESC
    LIMIT
        {FEED_RESULTS_PER_PAGE * FEED_SELECTIVITY}
), filtered_by_club AS (
    SELECT
        person_uuid,
        name,
        photo_uuid,
        photo_blurhash,
        is_verified,
        match_percentage,
        mapped_last_event_name AS type,
        iso8601_utc(mapped_last_online_time) AS time,
        mapped_last_online_time AS last_event_time,
        mapped_last_event_data,
        ({Q_COMPUTED_FLAIR}) AS flair,
        age,
        gender,
        location,
        CLAMP(
            0,
            100,
            45
            + CASE WHEN is_verified THEN 20 ELSE 0 END
            + CASE WHEN (
                SELECT COUNT(*)
                FROM photo
                WHERE photo.person_id = person_data.id
            ) >= 3 THEN 15 ELSE 0 END
            + CASE WHEN EXISTS (
                SELECT 1
                FROM audio
                WHERE audio.person_id = person_data.id
            ) THEN 5 ELSE 0 END
            + CASE WHEN COALESCE(count_answers, 0) >= 25 THEN 5 ELSE 0 END
            + CASE WHEN referred_by_code_id IS NOT NULL THEN 5 ELSE 0 END
            + CASE WHEN sign_up_time < NOW() - INTERVAL '30 days' THEN 5 ELSE 0 END
            - LEAST(
                30,
                12 * (
                    SELECT COUNT(*)
                    FROM skipped
                    WHERE
                        skipped.object_person_id = person_data.id
                    AND skipped.reported
                    AND skipped.report_reason <> ''
                )
            )
            - LEAST(
                20,
                8 * (
                    SELECT COUNT(*)
                    FROM antiabuse_flag
                    WHERE
                        antiabuse_flag.person_id = person_data.id
                    AND antiabuse_flag.status IN ('open', 'reviewing')
                )
            )
        ) AS trust_score
    FROM
        person_data,
        searcher
    ORDER BY
        EXISTS (
            SELECT
                1
            FROM
                search_preference_gender AS preference
            WHERE
                preference.person_id = searcher_id
            AND
                preference.gender_id = person_data.gender_id
        ) DESC,
        trust_score DESC,
        match_percentage DESC,
        mapped_last_online_time DESC
    LIMIT
        (SELECT round(count(*)::real / {FEED_SELECTIVITY}) FROM person_data)
)
SELECT
    jsonb_build_object(
        'person_uuid', person_uuid,
        'name', name,
        'photo_uuid', photo_uuid,
        'photo_blurhash', photo_blurhash,
        'is_verified', is_verified,
        'time', time,
        'type', type,
        'match_percentage', match_percentage,
        'flair', flair,
        'age', age,
        'gender', gender,
        'location', location
    ) || mapped_last_event_data AS j
FROM
    filtered_by_club
ORDER BY
    last_event_time DESC
"""
