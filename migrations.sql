CREATE OR REPLACE FUNCTION iso8601_utc(ts timestamp)
  RETURNS text
  LANGUAGE sql
  IMMUTABLE
  PARALLEL SAFE
  RETURNS NULL ON NULL INPUT
AS $$
    SELECT to_char(ts AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"');
$$;

ALTER TABLE person
    ADD COLUMN IF NOT EXISTS baptism_date DATE;

ALTER TABLE person
    ADD COLUMN IF NOT EXISTS country_of_birth TEXT;

ALTER TABLE person
    ADD COLUMN IF NOT EXISTS pioneer_status TEXT;

ALTER TABLE person
    ADD COLUMN IF NOT EXISTS referred_by_code_id INT;

ALTER TABLE onboardee
    ADD COLUMN IF NOT EXISTS baptism_date DATE;

ALTER TABLE duo_session
    ADD COLUMN IF NOT EXISTS referral_code_id INT;

CREATE TABLE IF NOT EXISTS referral_code (
    id SERIAL PRIMARY KEY,
    person_id INT NOT NULL REFERENCES person(id) ON DELETE CASCADE ON UPDATE CASCADE,
    code TEXT NOT NULL UNIQUE,
    disabled BOOLEAN NOT NULL DEFAULT FALSE,
    replaced_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fk__person__referred_by_code_id'
    ) THEN
        ALTER TABLE person
            ADD CONSTRAINT fk__person__referred_by_code_id
            FOREIGN KEY (referred_by_code_id) REFERENCES referral_code(id);
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fk__duo_session__referral_code_id'
    ) THEN
        ALTER TABLE duo_session
            ADD CONSTRAINT fk__duo_session__referral_code_id
            FOREIGN KEY (referral_code_id) REFERENCES referral_code(id);
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS idx__referral_code__person_id
    ON referral_code(person_id);

CREATE INDEX IF NOT EXISTS idx__referral_code__created_at
    ON referral_code(created_at DESC);

CREATE INDEX IF NOT EXISTS idx__duo_session__referral_code_id
    ON duo_session(referral_code_id);

CREATE INDEX IF NOT EXISTS idx__person__referred_by_code_id
    ON person(referred_by_code_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx__person__normalized_email__unique
    ON person(normalized_email);

UPDATE person
SET normalized_email = lower(normalized_email)
WHERE normalized_email <> lower(normalized_email);

CREATE TABLE IF NOT EXISTS admin_onboarding_step (
    id SERIAL PRIMARY KEY,
    step_name TEXT NOT NULL,
    is_required BOOLEAN NOT NULL DEFAULT FALSE,
    ordinal INT NOT NULL
);

CREATE TABLE IF NOT EXISTS admin_setting (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS jw_quiz_question (
    id SERIAL PRIMARY KEY,
    prompt TEXT NOT NULL,
    options JSONB NOT NULL,
    correct_option TEXT NOT NULL,
    ordinal INT NOT NULL DEFAULT 0,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS jw_quiz_attempt (
    id BIGSERIAL PRIMARY KEY,
    normalized_email TEXT NOT NULL,
    referral_code TEXT NOT NULL,
    challenge_token_hash TEXT NOT NULL UNIQUE,
    question_payload JSONB NOT NULL,
    expected_answers JSONB NOT NULL,
    time_limit_seconds INT NOT NULL DEFAULT 45,
    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    passed_at TIMESTAMP,
    score SMALLINT,
    total_questions SMALLINT,
    cooldown_until TIMESTAMP,
    is_consumed BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS admin_support_thread (
    person_id INT PRIMARY KEY REFERENCES person(id) ON DELETE CASCADE ON UPDATE CASCADE,
    status TEXT NOT NULL DEFAULT 'open',
    last_message_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_read_by_admin_at TIMESTAMP,
    last_read_by_user_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS admin_support_message (
    id BIGSERIAL PRIMARY KEY,
    person_id INT NOT NULL REFERENCES person(id) ON DELETE CASCADE ON UPDATE CASCADE,
    sender_role TEXT NOT NULL,
    body TEXT NOT NULL DEFAULT '',
    attachment_name TEXT,
    attachment_mime TEXT,
    attachment_bytes BYTEA,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

INSERT INTO admin_setting (key, value) VALUES
    ('public_footer_content_json', $${
      "sectionTitle":"About",
      "aboutText":"Duolicious is free software licensed under the AGPLv3. The source code used to make Duolicious is available here.",
      "aboutLink":"https://github.com/duolicious",
      "contactEmail":"support@duolicious.app",
      "contactPrefix":"You can contact us at",
      "contactSuffix":"to provide feedback, report abuse, or submit any other concerns or queries you have.",
      "versionLabel":"Duolicious Web Version"
    }$$),
    ('public_sidebar_cards_json', $$[
      {"id":"sfdating","name":"SFDating","image":"https://partner.duolicious.app/sfdating.jpg","link":"https://discord.gg/REbbHqzD9p","body":"Meet Bay Area singles in a real local community.","captionSuffix":" is a Duolicious partner","visible":true},
      {"id":"duo3k","name":"duo3k","image":"https://partner.duolicious.app/duo3k.webp","link":"https://discord.gg/duo3k","body":"Join the wider Duolicious-adjacent social space.","captionSuffix":" is a Duolicious partner","visible":true},
      {"id":"tiblur","name":"Tiblur","image":"https://partner.duolicious.app/tiblur.jpg","link":"https://tiblur.com/register","body":"Discover another community built around connection.","captionSuffix":" is a Duolicious partner","visible":true},
      {"id":"redux","name":"gg/redux","image":"https://partner.duolicious.app/redux.png","link":"https://discord.gg/EA3QYF9apJ","body":"Find conversation, events, and community off-platform.","captionSuffix":" is a Duolicious partner","visible":true},
      {"id":"neet-chat","name":"NEET_Chat","image":"https://partner.duolicious.app/neet-chat.png","link":"https://discord.gg/96JShH3N7Y","body":"Want to promote your social group for free? Inquire at admin@duolicious.app.","captionSuffix":" is a Duolicious partner","visible":true},
      {"id":"affinity","name":"Affinity","image":"https://partner.duolicious.app/affinity.png","link":"https://discord.gg/pvQ9EMVVq5","body":"A partner space for meeting people with shared interests.","captionSuffix":" is a Duolicious partner","visible":true}
    ]$$),
    ('public_footer_section_title', 'About'),
    ('public_footer_about_text', 'Duolicious is free software licensed under the AGPLv3. The source code used to make Duolicious is available here.'),
    ('public_footer_about_link', 'https://github.com/duolicious'),
    ('public_footer_contact_email', 'support@duolicious.app'),
    ('public_footer_contact_prefix', 'You can contact us at'),
    ('public_footer_contact_suffix', 'to provide feedback, report abuse, or submit any other concerns or queries you have.'),
    ('public_footer_version_label', 'Duolicious Web Version'),
    ('public_welcome_trust_content_json', $${
      "eyebrow":"Private introduction network",
      "headline":"Built for serious intent, not public exposure.",
      "body":"Profiles are shared carefully, referrals matter, and this space is designed to feel calm, private, and respectful from the start.",
      "bullets":["Private and reputation-conscious","Referral-based onboarding","Serious relationship intent only","Profiles are not publicly searchable"],
      "signals":["Private network","Invite-only access","Controlled visibility"]
    }$$),
    ('public_profile_quality_json', $${
      "title":"Profile quality matters here",
      "body":"Stronger profiles build more trust. Add enough photos and a meaningful bio before you ask the system to verify you.",
      "minPhotos":3,
      "minAboutChars":80
    }$$),
    ('public_verification_copy_json', $${
      "requiredTitle":"Verification is required before full access unlocks.",
      "optionalTitle":"Finish verification to strengthen your profile.",
      "introBody":"Complete your selfie check here, then add supporting documents if you want to strengthen your request.",
      "startHint":"Start with the selfie camera. After that, upload any extra proof you want an admin to inspect.",
      "lockedCtaLabel":"Get verified",
      "lockedHint":"Add your main photo plus enough profile detail to unlock verification.",
      "statusGetVerified":"Get verified",
      "statusCheckingSelfie":"Checking your selfie",
      "statusUnderReview":"Under review",
      "statusVerified":"Verified",
      "retryMessage":"Your verification needs one more pass. Please check your photos or documents and try again.",
      "selfieFailureMessage":"We could not verify that selfie yet. Please try again with a clear, front-facing photo.",
      "submittedMessage":"Your verification has been submitted and is being reviewed.",
      "startMessage":"Use the button below to take your verification selfie.",
      "submittedSelfieTitle":"Submitted selfie",
      "supportingDocumentsTitle":"Supporting documents"
    }$$),
    ('public_verification_gate_message', 'Finish verification or wait for approval to unlock Feed, Search, Inbox, and Visitors.'),
    ('public_referral_limited_message', 'Invites are temporarily limited while admins review recent referral quality.'),
    ('system_moderation_level_1', 'Level 1 - Minor: empty profile, spammy behavior, or low-effort requests. Use warning plus temporary limits.'),
    ('system_moderation_level_2', 'Level 2 - Serious: harassment, repeated misconduct, or serious misrepresentation concerns. Suspend and review carefully.'),
    ('system_moderation_level_3', 'Level 3 - Critical: fake identity/photos, abusive behavior, or confirmed dishonesty about major status claims like singleness. Preserve evidence and move to permanent ban review.'),
    ('system_misrepresentation_definition', 'Misrepresentation means claiming to be single while not, or using fake identity/photos. That should move to permanent-ban review, not a casual warning.'),
    ('public_report_categories_json', $$[
      {"id":"safety","label":"Safety concern","helper":"Threats, coercion, doxxing, or anything that feels unsafe."},
      {"id":"misrepresentation","label":"Misrepresentation","helper":"Lying about identity, singleness, photos, age, or major profile claims."},
      {"id":"harassment","label":"Harassment","helper":"Repeated unwanted contact, insults, or abusive behavior."},
      {"id":"money-or-spam","label":"Money or spam","helper":"Scams, solicitations, repeated promo, or requests for money or gifts."},
      {"id":"other","label":"Other","helper":"Anything else that needs a moderator to review carefully."}
    ]$$)
ON CONFLICT (key) DO NOTHING;

CREATE TABLE IF NOT EXISTS meeting_language (
    language_guid TEXT PRIMARY KEY,
    code TEXT NOT NULL,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS congregation (
    id SERIAL PRIMARY KEY,
    jw_meeting_id TEXT NOT NULL UNIQUE,
    jw_place_id TEXT,
    language_guid TEXT NOT NULL REFERENCES meeting_language(language_guid),
    name TEXT NOT NULL,
    address TEXT,
    phone_number TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    midweek_meeting_day SMALLINT,
    midweek_meeting_time TEXT,
    weekend_meeting_day SMALLINT,
    weekend_meeting_time TEXT,
    raw JSONB NOT NULL DEFAULT '{}'::JSONB,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS congregation_search_cache (
    location_long_friendly TEXT NOT NULL,
    language_guid TEXT NOT NULL REFERENCES meeting_language(language_guid),
    congregation_id INT NOT NULL REFERENCES congregation(id) ON DELETE CASCADE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (location_long_friendly, language_guid, congregation_id)
);

ALTER TABLE onboardee
    ADD COLUMN IF NOT EXISTS congregation_id INT REFERENCES congregation(id);

ALTER TABLE person
    ADD COLUMN IF NOT EXISTS congregation_id INT REFERENCES congregation(id);

CREATE INDEX IF NOT EXISTS idx__congregation__language_guid
    ON congregation(language_guid);

CREATE INDEX IF NOT EXISTS idx__congregation__name
    ON congregation(name);

CREATE INDEX IF NOT EXISTS idx__congregation_search_cache__location_language
    ON congregation_search_cache(location_long_friendly, language_guid);

CREATE INDEX IF NOT EXISTS idx__person__congregation_id
    ON person(congregation_id);

CREATE TABLE IF NOT EXISTS verification_review (
    id SERIAL PRIMARY KEY,
    person_id INT NOT NULL UNIQUE REFERENCES person(id) ON DELETE CASCADE ON UPDATE CASCADE,
    selfie_photo_uuid TEXT,
    ai_status TEXT NOT NULL DEFAULT 'pending-selfie',
    ai_message TEXT NOT NULL DEFAULT '',
    admin_status TEXT NOT NULL DEFAULT 'pending',
    admin_message TEXT NOT NULL DEFAULT '',
    reviewed_by_person_id INT REFERENCES person(id) ON DELETE SET NULL ON UPDATE CASCADE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS verification_review_asset (
    id SERIAL PRIMARY KEY,
    review_id INT NOT NULL REFERENCES verification_review(id) ON DELETE CASCADE ON UPDATE CASCADE,
    kind TEXT NOT NULL,
    label TEXT,
    photo_uuid TEXT NOT NULL,
    verified BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

ALTER TABLE verification_review_asset
ADD COLUMN IF NOT EXISTS verified BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx__verification_review__admin_status
    ON verification_review(admin_status);

CREATE INDEX IF NOT EXISTS idx__verification_review__ai_status
    ON verification_review(ai_status);

CREATE INDEX IF NOT EXISTS idx__verification_review_asset__review_id
    ON verification_review_asset(review_id);

CREATE INDEX IF NOT EXISTS idx__admin_support_message__person_id__created_at
    ON admin_support_message(person_id, created_at DESC);

CREATE TABLE IF NOT EXISTS intro_review (
    id BIGSERIAL PRIMARY KEY,
    from_person_id BIGINT NOT NULL REFERENCES person(id) ON DELETE CASCADE,
    to_person_id BIGINT NOT NULL REFERENCES person(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'pending',
    prompt TEXT NOT NULL DEFAULT '',
    round_count INT NOT NULL DEFAULT 0,
    accepted_at TIMESTAMPTZ,
    rejected_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (from_person_id, to_person_id)
);

CREATE INDEX IF NOT EXISTS idx__intro_review__to_person_id__status
    ON intro_review(to_person_id, status);

CREATE INDEX IF NOT EXISTS idx__intro_review__from_person_id__status
    ON intro_review(from_person_id, status);

CREATE TABLE IF NOT EXISTS intro_gate_question (
    id BIGSERIAL PRIMARY KEY,
    person_id BIGINT NOT NULL REFERENCES person(id) ON DELETE CASCADE,
    prompt TEXT NOT NULL DEFAULT '',
    answer_type TEXT NOT NULL DEFAULT 'free_text',
    response_mode TEXT NOT NULL DEFAULT 'text',
    is_required BOOLEAN NOT NULL DEFAULT TRUE,
    ordinal INT NOT NULL DEFAULT 0,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx__intro_gate_question__person_id__ordinal
    ON intro_gate_question(person_id, ordinal);

CREATE TABLE IF NOT EXISTS intro_request (
    id BIGSERIAL PRIMARY KEY,
    from_person_id BIGINT NOT NULL REFERENCES person(id) ON DELETE CASCADE,
    to_person_id BIGINT NOT NULL REFERENCES person(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'pending',
    note TEXT NOT NULL DEFAULT '',
    accepted_at TIMESTAMPTZ,
    rejected_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (from_person_id, to_person_id)
);

ALTER TABLE intro_request
    ADD COLUMN IF NOT EXISTS first_call_status TEXT NOT NULL DEFAULT 'not-planned';

ALTER TABLE intro_request
    ADD COLUMN IF NOT EXISTS first_call_timing TEXT NOT NULL DEFAULT '';

ALTER TABLE intro_request
    ADD COLUMN IF NOT EXISTS first_call_plan TEXT NOT NULL DEFAULT '';

ALTER TABLE intro_request
    ADD COLUMN IF NOT EXISTS ready_for_video_call BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE intro_request
    ADD COLUMN IF NOT EXISTS ready_for_family_introduction BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE intro_request
    ADD COLUMN IF NOT EXISTS courtship_prompt_dismissed_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx__intro_request__to_person_id__status
    ON intro_request(to_person_id, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx__intro_request__from_person_id__status
    ON intro_request(from_person_id, status, updated_at DESC);

ALTER TABLE intro_request
    ADD COLUMN IF NOT EXISTS reason_for_reaching_out TEXT NOT NULL DEFAULT '';

ALTER TABLE intro_request
    ADD COLUMN IF NOT EXISTS why_we_may_match TEXT NOT NULL DEFAULT '';

ALTER TABLE intro_request
    ADD COLUMN IF NOT EXISTS voice_note_audio_uuid TEXT;

CREATE TABLE IF NOT EXISTS intro_request_answer (
    id BIGSERIAL PRIMARY KEY,
    request_id BIGINT NOT NULL REFERENCES intro_request(id) ON DELETE CASCADE,
    question_id BIGINT REFERENCES intro_gate_question(id) ON DELETE SET NULL,
    prompt_snapshot TEXT NOT NULL DEFAULT '',
    answer_type TEXT NOT NULL DEFAULT 'free_text',
    response_mode TEXT NOT NULL DEFAULT 'text',
    answer_text TEXT NOT NULL DEFAULT '',
    answer_bool BOOLEAN,
    answer_audio_uuid TEXT,
    ordinal INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx__intro_request_answer__request_id__ordinal
    ON intro_request_answer(request_id, ordinal);

ALTER TABLE intro_gate_question
    ADD COLUMN IF NOT EXISTS response_mode TEXT NOT NULL DEFAULT 'text';

ALTER TABLE intro_request_answer
    ADD COLUMN IF NOT EXISTS response_mode TEXT NOT NULL DEFAULT 'text';

ALTER TABLE intro_request_answer
    ADD COLUMN IF NOT EXISTS answer_audio_uuid TEXT;

CREATE TABLE IF NOT EXISTS search_preference_baptism_years (
    person_id INT REFERENCES person(id) ON DELETE CASCADE ON UPDATE CASCADE,
    min_baptism_years SMALLINT NOT NULL,
    PRIMARY KEY (person_id)
);

CREATE TABLE IF NOT EXISTS search_preference_city (
    person_id INT REFERENCES person(id) ON DELETE CASCADE ON UPDATE CASCADE,
    city TEXT NOT NULL,
    PRIMARY KEY (person_id)
);

CREATE TABLE IF NOT EXISTS search_preference_state (
    person_id INT REFERENCES person(id) ON DELETE CASCADE ON UPDATE CASCADE,
    state TEXT NOT NULL,
    PRIMARY KEY (person_id)
);

CREATE TABLE IF NOT EXISTS search_preference_pioneer_status (
    person_id INT NOT NULL REFERENCES person(id) ON DELETE CASCADE ON UPDATE CASCADE,
    status TEXT NOT NULL,
    PRIMARY KEY (person_id, status)
);

CREATE TABLE IF NOT EXISTS external_report (
    id BIGSERIAL PRIMARY KEY,
    reporter_name TEXT NOT NULL DEFAULT '',
    reporter_email TEXT NOT NULL DEFAULT '',
    relationship_to_user TEXT NOT NULL DEFAULT '',
    target_name TEXT NOT NULL DEFAULT '',
    target_email TEXT NOT NULL DEFAULT '',
    target_profile_url TEXT NOT NULL DEFAULT '',
    claim TEXT NOT NULL DEFAULT '',
    evidence_details TEXT NOT NULL DEFAULT '',
    photo_uuid TEXT,
    status TEXT NOT NULL DEFAULT 'new',
    admin_note TEXT NOT NULL DEFAULT '',
    reviewed_by_person_id BIGINT REFERENCES person(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx__external_report__status__created_at
    ON external_report(status, created_at DESC);

ALTER TABLE person
    ADD COLUMN IF NOT EXISTS profile_status TEXT NOT NULL DEFAULT 'active';

ALTER TABLE person
    ADD COLUMN IF NOT EXISTS profile_status_changed_at TIMESTAMP NOT NULL DEFAULT NOW();

ALTER TABLE person
    ADD COLUMN IF NOT EXISTS waitlist_status TEXT NOT NULL DEFAULT 'active';

ALTER TABLE person
    ADD COLUMN IF NOT EXISTS waitlist_note TEXT NOT NULL DEFAULT '';

ALTER TABLE person
    ADD COLUMN IF NOT EXISTS invite_unlocked_at TIMESTAMP;

ALTER TABLE person
    ADD COLUMN IF NOT EXISTS service_goals TEXT;

ALTER TABLE person
    ADD COLUMN IF NOT EXISTS willingness_to_relocate TEXT;

ALTER TABLE person
    ADD COLUMN IF NOT EXISTS family_worship_habit TEXT;

ALTER TABLE person
    ADD COLUMN IF NOT EXISTS spiritual_routine TEXT;

ALTER TABLE person
    ADD COLUMN IF NOT EXISTS willing_to_involve_family_early TEXT;

ALTER TABLE person
    ADD COLUMN IF NOT EXISTS open_to_chaperoned_video_calls TEXT;

ALTER TABLE person
    ADD COLUMN IF NOT EXISTS congregation_compatibility TEXT;

ALTER TABLE person
    ADD COLUMN IF NOT EXISTS service_lifestyle TEXT;

ALTER TABLE person
    ADD COLUMN IF NOT EXISTS life_stage TEXT;

ALTER TABLE person
    ADD COLUMN IF NOT EXISTS emotional_temperament TEXT;

ALTER TABLE person
    ADD COLUMN IF NOT EXISTS communication_style TEXT;

ALTER TABLE duo_session
    ADD COLUMN IF NOT EXISTS device_fingerprint TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'ck__person__profile_status'
    ) THEN
        ALTER TABLE person
            ADD CONSTRAINT ck__person__profile_status
            CHECK (profile_status IN ('active', 'paused', 'serious'));
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'ck__person__waitlist_status'
    ) THEN
        ALTER TABLE person
            ADD CONSTRAINT ck__person__waitlist_status
            CHECK (waitlist_status IN ('active', 'pending', 'blocked'));
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS antiabuse_flag (
    id BIGSERIAL PRIMARY KEY,
    person_id INT REFERENCES person(id) ON DELETE CASCADE,
    category TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT 'medium',
    reason TEXT NOT NULL,
    evidence JSONB NOT NULL DEFAULT '{}'::JSONB,
    status TEXT NOT NULL DEFAULT 'open',
    resolution TEXT NOT NULL DEFAULT 'none',
    admin_note TEXT NOT NULL DEFAULT '',
    resolved_by_person_id INT REFERENCES person(id) ON DELETE SET NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx__antiabuse_flag__status__created_at
    ON antiabuse_flag(status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx__antiabuse_flag__person_id__status
    ON antiabuse_flag(person_id, status);

ALTER TABLE antiabuse_flag
    ADD COLUMN IF NOT EXISTS resolution TEXT NOT NULL DEFAULT 'none';

INSERT INTO admin_setting (key, value)
VALUES
    ('system_max_active_chats', '5'),
    ('system_intro_cooldown_hours', '48'),
    ('system_referral_low_trust_threshold', '45'),
    ('system_referral_block_threshold', '15'),
    ('system_invite_unlock_days', '7'),
    ('system_waitlist_mode', '0'),
    ('system_intro_requests_per_hour', '3'),
    ('system_intro_requests_per_day', '8'),
    ('system_low_trust_intro_requests_per_day', '2'),
    ('system_intro_rejection_review_threshold', '5'),
    ('system_conversation_auto_close_days', '10'),
    ('system_coalition_reporter_threshold', '2'),
    ('system_coalition_window_days', '30'),
    ('system_referral_ring_member_threshold', '3'),
    ('system_referral_ring_flagged_member_threshold', '2'),
    ('system_max_accounts_per_device', '2'),
    ('system_max_accounts_per_ip_30d', '4'),
    ('system_inactive_hide_days', '45'),
    ('system_same_target_reports_limit', '1'),
    ('system_same_target_report_window_days', '30'),
    ('system_trust_good_threshold', '70'),
    ('system_trust_warning_threshold', '40'),
    ('system_trust_request_block_threshold', '40'),
    ('system_profile_field_cooldown_days', '15'),
    ('system_max_reports_per_day', '8'),
    ('system_warning_report_threshold', '1'),
    ('system_temporary_ban_report_threshold', '3'),
    ('system_permaban_report_threshold', '5')
ON CONFLICT (key) DO NOTHING;

ALTER TABLE person
    ADD COLUMN IF NOT EXISTS who_can_contact_me TEXT NOT NULL DEFAULT 'Anyone who matches my filters';

ALTER TABLE person
    ADD COLUMN IF NOT EXISTS request_format_preference TEXT NOT NULL DEFAULT 'Thoughtful written request';

ALTER TABLE person
    ADD COLUMN IF NOT EXISTS message_pace_preference TEXT NOT NULL DEFAULT 'Steady';

INSERT INTO admin_setting (key, value) VALUES
    ('system_required_q_and_a_min', '30'),
    ('system_jw_quiz_question_count', '5'),
    ('system_jw_quiz_time_limit_seconds', '45'),
    ('system_jw_quiz_cooldown_days', '7')
ON CONFLICT (key) DO NOTHING;

INSERT INTO jw_quiz_question (prompt, options, correct_option, ordinal)
SELECT * FROM (
    VALUES
        ('Which annual event do Jehovah''s Witnesses observe each year?', '["The Memorial of Christ''s death","Christmas","Easter","New Year''s Eve"]'::JSONB, 'The Memorial of Christ''s death', 1),
        ('What is the usual name of a Jehovah''s Witness meeting place?', '["Kingdom Hall","Parish church","Cathedral","Temple"]'::JSONB, 'Kingdom Hall', 2),
        ('Do Jehovah''s Witnesses celebrate Christmas?', '["Yes","No","Only privately","Only with family"]'::JSONB, 'No', 3),
        ('What do Jehovah''s Witnesses commonly call their public preaching work?', '["The preaching ministry","Pilgrimage","Confession","Mass"]'::JSONB, 'The preaching ministry', 4),
        ('What personal name do Jehovah''s Witnesses use for God?', '["Jehovah","Yahweh only","Allah","Adonai only"]'::JSONB, 'Jehovah', 5),
        ('What book is central to Jehovah''s Witness beliefs and meetings?', '["The Bible","The Talmud","The Quran","The Book of Mormon"]'::JSONB, 'The Bible', 6),
        ('What do Jehovah''s Witnesses call baptism candidates before baptism?', '["Unbaptized publishers","Elders","Pioneers","Ministerial servants"]'::JSONB, 'Unbaptized publishers', 7),
        ('What is the weekly meeting workbook called in congregation life?', '["Life and Ministry Meeting Workbook","Catechism guide","Mass program","Parish bulletin"]'::JSONB, 'Life and Ministry Meeting Workbook', 8)
) AS seed(prompt, options, correct_option, ordinal)
WHERE NOT EXISTS (SELECT 1 FROM jw_quiz_question);

ALTER TABLE duo_session
    DROP COLUMN IF EXISTS pending_club_name;
