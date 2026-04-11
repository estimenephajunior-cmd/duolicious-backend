-- One-time bootstrap for production:
-- 1. Makes the earliest account the only admin-capable account.
-- 2. Creates a referral code you can hand to the next real user.
-- 3. Keeps normal signup referral-only after that.
--
-- Update the code value below before running if you want a different code.

WITH first_person AS (
    SELECT id
    FROM person
    ORDER BY id ASC
    LIMIT 1
), promoted_admin AS (
    UPDATE person
    SET roles = CASE
        WHEN roles @> ARRAY['bot']::TEXT[] THEN roles
        ELSE array_append(roles, 'bot')
    END
    WHERE id IN (SELECT id FROM first_person)
    RETURNING id
), created_code AS (
    INSERT INTO referral_code (person_id, code)
    SELECT id, 'JWBOOADMIN1'
    FROM promoted_admin
    ON CONFLICT (code) DO UPDATE
    SET person_id = EXCLUDED.person_id,
        disabled = FALSE,
        replaced_at = NULL
    RETURNING id, person_id, code, disabled, created_at
)
SELECT *
FROM created_code;

-- Optional cleanup after the first referred signup:
-- UPDATE referral_code
-- SET disabled = TRUE, replaced_at = NOW()
-- WHERE code = 'JWBOOADMIN1';
