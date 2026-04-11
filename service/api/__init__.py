from pathlib import Path
from flask import request
import duotypes as t
from service import (
    congregation,
    location,
    person,
    question,
    search,
)
from database import api_tx
import psycopg
from service.api.decorators import (
    app,
    adelete,
    aget,
    apatch,
    apost,
    aput,
    delete,
    get,
    patch,
    post,
    put,
    validate,
    limiter,
    shared_otp_limit,
    disable_ip_rate_limit,
    disable_account_rate_limit,
    limiter_account,
)
import time
from antiabuse.antispam.signupemail import normalize_email
import json

_init_sql_file = (
    Path(__file__).parent.parent.parent / 'init-api.sql')

_migrations_sql_file = (
    Path(__file__).parent.parent.parent / 'migrations.sql')

_email_domains_bad_file = (
    Path(__file__).parent.parent.parent / 'email-domains-bad.sql')

_email_domains_good_file = (
    Path(__file__).parent.parent.parent / 'email-domains-good.sql')

_banned_club_file = (
    Path(__file__).parent.parent.parent / 'banned-club.sql')

def get_ttl_hash(seconds=10):
    """Return the same value withing `seconds` time period"""
    return round(time.time() / seconds)

def migrate_unnormalized_emails():
    """
    It'll probably be necessary to call this function again if/when
    `normalize_email` normalizes more address.
    """
    with api_tx() as tx:
        q = "SELECT 1 FROM person WHERE normalized_email ILIKE '%@googlemail.com' LIMIT 1"
        if tx.execute(q).fetchone():
            print('Unnormalized emails found. Normalizing...')
        else:
            print('Emails already normalized. Not performing normalization.')
            return

    with api_tx() as tx:
        print('Selecting emails')
        q = "SELECT email FROM person"
        tx.execute('SET LOCAL statement_timeout = 300000') # 5 minutes
        rows = tx.execute(q).fetchall()
        print('Done selecting emails')

    print('Computing normalized emails')
    params_seq = [
        row | dict(normalized_email=normalize_email(row['email']))
        for row in rows
    ]
    print('Done computing normalized emails')

    with api_tx('read committed') as tx:
        q = """
        UPDATE person SET
        normalized_email = %(normalized_email)s
        WHERE email = %(email)s
        """
        print('Updating normalized emails in `person` table')
        tx.execute('SET LOCAL statement_timeout = 300000') # 5 minutes
        tx.executemany(q, params_seq)
        print('Done updating normalized emails in `person` table')

        q = """
        UPDATE banned_person bp
        SET
            normalized_email = %(normalized_email)s
        WHERE
            normalized_email = %(email)s
        AND NOT EXISTS (
            SELECT
                1
            FROM
                banned_person
            WHERE
                normalized_email = %(normalized_email)s
            AND
                ip_address = bp.ip_address
        )
        """
        print('Updating normalized emails in `banned_person` table')
        tx.executemany(q, params_seq)
        print('Done updating normalized emails in `banned_person` table')

def maybe_run_init():
    with api_tx() as tx:
        row = tx.execute("SELECT to_regclass('person')").fetchone()

    if row ['to_regclass'] is not None:
        print('Database already initialized')
        return

    with open(_init_sql_file, 'r') as f:
        init_sql_file = f.read()

    with api_tx() as tx:
        tx.execute(init_sql_file)

def init_db():
    with open(_migrations_sql_file, 'r') as f:
        migrations_sql_file = f.read()

    with open(_email_domains_bad_file, 'r') as f:
        email_domains_bad_file = f.read()

    with open(_email_domains_good_file, 'r') as f:
        email_domains_good_file = f.read()

    with open(_banned_club_file, 'r') as f:
        banned_club_file = f.read()

    maybe_run_init()

    with api_tx() as tx:
        tx.execute('SET LOCAL statement_timeout = 300000') # 5 minutes
        tx.execute(migrations_sql_file)

    with api_tx() as tx:
        tx.execute(email_domains_bad_file)

    with api_tx() as tx:
        tx.execute(email_domains_good_file)

    with api_tx() as tx:
        tx.execute('SET LOCAL statement_timeout = 300000') # 5 minutes
        tx.execute(banned_club_file)

    migrate_unnormalized_emails()

@post('/request-otp', limiter=shared_otp_limit)
@validate(t.PostRequestOtp)
def post_request_otp(req: t.PostRequestOtp):
    limit = "40 per day"
    scope = "request_otp"

    with (
        limiter.limit(
            limit,
            scope=scope,
            exempt_when=disable_ip_rate_limit),
        limiter.limit(
            limit,
            scope=scope,
            key_func=limiter_account,
            exempt_when=disable_account_rate_limit)
    ):
        return person.post_request_otp(req)


@post('/validate-referral-code')
@validate(t.PostValidateReferralCode)
def post_validate_referral_code(req: t.PostValidateReferralCode):
    return person.post_validate_referral_code(req)

@post('/jw-quiz/start')
@validate(t.PostStartJwQuiz)
def post_start_jw_quiz(req: t.PostStartJwQuiz):
    return person.post_start_jw_quiz(req)

@post('/jw-quiz/complete')
@validate(t.PostCompleteJwQuiz)
def post_complete_jw_quiz(req: t.PostCompleteJwQuiz):
    return person.post_complete_jw_quiz(req)


@apost(
    '/resend-otp',
    limiter=shared_otp_limit,
    expected_onboarding_status=None,
    expected_sign_in_status=False
)
def post_resend_otp(s: t.SessionInfo):
    return person.post_resend_otp(s)

@apost(
    '/check-otp',
    expected_onboarding_status=None,
    expected_sign_in_status=False
)
@validate(t.PostCheckOtp)
def post_check_otp(req: t.PostCheckOtp, s: t.SessionInfo):
    limit = "40 per day"
    scope = "check_otp"

    with (
        limiter.limit(
            limit,
            scope=scope,
            exempt_when=disable_ip_rate_limit),
        limiter.limit(
            limit,
            scope=scope,
            key_func=limiter_account,
            exempt_when=disable_account_rate_limit)
    ):
        return person.post_check_otp(req, s)

@apost('/sign-out', expected_onboarding_status=None)
def post_sign_out(s: t.SessionInfo):
    return person.post_sign_out(s)

@apost('/check-session-token', expected_onboarding_status=None)
def post_check_session_token(s: t.SessionInfo):
    return person.post_check_session_token(s)

@aget(
    '/search-locations',
    expected_onboarding_status=None,
    expected_sign_in_status=None,
)
def get_search_locations(_):
    return location.get_search_locations(q=request.args.get('q'))


@aget(
    '/congregation-languages',
    expected_onboarding_status=None,
    expected_sign_in_status=None,
)
def get_congregation_languages(_):
    return congregation.get_languages(q=request.args.get('q'))


@aget(
    '/search-congregations',
    expected_onboarding_status=None,
    expected_sign_in_status=None,
)
def get_search_congregations(_):
    location_text = request.args.get('location')
    language_guid = request.args.get('language_guid')

    if not location_text or not language_guid:
        return 'Missing location or language_guid', 400

    try:
        return congregation.get_congregations(
            location_text=location_text,
            language_guid=language_guid,
        )
    except ValueError as exc:
        return str(exc), 400
    except Exception:
        return 'Unable to fetch congregations right now', 502

@apatch('/onboardee-info', expected_onboarding_status=False)
@validate(t.PatchOnboardeeInfo)
def patch_onboardee_info(req: t.PatchOnboardeeInfo, s: t.SessionInfo):
    return person.patch_onboardee_info(req, s)

@adelete('/onboardee-info', expected_onboarding_status=False)
@validate(t.DeleteOnboardeeInfo)
def delete_onboardee_info(req: t.DeleteOnboardeeInfo, s: t.SessionInfo):
    return person.delete_onboardee_info(req, s)

@apost('/finish-onboarding', expected_onboarding_status=False)
def post_finish_onboarding(s: t.SessionInfo):
    return person.post_finish_onboarding(s)


@aget('/referrals/dashboard')
def get_referrals_dashboard(s: t.SessionInfo):
    return person.get_referral_dashboard(s)


@apost('/referrals/code/regenerate')
def post_regenerate_referral_code(s: t.SessionInfo):
    return person.regenerate_referral_code(s)

@aget('/next-questions')
def get_next_questions(s: t.SessionInfo):
    return question.get_next_questions(
        s=s,
        n=request.args.get('n', '10'),
        o=request.args.get('o', '0'),
    )

@apost('/answer')
@validate(t.PostAnswer)
def post_answer(req: t.PostAnswer, s: t.SessionInfo):
    return person.post_answer(req, s)

@adelete('/answer')
@validate(t.DeleteAnswer)
def delete_answer(req: t.DeleteAnswer, s: t.SessionInfo):
    return person.delete_answer(req, s)

@aget('/search')
def get_search(s: t.SessionInfo):
    if _requires_verification_gate(s):
        return _verification_gate_response()

    n = request.args.get('n')
    o = request.args.get('o')

    search_type, _ = search.get_search_type(n, o)

    limit = "15 per 2 minutes"
    scope = json.dumps([search_type])

    if search_type == 'uncached-search':
        with (
            limiter.limit(
                limit,
                scope=scope,
                exempt_when=disable_ip_rate_limit),
            limiter.limit(
                limit,
                scope=scope,
                key_func=limiter_account,
                exempt_when=disable_account_rate_limit)
        ):
            return search.get_search(s=s, n=n, o=o)
    else:
        return search.get_search(s=s, n=n, o=o)

@get('/health', limiter=limiter.exempt)
def get_health():
    return 'status: ok'

@aget('/me')
def get_me_by_session(s: t.SessionInfo):
    return person.get_me(person_id_as_int=s.person_id)

@get('/me/<person_id>')
def get_me_by_id(person_id: str):
    return person.get_me(person_id_as_str=person_id)

@aget('/prospect-profile/<prospect_uuid>')
def get_prospect_profile(s: t.SessionInfo, prospect_uuid: int):
    if _requires_verification_gate(s):
        return _verification_gate_response()

    return person.get_prospect_profile(s, prospect_uuid)

@apost('/skip/by-uuid/<prospect_uuid>')
@validate(t.PostSkip)
def post_skip_by_uuid(req: t.PostSkip, s: t.SessionInfo, prospect_uuid: str):
    limit = "1 per 5 seconds; 20 per day"
    scope = "report"

    if req.report_reason:
        with (
            limiter.limit(
                limit,
                scope=scope,
                exempt_when=disable_ip_rate_limit),
            limiter.limit(
                limit,
                scope=scope,
                key_func=limiter_account,
                exempt_when=disable_account_rate_limit)
        ):
            return person.post_skip_by_uuid(req, s, prospect_uuid)
    else:
        return person.post_skip_by_uuid(req, s, prospect_uuid)

# TODO: Delete
@apost('/unskip/<int:prospect_person_id>')
def post_unskip(s: t.SessionInfo, prospect_person_id: int):
    return person.post_unskip(s, prospect_person_id)

@apost('/unskip/by-uuid/<prospect_uuid>')
def post_unskip_by_uuid(s: t.SessionInfo, prospect_uuid: str):
    return person.post_unskip_by_uuid(s, prospect_uuid)

@aget(
    '/compare-personalities'
    '/<int:prospect_person_id>'
    '/<any(mbti, big5, attachment, politics, other):topic>'
)
def get_compare_personalities(
    s: t.SessionInfo,
    prospect_person_id: int,
    topic: str
):
    if _requires_verification_gate(s):
        return _verification_gate_response()

    return person.get_compare_personalities(s, prospect_person_id, topic)

@aget('/compare-answers/<int:prospect_person_id>')
def get_compare_answers(s: t.SessionInfo, prospect_person_id: int):
    if _requires_verification_gate(s):
        return _verification_gate_response()

    return person.get_compare_answers(
        s,
        prospect_person_id,
        agreement=request.args.get('agreement'),
        topic=request.args.get('topic'),
        n=request.args.get('n', '10'),
        o=request.args.get('o', '0'),
    )

@apost('/inbox-info')
@validate(t.PostInboxInfo)
def post_inbox_info(req: t.PostInboxInfo, s: t.SessionInfo):
    if _requires_verification_gate(s):
        return _verification_gate_response()

    return person.post_inbox_info(req, s)

@adelete('/account')
def delete_account(s: t.SessionInfo):
    return person.delete_or_ban_account(s=s)

@apost('/deactivate')
def post_deactivate(s: t.SessionInfo):
    return person.post_deactivate(s=s)

@aget('/profile-info')
def get_profile_info(s: t.SessionInfo):
    return person.get_profile_info(s)

@adelete('/profile-info')
@validate(t.DeleteProfileInfo)
def delete_profile_info(req: t.DeleteProfileInfo, s: t.SessionInfo):
    return person.delete_profile_info(req, s)

@apatch('/profile-info')
@validate(t.PatchProfileInfo)
def patch_profile_info(req: t.PatchProfileInfo, s: t.SessionInfo):
    return person.patch_profile_info(req, s)

@aget('/search-filters')
def get_search_filers(s: t.SessionInfo):
    if _requires_verification_gate(s):
        return _verification_gate_response()

    return person.get_search_filters(s)

@apost('/search-filter')
@validate(t.PostSearchFilter)
def post_search_filter(req: t.PostSearchFilter, s: t.SessionInfo):
    if _requires_verification_gate(s):
        return _verification_gate_response()

    return person.post_search_filter(req, s)

@aget('/search-filter-questions')
def get_search_filter_questions(s: t.SessionInfo):
    if _requires_verification_gate(s):
        return _verification_gate_response()

    return question.get_search_filter_questions(
        s=s,
        q=request.args.get('q', ''),
        n=request.args.get('n', '10'),
        o=request.args.get('o', '0'),
    )

@aget('/search-diagnostics')
def get_search_diagnostics(s: t.SessionInfo):
    if _requires_verification_gate(s):
        return _verification_gate_response()

    return search.get_search_diagnostics(s)

@apost('/search-filter-answer')
@validate(t.PostSearchFilterAnswer)
def post_search_filter_answer(req: t.PostSearchFilterAnswer, s: t.SessionInfo):
    if _requires_verification_gate(s):
        return _verification_gate_response()

    return person.post_search_filter_answer(req, s)

@get('/update-notifications')
def get_update_notifications():
    return person.get_update_notifications(
        email=request.args.get('email', ''),
        type=request.args.get('type', ''),
        frequency=request.args.get('frequency', ''),
    )

@aget('/feed')
def get_feed(s: t.SessionInfo):
    if _requires_verification_gate(s):
        return _verification_gate_response()

    valid_datetime = t.ValidDatetime.model_validate(
        {'datetime': request.args.get('before')}
    )

    return search.get_feed(s=s, before=valid_datetime.datetime)

@apost('/verification-selfie')
@validate(t.PostVerificationSelfie)
def post_verification_selfie(req: t.PostVerificationSelfie, s: t.SessionInfo):
    return person.post_verification_selfie(req, s)

@apost('/verification-document')
@validate(t.PostVerificationDocument)
def post_verification_document(req: t.PostVerificationDocument, s: t.SessionInfo):
    return person.post_verification_document(req, s)

@aget('/admin-support/thread')
def get_admin_support_thread(s: t.SessionInfo):
    return person.get_admin_support_thread(s)

@apost('/admin-support/message')
@validate(t.PostAdminSupportMessage)
def post_admin_support_message(req: t.PostAdminSupportMessage, s: t.SessionInfo):
    return person.post_admin_support_message(req, s)

@aget('/verification-request')
def get_verification_request(s: t.SessionInfo):
    return person.get_verification_request(s)

@adelete('/verification-document/<int:asset_id>')
def delete_verification_document(s: t.SessionInfo, asset_id: int):
    return person.delete_verification_document(asset_id, s)

@apost('/verify')
def post_verify(s: t.SessionInfo):
    limit = "8 per day"
    scope = "verify"

    with (
        limiter.limit(
            limit,
            scope=scope,
            exempt_when=disable_ip_rate_limit),
        limiter.limit(
            limit,
            scope=scope,
            key_func=limiter_account,
            exempt_when=disable_account_rate_limit)
    ):
        return person.post_verify(s)

@aget('/intro-review/<prospect_uuid>')
def get_intro_review(s: t.SessionInfo, prospect_uuid: str):
    if _requires_verification_gate(s):
        return _verification_gate_response()

    return person.get_intro_review(s, prospect_uuid)

@aget('/intro-gate-questions')
def get_intro_gate_questions(s: t.SessionInfo):
    return person.get_intro_gate_questions(s)

@aput('/intro-gate-questions')
@validate(t.PutIntroGateQuestions)
def put_intro_gate_questions(req: t.PutIntroGateQuestions, s: t.SessionInfo):
    return person.put_intro_gate_questions(req, s)

@aput('/admin/user/<int:person_id>/intro-gate-questions')
@validate(t.PutIntroGateQuestions)
def put_admin_intro_gate_questions(req: t.PutIntroGateQuestions, s: t.SessionInfo, person_id: int):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    return person.admin_put_intro_gate_questions(person_id, req)

@aget('/intro-requests')
def get_intro_requests(s: t.SessionInfo):
    if _requires_verification_gate(s):
        return _verification_gate_response()

    return person.get_intro_requests(s)

@aget('/intro-request/<prospect_uuid>')
def get_intro_request_state(s: t.SessionInfo, prospect_uuid: str):
    if _requires_verification_gate(s):
        return _verification_gate_response()

    return person.get_intro_request_state(s, prospect_uuid)

@apost('/intro-request/<prospect_uuid>')
@validate(t.PostIntroRequest)
def post_intro_request(req: t.PostIntroRequest, s: t.SessionInfo, prospect_uuid: str):
    if _requires_verification_gate(s):
        return _verification_gate_response()

    return person.post_intro_request(req, s, prospect_uuid)

@apost('/intro-request/<prospect_uuid>/accept')
def post_intro_request_accept(s: t.SessionInfo, prospect_uuid: str):
    if _requires_verification_gate(s):
        return _verification_gate_response()

    return person.accept_intro_request(s, prospect_uuid)

@apost('/intro-request/<prospect_uuid>/reject')
def post_intro_request_reject(s: t.SessionInfo, prospect_uuid: str):
    if _requires_verification_gate(s):
        return _verification_gate_response()

    return person.reject_intro_request(s, prospect_uuid)

@apost('/intro-review/<prospect_uuid>/request-more')
@validate(t.PostIntroReviewAction)
def post_intro_review_request_more(req: t.PostIntroReviewAction, s: t.SessionInfo, prospect_uuid: str):
    if _requires_verification_gate(s):
        return _verification_gate_response()

    return person.request_more_intro_review(s, prospect_uuid, req.prompt)

@apost('/intro-review/<prospect_uuid>/accept')
def post_intro_review_accept(s: t.SessionInfo, prospect_uuid: str):
    if _requires_verification_gate(s):
        return _verification_gate_response()

    return person.accept_intro_review(s, prospect_uuid)

@apost('/intro-review/<prospect_uuid>/reject')
def post_intro_review_reject(s: t.SessionInfo, prospect_uuid: str):
    if _requires_verification_gate(s):
        return _verification_gate_response()

    return person.reject_intro_review(s, prospect_uuid)

@aget('/courtship-state/<prospect_uuid>')
def get_courtship_state(s: t.SessionInfo, prospect_uuid: str):
    if _requires_verification_gate(s):
        return _verification_gate_response()

    return person.get_courtship_state(s, prospect_uuid)

@apatch('/courtship-state/<prospect_uuid>')
@validate(t.PatchCourtshipState)
def patch_courtship_state(req: t.PatchCourtshipState, s: t.SessionInfo, prospect_uuid: str):
    if _requires_verification_gate(s):
        return _verification_gate_response()

    return person.patch_courtship_state(req, s, prospect_uuid)

@aget('/check-verification')
def get_check_verification(s: t.SessionInfo):
    return person.get_check_verification(s=s)

@apost('/dismiss-donation')
def post_dismiss_donation(s: t.SessionInfo):
    return person.post_dismiss_donation(s=s)

@get('/stats')
def get_stats():
    return person.get_stats(ttl_hash=get_ttl_hash(seconds=60))


@get('/image/<path:filename>', limiter=limiter.exempt)
def get_public_image(filename: str):
    return person.get_public_image(filename)

@post('/external-report')
@validate(t.PostExternalReport)
def post_external_report(req: t.PostExternalReport):
    limit = "6 per day"
    scope = "external-report"

    with limiter.limit(limit, scope=scope, exempt_when=disable_ip_rate_limit):
        return person.create_external_report(req)

@get('/gender-stats')
def get_gender_stats():
    return person.get_gender_stats(ttl_hash=get_ttl_hash(seconds=60))

@get('/admin/ban-link/<token>')
def get_admin_ban_link(token: str):
    return person.get_admin_ban_link(token)

@get('/admin/ban/<token>')
def get_admin_ban(token: str):
    return person.get_admin_ban(token)

@get('/admin/delete-photo-link/<token>')
def get_admin_delete_photo_link(token: str):
    return person.get_admin_delete_photo_link(token)

@get('/admin/delete-photo/<token>')
def get_admin_delete_photo(token: str):
    return person.get_admin_delete_photo(token)


def _ensure_admin(s: t.SessionInfo):
    if not s.person_id:
        return False

    with api_tx('READ COMMITTED') as tx:
        row = tx.execute(
            """
            SELECT
                email,
                roles
            FROM person
            WHERE id = %(person_id)s
            """,
            {'person_id': s.person_id}
        ).fetchone()

    if not row:
        return False

    # Dev-friendly admin: admin/bot role or @example.com
    if row['roles'] and ('admin' in row['roles'] or 'bot' in row['roles']):
        return True

    if row['email'] and row['email'].lower().endswith('@example.com'):
        return True

    return False


def _requires_verification_gate(s: t.SessionInfo):
    if not s.person_id:
        return False

    with api_tx('READ COMMITTED') as tx:
        row = tx.execute(
            """
            SELECT
                verification_required,
                verification_level_id,
                waitlist_status
            FROM
                person
            WHERE
                id = %(person_id)s
            """,
            {'person_id': s.person_id},
        ).fetchone()

    if not row:
        return False

    return bool(
        (
            row['verification_required']
            and (row['verification_level_id'] or 0) <= 1
        )
        or row['waitlist_status'] != 'active'
    )


def _verification_gate_response():
    message = person.get_public_setting_value(
        'public_verification_gate_message',
        'Finish verification or wait for approval to unlock Feed, Search, Inbox, and Visitors.',
    )
    return {
        'error': 'verification_required',
        'message': message,
    }, 403


@aget('/admin/users')
def get_admin_users(s: t.SessionInfo):
    if not _ensure_admin(s):
        return 'Unauthorized', 401
    return person.get_admin_users()


@aget('/admin/user/<int:person_id>')
def get_admin_user(s: t.SessionInfo, person_id: int):
    if not _ensure_admin(s):
        return 'Unauthorized', 401
    return person.get_admin_user(person_id)


@apost('/admin/user')
def post_admin_user(s: t.SessionInfo):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    data = request.get_json(silent=True) or {}
    return person.create_admin_user(data)


@apatch('/admin/user/<int:person_id>')
def patch_admin_user(s: t.SessionInfo, person_id: int):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    data = request.get_json(silent=True) or {}
    return person.update_admin_user(person_id, data)


@aget('/admin/user/<int:person_id>/photos')
def get_admin_user_photos(s: t.SessionInfo, person_id: int):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    return person.get_admin_user_photos(person_id)


@adelete('/admin/user/<int:person_id>/photo/<photo_uuid>')
def delete_admin_user_photo(s: t.SessionInfo, person_id: int, photo_uuid: str):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    return person.delete_admin_user_photo(person_id, photo_uuid)


@apost('/admin/user/<int:person_id>/photo/<photo_uuid>/verify')
def post_admin_verify_user_photo(s: t.SessionInfo, person_id: int, photo_uuid: str):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    return person.verify_admin_user_photo(person_id, photo_uuid)


@adelete('/admin/user/<int:person_id>')
def delete_admin_user(s: t.SessionInfo, person_id: int):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    return person.deactivate_admin_user(person_id)


@apost('/admin/user/<int:person_id>/ban')
def post_admin_ban_user(s: t.SessionInfo, person_id: int):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    return person.hard_ban_admin_user(person_id)


@adelete('/admin/user/<int:person_id>/hard-delete')
def delete_admin_hard_delete_user(s: t.SessionInfo, person_id: int):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    return person.hard_delete_admin_user(person_id)


@aget('/admin/stats')
def get_admin_stats(s: t.SessionInfo):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    return person.get_admin_system_stats()


@aget('/admin/referrals')
def get_admin_referrals(s: t.SessionInfo):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    return person.get_admin_referrals()


@apost('/admin/referral-code/<int:code_id>/disable')
def post_admin_disable_referral_code(s: t.SessionInfo, code_id: int):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    return person.disable_admin_referral_code(code_id)


@aget('/admin/external-reports')
def get_admin_external_reports(s: t.SessionInfo):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    return person.get_admin_external_reports()


@aget('/admin/antiabuse-flags')
def get_admin_antiabuse_flags(s: t.SessionInfo):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    return person.get_admin_antiabuse_flags()


@apatch('/admin/antiabuse-flag/<int:flag_id>')
def patch_admin_antiabuse_flag(s: t.SessionInfo, flag_id: int):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    data = request.get_json(silent=True) or {}
    return person.update_admin_antiabuse_flag(
        flag_id,
        status=data.get('status', ''),
        resolution=data.get('resolution'),
        admin_note=data.get('admin_note'),
        resolved_by_person_id=s.person_id,
    )


@apatch('/admin/external-report/<int:report_id>')
def patch_admin_external_report(s: t.SessionInfo, report_id: int):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    data = request.get_json(silent=True) or {}
    return person.update_admin_external_report(
        report_id,
        status=data.get('status', ''),
        admin_note=data.get('admin_note'),
        reviewed_by_person_id=s.person_id,
    )


@aget('/admin/onboarding-steps')
def get_admin_onboarding_steps(s: t.SessionInfo):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    return person.get_admin_onboarding_steps()


@apost('/admin/onboarding-step')
def post_admin_onboarding_step(s: t.SessionInfo):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    data = request.get_json(silent=True) or {}
    return person.create_admin_onboarding_step(data)


@apatch('/admin/onboarding-step/<int:step_id>')
def patch_admin_onboarding_step(s: t.SessionInfo, step_id: int):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    data = request.get_json(silent=True) or {}
    return person.update_admin_onboarding_step(step_id, data)


@adelete('/admin/onboarding-step/<int:step_id>')
def delete_admin_onboarding_step(s: t.SessionInfo, step_id: int):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    return person.delete_admin_onboarding_step(step_id)


@aget('/admin/settings')
def get_admin_settings(s: t.SessionInfo):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    return person.get_admin_settings()


@aget('/admin/congregations')
def get_admin_congregations(s: t.SessionInfo):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    return congregation.get_admin_congregations(q=request.args.get('q'))


@apost('/admin/settings')
def post_admin_setting(s: t.SessionInfo):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    data = request.get_json(silent=True) or {}
    key = data.get('key')
    value = data.get('value')

    if not key or value is None:
        return 'Missing key/value', 400

    return person.upsert_admin_setting(key, str(value))


@apost('/admin/settings/bulk')
def post_admin_settings_bulk(s: t.SessionInfo):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    data = request.get_json(silent=True) or {}
    settings = data.get('settings')

    if not isinstance(settings, list) or not settings:
        return 'Missing settings', 400

    return person.upsert_admin_settings(settings)


@adelete('/admin/settings/<key>')
def delete_admin_setting(s: t.SessionInfo, key: str):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    return person.delete_admin_setting(key)


@get('/public-settings')
def get_public_settings():
    prefix = request.args.get('prefix', 'public_')
    return person.get_admin_settings_by_prefix(prefix)


@aget('/admin/questions')
def get_admin_questions(s: t.SessionInfo):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    return question.get_admin_questions()


@apost('/admin/question')
def post_admin_question(s: t.SessionInfo):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    data = request.get_json(silent=True) or {}
    return question.create_admin_question(data)


@apatch('/admin/question/<int:question_id>')
def patch_admin_question(s: t.SessionInfo, question_id: int):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    data = request.get_json(silent=True) or {}
    return question.update_admin_question(question_id, data)


@adelete('/admin/question/<int:question_id>')
def delete_admin_question(s: t.SessionInfo, question_id: int):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    return question.delete_admin_question(question_id)


@apost('/admin/questions/import')
def post_admin_questions_import(s: t.SessionInfo):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    data = request.get_json(silent=True)
    return question.import_admin_questions(data)


@aget('/admin/verification-requests')
def get_admin_verification_requests(s: t.SessionInfo):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    return person.get_admin_verification_reviews()


@aget('/admin/verification-request/<int:review_id>')
def get_admin_verification_request(s: t.SessionInfo, review_id: int):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    return person.get_admin_verification_review(review_id)


@adelete('/admin/verification-request/<int:review_id>/selfie')
def delete_admin_verification_request_selfie(s: t.SessionInfo, review_id: int):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    return person.delete_admin_verification_review_selfie(review_id)


@adelete('/admin/verification-request/<int:review_id>/asset/<int:asset_id>')
def delete_admin_verification_request_asset(s: t.SessionInfo, review_id: int, asset_id: int):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    return person.delete_admin_verification_review_asset(review_id, asset_id)


@apost('/admin/verification-request/<int:review_id>/asset/<int:asset_id>/verify')
def post_admin_verify_verification_request_asset(s: t.SessionInfo, review_id: int, asset_id: int):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    return person.verify_admin_verification_review_asset(review_id, asset_id)


@apost('/admin/verification-request/<int:review_id>/approve')
def post_admin_verification_request_approve(s: t.SessionInfo, review_id: int):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    data = request.get_json(silent=True) or {}
    return person.approve_admin_verification_review(
        review_id=review_id,
        reviewed_by_person_id=s.person_id,
        admin_message=data.get('admin_message'),
    )


@apost('/admin/verification-request/<int:review_id>/reject')
def post_admin_verification_request_reject(s: t.SessionInfo, review_id: int):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    data = request.get_json(silent=True) or {}
    return person.reject_admin_verification_review(
        review_id=review_id,
        reviewed_by_person_id=s.person_id,
        admin_message=data.get('admin_message'),
    )


@aget('/admin/support-threads')
def get_admin_support_threads(s: t.SessionInfo):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    return person.get_admin_support_threads()


@aget('/admin/support-thread/<int:person_id>')
def get_admin_support_thread_by_person(s: t.SessionInfo, person_id: int):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    return person.get_admin_support_thread_by_person(person_id)


@apost('/admin/support-thread/<int:person_id>/message')
@validate(t.PostAdminSupportMessage)
def post_admin_support_thread_message(req: t.PostAdminSupportMessage, s: t.SessionInfo, person_id: int):
    if not _ensure_admin(s):
        return 'Unauthorized', 401

    return person.post_admin_support_reply(person_id, req)


@aget('/export-data-token')
def get_export_data_token(s: t.SessionInfo):
    limit = "3 per day"
    scope = "export_data_token"

    with (
        limiter.limit(
            limit,
            scope=scope,
            exempt_when=disable_ip_rate_limit),
        limiter.limit(
            limit,
            scope=scope,
            key_func=limiter_account,
            exempt_when=disable_account_rate_limit)
    ):
        return person.get_export_data_token(s=s)

@get('/export-data/<token>')
def get_export_data(token: str):
    return person.get_export_data(token=token)

# RevenueCat / payments are no longer supported. Keep route for compatibility returns.
@post('/revenuecat')
def post_revenuecat():
    return 'Revenuecat endpoint is disabled', 404

@aget('/visitors')
def get_visitors(s: t.SessionInfo):
    if _requires_verification_gate(s):
        return _verification_gate_response()

    return person.get_visitors(s=s)

@apost('/mark-visitors-checked')
@validate(t.PostMarkVisitorsChecked)
def post_mark_visitors_checked(req: t.PostMarkVisitorsChecked, s: t.SessionInfo):
    return person.post_mark_visitors_checked(req=req, s=s)
