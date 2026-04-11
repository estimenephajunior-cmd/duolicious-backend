import os
import base64
import mimetypes
from database import api_tx, fetchall_sets
from typing import Any, Optional, Iterable, Tuple, Literal
import duotypes as t
import json
import secrets
from duohash import sha512
from PIL import Image
import io
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed
from service.person.sql import *
from service.search.sql import *
from commonsql import *
from service.person.template import otp_template
import traceback
import re
from smtp import aws_smtp
from flask import request, send_file
from dataclasses import dataclass
import psycopg
from functools import lru_cache
from antiabuse.antispam.signupemail import (
    check_and_update_bad_domains,
    normalize_email,
)
from antiabuse.lodgereport import (
    skip_by_uuid,
)
from antiabuse.firehol import firehol
import blurhash
import numpy
import erlastic
from datetime import datetime, timezone, date, timedelta
from dateutil.relativedelta import relativedelta
import string
from duoaudio import put_audio_in_object_store, audio_bucket
from service.person.aboutdiff import diff_addition_with_context
from verification.messages import (
    V_QUEUED,
    V_REUSED_SELFIE,
    V_UPLOADING_PHOTO,
)


class BytesEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            try:
                return obj.decode('utf-8')
            except:
                return str(obj)

        return super().default(obj)

DUO_ENV = os.environ['DUO_ENV']
REFERRAL_CODE_ALPHABET = string.ascii_uppercase + string.digits
REFERRAL_CODE_LENGTH = 8
REFERRAL_CODE_MAX_USES = 2
ADMIN_REFERRAL_CODE_MAX_USES = 1000000
MIN_AGE_BY_GENDER = {
    'Man': 25,
    'Woman': 21,
}
MIN_BAPTISM_YEARS = 2

R2_ACCT_ID = os.environ['DUO_R2_ACCT_ID']
R2_ACCESS_KEY_ID = os.environ['DUO_R2_ACCESS_KEY_ID']
R2_ACCESS_KEY_SECRET = os.environ['DUO_R2_ACCESS_KEY_SECRET']
R2_BUCKET_NAME = os.environ['DUO_R2_BUCKET_NAME']

BOTO_ENDPOINT_URL = os.getenv(
    'DUO_BOTO_ENDPOINT_URL',
    f'https://{R2_ACCT_ID}.r2.cloudflarestorage.com'
)

s3 = boto3.resource(
    's3',
    endpoint_url=BOTO_ENDPOINT_URL,
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_ACCESS_KEY_SECRET,
)

bucket = s3.Bucket(R2_BUCKET_NAME)


def _images_base_url() -> str:
    if os.environ.get('DUO_IMAGES_BASE_URL'):
        return os.environ['DUO_IMAGES_BASE_URL']
    if DUO_ENV.lower() in {'prod', 'production'}:
        return 'https://user-images.duolicious.app'
    return 'http://localhost:9090/s3-mock-bucket'


def _verification_photo_url(photo_uuid: str | None) -> str | None:
    if not photo_uuid:
        return None
    return f"{_images_base_url()}/450-{photo_uuid}.jpg"


def _verification_photo_data_url(photo_uuid: str | None) -> str | None:
    if not photo_uuid:
        return None

    try:
        obj = bucket.Object(f'450-{photo_uuid}.jpg')
        body = obj.get()['Body'].read()
        if not body:
            return None
        return (
            'data:image/jpeg;base64,'
            + base64.b64encode(body).decode('ascii')
        )
    except Exception:
        return None


def _profile_photo_data_url(photo_uuid: str | None) -> str | None:
    if not photo_uuid:
        return None

    try:
        obj = bucket.Object(f'450-{photo_uuid}.jpg')
        body = obj.get()['Body'].read()
        if not body:
            return None
        return (
            'data:image/jpeg;base64,'
            + base64.b64encode(body).decode('ascii')
        )
    except Exception:
        return None


def get_public_image(filename: str):
    if not re.fullmatch(r'[\w.\-]+', filename):
        return 'Invalid image path', 400

    try:
        body = bucket.Object(filename).get()['Body'].read()
    except Exception:
        return 'Not found', 404

    if not body:
        return 'Not found', 404

    mimetype = mimetypes.guess_type(filename)[0] or 'application/octet-stream'

    return send_file(
        io.BytesIO(body),
        mimetype=mimetype,
        download_name=filename,
        max_age=60 * 60 * 24 * 30,
        conditional=True,
    )


def _intro_request_audio_data_url(audio_uuid: str | None) -> str | None:
    if not audio_uuid:
        return None

    try:
        body = audio_bucket.Object(f'{audio_uuid}.aac').get()['Body'].read()
        if not body:
            return None
        return (
            'data:audio/aac;base64,'
            + base64.b64encode(body).decode('ascii')
        )
    except Exception:
        return None


def _admin_support_attachment_data_url(
    attachment_bytes: bytes | memoryview | None,
    attachment_mime: str | None,
) -> str | None:
    if attachment_bytes is None or not attachment_mime:
        return None

    raw_bytes = (
        attachment_bytes.tobytes()
        if isinstance(attachment_bytes, memoryview)
        else attachment_bytes
    )

    return (
        f'data:{attachment_mime};base64,'
        f"{base64.b64encode(raw_bytes).decode('ascii')}"
    )


def _serialize_admin_support_message(row: Any) -> dict[str, Any]:
    message = dict(row)
    attachment_bytes = message.pop('attachment_bytes', None)
    message['attachment_data_url'] = _admin_support_attachment_data_url(
        attachment_bytes=attachment_bytes,
        attachment_mime=message.get('attachment_mime'),
    )
    message['has_attachment'] = bool(message.get('attachment_data_url'))
    return message


def _hydrate_verification_review_media(row: Any) -> dict[str, Any]:
    review = dict(row)
    review['selfie_photo_url'] = (
        _verification_photo_data_url(review.get('selfie_photo_uuid'))
        or _verification_photo_url(review.get('selfie_photo_uuid'))
    )
    review['assets'] = [
        {
            **dict(asset),
            'photo_url': (
                _verification_photo_data_url(dict(asset).get('photo_uuid'))
                or _verification_photo_url(dict(asset).get('photo_uuid'))
            ),
        }
        for asset in (review.get('assets') or [])
    ]
    if (
        review.get('admin_status') == 'pending'
        and not review.get('verification_required', True)
        and int(review.get('verification_level_id') or 0) > 1
    ):
        review['admin_status'] = 'approved'
        if not (review.get('admin_message') or '').strip():
            review['admin_message'] = 'Already verified'
    return review


def _parse_iso8601_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None

    normalized = value.replace('Z', '+00:00')

    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _is_verification_review_finalized(review: dict[str, Any]) -> bool:
    if review.get('admin_status') in {'approved', 'rejected'}:
        return True

    if not review.get('verification_required', True) and int(review.get('verification_level_id') or 0) > 1:
        return True

    return False

def init_db():
    pass


def _parse_iso_date(value: str | date | None) -> date | None:
    if not value:
        return None
    if isinstance(value, date):
        return value
    return datetime.strptime(value, '%Y-%m-%d').date()


def _format_date(value: date) -> str:
    return value.strftime('%B %d, %Y')


def _opposite_gender(gender: str | None) -> str | None:
    if gender == 'Man':
        return 'Woman'
    if gender == 'Woman':
        return 'Man'
    return None


def _minimum_birth_date(gender: str, today: date | None = None) -> date:
    today_ = today or date.today()
    return today_ - relativedelta(years=MIN_AGE_BY_GENDER[gender])


def _minimum_baptism_date(today: date | None = None) -> date:
    today_ = today or date.today()
    return today_ - relativedelta(years=MIN_BAPTISM_YEARS)


def _compute_registration_error(
    *,
    gender: str | None,
    date_of_birth: date | None,
    baptism_date: date | None,
) -> str | None:
    if gender not in MIN_AGE_BY_GENDER:
        return None

    if date_of_birth is None or baptism_date is None:
        return None

    today_ = date.today()
    minimum_birth_date = _minimum_birth_date(gender, today_)
    minimum_baptism_date = _minimum_baptism_date(today_)

    age_ok = date_of_birth <= minimum_birth_date
    baptism_ok = baptism_date <= minimum_baptism_date

    if age_ok and baptism_ok:
        return None

    eligible_by_age = date_of_birth + relativedelta(years=MIN_AGE_BY_GENDER[gender])
    eligible_by_baptism = baptism_date + relativedelta(years=MIN_BAPTISM_YEARS)
    eligible_on = max(eligible_by_age, eligible_by_baptism)

    if not age_ok and baptism_ok:
        return f'Oops 😅 Come back after {_format_date(eligible_by_age)}.'

    if age_ok and not baptism_ok:
        return f'Almost there 🙏 You can register after {_format_date(eligible_by_baptism)}.'

    return f'Oops 😅 Come back after {_format_date(eligible_on)}.'


def _get_onboardee_snapshot(tx, email: str) -> dict[str, Any]:
    row = tx.execute(
        """
        SELECT
            onboardee.email,
            onboardee.date_of_birth,
            onboardee.baptism_date,
            gender.name AS gender,
            onboardee.congregation_id
        FROM onboardee
        LEFT JOIN gender ON gender.id = onboardee.gender_id
        WHERE onboardee.email = %(email)s
        """,
        {'email': email},
    ).fetchone()

    return dict(row or {})


def _validate_onboarding_snapshot(
    *,
    gender: str | None,
    date_of_birth: str | None,
    baptism_date: str | None,
) -> str | None:
    return _compute_registration_error(
        gender=gender,
        date_of_birth=_parse_iso_date(date_of_birth),
        baptism_date=_parse_iso_date(baptism_date),
    )


def _count_referral_uses(tx, referral_code_id: int) -> int:
    row = tx.execute(
        """
        SELECT COUNT(*) AS count
        FROM person
        WHERE referred_by_code_id = %(referral_code_id)s
        """,
        {'referral_code_id': referral_code_id},
    ).fetchone()

    return int(row['count']) if row else 0


def _get_referral_code(tx, code: str) -> dict[str, Any] | None:
    row = tx.execute(
        """
        SELECT
            referral_code.*,
            person.email AS owner_email,
            person.name AS owner_name
        FROM referral_code
        JOIN person ON person.id = referral_code.person_id
        WHERE referral_code.code = %(code)s
        """,
        {'code': code},
    ).fetchone()

    return dict(row) if row else None


def _is_admin_person(tx, person_id: int | None) -> bool:
    if not person_id:
        return False

    row = tx.execute(
        """
        SELECT
            email,
            roles
        FROM person
        WHERE id = %(person_id)s
        """,
        {'person_id': person_id},
    ).fetchone()

    if not row:
        return False

    roles = row['roles'] or []
    email = (row['email'] or '').lower()

    return (
        'admin' in roles
        or 'bot' in roles
        or email.endswith('@example.com')
    )


def _validate_referral_code_for_signup(tx, referral_code: str) -> tuple[int | None, str | None]:
    row = _get_referral_code(tx, referral_code)

    if not row:
        return None, "Oops 😅 This referral code doesn’t exist."

    trust_summary = _get_referral_trust_summary(tx, int(row['person_id']))

    if row['disabled'] or row['replaced_at'] is not None:
        if trust_summary.get('is_admin_bypass'):
            return int(row['id']), None
        return None, "This code has reached its limit. Please ask for a new one."

    if not trust_summary['can_invite']:
        return None, (
            trust_summary.get('pause_reason')
            or "This code is temporarily paused while referral quality is reviewed."
        )

    if (
        not trust_summary.get('is_admin_bypass')
        and _count_referral_uses(tx, row['id']) >= trust_summary['invite_limit']
    ):
        _replace_referral_code(tx, row['id'])
        return None, "This code has reached its limit. Please ask for a new one."

    return int(row['id']), None


def _replace_referral_code(tx, referral_code_id: int) -> dict[str, Any] | None:
    row = tx.execute(
        """
        UPDATE referral_code
        SET
            disabled = TRUE,
            replaced_at = COALESCE(replaced_at, NOW())
        WHERE id = %(id)s
        AND disabled = FALSE
        AND replaced_at IS NULL
        RETURNING person_id
        """,
        {'id': referral_code_id},
    ).fetchone()

    if not row:
        return None

    return _generate_referral_code(tx, row['person_id'])


def _generate_referral_code(tx, person_id: int) -> dict[str, Any]:
    for _ in range(32):
        code = ''.join(secrets.choice(REFERRAL_CODE_ALPHABET) for _ in range(REFERRAL_CODE_LENGTH))
        row = tx.execute(
            """
            INSERT INTO referral_code (person_id, code)
            VALUES (%(person_id)s, %(code)s)
            ON CONFLICT (code) DO NOTHING
            RETURNING *
            """,
            {'person_id': person_id, 'code': code},
        ).fetchone()
        if row:
            return dict(row)

    raise RuntimeError('Unable to generate referral code')


def _get_latest_referral_code(tx, person_id: int) -> dict[str, Any] | None:
    row = tx.execute(
        """
        SELECT *
        FROM referral_code
        WHERE person_id = %(person_id)s
        AND disabled = FALSE
        AND replaced_at IS NULL
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        {'person_id': person_id},
    ).fetchone()

    return dict(row) if row else None


def _get_admin_setting_int(tx, key: str, default: int) -> int:
    row = tx.execute(
        """
        SELECT value
        FROM admin_setting
        WHERE key = %(key)s
        LIMIT 1
        """,
        {'key': key},
    ).fetchone()

    if not row:
        return default

    try:
        return int(row['value'])
    except (TypeError, ValueError):
        return default


def _get_admin_setting_bool(tx, key: str, default: bool) -> bool:
    row = tx.execute(
        """
        SELECT value
        FROM admin_setting
        WHERE key = %(key)s
        """,
        {'key': key},
    ).fetchone()

    if not row:
        return default

    value = str(row['value']).strip().lower()
    if value in {'1', 'true', 'yes', 'on'}:
        return True
    if value in {'0', 'false', 'no', 'off'}:
        return False
    return default


def _get_admin_setting_str(tx, key: str, default: str) -> str:
    row = tx.execute(
        """
        SELECT value
        FROM admin_setting
        WHERE key = %(key)s
        LIMIT 1
        """,
        {'key': key},
    ).fetchone()

    if not row:
        return default

    value = str(row['value'] or '').strip()
    return value or default


def get_public_setting_value(key: str, default: str) -> str:
    with api_tx('READ COMMITTED') as tx:
        return _get_admin_setting_str(tx, key, default)


def _get_required_q_and_a_count(tx) -> int:
    configured = max(1, _get_admin_setting_int(tx, 'system_required_q_and_a_min', 30))
    row = tx.execute(
        """
        SELECT COUNT(*) AS count
        FROM question
        """
    ).fetchone()
    total_questions = int(row['count'] or 0) if row else 0
    if total_questions <= 0:
        return 0
    return min(configured, total_questions)


def _get_access_gate_state(tx, person_id: int | None) -> dict[str, Any]:
    if not person_id:
        return {
            'has_full_access': False,
            'access_gate': None,
        }

    row = tx.execute(
        """
        SELECT
            verification_required,
            verification_level_id,
            profile_status,
            waitlist_status,
            count_answers,
            roles
        FROM person
        WHERE id = %(person_id)s
        LIMIT 1
        """,
        {'person_id': person_id},
    ).fetchone()

    if not row:
        return {
            'has_full_access': False,
            'access_gate': None,
        }

    roles = row['roles'] or []
    is_admin_bypass = isinstance(roles, list) and 'admin' in roles
    verification_gate_open = (
        (not bool(row['verification_required']) or int(row['verification_level_id'] or 0) > 1)
        and str(row['waitlist_status'] or 'active') == 'active'
    )

    answered_count = int(row['count_answers'] or 0)
    required_count = _get_required_q_and_a_count(tx)
    remaining_count = max(0, required_count - answered_count)
    q_and_a_gate_open = is_admin_bypass or required_count <= 0 or remaining_count <= 0

    if not verification_gate_open:
        return {
            'has_full_access': False,
            'access_gate': {
                'kind': 'verification_required',
                'message': 'Verification is still required before full access unlocks.',
            },
            'required_q_and_a_count': required_count,
            'answered_q_and_a_count': answered_count,
            'remaining_required_q_and_a_count': remaining_count,
        }

    if not q_and_a_gate_open:
        return {
            'has_full_access': False,
            'access_gate': {
                'kind': 'qa_required',
                'message': f'Answer {remaining_count} more Q&A question{"s" if remaining_count != 1 else ""} to unlock the rest of the app.',
            },
            'required_q_and_a_count': required_count,
            'answered_q_and_a_count': answered_count,
            'remaining_required_q_and_a_count': remaining_count,
        }

    return {
        'has_full_access': True,
        'access_gate': None,
        'required_q_and_a_count': required_count,
        'answered_q_and_a_count': answered_count,
        'remaining_required_q_and_a_count': remaining_count,
    }


def _get_jw_quiz_question_count(tx) -> int:
    return max(3, min(8, _get_admin_setting_int(tx, 'system_jw_quiz_question_count', 5)))


def _get_jw_quiz_time_limit_seconds(tx) -> int:
    return max(15, min(300, _get_admin_setting_int(tx, 'system_jw_quiz_time_limit_seconds', 45)))


def _get_jw_quiz_cooldown_days(tx) -> int:
    return max(1, min(90, _get_admin_setting_int(tx, 'system_jw_quiz_cooldown_days', 7)))


def _get_jw_quiz_cooldown_until(tx, normalized_email: str) -> datetime | None:
    row = tx.execute(
        """
        SELECT MAX(cooldown_until) AS cooldown_until
        FROM jw_quiz_attempt
        WHERE normalized_email = %(normalized_email)s
        """,
        {'normalized_email': normalized_email},
    ).fetchone()
    return row['cooldown_until'] if row else None


def _build_jw_quiz_payload(tx, question_count: int) -> list[dict[str, Any]]:
    rows = tx.execute(
        """
        SELECT
            id,
            prompt,
            options,
            correct_option
        FROM jw_quiz_question
        WHERE is_active = TRUE
        ORDER BY MD5(id::TEXT || ' ' || gen_random_uuid()::TEXT)
        LIMIT %(question_count)s
        """,
        {'question_count': question_count},
    ).fetchall()

    payload = []
    for row in rows:
        options = list(row['options'] or [])
        secrets.SystemRandom().shuffle(options)
        payload.append({
            'question_id': int(row['id']),
            'prompt': row['prompt'],
            'options': options,
            'correct_option': row['correct_option'],
        })
    return payload


def _get_device_fingerprint() -> str:
    return (request.headers.get('X-Duo-Device-Fingerprint') or '').strip()[:256]


def _count_profile_photos(tx, person_id: int) -> int:
    row = tx.execute(
        """
        SELECT COUNT(*) AS count
        FROM photo
        WHERE person_id = %(person_id)s
        """,
        {'person_id': person_id},
    ).fetchone()

    return int(row['count'] or 0) if row else 0


def _person_exists_with_normalized_email(
    tx,
    *,
    normalized_email: str,
    exclude_person_id: int | None = None,
) -> bool:
    row = tx.execute(
        """
        SELECT id
        FROM person
        WHERE
            normalized_email = %(normalized_email)s
        AND (
            %(exclude_person_id)s::INT IS NULL
            OR id <> %(exclude_person_id)s
        )
        LIMIT 1
        """,
        {
            'normalized_email': normalized_email,
            'exclude_person_id': exclude_person_id,
        },
    ).fetchone()
    return bool(row)


def _get_person_trust_summary(tx, person_id: int) -> dict[str, Any]:
    row = tx.execute(
        """
        WITH profile AS (
            SELECT
                person.id,
                person.sign_up_time,
                person.activated,
                person.profile_status,
                person.waitlist_status,
                person.verification_level_id,
                person.about,
                person.count_answers,
                person.referred_by_code_id,
                EXISTS (
                    SELECT 1
                    FROM audio
                    WHERE audio.person_id = person.id
                ) AS has_audio,
                (
                    SELECT COUNT(*)
                    FROM photo
                    WHERE photo.person_id = person.id
                ) AS photo_count,
                (
                    SELECT COUNT(*)
                    FROM antiabuse_flag
                    WHERE
                        antiabuse_flag.person_id = person.id
                    AND antiabuse_flag.status = 'resolved'
                    AND antiabuse_flag.resolution IN ('warning', 'temporary_restriction', 'permanent_ban')
                    AND antiabuse_flag.created_at > NOW() - INTERVAL '120 days'
                ) AS validated_issue_count,
                (
                    SELECT COUNT(*)
                    FROM antiabuse_flag
                    WHERE
                        antiabuse_flag.person_id = person.id
                    AND antiabuse_flag.status = 'resolved'
                    AND antiabuse_flag.resolution = 'warning'
                    AND antiabuse_flag.created_at > NOW() - INTERVAL '120 days'
                ) AS validated_warning_count,
                (
                    SELECT COUNT(*)
                    FROM antiabuse_flag
                    WHERE
                        antiabuse_flag.person_id = person.id
                    AND antiabuse_flag.status = 'resolved'
                    AND antiabuse_flag.resolution = 'temporary_restriction'
                    AND antiabuse_flag.created_at > NOW() - INTERVAL '120 days'
                ) AS validated_temporary_count,
                (
                    SELECT COUNT(*)
                    FROM antiabuse_flag
                    WHERE
                        antiabuse_flag.person_id = person.id
                    AND antiabuse_flag.status = 'resolved'
                    AND antiabuse_flag.resolution = 'permanent_ban'
                    AND antiabuse_flag.created_at > NOW() - INTERVAL '120 days'
                ) AS validated_permaban_count,
                (
                    SELECT COUNT(*)
                    FROM antiabuse_flag
                    WHERE
                        antiabuse_flag.person_id = person.id
                    AND antiabuse_flag.status IN ('open', 'reviewing')
                ) AS open_flag_count,
                (
                    SELECT COUNT(*)
                    FROM skipped
                    WHERE
                        skipped.object_person_id = person.id
                    AND skipped.reported
                    AND skipped.report_reason <> ''
                    AND skipped.created_at > NOW() - INTERVAL '120 days'
                ) AS raw_report_count,
                (
                    SELECT COUNT(*)
                    FROM intro_review
                    WHERE
                        intro_review.from_person_id = person.id
                    AND intro_review.status = 'accepted'
                    AND intro_review.accepted_at > NOW() - INTERVAL '120 days'
                ) AS accepted_intro_count,
                (
                    SELECT COUNT(*)
                    FROM intro_review
                    WHERE
                        intro_review.from_person_id = person.id
                    AND intro_review.status = 'rejected'
                    AND intro_review.rejected_at > NOW() - INTERVAL '120 days'
                ) AS rejected_intro_count
            FROM person
            WHERE person.id = %(person_id)s
        )
        SELECT *
        FROM profile
        """,
        {'person_id': person_id},
    ).fetchone()

    if not row:
        return {
            'trust_score': 0,
            'level': 'warning',
            'signals': [],
            'milestones': [],
            'next_steps': [],
            'stage_label': 'Getting started',
            'report_count': 0,
            'raw_report_count': 0,
            'validated_issue_count': 0,
            'open_flag_count': 0,
            'profile_complete': False,
        }

    photo_count = int(row['photo_count'] or 0)
    count_answers = int(row['count_answers'] or 0)
    validated_issue_count = int(row['validated_issue_count'] or 0)
    validated_warning_count = int(row['validated_warning_count'] or 0)
    validated_temporary_count = int(row['validated_temporary_count'] or 0)
    validated_permaban_count = int(row['validated_permaban_count'] or 0)
    open_flag_count = int(row['open_flag_count'] or 0)
    raw_report_count = int(row['raw_report_count'] or 0)
    accepted_intro_count = int(row['accepted_intro_count'] or 0)
    rejected_intro_count = int(row['rejected_intro_count'] or 0)
    established_account = bool(
        row['sign_up_time'] and
        row['sign_up_time'] < datetime.utcnow() - timedelta(days=30)
    )
    about = row['about'] or ''
    profile_complete = (
        photo_count >= 3 and
        len(about.strip()) >= 80 and
        count_answers >= 20
    )

    trust_score = 45
    signals: list[str] = []

    if (row['verification_level_id'] or 0) > 1:
        trust_score += 20
        signals.append('Verified identity')

    if profile_complete:
        trust_score += 10
        signals.append('Complete profile')

    if row['has_audio']:
        trust_score += 5
        signals.append('Audio bio added')

    if row['referred_by_code_id'] is not None:
        trust_score += 10
        signals.append('Referral-backed signup')

    if established_account:
        trust_score += 5
        signals.append('Established account')

    q_and_a_bonus = min(30, max(0, count_answers) // 10 * 5)
    if q_and_a_bonus > 0:
        trust_score += q_and_a_bonus
        signals.append('Answered meaningful Q&A')

    if validated_issue_count == 0 and row['sign_up_time'] and row['sign_up_time'] < datetime.utcnow() - timedelta(days=14):
        trust_score += 10
        signals.append('Clean moderation history')

    if not row['activated']:
        trust_score -= 25

    if row['profile_status'] == 'serious':
        signals.append('In a serious connection')
    elif row['profile_status'] == 'paused':
        trust_score -= 5

    if row['waitlist_status'] != 'active':
        trust_score -= 15

    trust_score -= min(10, rejected_intro_count * 2)
    trust_score -= min(12, validated_warning_count * 6)
    trust_score -= min(25, validated_temporary_count * 12)
    trust_score -= min(50, validated_permaban_count * 25)
    trust_score -= min(20, open_flag_count * 8)
    trust_score = max(0, min(100, trust_score))

    good_threshold = _get_admin_setting_int(tx, 'system_trust_good_threshold', 70)
    warning_threshold = _get_admin_setting_int(tx, 'system_trust_warning_threshold', 40)

    level = 'good' if trust_score >= good_threshold else 'warning'
    if trust_score < warning_threshold:
        level = 'high-risk'

    milestones: list[str] = []

    if (row['verification_level_id'] or 0) > 1:
        milestones.append('Verified member')

    if profile_complete:
        milestones.append('Complete profile')

    if row['referred_by_code_id'] is not None:
        milestones.append('Referral-backed introduction')

    if established_account:
        milestones.append('Established member')

    if count_answers >= 60:
        milestones.append('Deep Q&A profile')
    elif count_answers >= 20:
        milestones.append('Thoughtful Q&A profile')

    if trust_score >= good_threshold:
        milestones.append('Trusted member')
    elif trust_score >= warning_threshold:
        milestones.append('Growing trust')

    next_steps: list[str] = []

    if (row['verification_level_id'] or 0) <= 1:
        next_steps.append('Complete verification to unlock stronger trust signals.')

    if photo_count < 3:
        next_steps.append('Add at least 3 profile photos.')

    if len(about.strip()) < 80:
        next_steps.append('Write a fuller about section so people can understand you quickly.')

    if count_answers < 20:
        next_steps.append('Answer more Q&A so the system can place you more accurately.')

    if validated_issue_count == 0 and open_flag_count == 0 and trust_score < good_threshold:
        next_steps.append('Keep a clean moderation history and your trust score will keep rising.')

    if trust_score >= good_threshold:
        stage_label = 'Trusted member'
    elif trust_score >= warning_threshold:
        stage_label = 'Building trust'
    else:
        stage_label = 'Early trust review'

    return {
        'trust_score': trust_score,
        'level': level,
        'signals': signals,
        'milestones': milestones[:6],
        'next_steps': next_steps[:4],
        'stage_label': stage_label,
        'report_count': validated_issue_count,
        'raw_report_count': raw_report_count,
        'validated_issue_count': validated_issue_count,
        'validated_warning_count': validated_warning_count,
        'validated_temporary_count': validated_temporary_count,
        'validated_permaban_count': validated_permaban_count,
        'open_flag_count': open_flag_count,
        'profile_complete': profile_complete,
        'photo_count': photo_count,
        'answer_count': count_answers,
    }


def _create_or_touch_antiabuse_flag(
    tx,
    *,
    person_id: int,
    category: str,
    severity: str,
    reason: str,
    evidence: dict[str, Any],
) -> None:
    existing = tx.execute(
        """
        SELECT id
        FROM antiabuse_flag
        WHERE
            person_id = %(person_id)s
        AND category = %(category)s
        AND status IN ('open', 'reviewing')
        ORDER BY id DESC
        LIMIT 1
        """,
        {
            'person_id': person_id,
            'category': category,
            'severity': severity,
            'reason': reason,
            'evidence': json.dumps(evidence),
        },
    ).fetchone()

    if existing:
        tx.execute(
            """
            UPDATE antiabuse_flag
            SET
                severity = %(severity)s,
                reason = %(reason)s,
                evidence = %(evidence)s::jsonb,
                updated_at = NOW()
            WHERE id = %(flag_id)s
            """,
            {
                'flag_id': existing['id'],
                'severity': severity,
                'reason': reason,
                'evidence': json.dumps(evidence),
            },
        )
        return

    tx.execute(
        """
        INSERT INTO antiabuse_flag (
            person_id,
            category,
            severity,
            reason,
            evidence
        )
        VALUES (
            %(person_id)s,
            %(category)s,
            %(severity)s,
            %(reason)s,
            %(evidence)s::jsonb
        )
        """,
        {
            'person_id': person_id,
            'category': category,
            'severity': severity,
            'reason': reason,
            'evidence': json.dumps(evidence),
        },
    )


def _detect_reporting_coalition(tx, *, target_person_id: int) -> None:
    coalition_threshold = max(2, _get_admin_setting_int(tx, 'system_coalition_reporter_threshold', 2))
    coalition_window_days = max(1, _get_admin_setting_int(tx, 'system_coalition_window_days', 30))

    same_owner_rows = tx.execute(
        """
        WITH recent_reporters AS (
            SELECT DISTINCT
                skipped.subject_person_id AS reporter_id,
                referral_code.person_id AS referral_owner_id
            FROM skipped
            JOIN person AS reporter ON reporter.id = skipped.subject_person_id
            LEFT JOIN referral_code ON referral_code.id = reporter.referred_by_code_id
            WHERE
                skipped.object_person_id = %(target_person_id)s
            AND skipped.reported
            AND skipped.report_reason <> ''
            AND skipped.created_at > NOW() - (%(window_days)s || ' days')::interval
        )
        SELECT
            referral_owner_id,
            ARRAY_AGG(reporter_id ORDER BY reporter_id) AS reporter_ids,
            COUNT(*) AS reporter_count
        FROM recent_reporters
        WHERE referral_owner_id IS NOT NULL
        GROUP BY referral_owner_id
        HAVING COUNT(*) >= %(coalition_threshold)s
        """,
        {
            'target_person_id': target_person_id,
            'window_days': coalition_window_days,
            'coalition_threshold': coalition_threshold,
        },
    ).fetchall()

    for row in same_owner_rows:
        reporter_ids = [int(x) for x in (row['reporter_ids'] or [])]
        evidence = {
            'target_person_id': target_person_id,
            'reporter_ids': reporter_ids,
            'reporter_count': int(row['reporter_count'] or 0),
            'shared_referral_owner_id': int(row['referral_owner_id']),
            'window_days': coalition_window_days,
        }
        owner_id = int(row['referral_owner_id'])
        _create_or_touch_antiabuse_flag(
            tx,
            person_id=owner_id,
            category='coalition-reporting-ring',
            severity='high',
            reason='Multiple referred accounts reported the same target within a short window.',
            evidence={**evidence, 'scope': 'owner'},
        )
        for reporter_id in reporter_ids:
            _create_or_touch_antiabuse_flag(
                tx,
                person_id=reporter_id,
                category='coalition-reporting-ring',
                severity='high',
                reason='This account appears to be part of a coordinated reporting cluster.',
                evidence={**evidence, 'scope': 'member'},
            )

    shared_device_rows = tx.execute(
        """
        WITH recent_reporters AS (
            SELECT DISTINCT skipped.subject_person_id AS reporter_id
            FROM skipped
            WHERE
                skipped.object_person_id = %(target_person_id)s
            AND skipped.reported
            AND skipped.report_reason <> ''
            AND skipped.created_at > NOW() - (%(window_days)s || ' days')::interval
        ), reporter_devices AS (
            SELECT DISTINCT
                recent_reporters.reporter_id,
                duo_session.device_fingerprint
            FROM recent_reporters
            JOIN duo_session ON duo_session.person_id = recent_reporters.reporter_id
            WHERE COALESCE(duo_session.device_fingerprint, '') <> ''
        )
        SELECT
            device_fingerprint,
            ARRAY_AGG(DISTINCT reporter_id ORDER BY reporter_id) AS reporter_ids,
            COUNT(DISTINCT reporter_id) AS reporter_count
        FROM reporter_devices
        GROUP BY device_fingerprint
        HAVING COUNT(DISTINCT reporter_id) >= %(coalition_threshold)s
        """,
        {
            'target_person_id': target_person_id,
            'window_days': coalition_window_days,
            'coalition_threshold': coalition_threshold,
        },
    ).fetchall()

    for row in shared_device_rows:
        reporter_ids = [int(x) for x in (row['reporter_ids'] or [])]
        evidence = {
            'target_person_id': target_person_id,
            'reporter_ids': reporter_ids,
            'reporter_count': int(row['reporter_count'] or 0),
            'shared_device_fingerprint': row['device_fingerprint'],
            'window_days': coalition_window_days,
        }
        for reporter_id in reporter_ids:
            _create_or_touch_antiabuse_flag(
                tx,
                person_id=reporter_id,
                category='coalition-reporting-ring',
                severity='high',
                reason='Multiple linked reporters with the same device fingerprint reported one target.',
                evidence={**evidence, 'scope': 'device-cluster'},
            )


def _detect_referral_ring_for_owner(tx, *, owner_person_id: int) -> None:
    member_threshold = max(2, _get_admin_setting_int(tx, 'system_referral_ring_member_threshold', 3))
    flagged_member_threshold = max(1, _get_admin_setting_int(tx, 'system_referral_ring_flagged_member_threshold', 2))
    coalition_window_days = max(1, _get_admin_setting_int(tx, 'system_coalition_window_days', 30))

    row = tx.execute(
        """
        WITH target_codes AS (
            SELECT id
            FROM referral_code
            WHERE person_id = %(owner_person_id)s
        ), referred_people AS (
            SELECT
                person.id,
                EXISTS (
                    SELECT 1
                    FROM antiabuse_flag
                    WHERE
                        antiabuse_flag.person_id = person.id
                    AND antiabuse_flag.status IN ('open', 'reviewing')
                ) AS has_open_flag,
                EXISTS (
                    SELECT 1
                    FROM antiabuse_flag
                    WHERE
                        antiabuse_flag.person_id = person.id
                    AND antiabuse_flag.status = 'resolved'
                    AND antiabuse_flag.resolution IN ('warning', 'temporary_restriction', 'permanent_ban')
                ) AS has_validated_issue
            FROM person
            WHERE
                person.referred_by_code_id IN (SELECT id FROM target_codes)
            AND person.sign_up_time > NOW() - (%(window_days)s || ' days')::interval
        ), shared_device_clusters AS (
            SELECT
                duo_session.device_fingerprint,
                COUNT(DISTINCT duo_session.person_id) AS member_count
            FROM duo_session
            JOIN referred_people ON referred_people.id = duo_session.person_id
            WHERE COALESCE(duo_session.device_fingerprint, '') <> ''
            GROUP BY duo_session.device_fingerprint
            HAVING COUNT(DISTINCT duo_session.person_id) >= 2
        )
        SELECT
            COALESCE((SELECT COUNT(*) FROM referred_people), 0) AS member_count,
            COALESCE((SELECT COUNT(*) FROM referred_people WHERE has_open_flag OR has_validated_issue), 0) AS suspicious_member_count,
            COALESCE((SELECT SUM(member_count) FROM shared_device_clusters), 0) AS shared_device_member_count,
            COALESCE((
                SELECT ARRAY_AGG(id ORDER BY id)
                FROM referred_people
                WHERE has_open_flag OR has_validated_issue
            ), '{}'::INT[]) AS suspicious_member_ids
        """,
        {
            'owner_person_id': owner_person_id,
            'window_days': coalition_window_days,
        },
    ).fetchone()

    if not row:
        return

    member_count = int(row['member_count'] or 0)
    suspicious_member_count = int(row['suspicious_member_count'] or 0)
    shared_device_member_count = int(row['shared_device_member_count'] or 0)

    if member_count < member_threshold:
        return

    if suspicious_member_count < flagged_member_threshold and shared_device_member_count < flagged_member_threshold:
        return

    _create_or_touch_antiabuse_flag(
        tx,
        person_id=owner_person_id,
        category='referral-ring-pattern',
        severity='high',
        reason='A referral cluster shows multiple suspicious linked accounts within the same referral network.',
        evidence={
            'member_count': member_count,
            'suspicious_member_count': suspicious_member_count,
            'shared_device_member_count': shared_device_member_count,
            'suspicious_member_ids': [int(x) for x in (row['suspicious_member_ids'] or [])],
            'member_threshold': member_threshold,
            'flagged_member_threshold': flagged_member_threshold,
            'window_days': coalition_window_days,
        },
    )


def _refresh_antiabuse_flags(tx, *, person_id: int) -> None:
    max_accounts_per_device = _get_admin_setting_int(tx, 'system_max_accounts_per_device', 2)
    max_accounts_per_ip = _get_admin_setting_int(tx, 'system_max_accounts_per_ip_30d', 4)
    warning_threshold = _get_admin_setting_int(tx, 'system_warning_report_threshold', 1)
    temp_threshold = _get_admin_setting_int(tx, 'system_temporary_ban_report_threshold', 3)
    perm_threshold = _get_admin_setting_int(tx, 'system_permaban_report_threshold', 5)

    row = tx.execute(
        """
        SELECT
            person.id,
            (
                SELECT duo_session.device_fingerprint
                FROM duo_session
                WHERE
                    duo_session.person_id = person.id
                AND COALESCE(duo_session.device_fingerprint, '') <> ''
                ORDER BY duo_session.session_expiry DESC, duo_session.otp_expiry DESC
                LIMIT 1
            ) AS device_fingerprint,
            (
                SELECT COUNT(DISTINCT duo_session.person_id)
                FROM duo_session
                WHERE
                    duo_session.person_id IS NOT NULL
                AND duo_session.device_fingerprint = (
                    SELECT duo_session.device_fingerprint
                    FROM duo_session
                    WHERE
                        duo_session.person_id = person.id
                    AND COALESCE(duo_session.device_fingerprint, '') <> ''
                    ORDER BY duo_session.session_expiry DESC, duo_session.otp_expiry DESC
                    LIMIT 1
                )
            ) AS accounts_per_device,
            (
                SELECT COUNT(DISTINCT duo_session.person_id)
                FROM duo_session
                WHERE
                    duo_session.person_id IS NOT NULL
                AND duo_session.ip_address IN (
                    SELECT DISTINCT ip_address
                    FROM duo_session
                    WHERE person_id = person.id
                )
                AND duo_session.session_expiry > NOW() - INTERVAL '30 days'
            ) AS accounts_per_ip_30d,
            (
                SELECT COUNT(*)
                FROM skipped
                WHERE
                    skipped.object_person_id = person.id
                AND skipped.reported
                AND skipped.report_reason <> ''
                AND skipped.created_at > NOW() - INTERVAL '14 days'
            ) AS recent_reports
        FROM person
        WHERE person.id = %(person_id)s
        """,
        {'person_id': person_id},
    ).fetchone()

    if not row:
        return

    if row['device_fingerprint'] and int(row['accounts_per_device'] or 0) > max_accounts_per_device:
        _create_or_touch_antiabuse_flag(
            tx,
            person_id=person_id,
            category='multi-account-device',
            severity='high',
            reason='Multiple accounts are using the same device fingerprint.',
            evidence={
                'device_fingerprint': row['device_fingerprint'],
                'accounts_per_device': int(row['accounts_per_device'] or 0),
                'threshold': max_accounts_per_device,
            },
        )

    if int(row['accounts_per_ip_30d'] or 0) > max_accounts_per_ip:
        _create_or_touch_antiabuse_flag(
            tx,
            person_id=person_id,
            category='multi-account-ip',
            severity='medium',
            reason='Too many accounts have recently used the same IP range.',
            evidence={
                'accounts_per_ip_30d': int(row['accounts_per_ip_30d'] or 0),
                'threshold': max_accounts_per_ip,
            },
        )

    if int(row['recent_reports'] or 0) >= 2:
        _create_or_touch_antiabuse_flag(
            tx,
            person_id=person_id,
            category='recent-reports',
            severity='high',
            reason='This account has received multiple recent reports.',
            evidence={
                'recent_reports': int(row['recent_reports'] or 0),
            },
        )

    recent_reports = int(row['recent_reports'] or 0)

    if recent_reports >= warning_threshold:
        _create_or_touch_antiabuse_flag(
            tx,
            person_id=person_id,
            category='reports-warning',
            severity='low',
            reason='This account crossed the warning threshold for recent reports.',
            evidence={
                'recent_reports': recent_reports,
                'threshold': warning_threshold,
                'outcome': 'warning-review',
            },
        )

    if recent_reports >= temp_threshold:
        _create_or_touch_antiabuse_flag(
            tx,
            person_id=person_id,
            category='reports-temporary-review',
            severity='medium',
            reason='This account reached the temporary restriction review threshold.',
            evidence={
                'recent_reports': recent_reports,
                'threshold': temp_threshold,
                'outcome': 'discovery-limited-until-review',
            },
        )

    if recent_reports >= perm_threshold:
        _create_or_touch_antiabuse_flag(
            tx,
            person_id=person_id,
            category='reports-permaban-review',
            severity='high',
            reason='This account reached the permanent-ban review threshold.',
            evidence={
                'recent_reports': recent_reports,
                'threshold': perm_threshold,
                'outcome': 'high-priority-ban-review',
            },
        )

    _detect_reporting_coalition(tx, target_person_id=person_id)

    owner_row = tx.execute(
        """
        SELECT referral_code.person_id AS owner_person_id
        FROM person
        JOIN referral_code ON referral_code.id = person.referred_by_code_id
        WHERE person.id = %(person_id)s
        LIMIT 1
        """,
        {'person_id': person_id},
    ).fetchone()

    if owner_row and owner_row['owner_person_id'] is not None:
        _detect_referral_ring_for_owner(tx, owner_person_id=int(owner_row['owner_person_id']))

    _detect_referral_ring_for_owner(tx, owner_person_id=person_id)


def _initialize_new_user_controls(tx, person_id: int) -> None:
    invite_unlock_days = _get_admin_setting_int(tx, 'system_invite_unlock_days', 7)
    waitlist_mode = _get_admin_setting_bool(tx, 'system_waitlist_mode', False)

    tx.execute(
        """
        UPDATE person
        SET
            invite_unlocked_at = COALESCE(invite_unlocked_at, NOW() + (%(invite_unlock_days)s || ' days')::interval),
            waitlist_status = CASE
                WHEN %(waitlist_mode)s THEN 'pending'
                ELSE waitlist_status
            END
        WHERE id = %(person_id)s
        """,
        {
            'person_id': person_id,
            'invite_unlock_days': invite_unlock_days,
            'waitlist_mode': waitlist_mode,
        },
    )


def _reporting_guard(
    tx,
    reporter_person_id: int,
    target_person_id: int | None = None,
) -> str | None:
    max_reports_per_day = _get_admin_setting_int(tx, 'system_max_reports_per_day', 8)
    same_target_limit = _get_admin_setting_int(tx, 'system_same_target_reports_limit', 1)
    same_target_window_days = _get_admin_setting_int(tx, 'system_same_target_report_window_days', 30)
    row = tx.execute(
        """
        SELECT COUNT(DISTINCT object_person_id) AS count
        FROM skipped
        WHERE
            subject_person_id = %(reporter_person_id)s
        AND reported
        AND report_reason <> ''
        AND created_at > NOW() - INTERVAL '1 day'
        """,
        {'reporter_person_id': reporter_person_id},
    ).fetchone()

    report_count = int(row['count'] or 0) if row else 0
    if report_count < max_reports_per_day:
        pass
    else:
        _create_or_touch_antiabuse_flag(
            tx,
            person_id=reporter_person_id,
            category='mass-reporting',
            severity='high',
            reason='This account is submitting reports unusually quickly.',
            evidence={
                'reported_accounts_last_day': report_count,
                'threshold': max_reports_per_day,
            },
        )

        return 'Too many reports were submitted from this account today. Please wait for admin review.'

    if target_person_id is not None:
        same_target_row = tx.execute(
            """
            SELECT COUNT(*) AS count
            FROM skipped
            WHERE
                subject_person_id = %(reporter_person_id)s
            AND object_person_id = %(target_person_id)s
            AND reported
            AND report_reason <> ''
            AND created_at > NOW() - (%(same_target_window_days)s || ' days')::interval
            """,
            {
                'reporter_person_id': reporter_person_id,
                'target_person_id': target_person_id,
                'same_target_window_days': same_target_window_days,
            },
        ).fetchone()

        same_target_count = int(same_target_row['count'] or 0) if same_target_row else 0
        if same_target_count >= same_target_limit:
            _create_or_touch_antiabuse_flag(
                tx,
                person_id=reporter_person_id,
                category='repeat-target-reporting',
                severity='medium',
                reason='This account keeps reporting the same person repeatedly.',
                evidence={
                    'target_person_id': target_person_id,
                    'reports_against_same_target': same_target_count,
                    'threshold': same_target_limit,
                    'window_days': same_target_window_days,
                },
            )
            return 'You already reported this account recently. Admin review is already in progress.'

    return None


def _count_recent_reports_for_category(
    tx,
    *,
    target_person_id: int,
    category_id: str,
    window_days: int = 30,
) -> int:
    row = tx.execute(
        """
        SELECT COUNT(*) AS count
        FROM skipped
        WHERE
            object_person_id = %(target_person_id)s
        AND reported
        AND report_reason ILIKE %(category_prefix)s
        AND created_at > NOW() - (%(window_days)s || ' days')::interval
        """,
        {
            'target_person_id': target_person_id,
            'category_prefix': f'[CategoryId] {category_id}%',
            'window_days': window_days,
        },
    ).fetchone()

    return int(row['count'] or 0) if row else 0


def _apply_structured_report_flags(
    tx,
    *,
    target_person_id: int,
    reporter_person_id: int,
    report_reason: str,
    report_category: str | None,
    report_context: str | None,
    report_photo_uuid: str | None,
    report_photo_url: str | None,
) -> None:
    normalized_reason = ' '.join((report_reason or '').lower().split())
    category_id = (report_category or '').strip().lower()
    category_report_count = (
        _count_recent_reports_for_category(
            tx,
            target_person_id=target_person_id,
            category_id=category_id,
        )
        if category_id else 0
    )

    shared_evidence = {
        'reporter_person_id': reporter_person_id,
        'report_category': category_id or 'unknown',
        'report_context': report_context or '',
        'recent_category_report_count': category_report_count,
        'report_summary': report_reason[:500],
        'photo_uuid': report_photo_uuid,
        'photo_url': report_photo_url,
    }

    if category_id == 'misrepresentation' or any(
        token in normalized_reason for token in (
            'catfish',
            'fake photo',
            'stolen photo',
            'pretending',
            'not single',
            'lied about',
            'married',
        )
    ):
        _create_or_touch_antiabuse_flag(
            tx,
            person_id=target_person_id,
            category='catfish-risk',
            severity='high' if category_report_count >= 2 else 'medium',
            reason='This account was reported for identity or profile misrepresentation.',
            evidence=shared_evidence,
        )

    if category_id == 'money-or-spam' or any(
        token in normalized_reason for token in (
            'send money',
            'cash app',
            'cashapp',
            'gift card',
            'wire transfer',
            'western union',
            'bitcoin',
            'crypto',
            'loan',
            'investment',
            'paypal me',
        )
    ):
        _create_or_touch_antiabuse_flag(
            tx,
            person_id=target_person_id,
            category='romance-scam-risk',
            severity='high',
            reason='This account was reported for money requests, solicitation, or scam behavior.',
            evidence=shared_evidence,
        )

    if category_id == 'safety' or any(
        token in normalized_reason for token in (
            'threat',
            'blackmail',
            'coerc',
            'stalk',
            'unsafe',
            'doxx',
            'extort',
        )
    ):
        _create_or_touch_antiabuse_flag(
            tx,
            person_id=target_person_id,
            category='safety-risk',
            severity='high',
            reason='This account was reported for a direct safety concern.',
            evidence=shared_evidence,
        )

    if category_id == 'harassment' or any(
        token in normalized_reason for token in (
            'harass',
            'abusive',
            'insult',
            'spam me',
            'keeps messaging',
        )
    ):
        _create_or_touch_antiabuse_flag(
            tx,
            person_id=target_person_id,
            category='harassment-pattern',
            severity='high' if category_report_count >= 2 else 'medium',
            reason='This account was reported for harassment or repeated unwanted contact.',
            evidence=shared_evidence,
        )


def _get_referral_trust_summary(tx, owner_person_id: int) -> dict[str, int | bool]:
    row = tx.execute(
        """
        WITH target_codes AS (
            SELECT id
            FROM referral_code
            WHERE person_id = %(owner_person_id)s
        ), referred_people AS (
            SELECT
                person.id,
                person.activated,
                person.verification_level_id,
                person.verification_required,
                EXISTS (
                    SELECT 1
                    FROM antiabuse_flag
                    WHERE
                        antiabuse_flag.person_id = person.id
                    AND antiabuse_flag.status = 'resolved'
                    AND antiabuse_flag.resolution IN ('warning', 'temporary_restriction', 'permanent_ban')
                ) AS has_validated_issue,
                EXISTS (
                    SELECT 1
                    FROM banned_person
                    WHERE banned_person.normalized_email = person.normalized_email
                ) AS is_banned
            FROM person
            WHERE referred_by_code_id IN (SELECT id FROM target_codes)
        )
        SELECT
            COUNT(*) AS total_referred,
            COUNT(*) FILTER (WHERE verification_level_id > 1) AS verified_referred,
            COUNT(*) FILTER (WHERE has_validated_issue) AS reported_referred,
            COUNT(*) FILTER (WHERE is_banned) AS banned_referred,
            COUNT(*) FILTER (WHERE activated) AS active_referred,
            COUNT(*) FILTER (
                WHERE verification_required AND COALESCE(verification_level_id, 0) <= 1
            ) AS gated_referred
        FROM referred_people
        """,
        {'owner_person_id': owner_person_id},
    ).fetchone()

    total_referred = int(row['total_referred'] or 0)
    verified_referred = int(row['verified_referred'] or 0)
    reported_referred = int(row['reported_referred'] or 0)
    banned_referred = int(row['banned_referred'] or 0)
    active_referred = int(row['active_referred'] or 0)
    gated_referred = int(row['gated_referred'] or 0)

    referral_trust_score = (
        50
        + verified_referred * 12
        + active_referred * 4
        - reported_referred * 14
        - banned_referred * 30
        - gated_referred * 6
    )
    person_trust = _get_person_trust_summary(tx, owner_person_id)
    trust_score = max(0, min(100, int(round((referral_trust_score + person_trust['trust_score']) / 2))))

    owner_row = tx.execute(
        """
        SELECT
            email,
            roles,
            invite_unlocked_at,
            waitlist_status,
            profile_status
        FROM person
        WHERE id = %(person_id)s
        """,
        {'person_id': owner_person_id},
    ).fetchone()

    low_trust_threshold = _get_admin_setting_int(
        tx,
        'system_referral_low_trust_threshold',
        45,
    )
    block_threshold = _get_admin_setting_int(
        tx,
        'system_referral_block_threshold',
        15,
    )

    invite_limit = REFERRAL_CODE_MAX_USES
    if trust_score < low_trust_threshold:
        invite_limit = 1
    if trust_score < block_threshold:
        invite_limit = 0

    is_admin_bypass = _is_admin_person(tx, owner_person_id)
    can_invite = invite_limit > 0
    pause_reason = None

    if is_admin_bypass:
        can_invite = True
        invite_limit = ADMIN_REFERRAL_CODE_MAX_USES

    if owner_row:
        invite_unlocked_at = owner_row['invite_unlocked_at']
        if not is_admin_bypass:
            if owner_row['waitlist_status'] != 'active':
                can_invite = False
                pause_reason = 'Invite access will unlock after account approval.'
            elif owner_row['profile_status'] != 'active':
                can_invite = False
                pause_reason = 'Invite access is paused while this profile is not active.'
            elif invite_unlocked_at and invite_unlocked_at > datetime.utcnow():
                can_invite = False
                unlock_text = invite_unlocked_at.strftime('%B %d, %Y')
                pause_reason = (
                    f'Invite access is under review right now and will unlock on {unlock_text}.'
                )
            elif invite_limit <= 0:
                pause_reason = 'Invite access is temporarily paused while referral quality is reviewed.'

    if not can_invite and pause_reason is None:
        pause_reason = 'Invite access is temporarily paused while referral quality is reviewed.'

    return {
        'trust_score': max(0, min(100, trust_score)),
        'total_referred': total_referred,
        'verified_referred': verified_referred,
        'reported_referred': reported_referred,
        'banned_referred': banned_referred,
        'active_referred': active_referred,
        'gated_referred': gated_referred,
        'invite_limit': invite_limit,
        'can_invite': can_invite,
        'pause_reason': pause_reason,
        'invite_unlocked_at': owner_row['invite_unlocked_at'] if owner_row else None,
        'is_admin_bypass': is_admin_bypass,
        'person_trust_score': person_trust['trust_score'],
        'person_trust_level': person_trust['level'],
    }


def _append_community_signals(
    tx,
    *,
    profile: dict[str, Any],
    viewer_person_id: int,
    prospect_person_id: int,
) -> None:
    trust_summary = _get_person_trust_summary(tx, prospect_person_id)
    row = tx.execute(
        """
        SELECT
            prospect.verification_level_id > 1 AS is_verified,
            prospect.congregation_id IS NOT NULL AS has_congregation,
            prospect.sign_up_time <= NOW() - INTERVAL '30 days' AS is_established,
            EXISTS (
                SELECT 1
                FROM referral_code
                WHERE referral_code.id = prospect.referred_by_code_id
            ) AS was_referred
        FROM person AS prospect
        WHERE prospect.id = %(prospect_person_id)s
        """,
        {
            'viewer_person_id': viewer_person_id,
            'prospect_person_id': prospect_person_id,
        },
    ).fetchone()

    if not row:
        profile['community_signals'] = []
        return

    signals = []
    if row['is_verified']:
        signals.append('Verified identity')
    if row['was_referred']:
        signals.append('Joined through a referral')
    if row['has_congregation']:
        signals.append('Congregation listed')
    if row['is_established']:
        signals.append('Established member')
    if trust_summary['trust_score'] >= 70:
        signals.append('High trust account')

    profile['trust_score'] = trust_summary['trust_score']
    profile['trust_level'] = trust_summary['level']
    profile['community_signals'] = signals[:5]
    profile['community_milestones'] = (trust_summary.get('milestones') or [])[:4]
    profile['community_stage_label'] = trust_summary.get('stage_label')


def _account_standing_label(*, waitlist_status: str | None, profile_status: str | None) -> str:
    if waitlist_status == 'blocked':
        return 'Review needed'
    if waitlist_status == 'pending':
        return 'In approval queue'
    if profile_status == 'paused':
        return 'Paused by user'
    if profile_status == 'serious':
        return 'Serious and active'
    return 'Active and in good standing'


def _ensure_referral_code(tx, person_id: int) -> dict[str, Any]:
    existing = _get_latest_referral_code(tx, person_id)
    if existing:
        return existing
    return _generate_referral_code(tx, person_id)

@dataclass
class CropSize:
    top: int
    left: int

def process_image_as_image(
    image: Image.Image,
    output_size: Optional[int] = None,
    crop_size: Optional[CropSize] = None,
) -> io.BytesIO:
    # Rotate the image according to EXIF data
    try:
        exif = image.getexif()
        orientation = exif[274] # 274 is the exif code for the orientation tag
    except:
        orientation = None

    if orientation is None:
        pass
    elif orientation == 1:
        # Normal, no changes needed
        pass
    elif orientation == 2:
        # Mirrored horizontally
        pass
    elif orientation == 3:
        # Rotated 180 degrees
        image = image.rotate(180, expand=True)
    elif orientation == 4:
        # Mirrored vertically
        pass
    elif orientation == 5:
        # Transposed
        image = image.rotate(-90, expand=True)
    elif orientation == 6:
        # Rotated -90 degrees
        image = image.rotate(-90, expand=True)
    elif orientation == 7:
        # Transverse
        image = image.rotate(90, expand=True)
    elif orientation == 8:
        # Rotated 90 degrees
        image = image.rotate(90, expand=True)

    # Crop the image to be square
    if output_size is not None:
        # Get the dimensions of the image
        width, height = image.size

        # Find the smaller dimension
        min_dim = min(width, height)

        # Compute the area to crop
        if crop_size is None:
            left = (width - min_dim) // 2
            top = (height - min_dim) // 2
            right = (width + min_dim) // 2
            bottom = (height + min_dim) // 2
        else:
            # Ensure the top left point is within range
            crop_size.top  = max(0, crop_size.top)
            crop_size.left = max(0, crop_size.left)

            crop_size.top  = min(height - min_dim, crop_size.top)
            crop_size.left = min(width  - min_dim, crop_size.left)

            # Compute the area to crop
            left = crop_size.left
            top = crop_size.top
            right = crop_size.left + min_dim
            bottom = crop_size.top + min_dim

        # Crop the image to be square
        crop_box = (left, top, right, bottom)
        image = image.crop(crop_box)

    # Scale the image to the desired size
    if output_size is not None and output_size != min_dim:
        image = image.resize((output_size, output_size))

    return image.convert('RGB')

def process_image_as_bytes(
    base64_file: t.Base64File,
    format: Literal['raw', 'jpeg'],
    output_size: Optional[int] = None,
    crop_size: Optional[CropSize] = None,
) -> io.BytesIO:
    if format == 'raw':
        return io.BytesIO(base64_file.bytes)

    output_bytes = io.BytesIO()

    image = process_image_as_image(base64_file.image, output_size, crop_size)

    image.save(
        output_bytes,
        format=format,
        quality=85,
        subsampling=2,
        progressive=True,
        optimize=True,
    )

    output_bytes.seek(0)

    return output_bytes

def compute_blurhash(image: Image.Image, crop_size: Optional[CropSize] = None):
    image = process_image_as_image(image, output_size=32, crop_size=crop_size)

    return blurhash.encode(numpy.array(image.convert("RGB")))

def put_image_in_object_store(
    uuid: str,
    base64_file: t.Base64File,
    crop_size: CropSize,
    sizes: list[Literal[None, 900, 450]] = [None, 900, 450],
):
    key_img = [
        (
            f'{size if size else "original"}-{uuid}.jpg',
            process_image_as_bytes(
                base64_file=base64_file,
                format='jpeg',
                output_size=size,
                crop_size=None if size is None else crop_size
            )
        )
        for size in sizes
    ]

    if base64_file.image.format == 'GIF' and None in sizes:
        key_img.append((
            f'{uuid}.gif',
            process_image_as_bytes(base64_file=base64_file, format='raw')
        ))

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(bucket.put_object, Key=key, Body=img)
            for key, img in key_img}

        for future in as_completed(futures):
            future.result()

def _has_gold(person_id: int) -> bool:
    # Gold is now universally unlocked for all users.
    # This effectively removes premium gating for name/show_my_location/show_my_age/hide_me_from_strangers/browse_invisibly/theme.
    return True


def post_answer(req: t.PostAnswer, s: t.SessionInfo):
    params_add_yes_no_count = dict(
        question_id=req.question_id,
        add_yes=1 if req.answer is True else 0,
        add_no=1 if req.answer is False else 0,
    )

    params_update_answer = dict(
        person_id=s.person_id,
        question_id_to_delete=None,
        question_id_to_insert=req.question_id,
        answer=req.answer,
        public=req.public,
    )

    with api_tx('READ COMMITTED') as tx:
        tx.execute(Q_ADD_YES_NO_COUNT, params_add_yes_no_count)

    with api_tx() as tx:
        tx.execute(Q_UPDATE_ANSWER, params_update_answer)
        return _get_access_gate_state(tx, s.person_id)

def delete_answer(req: t.DeleteAnswer, s: t.SessionInfo):
    params = dict(
        person_id=s.person_id,
        question_id_to_delete=req.question_id,
        question_id_to_insert=None,
        answer=None,
        public=None,
    )

    with api_tx() as tx:
        tx.execute(Q_UPDATE_ANSWER, params)
        return _get_access_gate_state(tx, s.person_id)

def _send_otp(email: str, otp: str):
    if email.endswith('@example.com'):
        return

    aws_smtp.send(
        subject="Sign in to JwBoo",
        body=otp_template(otp),
        to_addr=email,
        from_addr=os.environ.get('DUO_REPORT_EMAIL') or 'noreply-otp@duolicious.app',
    )

def post_request_otp(req: t.PostRequestOtp):
    if not request.remote_addr or firehol.matches(request.remote_addr):
        return 'IP address blocked', 460

    if not check_and_update_bad_domains(req.email):
        return 'Disposable email', 400

    session_token = secrets.token_hex(64)
    session_token_hash = sha512(session_token)

    params = dict(
        email=req.email,
        normalized_email=normalize_email(req.email),
        referral_code_id=None,
        is_dev=DUO_ENV == 'dev',
        session_token_hash=session_token_hash,
        ip_address=request.remote_addr,
        device_fingerprint=_get_device_fingerprint(),
    )

    with api_tx() as tx:
        quiz_attempt = None
        existing_person = tx.execute(
            """
            SELECT id
            FROM person
            WHERE normalized_email = %(normalized_email)s
            LIMIT 1
            """,
            {'normalized_email': params['normalized_email']},
        ).fetchone()

        if req.sign_in_only and not existing_person:
            return 'No account exists for this email yet. Use the referral flow to create one.', 404

        if not existing_person:
            if not req.referral_code:
                return 'A referral code is required to create an account.', 400

            referral_code_id, error = _validate_referral_code_for_signup(tx, req.referral_code)
            if error:
                return error, 400
            params['referral_code_id'] = referral_code_id

            cooldown_until = _get_jw_quiz_cooldown_until(tx, params['normalized_email'])
            if cooldown_until and cooldown_until > datetime.utcnow():
                return 'Access is cooling down for this email right now. Please try the JW check again later.', 429

            if not req.jw_quiz_token:
                return 'Complete the Jehovah’s Witness check before continuing.', 400

            quiz_attempt = tx.execute(
                """
                SELECT id
                FROM jw_quiz_attempt
                WHERE
                    challenge_token_hash = %(challenge_token_hash)s
                AND
                    normalized_email = %(normalized_email)s
                AND
                    referral_code = %(referral_code)s
                AND
                    passed_at IS NOT NULL
                AND
                    is_consumed = FALSE
                ORDER BY passed_at DESC
                LIMIT 1
                """,
                {
                    'challenge_token_hash': sha512(req.jw_quiz_token),
                    'normalized_email': params['normalized_email'],
                    'referral_code': req.referral_code,
                },
            ).fetchone()

            if not quiz_attempt:
                return 'Complete the Jehovah’s Witness check before continuing.', 400

        rows = tx.execute(Q_INSERT_DUO_SESSION, params).fetchall()

        if quiz_attempt:
            tx.execute(
                """
                UPDATE jw_quiz_attempt
                SET is_consumed = TRUE
                WHERE id = %(id)s
                """,
                {'id': quiz_attempt['id']},
            )

    try:
        row, *_ = rows
        otp = row['otp']
    except:
        return 'Banned', 461

    try:
        _send_otp(req.email, otp)
    except Exception:
        print(traceback.format_exc())
        return 'We could not send your code right now. Please try again in a moment.', 503

    return dict(session_token=session_token)


def post_validate_referral_code(req: t.PostValidateReferralCode):
    with api_tx() as tx:
        referral_code_id, error = _validate_referral_code_for_signup(
            tx,
            req.referral_code,
        )
        referral_row = _get_referral_code(tx, req.referral_code) if not error else None

    if error:
        return {'message': error}, 400

    return {
        'valid': True,
        'referral_code_id': referral_code_id,
        'referral_code': req.referral_code,
        'referrer_name': referral_row['owner_name'] if referral_row else None,
        'referrer_email': referral_row['owner_email'] if referral_row else None,
    }

    return dict(valid=True)


def post_start_jw_quiz(req: t.PostStartJwQuiz):
    normalized_email = normalize_email(req.email)

    with api_tx() as tx:
        existing_person = tx.execute(
            """
            SELECT id
            FROM person
            WHERE normalized_email = %(normalized_email)s
            LIMIT 1
            """,
            {'normalized_email': normalized_email},
        ).fetchone()

        if existing_person:
            return {'required': False, 'already_registered': True}

        referral_code_id, error = _validate_referral_code_for_signup(tx, req.referral_code)
        if error:
            return error, 400

        cooldown_until = _get_jw_quiz_cooldown_until(tx, normalized_email)
        if cooldown_until and cooldown_until > datetime.utcnow():
            return f'Please wait until {cooldown_until.strftime("%B %d, %Y at %I:%M %p UTC")} before trying the JW check again.', 429

        question_payload = _build_jw_quiz_payload(tx, _get_jw_quiz_question_count(tx))
        if not question_payload:
            return {'required': False, 'referral_code_id': referral_code_id}

        question_payload_for_client = [
            {
                'question_id': item['question_id'],
                'prompt': item['prompt'],
                'options': item['options'],
            }
            for item in question_payload
        ]
        expected_answers = {
            str(item['question_id']): item['correct_option']
            for item in question_payload
        }
        jw_quiz_token = secrets.token_urlsafe(24)
        time_limit_seconds = _get_jw_quiz_time_limit_seconds(tx)
        expires_at = datetime.utcnow() + timedelta(seconds=time_limit_seconds)

        tx.execute(
            """
            INSERT INTO jw_quiz_attempt (
                normalized_email,
                referral_code,
                challenge_token_hash,
                question_payload,
                expected_answers,
                time_limit_seconds,
                expires_at
            ) VALUES (
                %(normalized_email)s,
                %(referral_code)s,
                %(challenge_token_hash)s,
                %(question_payload)s::jsonb,
                %(expected_answers)s::jsonb,
                %(time_limit_seconds)s,
                %(expires_at)s
            )
            """,
            {
                'normalized_email': normalized_email,
                'referral_code': req.referral_code,
                'challenge_token_hash': sha512(jw_quiz_token),
                'question_payload': json.dumps(question_payload_for_client),
                'expected_answers': json.dumps(expected_answers),
                'time_limit_seconds': time_limit_seconds,
                'expires_at': expires_at,
            },
        )

    return {
        'required': True,
        'jw_quiz_token': jw_quiz_token,
        'questions': question_payload_for_client,
        'time_limit_seconds': time_limit_seconds,
        'expires_at': expires_at.isoformat(),
    }


def post_complete_jw_quiz(req: t.PostCompleteJwQuiz):
    with api_tx() as tx:
        row = tx.execute(
            """
            SELECT
                id,
                expected_answers,
                expires_at
            FROM jw_quiz_attempt
            WHERE challenge_token_hash = %(challenge_token_hash)s
            LIMIT 1
            """,
            {'challenge_token_hash': sha512(req.jw_quiz_token)},
        ).fetchone()

        if not row:
            return 'That JW check could not be found. Please start again.', 400

        now = datetime.utcnow()
        if row['expires_at'] and row['expires_at'] < now:
            cooldown_until = now + timedelta(days=_get_jw_quiz_cooldown_days(tx))
            tx.execute(
                """
                UPDATE jw_quiz_attempt
                SET
                    completed_at = NOW(),
                    score = 0,
                    total_questions = jsonb_object_length(expected_answers),
                    cooldown_until = %(cooldown_until)s
                WHERE id = %(id)s
                """,
                {
                    'id': row['id'],
                    'cooldown_until': cooldown_until,
                },
            )
            return f'Time ran out. Please wait until {cooldown_until.strftime("%B %d, %Y at %I:%M %p UTC")} before trying again.', 429

        expected_answers = row['expected_answers'] or {}
        submitted_answers = {
            str(item.question_id): item.selected_option
            for item in req.answers
        }
        total_questions = len(expected_answers.keys())
        score = sum(
            1
            for question_id, correct_option in expected_answers.items()
            if submitted_answers.get(question_id) == correct_option
        )
        passed = total_questions > 0 and score == total_questions
        cooldown_until = None if passed else now + timedelta(days=_get_jw_quiz_cooldown_days(tx))

        tx.execute(
            """
            UPDATE jw_quiz_attempt
            SET
                completed_at = NOW(),
                passed_at = CASE WHEN %(passed)s THEN NOW() ELSE NULL END,
                score = %(score)s,
                total_questions = %(total_questions)s,
                cooldown_until = %(cooldown_until)s
            WHERE id = %(id)s
            """,
            {
                'id': row['id'],
                'passed': passed,
                'score': score,
                'total_questions': total_questions,
                'cooldown_until': cooldown_until,
            },
        )

    if not passed:
        return f'That JW check did not pass. Please wait until {cooldown_until.strftime("%B %d, %Y at %I:%M %p UTC")} before trying again.', 429

    return {
        'passed': True,
        'score': score,
        'total_questions': total_questions,
        'jw_quiz_token': req.jw_quiz_token,
        'message': 'JW check passed. You can continue now.',
    }


def post_resend_otp(s: t.SessionInfo):
    if not request.remote_addr or firehol.matches(request.remote_addr):
        return 'IP address blocked', 460

    params = dict(
        email=s.email,
        normalized_email=normalize_email(s.email),
        is_dev=DUO_ENV == 'dev',
        session_token_hash=s.session_token_hash,
        ip_address=request.remote_addr,
    )

    with api_tx() as tx:
        rows = tx.execute(Q_UPDATE_OTP, params).fetchall()

    try:
        row, *_ = rows
        otp = row['otp']
    except:
        return 'Banned', 461

    try:
        _send_otp(s.email, otp)
    except Exception:
        print(traceback.format_exc())
        return 'We could not send your code right now. Please try again in a moment.', 503

def post_check_otp(req: t.PostCheckOtp, s: t.SessionInfo):
    if not request.remote_addr or firehol.matches(request.remote_addr):
        return 'IP address blocked', 460

    params = dict(
        otp=req.otp,
        session_token_hash=s.session_token_hash,
    )

    with api_tx() as tx:
        tx.execute(Q_MAYBE_DELETE_ONBOARDEE, params)
        tx.execute(Q_MAYBE_SIGN_IN, params)
        row = tx.fetchone()

        if not row:
            return 'Invalid OTP', 401

        tx.execute(Q_UPDATE_LAST, dict(person_uuid=row['person_uuid']))
        access_gate = _get_access_gate_state(tx, row['person_id'])

    return dict(
        onboarded=row['person_id'] is not None,
        **row,
        **access_gate,
    )

def post_sign_out(s: t.SessionInfo):
    params = dict(session_token_hash=s.session_token_hash)

    with api_tx('READ COMMITTED') as tx:
        tx.execute(Q_DELETE_DUO_SESSION, params)

def post_check_session_token(s: t.SessionInfo):
    params = dict(
        person_id=s.person_id,
    )

    with api_tx() as tx:
        row = tx.execute(Q_CHECK_SESSION_TOKEN, params).fetchone()

        if not row:
            return 'Invalid token', 401

        if s.person_id:
            _refresh_antiabuse_flags(tx, person_id=s.person_id)
            trust_summary = _get_person_trust_summary(tx, s.person_id)
            access_gate = _get_access_gate_state(tx, s.person_id)
        else:
            trust_summary = {'trust_score': 0, 'level': 'warning'}
            access_gate = {
                'has_full_access': False,
                'access_gate': None,
                'required_q_and_a_count': 0,
                'answered_q_and_a_count': 0,
                'remaining_required_q_and_a_count': 0,
            }

        return dict(
            person_id=s.person_id,
            person_uuid=s.person_uuid,
            onboarded=s.onboarded,
            **row,
            **access_gate,
            trust_score=trust_summary['trust_score'],
            trust_level=trust_summary['level'],
        )

def patch_onboardee_info(req: t.PatchOnboardeeInfo, s: t.SessionInfo):
    [field_name] = req.__pydantic_fields_set__
    field_value = req.dict()[field_name]

    if field_name == 'referral_code':
        with api_tx() as tx:
            referral_code_id, error = _validate_referral_code_for_signup(tx, field_value)
            if error:
                return error, 400

            tx.execute(
                """
                UPDATE duo_session
                SET referral_code_id = %(referral_code_id)s
                WHERE session_token_hash = %(session_token_hash)s
                """,
                {
                    'referral_code_id': referral_code_id,
                    'session_token_hash': s.session_token_hash,
                },
            )

            if tx.rowcount != 1:
                return 'Unable to save referral code', 400
    elif field_name in ['name', 'date_of_birth', 'baptism_date']:
        params = dict(
            email=s.email,
            field_value=field_value
        )

        q_set_onboardee_field = """
            INSERT INTO onboardee (
                email,
                $field_name
            ) VALUES (
                %(email)s,
                %(field_value)s
            ) ON CONFLICT (email) DO UPDATE SET
                $field_name = EXCLUDED.$field_name
            """.replace('$field_name', field_name)

        with api_tx() as tx:
            snapshot = _get_onboardee_snapshot(tx, s.email)
            error = _validate_onboarding_snapshot(
                gender=snapshot.get('gender'),
                date_of_birth=(
                    field_value if field_name == 'date_of_birth'
                    else snapshot.get('date_of_birth')
                ),
                baptism_date=(
                    field_value if field_name == 'baptism_date'
                    else snapshot.get('baptism_date')
                ),
            )
            if error:
                return error, 400

            tx.execute(q_set_onboardee_field, params)
    elif field_name == 'location':
        params = dict(
            email=s.email,
            long_friendly=field_value
        )

        q_set_onboardee_field = """
            INSERT INTO onboardee (
                email,
                coordinates,
                congregation_id
            ) SELECT
                %(email)s,
                coordinates,
                NULL
            FROM location
            WHERE long_friendly = %(long_friendly)s
            ON CONFLICT (email) DO UPDATE SET
                coordinates = EXCLUDED.coordinates,
                congregation_id = EXCLUDED.congregation_id
            """
        with api_tx() as tx:
            tx.execute(q_set_onboardee_field, params)
            if tx.rowcount != 1:
                return 'Unknown location', 400
    elif field_name == 'congregation_id':
        with api_tx() as tx:
            row = tx.execute(
                """
                SELECT congregation.id
                FROM onboardee
                JOIN congregation_search_cache
                  ON congregation_search_cache.location_long_friendly = (
                      SELECT long_friendly
                      FROM location
                      ORDER BY location.coordinates <-> onboardee.coordinates
                      LIMIT 1
                  )
                JOIN congregation
                  ON congregation.id = congregation_search_cache.congregation_id
                WHERE onboardee.email = %(email)s
                  AND congregation.id = %(congregation_id)s
                """,
                {
                    'email': s.email,
                    'congregation_id': field_value,
                },
            ).fetchone()

            if not row:
                row = tx.execute(
                    """
                    SELECT id
                    FROM congregation
                    WHERE id = %(congregation_id)s
                    """,
                    {'congregation_id': field_value},
                ).fetchone()

            if not row:
                return 'Unknown congregation', 400

            tx.execute(
                """
                INSERT INTO onboardee (
                    email,
                    congregation_id
                ) VALUES (
                    %(email)s,
                    %(congregation_id)s
                ) ON CONFLICT (email) DO UPDATE SET
                    congregation_id = EXCLUDED.congregation_id
                """,
                {
                    'email': s.email,
                    'congregation_id': field_value,
                },
            )
    elif field_name == 'gender':
        opposite_gender = _opposite_gender(field_value)
        if not opposite_gender:
            return 'Only Man or Woman are allowed', 400

        params = dict(
            email=s.email,
            gender=field_value
        )

        q_set_onboardee_field = """
            INSERT INTO onboardee (
                email,
                gender_id
            ) SELECT
                %(email)s,
                id
            FROM gender
            WHERE name = %(gender)s
            ON CONFLICT (email) DO UPDATE SET
                gender_id = EXCLUDED.gender_id
            """
        with api_tx() as tx:
            snapshot = _get_onboardee_snapshot(tx, s.email)
            error = _validate_onboarding_snapshot(
                gender=field_value,
                date_of_birth=snapshot.get('date_of_birth'),
                baptism_date=snapshot.get('baptism_date'),
            )
            if error:
                return error, 400

            tx.execute(q_set_onboardee_field, params)
            tx.execute(
                """
                DELETE FROM onboardee_search_preference_gender
                WHERE email = %(email)s
                """,
                {'email': s.email},
            )
            tx.execute(
                """
                INSERT INTO onboardee_search_preference_gender (email, gender_id)
                SELECT %(email)s, id
                FROM gender
                WHERE name = %(gender)s
                """,
                {'email': s.email, 'gender': opposite_gender},
            )

    elif field_name == 'other_peoples_genders':
        with api_tx() as tx:
            snapshot = _get_onboardee_snapshot(tx, s.email)
            target_gender = _opposite_gender(snapshot.get('gender'))

            if not target_gender:
                return 'Select your gender first', 400

            tx.execute(
                """
                DELETE FROM onboardee_search_preference_gender
                WHERE email = %(email)s
                """,
                {'email': s.email},
            )
            tx.execute(
                """
                INSERT INTO onboardee_search_preference_gender (email, gender_id)
                SELECT %(email)s, id
                FROM gender
                WHERE name = %(gender)s
                ON CONFLICT (email, gender_id) DO NOTHING
                """,
                {'email': s.email, 'gender': target_gender},
            )
    elif field_name == 'base64_file':
        base64_file = t.Base64File(**field_value)

        crop_size = CropSize(
                top=base64_file.top,
                left=base64_file.left)
        uuid = secrets.token_hex(32)
        blurhash_ = compute_blurhash(base64_file.image, crop_size=crop_size)
        extra_exts = ['gif'] if base64_file.image.format == 'GIF' else []

        params = dict(
            email=s.email,
            position=base64_file.position,
            uuid=uuid,
            blurhash=blurhash_,
            extra_exts=extra_exts,
            hash=base64_file.md5_hash,
        )

        # Create new onboardee photos. Because we:
        #   1. Create DB entries; then
        #   2. Create photos,
        # the DB might refer to DB entries that don't exist. The front end needs
        # to handle that possibility. Doing it like this makes later deletion
        # from the object store easier, which is important because storing
        # objects is expensive.
        q_set_onboardee_field = """
            WITH existing_uuid AS (
                SELECT
                    uuid
                FROM
                    onboardee_photo
                WHERE
                    email = %(email)s
                AND
                    position = %(position)s
            ), undeleted_photo_insertion AS (
                INSERT INTO undeleted_photo (
                    uuid
                )
                SELECT
                    uuid
                FROM
                    existing_uuid
            ), onboardee_photo_insertion AS (
                INSERT INTO onboardee_photo (
                    email,
                    position,
                    uuid,
                    blurhash,
                    extra_exts,
                    hash
                ) VALUES (
                    %(email)s,
                    %(position)s,
                    %(uuid)s,
                    %(blurhash)s,
                    %(extra_exts)s,
                    %(hash)s
                ) ON CONFLICT (email, position) DO UPDATE SET
                    uuid = EXCLUDED.uuid,
                    blurhash = EXCLUDED.blurhash,
                    extra_exts = EXCLUDED.extra_exts
            )
            SELECT 1
            """

        with api_tx() as tx:
            tx.execute(q_set_onboardee_field, params)

        try:
            put_image_in_object_store(uuid, base64_file, crop_size)
        except Exception as e:
            print('Upload failed with exception:', e)
            return '', 500

    else:
        return f'Invalid field name {field_name}', 400

def delete_onboardee_info(req: t.DeleteOnboardeeInfo, s: t.SessionInfo):
    params = [
        dict(email=s.email, position=position)
        for position in req.files
    ]

    with api_tx() as tx:
        tx.executemany(Q_DELETE_ONBOARDEE_PHOTO, params)

def post_finish_onboarding(s: t.SessionInfo):
    api_params = dict(
        email=s.email,
        normalized_email=normalize_email(s.email),
        session_token_hash=s.session_token_hash,
    )

    with api_tx() as tx:
        referral_row = tx.execute(
            """
            SELECT referral_code_id
            FROM duo_session
            WHERE session_token_hash = %(session_token_hash)s
            """,
            {'session_token_hash': s.session_token_hash},
        ).fetchone()

        if not referral_row:
            return 'Session expired. Request a new code and try again.', 400

        if referral_row['referral_code_id'] is None:
            return 'Referral code is required', 400

        snapshot = _get_onboardee_snapshot(tx, s.email)
        error = _validate_onboarding_snapshot(
            gender=snapshot.get('gender'),
            date_of_birth=snapshot.get('date_of_birth'),
            baptism_date=snapshot.get('baptism_date'),
        )
        if error:
            return error, 400

        if snapshot.get('gender') not in MIN_AGE_BY_GENDER:
            return 'Select Man or Woman before continuing', 400

        if not snapshot.get('date_of_birth'):
            return 'Birth date is required', 400

        if not snapshot.get('baptism_date'):
            return 'Baptism date is required', 400

        if not snapshot.get('congregation_id'):
            return 'Congregation is required', 400

        tx.execute('SET LOCAL statement_timeout = 15000') # 15 seconds
        tx.execute(Q_FINISH_ONBOARDING, params=api_params)
        row = tx.fetchone()

        if not row:
            return 'Unable to finish onboarding', 400

        if referral_row['referral_code_id'] is not None:
            usage_count = _count_referral_uses(tx, referral_row['referral_code_id'])
            trust_summary = _get_referral_trust_summary(tx, row['person_id'])
            if (
                not trust_summary.get('is_admin_bypass')
                and usage_count >= REFERRAL_CODE_MAX_USES
            ):
                _replace_referral_code(tx, referral_row['referral_code_id'])

        _initialize_new_user_controls(tx, row['person_id'])
        _refresh_antiabuse_flags(tx, person_id=row['person_id'])
        _ensure_referral_code(tx, row['person_id'])
        access_gate = _get_access_gate_state(tx, row['person_id'])

    chat_params = dict(
        person_id=row['person_id'],
        person_uuid=row['person_uuid'],
    )

    return dict(**row, **access_gate)

def get_me(
    person_id_as_int: int | None = None,
    person_id_as_str: str | None = None,
):
    if person_id_as_int is None and person_id_as_str is None:
        raise ValueError('pass an arg, please')

    params = dict(
        person_id_as_int=person_id_as_int,
        person_id_as_str=person_id_as_str,
        prospect_person_id=None,
        topic=None,
    )

    with api_tx('READ COMMITTED') as tx:
        personality = tx.execute(Q_SELECT_PERSONALITY, params).fetchall()

    try:
        return {
            'name': personality[0]['person_name'],
            'person_id': personality[0]['person_id'],
            'personality': [
                {
                    'trait_name': trait['trait_name'],
                    'trait_min_label': trait['trait_min_label'],
                    'trait_max_label': trait['trait_max_label'],
                    'trait_description': trait['trait_description'],
                    'person_percentage': trait['person_percentage'],
                }
                for trait in personality
            ]
        }
    except:
        return '', 404

def get_prospect_profile(s: t.SessionInfo, prospect_uuid):
    params = dict(
        person_id=s.person_id,
        prospect_uuid=prospect_uuid,
    )

    with api_tx('READ COMMITTED') as tx:
        api_row = tx.execute(Q_SELECT_PROSPECT_PROFILE, params).fetchone()
        if not api_row:
            return '', 404

        profile = api_row.get('j')
        if not profile:
            return '', 404

    # Timeout in case someone with lots of messages hogs CPU time
    try:
        with api_tx('READ COMMITTED') as tx:
            tx.execute('SET LOCAL statement_timeout = 1000') # 1 second

            message_stats = tx.execute(Q_MESSAGE_STATS, params).fetchone()
    except psycopg.errors.QueryCanceled:
        message_stats = dict(
            gets_reply_percentage=None,
            gives_reply_percentage=None,
        )

    profile.update(message_stats)

    with api_tx('READ COMMITTED') as tx:
        _append_community_signals(
            tx=tx,
            viewer_person_id=s.person_id,
            prospect_person_id=profile['person_id'],
            profile=profile,
        )

    profile['trust_summary'] = {
        'trust_score': profile.get('trust_score') or 0,
        'stage_label': profile.get('community_stage_label') or 'Building trust',
        'manually_verified': bool(
            profile.get('verification_level_id', 1) > 1
            or profile.get('verified_age')
            or profile.get('verified_gender')
            or profile.get('verified_ethnicity')
            or any(profile.get('photo_verifications') or [])
        ),
        'congregation_visible': bool(profile.get('congregation_name')),
        'baptism_date_visible': bool(profile.get('baptism_date')),
        'profile_reviewed': profile.get('waitlist_status') == 'active',
        'intro_acceptance_rate': profile.get('gets_reply_percentage'),
        'account_standing': _account_standing_label(
            waitlist_status=profile.get('waitlist_status'),
            profile_status=profile.get('profile_status'),
        ),
    }

    return profile

def post_skip_by_uuid(req: t.PostSkip, s: t.SessionInfo, prospect_uuid: str):
    if not s.person_uuid:
        return 'Authentication required', 401

    target_person_id = None
    report_photo_uuid = None
    report_photo_url = None
    crop_size = None
    if req.report_reason:
        with api_tx() as tx:
            target_row = tx.execute(
                """
                SELECT id
                FROM person
                WHERE uuid = %(prospect_uuid)s
                LIMIT 1
                """,
                {'prospect_uuid': prospect_uuid},
            ).fetchone()

            target_person_id = int(target_row['id']) if target_row else None

            error = _reporting_guard(
                tx,
                s.person_id,
                target_person_id=target_person_id,
            )
            if error:
                return error, 429

        if req.base64_file is not None:
            report_photo_uuid = secrets.token_hex(32)
            crop_size = CropSize(
                top=req.base64_file.top,
                left=req.base64_file.left,
            )
            report_photo_url = f'https://user-images.duolicious.app/450-{report_photo_uuid}.jpg'

    skip_by_uuid(
        subject_uuid=s.person_uuid,
        object_uuid=prospect_uuid,
        reason=req.report_reason or '',
    )

    if req.report_reason:
        with api_tx() as tx:
            if target_person_id is None:
                target_row = tx.execute(
                    """
                    SELECT id
                    FROM person
                    WHERE uuid = %(prospect_uuid)s
                    LIMIT 1
                    """,
                    {'prospect_uuid': prospect_uuid},
                ).fetchone()
                target_person_id = int(target_row['id']) if target_row else None

            if target_person_id is not None:
                if report_photo_uuid:
                    _create_or_touch_antiabuse_flag(
                        tx,
                        person_id=target_person_id,
                        category='reported-evidence',
                        severity='medium',
                        reason='A signed-in user submitted a report with image evidence.',
                        evidence={
                            'reporter_person_id': s.person_id,
                            'reporter_person_uuid': s.person_uuid,
                            'report_summary': req.report_reason[:500],
                            'photo_uuid': report_photo_uuid,
                            'photo_url': report_photo_url,
                        },
                    )
                _apply_structured_report_flags(
                    tx,
                    target_person_id=target_person_id,
                    reporter_person_id=s.person_id,
                    report_reason=req.report_reason,
                    report_category=req.report_category,
                    report_context=req.report_context,
                    report_photo_uuid=report_photo_uuid,
                    report_photo_url=report_photo_url,
                )
                _refresh_antiabuse_flags(tx, person_id=target_person_id)

            _refresh_antiabuse_flags(tx, person_id=s.person_id)

        if req.base64_file is not None and report_photo_uuid and crop_size:
            try:
                put_image_in_object_store(report_photo_uuid, req.base64_file, crop_size, sizes=[450])
            except Exception as e:
                print('In-app report evidence upload failed with exception:', e)


def post_unskip(s: t.SessionInfo, prospect_person_id: int):
    params = dict(
        subject_person_id=s.person_id,
        object_person_id=prospect_person_id,
    )

    with api_tx() as tx:
        tx.execute(Q_DELETE_SKIPPED, params)

def post_unskip_by_uuid(s: t.SessionInfo, prospect_uuid: str):
    params = dict(
        subject_person_id=s.person_id,
        prospect_uuid=prospect_uuid,
    )

    with api_tx() as tx:
        tx.execute(Q_DELETE_SKIPPED_BY_UUID, params)

def get_compare_personalities(
    s: t.SessionInfo,
    prospect_person_id: int,
    topic: str
):
    url_topic_to_db_topic = {
        'mbti': 'MBTI',
        'big5': 'Big 5',
        'attachment': 'Attachment Style',
        'politics': 'Politics',
        'other': 'Other',
    }

    if topic not in url_topic_to_db_topic:
        return 'Topic not found', 404

    db_topic = url_topic_to_db_topic[topic]

    params = dict(
        person_id_as_int=s.person_id,
        person_id_as_str=None,
        prospect_person_id=prospect_person_id,
        topic=db_topic,
    )

    with api_tx('READ COMMITTED') as tx:
        return tx.execute(Q_SELECT_PERSONALITY, params).fetchall()

def get_compare_answers(
    s: t.SessionInfo,
    prospect_person_id: int,
    agreement: Optional[str],
    topic: Optional[str],
    n: Optional[str],
    o: Optional[str],
):
    valid_agreements = ['all', 'agree', 'disagree', 'unanswered']
    valid_topics = ['all', 'values', 'sex', 'interpersonal', 'other']

    if agreement not in valid_agreements:
        return 'Invalid agreement', 400

    if topic not in valid_topics:
        return 'Invalid topic', 400

    try:
        n_int = int(n)
    except:
        return 'Invalid n', 400

    try:
        o_int = int(o)
    except:
        return 'Invalid o', 400

    params = dict(
        person_id=s.person_id,
        prospect_person_id=prospect_person_id,
        agreement=agreement.capitalize(),
        topic=topic.capitalize(),
        n=n,
        o=o,
    )

    with api_tx('READ COMMITTED') as tx:
        return tx.execute(Q_ANSWER_COMPARISON, params).fetchall()

def post_inbox_info(req: t.PostInboxInfo, s: t.SessionInfo):
    params = dict(
        person_id=s.person_id,
        prospect_person_uuids=req.person_uuids
    )

    with api_tx('READ COMMITTED') as tx:
        tx.execute(
            """
            WITH config AS (
                SELECT COALESCE(
                    MAX(CASE WHEN key = 'system_conversation_auto_close_days' THEN NULLIF(value, '')::INT END),
                    10
                ) AS auto_close_days
                FROM admin_setting
            )
            UPDATE inbox
            SET
                box = 'archive',
                unread_count = 0
            FROM config
            WHERE
                inbox.luser = (SELECT uuid::TEXT FROM person WHERE id = %(person_id)s)
            AND inbox.box = 'chats'
            AND inbox.timestamp < ((EXTRACT(EPOCH FROM NOW() - (config.auto_close_days || ' days')::interval) * 1e6)::BIGINT)
            """,
            {'person_id': s.person_id},
        )
        return tx.execute(Q_INBOX_INFO, params).fetchall()

def delete_or_ban_account(
    s: Optional[t.SessionInfo],
    admin_ban_token: Optional[str] = None,
):
    with api_tx() as tx:
        tx.execute('SET LOCAL statement_timeout = 30_000')  # 30 seconds

        if admin_ban_token:
            rows = tx.execute(
                Q_ADMIN_BAN,
                params=dict(token=admin_ban_token)
            ).fetchall()
        elif s:
            rows = [
                dict(
                    person_id=s.person_id,
                    person_uuid=s.person_uuid
                )
            ]
        else:
            raise ValueError('At least one parameter must not be None')

        tx.executemany(Q_DELETE_ACCOUNT, params_seq=rows)

    return rows

def post_deactivate(s: t.SessionInfo):
    params = dict(person_id=s.person_id)

    with api_tx() as tx:
        tx.execute(Q_POST_DEACTIVATE, params)

def get_profile_info(s: t.SessionInfo):
    params = dict(person_id=s.person_id)

    with api_tx('READ COMMITTED') as tx:
        profile = tx.execute(Q_GET_PROFILE_INFO, params).fetchone()['j']
        trust_summary = _get_person_trust_summary(tx, s.person_id)
        referral_summary = _get_referral_trust_summary(tx, s.person_id)
        active_conversation_row = tx.execute(
            """
            WITH config AS (
                SELECT COALESCE(
                    MAX(CASE WHEN key = 'system_conversation_auto_close_days' THEN NULLIF(value, '')::INT END),
                    10
                ) AS auto_close_days
                FROM admin_setting
            ), self_person AS (
                SELECT id, uuid::TEXT AS uuid_text
                FROM person
                WHERE id = %(person_id)s
            )
            SELECT COUNT(DISTINCT remote_bare_jid) AS count
            FROM inbox
            CROSS JOIN config
            CROSS JOIN self_person
            JOIN person AS remote_person
              ON remote_person.uuid::TEXT = split_part(inbox.remote_bare_jid, '@', 1)
            WHERE
                luser = self_person.uuid_text
            AND box = 'chats'
            AND timestamp >= ((EXTRACT(EPOCH FROM NOW() - (config.auto_close_days || ' days')::interval) * 1e6)::BIGINT)
            AND EXISTS (
                SELECT 1
                FROM messaged
                WHERE subject_person_id = self_person.id
                  AND object_person_id = remote_person.id
            )
            AND EXISTS (
                SELECT 1
                FROM messaged
                WHERE subject_person_id = remote_person.id
                  AND object_person_id = self_person.id
            )
            AND NOT EXISTS (
                SELECT 1
                FROM skipped
                WHERE
                    (subject_person_id = self_person.id AND object_person_id = remote_person.id)
                 OR (subject_person_id = remote_person.id AND object_person_id = self_person.id)
            )
            """,
            {'person_id': s.person_id},
        ).fetchone()

    profile['trust_score'] = trust_summary['trust_score']
    profile['trust_level'] = trust_summary['level']
    profile['trust_signals'] = trust_summary['signals']
    profile['trust_milestones'] = trust_summary.get('milestones', [])
    profile['trust_next_steps'] = trust_summary.get('next_steps', [])
    profile['trust_stage_label'] = trust_summary.get('stage_label')
    profile['invite_unlocked'] = bool(referral_summary.get('can_invite'))
    profile['active_conversation_count'] = int(active_conversation_row['count'] or 0) if active_conversation_row else 0

    return profile


def get_referral_dashboard(s: t.SessionInfo):
    with api_tx() as tx:
        code = _ensure_referral_code(tx, s.person_id)
        trust_summary = _get_referral_trust_summary(tx, s.person_id)
        usage_count = _count_referral_uses(tx, code['id'])

        referrer = tx.execute(
            """
            SELECT
                owner.id,
                owner.name,
                referral_code.code
            FROM person
            JOIN referral_code ON referral_code.id = person.referred_by_code_id
            JOIN person AS owner ON owner.id = referral_code.person_id
            WHERE person.id = %(person_id)s
            LIMIT 1
            """,
            {'person_id': s.person_id},
        ).fetchone()

        joined = tx.execute(
            """
            SELECT
                person.id,
                person.name,
                'joined' AS status
            FROM person
            JOIN referral_code ON referral_code.id = person.referred_by_code_id
            WHERE referral_code.person_id = %(person_id)s
            ORDER BY person.sign_up_time DESC, person.id DESC
            """,
            {'person_id': s.person_id},
        ).fetchall()

        pending = tx.execute(
            """
            SELECT DISTINCT
                COALESCE(onboardee.name, 'Pending referral') AS name,
                lower(duo_session.email) AS email_key,
                'pending' AS status
            FROM duo_session
            JOIN referral_code ON referral_code.id = duo_session.referral_code_id
            LEFT JOIN onboardee ON onboardee.email = duo_session.email
            WHERE referral_code.person_id = %(person_id)s
            AND duo_session.person_id IS NULL
            AND NOT EXISTS (
                SELECT 1
                FROM person
                WHERE lower(person.email) = lower(duo_session.email)
            )
            ORDER BY name, email_key
            """,
            {'person_id': s.person_id},
        ).fetchall()

    return {
        'current_code': {
            'id': code['id'],
            'code': code['code'],
            'usage_count': usage_count,
            'max_uses': trust_summary['invite_limit'],
            'disabled': code['disabled'],
            'is_active': bool(trust_summary['can_invite']),
        },
        'trust_score': trust_summary['person_trust_score'],
        'referral_access_score': trust_summary['trust_score'],
        'trust_summary': trust_summary,
        'referral_access_message': trust_summary.get('pause_reason'),
        'invite_unlocked_at': trust_summary.get('invite_unlocked_at'),
        'is_admin_bypass': bool(trust_summary.get('is_admin_bypass')),
        'can_generate_new_code': bool(trust_summary['can_invite']),
        'referred_by': dict(referrer) if referrer else None,
        'referred_people': [
            *[dict(row) for row in joined],
            *[
                {
                    'id': None,
                    'name': row['name'],
                    'email_key': row['email_key'],
                    'status': row['status'],
                }
                for row in pending
            ],
        ],
    }


def regenerate_referral_code(s: t.SessionInfo):
    with api_tx() as tx:
        trust_summary = _get_referral_trust_summary(tx, s.person_id)
        if not trust_summary['can_invite']:
            return 'Referral access is temporarily limited while admins review your trust score.', 403

        latest = _get_latest_referral_code(tx, s.person_id)
        code = (
            _replace_referral_code(tx, latest['id'])
            if latest else
            _generate_referral_code(tx, s.person_id)
        )

    return {
        'id': code['id'],
        'code': code['code'],
        'usage_count': 0,
        'max_uses': trust_summary['invite_limit'],
        'disabled': False,
        'is_active': True,
        'trust_score': trust_summary['trust_score'],
    }

def delete_profile_info(req: t.DeleteProfileInfo, s: t.SessionInfo):
    files_params = [
        dict(person_id=s.person_id, position=position)
        for position in req.files or []
    ]

    audio_files_params = [
        dict(person_id=s.person_id, position=-1)
        for position in req.audio_files or []
    ]

    if files_params:
        with api_tx() as tx:
            tx.executemany(Q_DELETE_PROFILE_INFO_PHOTO, files_params)
            tx.execute(Q_UPDATE_VERIFICATION_LEVEL, files_params[0])

    if audio_files_params:
        with api_tx() as tx:
            tx.executemany(Q_DELETE_PROFILE_INFO_AUDIO, audio_files_params)

def _patch_profile_info_about(person_id: int, new_about: str):
    select = """
    SELECT about AS old_about FROM person WHERE id = %(person_id)s
    """

    update = """
    WITH updated_person AS (
        UPDATE person
        SET
            about = %(new_about)s::TEXT,

            last_event_time =
                CASE
                    WHEN %(added_text)s::TEXT IS NULL
                    THEN sign_up_time
                    ELSE now()
                END,

            last_event_name =
                CASE
                    WHEN %(added_text)s::TEXT IS NULL
                    THEN 'joined'::person_event
                    ELSE 'updated-bio'::person_event
                END,

            last_event_data =
                CASE
                    WHEN %(added_text)s::TEXT IS NULL
                    THEN
                        '{}'::JSONB
                    ELSE
                        jsonb_build_object(
                            'added_text', %(added_text)s::TEXT,
                            'body_color', body_color,
                            'background_color', background_color
                        )
                END
        WHERE
            id = %(person_id)s
    ), updated_unmoderated_person AS (
        INSERT INTO
            unmoderated_person (person_id, trait)
        VALUES
            (%(person_id)s, 'about')
        ON CONFLICT DO NOTHING
    )
    SELECT 1
    """

    with api_tx() as tx:
        select_params = dict(
            person_id=person_id,
        )

        tx.execute(select, select_params)

        old_about = tx.fetchone()['old_about']

        update_params = dict(
            person_id=person_id,
            new_about=new_about,
            added_text=diff_addition_with_context(old=old_about, new=new_about),
        )

        tx.execute(update, update_params)

def patch_profile_info(req: t.PatchProfileInfo, s: t.SessionInfo):
    if not s.person_id:
        return 'Not authorized', 400

    [field_name] = req.__pydantic_fields_set__
    field_value = req.dict()[field_name]

    params = dict(
        person_id=s.person_id,
        field_value=field_value,
    )

    q1 = None
    q2 = None

    uuid = None
    base64_file = None
    crop_size = None

    base64_audio_file = None

    if field_name == 'base64_file':
        base64_file = t.Base64File(**field_value)

        crop_size = CropSize(
                top=base64_file.top,
                left=base64_file.left)
        uuid = secrets.token_hex(32)
        blurhash_ = compute_blurhash(base64_file.image, crop_size=crop_size)
        extra_exts = ['gif'] if base64_file.image.format == 'GIF' else []

        params = dict(
            person_id=s.person_id,
            position=base64_file.position,
            uuid=uuid,
            blurhash=blurhash_,
            extra_exts=extra_exts,
            hash=base64_file.md5_hash,
        )

        q1 = """
        WITH existing_uuid AS (
            SELECT
                uuid
            FROM
                photo
            WHERE
                person_id = %(person_id)s
            AND
                position = %(position)s
        ), undeleted_photo_insertion AS (
            INSERT INTO undeleted_photo (
                uuid
            )
            SELECT
                uuid
            FROM
                existing_uuid
        ), photo_insertion AS (
            INSERT INTO photo (
                person_id,
                position,
                uuid,
                blurhash,
                extra_exts,
                hash
            ) VALUES (
                %(person_id)s,
                %(position)s,
                %(uuid)s,
                %(blurhash)s,
                %(extra_exts)s,
                %(hash)s
            ) ON CONFLICT (person_id, position) DO UPDATE SET
                uuid = EXCLUDED.uuid,
                blurhash = EXCLUDED.blurhash,
                extra_exts = EXCLUDED.extra_exts,
                hash = EXCLUDED.hash,
                verified = FALSE
        ), updated_person AS (
            UPDATE person
            SET
                last_event_time = now(),
                last_event_name = 'added-photo',
                last_event_data = jsonb_build_object(
                    'added_photo_uuid', %(uuid)s,
                    'added_photo_blurhash', %(blurhash)s,
                    'added_photo_extra_exts', %(extra_exts)s::TEXT[]
                )
            WHERE
                id = %(person_id)s
        )
        SELECT 1
        """

        q2 = Q_UPDATE_VERIFICATION_LEVEL
    elif field_name == 'base64_audio_file':
        base64_audio_file = t.Base64AudioFile(**field_value)

        uuid = secrets.token_hex(32)

        params = dict(
            person_id=s.person_id,
            uuid=uuid,
        )

        q1 = """
        WITH existing_uuid AS (
            SELECT
                uuid
            FROM
                audio
            WHERE
                person_id = %(person_id)s
            AND
                position = -1
        ), undeleted_audio_insertion AS (
            INSERT INTO undeleted_audio (
                uuid
            )
            SELECT
                uuid
            FROM
                existing_uuid
        ), audio_insertion AS (
            INSERT INTO audio (
                person_id,
                position,
                uuid
            ) VALUES (
                %(person_id)s,
                -1,
                %(uuid)s
            ) ON CONFLICT (person_id, position) DO UPDATE SET
                uuid = EXCLUDED.uuid
        ), updated_person AS (
            UPDATE person
            SET
                last_event_time = now(),
                last_event_name = 'added-voice-bio',
                last_event_data = jsonb_build_object(
                    'added_audio_uuid', %(uuid)s
                )
            WHERE
                id = %(person_id)s
        )
        SELECT 1
        """
    elif field_name == 'photo_assignments':
        case_sql = '\n'.join(
            f'WHEN position = {int(k)} THEN {int(v)}'
            for k, v in field_value.items()
        )

        # We set the positions to negative indexes first, to avoid violating
        # uniqueness constraints
        q1 = f"""
        UPDATE
            photo
        SET
            position = - (CASE {case_sql} ELSE position END)
        WHERE
            person_id = %(person_id)s
        """

        q2 = """
        UPDATE
            photo
        SET
            position = ABS(position)
        WHERE
            person_id = %(person_id)s
        """
    elif field_name == 'name':
        if not _has_gold(person_id=s.person_id):
            return 'Requires gold', 403

        q1 = """
        UPDATE person
        SET name = %(field_value)s
        WHERE id = %(person_id)s
        """
    elif field_name == 'about':
        return _patch_profile_info_about(s.person_id, field_value)
    elif field_name == 'gender':
        q1 = """
        UPDATE person
        SET gender_id = gender.id, verified_gender = false
        FROM gender
        WHERE person.id = %(person_id)s
        AND gender.name = %(field_value)s
        AND person.gender_id <> gender.id
        """

        q2 = Q_UPDATE_VERIFICATION_LEVEL
    elif field_name == 'orientation':
        q1 = """
        UPDATE person SET orientation_id = orientation.id
        FROM orientation
        WHERE person.id = %(person_id)s
        AND orientation.name = %(field_value)s
        """
    elif field_name == 'ethnicity':
        q1 = """
        UPDATE person
        SET ethnicity_id = ethnicity.id, verified_ethnicity = false
        FROM ethnicity
        WHERE person.id = %(person_id)s
        AND ethnicity.name = %(field_value)s
        AND person.ethnicity_id <> ethnicity.id
        """

        q2 = Q_UPDATE_VERIFICATION_LEVEL
    elif field_name == 'location':
        q1 = """
        UPDATE person
        SET
            coordinates
                = location.coordinates,

            verification_required
                = location.verification_required OR person.verification_required,

            location_short_friendly
                = location.short_friendly,

            location_long_friendly
                = location.long_friendly
        FROM location
        WHERE person.id = %(person_id)s
        AND long_friendly = %(field_value)s
        """
    elif field_name == 'occupation':
        q1 = """
        UPDATE person SET occupation = %(field_value)s
        WHERE person.id = %(person_id)s
        """
    elif field_name == 'education':
        q1 = """
        UPDATE person SET education = %(field_value)s
        WHERE person.id = %(person_id)s
        """
    elif field_name == 'height':
        q1 = """
        UPDATE person SET height_cm = %(field_value)s
        WHERE person.id = %(person_id)s
        """
    elif field_name == 'looking_for':
        q1 = """
        UPDATE person SET looking_for_id = looking_for.id
        FROM looking_for
        WHERE person.id = %(person_id)s
        AND looking_for.name = %(field_value)s
        """
    elif field_name == 'smoking':
        q1 = """
        UPDATE person SET smoking_id = yes_no_optional.id
        FROM yes_no_optional
        WHERE person.id = %(person_id)s
        AND yes_no_optional.name = %(field_value)s
        """
    elif field_name == 'drinking':
        q1 = """
        UPDATE person SET drinking_id = frequency.id
        FROM frequency
        WHERE person.id = %(person_id)s
        AND frequency.name = %(field_value)s
        """
    elif field_name == 'drugs':
        q1 = """
        UPDATE person SET drugs_id = yes_no_optional.id
        FROM yes_no_optional
        WHERE person.id = %(person_id)s
        AND yes_no_optional.name = %(field_value)s
        """
    elif field_name == 'long_distance':
        q1 = """
        UPDATE person SET long_distance_id = yes_no_optional.id
        FROM yes_no_optional
        WHERE person.id = %(person_id)s
        AND yes_no_optional.name = %(field_value)s
        """
    elif field_name == 'relationship_status':
        q1 = """
        UPDATE person SET relationship_status_id = relationship_status.id
        FROM relationship_status
        WHERE person.id = %(person_id)s
        AND relationship_status.name = %(field_value)s
        """
    elif field_name == 'pioneer_status':
        q1 = """
        UPDATE person SET pioneer_status = %(field_value)s
        WHERE person.id = %(person_id)s
        """
    elif field_name == 'service_goals':
        q1 = """
        UPDATE person SET service_goals = %(field_value)s
        WHERE person.id = %(person_id)s
        """
    elif field_name == 'willingness_to_relocate':
        q1 = """
        UPDATE person SET willingness_to_relocate = %(field_value)s
        WHERE person.id = %(person_id)s
        """
    elif field_name == 'family_worship_habit':
        q1 = """
        UPDATE person SET family_worship_habit = %(field_value)s
        WHERE person.id = %(person_id)s
        """
    elif field_name == 'spiritual_routine':
        q1 = """
        UPDATE person SET spiritual_routine = %(field_value)s
        WHERE person.id = %(person_id)s
        """
    elif field_name == 'willing_to_involve_family_early':
        q1 = """
        UPDATE person SET willing_to_involve_family_early = %(field_value)s
        WHERE person.id = %(person_id)s
        """
    elif field_name == 'open_to_chaperoned_video_calls':
        q1 = """
        UPDATE person SET open_to_chaperoned_video_calls = %(field_value)s
        WHERE person.id = %(person_id)s
        """
    elif field_name == 'congregation_compatibility':
        q1 = """
        UPDATE person SET congregation_compatibility = %(field_value)s
        WHERE person.id = %(person_id)s
        """
    elif field_name == 'service_lifestyle':
        q1 = """
        UPDATE person SET service_lifestyle = %(field_value)s
        WHERE person.id = %(person_id)s
        """
    elif field_name == 'life_stage':
        q1 = """
        UPDATE person SET life_stage = %(field_value)s
        WHERE person.id = %(person_id)s
        """
    elif field_name == 'emotional_temperament':
        q1 = """
        UPDATE person SET emotional_temperament = %(field_value)s
        WHERE person.id = %(person_id)s
        """
    elif field_name == 'communication_style':
        q1 = """
        UPDATE person SET communication_style = %(field_value)s
        WHERE person.id = %(person_id)s
        """
    elif field_name == 'who_can_contact_me':
        q1 = """
        UPDATE person SET who_can_contact_me = %(field_value)s
        WHERE person.id = %(person_id)s
        """
    elif field_name == 'request_format_preference':
        q1 = """
        UPDATE person SET request_format_preference = %(field_value)s
        WHERE person.id = %(person_id)s
        """
    elif field_name == 'message_pace_preference':
        q1 = """
        UPDATE person SET message_pace_preference = %(field_value)s
        WHERE person.id = %(person_id)s
        """
    elif field_name == 'has_kids':
        q1 = """
        UPDATE person SET has_kids_id = yes_no_maybe.id
        FROM yes_no_maybe
        WHERE person.id = %(person_id)s
        AND yes_no_maybe.name = %(field_value)s
        """
    elif field_name == 'wants_kids':
        q1 = """
        UPDATE person SET wants_kids_id = yes_no_maybe.id
        FROM yes_no_maybe
        WHERE person.id = %(person_id)s
        AND yes_no_maybe.name = %(field_value)s
        """
    elif field_name == 'exercise':
        q1 = """
        UPDATE person SET exercise_id = frequency.id
        FROM frequency
        WHERE person.id = %(person_id)s
        AND frequency.name = %(field_value)s
        """
    elif field_name == 'religion':
        q1 = """
        UPDATE person SET religion_id = religion.id
        FROM religion
        WHERE person.id = %(person_id)s
        AND religion.name = %(field_value)s
        """
    elif field_name == 'star_sign':
        q1 = """
        UPDATE person SET star_sign_id = star_sign.id
        FROM star_sign
        WHERE person.id = %(person_id)s
        AND star_sign.name = %(field_value)s
        """
    elif field_name == 'units':
        q1 = """
        UPDATE person SET unit_id = unit.id
        FROM unit
        WHERE person.id = %(person_id)s
        AND unit.name = %(field_value)s
        """
    elif field_name == 'chats':
        q1 = """
        UPDATE person SET chats_notification = immediacy.id
        FROM immediacy
        WHERE person.id = %(person_id)s
        AND immediacy.name = %(field_value)s
        """
    elif field_name == 'intros':
        q1 = """
        UPDATE person SET intros_notification = immediacy.id
        FROM immediacy
        WHERE person.id = %(person_id)s
        AND immediacy.name = %(field_value)s
        """
    elif field_name == 'verification_level':
        q1 = """
        UPDATE person
        SET privacy_verification_level_id = verification_level.id
        FROM verification_level
        WHERE person.id = %(person_id)s AND
        verification_level.name = %(field_value)s
        """
    elif field_name == 'profile_status':
        q1 = """
        UPDATE person
        SET
            profile_status = %(field_value)s,
            profile_status_changed_at = NOW()
        WHERE id = %(person_id)s
        """
    elif field_name == 'show_my_location':
        if not _has_gold(person_id=s.person_id):
            return 'Requires gold', 403

        q1 = """
        UPDATE person
        SET show_my_location = (
            CASE WHEN %(field_value)s = 'Yes' THEN TRUE ELSE FALSE END)
        WHERE id = %(person_id)s
        """
    elif field_name == 'show_my_age':
        if not _has_gold(person_id=s.person_id):
            return 'Requires gold', 403

        q1 = """
        UPDATE person
        SET show_my_age = (
            CASE WHEN %(field_value)s = 'Yes' THEN TRUE ELSE FALSE END)
        WHERE id = %(person_id)s
        """
    elif field_name == 'hide_me_from_strangers':
        if not _has_gold(person_id=s.person_id):
            return 'Requires gold', 403

        q1 = """
        UPDATE person
        SET hide_me_from_strangers = (
            CASE WHEN %(field_value)s = 'Yes' THEN TRUE ELSE FALSE END)
        WHERE id = %(person_id)s
        """
    elif field_name == 'browse_invisibly':
        if not _has_gold(person_id=s.person_id):
            return 'Requires gold', 403

        q1 = """
        UPDATE person
        SET browse_invisibly = (
            CASE WHEN %(field_value)s = 'Yes' THEN TRUE ELSE FALSE END)
        WHERE id = %(person_id)s
        """
    elif field_name == 'theme':
        if not _has_gold(person_id=s.person_id):
            return 'Requires gold', 403

        try:
            title_color = field_value['title_color']
            body_color = field_value['body_color']
            background_color = field_value['background_color']

            params.update(
                dict(
                    title_color=title_color,
                    body_color=body_color,
                    background_color=background_color,
                )
            )
        except:
            return f'Invalid colors', 400

        q1 = """
        UPDATE person
        SET
            title_color = %(title_color)s,
            body_color = %(body_color)s,
            background_color = %(background_color)s
        WHERE id = %(person_id)s
        """
    else:
        return f'Unhandled field name {field_name}', 500

    with api_tx() as tx:
        if q1: tx.execute(q1, params)
        if q2: tx.execute(q2, params)
        _refresh_antiabuse_flags(tx, person_id=s.person_id)

    if uuid and base64_file and crop_size:
        try:
            put_image_in_object_store(uuid, base64_file, crop_size)
        except:
            print(traceback.format_exc())
            return '', 500

    if uuid and base64_audio_file:
        try:
            put_audio_in_object_store(
                uuid=uuid,
                audio_file_bytes=base64_audio_file.transcoded,
            )
        except:
            print(traceback.format_exc())
            return '', 500

def get_search_filters(s: t.SessionInfo):
    return get_search_filters_by_person_id(person_id=s.person_id)

def get_search_filters_by_person_id(person_id: Optional[int]):
    params = dict(person_id=person_id)

    with api_tx('READ COMMITTED') as tx:
        return tx.execute(Q_GET_SEARCH_FILTERS, params).fetchone()['j']

def post_search_filter(req: t.PostSearchFilter, s: t.SessionInfo):
    [field_name] = req.__pydantic_fields_set__
    field_value = req.dict()[field_name]

    # Modify `field_value` for certain `field_name`s
    if field_name == 'age' and isinstance(field_value, dict):
        with api_tx('READ COMMITTED') as tx:
            row = tx.execute(
                """
                SELECT gender.name AS gender
                FROM person
                JOIN gender
                ON gender.id = person.gender_id
                WHERE person.id = %(person_id)s
                """,
                {'person_id': s.person_id},
            ).fetchone()

        opposite_gender = _opposite_gender(dict(row or {}).get('gender'))
        minimum_age = MIN_AGE_BY_GENDER.get(opposite_gender or '', 18)
        if field_value.get('min_age') is not None:
            field_value['min_age'] = max(int(field_value['min_age']), minimum_age)
        if field_value.get('max_age') is not None:
            field_value['max_age'] = max(int(field_value['max_age']), minimum_age)
        field_value = json.dumps(field_value)
    elif field_name in ['height']:
        field_value = json.dumps(field_value)

    params = dict(
        person_id=s.person_id,
        field_value=field_value,
    )

    with api_tx() as tx:
        if field_name == 'gender':
            field_value = [dict(tx.execute(
                """
                SELECT CASE
                    WHEN gender.name = 'Man' THEN 'Woman'
                    ELSE 'Man'
                END AS name
                FROM person
                JOIN gender
                ON gender.id = person.gender_id
                WHERE person.id = %(person_id)s
                """,
                {'person_id': s.person_id},
            ).fetchone() or {'name': 'Man'})['name']]

            q1 = """
            DELETE FROM search_preference_gender
            WHERE person_id = %(person_id)s"""

            q2 = """
            INSERT INTO search_preference_gender (
                person_id, gender_id
            )
            SELECT %(person_id)s, id
            FROM gender WHERE name = ANY(%(field_value)s)
            """
        elif field_name == 'orientation':
            q1 = """
            DELETE FROM search_preference_orientation
            WHERE person_id = %(person_id)s"""

            q2 = """
            INSERT INTO search_preference_orientation (
                person_id, orientation_id
            )
            SELECT %(person_id)s, id
            FROM orientation WHERE name = ANY(%(field_value)s)
            """
        elif field_name == 'ethnicity':
            q1 = """
            DELETE FROM search_preference_ethnicity
            WHERE person_id = %(person_id)s"""

            q2 = """
            INSERT INTO search_preference_ethnicity (
                person_id, ethnicity_id
            )
            SELECT %(person_id)s, id
            FROM ethnicity WHERE name = ANY(%(field_value)s)
            """
        elif field_name == 'city':
            q1 = """
            DELETE FROM search_preference_city
            WHERE person_id = %(person_id)s"""

            q2 = """
            INSERT INTO search_preference_city (person_id, city)
            VALUES (%(person_id)s, %(field_value)s)
            """
        elif field_name == 'state':
            q1 = """
            DELETE FROM search_preference_state
            WHERE person_id = %(person_id)s"""

            q2 = """
            INSERT INTO search_preference_state (person_id, state)
            VALUES (%(person_id)s, %(field_value)s)
            """
        elif field_name == 'age':
            q1 = """
            DELETE FROM search_preference_age
            WHERE person_id = %(person_id)s"""

            q2 = """
            INSERT INTO search_preference_age (
                person_id, min_age, max_age
            ) SELECT
                %(person_id)s,
                (json_data->>'min_age')::SMALLINT,
                (json_data->>'max_age')::SMALLINT
            FROM to_json(%(field_value)s::json) AS json_data"""
        elif field_name == 'baptism_years':
            q1 = """
            DELETE FROM search_preference_baptism_years
            WHERE person_id = %(person_id)s"""

            q2 = """
            INSERT INTO search_preference_baptism_years (
                person_id, min_baptism_years
            )
            VALUES (%(person_id)s, %(field_value)s)
            """
        elif field_name == 'furthest_distance':
            q1 = """
            DELETE FROM search_preference_distance
            WHERE person_id = %(person_id)s"""

            q2 = """
            INSERT INTO search_preference_distance (person_id, distance)
            VALUES (%(person_id)s, %(field_value)s)
            """
        elif field_name == 'height':
            q1 = """
            DELETE FROM search_preference_height_cm
            WHERE person_id = %(person_id)s"""

            q2 = """
            INSERT INTO search_preference_height_cm (
                person_id, min_height_cm, max_height_cm
            ) SELECT
                %(person_id)s,
                (json_data->>'min_height_cm')::SMALLINT,
                (json_data->>'max_height_cm')::SMALLINT
            FROM to_json(%(field_value)s::json) AS json_data"""
        elif field_name == 'has_a_profile_picture':
            q1 = """
            DELETE FROM search_preference_has_profile_picture
            WHERE person_id = %(person_id)s"""

            q2 = """
            INSERT INTO search_preference_has_profile_picture (
                person_id, has_profile_picture_id
            ) SELECT %(person_id)s, id
            FROM yes_no WHERE name = ANY(%(field_value)s)
            """
        elif field_name == 'looking_for':
            q1 = """
            DELETE FROM search_preference_looking_for
            WHERE person_id = %(person_id)s"""

            q2 = """
            INSERT INTO search_preference_looking_for (
                person_id, looking_for_id
            ) SELECT %(person_id)s, id
            FROM looking_for WHERE name = ANY(%(field_value)s)
            """
        elif field_name == 'smoking':
            q1 = """
            DELETE FROM search_preference_smoking
            WHERE person_id = %(person_id)s"""

            q2 = """
            INSERT INTO search_preference_smoking (
                person_id, smoking_id
            )
            SELECT %(person_id)s, id
            FROM yes_no_optional WHERE name = ANY(%(field_value)s)
            """
        elif field_name == 'drinking':
            q1 = """
            DELETE FROM search_preference_drinking
            WHERE person_id = %(person_id)s"""

            q2 = """
            INSERT INTO search_preference_drinking (
                person_id, drinking_id
            )
            SELECT %(person_id)s, id
            FROM frequency WHERE name = ANY(%(field_value)s)
            """
        elif field_name == 'drugs':
            q1 = """
            DELETE FROM search_preference_drugs
            WHERE person_id = %(person_id)s"""

            q2 = """
            INSERT INTO search_preference_drugs (
                person_id, drugs_id
            )
            SELECT %(person_id)s, id
            FROM yes_no_optional WHERE name = ANY(%(field_value)s)
            """
        elif field_name == 'long_distance':
            q1 = """
            DELETE FROM search_preference_long_distance
            WHERE person_id = %(person_id)s"""

            q2 = """
            INSERT INTO search_preference_long_distance (
                person_id, long_distance_id
            )
            SELECT %(person_id)s, id
            FROM yes_no_optional WHERE name = ANY(%(field_value)s)
            """
        elif field_name == 'relationship_status':
            q1 = """
            DELETE FROM search_preference_relationship_status
            WHERE person_id = %(person_id)s"""

            q2 = """
            INSERT INTO search_preference_relationship_status (
                person_id, relationship_status_id
            )
            SELECT %(person_id)s, id
            FROM relationship_status WHERE name = ANY(%(field_value)s)
            """
        elif field_name == 'pioneer_status':
            q1 = """
            DELETE FROM search_preference_pioneer_status
            WHERE person_id = %(person_id)s"""

            q2 = """
            INSERT INTO search_preference_pioneer_status (
                person_id, status
            )
            SELECT %(person_id)s, UNNEST(%(field_value)s::TEXT[])
            """
        elif field_name == 'has_kids':
            q1 = """
            DELETE FROM search_preference_has_kids
            WHERE person_id = %(person_id)s"""

            q2 = """
            INSERT INTO search_preference_has_kids (
                person_id, has_kids_id
            )
            SELECT %(person_id)s, id
            FROM yes_no_optional WHERE name = ANY(%(field_value)s)
            """
        elif field_name == 'wants_kids':
            q1 = """
            DELETE FROM search_preference_wants_kids
            WHERE person_id = %(person_id)s"""

            q2 = """
            INSERT INTO search_preference_wants_kids (
                person_id, wants_kids_id
            )
            SELECT %(person_id)s, id
            FROM yes_no_maybe WHERE name = ANY(%(field_value)s)
            """
        elif field_name == 'exercise':
            q1 = """
            DELETE FROM search_preference_exercise
            WHERE person_id = %(person_id)s"""

            q2 = """
            INSERT INTO search_preference_exercise (
                person_id, exercise_id
            )
            SELECT %(person_id)s, id
            FROM frequency WHERE name = ANY(%(field_value)s)
            """
        elif field_name == 'religion':
            q1 = """
            DELETE FROM search_preference_religion
            WHERE person_id = %(person_id)s"""

            q2 = """
            INSERT INTO search_preference_religion (
                person_id, religion_id
            )
            SELECT %(person_id)s, id
            FROM religion WHERE name = ANY(%(field_value)s)
            """
        elif field_name == 'star_sign':
            q1 = """
            DELETE FROM search_preference_star_sign
            WHERE person_id = %(person_id)s"""

            q2 = """
            INSERT INTO search_preference_star_sign (
                person_id, star_sign_id
            )
            SELECT %(person_id)s, id
            FROM star_sign WHERE name = ANY(%(field_value)s)
            """
        elif field_name == 'people_you_messaged':
            q1 = """
            DELETE FROM search_preference_messaged
            WHERE person_id = %(person_id)s"""

            q2 = """
            INSERT INTO search_preference_messaged (
                person_id, messaged_id
            )
            SELECT %(person_id)s, id
            FROM yes_no WHERE name = %(field_value)s
            """
        elif field_name == 'people_you_skipped':
            q1 = """
            DELETE FROM search_preference_skipped
            WHERE person_id = %(person_id)s"""

            q2 = """
            INSERT INTO search_preference_skipped (
                person_id, skipped_id
            )
            SELECT %(person_id)s, id
            FROM yes_no WHERE name = %(field_value)s
            """
        else:
            return f'Invalid field name {field_name}', 400

        tx.execute(q1, params)
        tx.execute(q2, params)

def post_search_filter_answer(req: t.PostSearchFilterAnswer, s: t.SessionInfo):
    max_search_filter_answers = 20
    error = f'You can’t set more than {max_search_filter_answers} Q&A filters'

    params = dict(
        person_id=s.person_id,
        question_id=req.question_id,
        answer=req.answer,
        accept_unanswered=req.accept_unanswered,
    )

    if req.answer is None:
        q = f"""
        WITH deleted_answer AS (
            DELETE FROM search_preference_answer
            WHERE
                person_id = %(person_id)s AND
                question_id = %(question_id)s
            RETURNING *
        )
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
        WHERE
            person_id = %(person_id)s AND
            question_id != (SELECT question_id FROM deleted_answer)
        """
    else:
        q = f"""
        WITH existing_search_preference_answer AS (
            SELECT
                person_id,
                question_id,
                answer,
                accept_unanswered,
                0 AS precedence
            FROM search_preference_answer
            WHERE person_id = %(person_id)s
        ), new_search_preference_answer AS (
            SELECT
                %(person_id)s AS person_id,
                %(question_id)s AS question_id,
                %(answer)s AS answer,
                %(accept_unanswered)s AS accept_unanswered,
                1 AS precedence
        ), updated_search_preference_answer AS (
            SELECT DISTINCT ON (person_id, question_id)
                person_id,
                question_id,
                answer,
                accept_unanswered
            FROM (
                (SELECT * from existing_search_preference_answer)
                UNION
                (SELECT * from new_search_preference_answer)
            ) AS t
            ORDER BY person_id, question_id, precedence DESC
        ), inserted_search_preference_answer AS (
            INSERT INTO search_preference_answer (
                person_id, question_id, answer, accept_unanswered
            ) SELECT
                person_id, question_id, answer, accept_unanswered
            FROM
                new_search_preference_answer
            WHERE (
                SELECT COUNT(*) FROM updated_search_preference_answer
            ) <= {max_search_filter_answers}
            ON CONFLICT (person_id, question_id) DO UPDATE SET
                answer            = EXCLUDED.answer,
                accept_unanswered = EXCLUDED.accept_unanswered
        )
        SELECT array_agg(
            json_build_object(
                'question_id', question_id,
                'question', question,
                'topic', topic,
                'answer', answer,
                'accept_unanswered', accept_unanswered
            )
            ORDER BY question_id
        ) AS j
        FROM updated_search_preference_answer
        LEFT JOIN question
        ON question.id = question_id
        WHERE (
            SELECT COUNT(*) FROM updated_search_preference_answer
        ) <= {max_search_filter_answers}
        """

    with api_tx() as tx:
        answer = tx.execute(q, params).fetchone().get('j')
        if answer is None:
            return dict(error=error), 400
        else:
            return dict(answer=answer)

def get_update_notifications(email: str, type: str, frequency: str):
    params = dict(
        email=email,
        frequency=frequency,
    )

    if type == 'Intros':
        queries = [Q_UPDATE_INTROS_NOTIFICATIONS]
    elif type == 'Chats':
        queries = [Q_UPDATE_CHATS_NOTIFICATIONS]
    elif type == 'Every':
        queries = [Q_UPDATE_INTROS_NOTIFICATIONS, Q_UPDATE_CHATS_NOTIFICATIONS]
    else:
        return 'Invalid type', 400

    with api_tx('READ COMMITTED') as tx:
        query_results = [tx.execute(q, params).fetchone()['ok'] for q in queries]

    if all(query_results):
        return (
            f"✅ "
            f"<b>{type}</b> notification frequency set to "
            f"<b>{frequency}</b> for "
            f"<b>{email}</b>")
    else:
        return 'Invalid email address or notification frequency', 400

def post_verification_selfie(req: t.PostVerificationSelfie, s: t.SessionInfo):
    base64 = req.base64_file.base64
    image = req.base64_file.image
    top = req.base64_file.top
    left = req.base64_file.left
    hash = req.base64_file.md5_hash

    crop_size = CropSize(top=top, left=left)
    photo_uuid = secrets.token_hex(32)

    params_ok = dict(
        person_id=s.person_id,
        photo_uuid=photo_uuid,
        photo_hash=hash,
        expected_previous_status=None,
    )

    params_bad = dict(
        person_id=s.person_id,
        status='failure',
        message=V_REUSED_SELFIE,
        expected_previous_status=None,
    )

    with api_tx() as tx:
        if tx.execute(Q_INSERT_VERIFICATION_PHOTO_HASH, params_ok).fetchall():
            tx.execute(Q_DELETE_VERIFICATION_JOB, params_ok)
            tx.execute(Q_INSERT_VERIFICATION_JOB, params_ok)
            tx.execute(Q_UPSERT_VERIFICATION_REVIEW_SELFIE, params_ok)
            tx.execute(
                Q_UPSERT_VERIFICATION_REVIEW_AI_STATUS,
                {
                    'person_id': s.person_id,
                    'ai_status': 'submitted',
                    'ai_message': 'Waiting for manual review.',
                },
            )
        else:
            tx.execute(Q_UPDATE_VERIFICATION_JOB, params_bad)
            tx.execute(
                Q_UPSERT_VERIFICATION_REVIEW_AI_STATUS,
                {
                    'person_id': s.person_id,
                    'ai_status': 'failure',
                    'ai_message': V_REUSED_SELFIE,
                },
            )

    try:
        put_image_in_object_store(
            photo_uuid, req.base64_file, crop_size, sizes=[450])
    except Exception as e:
        print('Upload failed with exception:', e)
        return '', 500

def post_verify(s: t.SessionInfo):
    with api_tx() as tx:
        tx.execute(
            Q_UPSERT_VERIFICATION_REVIEW_AI_STATUS,
            {
                'person_id': s.person_id,
                'ai_status': 'submitted',
                'ai_message': 'Waiting for manual review.',
            },
        )


def post_verification_document(req: t.PostVerificationDocument, s: t.SessionInfo):
    photo_uuid = secrets.token_hex(32)
    crop_size = CropSize(
        top=req.base64_file.top,
        left=req.base64_file.left,
    )

    with api_tx() as tx:
        tx.execute(
            Q_INSERT_VERIFICATION_REVIEW_ASSET,
            dict(
                person_id=s.person_id,
                kind=req.kind,
                label=req.label,
                photo_uuid=photo_uuid,
            ),
        )

    try:
        put_image_in_object_store(photo_uuid, req.base64_file, crop_size, sizes=[450])
    except Exception as e:
        print('Verification document upload failed with exception:', e)
        return '', 500


def delete_verification_document(asset_id: int, s: t.SessionInfo):
    with api_tx() as tx:
        row = tx.execute(
            Q_DELETE_VERIFICATION_REVIEW_ASSET,
            dict(
                asset_id=asset_id,
                person_id=s.person_id,
            ),
        ).fetchone()

    if not row:
        return 'Document not found', 404


def get_verification_request(s: t.SessionInfo):
    with api_tx('READ COMMITTED') as tx:
        row = tx.execute(
            Q_GET_VERIFICATION_REVIEW,
            dict(person_id=s.person_id),
        ).fetchone()

    if row:
        review = _hydrate_verification_review_media(row)
        has_selfie = bool(review.get('selfie_photo_uuid'))
        has_assets = bool(review.get('assets'))

        if not has_selfie:
            review['ai_status'] = 'pending-selfie'
            review['ai_message'] = ''

        if review.get('admin_status') == 'pending':
            with api_tx('READ COMMITTED') as tx:
                person_row = tx.execute(
                    """
                    SELECT verification_required, verification_level_id
                    FROM person
                    WHERE id = %(person_id)s
                    """,
                    {'person_id': s.person_id},
                ).fetchone()
            if person_row and (not person_row['verification_required']) and int(person_row['verification_level_id'] or 0) > 1:
                review['admin_status'] = 'approved'
                if not (review.get('admin_message') or '').strip():
                    review['admin_message'] = 'Already verified'
            elif person_row and person_row['verification_required'] and not has_selfie and not has_assets:
                review['admin_message'] = ''

        if review.get('admin_status') == 'approved':
            updated_at = _parse_iso8601_timestamp(review.get('updated_at'))
            if updated_at and updated_at + timedelta(hours=24) < datetime.now(timezone.utc):
                review['admin_message'] = ''

        return review
    return {
        'id': None,
        'person_id': s.person_id,
        'selfie_photo_uuid': None,
        'selfie_photo_url': None,
        'ai_status': 'pending-selfie',
        'ai_message': '',
        'admin_status': 'pending',
        'admin_message': '',
        'created_at': None,
        'updated_at': None,
        'assets': [],
    }

def get_check_verification(s: t.SessionInfo):
    with api_tx() as tx:
        row = tx.execute(
            Q_CHECK_VERIFICATION,
            dict(person_id=s.person_id)
        ).fetchone()

    if row:
        return row
    return '', 400

def post_dismiss_donation(s: t.SessionInfo):
    with api_tx() as tx:
        tx.execute(Q_DISMISS_DONATION, dict(person_id=s.person_id))

@lru_cache()
def get_stats(ttl_hash=None):
    with api_tx('READ COMMITTED') as tx:
        row = tx.execute(Q_STATS).fetchone()
        return dict(row) if row is not None else {}

@lru_cache()
def get_gender_stats(ttl_hash=None):
    with api_tx('READ COMMITTED') as tx:
        row = tx.execute(Q_GENDER_STATS).fetchone()
        return dict(row) if row is not None else {}

def get_admin_ban_link(token: str):
    params = dict(token=token)

    err_invalid_token = (
        'Invalid token. User might have already been banned', 401)

    try:
        with api_tx() as tx:
            person_uuid = tx.execute(
                Q_ADMIN_TOKEN_TO_UUID,
                params,
            ).fetchone()['person_uuid']
    except TypeError:
        return err_invalid_token

    try:
        with api_tx('READ COMMITTED') as tx:
            rows = tx.execute(Q_CHECK_ADMIN_BAN_TOKEN, params).fetchall()
    except psycopg.errors.InvalidTextRepresentation:
        return err_invalid_token

    if rows:
        link = f'https://api.duolicious.app/admin/ban/{token}'
        return f'<a href="{link}">Click to confirm. Token: {token}</a>'
    else:
        return err_invalid_token

def get_admin_ban(token: str):
    rows = delete_or_ban_account(s=None, admin_ban_token=token)

    if rows:
        return f'Banned {rows}'
    else:
        return 'Ban failed; User already banned or token invalid', 401

def get_admin_delete_photo_link(token: str):
    params = dict(token=token)

    try:
        with api_tx('READ COMMITTED') as tx:
            tx.execute(Q_CHECK_ADMIN_DELETE_PHOTO_TOKEN, params)
            rows = tx.fetchall()
    except psycopg.errors.InvalidTextRepresentation:
        return 'Invalid token', 401

    if rows:
        link = f'https://api.duolicious.app/admin/delete-photo/{token}'
        return f'<a href="{link}">Click to confirm. Token {token}</a>'
    else:
        return 'Invalid token', 401

def get_admin_delete_photo(token: str):
    params = dict(token=token)

    with api_tx('READ COMMITTED') as tx:
        rows = tx.execute(Q_ADMIN_DELETE_PHOTO, params).fetchall()

        if rows:
            params = dict(person_id=rows[0]['person_id'])
            tx.execute(Q_UPDATE_VERIFICATION_LEVEL, params)

    if rows:
        return f'Deleted photo {rows}'
    else:
        return 'Photo deletion failed', 401


def get_admin_users():
    with api_tx('READ COMMITTED') as tx:
        rows = tx.execute(Q_ADMIN_LIST_USERS).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            trust_summary = _get_person_trust_summary(tx, item['id'])
            item['trust_score'] = trust_summary['trust_score']
            item['trust_level'] = trust_summary['level']
            result.append(item)
        return result


def get_admin_user(person_id: int):
    with api_tx('READ COMMITTED') as tx:
        row = tx.execute(Q_ADMIN_GET_USER, {'person_id': person_id}).fetchone()
        if row:
            result = dict(row)
            trust_summary = _get_person_trust_summary(tx, person_id)
            result['trust_score'] = trust_summary['trust_score']
            result['trust_level'] = trust_summary['level']
            return result

    if not row:
        return 'User not found', 404
    return row


def create_admin_user(data: dict):
    # This endpoint is intentionally permissive for local dev admin usage.
    email = data.get('email')
    name = data.get('name')
    about = data.get('about', 'Hi there!')
    date_of_birth = data.get('date_of_birth', '1990-01-01')

    if not email or not name:
        return 'Missing required fields email or name', 400

    normalized_email = normalize_email(email)

    with api_tx() as tx:
        if _person_exists_with_normalized_email(tx, normalized_email=normalized_email):
            return 'An account with this email already exists', 409

        params = {
            'email': email,
            'normalized_email': normalized_email,
            'name': name,
            'about': about,
            'date_of_birth': date_of_birth,
            'lon': data.get('lon', 0),
            'lat': data.get('lat', 0),
            'gender_id': data.get('gender_id', 1),
            'unit_id': data.get('unit_id', 1),
            'location_short_friendly': data.get('location_short_friendly', 'Unknown'),
            'location_long_friendly': data.get('location_long_friendly', 'Unknown'),
        }
        return tx.execute(Q_ADMIN_CREATE_USER, params).fetchone()


def update_admin_user(person_id: int, data: dict):
    normalized_email = normalize_email(data.get('email', '')) if data.get('email') else ''

    with api_tx() as tx:
        if normalized_email and _person_exists_with_normalized_email(
            tx,
            normalized_email=normalized_email,
            exclude_person_id=person_id,
        ):
            return 'Another account already uses this email', 409

        params = {
            'person_id': person_id,
            'name': data.get('name', ''),
            'email': data.get('email', ''),
            'normalized_email': normalized_email,
            'about': data.get('about', None),
            'activated': data.get('activated', None),
            'congregation_id': data.get('congregation_id', None),
            'gender': data.get('gender', ''),
            'date_of_birth': data.get('date_of_birth', None),
            'baptism_date': data.get('baptism_date', None),
            'country_of_birth': data.get('country_of_birth', None),
            'profile_status': data.get('profile_status', ''),
            'waitlist_status': data.get('waitlist_status', ''),
            'waitlist_note': data.get('waitlist_note', None),
            'invite_unlocked_at': data.get('invite_unlocked_at', None),
            'pioneer_status': data.get('pioneer_status', None),
            'verified_gender': data.get('verified_gender', None),
            'verified_age': data.get('verified_age', None),
            'verified_ethnicity': data.get('verified_ethnicity', None),
            'title_color': data.get('title_color', ''),
            'body_color': data.get('body_color', ''),
            'background_color': data.get('background_color', ''),
        }
        row = tx.execute(Q_ADMIN_UPDATE_USER, params).fetchone()
        if row:
            _refresh_antiabuse_flags(tx, person_id=person_id)

    if not row:
        return 'User not found', 404

    return row


def deactivate_admin_user(person_id: int):
    with api_tx() as tx:
        row = tx.execute(Q_ADMIN_DEACTIVATE_USER, {'person_id': person_id}).fetchone()

    if not row:
        return 'User not found', 404

    return {'id': person_id, 'deactivated': True}


def hard_ban_admin_user(person_id: int):
    with api_tx() as tx:
        row = tx.execute(Q_ADMIN_BAN_USER, {'person_id': person_id}).fetchone()

    if not row:
        return 'User not found', 404

    return {
        'id': person_id,
        'banned': True,
        'person_uuid': row['person_uuid'],
    }


def hard_delete_admin_user(person_id: int):
    with api_tx('READ COMMITTED') as tx:
        row = tx.execute(
            """
            SELECT
                id AS person_id,
                uuid::TEXT AS person_uuid
            FROM
                person
            WHERE
                id = %(person_id)s
            """,
            {'person_id': person_id},
        ).fetchone()

    if not row:
        return 'User not found', 404

    with api_tx() as tx:
        tx.execute('SET LOCAL statement_timeout = 30_000')
        tx.executemany(Q_DELETE_ACCOUNT, params_seq=[row])

    return {
        'id': person_id,
        'deleted': True,
    }


def get_admin_system_stats():
    with api_tx('READ COMMITTED') as tx:
        return tx.execute(Q_ADMIN_SYSTEM_STATS).fetchone()


def get_admin_antiabuse_flags():
    with api_tx('READ COMMITTED') as tx:
        return tx.execute(
            """
            SELECT
                antiabuse_flag.id,
                antiabuse_flag.person_id,
                person.uuid::TEXT AS person_uuid,
                person.name,
                person.email,
                antiabuse_flag.category,
                antiabuse_flag.severity,
                antiabuse_flag.reason,
                antiabuse_flag.evidence,
                antiabuse_flag.status,
                antiabuse_flag.resolution,
                antiabuse_flag.admin_note,
                antiabuse_flag.created_at::TEXT AS created_at,
                antiabuse_flag.updated_at::TEXT AS updated_at
            FROM antiabuse_flag
            LEFT JOIN person ON person.id = antiabuse_flag.person_id
            ORDER BY antiabuse_flag.status, antiabuse_flag.created_at DESC, antiabuse_flag.id DESC
            """,
        ).fetchall()


def update_admin_antiabuse_flag(
    flag_id: int,
    *,
    status: str,
    resolution: str | None,
    admin_note: str | None,
    resolved_by_person_id: int,
):
    if status not in {'open', 'reviewing', 'resolved', 'dismissed'}:
        return 'Invalid anti-abuse flag status', 400

    normalized_resolution = (resolution or 'none').strip()
    if normalized_resolution not in {
        'none',
        'warning',
        'temporary_restriction',
        'permanent_ban',
        'cleared',
    }:
        return 'Invalid anti-abuse resolution', 400

    with api_tx() as tx:
        existing = tx.execute(
            """
            SELECT id, person_id, status, resolution
            FROM antiabuse_flag
            WHERE id = %(flag_id)s
            """,
            {'flag_id': flag_id},
        ).fetchone()

        if not existing:
            return 'Anti-abuse flag not found', 404

        if existing['status'] in {'resolved', 'dismissed'} and status in {'open', 'reviewing'}:
            return 'Finalized anti-abuse decisions cannot move back to open or reviewing', 400

        if status == 'resolved' and normalized_resolution == 'none':
            return 'Choose a moderation outcome before resolving this flag', 400

        row = tx.execute(
            """
            UPDATE antiabuse_flag
            SET
                status = %(status)s,
                resolution = CASE
                    WHEN %(status)s = 'dismissed' THEN 'cleared'
                    WHEN %(status)s = 'resolved' THEN %(resolution)s
                    ELSE resolution
                END,
                admin_note = COALESCE(%(admin_note)s, admin_note),
                resolved_by_person_id = CASE
                    WHEN %(status)s IN ('resolved', 'dismissed')
                    THEN %(resolved_by_person_id)s
                    ELSE resolved_by_person_id
                END,
                updated_at = NOW()
            WHERE id = %(flag_id)s
            RETURNING id, status
            """,
            {
                'flag_id': flag_id,
                'status': status,
                'resolution': normalized_resolution,
                'admin_note': admin_note,
                'resolved_by_person_id': resolved_by_person_id,
            },
        ).fetchone()

        affected_person_id = int(existing['person_id']) if existing['person_id'] is not None else None

        if affected_person_id is not None and status == 'resolved':
            note = (admin_note or '').strip()

            if normalized_resolution == 'warning':
                tx.execute(
                    """
                    UPDATE person
                    SET
                        waitlist_status = CASE
                            WHEN waitlist_status = 'active' THEN 'pending'
                            ELSE waitlist_status
                        END,
                        waitlist_note = CASE
                            WHEN %(admin_note)s <> '' THEN %(admin_note)s
                            ELSE COALESCE(waitlist_note, 'Admin warning issued. Review required before full trust is restored.')
                        END
                    WHERE id = %(person_id)s
                    """,
                    {
                        'person_id': affected_person_id,
                        'admin_note': note,
                    },
                )
            elif normalized_resolution == 'temporary_restriction':
                tx.execute(
                    """
                    UPDATE person
                    SET
                        activated = FALSE,
                        profile_status = 'paused',
                        profile_status_changed_at = NOW(),
                        waitlist_status = 'blocked',
                        waitlist_note = CASE
                            WHEN %(admin_note)s <> '' THEN %(admin_note)s
                            ELSE 'Temporarily restricted after admin review.'
                        END
                    WHERE id = %(person_id)s
                    """,
                    {
                        'person_id': affected_person_id,
                        'admin_note': note,
                    },
                )
            elif normalized_resolution == 'permanent_ban':
                tx.execute(Q_ADMIN_BAN_USER, {'person_id': affected_person_id}).fetchone()

        if affected_person_id is not None:
            _refresh_antiabuse_flags(tx, person_id=affected_person_id)

    return {'id': row['id'], 'status': row['status']}


def get_admin_referrals():
    with api_tx('READ COMMITTED') as tx:
        rows = tx.execute(
            """
            SELECT
                referral_code.id,
                referral_code.code,
                referral_code.disabled,
                referral_code.replaced_at,
                referral_code.created_at,
                owner.id AS owner_id,
                owner.name AS owner_name,
                owner.email AS owner_email
            FROM referral_code
            JOIN person AS owner ON owner.id = referral_code.person_id
            WHERE referral_code.disabled = FALSE
            AND referral_code.replaced_at IS NULL
            ORDER BY referral_code.created_at DESC, referral_code.id DESC
            """
        ).fetchall()

        result = []
        for row in rows:
            trust_summary = _get_referral_trust_summary(tx, row['owner_id'])
            usage_count = _count_referral_uses(tx, row['id'])
            chain = tx.execute(
                """
                SELECT
                    person.id,
                    person.name,
                    'joined' AS status
                FROM person
                WHERE person.referred_by_code_id = %(code_id)s
                ORDER BY person.sign_up_time DESC, person.id DESC
                """,
                {'code_id': row['id']},
            ).fetchall()

            result.append({
                **dict(row),
                'usage_count': usage_count,
                'max_uses': trust_summary['invite_limit'],
                'trust_score': trust_summary['trust_score'],
                'trust_summary': trust_summary,
                'chain': [dict(item) for item in chain],
            })

        return result


def disable_admin_referral_code(code_id: int):
    with api_tx() as tx:
        replacement = _replace_referral_code(tx, code_id)

    if not replacement:
        return 'Referral code not found', 404

    return {
        'id': replacement['id'],
        'code': replacement['code'],
        'disabled': False,
    }


def get_admin_onboarding_steps():
    with api_tx('READ COMMITTED') as tx:
        return tx.execute(Q_ADMIN_ONBOARDING_STEPS).fetchall()


def create_admin_onboarding_step(data: dict):
    step_name = data.get('step_name')
    is_required = bool(data.get('is_required', False))
    ordinal = data.get('ordinal')

    if not step_name or ordinal is None:
        return 'Missing required fields step_name or ordinal', 400

    params = {
        'step_name': step_name,
        'is_required': is_required,
        'ordinal': ordinal,
    }

    with api_tx() as tx:
        return tx.execute(Q_ADMIN_CREATE_ONBOARDING_STEP, params).fetchone()


def update_admin_onboarding_step(step_id: int, data: dict):
    params = {
        'id': step_id,
        'step_name': data.get('step_name', ''),
        'is_required': data.get('is_required', None),
        'ordinal': data.get('ordinal', None),
    }

    with api_tx() as tx:
        row = tx.execute(Q_ADMIN_UPDATE_ONBOARDING_STEP, params).fetchone()

    if not row:
        return 'Step not found', 404

    return row


def delete_admin_onboarding_step(step_id: int):
    with api_tx() as tx:
        row = tx.execute(Q_ADMIN_DELETE_ONBOARDING_STEP, {'id': step_id}).fetchone()

    if not row:
        return 'Step not found', 404

    return {'id': step_id, 'deleted': True}


def _serialize_intro_gate_questions(tx, person_id: int) -> list[dict[str, Any]]:
    rows = tx.execute(
        """
        SELECT
            id,
            prompt,
            answer_type,
            response_mode,
            is_required,
            ordinal,
            is_active
        FROM intro_gate_question
        WHERE person_id = %(person_id)s
        ORDER BY ordinal, id
        """,
        {'person_id': person_id},
    ).fetchall()

    return [dict(row) for row in rows]


def _serialize_intro_request(tx, request_id: int, viewer_person_id: int) -> dict[str, Any] | None:
    row = tx.execute(
        """
        SELECT
            intro_request.id,
            intro_request.from_person_id,
            intro_request.to_person_id,
            intro_request.status,
            intro_request.note,
            intro_request.reason_for_reaching_out,
            intro_request.why_we_may_match,
            intro_request.voice_note_audio_uuid,
            intro_request.first_call_status,
            intro_request.first_call_timing,
            intro_request.first_call_plan,
            intro_request.ready_for_video_call,
            intro_request.ready_for_family_introduction,
            intro_request.courtship_prompt_dismissed_at::TEXT AS courtship_prompt_dismissed_at,
            intro_request.accepted_at::TEXT AS accepted_at,
            intro_request.rejected_at::TEXT AS rejected_at,
            intro_request.created_at::TEXT AS created_at,
            intro_request.updated_at::TEXT AS updated_at,
            from_person.uuid::TEXT AS from_person_uuid,
            from_person.name AS from_person_name,
            to_person.uuid::TEXT AS to_person_uuid,
            to_person.name AS to_person_name,
            CASE
                WHEN intro_request.from_person_id = %(viewer_person_id)s THEN 'requester'
                WHEN intro_request.to_person_id = %(viewer_person_id)s THEN 'recipient'
                ELSE 'viewer'
            END AS role
        FROM intro_request
        JOIN person AS from_person ON from_person.id = intro_request.from_person_id
        JOIN person AS to_person ON to_person.id = intro_request.to_person_id
        WHERE intro_request.id = %(request_id)s
        """,
        {
            'request_id': request_id,
            'viewer_person_id': viewer_person_id,
        },
    ).fetchone()

    if not row:
        return None

    answers = tx.execute(
        """
        SELECT
            id,
            question_id,
            prompt_snapshot,
            answer_type,
            response_mode,
            answer_text,
            answer_bool,
            answer_audio_uuid,
            ordinal
        FROM intro_request_answer
        WHERE request_id = %(request_id)s
        ORDER BY ordinal, id
        """,
        {'request_id': request_id},
    ).fetchall()

    payload = dict(row)
    payload['voice_note_audio_data_url'] = _intro_request_audio_data_url(
        row.get('voice_note_audio_uuid'),
    )
    payload['answers'] = [
        {
            **dict(answer),
            'answer_audio_data_url': _intro_request_audio_data_url(dict(answer).get('answer_audio_uuid')),
        }
        for answer in answers
    ]
    payload['questions'] = _serialize_intro_gate_questions(
        tx,
        row['to_person_id'],
    )
    payload['can_start_intro'] = bool(
        row['status'] == 'accepted' and row['from_person_id'] == viewer_person_id
    )
    return payload


def _find_intro_request_between(tx, *, person_id: int, prospect_uuid: str):
    return tx.execute(
        """
        WITH prospect AS (
            SELECT id, uuid::TEXT AS person_uuid, name
            FROM person
            WHERE uuid = uuid_or_null(%(prospect_uuid)s)
        )
        SELECT
            intro_request.id,
            intro_request.from_person_id,
            intro_request.to_person_id,
            intro_request.status,
            intro_request.first_call_status,
            intro_request.first_call_timing,
            intro_request.first_call_plan,
            intro_request.ready_for_video_call,
            intro_request.ready_for_family_introduction,
            intro_request.courtship_prompt_dismissed_at::TEXT AS courtship_prompt_dismissed_at,
            intro_request.accepted_at::TEXT AS accepted_at,
            intro_request.updated_at::TEXT AS updated_at,
            prospect.id AS prospect_id,
            prospect.person_uuid,
            prospect.name,
            CASE
                WHEN intro_request.from_person_id = %(person_id)s THEN 'requester'
                WHEN intro_request.to_person_id = %(person_id)s THEN 'recipient'
                ELSE 'viewer'
            END AS role
        FROM intro_request
        JOIN prospect
          ON (
                intro_request.from_person_id = %(person_id)s
            AND intro_request.to_person_id = prospect.id
          ) OR (
                intro_request.from_person_id = prospect.id
            AND intro_request.to_person_id = %(person_id)s
          )
        ORDER BY intro_request.updated_at DESC
        LIMIT 1
        """,
        {
            'person_id': person_id,
            'prospect_uuid': prospect_uuid,
        },
    ).fetchone()


def _get_intro_message_count(tx, *, owner_person_uuid: str, prospect_uuid: str) -> int:
    row = tx.execute(
        """
        SELECT COUNT(*) AS count
        FROM mam_message
        WHERE
            person_id = (
                SELECT id
                FROM person
                WHERE uuid = uuid_or_null(%(owner_person_uuid)s)
                LIMIT 1
            )
        AND remote_bare_jid = %(prospect_uuid)s
        """,
        {
            'owner_person_uuid': owner_person_uuid,
            'prospect_uuid': prospect_uuid,
        },
    ).fetchone()

    return int(row['count'] or 0) if row else 0


def _courtship_milestone_payload(row: dict[str, Any], message_count: int) -> dict[str, str]:
    status = row.get('status') or 'pending'
    first_call_status = row.get('first_call_status') or 'not-planned'
    ready_for_video_call = bool(row.get('ready_for_video_call'))
    ready_for_family_introduction = bool(row.get('ready_for_family_introduction'))

    if status == 'rejected':
        return {'key': 'closed', 'label': 'Closed'}
    if status != 'accepted':
        if status == 'pending':
            return {'key': 'request-pending', 'label': 'Interest request pending'}
        return {'key': 'request-stage', 'label': 'Interest request in progress'}
    if ready_for_family_introduction:
        return {'key': 'family-introduction', 'label': 'Ready for family introduction'}
    if ready_for_video_call:
        return {'key': 'video-call', 'label': 'Ready for video call'}
    if first_call_status == 'completed':
        return {'key': 'post-call', 'label': 'First call completed'}
    if first_call_status == 'scheduled':
        return {'key': 'call-scheduled', 'label': 'First call scheduled'}
    if first_call_status == 'planning':
        return {'key': 'call-planning', 'label': 'Planning first call'}
    if message_count >= 1:
        return {'key': 'getting-to-know', 'label': 'Getting to know each other'}
    return {'key': 'chat-open', 'label': 'Chat open'}


def _courtship_check_in_prompt(row: dict[str, Any], message_count: int) -> str:
    if row.get('status') != 'accepted':
        return ''

    if row.get('courtship_prompt_dismissed_at'):
        return ''

    first_call_status = row.get('first_call_status') or 'not-planned'

    if message_count >= 6 and first_call_status == 'not-planned':
        return 'You have chatted a bit now. Consider planning a first call so the conversation can move forward intentionally.'

    if message_count >= 12 and not row.get('ready_for_video_call'):
        return 'This may be a good time to ask whether you both feel ready for a video call.'

    if message_count >= 20 and bool(row.get('ready_for_video_call')) and not row.get('ready_for_family_introduction'):
        return 'If things still feel steady, you may want to talk about whether you are ready for family introduction.'

    return ''


def get_courtship_state(s: t.SessionInfo, prospect_uuid: str):
    if not s.person_uuid:
        return 'Authentication required', 401

    with api_tx('READ COMMITTED') as tx:
        row = _find_intro_request_between(
            tx,
            person_id=s.person_id,
            prospect_uuid=prospect_uuid,
        )

        if not row:
            return {
                'exists': False,
                'status': 'ready',
                'role': 'viewer',
                'message_count': 0,
                'milestone_key': 'request',
                'milestone_label': 'Interest request not started',
                'check_in_prompt': '',
                'can_edit': False,
            }

        payload = dict(row)
        message_count = _get_intro_message_count(
            tx,
            owner_person_uuid=s.person_uuid,
            prospect_uuid=prospect_uuid,
        )
        milestone = _courtship_milestone_payload(payload, message_count)

        return {
            'exists': True,
            'request_id': int(payload['id']),
            'status': payload['status'],
            'role': payload['role'],
            'first_call_status': payload.get('first_call_status') or 'not-planned',
            'first_call_timing': payload.get('first_call_timing') or '',
            'first_call_plan': payload.get('first_call_plan') or '',
            'ready_for_video_call': bool(payload.get('ready_for_video_call')),
            'ready_for_family_introduction': bool(payload.get('ready_for_family_introduction')),
            'accepted_at': payload.get('accepted_at'),
            'updated_at': payload.get('updated_at'),
            'message_count': message_count,
            'milestone_key': milestone['key'],
            'milestone_label': milestone['label'],
            'check_in_prompt': _courtship_check_in_prompt(payload, message_count),
            'can_edit': payload.get('status') == 'accepted',
        }


def patch_courtship_state(req: t.PatchCourtshipState, s: t.SessionInfo, prospect_uuid: str):
    with api_tx() as tx:
        row = _find_intro_request_between(
            tx,
            person_id=s.person_id,
            prospect_uuid=prospect_uuid,
        )

        if not row:
            return 'Interest request not found', 404

        if row['status'] != 'accepted':
            return 'Courtship tools unlock after the request is accepted', 400

        updates: list[str] = []
        params: dict[str, Any] = {'request_id': row['id']}

        if 'first_call_status' in req.__pydantic_fields_set__:
            updates.append("first_call_status = %(first_call_status)s")
            params['first_call_status'] = req.first_call_status or 'not-planned'

        if 'first_call_timing' in req.__pydantic_fields_set__:
            updates.append("first_call_timing = %(first_call_timing)s")
            params['first_call_timing'] = req.first_call_timing or ''

        if 'first_call_plan' in req.__pydantic_fields_set__:
            updates.append("first_call_plan = %(first_call_plan)s")
            params['first_call_plan'] = req.first_call_plan or ''

        if 'ready_for_video_call' in req.__pydantic_fields_set__:
            updates.append("ready_for_video_call = %(ready_for_video_call)s")
            params['ready_for_video_call'] = bool(req.ready_for_video_call)

        if 'ready_for_family_introduction' in req.__pydantic_fields_set__:
            updates.append("ready_for_family_introduction = %(ready_for_family_introduction)s")
            params['ready_for_family_introduction'] = bool(req.ready_for_family_introduction)

        if req.dismiss_prompt:
            updates.append("courtship_prompt_dismissed_at = NOW()")
        elif 'dismiss_prompt' in req.__pydantic_fields_set__:
            updates.append("courtship_prompt_dismissed_at = NULL")

        if not updates:
            return 'Nothing to update', 400

        tx.execute(
            f"""
            UPDATE intro_request
            SET
                {', '.join(updates)},
                updated_at = NOW()
            WHERE id = %(request_id)s
            """,
            params,
        )
        refreshed = _find_intro_request_between(
            tx,
            person_id=s.person_id,
            prospect_uuid=prospect_uuid,
        )

        if not refreshed:
            return 'Interest request not found', 404

        payload = dict(refreshed)
        message_count = _get_intro_message_count(
            tx,
            owner_person_uuid=s.person_uuid,
            prospect_uuid=prospect_uuid,
        )
        milestone = _courtship_milestone_payload(payload, message_count)

        return {
            'exists': True,
            'request_id': int(payload['id']),
            'status': payload['status'],
            'role': payload['role'],
            'first_call_status': payload.get('first_call_status') or 'not-planned',
            'first_call_timing': payload.get('first_call_timing') or '',
            'first_call_plan': payload.get('first_call_plan') or '',
            'ready_for_video_call': bool(payload.get('ready_for_video_call')),
            'ready_for_family_introduction': bool(payload.get('ready_for_family_introduction')),
            'accepted_at': payload.get('accepted_at'),
            'updated_at': payload.get('updated_at'),
            'message_count': message_count,
            'milestone_key': milestone['key'],
            'milestone_label': milestone['label'],
            'check_in_prompt': _courtship_check_in_prompt(payload, message_count),
            'can_edit': payload.get('status') == 'accepted',
        }


def get_intro_gate_questions(s: t.SessionInfo):
    with api_tx('READ COMMITTED') as tx:
        return {
            'questions': _serialize_intro_gate_questions(tx, s.person_id),
        }


def _upsert_intro_gate_questions(
    tx,
    person_id: int,
    req: t.PutIntroGateQuestions,
):
    cleaned_questions = []
    for index, item in enumerate(req.questions):
        prompt = item.prompt.strip()
        if not prompt:
            continue
        cleaned_questions.append({
            'id': item.id,
            'prompt': prompt,
            'answer_type': item.answer_type,
            'response_mode': (
                item.response_mode
                if item.answer_type == 'free_text'
                else 'text'
            ),
            'is_required': bool(item.is_required),
            'ordinal': index,
            'is_active': bool(item.is_active),
        })

    existing_ids = {
        int(row['id'])
        for row in tx.execute(
            """
            SELECT id
            FROM intro_gate_question
            WHERE person_id = %(person_id)s
            """,
            {'person_id': person_id},
        ).fetchall()
    }

    kept_ids: set[int] = set()
    for question in cleaned_questions:
        if question['id'] and int(question['id']) in existing_ids:
            kept_ids.add(int(question['id']))
            tx.execute(
                """
                UPDATE intro_gate_question
                SET
                    prompt = %(prompt)s,
                    answer_type = %(answer_type)s,
                    response_mode = %(response_mode)s,
                    is_required = %(is_required)s,
                    ordinal = %(ordinal)s,
                    is_active = %(is_active)s,
                    updated_at = NOW()
                WHERE id = %(id)s
                AND person_id = %(person_id)s
                """,
                {
                    **question,
                    'person_id': person_id,
                },
            )
        else:
            row = tx.execute(
                """
                INSERT INTO intro_gate_question (
                    person_id,
                    prompt,
                    answer_type,
                    response_mode,
                    is_required,
                    ordinal,
                    is_active
                )
                VALUES (
                    %(person_id)s,
                    %(prompt)s,
                    %(answer_type)s,
                    %(response_mode)s,
                    %(is_required)s,
                    %(ordinal)s,
                    %(is_active)s
                )
                RETURNING id
                """,
                {
                    **question,
                    'person_id': person_id,
                },
            ).fetchone()
            kept_ids.add(int(row['id']))

    delete_ids = [question_id for question_id in existing_ids if question_id not in kept_ids]
    if delete_ids:
        tx.execute(
            """
            DELETE FROM intro_gate_question
            WHERE person_id = %(person_id)s
            AND id = ANY(%(delete_ids)s)
            """,
            {
                'person_id': person_id,
                'delete_ids': delete_ids,
            },
        )

    return {
        'questions': _serialize_intro_gate_questions(tx, person_id),
    }


def put_intro_gate_questions(req: t.PutIntroGateQuestions, s: t.SessionInfo):
    with api_tx() as tx:
        return _upsert_intro_gate_questions(tx, s.person_id, req)


def admin_put_intro_gate_questions(person_id: int, req: t.PutIntroGateQuestions):
    with api_tx() as tx:
        return {
            'person_id': person_id,
            **_upsert_intro_gate_questions(tx, person_id, req),
        }


def get_intro_request_state(s: t.SessionInfo, prospect_uuid: str):
    params = {
        'person_id': s.person_id,
        'prospect_uuid': prospect_uuid,
    }

    with api_tx('READ COMMITTED') as tx:
        prospect = tx.execute(
            """
            SELECT
                id,
                uuid::TEXT AS person_uuid,
                name,
                who_can_contact_me,
                request_format_preference,
                message_pace_preference
            FROM person
            WHERE uuid = uuid_or_null(%(prospect_uuid)s)
            """,
            params,
        ).fetchone()

        if not prospect:
            return 'Prospect not found', 404

        request_row = tx.execute(
            """
            SELECT id
            FROM intro_request
            WHERE
                (from_person_id = %(person_id)s AND to_person_id = %(prospect_id)s)
             OR (from_person_id = %(prospect_id)s AND to_person_id = %(person_id)s)
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            {
                'person_id': s.person_id,
                'prospect_id': prospect['id'],
            },
        ).fetchone()

        serialized_request = (
            _serialize_intro_request(tx, int(request_row['id']), s.person_id)
            if request_row else None
        )

        return {
            'prospect_uuid': prospect['person_uuid'],
            'prospect_name': prospect['name'],
            'who_can_contact_me': prospect['who_can_contact_me'],
            'request_format_preference': prospect['request_format_preference'],
            'message_pace_preference': prospect['message_pace_preference'],
            'request': serialized_request,
            'questions': _serialize_intro_gate_questions(tx, int(prospect['id'])),
        }


def post_intro_request(req: t.PostIntroRequest, s: t.SessionInfo, prospect_uuid: str):
    params = {
        'person_id': s.person_id,
        'prospect_uuid': prospect_uuid,
    }

    audio_uploads: list[tuple[str, bytes]] = []
    voice_note_audio_uuid: str | None = None

    with api_tx() as tx:
        prospect = tx.execute(
            """
            SELECT
                id,
                who_can_contact_me,
                request_format_preference
            FROM person
            WHERE uuid = uuid_or_null(%(prospect_uuid)s)
            """,
            params,
        ).fetchone()

        if not prospect:
            return 'Prospect not found', 404

        if int(prospect['id']) == s.person_id:
            return 'You cannot send a request to yourself', 400

        sender_state = tx.execute(
            """
            SELECT
                verification_level_id,
                profile_status
            FROM person
            WHERE id = %(person_id)s
            """,
            {'person_id': s.person_id},
        ).fetchone()

        sender_verification_level = int(sender_state['verification_level_id'] or 0)
        sender_profile_status = sender_state['profile_status'] or 'active'

        who_can_contact_me = prospect.get('who_can_contact_me') or 'Anyone who matches my filters'
        request_format_preference = prospect.get('request_format_preference') or 'Thoughtful written request'

        if who_can_contact_me == 'Nobody new right now':
            return 'This person is not taking new interest requests right now.', 400

        if who_can_contact_me == 'Verified members only' and sender_verification_level <= 1:
            return 'This person only accepts interest requests from verified members.', 400

        if (
            who_can_contact_me == 'Serious and verified members only'
            and (sender_verification_level <= 1 or sender_profile_status != 'serious')
        ):
            return 'This person only accepts requests from serious, verified members.', 400

        questions = _serialize_intro_gate_questions(tx, int(prospect['id']))
        active_questions = [question for question in questions if question['is_active']]
        incoming_answers = req.answers or []

        if len(incoming_answers) < len([question for question in active_questions if question['is_required']]):
            return 'Please answer the required private questions.', 400

        normalized_answers: list[dict[str, Any]] = []
        for index, question in enumerate(active_questions):
            matching_answer = next(
                (
                    answer for answer in incoming_answers
                    if answer.question_id == question['id']
                ),
                incoming_answers[index] if index < len(incoming_answers) else None,
            )

            if not matching_answer:
                if question['is_required']:
                    return 'Please answer every required private question.', 400
                continue

            if question['answer_type'] == 'yes_no':
                if matching_answer.answer_bool is None:
                    return 'Please answer each yes/no question.', 400
                answer_text = ''
                answer_bool = bool(matching_answer.answer_bool)
                response_mode = 'text'
                answer_audio_uuid = None
            else:
                response_mode = question.get('response_mode') or 'text'
                answer_text = (matching_answer.answer_text or '').strip()
                answer_bool = None
                answer_audio_uuid = None

                if response_mode in {'text', 'both'} and question['is_required'] and not answer_text:
                    return 'Please answer every required private question.', 400

                if response_mode in {'voice', 'both'}:
                    if not matching_answer.base64_audio_file:
                        if question['is_required']:
                            return 'Please record every required voice answer.', 400
                    else:
                        answer_audio_uuid = secrets.token_hex(16)
                        audio_uploads.append((
                            answer_audio_uuid,
                            matching_answer.base64_audio_file.transcoded,
                        ))

                if response_mode == 'voice':
                    answer_text = ''

            normalized_answers.append({
                'question_id': question['id'],
                'prompt_snapshot': question['prompt'],
                'answer_type': question['answer_type'],
                'response_mode': response_mode,
                'answer_text': answer_text,
                'answer_bool': answer_bool,
                'answer_audio_uuid': answer_audio_uuid,
                'ordinal': question['ordinal'],
            })

        if req.base64_audio_file:
            voice_note_audio_uuid = secrets.token_hex(16)
            audio_uploads.append((
                voice_note_audio_uuid,
                req.base64_audio_file.transcoded,
            ))

        reason_for_reaching_out = (req.reason_for_reaching_out or '').strip()
        why_we_may_match = (req.why_we_may_match or '').strip()

        if request_format_preference == 'Thoughtful written request':
            if not reason_for_reaching_out or not why_we_may_match:
                return 'This person asks for both a reason for reaching out and why you may match.', 400

        if request_format_preference == 'Voice note required' and not voice_note_audio_uuid:
            return 'This person asks for a voice note with the request.', 400

        has_answer_content = any(
            answer['answer_bool'] is not None
            or bool(answer['answer_text'])
            or bool(answer['answer_audio_uuid'])
            for answer in normalized_answers
        )

        if not reason_for_reaching_out and not why_we_may_match and not voice_note_audio_uuid and not has_answer_content:
            return 'Please add a reason, voice note, or private answer before sending this request.', 400

        row = tx.execute(
            """
            INSERT INTO intro_request (
                from_person_id,
                to_person_id,
                status,
                note,
                reason_for_reaching_out,
                why_we_may_match,
                voice_note_audio_uuid,
                accepted_at,
                rejected_at,
                updated_at
            )
            VALUES (
                %(from_person_id)s,
                %(to_person_id)s,
                'pending',
                %(note)s,
                %(reason_for_reaching_out)s,
                %(why_we_may_match)s,
                %(voice_note_audio_uuid)s,
                NULL,
                NULL,
                NOW()
            )
            ON CONFLICT (from_person_id, to_person_id) DO UPDATE
            SET
                status = 'pending',
                note = EXCLUDED.note,
                reason_for_reaching_out = EXCLUDED.reason_for_reaching_out,
                why_we_may_match = EXCLUDED.why_we_may_match,
                voice_note_audio_uuid = EXCLUDED.voice_note_audio_uuid,
                accepted_at = NULL,
                rejected_at = NULL,
                updated_at = NOW()
            RETURNING id
            """,
            {
                'from_person_id': s.person_id,
                'to_person_id': int(prospect['id']),
                'note': req.note or '',
                'reason_for_reaching_out': reason_for_reaching_out,
                'why_we_may_match': why_we_may_match,
                'voice_note_audio_uuid': voice_note_audio_uuid,
            },
        ).fetchone()

        request_id = int(row['id'])
        tx.execute(
            """
            DELETE FROM intro_request_answer
            WHERE request_id = %(request_id)s
            """,
            {'request_id': request_id},
        )

        for answer in normalized_answers:
            tx.execute(
                """
                INSERT INTO intro_request_answer (
                    request_id,
                    question_id,
                    prompt_snapshot,
                    answer_type,
                    response_mode,
                    answer_text,
                    answer_bool,
                    answer_audio_uuid,
                    ordinal
                )
                VALUES (
                    %(request_id)s,
                    %(question_id)s,
                    %(prompt_snapshot)s,
                    %(answer_type)s,
                    %(response_mode)s,
                    %(answer_text)s,
                    %(answer_bool)s,
                    %(answer_audio_uuid)s,
                    %(ordinal)s
                )
                """,
                {
                    **answer,
                    'request_id': request_id,
                },
            )

        serialized = _serialize_intro_request(tx, request_id, s.person_id)

    for audio_uuid, transcoded_audio in audio_uploads:
        put_audio_in_object_store(uuid=audio_uuid, audio_file_bytes=transcoded_audio)

    return serialized


def accept_intro_request(s: t.SessionInfo, prospect_uuid: str):
    with api_tx() as tx:
        row = tx.execute(
            """
            WITH prospect AS (
                SELECT id
                FROM person
                WHERE uuid = uuid_or_null(%(prospect_uuid)s)
            )
            UPDATE intro_request
            SET
                status = 'accepted',
                accepted_at = NOW(),
                rejected_at = NULL,
                updated_at = NOW()
            WHERE
                from_person_id = (SELECT id FROM prospect)
            AND
                to_person_id = %(person_id)s
            AND
                status = 'pending'
            RETURNING id
            """,
            {
                'person_id': s.person_id,
                'prospect_uuid': prospect_uuid,
            },
        ).fetchone()

        if not row:
            return 'Introduction request not found', 404

        return _serialize_intro_request(tx, int(row['id']), s.person_id)


def reject_intro_request(s: t.SessionInfo, prospect_uuid: str):
    with api_tx() as tx:
        row = tx.execute(
            """
            WITH prospect AS (
                SELECT id
                FROM person
                WHERE uuid = uuid_or_null(%(prospect_uuid)s)
            )
            UPDATE intro_request
            SET
                status = 'rejected',
                rejected_at = NOW(),
                updated_at = NOW()
            WHERE
                from_person_id = (SELECT id FROM prospect)
            AND
                to_person_id = %(person_id)s
            AND
                status = 'pending'
            RETURNING id
            """,
            {
                'person_id': s.person_id,
                'prospect_uuid': prospect_uuid,
            },
        ).fetchone()

        if not row:
            return 'Introduction request not found', 404

        return _serialize_intro_request(tx, int(row['id']), s.person_id)


def get_intro_requests(s: t.SessionInfo):
    with api_tx('READ COMMITTED') as tx:
        rows = tx.execute(
            """
            SELECT
                intro_request.id,
                intro_request.status,
                intro_request.note,
                intro_request.created_at::TEXT AS created_at,
                intro_request.updated_at::TEXT AS updated_at,
                from_person.uuid::TEXT AS from_person_uuid,
                from_person.name AS from_person_name,
                to_person.uuid::TEXT AS to_person_uuid,
                to_person.name AS to_person_name,
                CASE
                    WHEN intro_request.from_person_id = %(person_id)s THEN 'outgoing'
                    ELSE 'incoming'
                END AS direction
            FROM intro_request
            JOIN person AS from_person ON from_person.id = intro_request.from_person_id
            JOIN person AS to_person ON to_person.id = intro_request.to_person_id
            WHERE
                intro_request.from_person_id = %(person_id)s
             OR intro_request.to_person_id = %(person_id)s
            ORDER BY intro_request.updated_at DESC
            LIMIT 50
            """,
            {'person_id': s.person_id},
        ).fetchall()

        return {
            'requests': [dict(row) for row in rows],
        }


def get_intro_review(s: t.SessionInfo, prospect_uuid: str):
    params = {
        'person_id': s.person_id,
        'prospect_uuid': prospect_uuid,
    }

    with api_tx('READ COMMITTED') as tx:
        row = tx.execute(
            """
            WITH prospect AS (
                SELECT id, uuid::TEXT AS person_uuid, name
                FROM person
                WHERE uuid = uuid_or_null(%(prospect_uuid)s)
            )
            SELECT
                intro_review.id,
                intro_review.from_person_id,
                intro_review.to_person_id,
                intro_review.status,
                intro_review.prompt,
                intro_review.round_count,
                intro_review.created_at::TEXT AS created_at,
                intro_review.updated_at::TEXT AS updated_at,
                prospect.person_uuid,
                prospect.name,
                CASE
                    WHEN intro_review.from_person_id = %(person_id)s THEN 'sender'
                    WHEN intro_review.to_person_id = %(person_id)s THEN 'recipient'
                    ELSE 'viewer'
                END AS role
            FROM intro_review
            JOIN prospect
              ON (
                    intro_review.from_person_id = %(person_id)s
                AND intro_review.to_person_id = prospect.id
              ) OR (
                    intro_review.from_person_id = prospect.id
                AND intro_review.to_person_id = %(person_id)s
              )
            ORDER BY intro_review.updated_at DESC
            LIMIT 1
            """,
            params,
        ).fetchone()

    return dict(row) if row else {
        'id': None,
        'status': 'ready',
        'prompt': '',
        'round_count': 0,
        'role': 'sender',
        'person_uuid': prospect_uuid,
    }


def request_more_intro_review(
    s: t.SessionInfo,
    prospect_uuid: str,
    prompt: str | None,
):
    with api_tx() as tx:
        row = tx.execute(
            """
            WITH prospect AS (
                SELECT id
                FROM person
                WHERE uuid = uuid_or_null(%(prospect_uuid)s)
            )
            UPDATE intro_review
            SET
                status = 'needs_more_answers',
                prompt = COALESCE(NULLIF(%(prompt)s, ''), prompt),
                updated_at = NOW()
            WHERE
                from_person_id = (SELECT id FROM prospect)
            AND
                to_person_id = %(person_id)s
            AND
                status IN ('pending', 'needs_more_answers')
            RETURNING id
            """,
            {
                'person_id': s.person_id,
                'prospect_uuid': prospect_uuid,
                'prompt': prompt,
            },
        ).fetchone()

    if not row:
        return 'Introduction request not found', 404

    return {'id': row['id'], 'status': 'needs_more_answers'}


def accept_intro_review(s: t.SessionInfo, prospect_uuid: str):
    with api_tx() as tx:
        row = tx.execute(
            """
            WITH prospect AS (
                SELECT id
                FROM person
                WHERE uuid = uuid_or_null(%(prospect_uuid)s)
            )
            UPDATE intro_review
            SET
                status = 'accepted',
                prompt = '',
                accepted_at = NOW(),
                updated_at = NOW()
            WHERE
                from_person_id = (SELECT id FROM prospect)
            AND
                to_person_id = %(person_id)s
            AND
                status IN ('pending', 'needs_more_answers')
            RETURNING id
            """,
            {
                'person_id': s.person_id,
                'prospect_uuid': prospect_uuid,
            },
        ).fetchone()

    if not row:
        return 'Introduction request not found', 404

    return {'id': row['id'], 'status': 'accepted'}


def reject_intro_review(s: t.SessionInfo, prospect_uuid: str):
    with api_tx() as tx:
        row = tx.execute(
            """
            WITH prospect AS (
                SELECT id
                FROM person
                WHERE uuid = uuid_or_null(%(prospect_uuid)s)
            ), updated_review AS (
                UPDATE intro_review
                SET
                    status = 'rejected',
                    rejected_at = NOW(),
                    updated_at = NOW()
                WHERE
                    from_person_id = (SELECT id FROM prospect)
                AND
                    to_person_id = %(person_id)s
                AND
                    status IN ('pending', 'needs_more_answers')
                RETURNING id, from_person_id
            ), inserted_skip AS (
                INSERT INTO skipped (subject_person_id, object_person_id, reported, report_reason)
                SELECT %(person_id)s, from_person_id, FALSE, ''
                FROM updated_review
                ON CONFLICT DO NOTHING
            )
            SELECT id
            FROM updated_review
            """,
            {
                'person_id': s.person_id,
                'prospect_uuid': prospect_uuid,
            },
        ).fetchone()

    if not row:
        return 'Introduction request not found', 404

    return {'id': row['id'], 'status': 'rejected'}


def create_external_report(req: t.PostExternalReport):
    photo_uuid = None
    crop_size = None

    if req.base64_file is not None:
        photo_uuid = secrets.token_hex(32)
        crop_size = CropSize(
            top=req.base64_file.top,
            left=req.base64_file.left,
        )

    with api_tx() as tx:
        row = tx.execute(
            """
            INSERT INTO external_report (
                reporter_name,
                reporter_email,
                relationship_to_user,
                target_name,
                target_email,
                target_profile_url,
                claim,
                evidence_details,
                photo_uuid
            )
            VALUES (
                %(reporter_name)s,
                %(reporter_email)s,
                %(relationship_to_user)s,
                %(target_name)s,
                %(target_email)s,
                %(target_profile_url)s,
                %(claim)s,
                %(evidence_details)s,
                %(photo_uuid)s
            )
            RETURNING id
            """,
            {
                'reporter_name': req.reporter_name,
                'reporter_email': req.reporter_email,
                'relationship_to_user': req.relationship_to_user,
                'target_name': req.target_name or '',
                'target_email': req.target_email or '',
                'target_profile_url': req.target_profile_url or '',
                'claim': req.claim,
                'evidence_details': req.evidence_details or '',
                'photo_uuid': photo_uuid,
            },
        ).fetchone()

        matched_person = tx.execute(
            """
            SELECT
                id
            FROM person
            WHERE
                (%(target_email)s <> '' AND lower(email) = lower(%(target_email)s))
            OR
                (%(target_profile_url)s <> '' AND position(uuid::TEXT in %(target_profile_url)s) > 0)
            LIMIT 1
            """,
            {
                'target_email': req.target_email or '',
                'target_profile_url': req.target_profile_url or '',
            },
        ).fetchone()

        if matched_person:
            _create_or_touch_antiabuse_flag(
                tx,
                person_id=int(matched_person['id']),
                category='external-report',
                severity='high',
                reason='An outside report with evidence was submitted for this account.',
                evidence={
                    'external_report_id': int(row['id']),
                    'reporter_email': req.reporter_email,
                    'claim': req.claim,
                },
            )

    if req.base64_file is not None and photo_uuid and crop_size:
        try:
            put_image_in_object_store(photo_uuid, req.base64_file, crop_size, sizes=[450])
        except Exception as e:
            print('External report upload failed with exception:', e)
            return '', 500

    return {'id': row['id'], 'status': 'submitted'}


def get_admin_external_reports():
    with api_tx('READ COMMITTED') as tx:
        return tx.execute(
            """
            SELECT
                id,
                reporter_name,
                reporter_email,
                relationship_to_user,
                target_name,
                target_email,
                target_profile_url,
                claim,
                evidence_details,
                photo_uuid,
                CASE
                    WHEN photo_uuid IS NOT NULL
                    THEN %(images_url)s || '/450-' || photo_uuid || '.jpg'
                    ELSE NULL
                END AS photo_url,
                status,
                admin_note,
                reviewed_by_person_id,
                created_at::TEXT AS created_at,
                updated_at::TEXT AS updated_at
            FROM external_report
            ORDER BY created_at DESC, id DESC
            """,
            {'images_url': os.environ.get('DUO_IMAGES_BASE_URL', 'https://user-images.duolicious.app')},
        ).fetchall()


def update_admin_external_report(
    report_id: int,
    *,
    status: str,
    admin_note: str | None,
    reviewed_by_person_id: int,
):
    if status not in {'new', 'reviewing', 'resolved', 'dismissed'}:
        return 'Invalid external report status', 400

    with api_tx() as tx:
        existing = tx.execute(
            """
            SELECT id, status
            FROM external_report
            WHERE id = %(report_id)s
            """,
            {'report_id': report_id},
        ).fetchone()

        if not existing:
            return 'External report not found', 404

        if existing['status'] in {'resolved', 'dismissed'} and status in {'new', 'reviewing'}:
            return 'Finalized external reports cannot move back to new or reviewing', 400

        row = tx.execute(
            """
            UPDATE external_report
            SET
                status = %(status)s,
                admin_note = COALESCE(%(admin_note)s, admin_note),
                reviewed_by_person_id = %(reviewed_by_person_id)s,
                updated_at = NOW()
            WHERE
                id = %(report_id)s
            RETURNING id, status
            """,
            {
                'report_id': report_id,
                'status': status,
                'admin_note': admin_note,
                'reviewed_by_person_id': reviewed_by_person_id,
            },
        ).fetchone()

    return {'id': row['id'], 'status': row['status']}


def get_admin_settings():
    with api_tx('READ COMMITTED') as tx:
        return tx.execute(Q_ADMIN_LIST_SETTINGS).fetchall()


def get_admin_support_thread(s: t.SessionInfo):
    with api_tx() as tx:
        tx.execute(
            """
            INSERT INTO admin_support_thread (
                person_id,
                status,
                last_message_at,
                last_read_by_user_at,
                created_at,
                updated_at
            )
            VALUES (
                %(person_id)s,
                'open',
                NOW(),
                NOW(),
                NOW(),
                NOW()
            )
            ON CONFLICT (person_id) DO UPDATE SET
                last_read_by_user_at = NOW(),
                updated_at = NOW()
            """,
            {'person_id': s.person_id},
        )

        thread = tx.execute(
            """
            SELECT
                person_id,
                status,
                last_message_at::TEXT AS last_message_at,
                last_read_by_admin_at::TEXT AS last_read_by_admin_at,
                last_read_by_user_at::TEXT AS last_read_by_user_at,
                created_at::TEXT AS created_at,
                updated_at::TEXT AS updated_at
            FROM admin_support_thread
            WHERE person_id = %(person_id)s
            """,
            {'person_id': s.person_id},
        ).fetchone()

        messages = tx.execute(
            """
            SELECT
                id,
                sender_role,
                body,
                attachment_name,
                attachment_mime,
                attachment_bytes,
                created_at::TEXT AS created_at
            FROM admin_support_message
            WHERE person_id = %(person_id)s
            ORDER BY created_at ASC, id ASC
            """,
            {'person_id': s.person_id},
        ).fetchall()

    return {
        **dict(thread or {}),
        'messages': [_serialize_admin_support_message(row) for row in messages],
    }


def post_admin_support_message(req: t.PostAdminSupportMessage, s: t.SessionInfo):
    attachment = req.attachment

    with api_tx() as tx:
        tx.execute(
            """
            INSERT INTO admin_support_thread (
                person_id,
                status,
                last_message_at,
                last_read_by_user_at,
                created_at,
                updated_at
            )
            VALUES (
                %(person_id)s,
                'open',
                NOW(),
                NOW(),
                NOW(),
                NOW()
            )
            ON CONFLICT (person_id) DO UPDATE SET
                status = 'open',
                last_message_at = NOW(),
                last_read_by_user_at = NOW(),
                updated_at = NOW()
            """,
            {'person_id': s.person_id},
        )

        row = tx.execute(
            """
            INSERT INTO admin_support_message (
                person_id,
                sender_role,
                body,
                attachment_name,
                attachment_mime,
                attachment_bytes
            ) VALUES (
                %(person_id)s,
                'user',
                %(body)s,
                %(attachment_name)s,
                %(attachment_mime)s,
                %(attachment_bytes)s
            )
            RETURNING
                id,
                sender_role,
                body,
                attachment_name,
                attachment_mime,
                attachment_bytes,
                created_at::TEXT AS created_at
            """,
            {
                'person_id': s.person_id,
                'body': req.body or '',
                'attachment_name': attachment.file_name if attachment else None,
                'attachment_mime': attachment.mime_type if attachment else None,
                'attachment_bytes': attachment.bytes if attachment else None,
            },
        ).fetchone()

    return _serialize_admin_support_message(row)


def get_admin_support_threads():
    with api_tx('READ COMMITTED') as tx:
        rows = tx.execute(
            """
            SELECT
                p.id AS person_id,
                p.uuid::TEXT AS person_uuid,
                p.name,
                p.email,
                p.location_short_friendly AS location,
                t.status,
                t.last_message_at::TEXT AS last_message_at,
                t.last_read_by_admin_at::TEXT AS last_read_by_admin_at,
                t.last_read_by_user_at::TEXT AS last_read_by_user_at,
                COUNT(m.id) AS message_count,
                COUNT(m.id) FILTER (
                    WHERE
                        m.sender_role = 'user'
                    AND
                        (
                            t.last_read_by_admin_at IS NULL
                        OR
                            m.created_at > t.last_read_by_admin_at
                        )
                ) AS unread_count,
                MAX(m.created_at)::TEXT AS latest_message_at,
                COALESCE(
                    (
                        ARRAY_AGG(
                            CASE
                                WHEN COALESCE(m.body, '') <> '' THEN m.body
                                ELSE COALESCE(m.attachment_name, 'Attachment')
                            END
                            ORDER BY m.created_at DESC, m.id DESC
                        )
                    )[1],
                    ''
                ) AS latest_preview
            FROM admin_support_thread t
            JOIN person p
              ON p.id = t.person_id
            LEFT JOIN admin_support_message m
              ON m.person_id = t.person_id
            GROUP BY
                p.id,
                p.uuid,
                p.name,
                p.email,
                p.location_short_friendly,
                t.status,
                t.last_message_at,
                t.last_read_by_admin_at,
                t.last_read_by_user_at
            ORDER BY
                COALESCE(MAX(m.created_at), t.last_message_at) DESC,
                p.id DESC
            """
        ).fetchall()

    return [dict(row) for row in rows]


def get_admin_support_thread_by_person(person_id: int):
    with api_tx() as tx:
        thread = tx.execute(
            """
            INSERT INTO admin_support_thread (
                person_id,
                status,
                last_message_at,
                created_at,
                updated_at
            )
            SELECT
                id,
                'open',
                NOW(),
                NOW(),
                NOW()
            FROM person
            WHERE id = %(person_id)s
            ON CONFLICT (person_id) DO UPDATE SET
                last_read_by_admin_at = NOW(),
                updated_at = NOW()
            RETURNING person_id
            """,
            {'person_id': person_id},
        ).fetchone()

        if not thread:
            return 'User not found', 404

        tx.execute(
            """
            UPDATE admin_support_thread
            SET
                last_read_by_admin_at = NOW(),
                updated_at = NOW()
            WHERE person_id = %(person_id)s
            """,
            {'person_id': person_id},
        )

        detail = tx.execute(
            """
            SELECT
                p.id AS person_id,
                p.uuid::TEXT AS person_uuid,
                p.name,
                p.email,
                p.location_short_friendly AS location,
                t.status,
                t.last_message_at::TEXT AS last_message_at,
                t.last_read_by_admin_at::TEXT AS last_read_by_admin_at,
                t.last_read_by_user_at::TEXT AS last_read_by_user_at
            FROM admin_support_thread t
            JOIN person p
              ON p.id = t.person_id
            WHERE t.person_id = %(person_id)s
            """,
            {'person_id': person_id},
        ).fetchone()

        messages = tx.execute(
            """
            SELECT
                id,
                sender_role,
                body,
                attachment_name,
                attachment_mime,
                attachment_bytes,
                created_at::TEXT AS created_at
            FROM admin_support_message
            WHERE person_id = %(person_id)s
            ORDER BY created_at ASC, id ASC
            """,
            {'person_id': person_id},
        ).fetchall()

    return {
        **dict(detail),
        'messages': [_serialize_admin_support_message(row) for row in messages],
    }


def post_admin_support_reply(
    person_id: int,
    req: t.PostAdminSupportMessage,
):
    attachment = req.attachment

    with api_tx() as tx:
        exists = tx.execute(
            "SELECT 1 FROM person WHERE id = %(person_id)s",
            {'person_id': person_id},
        ).fetchone()

        if not exists:
            return 'User not found', 404

        tx.execute(
            """
            INSERT INTO admin_support_thread (
                person_id,
                status,
                last_message_at,
                last_read_by_admin_at,
                created_at,
                updated_at
            )
            VALUES (
                %(person_id)s,
                'open',
                NOW(),
                NOW(),
                NOW(),
                NOW()
            )
            ON CONFLICT (person_id) DO UPDATE SET
                status = 'open',
                last_message_at = NOW(),
                last_read_by_admin_at = NOW(),
                updated_at = NOW()
            """,
            {'person_id': person_id},
        )

        row = tx.execute(
            """
            INSERT INTO admin_support_message (
                person_id,
                sender_role,
                body,
                attachment_name,
                attachment_mime,
                attachment_bytes
            ) VALUES (
                %(person_id)s,
                'admin',
                %(body)s,
                %(attachment_name)s,
                %(attachment_mime)s,
                %(attachment_bytes)s
            )
            RETURNING
                id,
                sender_role,
                body,
                attachment_name,
                attachment_mime,
                attachment_bytes,
                created_at::TEXT AS created_at
            """,
            {
                'person_id': person_id,
                'body': req.body or '',
                'attachment_name': attachment.file_name if attachment else None,
                'attachment_mime': attachment.mime_type if attachment else None,
                'attachment_bytes': attachment.bytes if attachment else None,
            },
        ).fetchone()

    return _serialize_admin_support_message(row)


def get_admin_settings_by_prefix(prefix: str):
    with api_tx('READ COMMITTED') as tx:
        return tx.execute(
            Q_ADMIN_LIST_SETTINGS_BY_PREFIX,
            {'prefix': prefix},
        ).fetchall()


def upsert_admin_setting(key: str, value: str):
    with api_tx() as tx:
        return tx.execute(Q_ADMIN_UPSERT_SETTING, {'key': key, 'value': value}).fetchone()


def upsert_admin_settings(settings: list[dict[str, str]]):
    normalized_settings = [
        {
            'key': str(item.get('key') or '').strip(),
            'value': str(item.get('value') if item.get('value') is not None else ''),
        }
        for item in settings
        if str(item.get('key') or '').strip()
    ]

    if not normalized_settings:
        return []

    with api_tx() as tx:
        tx.executemany(Q_ADMIN_UPSERT_SETTING, normalized_settings)

    return normalized_settings


def delete_admin_setting(key: str):
    with api_tx() as tx:
        row = tx.execute(Q_ADMIN_DELETE_SETTING, {'key': key}).fetchone()

    if not row:
        return 'Setting not found', 404

    return {'key': key, 'deleted': True}


def get_admin_user_photos(person_id: int):
    with api_tx('READ COMMITTED') as tx:
        rows = tx.execute(
            Q_ADMIN_LIST_USER_PHOTOS,
            {
                'person_id': person_id,
                'images_url': _images_base_url(),
            },
        ).fetchall()

    return [
        {
            **dict(row),
            'photo_url': (
                _profile_photo_data_url(dict(row).get('uuid'))
                or dict(row).get('photo_url')
            ),
        }
        for row in rows
    ]


def delete_admin_user_photo(person_id: int, photo_uuid: str):
    with api_tx() as tx:
        row = tx.execute(
            Q_ADMIN_DELETE_USER_PHOTO,
            {
                'person_id': person_id,
                'photo_uuid': photo_uuid,
            },
        ).fetchone()

    if not row:
        return 'Photo not found', 404

    return {
        'person_id': person_id,
        'photo_uuid': photo_uuid,
        'deleted': True,
    }


def verify_admin_user_photo(person_id: int, photo_uuid: str):
    with api_tx() as tx:
        row = tx.execute(
            Q_ADMIN_VERIFY_USER_PHOTO,
            {
                'person_id': person_id,
                'photo_uuid': photo_uuid,
            },
        ).fetchone()

    if not row:
        return 'Photo not found', 404

    return {
        'person_id': person_id,
        'photo_uuid': photo_uuid,
        'verified': True,
    }


def get_admin_verification_reviews():
    with api_tx('READ COMMITTED') as tx:
        return [
            _hydrate_verification_review_media(row)
            for row in tx.execute(Q_ADMIN_LIST_VERIFICATION_REVIEWS).fetchall()
        ]


def get_admin_verification_review(review_id: int):
    with api_tx('READ COMMITTED') as tx:
        row = tx.execute(
            Q_ADMIN_GET_VERIFICATION_REVIEW,
            {'review_id': review_id},
        ).fetchone()

    if not row:
        return 'Verification request not found', 404

    return _hydrate_verification_review_media(row)


def delete_admin_verification_review_selfie(review_id: int):
    with api_tx() as tx:
        row = tx.execute(
            Q_ADMIN_DELETE_VERIFICATION_REVIEW_SELFIE,
            {'review_id': review_id},
        ).fetchone()

    if not row:
        return 'Pending verification selfie not found', 404

    return {
        'review_id': review_id,
        'deleted': True,
        'kind': 'selfie',
    }


def delete_admin_verification_review_asset(review_id: int, asset_id: int):
    with api_tx() as tx:
        row = tx.execute(
            Q_ADMIN_DELETE_VERIFICATION_REVIEW_ASSET,
            {
                'review_id': review_id,
                'asset_id': asset_id,
            },
        ).fetchone()

    if not row:
        return 'Pending verification asset not found', 404

    return {
        'review_id': review_id,
        'asset_id': asset_id,
        'deleted': True,
    }


def verify_admin_verification_review_asset(review_id: int, asset_id: int):
    with api_tx() as tx:
        row = tx.execute(
            Q_ADMIN_VERIFY_VERIFICATION_REVIEW_ASSET,
            {
                'review_id': review_id,
                'asset_id': asset_id,
            },
        ).fetchone()

    if not row:
        return 'Pending verification asset not found', 404

    return {
        'review_id': review_id,
        'asset_id': asset_id,
        'verified': True,
    }


def approve_admin_verification_review(
    review_id: int,
    reviewed_by_person_id: int,
    admin_message: str | None,
):
    with api_tx() as tx:
        existing = tx.execute(
            Q_ADMIN_GET_VERIFICATION_REVIEW,
            {'review_id': review_id},
        ).fetchone()

        if not existing:
            return 'Verification request not found', 404

        hydrated = _hydrate_verification_review_media(existing)

        if _is_verification_review_finalized(hydrated):
            return 'Verification request is already finalized', 409

        row = tx.execute(
            Q_ADMIN_APPROVE_VERIFICATION_REVIEW,
            {
                'review_id': review_id,
                'reviewed_by_person_id': reviewed_by_person_id,
                'admin_message': admin_message,
            },
        ).fetchone()

    if not row:
        return 'Verification request not found', 404

    return {
        'review_id': review_id,
        'approved': True,
        'person_id': row['id'],
    }


def reject_admin_verification_review(
    review_id: int,
    reviewed_by_person_id: int,
    admin_message: str | None,
):
    with api_tx() as tx:
        existing = tx.execute(
            Q_ADMIN_GET_VERIFICATION_REVIEW,
            {'review_id': review_id},
        ).fetchone()

        if not existing:
            return 'Verification request not found', 404

        hydrated = _hydrate_verification_review_media(existing)

        if _is_verification_review_finalized(hydrated):
            return 'Verification request is already finalized', 409

        row = tx.execute(
            Q_ADMIN_REJECT_VERIFICATION_REVIEW,
            {
                'review_id': review_id,
                'reviewed_by_person_id': reviewed_by_person_id,
                'admin_message': admin_message,
            },
        ).fetchone()

    if not row:
        return 'Verification request not found', 404

    return {
        'review_id': review_id,
        'rejected': True,
        'person_id': row['person_id'],
    }


def get_export_data_token(s: t.SessionInfo):
    params = dict(person_id=s.person_id)

    with api_tx() as tx:
        return tx.execute(Q_INSERT_EXPORT_DATA_TOKEN, params).fetchone()

def get_export_data(token: str):
    token_params = dict(token=token)

    # Fetch data from database
    with api_tx('read committed') as tx:
        params = tx.execute(Q_CHECK_EXPORT_DATA_TOKEN, token_params).fetchone()

    if not params:
        return 'Invalid token. Link might have expired.', 401

    with api_tx('read committed') as tx:
        tx.execute('SET LOCAL statement_timeout = 30000') # 30 seconds
        raw_data = tx.execute(Q_EXPORT_API_DATA, params).fetchone()['j']

    person_id = params['person_id']

    inferred_personality_data = get_me(person_id_as_int=person_id)

    search_filters = get_search_filters_by_person_id(person_id=person_id)

    # Redact sensitive fields
    for person in raw_data['person']:
        del person['id_salt']

    # Decode messages
    for row in raw_data['mam_message'] or []:
        row['timestamp'] = datetime.fromtimestamp(
            timestamp=(row['id'] >> 8) / 1_000_000,
            tz=timezone.utc,
        ).isoformat()

        # this is a json string that looks like: \x836804640005786d6c656c6d00000
        message = row['message']

        # Remove the \x prefix
        no_prefix = message[2:]

        # Bytes object
        json_decoded = bytes.fromhex(no_prefix)

        erlang_decoded = erlastic.decode(json_decoded)

        row['message'] = json.dumps(erlang_decoded, cls=BytesEncoder)

    # Return the result
    exported_dict = dict(
        raw_data=raw_data,
        inferred_personality_data=inferred_personality_data,
        search_filters=search_filters,
    )

    exported_string = json.dumps(exported_dict, indent=2)

    exported_bytes = exported_string.encode()

    exported_bytesio = io.BytesIO(exported_bytes)

    return send_file(
        exported_bytesio,
        mimetype='text/json',
        as_attachment=True,
        download_name='export.json',
    )

def post_revenuecat(req: t.PostRevenuecat):
    # Fully disabled payment route, platform now promo-forever and not gated by purchases.
    return 'Revenuecat support removed', 410

def get_visitors(s: t.SessionInfo):
    with api_tx('READ COMMITTED') as tx:
        tx.execute(Q_VISITORS, dict(person_id=s.person_id))
        return tx.fetchone()['j']

def post_mark_visitors_checked(
    req: t.PostMarkVisitorsChecked,
    s: t.SessionInfo
):
    params = dict(
        person_id=s.person_id,
        when=req.time,
    )
    with api_tx('READ COMMITTED') as tx:
        tx.execute(Q_MARK_VISITORS_CHECKED, params)
