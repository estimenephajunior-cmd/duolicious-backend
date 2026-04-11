import os
from database.asyncdatabase import api_tx, check_connections_forever
import asyncio
import duohash
import regex
import traceback
import sys
from websockets.exceptions import ConnectionClosedError
import notify
from async_lru_cache import AsyncLruCache
import random
from typing import Any, Tuple, Callable, Tuple, Iterable
from datetime import datetime
from service.chat.robot9000 import Q_SELECT_INTRO_HASH, upsert_intro_hash
from service.chat.mayberegister import maybe_register
from service.chat.rude import (is_rude_message, store_rude_message)
from service.chat.spam import is_spam_message
from service.chat.upsertlastnotification import upsert_last_notification
from service.chat.xmlparse import parse_xml_or_none
from service.chat.messagestorage.inbox import (
    maybe_get_inbox,
    maybe_mark_displayed,
)
from service.chat.messagestorage.mam import maybe_get_conversation
from service.chat.messagestorage import store_message
from service.chat.session import (
    Session,
    maybe_get_session_response,
)
from service.chat.online import (
    maybe_redis_subscribe_online,
    maybe_redis_unsubscribe_online,
    update_online_once,
    update_online_forever,
)
from service.chat.ratelimit import (
    maybe_fetch_rate_limit,
)
from lxml import etree
from service.chat.chatutil import (
    fetch_is_skipped,
    message_string_to_etree,
    to_bare_jid,
    fetch_id_from_username,
)
from service.chat.message import (
    AudioMessage,
    ChatMessage,
    Message,
    TypingMessage,
    xml_to_message,
)
from service.chat.audiomessage import (
    transcode_and_put,
)
import redis.asyncio as redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import xmltodict
import json
from constants import (
    MAX_NOTIFICATION_LENGTH,
)
from util import truncate_text
from service.chat.verification import (
    FMT_VERIFICATION_REQUIRED,
    verification_required,
)

app = FastAPI()

# Global publisher connection, created once per worker.
REDIS_HOST: str = os.environ.get("DUO_REDIS_HOST", "redis")
REDIS_PORT: int = int(os.environ.get("DUO_REDIS_PORT", 6379))
REDIS_WORKER_CLIENT: redis.Redis = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True)

InputMiddleware = Callable[[str], etree._Element | None]
OutputMiddleware = Callable[[str], str]
Middleware = Tuple[InputMiddleware, OutputMiddleware]

Q_HAS_MESSAGE = """
SELECT
    1
FROM
    messaged
WHERE
    subject_person_id = %(to_id)s AND object_person_id = %(from_id)s
"""

# Accounts are trusted after they've been around for a day. Verified accounts
# are trusted a bit sooner.
Q_IS_TRUSTED_ACCOUNT = """
SELECT
    1
FROM
    person
WHERE
    id = %(from_id)s
AND
    sign_up_time < now() - (interval '1 day') / power(verification_level_id, 2)
"""

Q_IMMEDIATE_DATA = """
WITH to_notification AS (
    SELECT
        1
    FROM
        person
    WHERE
        id = %(to_id)s
    AND
        [[type]]_notification = 1 -- Immediate notification ID
)
SELECT
    person.id AS person_id,
    person.uuid::TEXT AS person_uuid,
    person.name AS name,
    photo.uuid AS photo_uuid,
    photo.blurhash AS photo_blurhash
FROM
    person
LEFT JOIN
    photo
ON
    photo.person_id = person.id
WHERE
    id = %(from_id)s
AND
    EXISTS (SELECT 1 FROM to_notification)
ORDER BY
    photo.position
LIMIT 1
"""

Q_IMMEDIATE_INTRO_DATA = Q_IMMEDIATE_DATA.replace('[[type]]', 'intros')

Q_IMMEDIATE_CHAT_DATA = Q_IMMEDIATE_DATA.replace('[[type]]', 'chats')

Q_SELECT_PUSH_TOKEN = """
SELECT
    push_token AS token
FROM
    person
WHERE
    uuid = uuid_or_null(%(username)s)
"""

Q_MESSAGE_PACE_GUARD = """
SELECT
    recipient.message_pace_preference,
    (
        SELECT MAX(created_at)
        FROM messaged
        WHERE subject_person_id = %(from_id)s
          AND object_person_id = %(to_id)s
    ) AS sender_last_sent_at,
    (
        SELECT MAX(created_at)
        FROM messaged
        WHERE subject_person_id = %(to_id)s
          AND object_person_id = %(from_id)s
    ) AS recipient_last_sent_at
FROM person AS recipient
WHERE recipient.id = %(to_id)s
"""

Q_INTRO_GUARD_STATE = """
WITH config AS (
    SELECT
        COALESCE(
            MAX(CASE WHEN key = 'system_max_active_chats' THEN NULLIF(value, '')::INT END),
            5
        ) AS max_active_chats,
        COALESCE(
            MAX(CASE WHEN key = 'system_intro_cooldown_hours' THEN NULLIF(value, '')::INT END),
            48
        ) AS intro_cooldown_hours,
        COALESCE(
            MAX(CASE WHEN key = 'system_intro_requests_per_hour' THEN NULLIF(value, '')::INT END),
            3
        ) AS intro_requests_per_hour,
        COALESCE(
            MAX(CASE WHEN key = 'system_intro_requests_per_day' THEN NULLIF(value, '')::INT END),
            8
        ) AS intro_requests_per_day,
        COALESCE(
            MAX(CASE WHEN key = 'system_low_trust_intro_requests_per_day' THEN NULLIF(value, '')::INT END),
            2
        ) AS low_trust_intro_requests_per_day,
        COALESCE(
            MAX(CASE WHEN key = 'system_intro_rejection_review_threshold' THEN NULLIF(value, '')::INT END),
            5
        ) AS intro_rejection_review_threshold,
        COALESCE(
            MAX(CASE WHEN key = 'system_trust_warning_threshold' THEN NULLIF(value, '')::INT END),
            40
        ) AS trust_warning_threshold,
        COALESCE(
            MAX(CASE WHEN key = 'system_trust_request_block_threshold' THEN NULLIF(value, '')::INT END),
            40
        ) AS trust_request_block_threshold,
        COALESCE(
            MAX(CASE WHEN key = 'system_conversation_auto_close_days' THEN NULLIF(value, '')::INT END),
            10
        ) AS auto_close_days
    FROM admin_setting
), active_chats AS (
    SELECT COUNT(DISTINCT remote_bare_jid) AS count
    FROM inbox
    CROSS JOIN config
    JOIN person AS self_person
      ON self_person.id = %(from_id)s
    JOIN person AS remote_person
      ON remote_person.uuid::TEXT = split_part(inbox.remote_bare_jid, '@', 1)
    WHERE
        luser = self_person.uuid::TEXT
    AND
        box = 'chats'
    AND
        timestamp >= ((EXTRACT(EPOCH FROM NOW() - (config.auto_close_days || ' days')::interval) * 1e6)::BIGINT)
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
), latest_skip AS (
    SELECT MAX(created_at) AS created_at
    FROM skipped
    WHERE
        subject_person_id = %(from_id)s
    AND
        reported = FALSE
), intro_volume AS (
    SELECT
        COUNT(DISTINCT object_person_id) FILTER (
            WHERE created_at > NOW() - INTERVAL '1 hour'
        ) AS intro_count_last_hour,
        COUNT(DISTINCT object_person_id) FILTER (
            WHERE created_at > NOW() - INTERVAL '1 day'
        ) AS intro_count_last_day
    FROM messaged
    WHERE
        subject_person_id = %(from_id)s
    AND
        created_at > NOW() - INTERVAL '1 day'
), intro_rejections AS (
    SELECT
        COUNT(*) FILTER (
            WHERE rejected_at > NOW() - INTERVAL '30 days'
        ) AS rejection_count_last_30_days
    FROM intro_review
    WHERE
        from_person_id = %(from_id)s
    AND
        status = 'rejected'
), sender_profile AS (
    SELECT
        person.id,
        person.verification_level_id,
        person.sign_up_time,
        person.profile_status,
        person.waitlist_status,
        person.referred_by_code_id,
        person.count_answers,
        person.about,
        (
            SELECT COUNT(*)
            FROM photo
            WHERE photo.person_id = person.id
        ) AS photo_count,
        EXISTS (
            SELECT 1
            FROM audio
            WHERE audio.person_id = person.id
        ) AS has_audio,
        (
            SELECT COUNT(*)
            FROM antiabuse_flag
            WHERE
                antiabuse_flag.person_id = person.id
            AND antiabuse_flag.status = 'resolved'
            AND antiabuse_flag.resolution = 'warning'
        ) AS validated_warning_count,
        (
            SELECT COUNT(*)
            FROM antiabuse_flag
            WHERE
                antiabuse_flag.person_id = person.id
            AND antiabuse_flag.status = 'resolved'
            AND antiabuse_flag.resolution = 'temporary_restriction'
        ) AS validated_temporary_count,
        (
            SELECT COUNT(*)
            FROM antiabuse_flag
            WHERE
                antiabuse_flag.person_id = person.id
            AND antiabuse_flag.status = 'resolved'
            AND antiabuse_flag.resolution = 'permanent_ban'
        ) AS validated_permaban_count,
        (
            SELECT COUNT(*)
            FROM antiabuse_flag
            WHERE
                antiabuse_flag.person_id = person.id
            AND antiabuse_flag.status = 'resolved'
            AND antiabuse_flag.resolution IN ('warning', 'temporary_restriction', 'permanent_ban')
        ) AS validated_issue_count,
        (
            SELECT COUNT(*)
            FROM antiabuse_flag
            WHERE
                antiabuse_flag.person_id = person.id
            AND antiabuse_flag.status IN ('open', 'reviewing')
        ) AS open_flag_count
    FROM person
    WHERE person.id = %(from_id)s
), intro_review_state AS (
    SELECT
        id,
        from_person_id,
        to_person_id,
        status,
        prompt,
        round_count
    FROM intro_review
    WHERE
        (from_person_id = %(from_id)s AND to_person_id = %(to_id)s)
    OR
        (from_person_id = %(to_id)s AND to_person_id = %(from_id)s)
    ORDER BY updated_at DESC
    LIMIT 1
), intro_request_state AS (
    SELECT
        id,
        from_person_id,
        to_person_id,
        status
    FROM intro_request
    WHERE
        (from_person_id = %(from_id)s AND to_person_id = %(to_id)s)
    OR
        (from_person_id = %(to_id)s AND to_person_id = %(from_id)s)
    ORDER BY updated_at DESC
    LIMIT 1
)
SELECT
    config.max_active_chats,
    config.intro_cooldown_hours,
    config.intro_requests_per_hour,
    config.intro_requests_per_day,
    config.low_trust_intro_requests_per_day,
    config.intro_rejection_review_threshold,
    config.trust_warning_threshold,
    config.trust_request_block_threshold,
    active_chats.count AS active_chat_count,
    latest_skip.created_at AS last_skip_created_at,
    intro_volume.intro_count_last_hour,
    intro_volume.intro_count_last_day,
    intro_rejections.rejection_count_last_30_days,
    CLAMP(
        0,
        100,
        45
        + CASE WHEN sender_profile.verification_level_id > 1 THEN 20 ELSE 0 END
        + CASE WHEN sender_profile.referred_by_code_id IS NOT NULL THEN 10 ELSE 0 END
        + CASE WHEN sender_profile.photo_count >= 3 AND length(COALESCE(sender_profile.about, '')) >= 80 THEN 10 ELSE 0 END
        + CASE WHEN sender_profile.validated_issue_count = 0 AND sender_profile.sign_up_time < NOW() - INTERVAL '14 days' THEN 10 ELSE 0 END
        + CASE WHEN sender_profile.has_audio THEN 5 ELSE 0 END
        - LEAST(12, sender_profile.validated_warning_count * 6)
        - LEAST(25, sender_profile.validated_temporary_count * 12)
        - LEAST(50, sender_profile.validated_permaban_count * 25)
        - LEAST(10, intro_rejections.rejection_count_last_30_days * 5)
        - CASE WHEN sender_profile.photo_count < 3 OR length(COALESCE(sender_profile.about, '')) < 40 THEN 5 ELSE 0 END
        - CASE WHEN sender_profile.open_flag_count > 0 THEN 15 ELSE 0 END
    ) AS sender_trust_score,
    intro_request_state.id AS intro_request_id,
    intro_request_state.from_person_id AS intro_request_from_person_id,
    intro_request_state.to_person_id AS intro_request_to_person_id,
    intro_request_state.status AS intro_request_status,
    intro_review_state.id AS intro_review_id,
    intro_review_state.from_person_id,
    intro_review_state.to_person_id,
    intro_review_state.status AS intro_review_status,
    intro_review_state.prompt AS intro_review_prompt,
    intro_review_state.round_count AS intro_review_round_count
FROM config, active_chats, latest_skip, intro_volume, intro_rejections, sender_profile
LEFT JOIN intro_request_state ON TRUE
LEFT JOIN intro_review_state ON TRUE
"""

Q_ARCHIVE_STALE_CHATS_FOR_PERSON = """
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
    luser = (SELECT uuid::TEXT FROM person WHERE id = %(from_id)s)
AND box = 'chats'
AND timestamp < ((EXTRACT(EPOCH FROM NOW() - (config.auto_close_days || ' days')::interval) * 1e6)::BIGINT)
"""

Q_SELECT_SENDER_RISK = """
SELECT
    verification_level_id,
    profile_status,
    waitlist_status
FROM person
WHERE id = %(from_id)s
"""

Q_SELECT_RECIPIENT_SKIP_STATE = """
SELECT
    EXISTS (
        SELECT 1
        FROM skipped
        WHERE
            subject_person_id = %(to_id)s
        AND object_person_id = %(from_id)s
    ) AS recipient_skipped_sender
"""

Q_SELECT_OPEN_FLAG = """
SELECT id
FROM antiabuse_flag
WHERE
    person_id = %(person_id)s
AND category = %(category)s
AND status IN ('open', 'reviewing')
LIMIT 1
"""

Q_UPDATE_FLAG = """
UPDATE antiabuse_flag
SET
    severity = %(severity)s,
    reason = %(reason)s,
    evidence = %(evidence)s::jsonb,
    updated_at = NOW()
WHERE id = %(flag_id)s
"""

Q_INSERT_FLAG = """
INSERT INTO antiabuse_flag (
    person_id,
    category,
    severity,
    reason,
    evidence
) VALUES (
    %(person_id)s,
    %(category)s,
    %(severity)s,
    %(reason)s,
    %(evidence)s::jsonb
)
"""

Q_UPSERT_INTRO_REVIEW_AFTER_SEND = """
INSERT INTO intro_review (
    from_person_id,
    to_person_id,
    status,
    prompt,
    round_count,
    updated_at
)
VALUES (
    %(from_id)s,
    %(to_id)s,
    'pending',
    '',
    0,
    NOW()
)
ON CONFLICT (from_person_id, to_person_id) DO UPDATE
SET
    status = 'pending',
    prompt = '',
    round_count = CASE
        WHEN intro_review.status = 'needs_more_answers'
        THEN intro_review.round_count + 1
        ELSE intro_review.round_count
    END,
    updated_at = NOW()
"""

MAX_MESSAGE_LEN = 5000

NON_ALPHANUMERIC_RE = regex.compile(r'[^\p{L}\p{N}]')
REPEATED_CHARACTERS_RE = regex.compile(r'(.)\1{1,}')


async def redis_publish(channel: str, message: str):
    await REDIS_WORKER_CLIENT.publish(channel, message)


async def redis_publish_many(channel: str, messages: Iterable[str]):
    for message in messages:
        await redis_publish(channel, message)


async def redis_forward_to_websocket(
    pubsub: redis.client.PubSub,
    middleware: OutputMiddleware,
    websocket: WebSocket
) -> None:
    """
    Listens on the Redis subscription channel and forwards any messages
    to the connected websocket client.
    """
    try:
        async for message in pubsub.listen():
            if message is None or message.get("type") != "message":
                continue

            try:
                data = middleware(message['data'])
            except:
                continue

            await websocket.send_text(data)
    except asyncio.CancelledError:
        raise
    except:
        print(traceback.format_exc())


async def send_notification(
    from_name: str | None,
    to_username: str | None,
    message: str | None,
    is_intro: bool,
    data: Any,
):
    if from_name is None:
        return

    if to_username is None:
        return

    if message is None:
        return

    if data is None:
        return

    to_token = await fetch_push_token(username=to_username)

    if to_token is None:
        return

    truncated_message = truncate_text(message, MAX_NOTIFICATION_LENGTH)

    notify.enqueue_mobile_notification(
        token=to_token,
        title=f"{from_name} sent you a message",
        body=truncated_message,
        data=data,
    )

    upsert_last_notification(username=to_username, is_intro=is_intro)


def normalize_message(message_str: str) -> str:
    message_str = message_str.lower()

    # Remove everything but non-alphanumeric characters
    message_str = NON_ALPHANUMERIC_RE.sub('', message_str)

    # Remove repeated characters
    message_str = REPEATED_CHARACTERS_RE.sub(r'\1', message_str)

    return message_str


def is_text_too_long(message: Message) -> bool:
    if isinstance(message, ChatMessage):
        return len(message.body) > MAX_MESSAGE_LEN
    else:
        return False


def is_ping(parsed_xml) -> bool:
    try:
        return parsed_xml.tag == 'duo_ping'
    except:
        return False


@AsyncLruCache(cache_condition=lambda x: not x)
async def is_unique_message(message: Message):
    if isinstance(message, AudioMessage):
        return True

    if isinstance(message, TypingMessage):
        return True

    normalized = normalize_message(message.body)
    hashed = duohash.md5(normalized)

    params = dict(hash=hashed)

    async with api_tx('read committed') as tx:
        cursor = await tx.execute(Q_SELECT_INTRO_HASH, params)
        row = await cursor.fetchone()

    upsert_intro_hash(hashed)

    return row is None

@AsyncLruCache(cache_condition=lambda x: not x)
async def fetch_is_intro(from_id: int, to_id: int) -> bool:
    async with api_tx('read committed') as tx:
        await tx.execute(Q_HAS_MESSAGE, dict(from_id=from_id, to_id=to_id))
        row = await tx.fetchone()

    return not bool(row)

@AsyncLruCache(ttl=5)  # 5 seconds
async def fetch_is_trusted_account(from_id: int) -> bool:
    async with api_tx('read committed') as tx:
        await tx.execute(
                Q_IS_TRUSTED_ACCOUNT,
                dict(from_id=from_id))
        row = await tx.fetchone()

    return bool(row)

@AsyncLruCache(ttl=2 * 60)  # 2 minutes
async def fetch_push_token(username: str) -> str | None:
    async with api_tx('read committed') as tx:
        await tx.execute(Q_SELECT_PUSH_TOKEN, dict(username=username))
        row = await tx.fetchone()

    return row.get('token') if row else None


async def maybe_fetch_intro_guard(
    *,
    from_id: int,
    to_id: int,
    stanza_id: str,
    is_intro: bool,
) -> tuple[list[str] | None, str]:
    async with api_tx('read committed') as tx:
        await tx.execute(
            Q_ARCHIVE_STALE_CHATS_FOR_PERSON,
            {
                'from_id': from_id,
            },
        )
        await tx.execute(
            Q_INTRO_GUARD_STATE,
            {
                'from_id': from_id,
                'to_id': to_id,
            },
        )
        row = await tx.fetchone()

    if not row:
        return None, 'none'

    active_chat_count = int(row.get('active_chat_count') or 0)
    max_active_chats = int(row.get('max_active_chats') or 5)
    cooldown_hours = int(row.get('intro_cooldown_hours') or 48)
    intro_requests_per_hour = int(row.get('intro_requests_per_hour') or 3)
    intro_requests_per_day = int(row.get('intro_requests_per_day') or 8)
    low_trust_intro_requests_per_day = int(row.get('low_trust_intro_requests_per_day') or 2)
    intro_rejection_review_threshold = int(row.get('intro_rejection_review_threshold') or 5)
    trust_warning_threshold = int(row.get('trust_warning_threshold') or 40)
    trust_request_block_threshold = int(row.get('trust_request_block_threshold') or 40)
    last_skip_created_at = row.get('last_skip_created_at')
    intro_count_last_hour = int(row.get('intro_count_last_hour') or 0)
    intro_count_last_day = int(row.get('intro_count_last_day') or 0)
    rejection_count_last_30_days = int(row.get('rejection_count_last_30_days') or 0)
    sender_trust_score = int(row.get('sender_trust_score') or 0)
    intro_request_id = row.get('intro_request_id')
    intro_request_from = row.get('intro_request_from_person_id')
    intro_request_status = row.get('intro_request_status')
    review_status = row.get('intro_review_status')
    review_from = row.get('from_person_id')

    if is_intro and active_chat_count >= max_active_chats:
        return ([f'<duo_message_blocked id="{stanza_id}" reason="active-chat-cap"/>'], 'none')

    if is_intro and last_skip_created_at:
        hours_since_skip = (datetime.utcnow() - last_skip_created_at.replace(tzinfo=None)).total_seconds() / 3600
        if hours_since_skip < cooldown_hours:
            return ([f'<duo_message_blocked id="{stanza_id}" reason="intro-cooldown"/>'], 'none')

    if is_intro and intro_count_last_hour >= intro_requests_per_hour:
        await create_or_touch_intro_flag(
            person_id=from_id,
            category='request-rate-limit',
            severity='medium',
            reason='This account is sending new intros too quickly.',
            evidence={
                'intro_count_last_hour': intro_count_last_hour,
                'threshold': intro_requests_per_hour,
            },
        )
        return ([f'<duo_message_blocked id="{stanza_id}" reason="intro-rate-cap"/>'], 'none')

    if is_intro and intro_count_last_day >= intro_requests_per_day:
        await create_or_touch_intro_flag(
            person_id=from_id,
            category='request-volume-limit',
            severity='medium',
            reason='This account reached the daily intro request limit.',
            evidence={
                'intro_count_last_day': intro_count_last_day,
                'threshold': intro_requests_per_day,
            },
        )
        return ([f'<duo_message_blocked id="{stanza_id}" reason="intro-rate-cap"/>'], 'none')

    if is_intro and sender_trust_score < trust_request_block_threshold:
        await create_or_touch_intro_flag(
            person_id=from_id,
            category='trust-request-review',
            severity='high',
            reason='This account must be reviewed before sending new intros.',
            evidence={
                'sender_trust_score': sender_trust_score,
                'threshold': trust_request_block_threshold,
            },
        )
        return ([f'<duo_message_blocked id="{stanza_id}" reason="trust-review"/>'], 'none')

    if is_intro and sender_trust_score < trust_warning_threshold and intro_count_last_day >= low_trust_intro_requests_per_day:
        await create_or_touch_intro_flag(
            person_id=from_id,
            category='low-trust-request-limit',
            severity='medium',
            reason='This account hit the lower intro limit applied to risky trust scores.',
            evidence={
                'sender_trust_score': sender_trust_score,
                'trust_warning_threshold': trust_warning_threshold,
                'intro_count_last_day': intro_count_last_day,
                'threshold': low_trust_intro_requests_per_day,
            },
        )
        return ([f'<duo_message_blocked id="{stanza_id}" reason="low-trust-intro-cap"/>'], 'none')

    if is_intro and rejection_count_last_30_days >= intro_rejection_review_threshold:
        await create_or_touch_intro_flag(
            person_id=from_id,
            category='repeated-intro-rejection',
            severity='medium',
            reason='This account has been rejected too many times recently.',
            evidence={
                'rejection_count_last_30_days': rejection_count_last_30_days,
                'threshold': intro_rejection_review_threshold,
            },
        )
        return ([f'<duo_message_blocked id="{stanza_id}" reason="intro-rejection-review"/>'], 'none')

    if not review_status:
        if is_intro and not intro_request_id:
            return ([f'<duo_message_blocked id="{stanza_id}" reason="intro-request-required"/>'], 'none')
        if is_intro and intro_request_status == 'rejected':
            return ([f'<duo_message_blocked id="{stanza_id}" reason="intro-request-rejected"/>'], 'none')
        if is_intro and intro_request_status == 'pending':
            if intro_request_from == from_id:
                return ([f'<duo_message_blocked id="{stanza_id}" reason="intro-request-pending"/>'], 'none')
            return ([f'<duo_message_blocked id="{stanza_id}" reason="intro-request-review-required"/>'], 'none')
        if is_intro and intro_request_status == 'accepted' and intro_request_from != from_id:
            return ([f'<duo_message_blocked id="{stanza_id}" reason="intro-request-wait-requester"/>'], 'none')
        return (None, 'track') if is_intro else (None, 'none')

    if review_status == 'accepted':
        return None, 'none'

    if review_status == 'rejected':
        return ([f'<duo_message_blocked id="{stanza_id}" reason="intro-review-rejected"/>'], 'none')

    if review_status == 'needs_more_answers':
        if review_from == from_id:
            return None, 'resume'
        return ([f'<duo_message_blocked id="{stanza_id}" reason="intro-review-action-required"/>'], 'none')

    if review_status == 'pending':
        if review_from == from_id:
            return ([f'<duo_message_blocked id="{stanza_id}" reason="intro-review-pending"/>'], 'none')
        return ([f'<duo_message_blocked id="{stanza_id}" reason="intro-review-action-required"/>'], 'none')

    return None, 'none'


async def maybe_fetch_message_pace_guard(
    *,
    from_id: int,
    to_id: int,
    stanza_id: str,
    is_intro: bool,
) -> list[str] | None:
    if is_intro:
        return None

    async with api_tx('read committed') as tx:
        await tx.execute(
            Q_MESSAGE_PACE_GUARD,
            {
                'from_id': from_id,
                'to_id': to_id,
            },
        )
        row = await tx.fetchone()

    if not row:
        return None

    preference = row.get('message_pace_preference') or 'Steady'
    sender_last_sent_at = row.get('sender_last_sent_at')
    recipient_last_sent_at = row.get('recipient_last_sent_at')

    if not sender_last_sent_at:
        return None

    waiting_for_reply = (
        recipient_last_sent_at is None
        or recipient_last_sent_at <= sender_last_sent_at
    )

    if not waiting_for_reply:
        return None

    minimum_hours = {
        'Steady': 0,
        'Intentional': 6,
        'Slow and steady': 24,
    }.get(preference, 0)

    if minimum_hours <= 0:
        return None

    hours_since_last_message = (
        datetime.utcnow() - sender_last_sent_at.replace(tzinfo=None)
    ).total_seconds() / 3600

    if hours_since_last_message >= minimum_hours:
        return None

    return [f'<duo_message_blocked id="{stanza_id}" reason="message-pace"/>']


async def create_or_touch_intro_flag(
    *,
    person_id: int,
    category: str,
    severity: str,
    reason: str,
    evidence: dict[str, Any],
) -> None:
    async with api_tx('read committed') as tx:
        await tx.execute(
            Q_SELECT_OPEN_FLAG,
            {
                'person_id': person_id,
                'category': category,
            },
        )
        row = await tx.fetchone()

        params = {
            'person_id': person_id,
            'category': category,
            'severity': severity,
            'reason': reason,
            'evidence': json.dumps(evidence),
        }

        if row:
            params['flag_id'] = row['id']
            await tx.execute(Q_UPDATE_FLAG, params)
        else:
            await tx.execute(Q_INSERT_FLAG, params)

@AsyncLruCache(ttl=10)  # 10 seconds
async def fetch_immediate_data(from_id: int, to_id: int, is_intro: bool):
    q = Q_IMMEDIATE_INTRO_DATA if is_intro else Q_IMMEDIATE_CHAT_DATA

    async with api_tx('read committed') as tx:
        await tx.execute(q, dict(from_id=from_id, to_id=to_id))
        row = await tx.fetchone()

    return row if row else None

def get_middleware(subprotocol: str) -> Middleware:
    if subprotocol == 'json':
        def input_middleware(text: str):
            json_data = json.loads(text)
            xml_str = xmltodict.unparse(json_data, full_document=False)
            return parse_xml_or_none(xml_str)

        def output_middleware(text: str):
            if text == '</stream:stream>':
                return '{"stream": null}'

            dict_obj = xmltodict.parse(text)
            return json.dumps(dict_obj)
    else:
        def input_middleware(text: str):
            return parse_xml_or_none(text)

        def output_middleware(text: str):
            return text

    return input_middleware, output_middleware

async def process_text(
    session: Session,
    middleware: InputMiddleware,
    pubsub: redis.client.PubSub,
    text: str
):
    from_username = session.username
    connection_uuid = session.connection_uuid

    parsed_xml = middleware(text)

    if parsed_xml is None:
        return

    maybe_session_response = await maybe_get_session_response(
            parsed_xml, session)

    if maybe_session_response:
        return await redis_publish_many(connection_uuid, maybe_session_response)

    if is_ping(parsed_xml):
        return await redis_publish_many(connection_uuid, [
            '<duo_pong preferred_interval="10000" preferred_timeout="5000" />',
        ])

    if not from_username:
        return

    if maybe_register(parsed_xml, from_username):
        return await redis_publish_many(connection_uuid, [
            '<duo_registration_successful />'
        ])

    maybe_conversation = await maybe_get_conversation(parsed_xml, from_username)
    if maybe_conversation:
        return await redis_publish_many(connection_uuid, maybe_conversation)

    maybe_inbox = await maybe_get_inbox(parsed_xml, from_username)
    if maybe_inbox:
        return await redis_publish_many(connection_uuid, maybe_inbox)

    if maybe_mark_displayed(parsed_xml, from_username):
        return

    maybe_subscription = await maybe_redis_subscribe_online(
            from_username=from_username,
            parsed_xml=parsed_xml,
            redis_client=REDIS_WORKER_CLIENT,
            pubsub=pubsub)
    if maybe_subscription:
        return await redis_publish_many(connection_uuid, maybe_subscription)

    maybe_unsubscription = await maybe_redis_unsubscribe_online(
            parsed_xml=parsed_xml,
            pubsub=pubsub)
    if maybe_unsubscription:
        return await redis_publish_many(connection_uuid, maybe_unsubscription)

    maybe_message = xml_to_message(parsed_xml)

    if not maybe_message:
        return

    stanza_id = maybe_message.stanza_id

    to_username = maybe_message.to_username

    from_id = await fetch_id_from_username(from_username)

    if not from_id:
        return

    to_id = await fetch_id_from_username(to_username)

    if not to_id:
        return

    if await verification_required(person_id=from_id):
        return await redis_publish_many(connection_uuid, [
            FMT_VERIFICATION_REQUIRED.format(stanza_id=stanza_id)
        ])

    if await fetch_is_skipped(from_id=from_id, to_id=to_id):
        return await redis_publish_many(connection_uuid, [
            f'<duo_message_blocked id="{stanza_id}"/>'
        ])

    if isinstance(maybe_message, TypingMessage):
        return await redis_publish_many(to_username, [
            etree.tostring(
                message_string_to_etree(
                    to_username=to_username,
                    from_username=from_username,
                    id=maybe_message.stanza_id,
                    type='typing',
                ),
                encoding='unicode',
                pretty_print=False,
            )
        ])

    if is_text_too_long(maybe_message):
        return await redis_publish_many(connection_uuid, [
            f'<duo_message_too_long id="{stanza_id}"/>'
        ])

    is_intro = await fetch_is_intro(from_id=from_id, to_id=to_id)

    intro_guard_messages, intro_review_mode = await maybe_fetch_intro_guard(
        from_id=from_id,
        to_id=to_id,
        stanza_id=stanza_id,
        is_intro=is_intro,
    )

    if intro_guard_messages:
        return await redis_publish_many(connection_uuid, intro_guard_messages)

    pace_guard_messages = await maybe_fetch_message_pace_guard(
        from_id=from_id,
        to_id=to_id,
        stanza_id=stanza_id,
        is_intro=is_intro,
    )

    if pace_guard_messages:
        return await redis_publish_many(connection_uuid, pace_guard_messages)

    if is_intro and is_rude_message(maybe_message):
        await store_rude_message(
            person_id=from_id,
            message=maybe_message
        )

        return await redis_publish_many(connection_uuid, [
            f'<duo_message_blocked id="{stanza_id}" reason="offensive"/>'
        ])

    if \
            is_intro and \
            is_spam_message(maybe_message) and \
            not await fetch_is_trusted_account(from_id=from_id):
        return await redis_publish_many(connection_uuid, [
            f'<duo_message_blocked id="{stanza_id}" reason="spam"/>'
        ])

    if is_intro:
        maybe_rate_limit = await maybe_fetch_rate_limit(
                from_id=from_id,
                stanza_id=stanza_id)

        if maybe_rate_limit:
            return await redis_publish_many(connection_uuid, maybe_rate_limit)

    if is_intro and not await is_unique_message(maybe_message):
        return await redis_publish_many(connection_uuid, [
            f'<duo_message_not_unique id="{stanza_id}"/>'
        ])

    async def store_audio_and_notify() -> None:
        if \
                isinstance(maybe_message, AudioMessage) and \
                not transcode_and_put(
                    uuid=maybe_message.audio_uuid,
                    audio_base64=maybe_message.audio_base64,
                ):
            return await redis_publish_many(connection_uuid, [
                f'<duo_server_error id="{stanza_id}"/>'
            ])

        audio_uuid = (
                maybe_message.audio_uuid
                if isinstance(maybe_message, AudioMessage)
                else None)

        sanitized_xml = etree.tostring(
            message_string_to_etree(
                to_username=to_username,
                from_username=from_username,
                id=maybe_message.stanza_id,
                message_body=maybe_message.body,
                audio_uuid=audio_uuid,
            ),
            encoding='unicode',
            pretty_print=False)

        immediate_data = await fetch_immediate_data(
                from_id=from_id,
                to_id=to_id,
                is_intro=is_intro)

        if intro_review_mode in ('track', 'resume'):
            async with api_tx() as tx:
                await tx.execute(
                    Q_UPSERT_INTRO_REVIEW_AFTER_SEND,
                    {
                        'from_id': from_id,
                        'to_id': to_id,
                    },
                )

        if immediate_data is not None:
            await send_notification(
                from_name=immediate_data['name'],
                to_username=to_username,
                message=maybe_message.body,
                is_intro=is_intro,
                data={
                    'screen': 'Conversation Screen',
                    'params': {
                        'personId': immediate_data['person_id'],
                        'personUuid': immediate_data['person_uuid'],
                        'name': immediate_data['name'],

                        'photoUuid': immediate_data['photo_uuid'],
                        'photoBlurhash': immediate_data['photo_blurhash'],

                        # TODO: Deprecate these fields
                        'imageUuid': immediate_data['photo_uuid'],
                        'imageBlurhash': immediate_data['photo_blurhash'],
                    },
                },
            )

        if isinstance(maybe_message, AudioMessage):
            response = (
                f'<duo_message_delivered '
                f'id="{stanza_id}" '
                f'audio_uuid="{maybe_message.audio_uuid}" '
            ).strip() + '/>'
        else:
            response = (
                f'<duo_message_delivered '
                f'id="{stanza_id}" '
            ).strip() + '/>'

        await redis_publish_many(to_username, [sanitized_xml])

        await redis_publish_many(connection_uuid, [response])

    store_message(
        from_username=from_username,
        to_username=to_username,
        from_id=from_id,
        to_id=to_id,
        msg_id=stanza_id,
        message=maybe_message,
        callback=store_audio_and_notify)


@app.websocket("/")
async def process_websocket_messages(websocket: WebSocket) -> None:
    subprotocol_header = websocket.headers.get('sec-websocket-protocol')

    if subprotocol_header == 'json':
        subprotocol = 'json'
    else:
        subprotocol = 'xmpp'

    await websocket.accept(subprotocol=subprotocol)

    input_middleware, output_middleware = get_middleware(subprotocol)

    session = Session()

    redis_websocket_client: redis.Redis = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True)

    pubsub = redis_websocket_client.pubsub()

    await pubsub.subscribe(session.connection_uuid)

    # asyncio.create_task requires some manual memory management!
    # https://docs.python.org/3/library/asyncio-task.html#asyncio.create_task
    # https://github.com/python/cpython/issues/91887
    update_online_task = None

    redis_forward_to_websocket_task = asyncio.create_task(
            redis_forward_to_websocket(pubsub, output_middleware, websocket))

    is_subscribed_by_username = False

    try:
        while True:
            text = await websocket.receive_text()

            await asyncio.shield(
                    process_text(
                        session=session,
                        middleware=input_middleware,
                        pubsub=pubsub,
                        text=text))

            if not update_online_task and session.username:
                update_online_task = asyncio.create_task(
                    update_online_forever(
                        redis_client=REDIS_WORKER_CLIENT,
                        session=session,
                        online=True
                    )
                )


            if not is_subscribed_by_username and session.username:
                await pubsub.subscribe(session.username)
                is_subscribed_by_username = True
    except WebSocketDisconnect:
        pass
    except asyncio.CancelledError:
        raise
    except:
        print(
            datetime.utcnow(),
            f"Exception while processing for username: {session.username}"
        )
        print(traceback.format_exc())
    finally:
        if update_online_task:
            update_online_task.cancel()

            try:
                await update_online_task
            except asyncio.CancelledError:
                pass

            try:
                await update_online_once(
                    redis_client=REDIS_WORKER_CLIENT,
                    session=session,
                    online=False,
                )
            except asyncio.CancelledError:
                pass

        if redis_forward_to_websocket_task:
            redis_forward_to_websocket_task.cancel()
            try:
                await redis_forward_to_websocket_task
            except asyncio.CancelledError:
                pass

        await pubsub.close()
        await redis_websocket_client.close()
