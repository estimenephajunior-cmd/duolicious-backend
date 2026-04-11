from __future__ import annotations

from database import api_tx
from typing import Any, Optional
import json
import math
import urllib.parse
import urllib.request


JW_BASE_URL = 'https://hub.jw.org'
JW_SITE_LANGUAGE_GUID = 'bafaf6d8-1e69-47c2-abcb-b3bdfc28eebe'
JW_CLIENT_VERSION = '1.35.2'


def _jw_get_json(path: str, query: dict[str, Any]) -> Any:
    url = f'{JW_BASE_URL}{path}?{urllib.parse.urlencode(query, doseq=True)}'

    request = urllib.request.Request(
        url,
        headers={
            'Accept': 'application/json',
            'Accept-Language': 'en',
            'Referer': 'https://hub.jw.org/meetings/en',
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/146.0.0.0 Safari/537.36'
            ),
            'x-client-version': JW_CLIENT_VERSION,
            'x-requested-with': 'cdh-application',
        },
    )

    with urllib.request.urlopen(request, timeout=20) as response:
        return json.loads(response.read().decode('utf-8'))


def _ensure_languages_populated() -> None:
    with api_tx() as tx:
        row = tx.execute(
            'SELECT COUNT(*) AS count FROM meeting_language'
        ).fetchone()
        if row and row['count'] > 0:
            return

    payload = _jw_get_json(
        '/meetings/api/languages',
        {'languageGuid': JW_SITE_LANGUAGE_GUID},
    )

    rows = [
        {
            'language_guid': item['languageGuid'],
            'code': item['code'],
            'name': item['name'],
        }
        for item in payload
        if item.get('languageGuid') and item.get('name') and item.get('code')
    ]

    if not rows:
        return

    with api_tx() as tx:
        tx.executemany(
            """
            INSERT INTO meeting_language (language_guid, code, name)
            VALUES (%(language_guid)s, %(code)s, %(name)s)
            ON CONFLICT (language_guid) DO UPDATE SET
                code = EXCLUDED.code,
                name = EXCLUDED.name
            """,
            rows,
        )


def get_languages(q: Optional[str]) -> list[dict[str, Any]]:
    _ensure_languages_populated()

    normalized_q = ' '.join((q or '').split())

    with api_tx('READ COMMITTED') as tx:
        if normalized_q:
            rows = tx.execute(
                """
                SELECT language_guid, code, name
                FROM meeting_language
                WHERE name ILIKE %(pattern)s
                ORDER BY name ASC
                LIMIT 30
                """,
                {'pattern': f'%{normalized_q}%'},
            ).fetchall()
        else:
            rows = tx.execute(
                """
                SELECT language_guid, code, name
                FROM meeting_language
                ORDER BY name ASC
                LIMIT 50
                """
            ).fetchall()

    return [dict(row) for row in rows]


def _get_location(tx, location_text: str) -> dict[str, Any] | None:
    row = tx.execute(
        """
        SELECT
            long_friendly,
            city,
            subdivision,
            country,
            ST_Y(coordinates::geometry) AS latitude,
            ST_X(coordinates::geometry) AS longitude
        FROM location
        WHERE long_friendly = %(location_text)s
        """,
        {'location_text': location_text},
    ).fetchone()

    return dict(row) if row else None


def _search_cached_congregations(
    tx,
    *,
    location_text: str,
    language_guid: str,
) -> list[dict[str, Any]]:
    rows = tx.execute(
        """
        SELECT
            congregation.id,
            congregation.jw_meeting_id,
            congregation.name,
            congregation.address,
            congregation.phone_number,
            congregation.language_guid,
            meeting_language.name AS language_name,
            congregation.latitude,
            congregation.longitude,
            congregation.midweek_meeting_day,
            congregation.midweek_meeting_time,
            congregation.weekend_meeting_day,
            congregation.weekend_meeting_time
        FROM congregation_search_cache
        JOIN congregation
          ON congregation.id = congregation_search_cache.congregation_id
        JOIN meeting_language
          ON meeting_language.language_guid = congregation.language_guid
        WHERE congregation_search_cache.location_long_friendly = %(location_text)s
          AND congregation_search_cache.language_guid = %(language_guid)s
        ORDER BY congregation.name ASC
        """,
        {
            'location_text': location_text,
            'language_guid': language_guid,
        },
    ).fetchall()

    return [dict(row) for row in rows]


def _fetch_remote_congregations(
    *,
    location_text: str,
    latitude: float,
    longitude: float,
    language_guid: str,
) -> list[dict[str, Any]]:
    latitude_delta = 0.4
    longitude_delta = 0.4 / max(0.25, math.cos(math.radians(latitude)))

    payload = _jw_get_json(
        '/meetings/api/meeting-search',
        {
            'first': 20,
            'northEastLatitude': latitude + latitude_delta,
            'northEastLongitude': longitude + longitude_delta,
            'southWestLatitude': latitude - latitude_delta,
            'southWestLongitude': longitude - longitude_delta,
            'searchLatitude': latitude,
            'searchLongitude': longitude,
            'eventTypes': ['congregation', 'congregationGroup'],
            'languageGuids': [language_guid],
            'siteLanguageGuid': JW_SITE_LANGUAGE_GUID,
            'isInitialLocationSearch': 'false',
            'search': location_text,
        },
    )

    rows: list[dict[str, Any]] = []
    for item in payload.get('items', []):
        for meeting in item.get('congregationMeetings', []):
            rows.append(
                {
                    'jw_meeting_id': meeting.get('id'),
                    'jw_place_id': item.get('id'),
                    'language_guid': meeting.get('languageGuid'),
                    'name': meeting.get('name'),
                    'address': (meeting.get('address') or '').strip() or None,
                    'phone_number': meeting.get('phoneNumber'),
                    'latitude': item.get('latitude'),
                    'longitude': item.get('longitude'),
                    'midweek_meeting_day': meeting.get('midweekMeetingDay'),
                    'midweek_meeting_time': meeting.get('midweekMeetingTime'),
                    'weekend_meeting_day': meeting.get('weekendMeetingDay'),
                    'weekend_meeting_time': meeting.get('weekendMeetingTime'),
                    'raw': json.dumps(
                        {
                            'item': item,
                            'meeting': meeting,
                        }
                    ),
                }
            )

    return [
        row for row in rows
        if row['jw_meeting_id'] and row['language_guid'] and row['name']
    ]


def _cache_remote_congregations(
    tx,
    *,
    location_text: str,
    language_guid: str,
    rows: list[dict[str, Any]],
) -> None:
    tx.execute(
        """
        DELETE FROM congregation_search_cache
        WHERE location_long_friendly = %(location_text)s
          AND language_guid = %(language_guid)s
        """,
        {
            'location_text': location_text,
            'language_guid': language_guid,
        },
    )

    if not rows:
        return

    tx.executemany(
        """
        INSERT INTO congregation (
            jw_meeting_id,
            jw_place_id,
            language_guid,
            name,
            address,
            phone_number,
            latitude,
            longitude,
            midweek_meeting_day,
            midweek_meeting_time,
            weekend_meeting_day,
            weekend_meeting_time,
            raw,
            updated_at
        ) VALUES (
            %(jw_meeting_id)s,
            %(jw_place_id)s,
            %(language_guid)s,
            %(name)s,
            %(address)s,
            %(phone_number)s,
            %(latitude)s,
            %(longitude)s,
            %(midweek_meeting_day)s,
            %(midweek_meeting_time)s,
            %(weekend_meeting_day)s,
            %(weekend_meeting_time)s,
            %(raw)s::jsonb,
            NOW()
        )
        ON CONFLICT (jw_meeting_id) DO UPDATE SET
            jw_place_id = EXCLUDED.jw_place_id,
            language_guid = EXCLUDED.language_guid,
            name = EXCLUDED.name,
            address = EXCLUDED.address,
            phone_number = EXCLUDED.phone_number,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            midweek_meeting_day = EXCLUDED.midweek_meeting_day,
            midweek_meeting_time = EXCLUDED.midweek_meeting_time,
            weekend_meeting_day = EXCLUDED.weekend_meeting_day,
            weekend_meeting_time = EXCLUDED.weekend_meeting_time,
            raw = EXCLUDED.raw,
            updated_at = NOW()
        """,
        rows,
    )

    tx.execute(
        """
        INSERT INTO congregation_search_cache (
            location_long_friendly,
            language_guid,
            congregation_id
        )
        SELECT
            %(location_text)s,
            %(language_guid)s,
            congregation.id
        FROM congregation
        WHERE congregation.jw_meeting_id = ANY(%(jw_meeting_ids)s)
        ON CONFLICT DO NOTHING
        """,
        {
            'location_text': location_text,
            'language_guid': language_guid,
            'jw_meeting_ids': [row['jw_meeting_id'] for row in rows],
        },
    )


def get_congregations(
    *,
    location_text: str,
    language_guid: str,
) -> list[dict[str, Any]]:
    _ensure_languages_populated()

    with api_tx() as tx:
        location = _get_location(tx, location_text)
        if not location:
            raise ValueError('Unknown location')

        cached = _search_cached_congregations(
            tx,
            location_text=location_text,
            language_guid=language_guid,
        )
        if cached:
            return cached

    remote_rows = _fetch_remote_congregations(
        location_text=location_text,
        latitude=float(location['latitude']),
        longitude=float(location['longitude']),
        language_guid=language_guid,
    )

    with api_tx() as tx:
        _cache_remote_congregations(
            tx,
            location_text=location_text,
            language_guid=language_guid,
            rows=remote_rows,
        )
        return _search_cached_congregations(
            tx,
            location_text=location_text,
            language_guid=language_guid,
        )


def get_admin_congregations(q: Optional[str]) -> list[dict[str, Any]]:
    normalized_q = ' '.join((q or '').split())

    with api_tx('READ COMMITTED') as tx:
        if normalized_q:
            rows = tx.execute(
                """
                SELECT
                    congregation.id,
                    congregation.name,
                    congregation.address,
                    congregation.phone_number,
                    congregation.language_guid,
                    meeting_language.name AS language_name
                FROM congregation
                JOIN meeting_language
                  ON meeting_language.language_guid = congregation.language_guid
                WHERE congregation.name ILIKE %(pattern)s
                   OR COALESCE(congregation.address, '') ILIKE %(pattern)s
                ORDER BY congregation.name ASC
                LIMIT 50
                """,
                {'pattern': f'%{normalized_q}%'},
            ).fetchall()
        else:
            rows = tx.execute(
                """
                SELECT
                    congregation.id,
                    congregation.name,
                    congregation.address,
                    congregation.phone_number,
                    congregation.language_guid,
                    meeting_language.name AS language_name
                FROM congregation
                JOIN meeting_language
                  ON meeting_language.language_guid = congregation.language_guid
                ORDER BY congregation.updated_at DESC, congregation.name ASC
                LIMIT 50
                """
            ).fetchall()

    return [dict(row) for row in rows]
