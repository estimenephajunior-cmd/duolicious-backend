import os
from database import api_tx
import duotypes as t
from questions.archetypeise_questions import load_questions
from typing import List, Optional
import json

_categorised_question_json_file = os.path.join(
        os.path.dirname(__file__), '..', '..',
        'questions', 'questions-categorised.txt')

_archetypeised_question_json_file = os.path.join(
        os.path.dirname(__file__), '..', '..',
        'questions', 'questions-archetypeised.txt')

_questions_text_file = os.path.join(
        os.path.dirname(__file__), '..', '..',
        'questions', 'questions.txt')

Q_GET_NEXT_QUESTIONS = """
SELECT
    question.id,
    question.question,
    question.topic,
    question.count_yes,
    question.count_no
FROM
    question
LEFT JOIN
    answer
ON
    answer.question_id = question.id
AND
    answer.person_id = %(person_id)s
WHERE
    answer.question_id IS NULL
ORDER BY
    CASE
        WHEN question.id <= 10
        THEN LPAD(question.id::text, 5, '0')
        ELSE MD5 (question.id::text || ' ' || %(person_id)s::text)
    END
LIMIT
    %(n)s
OFFSET
    %(o)s;
"""

Q_GET_SEARCH_FILTER_QUESTIONS = """
SELECT
    id AS question_id,
    question,
    topic,
    answer,
    COALESCE(accept_unanswered, TRUE) AS accept_unanswered
FROM
    question
LEFT JOIN
    search_preference_answer
ON
    question.id = question_id
AND
    person_id = %(person_id)s
ORDER BY question <-> %(q)s
LIMIT %(n)s
OFFSET %(o)s
"""

Q_ADMIN_LIST_QUESTIONS = """
SELECT
    id,
    question,
    topic,
    count_yes,
    count_no
FROM
    question
ORDER BY
    id ASC
"""

Q_ADMIN_CREATE_QUESTION = """
INSERT INTO question (
    question,
    topic,
    presence_given_yes,
    presence_given_no,
    absence_given_yes,
    absence_given_no
)
VALUES (
    %(question)s,
    %(topic)s,
    ARRAY[]::INT[],
    ARRAY[]::INT[],
    ARRAY[]::INT[],
    ARRAY[]::INT[]
)
RETURNING
    id,
    question,
    topic,
    count_yes,
    count_no
"""

Q_ADMIN_UPDATE_QUESTION = """
UPDATE
    question
SET
    question = COALESCE(NULLIF(%(question)s, ''), question),
    topic = COALESCE(NULLIF(%(topic)s, ''), topic)
WHERE
    id = %(id)s
RETURNING
    id,
    question,
    topic,
    count_yes,
    count_no
"""

Q_ADMIN_DELETE_QUESTION = """
DELETE FROM
    question
WHERE
    id = %(id)s
RETURNING
    id
"""

def init_db():
    with api_tx() as tx:
        row = tx.execute('SELECT COUNT(*) AS count FROM question').fetchone()
        question_count = int(row['count'] or 0)

    if question_count == 0:
        if not os.path.exists(_categorised_question_json_file):
            print(
                'Question seed file missing. Skipping question bootstrap: '
                + _categorised_question_json_file
            )
            return
    else:
        with api_tx() as tx:
            row = tx.execute(
                """
                SELECT COUNT(*) AS count
                FROM question
                WHERE presence_given_yes = ARRAY[]::INT[]
                """
            ).fetchone()
            needs_trait_vectors = int(row['count'] or 0) > 0

        if not needs_trait_vectors:
            return

    required_seed_files = [
        _categorised_question_json_file,
        _questions_text_file,
        _archetypeised_question_json_file,
    ]

    missing_seed_files = [path for path in required_seed_files if not os.path.exists(path)]

    if missing_seed_files:
        print(
            'Question seed file(s) missing. Skipping question bootstrap: '
            + ', '.join(missing_seed_files)
        )
        return

    with open(_categorised_question_json_file) as f:
        categorised_questions = json.load(f)

    with open(_questions_text_file) as f:
        question_to_index = {l.strip(): i for i, l in enumerate(f.readlines())}

    categorised_questions["categorised"].sort(
        key=lambda q: question_to_index[q["question"]])

    with api_tx() as tx:
        if question_count == 0:
            tx.executemany(
                """
                INSERT INTO question (
                    question,
                    topic,
                    presence_given_yes,
                    presence_given_no,
                    absence_given_yes,
                    absence_given_no
                ) VALUES (
                    %(question)s,
                    %(topic)s,
                    ARRAY[]::INT[],
                    ARRAY[]::INT[],
                    ARRAY[]::INT[],
                    ARRAY[]::INT[]
                )
                """,
                [
                    dict(
                        question=question["question"],
                        topic=question["category"].capitalize(),
                    )
                    for question in categorised_questions["categorised"]
                ]
            )

    archetypeised_questions = load_questions(_archetypeised_question_json_file)

    with api_tx() as tx:
        if question_count == 0 or needs_trait_vectors:
            tx.execute(
                """
                CREATE TEMPORARY TABLE question_trait_pair (
                    question_id SMALLSERIAL NOT NULL,
                    trait_id SMALLSERIAL NOT NULL,
                    presence_given_yes SMALLINT NOT NULL,
                    presence_given_no SMALLINT NOT NULL,
                    absence_given_yes SMALLINT NOT NULL,
                    absence_given_no SMALLINT NOT NULL,
                    CHECK (presence_given_yes >= 0),
                    CHECK (presence_given_no >= 0),
                    CHECK (absence_given_yes >= 0),
                    CHECK (absence_given_no >= 0),
                    PRIMARY KEY (question_id, trait_id)
                )
                """
            )
            tx.executemany(
                """
                INSERT INTO question_trait_pair (
                    question_id,
                    trait_id,
                    presence_given_yes,
                    presence_given_no,
                    absence_given_yes,
                    absence_given_no
                )
                VALUES (
                    (SELECT id FROM question WHERE question = %(question)s),
                    (SELECT id FROM trait WHERE name = %(trait)s),
                    %(presence_given_yes)s,
                    %(presence_given_no)s,
                    %(absence_given_yes)s,
                    %(absence_given_no)s
                );
                """,
                [
                    dict(
                        question=question.question,
                        trait=question.trait,
                        presence_given_yes=round(
                            1000 * question.presence_given_yes()),
                        presence_given_no=round(
                            1000 * question.presence_given_no()),
                        absence_given_yes=round(
                            1000 * question.absence_given_yes()),
                        absence_given_no=round(
                            1000 * question.absence_given_no()),
                    )
                    for question in archetypeised_questions.archetypeised
                ]
            )
            tx.execute(
                """
                UPDATE question
                SET
                    presence_given_yes = vector.pgy,
                    presence_given_no  = vector.pgn,
                    absence_given_yes  = vector.agy,
                    absence_given_no   = vector.agn
                FROM (
                    SELECT
                        question_id,
                        ARRAY_AGG(presence_given_yes ORDER BY trait_id) AS pgy,
                        ARRAY_AGG(presence_given_no  ORDER BY trait_id) AS pgn,
                        ARRAY_AGG(absence_given_yes  ORDER BY trait_id) AS agy,
                        ARRAY_AGG(absence_given_no   ORDER BY trait_id) AS agn
                    FROM question_trait_pair
                    GROUP BY question_id
                ) AS vector
                WHERE vector.question_id = question.id
                """
            )

def get_next_questions(s: t.SessionInfo, n: str, o: str):
    params = dict(
        person_id=s.person_id,
        n=int(n),
        o=int(o)
    )

    with api_tx('READ COMMITTED') as tx:
        return tx.execute(Q_GET_NEXT_QUESTIONS, params).fetchall()

def get_search_filter_questions(s: t.SessionInfo, q: str, n: str, o: str):
    params = dict(
        person_id=s.person_id,
        q=q,
        n=int(n),
        o=int(o)
    )

    with api_tx('READ COMMITTED') as tx:
        return tx.execute(Q_GET_SEARCH_FILTER_QUESTIONS, params).fetchall()


def get_admin_questions():
    with api_tx('READ COMMITTED') as tx:
        return tx.execute(Q_ADMIN_LIST_QUESTIONS).fetchall()


def create_admin_question(data: dict):
    question_text = (data.get('question') or '').strip()
    topic = (data.get('topic') or '').strip()

    if not question_text or not topic:
        return 'Missing required fields question or topic', 400

    with api_tx() as tx:
        return tx.execute(
            Q_ADMIN_CREATE_QUESTION,
            {
                'question': question_text,
                'topic': topic,
            },
        ).fetchone()


def update_admin_question(question_id: int, data: dict):
    with api_tx() as tx:
        row = tx.execute(
            Q_ADMIN_UPDATE_QUESTION,
            {
                'id': question_id,
                'question': data.get('question', ''),
                'topic': data.get('topic', ''),
            },
        ).fetchone()

    if not row:
        return 'Question not found', 404

    return row


def delete_admin_question(question_id: int):
    with api_tx() as tx:
        row = tx.execute(Q_ADMIN_DELETE_QUESTION, {'id': question_id}).fetchone()

    if not row:
        return 'Question not found', 404

    return {'id': question_id, 'deleted': True}


def import_admin_questions(data):
    raw_questions = data.get('questions') if isinstance(data, dict) else data

    if not isinstance(raw_questions, list) or not raw_questions:
        return 'JSON must be an array of questions or an object with a questions array.', 400

    normalized_rows: list[dict[str, str]] = []
    seen_questions: set[str] = set()
    allowed_topics = {topic.lower(): topic for topic in QUESTION_CATEGORIES}

    for item in raw_questions:
        if not isinstance(item, dict):
            return 'Each imported question must be an object with question and topic.', 400

        question_text = str(item.get('question') or '').strip()
        topic_text = str(item.get('topic') or '').strip()

        if not question_text or not topic_text:
            return 'Each imported question needs question and topic.', 400

        normalized_topic = allowed_topics.get(topic_text.lower(), topic_text)
        dedupe_key = question_text.lower()
        if dedupe_key in seen_questions:
            continue

        seen_questions.add(dedupe_key)
        normalized_rows.append({
            'question': question_text,
            'topic': normalized_topic,
        })

    if not normalized_rows:
        return {'created': 0, 'skipped_existing': 0, 'total_processed': 0}

    created = 0
    skipped_existing = 0

    with api_tx() as tx:
        for row in normalized_rows:
            existing = tx.execute(
                """
                SELECT id
                FROM question
                WHERE LOWER(question) = LOWER(%(question)s)
                LIMIT 1
                """,
                row,
            ).fetchone()

            if existing:
                skipped_existing += 1
                continue

            tx.execute(Q_ADMIN_CREATE_QUESTION, row)
            created += 1

    return {
        'created': created,
        'skipped_existing': skipped_existing,
        'total_processed': len(normalized_rows),
    }


QUESTION_CATEGORIES = [
    'Interpersonal',
    'Values',
    'Lifestyle',
    'Long-term',
    'Fun',
    'Other',
]
