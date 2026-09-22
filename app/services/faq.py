"""Port of the n8n sub-workflow "k2 - search_clinic_faq" (n8n_reference/k2_search_clinic_faq.json).

Node chain (1:1):
    When called by K2 (executeWorkflowTrigger)
        -> Search FAQ in knowledge_base (postgres, executeQuery)
"""

import asyncio
import json
import logging
from typing import Any, Dict, Optional

from pydantic import BaseModel

logger = logging.getLogger(__name__)

__all__ = [
    "FaqSearchInput",
    "search_clinic_faq",
]


class FaqSearchInput(BaseModel):
    """Declared inputs of node "When called by K2" (executeWorkflowTrigger)."""

    clinic_id: Optional[str] = None
    question: Optional[str] = None


# Node "Search FAQ in knowledge_base" (postgres) — query copied verbatim from the
# workflow export (do not reformat). $1 = clinic_id (raw), $2 = question trimmed
# and sliced to 500 chars, per the node's queryReplacement expression.
_FAQ_SEARCH_SQL = r"""WITH input AS (
  SELECT
    $1::uuid AS clinic_id,
    BTRIM(COALESCE($2::text, '')) AS question,
    regexp_split_to_array(
      lower(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        BTRIM(COALESCE($2::text, '')),
        '[ًٌٍَُِّْـ]', '', 'g'),
        '[أإآٱ]', 'ا', 'g'),
        'ى', 'ي', 'g'),
        'ة', 'ه', 'g'),
        '[[:punct:]]+', ' ', 'g')
      ),
      '\s+'
    ) AS tokens
), candidates AS (
  SELECT
    k.id, k.title, k.content, k.document_name, k.source, k.language, k.chunk_index, k.updated_at,
    i.clinic_id, i.question, i.tokens,
    lower(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
      COALESCE(k.title, '') || ' ' || k.content,
      '[ًٌٍَُِّْـ]', '', 'g'),
      '[أإآٱ]', 'ا', 'g'),
      'ى', 'ي', 'g'),
      'ة', 'ه', 'g')
    ) AS searchable_text,
    lower(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
      COALESCE(k.title, ''),
      '[ًٌٍَُِّْـ]', '', 'g'),
      '[أإآٱ]', 'ا', 'g'),
      'ى', 'ي', 'g'),
      'ة', 'ه', 'g')
    ) AS title_text
  FROM public.knowledge_base k
  CROSS JOIN input i
  WHERE k.clinic_id = i.clinic_id
    AND k.is_published = true
    AND k.deleted_at IS NULL
    AND i.question <> ''
), ranked AS (
  SELECT
    c.*,
    (
      SELECT COUNT(*)::int
      FROM unnest(c.tokens) AS token
      WHERE char_length(token) >= 3
        AND token NOT IN ('وش', 'ايش', 'ما', 'ماذا', 'هل', 'من', 'في', 'عن', 'على', 'الى', 'إلى', 'مع', 'عند', 'كيف', 'كم', 'وين', 'أين', 'و', 'اين', 'فين', 'نعم', 'ايوه', 'ايوا', 'ايه', 'اها', 'طيب', 'تمام', 'اوك', 'اوكي', 'خلاص', 'ماشي', 'حاضر', 'اكد', 'اكدي', 'اجل')
        AND c.searchable_text ILIKE '%' || token || '%'
    ) AS matched_terms,
    (
      SELECT COUNT(*)::int
      FROM unnest(c.tokens) AS token
      WHERE char_length(token) >= 3
        AND token NOT IN ('وش', 'ايش', 'ما', 'ماذا', 'هل', 'من', 'في', 'عن', 'على', 'الى', 'إلى', 'مع', 'عند', 'كيف', 'كم', 'وين', 'أين', 'و', 'اين', 'فين', 'نعم', 'ايوه', 'ايوا', 'ايه', 'اها', 'طيب', 'تمام', 'اوك', 'اوكي', 'خلاص', 'ماشي', 'حاضر', 'اكد', 'اكدي', 'اجل')
        AND c.title_text ILIKE '%' || token || '%'
    ) AS title_matches
  FROM candidates c
), top_matches AS (
  SELECT *
  FROM ranked
  WHERE (matched_terms >= 2 OR title_matches >= 1)
  ORDER BY title_matches DESC, matched_terms DESC, chunk_index ASC, updated_at DESC
  LIMIT 5
)
SELECT
  r.clinic_id,
  r.question,
  COALESCE(
    json_agg(
      json_build_object(
        'id', r.id,
        'title', r.title,
        'content', r.content,
        'document_name', r.document_name,
        'source', r.source,
        'language', r.language,
        'chunk_index', r.chunk_index,
        'matched_terms', r.matched_terms,
        'title_matches', r.title_matches
      )
      ORDER BY r.title_matches DESC, r.matched_terms DESC, r.chunk_index ASC, r.updated_at DESC
    ),
    '[]'::json
  ) AS results,
  COUNT(*)::int AS count
FROM top_matches r
GROUP BY r.clinic_id, r.question
UNION ALL
SELECT $1::uuid, BTRIM(COALESCE($2::text, '')), '[]'::json, 0
WHERE NOT EXISTS (SELECT 1 FROM ranked WHERE matched_terms >= 2 OR title_matches >= 1);"""


async def search_clinic_faq(payload: FaqSearchInput) -> Dict[str, Any]:
    """Source workflow: k2 - search_clinic_faq (n8n_reference/k2_search_clinic_faq.json).

    Searches public.knowledge_base with clinic isolation (the SQL filters on
    k.clinic_id = $1) and returns the postgres node's row:
    {clinic_id, question, results: [...], count}."""
    values = payload.model_dump()

    # Node "Search FAQ in knowledge_base" — queryReplacement:
    #   [$json.clinic_id, ($json.question || '').toString().trim().slice(0, 500)]
    clinic_id = values.get("clinic_id")
    # JS slice(0, 500) counts UTF-16 code units; Python counts code points —
    # identical for BMP text (Arabic/ASCII), which is the realistic input here.
    question = str(values.get("question") or "").strip()[:500]

    fetched: Optional[Any] = None
    succeeded = False
    last_error: Optional[BaseException] = None
    # n8n postgres node config: retryOnFail=true, maxTries=2, waitBetweenTries=1000.
    for attempt in range(2):
        try:
            # Imported lazily so this module stays importable (and unit-testable)
            # on machines without the asyncpg driver installed.
            from app.db.pool import get_pool

            pool = await get_pool()
            async with pool.acquire() as connection:
                fetched = await connection.fetchrow(_FAQ_SEARCH_SQL, clinic_id, question)
            succeeded = True
            break
        except Exception as error:  # mirrors the n8n node failure boundary
            last_error = error
            if attempt == 0:
                await asyncio.sleep(1.0)

    if not succeeded:
        logger.warning(f"FAQ search failed after retries: {last_error}")
        # PORT-TODO(n8n): the postgres node's terminal failure goes to the n8n error
        # workflow KmQZ9bXmmP1YEZht; this workflow defines no success/code envelope,
        # so the port returns the row shape with results=[]/count=0 and an error
        # marker instead of raising.
        return {
            "clinic_id": str(clinic_id) if clinic_id is not None else None,
            "question": question,
            "results": [],
            "count": 0,
            "error": "DATABASE_ERROR",
        }

    row = dict(fetched) if fetched else {}
    results = row.get("results")
    if isinstance(results, str):  # asyncpg returns json/jsonb columns as str
        try:
            results = json.loads(results)
        except Exception:
            pass  # keep the raw value; PG guarantees valid json for this column
    clinic_id_value = row.get("clinic_id")
    return {
        # n8n renders the uuid column as a string in the item json.
        "clinic_id": str(clinic_id_value) if clinic_id_value is not None else None,
        "question": row.get("question"),
        "results": results,
        "count": row.get("count"),
    }
