from __future__ import annotations

import json
from typing import List, Dict
from pathlib import Path
from email.utils import parsedate_to_datetime

class KaggleArxivClient:
    def __init__(self, snapshot_path: str):
        self.snapshot_path = str(snapshot_path)
        # simple in-memory cache for lookups
        self._cache = {}

    def _normalize_id(self, arxiv_id: str) -> str:
        # remove version suffix if present
        if arxiv_id is None:
            return ''
        s = str(arxiv_id).strip()
        if 'v' in s:
            return s.split('v')[0]
        return s

    def _parse_date(self, date_str: str) -> str:
        """Parse a date string like 'Mon, 2 Apr 2007 19:18:42 GMT' to ISO format.

        If parsing fails, return the original string.
        """
        if not date_str:
            return ''
        try:
            dt = parsedate_to_datetime(date_str)
            return dt.isoformat()
        except Exception:
            try:
                # Some entries might already be ISO-like
                return str(date_str)
            except Exception:
                return ''

    def _build_metadata_from_record(self, record: Dict) -> Dict:
        # Some files wrap content under 'root'
        if isinstance(record, dict) and 'root' in record:
            rec = record.get('root') or {}
        else:
            rec = record

        title = rec.get('title') or ''

        # Prefer authors_parsed when available
        authors = []
        ap = rec.get('authors_parsed') or []
        if isinstance(ap, list) and ap:
            for parts in ap:
                try:
                    family = parts[0] if len(parts) > 0 else ''
                    given = parts[1] if len(parts) > 1 else ''
                    if given:
                        name = f"{given.strip()} {family.strip()}".strip()
                    else:
                        name = family.strip()
                    if name:
                        authors.append(name)
                except Exception:
                    continue
        else:
            # Fallback to raw authors string
            raw = rec.get('authors') or rec.get('author') or ''
            if isinstance(raw, str) and raw:
                for a in [x.strip() for x in raw.split(',') if x.strip()]:
                    authors.append(a)

        # versions -> list of {version, created}
        revised_dates = []
        try:
            versions = rec.get('versions') or []
            if not isinstance(versions, list):
                versions = [versions]
            for v in versions:
                if isinstance(v, dict):
                    created = v.get('created') or ''
                    if created:
                        iso = self._parse_date(created)
                        if iso:
                            revised_dates.append(iso)
                        else:
                            revised_dates.append(str(created))
        except Exception:
            pass

        # Fallback to update_date / other fields if versions missing
        if not revised_dates:
            ud = rec.get('update_date') or rec.get('updated') or ''
            if ud:
                rd = self._parse_date(ud)
                if rd:
                    revised_dates.append(rd)

        submission_date = revised_dates[0] if revised_dates else ''

        journal_ref = rec.get('journal-ref') or rec.get('journal_ref') or ''

        return {
            'title': title,
            'authors': authors,
            'submission_date': submission_date,
            'revised_dates': revised_dates,
            'journal_ref': journal_ref,
        }

    def get_paper_metadata(self, arxiv_id: str) -> Dict:
        """Lookup a single paper by arXiv id (base id without version).

        This streams the snapshot file and returns the first match. If no match,
        returns an empty dict.
        """
        target = self._normalize_id(arxiv_id)

        # Check cache first
        if target in self._cache:
            return self._cache[target]

        p = Path(self.snapshot_path)
        if not p.exists():
            return {}

        try:
            with p.open('r', encoding='utf-8') as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception:
                        # Skip malformed lines
                        continue

                    rec = obj.get('root') if isinstance(obj, dict) and 'root' in obj else obj
                    rid = rec.get('id') if isinstance(rec, dict) else None
                    if not rid:
                        continue
                    rid_norm = self._normalize_id(rid)
                    if rid_norm == target:
                        md = self._build_metadata_from_record(rec)
                        # cache small results
                        try:
                            self._cache[target] = md
                        except Exception:
                            pass
                        return md
        except Exception:
            return {}

        return {}

    def get_batch_metadata(self, arxiv_ids: List[str]) -> Dict[str, Dict]:
        targets = {self._normalize_id(a) for a in (arxiv_ids or [])}
        out = {}

        p = Path(self.snapshot_path)
        if not p.exists():
            # fallback to per-id lookups (which will return {})
            for aid in arxiv_ids:
                try:
                    m = self.get_paper_metadata(aid)
                    if m:
                        out[self._normalize_id(aid)] = m
                except Exception:
                    continue
            return out

        try:
            with p.open('r', encoding='utf-8') as fh:
                for line in fh:
                    if not targets:
                        break
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue

                    rec = obj.get('root') if isinstance(obj, dict) and 'root' in obj else obj
                    rid = rec.get('id') if isinstance(rec, dict) else None
                    if not rid:
                        continue
                    rid_norm = self._normalize_id(rid)
                    if rid_norm in targets:
                        md = self._build_metadata_from_record(rec)
                        out[rid_norm] = md
                        # cache
                        try:
                            self._cache[rid_norm] = md
                        except Exception:
                            pass
                        targets.remove(rid_norm)
        except Exception:
            # If streaming fails, fallback to individual lookups
            for aid in arxiv_ids:
                try:
                    m = self.get_paper_metadata(aid)
                    if m:
                        out[self._normalize_id(aid)] = m
                except Exception:
                    continue

        return out
