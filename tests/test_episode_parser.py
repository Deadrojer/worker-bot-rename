"""
tests/test_episode_parser.py
════════════════════════════
Regression tests for _extract_episode_number (auto_rename).
Run: python tests/test_episode_parser.py   or   python -m pytest tests/ -v
"""
import re, sys

_QUAL_INDS = [
    r'\d{2,4}[pP]', r'\dK', r'HD(?:RIP)?', r'WEB(?:-)?DL', r'BLURAY',
    r'X264', r'X265', r'HEVC', r'FHD', r'UHD', r'HDR', r'H\.264', r'H\.265',
    r'(?:19|20)\d{2}', r'Multi(?:audio)?', r'Dual(?:audio)?',
]
_QPAT = r'(?:' + '|'.join(r'(?:[\s._-]*' + q + r')' for q in _QUAL_INDS) + r')'
_SKIP = {360, 480, 720, 1080, 1440, 2160, 2020, 2021, 2022, 2023, 2024, 2025}

def _strip(s): return re.sub(r'(\d+)\s*[vV]\d+', r'\1', s)

def _ep(text):
    if not text: return None
    for pat in [
        re.compile(r'S(\d+)E(\d+)(?:\s*[vV]\d+)?',                         re.I),
        re.compile(r'S(\d+)\s+E(\d+)(?:\s*[vV]\d+)?',                      re.I),
        re.compile(r'S(\d+)[._-]E(\d+)(?:\s*[vV]\d+)?',                    re.I),
        re.compile(r'S(\d+)\s*-\s*E(\d+)(?:\s*[vV]\d+)?',                  re.I),
        re.compile(r'(?<![A-Za-z\d])(\d+)E(\d+)(?:\s*[vV]\d+)?(?!\d)',     re.I),
        re.compile(r'(?<![A-Za-z\d])(\d+)\s*[-_.]?\s*E(\d+)(?:\s*[vV]\d+)?(?!\d)', re.I),
    ]:
        for m in pat.findall(text):
            raw = m[1] if isinstance(m,tuple) and len(m)>=2 else m
            try:
                n=int(raw)
                if 1<=n<=9999 and n not in _SKIP: return n
            except: pass
    for pat in [re.compile(r'S\d+\s*-\s*(\d+)(?:\s*[vV]\d+)?',re.I),
                re.compile(r'S\d+[._]+(\d+)(?:\s*[vV]\d+)?',re.I)]:
        for m in pat.findall(text):
            raw=m[0] if isinstance(m,tuple) else m
            try:
                n=int(raw)
                if 1<=n<=9999 and n not in _SKIP: return n
            except: pass
    for pat in [re.compile(r'\bEpisode\s+(\d+)(?:\s*[vV]\d+)?',re.I),
                re.compile(r'\bEP\s*(\d+)(?:\s*[vV]\d+)?\b',re.I)]:
        for m in pat.findall(text):
            raw=m[0] if isinstance(m,tuple) else m
            try:
                n=int(raw)
                if 1<=n<=9999 and n not in _SKIP: return n
            except: pass
    for pat in [re.compile(r'(?<![A-Za-z\d])E(\d+)(?:\s*[vV]\d+)?(?!\d)',re.I),
                re.compile(r'[\[\(]E(\d+)(?:\s*[vV]\d+)?[\]\)]',re.I)]:
        for m in pat.findall(text):
            raw=m[0] if isinstance(m,tuple) else m
            try:
                n=int(raw)
                if 1<=n<=9999 and n not in _SKIP: return n
            except: pass
    m=re.search(r'\b(\d+)\s*of\s*\d+\b',text,re.I)
    if m:
        try:
            n=int(m.group(1))
            if 1<=n<=9999 and n not in _SKIP: return n
        except: pass
    fb=re.compile(r'(?:^|[^0-9A-Za-z])(\d{1,4})(?:[^0-9A-Za-z]|$)(?!'+_QPAT+r')',re.I)
    for m in fb.findall(_strip(text)):
        raw=m[0] if isinstance(m,tuple) else m
        try:
            n=int(raw)
            if 1<=n<=9999 and n not in _SKIP: return n
        except: pass
    return None

CASES = [
    # ── Spec-mandated bugs ────────────────────────────────────────────────────
    ("[Starbez] Tomo-chan is a Girl - 01E02 [BD 1080p x265 10bit FLAC] [99568175].mkv", 2),
    ("I.Was.Reincarnated.as.the.7th.Prince.so.I.Can.Take.My.Time.Perfecting.My.Magical.Ability.S01E07.1080p.BluRay.Dual-Audio.Opus.2.0.x265-Headpatter.mkv", 7),
    ("Anime S01E12v2.mkv", 12),
    ("Anime S1 - 12v3.mkv", 12),
    ("Anime 12v10.mkv", 12),
    ("Anime EP07.mkv", 7),
    ("Anime Episode 12.mkv", 12),
    ("Anime E07.mkv", 7),
    # ── SxxEyy variants ──────────────────────────────────────────────────────
    ("Show.S01E07.1080p.mkv", 7), ("Show S01 E07.mkv", 7),
    ("Show S01-E07.mkv", 7), ("Show S01 - E07.mkv", 7),
    ("Show S01.E07.mkv", 7), ("Show S10E03.mkv", 3), ("Show S02E12.mkv", 12),
    # ── xxEyy variants ───────────────────────────────────────────────────────
    ("Show - 01E02.mkv", 2), ("Show 1E02.mkv", 2),
    ("Show 01 E02.mkv", 2), ("Show 01-E02.mkv", 2), ("Show 01 - E02.mkv", 2),
    # ── Season-dash-bare ─────────────────────────────────────────────────────
    ("S1-163 Black Clover.mkv", 163), ("S1-167 Black Clover.mkv", 167),
    ("S01-12 Show.mkv", 12),
    # ── Version suffix ───────────────────────────────────────────────────────
    ("12v2.mkv", 12), ("12V3.mkv", 12), ("12v10.mkv", 12), ("12V20.mkv", 12),
    ("S01E12v2.mkv", 12), ("S01E07 V3.mkv", 7), ("S1 - 12v2.mkv", 12),
    # ── Technical metadata safe ──────────────────────────────────────────────
    ("Anime.S01E07.1080p.BluRay.x265.mkv", 7), ("Anime.S02E12.720p.WEB-DL.mkv", 12),
    # ── EP / Episode keyword ─────────────────────────────────────────────────
    ("Anime EP 07.mkv", 7), ("Anime Episode 07.mkv", 7),
    # ── Standalone E ────────────────────────────────────────────────────────
    ("Anime E12.mkv", 12), ("[E07] Anime.mkv", 7), ("(E12) Anime.mkv", 12),
]

def run():
    passed = failed = 0
    for text, want in CASES:
        got = _ep(text)
        if got == want:
            passed += 1
        else:
            failed += 1
            print(f"❌  got={got!s:5}  want={want:5}  {text}")
    print(f"\n{'─'*55}")
    print(f"Results: {passed}/{passed+failed}  {'✅ ALL OK' if not failed else f'❌ {failed} FAILED'}")
    return failed == 0

def test_episode_parser(): assert run()

if __name__ == "__main__":
    sys.exit(0 if run() else 1)
