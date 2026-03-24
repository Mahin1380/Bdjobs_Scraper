import re
import pandas as pd


_FULL_TO_ABBREV = {
    'BACHELOR OF BUSINESS ADMINISTRATION': 'BBA',
    'MASTER OF BUSINESS ADMINISTRATION':   'MBA',
    'BACHELOR OF SCIENCE':                 'BSc',
    'MASTER OF SCIENCE':                   'MSc',
    'BACHELOR OF COMMERCE':                'BCom',
    'MASTER OF COMMERCE':                  'MCom',
    'BACHELOR OF LAW':                     'LLB',
    'MASTER OF LAW':                       'LLM',
    'BACHELOR OF SOCIAL SCIENCE':          'BSS',
    'MASTER OF SOCIAL SCIENCE':            'MSS',
    'BACHELOR OF ARTS':                    'BA',
    'MASTER OF ARTS':                      'MA',
    'BACHELORS':                           'Bachelor',
    'MASTERS':                             'Master',
    'BACHELOR':                            'Bachelor',
    'MASTER':                              'Master',
}
 
# Abbreviation tokens → canonical display form
_ABBREV_MAP = {
    'PHD': 'PhD', 'MBBS': 'MBBS',
    'MBA': 'MBA', 'BBA': 'BBA',
    'BSC': 'BSc', 'MSC': 'MSc',
    'BCOM': 'BCom', 'MCOM': 'MCom',
    'BSS': 'BSS', 'MSS': 'MSS',
    'LLB': 'LLB', 'LLM': 'LLM',
    'DIPLOMA': 'Diploma',
    'HONOURS': 'Honours', 'HONORS': 'Honours', 'HONS': 'Honours',
    # Professional certifications (degree-level, not majors)
    'CA': 'CA', 'CMA': 'CMA', 'ACCA': 'ACCA',
    'CFA': 'CFA', 'CPA': 'CPA', 'CIMA': 'CIMA', 'FMVA': 'FMVA',
}
 
_DEGREE_RE = re.compile(
    r'\b('
    + '|'.join(re.escape(k) for k in _FULL_TO_ABBREV)   # full forms first
    + r'|Ph\.?D|MBBS|MBA|BBA|BCom|MCom|BSS|MSS|LLB|LLM|BSc|MSc'
    + r'|ACCA|CMA|CFA|CPA|CIMA|FMVA|CA|Diploma|Honours|Honors|Hons'
    + r')\b',
    flags=re.IGNORECASE,
)
 
# Known major subjects — longer/specific phrases listed before their substrings
_MAJORS = [
    'Accounting & Information System',
    'Cost & Management Accounting',
    'Supply Chain Management',
    'Human Resource Management',
    'Finance & Banking',
    'Computer Science & Engineering',
    'Electrical & Electronic Engineering',
    'Business Administration',
    'Computer Science',
    'Civil Engineering',
    'Development Studies',
    'Social Science',
    'Information Technology',
    'Data Science',
    'Accounting',
    'Finance',
    'Marketing',
    'Economics',
    'Statistics',
    'Commerce',
    'Management',
    'Architecture',
    'Mathematics',
    'Banking',
    'Law',
]
 
_MAJOR_RE = re.compile(
    r'\bin\s+(' + '|'.join(re.escape(m) for m in _MAJORS) + r')\b',
    flags=re.IGNORECASE,
)
 
 
def parse_edu(text: str) -> tuple:
    """
    Parse a raw eduRec string into (degree_level, major).
 
    Returns a tuple of two strings (comma-separated if multiple),
    or (pd.NA, pd.NA) if the input is null.
 
    Examples
    --------
    >>> parse_edu("Bachelor of Business Administration (BBA) in Finance")
    ('BBA', 'Finance')
 
    >>> parse_edu("BBA in Accounting Master of Business Administration (MBA)")
    ('BBA, MBA', 'Accounting')
    """
    if pd.isna(text):
        return pd.NA, pd.NA
 
    # ── Degree levels ────────────────────────────────────────────────────────
    seen_degrees = []
    for match in _DEGREE_RE.finditer(text):
        raw = match.group(0).upper()
        # Try full-form map first, then abbreviation map
        canonical = (
            _FULL_TO_ABBREV.get(raw)
            or _ABBREV_MAP.get(raw.replace('.', ''))
        )
        if canonical and canonical not in seen_degrees:
            seen_degrees.append(canonical)
 
    # ── Majors ───────────────────────────────────────────────────────────────
    seen_majors = []
    for match in _MAJOR_RE.finditer(text):
        major = match.group(1).title()
        if major not in seen_majors:
            seen_majors.append(major)
 
    return (
        ', '.join(seen_degrees) if seen_degrees else pd.NA,
        ', '.join(seen_majors)  if seen_majors  else pd.NA,
    )
 
 
def add_edu_columns(df: pd.DataFrame, source_col: str = 'eduRec') -> pd.DataFrame:
    """
    Add `degree_level` and `major` columns to a DataFrame by parsing `source_col`.
    The two new columns are inserted right after `source_col`.
    """
    parsed = df[source_col].apply(lambda x: pd.Series(parse_edu(x), index=['degree_level', 'major']))
    insert_pos = df.columns.get_loc(source_col) + 1
    df.insert(insert_pos,     'degree_level', parsed['degree_level'])
    df.insert(insert_pos + 1, 'major',        parsed['major'])
    return df

def clean_jobs(df: pd.DataFrame) -> pd.DataFrame:
    def strip_html(text):
        if pd.isna(text):
            return text
        return re.sub(r'<[^>]+>', ' ', str(text)).strip()

    # 1. Remove duplicates
    df = df.drop_duplicates()
    df = df.drop_duplicates(subset='Jobid', keep='first')

    # 2. Strip HTML from text fields
    for col in ['eduRec', 'jobContext']:
        if col in df.columns:
            df[col] = df[col].apply(strip_html)

    # 3. Normalize whitespace in string columns
    str_cols = ['jobTitle', 'companyName', 'JobTitleBng',
                'location', 'experience', 'eduRec', 'jobContext']
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: re.sub(r'\s+', ' ', str(x)).strip() if pd.notna(x) else x
            )

    # 4. Standardize dates to YYYY-MM-DD
    for col in ['deadline', 'deadlineDB', 'publishDate']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')

    # 5. Fix experience: "Na" string → proper null
    if 'experience' in df.columns:
        df['experience'] = df['experience'].replace('Na', pd.NA)

    # 6. Standardize location casing
    if 'location' in df.columns:
        df['location'] = (df['location']
            .str.replace('GULSHAN 1', 'Gulshan 1', regex=False)
            .str.replace('GULSHAN 2', 'Gulshan 2', regex=False))

    # 7. Fill missing logo with empty string
    if 'logo' in df.columns:
        df['logo'] = df['logo'].fillna('')

    # 8. Ensure correct boolean types
    for col in ['isEarlyAccess', 'OnlineJob']:
        if col in df.columns:
            df[col] = df[col].astype(bool)

    # 9. Drop constant columns (same value in every row)
    constant_cols = [col for col in df.columns if df[col].nunique() <= 1]
    if constant_cols:
        print(f"Dropping constant columns: {constant_cols}")
        df = df.drop(columns=constant_cols)

    df = add_edu_columns(df, source_col='eduRec')
    

    return df.reset_index(drop=True)