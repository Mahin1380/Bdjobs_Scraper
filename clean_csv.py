import re
import pandas as pd


# ── Degree parsing ─────────────────────────────────────────────────────────────

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

_ABBREV_MAP = {
    'PHD': 'PhD', 'MBBS': 'MBBS',
    'MBA': 'MBA', 'BBA': 'BBA',
    'BSC': 'BSc', 'MSC': 'MSc',
    'BCOM': 'BCom', 'MCOM': 'MCom',
    'BSS': 'BSS', 'MSS': 'MSS',
    'LLB': 'LLB', 'LLM': 'LLM',
    'DIPLOMA': 'Diploma',
    'HONOURS': 'Honours', 'HONORS': 'Honours', 'HONS': 'Honours',
    'CA': 'CA', 'CMA': 'CMA', 'ACCA': 'ACCA',
    'CFA': 'CFA', 'CPA': 'CPA', 'CIMA': 'CIMA', 'FMVA': 'FMVA',
}

_DEGREE_RE = re.compile(
    r'\b('
    + '|'.join(re.escape(k) for k in _FULL_TO_ABBREV)
    + r'|Ph\.?D|MBBS|MBA|BBA|BCom|MCom|BSS|MSS|LLB|LLM|BSc|MSc'
    + r'|ACCA|CMA|CFA|CPA|CIMA|FMVA|CA|Diploma|Honours|Honors|Hons'
    + r')\b',
    flags=re.IGNORECASE,
)

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

    seen_degrees = []
    for match in _DEGREE_RE.finditer(text):
        raw = match.group(0).upper()
        canonical = (
            _FULL_TO_ABBREV.get(raw)
            or _ABBREV_MAP.get(raw.replace('.', ''))
        )
        if canonical and canonical not in seen_degrees:
            seen_degrees.append(canonical)

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
    Add `degree_level` and `major` columns derived from `source_col`.
    Inserted right after `source_col`.
    """
    parsed = df[source_col].apply(
        lambda x: pd.Series(parse_edu(x), index=['degree_level', 'major'])
    )
    insert_pos = df.columns.get_loc(source_col) + 1
    df.insert(insert_pos,     'degree_level', parsed['degree_level'])
    df.insert(insert_pos + 1, 'major',        parsed['major'])
    return df


# ── Division lookup ────────────────────────────────────────────────────────────

_LOCATION_TO_DIVISION = {
    # ── Dhaka city neighbourhoods ─────────────────────────────────────────
    'dhaka': 'Dhaka', 'banani': 'Dhaka', 'gulshan': 'Dhaka',
    'gulshan 1': 'Dhaka', 'gulshan 2': 'Dhaka',
    'mohakhali': 'Dhaka', 'uttara': 'Dhaka', 'mirpur': 'Dhaka',
    'mirpur 1': 'Dhaka', 'mirpur 2': 'Dhaka', 'mirpur 10': 'Dhaka',
    'dohs mirpur': 'Dhaka', 'dohs baridhara': 'Dhaka', 'dohs mohakhali': 'Dhaka',
    'baridhara': 'Dhaka', 'baridhara j block': 'Dhaka',
    'motijheel': 'Dhaka', 'dhanmondi': 'Dhaka', 'dhanmondi 27': 'Dhaka',
    'mohammadpur': 'Dhaka', 'shyamoli': 'Dhaka', 'lalmatia': 'Dhaka',
    'tejgaon': 'Dhaka', 'tejgaon industrial area': 'Dhaka',
    'kawran bazar': 'Dhaka', 'panthapath': 'Dhaka', 'elephant road': 'Dhaka',
    'wari': 'Dhaka', 'shyampur': 'Dhaka', 'badda': 'Dhaka',
    'banasree': 'Dhaka', 'basundhara ra': 'Dhaka', 'aftabnagar': 'Dhaka',
    'vatara': 'Dhaka', 'khilkhet': 'Dhaka', 'nikunja': 'Dhaka',
    'shantinagar': 'Dhaka', 'kakrail': 'Dhaka', 'paltan': 'Dhaka',
    'purana paltan': 'Dhaka', 'naya paltan': 'Dhaka',
    'adabor': 'Dhaka', 'banglamotor': 'Dhaka',
    'uttara sector 3': 'Dhaka', 'uttara sector 4': 'Dhaka',
    'uttara sector 10': 'Dhaka', 'uttara sector 13': 'Dhaka',
    'uttara sector 15': 'Dhaka', 'dhaka sadar': 'Dhaka',
    # ── Dhaka division (outside city) ────────────────────────────────────
    'gazipur': 'Dhaka', 'kapasia': 'Dhaka', 'kaliakair': 'Dhaka',
    'sreepur': 'Dhaka', 'tongi': 'Dhaka', 'kaliganj': 'Dhaka',
    'kashimpur': 'Dhaka', 'ashulia': 'Dhaka', 'savar': 'Dhaka',
    'keraniganj': 'Dhaka', 'zanjira': 'Dhaka',
    'narayanganj': 'Dhaka', 'rupganj': 'Dhaka',
    'narsingdi': 'Dhaka', 'manikganj': 'Dhaka', 'munshiganj': 'Dhaka',
    'faridpur': 'Dhaka', 'rajbari': 'Dhaka', 'rajbari sadar': 'Dhaka',
    'madaripur': 'Dhaka', 'shariatpur': 'Dhaka', 'gopalganj': 'Dhaka',
    'tangail': 'Dhaka',
    # ── Chattogram division ───────────────────────────────────────────────
    'chattogram': 'Chattogram', 'chattogram sadar': 'Chattogram',
    'chittagong': 'Chattogram',
    'cumilla': 'Chattogram', 'cumilla sadar': 'Chattogram',
    'chandpur': 'Chattogram', 'lakshmipur': 'Chattogram',
    'noakhali': 'Chattogram', 'feni': 'Chattogram',
    'hatiya': 'Chattogram', 'mirsharai': 'Chattogram',
    "cox's bazar": 'Chattogram', "cox's bazar sadar": 'Chattogram',
    "cox`s bazar": 'Chattogram', 'teknaf': 'Chattogram', 'ukhia': 'Chattogram',
    'khagrachhari': 'Chattogram', 'rangamati': 'Chattogram',
    'bandarban': 'Chattogram',
    # ── Sylhet division ───────────────────────────────────────────────────
    'sylhet': 'Sylhet', 'sylhet sadar': 'Sylhet',
    'moulvibazar': 'Sylhet', 'sreemangal': 'Sylhet',
    'sunamganj': 'Sylhet', 'jaintapur': 'Sylhet',
    'kanaighat': 'Sylhet', 'habiganj': 'Sylhet',
    # ── Rajshahi division ─────────────────────────────────────────────────
    'rajshahi': 'Rajshahi', 'rajshahi sadar': 'Rajshahi',
    'bogura': 'Rajshahi', 'bogura sadar': 'Rajshahi',
    'natore': 'Rajshahi', 'natore sadar': 'Rajshahi',
    'pabna': 'Rajshahi', 'pabna sadar': 'Rajshahi', 'ishwardi': 'Rajshahi',
    'sirajganj': 'Rajshahi', 'joypurhat': 'Rajshahi', 'joypurhat sadar': 'Rajshahi',
    'chapainawabganj': 'Rajshahi', 'nawabganj': 'Rajshahi',
    # ── Khulna division ───────────────────────────────────────────────────
    'khulna': 'Khulna', 'khulna sadar': 'Khulna',
    'jashore': 'Khulna', 'satkhira': 'Khulna',
    'bagerhat': 'Khulna', 'chuadanga': 'Khulna', 'chuadanga sadar': 'Khulna',
    'jhenaidah': 'Khulna', 'jhenaidah sadar': 'Khulna',
    'magura': 'Khulna', 'narail': 'Khulna', 'meherpur': 'Khulna',
    'kushtia': 'Khulna',
    # ── Barishal division ─────────────────────────────────────────────────
    'barishal': 'Barishal', 'barishal sadar': 'Barishal',
    'barguna': 'Barishal', 'patuakhali': 'Barishal', 'bhola': 'Barishal',
    'pirojpur': 'Barishal', 'jhalokati': 'Barishal',
    # ── Rangpur division ──────────────────────────────────────────────────
    'rangpur': 'Rangpur', 'dinajpur': 'Rangpur', 'parbatipur': 'Rangpur',
    'lalmonirhat': 'Rangpur', 'kurigram': 'Rangpur',
    'gaibandha': 'Rangpur', 'nilphamari': 'Rangpur', 'thakurgaon': 'Rangpur',
    'panchagarh': 'Rangpur',
    # ── Mymensingh division ───────────────────────────────────────────────
    'mymensingh': 'Mymensingh', 'netrokona': 'Mymensingh',
    'jamalpur': 'Mymensingh', 'sherpur': 'Mymensingh',
    'bhaluka': 'Mymensingh', 'katiadi': 'Mymensingh',
    # ── Remote / Abroad ───────────────────────────────────────────────────
    'anywhere in bangladesh': 'Remote',
    'bangladesh': 'Remote',
    'malaysia': 'Abroad', 'saudi arabia': 'Abroad',
    'united arab emirates': 'Abroad',
}


def _resolve_division(location: str) -> str:
    """
    Given a (possibly multi-location) string, return the division label.

    Logic:
    - Split on comma → check each token against the lookup table.
    - If all resolved tokens agree on one division → return it.
    - If one real division is mixed with 'Remote' → return the real division.
    - If multiple real divisions appear → return 'Multiple'.
    - If nothing matches → return 'Other'.
    """
    if pd.isna(location):
        return pd.NA

    parts = [p.strip().lower() for p in location.split(',')]
    divisions = set()
    for part in parts:
        div = _LOCATION_TO_DIVISION.get(part)
        if div:
            divisions.add(div)

    if not divisions:
        return 'Other'
    if len(divisions) == 1:
        return divisions.pop()
    # Strip 'Remote' — if only one real division remains, use it
    real = divisions - {'Remote'}
    if len(real) == 1:
        return real.pop()
    return 'Multiple'


def add_division_column(df: pd.DataFrame, source_col: str = 'location') -> pd.DataFrame:
    """
    Add a `division` column derived from `source_col`.
    Inserted right after `source_col`.

    Values: 'Dhaka', 'Chattogram', 'Sylhet', 'Rajshahi', 'Khulna',
            'Barishal', 'Rangpur', 'Mymensingh',
            'Remote', 'Abroad', 'Multiple', 'Other'
    """
    insert_pos = df.columns.get_loc(source_col) + 1
    df.insert(insert_pos, 'division', df[source_col].apply(_resolve_division))
    return df


# ── Experience level ───────────────────────────────────────────────────────────

def _parse_experience_level(text: str) -> str:
    """
    Categorize an experience string into Entry / Mid / Senior.

    Uses the maximum number mentioned to be conservative
    (e.g. '3 to 5 years' -> max=5 -> Mid).
    Returns None for null or unparseable input.
    """
    if pd.isna(text):
        return None

    nums = [int(x) for x in re.findall(r'\d+', text)]
    if not nums:
        return None

    max_exp = max(nums)

    if max_exp <= 2:
        return 'Entry'
    elif max_exp <= 5:
        return 'Mid'
    else:
        return 'Senior'


def add_experience_level_column(df: pd.DataFrame, source_col: str = 'experience') -> pd.DataFrame:
    """
    Add an `experience_level` column (Entry / Mid / Senior) derived from `source_col`.
    Inserted right after `source_col`.
    """
    insert_pos = df.columns.get_loc(source_col) + 1
    df.insert(insert_pos, 'experience_level', df[source_col].apply(_parse_experience_level))
    return df


# ── Master cleaning function ───────────────────────────────────────────────────

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

    # 5. Fix experience: "Na" string -> proper null
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

    # 10. Enrich with parsed columns
    if 'eduRec' in df.columns:
        df = add_edu_columns(df)
    if 'location' in df.columns:
        df = add_division_column(df)
    if 'experience' in df.columns:
        df = add_experience_level_column(df)

    return df.reset_index(drop=True)
