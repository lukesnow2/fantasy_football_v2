"""dim_manager's is_current / include_in_analysis, derived from the data."""
from src.edw_schema.edw_etl_processor import (
    ALUMNI_MIN_SEASONS, derive_manager_membership, is_placeholder_manager)

# (manager_name, last_season_year, total_seasons) as of the 2026 season.
LEAGUE_2026 = [
    ('-- hidden --', 2010, 1), ('--hidden--', 2011, 5), ('Bobby', 2018, 9),
    ('Cody Benbow', 2008, 2), ('Craig', 2026, 18), ('Erik Snow', 2026, 18),
    ('Frank', 2006, 1), ('Gabe Flores', 2026, 18), ('Gabe the Younger', 2026, 16),
    ('Israel', 2026, 20), ('Jesse Brown', 2008, 2), ('Justin', 2007, 3),
    ('Katie', 2009, 2), ('Luke S', 2026, 22), ('Nick', 2026, 15), ('Omar', 2026, 16),
    ('Ryan Pfiffer', 2005, 1), ('Trevor', 2026, 22), ('Troy Colvin', 2026, 16),
    ('Tyler', 2006, 1),
]

# The hardcoded lists this rule replaces.
CURRENT_BEFORE = {'Craig', 'Erik Snow', 'Gabe Flores', 'Gabe the Younger', 'Israel',
                  'Luke S', 'Nick', 'Omar', 'Trevor', 'Troy Colvin'}
ANALYSIS_BEFORE = CURRENT_BEFORE | {'Bobby'}


def records(rows):
    return [{'manager_name': n, 'last_season_year': last, 'total_seasons': seasons}
            for n, last, seasons in rows]


def flags(rows):
    out = derive_manager_membership(records(rows))
    return ({m['manager_name'] for m in out if m['is_current']},
            {m['manager_name'] for m in out if m['include_in_analysis']})


def test_reproduces_the_hardcoded_lists():
    current, analysis = flags(LEAGUE_2026)
    assert current == CURRENT_BEFORE
    assert analysis == ANALYSIS_BEFORE


def test_a_new_manager_in_the_latest_season_joins_automatically():
    current, analysis = flags(LEAGUE_2026 + [('Newcomer', 2026, 1)])
    assert 'Newcomer' in current
    assert 'Newcomer' in analysis


def test_a_departed_veteran_stays_in_analysis_but_is_not_current():
    # 2027 loads without Omar: 16 seasons keeps him in the history pages.
    rows = [(n, 2027 if last == 2026 and n != 'Omar' else last, s)
            for n, last, s in LEAGUE_2026]
    current, analysis = flags(rows)
    assert 'Omar' not in current
    assert 'Omar' in analysis
    assert current == CURRENT_BEFORE - {'Omar'}


def test_a_short_stint_does_not_count_as_alumni():
    _, analysis = flags([('Luke S', 2026, 22), ('Brief', 2024, ALUMNI_MIN_SEASONS - 1),
                         ('Veteran', 2024, ALUMNI_MIN_SEASONS)])
    assert analysis == {'Luke S', 'Veteran'}


def test_placeholders_are_never_current_or_analyzed():
    current, analysis = flags([('Luke S', 2026, 22), ('--hidden--', 2026, 12),
                               ('-- hidden --', 2026, 1)])
    assert current == {'Luke S'}
    assert analysis == {'Luke S'}


def test_placeholder_names():
    assert is_placeholder_manager('--hidden--')
    assert is_placeholder_manager('-- hidden --')
    assert is_placeholder_manager('-- Hidden --')
    assert not is_placeholder_manager('Hiddenite')
    assert not is_placeholder_manager('Luke S')
    assert not is_placeholder_manager('')


def test_no_seasons_means_no_one_is_current():
    out = derive_manager_membership([{'manager_name': 'X', 'last_season_year': None,
                                      'total_seasons': 0}])
    assert not out[0]['is_current'] and not out[0]['include_in_analysis']
