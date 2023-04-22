# Build the World Cup database from source

## Setup the environment

Import the dependencies.

```python
import os
import shutil
import requests
import pandas as pd
import polars as pl
import duckdb
```

Report the versions.

```python
%load_ext watermark
%watermark -iv
```

Initialise the database.

```python
db_path = '../data/worldcup.duckdb'

if os.path.exists(db_path):
    os.remove(db_path)

conn = duckdb.connect(db_path)
```

## Functions

```python
def get_path(
        csv_f: str,
        data_dir: str = '../data/raw',
        gh_repo: str = (
            'https://raw.githubusercontent.com/jfjelstul/worldcup'
            + '/master/data-csv'
        )
) -> str:
    """Return the relative path to the CSV file.
    If the file does not exists, then it first caches it to the given
    data_dir.
    """
    local_path = f'{data_dir}/{csv_f}.csv'
    if not os.path.exists(local_path):
        resp = requests.get(f'{gh_repo}/{csv_f}.csv', allow_redirects=True)
        with open(local_path, 'wb') as f:
            f.write(resp.content)

    assert os.path.exists(local_path)
    return local_path
```

```python
def report_schema(conn, tbl) -> pd.DataFrame:
    return (
        conn.execute(f"""
            SELECT
                table_name AS table,
                ordinal_position AS "#",
                column_name AS column,
                data_type AS dtype
            FROM
                information_schema.columns
            WHERE
                table_name = '{tbl}'
        """)
        .df()
    )
```

```python
def load_ldf(
        conn: duckdb.DuckDBPyConnection,
        tbl_name: str,
        ldf: pl.LazyFrame,
        verbose=True
) -> pd.DataFrame | None:
    conn.execute(f"""
        INSERT INTO {tbl_name}
            SELECT * FROM ldf
    """)
    if verbose:
        return report_schema(conn, f'{tbl_name}')
```

```python
def preview_tbl(conn, tbl_name,) -> pd.DataFrame:
    return (
        conn.execute(f"""
            SELECT * FROM {tbl_name} LIMIT 5
        """)
        .df()
    )
```

## Extract-Transform


### `award`

( **id** )

The awards distributed at the World Cup by FIFA.

```python
# EXTRACT-TRANSFORM
award_ldf = (
    pl.read_csv(
        get_path('awards'),
        columns=[*range(1, 5)]
    )
    .lazy()
)

# CREATE THE TABLE
conn.execute(
    """CREATE OR REPLACE TABLE award (
        id             TEXT PRIMARY KEY,
        name           TEXT,
        description    TEXT,
        year_introuced INTEGER
    );
    """
)

# LOAD THE LAZYFRAME
load_ldf(conn, 'award', award_ldf)
```

### `city`

( **id** )

Cities that have hosted World Cup matches.

This is a new table taken from the `stadiums`.

```python
# EXTRACT-TRANSFORM
city_ldf = (
    pl.read_csv(
        get_path('stadiums'),
        columns=[3, 4, 7]
    )
    .unique()
    .with_row_count(offset=1)
    .select(
        ('CTY-' + pl.col('row_nr').cast(str)).alias('id'),
        'city_name',
        'country_name',
        'city_wikipedia_link'
    )
    .lazy()
)

# CREATE THE TABLE
conn.execute(
    """CREATE OR REPLACE TABLE city (
        id             TEXT PRIMARY KEY,
        name           TEXT,
        country_name   TEXT,
        wikipedia_link TEXT
    );
    """
)

# LOAD THE LAZYFRAME
load_ldf(conn, 'city', city_ldf)
```

### `confederation`

( **id** )

Football confederations.

```python
# EXTRACT-TRANSFORM
confederation_ldf = (
    pl.read_csv(
        get_path('confederations'),
        columns=[*range(1, 5)]
    )
    .lazy()
    .select(
        'confederation_id',
        'confederation_code',
        'confederation_name',
        'confederation_wikipedia_link'
    )
)

# CREATE THE TABLE
conn.execute(
    """CREATE OR REPLACE TABLE confederation (
        id             TEXT PRIMARY KEY,
        code           TEXT,
        name           TEXT,
        wikipedia_link Text
    );
    """
)

# LOAD THE LAZYFRAME
load_ldf(conn, 'confederation', confederation_ldf)
```

### `event`

( **id** )

Events that occur in football matches.

The *orig_name* column contains the column names in the original `bookings`, `goals`, and `substitutions` tables.
It is dropped before being loaded into the database.

This is a new table.

```python
# INITIALISE THE LAZYFRAME
event_type_ldf = (
    pl.DataFrame({
        'orig_name': [
            'goal',
            'own goal',
            'penalty',
            'going_off',
            'coming_on',
            'yellow_card',
            'second_yellow_card',
            'red_card'
        ],
    })
    .lazy()
    .with_row_count(offset=1)
    .select(
        ('EVT-' + pl.col('row_nr').cast(str)).alias('id'),
        pl.col('orig_name').str.replace('_', ' ').alias('name'),
        (
            pl.when(pl.col('orig_name').is_in(['goal', 'own goal', 'penalty']))
            .then('goal')
            .when(pl.col('orig_name').is_in(['going_off', 'coming_on']))
            .then('substitution')
            .otherwise('booking')
            .alias('type')
        ),
        'orig_name'
    )
)

# CREATE THE TABLE
conn.execute(
    """CREATE OR REPLACE TABLE event_type (
        id         TEXT PRIMARY KEY,
        name       TEXT,
        super_type TEXT
    );
    """
)

# LOAD THE LAZYFRAME
load_ldf(conn, 'event_type', event_type_ldf.drop('orig_name'))
```

### `federation`

( **id**, *confederation_id* )

Football federations.

This is a new table taken from the `teams`.
This was implemented to reduce the width of the `teams` table, as it is not desirable to always return the federation and confederation details when reporting on a team.

```python
# EXTRACT-TRANSFORM
federation_ldf = (
    pl.read_csv(
        get_path('teams'),
        columns=[4, 5, 6, 10]
    )
    .lazy()
    .unique()
    .with_row_count(offset=1)
    .select(
        ('FED-' + pl.col('row_nr').cast(str)).alias('id'),
        'federation_name',
        (
            pl.when(pl.col('region_name') == "Europe, Asia")
            .then('Eurasia')
            .otherwise(pl.col('region_name'))
            .keep_name()
        ),
        'federation_wikipedia_link',
        'confederation_id'
    )
)

# CREATE THE TABLE
conn.execute(
    """CREATE OR REPLACE TABLE federation (
        id               TEXT PRIMARY KEY,
        name             TEXT,
        region_name      TEXT,
        wikipedia_link   TEXT,
        confederation_id TEXT REFERENCES confederation (id)
    );
    """
)

# LOAD THE LAZYFRAME
load_ldf(conn, 'federation', federation_ldf)
```

### `manager`

( **id** )

The manager's that have attended the World Cup.

```python
# EXTRACT-TRANSFORM
manager_ldf = (
    pl.read_csv(
        get_path('managers'),
        columns=[*range(1, 5)]
    )
    .lazy()
)

# CREATE THE TABLE
conn.execute(
    """CREATE OR REPLACE TABLE manager (
        id           TEXT PRIMARY KEY,
        family_name  TEXT,
        given_type   TEXT,
        country_name TEXT
    );
    """
)

# LOAD THE LAZYFRAME
load_ldf(conn, 'manager', manager_ldf, verbose=True)
```

### `position_type`

( **id** )

Types of positions.

This is a new table.
It was implemented to represent the Boolean columns found in the `players` table.
It also acts as a super type for the positions that are used in the `player_appearances` table, which are more granular.

```python
# INITIALISE THE LAZYFRAME
position_type_ldf = (
    pl.DataFrame({
        'id': [
            'PTYP-1',
            'PTYP-2',
            'PTYP-3',
            'PTYP-4',
        ],
        'code': [
            'GK',
            'DF',
            'MF',
            'FW'
        ],
        'name': [
            'Goal Keeper',
            'Defence',
            'Midfield',
            'Forward'
        ]
    })
    .lazy()
)

# CREATE THE TABLE
conn.execute(
    """CREATE OR REPLACE TABLE position_type (
        id    TEXT PRIMARY KEY,
        code  TEXT,
        name  TEXT,
    );
    """
)

# LOAD THE LAZYFRAME
load_ldf(conn, 'position_type', position_type_ldf, verbose=True)
```

### `position`

( **id**, *position_type_id* )

Positions a player takes in a team.

This a new table.
It was implemented to hold the *position_code*, *position_name* columns in `player_appearances`.

```python
# EXTRACT-TRANSFORM
position_ldf = (
    pl.read_csv(
        get_path('player_appearances'),
        columns=[17, 18],
    )
    .lazy()
    .unique()
    .with_row_count(offset=1)
    .select(
        ('POS-' + pl.col('row_nr').cast(str)).alias('id'),
        'position_code',
        'position_name',
        (
            pl.when(pl.col('position_code') == 'GK').then('PTYP-1')
            .when(
                pl.col('position_code')
                .is_in(['DF', 'RB', 'LB', 'CB', 'SW', 'RWB', 'LWB'])
            )
            .then('PTYP-2')
            .when(
                pl.col('position_code')
                .is_in(['FW', 'CF', 'SS', 'LF', 'RF'])
            )
            .then('PTYP-4')
            .otherwise('PTYP-3')
            .alias('position_type_id')
        )
    )
)

# CREATE THE TABLE
conn.execute(
    """CREATE OR REPLACE TABLE position (
        id               TEXT PRIMARY KEY,
        code             TEXT,
        name             TEXT,
        position_type_id TEXT REFERENCES position_type (id)
    );
    """
)

# LOAD THE LAZYFRAME
load_ldf(conn, 'position', position_ldf, verbose=True)
```

### `player`

( **id** )

The players that have appeared at the World Cup.

```python
# EXTRACT-TRANFROM
player_ldf = (
    pl.read_csv(
        get_path('players'),
        columns=[*range(1, 5)] + [9, 11],
        try_parse_dates=True
    )
    .lazy()
)

# CREATE THE TABLE
conn.execute(
    """CREATE OR REPLACE TABLE player (
        id             TEXT PRIMARY KEY,
        family_name    TEXT,
        given_type     TEXT,
        birth_date     DATE,
        n_tournaments  INTEGER,
        wikipedia_link TEXT
    );
    """
)

# LOAD THE LAZYFRAME
load_ldf(conn, 'player', player_ldf)
```

### `referee`

( **id**, *confederation_id* )

The referees that have appeared at the World Cup.

```python
# EXTRACT-TRANSFORM
referee_ldf = (
    pl.read_csv(
        get_path('referees'),
        columns=[*range(1, 6)] + [8],
        try_parse_dates=True
    )
    .lazy()
)

# CREATE THE TABLE
conn.execute(
    """CREATE OR REPLACE TABLE referee (
        id               TEXT PRIMARY KEY,
        family_name      TEXT,
        given_type       TEXT,
        country_name     TEXT,
        confederation_id TEXT,
        wikipedia_link   TEXT,
    );
    """
)

# LOAD THE LAZYFRAME
load_ldf(conn, 'referee', referee_ldf)
```

### `stadium`

( **id**, *city_id* )

The stadiums have have hosted World Cup matches.

```python
# EXTRACT-TRANSFORM
stadium_ldf = (
    pl.read_csv(
        get_path('stadiums'),
        columns=[*range(1, 8)]
    )
    .lazy()
    .join(
        city_ldf,
        on='city_name'
    )
    .select(
        'stadium_id',
        'stadium_name',
        'stadium_capacity',
        'stadium_wikipedia_link',
        pl.col('id').alias('city_id')
    )
)

# CREATE THE TABLE
conn.execute(
    """CREATE OR REPLACE TABLE stadium (
        id             TEXT PRIMARY KEY,
        name           TEXT,
        capacity       INTEGER,
        wikipedia_link TEXT,
        city_id        TEXT REFERENCES city (id)
    );
    """
)

# LOAD THE LAZYFRAME
load_ldf(conn, 'stadium', stadium_ldf)
```

### `stage`

( **id** )

The possible stages at a World Cup.

This is a new table.
It was implemented to reduce duplication.

```python
# EXTRACT-TRANSFORM
stage_ldf = (
    pl.read_csv(
        get_path('tournament_stages'),
        columns=[4, 5]
    )
    .lazy()
    .unique()
    .with_row_count(offset=1)
    .select(
        ('STG-' + pl.col('row_nr').cast(str)).alias('id'),
        (
            pl.col('stage_name')
            .apply(lambda s: s[0].upper() + s[1:])
            .cast(str)
            .alias('name')
        ),
        (
            pl.when(pl.col('group_stage') == 1)
            .then('Group')
            .otherwise('Knockout')
            .cast(str)
            .alias('type')
        ),
        pl.col('stage_name').alias('orig_stage_name'),
    )
)

# CREATE THE TABLE
conn.execute(
    """CREATE OR REPLACE TABLE stage (
        id   TEXT PRIMARY KEY,
        name TEXT,
        type TEXT
    );
    """
)

# LOAD THE LAZYFRAME
load_ldf(conn, 'stage', stage_ldf.drop('orig_stage_name'))
```

### `team`

( **id**, *federation_id* )

The teams have appeared at the World Cup.

```python
# EXTRACT-TRANSFORM
team_ldf = (
    pl.read_csv(
        get_path('teams'),
        columns=[1, 2, 3, 4, 9]
    )
    .lazy()
    .join(
        federation_ldf,
        on='federation_name'
    )
    .select(
        'team_id',
        'team_code',
        'team_name',
        'team_wikipedia_link',
        pl.col('id').alias('federation_id')
    )
)

# CREATE THE TABLE
conn.execute(
    """CREATE OR REPLACE TABLE team (
        id             TEXT PRIMARY KEY,
        code           TEXT,
        name           TEXT,
        wikipedia_link TEXT,
        federation_id  TEXT
    );
    """
)

# LOAD THE LAZYFRAME
load_ldf(conn, 'team', team_ldf)
```

### `tournament`

( **id** )

```python
# EXTRACT-TRANSFORM
tournament_ldf = (
    pl.read_csv(
        get_path('tournaments'),
        columns=[*range(1, 10)],
        try_parse_dates=True
    )
    .lazy()
    .join(
        team_ldf.select('team_id', 'team_name'),
        left_on='winner',
        right_on='team_name'
    )
    .rename({
        'team_id': 'winning_team_id'
    })
    .drop([
        'winner',
        'host_won',
        'host_country',
    ])
)

# CREATE THE TABLE
conn.execute(
    """CREATE OR REPLACE TABLE tournament (
        id             TEXT PRIMARY KEY,
        name           TEXT,
        year           INTEGER,
        start_date     DATE,
        end_date       DATE,
        n_team         INTEGER,
        wining_team_id TEXT REFERENCES team (id)
    );
    """
)

# LOAD THE LAZYFRAME
load_ldf(conn, 'tournament', tournament_ldf)
```

### `tournament_referee`

( ***tournament_id***, ***referee_id*** )

The relationship between tournaments and the referees that attended.

```python
# EXTRACT-TRANSFORM
tournament_referee_ldf = (
    pl.read_csv(
        get_path('referee_appointments'),
        columns=[1, 3],
    )
    .lazy()
)
tournament_referee_ldf.schema

# CREATE THE TABLE
conn.execute(
    """CREATE OR REPLACE TABLE tournament_referee (
        tournament_id TEXT REFERENCES tournament (id),
        referee_id    TEXT REFERENCES referee (id),
        PRIMARY KEY (tournament_id, referee_id)
    );
    """
)

# LOAD THE LAZYFRAME
load_ldf(conn, 'tournament_referee', tournament_referee_ldf)
```

### `tournament_schedule`

( ***tournament_id***, ***stage_id***, ***stage_detail*** )

```python
# ExTRACT-TRANSFORM
tournament_schedule_ldf = (
    pl.read_csv(
        get_path('matches'),
        columns=[1, 5, 6],
    )
    .join(
        pl.read_csv(
            get_path('tournament_stages'),
            columns=[1, 3, 4]+[*range(8, 16)],
            try_parse_dates=True,
        ),
        on=['tournament_id', 'stage_name']
    )
    .lazy()
    .unique()
    .join(
        stage_ldf.rename({'id': 'stage_id'}),
        left_on='stage_name',
        right_on='orig_stage_name'
    )
    .select(
        'tournament_id',
        'stage_id',
        (
            pl.when(pl.col('group_name').str.contains('Group'))
            .then(pl.col('group_name'))
            .otherwise(
                pl.when(pl.col('stage_name').str.ends_with('s'))
                .then(
                    pl.col('stage_name')
                    .apply(lambda s: s[0].upper() + s[1:-1])
                )
                .otherwise(
                    pl.col('stage_name')
                    .apply(lambda s: s[0].upper() + s[1:])
                )
            )
            .cast(str)
            .alias('stage_detail')
        ),
        pl.col('stage_number').alias('sort_order'),
        'start_date',
        'end_date',
        'count_matches',
        'count_teams',
        'count_scheduled',
        'count_replays',
        'count_playoffs',
        'count_walkovers',
    )
)

# CREATE THE TABLE
conn.execute(
    """CREATE OR REPLACE TABLE tournament_schedule (
        tournament_id TEXT REFERENCES tournament (id),
        stage_id      TEXT REFERENCES stage (id),
        stage_detail  TEXT,
        sort_order    INTEGER,
        start_date    DATE,
        end_date      DATE,
        n_matches     INTEGER,
        n_teams       INTEGER,
        n_scheduled   INTEGER,
        n_replays     INTEGER,
        n_playoffs    INTEGER,
        n_walkovers   INTEGER,
        PRIMARY KEY (tournament_id, stage_id, stage_detail)
    );
    """
)

# LOAD THE LAZYFRAME
load_ldf(conn, 'tournament_schedule', tournament_schedule_ldf)
```

### `tournament_manager`

( ***tournament_id***, ***manager_id***, *team_id* )

```python
# EXTRACT-TRANSFORM
tournament_manager_ldf = (
    pl.read_csv(
        get_path('manager_appointments'),
        columns=[1, 3, 6],
    )
    .lazy()
)

# CREATE THE TABLE
conn.execute(
    """CREATE OR REPLACE TABLE tournament_manager (
        tournament_id TEXT REFERENCES tournament (id),
        team_id       TEXT REFERENCES team (id),
        manager_id    TEXT REFERENCES manager (id),
        PRIMARY KEY (tournament_id, manager_id)
    );
    """
)

# LOAD THE LAZYFRAME
load_ldf(conn, 'tournament_manager', tournament_manager_ldf)
```

### `tournament_squad`

( ***tournament_id***, ***team_id***, ***player_id***, *position_id* )

```python
tournament_team_player_ldf = (
    pl.read_csv(
        get_path('squads'),
        columns=[1, 3, 6, 9, 10, 11],
    )
    .lazy()
    .with_columns(
        pl.when(pl.col('shirt_number') != 0).then(pl.col('shirt_number'))
        .keep_name()
    )
    .join(
        position_ldf,
        on='position_name'
    )
    .select(
        'tournament_id',
        'team_id',
        'player_id',
        pl.col('shirt_number').cast(str),
        pl.col('id').alias('position_id')
    )
)
tournament_team_player_ldf.schema

# CREATE THE TABLE
conn.execute(
    """CREATE OR REPLACE TABLE tournament_squad (
        tournament_id TEXT REFERENCES tournament (id),
        team_id       TEXT REFERENCES team (id),
        player_id     TEXT REFERENCES player (id),
        shirt_number  TEXT,
        position_id   TEXT REFERENCES position (id),
        PRIMARY KEY (tournament_id, team_id, player_id)
    );
    """
)

# LOAD THE LAZYFRAME
load_ldf(conn, 'tournament_squad', tournament_team_player_ldf)
```

### `tournament_team`

( ***tournament_id***, ***team_id*** )

```python
# EXTRACT-TRANSFORM
tournament_team_ldf = (
    pl.read_csv(
        get_path('qualified_teams'),
        columns=[1, 3, 6, 7],
    )
    .join(
        pl.read_csv(
            get_path('host_countries'),
            columns=[1, 3, 4]
        ),
        on=['tournament_id', 'team_id'],
        how='left'
    )
    .select(
        'tournament_id',
        'team_id',
        'count_matches',
        'performance',
        (
            pl.when(pl.col('team_name').is_null()).then(False)
            .otherwise(True)
            .cast(bool)
            .keep_name()
        )
    )
)

# CREATE THE TABLE
conn.execute(
    """CREATE OR REPLACE TABLE tournament_team (
        tournament_id TEXT REFERENCES tournament (id),
        team_id       TEXT REFERENCES team (id),
        n_matches     INTEGER,
        performance   TEXT,
        is_host       BOOL,
        PRIMARY KEY (tournament_id, team_id)
    );
    """
)

# LOAD THE LAZYFRAME
load_ldf(conn, 'tournament_team', tournament_team_ldf)
```

### `match`

( **id**, *tournament_id*, *stage_id*, *home_team_id*, *away_team_id*, *stadium_id* )

```python
# EXTRACT-TRANSFORM
match_ldf = (
    pl.read_csv(
        get_path('matches'),
    )
    .lazy()
    .join(
        stage_ldf.rename({'id': 'stage_id'}),
        left_on='stage_name',
        right_on='orig_stage_name'
    )
    .select(
        'match_id',
        'tournament_id',
        'stage_id',
        (
            pl.when(pl.col('group_name').str.contains('Group'))
            .then(pl.col('group_name'))
            .otherwise(
                pl.when(pl.col('stage_name').str.ends_with('s'))
                .then(
                    pl.col('stage_name')
                    .apply(lambda s: s[0].upper() + s[1:-1])
                )
                .otherwise(
                    pl.col('stage_name')
                    .apply(lambda s: s[0].upper() + s[1:])
                )
            )
            .cast(str)
            .alias('stage_detail')
        ),
        'home_team_id',
        'away_team_id',
        'match_name',
        (
            (pl.col('match_date') + ' ' + pl.col('match_time'))
            .str.strptime(pl.Datetime, format='%Y-%m-%d %H:%M')
            .alias('datetime')
        ),
        'stadium_id',
        (
            pl.when(pl.col('extra_time') + pl.col('penalty_shootout') == 0)
            .then('FT')
            .when(pl.col('extra_time') + pl.col('penalty_shootout') == 1)
            .then('ET')
            .otherwise('PS')
            .alias('completed')
        ),
        'result',
        'score',
        'home_team_score',
        'away_team_score',
        (
            pl.when(pl.col('penalty_shootout') == 1)
            .then(pl.col('score_penalties'))
            .alias('penalty_shootout_score')
        ),
        (
            pl.when(pl.col('penalty_shootout') == 1)
            .then(pl.col('home_team_score_penalties'))
            .alias('home_team_penalty_shootout_score')
        ),
        (
            pl.when(pl.col('penalty_shootout') == 1)
            .then(pl.col('away_team_score_penalties'))
            .alias('away_team_penalty_shootout_score')
        ),
    )
)

# CREATE THE TABLE
conn.execute(
    """CREATE OR REPLACE TABLE match (
        id                      TEXT PRIMARY KEY,
        tournament_id           TEXT REFERENCES tournament (id),
        stage_id                TEXT REFERENCES stage (id),
        stage_detail            TEXT,
        home_team_id            TEXT REFERENCES team (id),
        away_team_id            TEXT REFERENCES team (id),
        name                    TEXT,
        datetime                DATETIME,
        stadium_id              TEXT REFERENCES stadium (id),
        completed               TEXT,
        result                  TEXT,
        score                   TEXT,
        home_team_score         INTEGER,
        away_team_score         INTEGER,
        penalty_shootout_score  TEXT,
        home_team_penalty_score INTEGER,
        away_team_penalty_score INTEGER,
    );
    """
)

# LOAD THE LAZYFRAME
load_ldf(conn, 'match', match_ldf)
```

### `event`

( **id**, *match_id*, *team_id*, *player_id*, *event_id* )

```python
event_ldf = (
    pl.concat(
        items=[
            (
                pl.read_csv(
                    get_path('goals'),
                    columns=[4, 9, 14, 21, 22, 23, 24, 25, 26]
                )
                .with_columns(
                    (
                        pl.when(pl.col('own_goal') == 1).then('own goal')
                        .when(pl.col('penalty') == 1).then('penalty')
                        .otherwise('goal')
                        .alias('variable')
                    )
                )
            ),
            (
                pl.read_csv(
                    get_path('bookings'),
                    columns=[4, 9, 14, 18, 19, 20, 21, 22, 23, 24]
                )
                .melt(
                    id_vars=[
                        'match_id',
                        'team_id',
                        'player_id',
                        'minute_label',
                        'minute_regulation',
                        'minute_stoppage',
                        'match_period',
                    ],
                )
                .filter(pl.col('value') == 1)
            ),
            (
                pl.read_csv(
                    get_path('substitutions'),
                    columns=[4, 9, 14, 18, 19, 20, 21, 22, 23]
                )
                .melt(
                    id_vars=[
                        'match_id',
                        'team_id',
                        'player_id',
                        'minute_label',
                        'minute_regulation',
                        'minute_stoppage',
                        'match_period',
                    ],
                )
                .filter(pl.col('value') == 1)
            ),
         ],
        how='diagonal'
    )
    .lazy()
    .join(
        event_type_ldf,
        left_on='variable',
        right_on='orig_name'
    )
    .with_row_count(offset=1)
    .select(
        ('EV-' + pl.col('row_nr').cast(str)).alias('id'),
        'match_id',
        'team_id',
        'player_id',
        pl.col('id').alias('event_type_id'),
        'minute_label',
        'minute_regulation',
        'minute_stoppage',
        'match_period',
    )
)

# CREATE THE TABLE
conn.execute(
   """CREATE OR REPLACE TABLE event (
        id                TEXT PRIMARY KEY,
        match_id          TEXT REFERENCES match (id),
        team_id           TEXT REFERENCES team (id),
        player_id         TEXT REFERENCES player (id),
        event_type_id     TEXT REFERENCES event_type (id),
        minute_label      TEXT,
        minute_regulation INTEGER,
        minute_stoppage   INTEGER,
        match_period      TEXT
    );
    """
)

# LOAD THE LAZYFRAME
load_ldf(conn, 'event', event_ldf)
```

### `penalty_kick`

( **id**, *match_id*, *team_id*, *player_id* )

```python
# EXTRACT-TRANSFORM
penalty_kick_ldf = (
    pl.read_csv(
        get_path('penalty_kicks'),
        columns=[1, 4, 9, 14, 18]
    )
    .lazy()
    .with_columns(
        pl.col('converted').cast(bool)
    )
)

# CREATE THE TABLE
conn.execute(
   """CREATE OR REPLACE TABLE penalty_kick (
        id          TEXT PRIMARY KEY,
        match_id    TEXT REFERENCES match (id),
        team_id     TEXT REFERENCES team (id),
        player_id   TEXT REFERENCES player (id),
        did_convert BOOLEAN
    );
    """
)

# LOAD THE LAZYFRAME
load_ldf(conn, 'penalty_kick', penalty_kick_ldf)
```

### `match_player`

( ***match_id***, ***player_id***, *position_id*, *team_id* )

```python
match_player_ldf = (
    pl.read_csv(
        get_path('player_appearances'),
        columns=[3, 8, 13, 17, 18, 19, 21],
    )
    .lazy()
    .join(
        position_ldf.rename({'id': 'position_id'}),
        on='position_code'
    )
    .select(
        'match_id',
        'team_id',
        'player_id',
        'position_id',
        pl.col('starter').cast(bool),
        pl.col('captain').cast(bool)
    )
)

# CREATE THE TABLE
conn.execute(
   """CREATE OR REPLACE TABLE match_player (
        match_id    TEXT REFERENCES match (id),
        team_id     TEXT REFERENCES team (id),
        player_id   TEXT REFERENCES player (id),
        position_id TEXT REFERENCES position (id),
        is_starter  BOOLEAN,
        is_captain  BOOLEAN,
        PRIMARY KEY (match_id, player_id)
    );
    """
)

# LOAD THE LAZYFRAME
load_ldf(conn, 'match_player', match_player_ldf)
```

### `match_replay`

( ***first_match_id***, ***second_match_id*** )

```python
match_replay_ldf = (
    pl.read_csv(
        get_path('matches'),
        columns=[1, 3, 9, 17, 20]
    )
    .filter(pl.col('replayed') == 1)
    .join(
        (
            pl.read_csv(
                get_path('matches'),
                columns=[1, 3, 10, 17, 20]
            )
            .filter(pl.col('replay') == 1)
        ),
        on=['home_team_id', 'away_team_id']
    )
    .lazy()
    .select(
        'match_id',
        'match_id_right'
    )
)

# CREATE THE TABLE
conn.execute(
   """CREATE OR REPLACE TABLE match_replay (
        first_match_id  TEXT REFERENCES match (id),
        second_match_id TEXT REFERENCES match (id),
        PRIMARY KEY (first_match_id, second_match_id)
    );
    """
)

# LOAD THE LAZYFRAME
load_ldf(conn, 'match_replay', match_replay_ldf)
```

### `match_team`

(***match_id***, ***team_id***)

```python
team_appearance_ldf = (
    pl.read_csv(
        get_path('team_appearances'),
        columns=[3, 17, 20, 23, 25, 26, 27, 28, 29, 30, 31, 32]
    )
    .lazy()
    .select(
        'match_id',
        'team_id',
        'opponent_id',
        (
            pl.when(pl.col('home_team') == 1).then('home')
            .otherwise('away')
            .alias('home')
        ),
        'result',
        'goals_for',
        'goals_against',
        'goal_differential',
        (
            pl.when(pl.col('penalty_shootout') == 1)
            .then(pl.col('penalties_for'))
            .alias('penalties_for')
        ),
        (
            pl.when(pl.col('penalty_shootout') == 1)
            .then(pl.col('penalties_against'))
            .alias('penalties_against')
        ),
        (
            pl.when(pl.col('penalty_shootout') == 1)
            .then(pl.col('penalties_for') - pl.col('penalties_against'))
            .alias('penalties_differential')
        ),
    )
)
team_appearance_ldf.schema

# CREATE THE TABLE
conn.execute(
    """CREATE OR REPLACE TABLE team_appearance (
        match_id               TEXT,
        team_id                TEXT REFERENCES team (id),
        opponent_team_id       TEXT REFERENCES team (id),
        home_away              TEXT,
        result                 TEXT,
        goals_for              INTEGER,
        goals_againt           INTEGER,
        goal_differential      INTEGER,
        penalties_for          INTEGER,
        penalties_againt       INTEGER,
        penalties_differential INTEGER,
        PRIMARY KEY (match_id, team_id)
    );
    """
)

# LOAD THE LAZYFRAME
load_ldf(conn, 'team_appearance', team_appearance_ldf)
```

### `award_winner`

( ***tournament_id***, ***award_id***, ***player_id*** )

```python
award_winner_ldf = (
    pl.read_csv(
        get_path('award_winners'),
        columns=[1, 3, 6]
    )
    .lazy()
)
award_winner_ldf.schema

# CREATE THE TABLE
conn.execute(
    """CREATE OR REPLACE TABLE award_winner (
        tournament_id TEXT,
        award_id      TEXT,
        player_id     TEXT,
        PRIMARY KEY (tournament_id, award_id, player_id)
    );
    """
)

# LOAD THE LAZYFRAME
load_ldf(conn, 'award_winner', award_winner_ldf)
```

### `tournament_group_stage_summary`

( ***tournament_id***, ***stage_id***, **stage_detail**, ***team_id*** )

```python
tournament_team_group_performance_ldf = (
    pl.read_csv(
        get_path('group_standings'),
    )
    .lazy()
    .join(
        stage_ldf.rename({'id': 'stage_id'}),
        left_on='stage_name',
        right_on='orig_stage_name'
    )
    .select(
        'tournament_id',
        'stage_id',
        pl.col('group_name').alias('stage_detail'),
        'team_id',
        'position',
        'played',
        'wins',
        'draws',
        'losses',
        'goals_for',
        'goals_against',
        'goal_difference',
        'points',
        pl.col('advanced').cast(bool)
    )
)

# CREATE THE TABLE
conn.execute(
    """CREATE OR REPLACE TABLE tournament_team_group_performance (
        tournament_id   TEXT REFERENCES tournament (id),
        stage_id        TEXT REFERENCES stage (id),
        stage_detail    TEXT,
        team_id         TEXT REFERENCES team (id),
        position        INTEGER,
        n_played        INTEGER,
        n_wins          INTEGER,
        n_draws         INTEGER,
        n_losses        INTEGER,
        goals_for       INTEGER,
        goals_against   INTEGER,
        goal_difference INTEGER,
        points          INTEGER,
        did_advance     BOOLEAN,
        PRIMARY KEY (tournament_id, stage_id, stage_detail, team_id)
    );
    """
)

# LOAD THE LAZYFRAME
load_ldf(
    conn,
    'tournament_team_group_performance',
    tournament_team_group_performance_ldf
)
```

## Export the database to stage

We first export a backup of the database, and then we use *pandas*  to create and export a tabular version of the schema.

```python
conn.execute("EXPORT DATABASE '../data/stage';")
```

Copy the schema to the `docs` folder.

```python
shutil.copy('../data/stage/schema.sql', '../docs/schema.sql')
```



```python
(
    conn.execute(f"""
        SELECT
            table_name AS table,
            ordinal_position AS "#",
            column_name AS column,
            data_type AS dtype,
            CASE
                WHEN column_name = 'id' THEN 'PK'
                WHEN contains(column_name, '_id') THEN 'FK'
            END AS constraint
        FROM
            information_schema.columns
    """)
    .df().to_csv('../docs/schema.csv', index=False)
)
```

## Close the connection

```python
# conn.close()
```
