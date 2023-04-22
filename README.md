# World-Cup-DuckDB

 The World Cup database implemented as a DuckDB database.

## Changes

## Schema

### `award`

( **id** )

The awards distributed at the World Cup by FIFA.

### `award_winner`

( ***tournament_id***, ***award_id***, ***player_id*** )

The relationship between a tournament, a player, and the award they won after appearing.

### `city`

( **id** )

Cities that have hosted World Cup matches.

### `confederation`

( **id** )

Football confederations represetned at the World Cup.

### `event`

( **id**, *match_id*, *team_id*, *player_id*, *event_type_id* )

An event during a match at the World Cup.

### `event_type`

( **id** )

Types of events that can occur in football matches.

### `federation`

( **id**, *confederation_id* )

Football federations represented at the World Cup.

### `manager`

( **id** )

Managers that have attended the World Cup.

### `match`

( **id**, *tournament_id*, *stage_id*, *home_team_id*, *away_team_id*, *stadium_id* )

Matches at a World Cup tournament.

### `match_player`

( ***match_id***, ***player_id***, *position_id*, *team_id* )

The relationship between a match and the players that appeared.

### `match_replay`

( ***first_match_id***, ***second_match_id*** )

The relationship between the first match and the second match, when the first match resulted in a replay.

### `match_team`

(***match_id***, ***team_id***)

The relationship between matches and the teams that played in them.

### `penalty_kick`

( **id**, *match_id*, *team_id*, *player_id* )

Penalty kicks in matches at the World Cup.

### `position`

( **id**, *position_type_id* )

Positions a player can take in a match.

### `position_type`

( **id** )

Types of positions.

### `player`

( **id** )

Players that have appeared at the World Cup.

### `referee`

( **id**, *confederation_id* )

Referees that have appeared at the World Cup.

### `stadium`

( **id**, *city_id* )

Stadiums that have hosted World Cup matches.

### `stage`

( **id** )

Stages at the World Cup.

### `team`

( **id**, *federation_id* )

Teams that have appeared at the World Cup.

### `tournament`

( **id** )

World Cup tournaments.

### `tournament_group_stage_summary`

( ***tournament_id***, ***stage_id***, **stage_detail**, ***team_id*** )

The final standings of each group at a tournament.

### `tournament_manager`

( ***tournament_id***, ***manager_id***, *team_id* )

The relationship between a tournament, a manager, and the team they managed.

### `tournament_referee`

( ***tournament_id***, ***referee_id*** )

The relationship between a tournament and a referee.

### `tournament_schedule`

( ***tournament_id***, ***stage_id***, ***stage_detail*** )

Information on each stage at a tournament.

### `tournament_squad`

( ***tournament_id***, ***team_id***, ***player_id***, *position_id* )

The relationship between a tournament, a qualified team, and their players.

### `tournament_team`

( ***tournament_id***, ***team_id*** )

The relationship between a tournament and the qualified teams.
