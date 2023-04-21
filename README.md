# World-Cup-DuckDB

 The World Cup database implemented as a DuckDB database.

## Changes

## Schema

### `award`

( **id** )

The awards distributed at the World Cup by FIFA.

### `city`

( **id** )

Cities that have hosted World Cup matches.

### `confederation`

( **id** )

Football confederations represetned at the World Cup.

### `event`

( **id** )

Events that occur in football matches.

### `federation`

( **id**, *confederation_id* )

Football federations represented at the World Cup.

### `manager`

( **id** )

Managers that have attended the World Cup.

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

### `tournament_referee`

( ***tournament_id***, ***referee_id*** )

The relationship between a tournament and a referee.

### `tournament_schedule`

( ***tournament_id***, ***stage_id***, ***stage_detail*** )
