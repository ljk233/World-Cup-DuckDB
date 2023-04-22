


CREATE TABLE award_winner(tournament_id VARCHAR, award_id VARCHAR, player_id VARCHAR, PRIMARY KEY(tournament_id, award_id, player_id));
CREATE TABLE award(id VARCHAR PRIMARY KEY, "name" VARCHAR, description VARCHAR, year_introuced INTEGER);
CREATE TABLE city(id VARCHAR PRIMARY KEY, "name" VARCHAR, country_name VARCHAR, wikipedia_link VARCHAR);
CREATE TABLE confederation(id VARCHAR PRIMARY KEY, code VARCHAR, "name" VARCHAR, wikipedia_link VARCHAR);
CREATE TABLE event_type(id VARCHAR PRIMARY KEY, "name" VARCHAR, super_type VARCHAR);
CREATE TABLE manager(id VARCHAR PRIMARY KEY, family_name VARCHAR, given_type VARCHAR, country_name VARCHAR);
CREATE TABLE position_type(id VARCHAR PRIMARY KEY, code VARCHAR, "name" VARCHAR);
CREATE TABLE player(id VARCHAR PRIMARY KEY, family_name VARCHAR, given_type VARCHAR, birth_date DATE, n_tournaments INTEGER, wikipedia_link VARCHAR);
CREATE TABLE referee(id VARCHAR PRIMARY KEY, family_name VARCHAR, given_type VARCHAR, country_name VARCHAR, confederation_id VARCHAR, wikipedia_link VARCHAR);
CREATE TABLE stage(id VARCHAR PRIMARY KEY, "name" VARCHAR, "type" VARCHAR);
CREATE TABLE team(id VARCHAR PRIMARY KEY, code VARCHAR, "name" VARCHAR, wikipedia_link VARCHAR, federation_id VARCHAR);
CREATE TABLE team_appearance(match_id VARCHAR, team_id VARCHAR, opponent_team_id VARCHAR, home_away VARCHAR, result VARCHAR, goals_for INTEGER, goals_againt INTEGER, goal_differential INTEGER, penalties_for INTEGER, penalties_againt INTEGER, penalties_differential INTEGER, FOREIGN KEY (team_id) REFERENCES team(id), FOREIGN KEY (opponent_team_id) REFERENCES team(id), PRIMARY KEY(match_id, team_id));
CREATE TABLE federation(id VARCHAR PRIMARY KEY, "name" VARCHAR, region_name VARCHAR, wikipedia_link VARCHAR, confederation_id VARCHAR, FOREIGN KEY (confederation_id) REFERENCES confederation(id));
CREATE TABLE "position"(id VARCHAR PRIMARY KEY, code VARCHAR, "name" VARCHAR, position_type_id VARCHAR, FOREIGN KEY (position_type_id) REFERENCES position_type(id));
CREATE TABLE stadium(id VARCHAR PRIMARY KEY, "name" VARCHAR, capacity INTEGER, wikipedia_link VARCHAR, city_id VARCHAR, FOREIGN KEY (city_id) REFERENCES city(id));
CREATE TABLE tournament(id VARCHAR PRIMARY KEY, "name" VARCHAR, "year" INTEGER, start_date DATE, end_date DATE, n_team INTEGER, wining_team_id VARCHAR, FOREIGN KEY (wining_team_id) REFERENCES team(id));
CREATE TABLE tournament_referee(tournament_id VARCHAR, referee_id VARCHAR, FOREIGN KEY (tournament_id) REFERENCES tournament(id), FOREIGN KEY (referee_id) REFERENCES referee(id), PRIMARY KEY(tournament_id, referee_id));
CREATE TABLE tournament_schedule(tournament_id VARCHAR, stage_id VARCHAR, stage_detail VARCHAR, sort_order INTEGER, start_date DATE, end_date DATE, n_matches INTEGER, n_teams INTEGER, n_scheduled INTEGER, n_replays INTEGER, n_playoffs INTEGER, n_walkovers INTEGER, FOREIGN KEY (tournament_id) REFERENCES tournament(id), FOREIGN KEY (stage_id) REFERENCES stage(id), PRIMARY KEY(tournament_id, stage_id, stage_detail));
CREATE TABLE tournament_team_group_performance(tournament_id VARCHAR, stage_id VARCHAR, stage_detail VARCHAR, team_id VARCHAR, "position" INTEGER, n_played INTEGER, n_wins INTEGER, n_draws INTEGER, n_losses INTEGER, goals_for INTEGER, goals_against INTEGER, goal_difference INTEGER, points INTEGER, did_advance BOOLEAN, FOREIGN KEY (tournament_id) REFERENCES tournament(id), FOREIGN KEY (stage_id) REFERENCES stage(id), FOREIGN KEY (team_id) REFERENCES team(id), PRIMARY KEY(tournament_id, stage_id, stage_detail, team_id));
CREATE TABLE "match"(id VARCHAR PRIMARY KEY, tournament_id VARCHAR, stage_id VARCHAR, stage_detail VARCHAR, home_team_id VARCHAR, away_team_id VARCHAR, "name" VARCHAR, datetime TIMESTAMP, stadium_id VARCHAR, completed VARCHAR, result VARCHAR, score VARCHAR, home_team_score INTEGER, away_team_score INTEGER, penalty_shootout_score VARCHAR, home_team_penalty_score INTEGER, away_team_penalty_score INTEGER, FOREIGN KEY (tournament_id) REFERENCES tournament(id), FOREIGN KEY (stage_id) REFERENCES stage(id), FOREIGN KEY (home_team_id) REFERENCES team(id), FOREIGN KEY (away_team_id) REFERENCES team(id), FOREIGN KEY (stadium_id) REFERENCES stadium(id));
CREATE TABLE tournament_team(tournament_id VARCHAR, team_id VARCHAR, n_matches INTEGER, performance VARCHAR, is_host BOOLEAN, FOREIGN KEY (tournament_id) REFERENCES tournament(id), FOREIGN KEY (team_id) REFERENCES team(id), PRIMARY KEY(tournament_id, team_id));
CREATE TABLE tournament_squad(tournament_id VARCHAR, team_id VARCHAR, player_id VARCHAR, shirt_number VARCHAR, position_id VARCHAR, FOREIGN KEY (tournament_id) REFERENCES tournament(id), FOREIGN KEY (team_id) REFERENCES team(id), FOREIGN KEY (player_id) REFERENCES player(id), FOREIGN KEY (position_id) REFERENCES position(id), PRIMARY KEY(tournament_id, team_id, player_id));
CREATE TABLE tournament_manager(tournament_id VARCHAR, team_id VARCHAR, manager_id VARCHAR, FOREIGN KEY (tournament_id) REFERENCES tournament(id), FOREIGN KEY (team_id) REFERENCES team(id), FOREIGN KEY (manager_id) REFERENCES manager(id), PRIMARY KEY(tournament_id, manager_id));
CREATE TABLE match_replay(first_match_id VARCHAR, second_match_id VARCHAR, FOREIGN KEY (first_match_id) REFERENCES match(id), FOREIGN KEY (second_match_id) REFERENCES match(id), PRIMARY KEY(first_match_id, second_match_id));
CREATE TABLE match_player(match_id VARCHAR, team_id VARCHAR, player_id VARCHAR, position_id VARCHAR, is_starter BOOLEAN, is_captain BOOLEAN, FOREIGN KEY (match_id) REFERENCES match(id), FOREIGN KEY (team_id) REFERENCES team(id), FOREIGN KEY (player_id) REFERENCES player(id), FOREIGN KEY (position_id) REFERENCES position(id), PRIMARY KEY(match_id, player_id));
CREATE TABLE penalty_kick(id VARCHAR PRIMARY KEY, match_id VARCHAR, team_id VARCHAR, player_id VARCHAR, did_convert BOOLEAN, FOREIGN KEY (match_id) REFERENCES match(id), FOREIGN KEY (team_id) REFERENCES team(id), FOREIGN KEY (player_id) REFERENCES player(id));
CREATE TABLE "event"(id VARCHAR PRIMARY KEY, match_id VARCHAR, team_id VARCHAR, player_id VARCHAR, event_type_id VARCHAR, minute_label VARCHAR, minute_regulation INTEGER, minute_stoppage INTEGER, match_period VARCHAR, FOREIGN KEY (match_id) REFERENCES match(id), FOREIGN KEY (team_id) REFERENCES team(id), FOREIGN KEY (player_id) REFERENCES player(id), FOREIGN KEY (event_type_id) REFERENCES event_type(id));




