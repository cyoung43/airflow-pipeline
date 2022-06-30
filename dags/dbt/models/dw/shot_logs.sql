with parsed as (
    select
        parse_json(nba._AIRBYTE_DATA) as parse
    from {{ source('data', '_AIRBYTE_RAW_NBA_DATA') }} nba
)

select
    p.parse:GAME_ID::int as GAME_ID, --@col Game Id. Id the of the NBA game played
    p.parse:MATCHUP::varchar as MATCHUP,
    p.parse:LOCATION::varchar as LOCATION,
    p.parse:player_name::varchar as PLAYER_NAME, --@col Player Name. Name of the player taking the shot
    p.parse:player_id::int as PLAYER_ID, --@col ID of the player taking the shot
    p.parse:SHOT_NUMBER::int as SHOT_NUMBER, --@col Shot number the player has taken during the game
    p.parse:PERIOD::int as QUARTER,
    p.parse:GAME_CLOCK::varchar as GAME_CLOCK,
    p.parse:SHOT_CLOCK::numeric(5, 1) as SHOT_CLOCK,--@col How much time available on the shot clock when player took shot
    p.parse:DRIBBLES::int as DRIBBLES, --@col How many dribbles player took before shot [int]
    p.parse:TOUCH_TIME::numeric(5, 1) as TOUCH_TIME, --@col How long the player had the ball before shot [int]
    p.parse:SHOT_DIST::numeric(5, 1) as SHOT_DISTANCE, --@col Distance of the shot [double]
    p.parse:PTS_TYPE::int as POINTS_TYPE,
    p.parse:SHOT_RESULT::varchar as SHOT_RESULT,
    p.parse:CLOSEST_DEFENDER::varchar as CLOSEST_DEFENDER,
    p.parse:CLOSEST_DEFENDER_PLAYER_ID::int as DEFENDER_ID,
    p.parse:CLOSE_DEFENDER_DIST::numeric(5, 1) as DEFENDER_DISTANCE,
    p.parse:FGM::boolean as FIELD_GOAL_MADE,
    p.parse:PTS::int as POINTS,
    p.parse:W::varchar as WIN,
    p.parse:FINAL_MARGIN::int as FINAL_MARGIN
from parsed p