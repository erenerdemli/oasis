CREATE TABLE IF NOT EXISTS OA_EXT_APP (
    app_id          INT PRIMARY KEY AUTO_INCREMENT,
    name            VARCHAR(127),
    token           CHAR(32),
    key_secret      BLOB,
    key_public      BLOB,
    is_internal     TINYINT DEFAULT 0,
    is_downloaded   TINYINT DEFAULT 0,
    is_active       TINYINT DEFAULT 1,
    for_all_games   TINYINT DEFAULT 0,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS OA_EXT_APP_EVENT (
    app_id          INT,
    event_type      VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS OA_EXT_APP_GAME (
    app_id          INT,
    game_id         INT
);

CREATE TABLE IF NOT EXISTS OA_GAME_DEF (
    game_id         INT PRIMARY KEY AUTO_INCREMENT,
    name            VARCHAR(64),
    description     VARCHAR(1024),

    current_state   VARCHAR(12),
    is_active       TINYINT DEFAULT 1
);

CREATE TABLE IF NOT EXISTS OA_GAME_STATE (
    game_id         INT,
    prev_state      VARCHAR(12),
    current_state   VARCHAR(12),
    changed_at      BIGINT
);

