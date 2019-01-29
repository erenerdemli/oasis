INSERT INTO OA_FEED (
    game_id,
    user_id,
    team_id,
    team_scope_id,
    def_kind_id,
    def_id,
    action_id,
    message,
    sub_message,
    event_type,
    caused_event,
    tag,
    ts
) VALUES (
    :gameId,
    :userId,
    :teamId,
    :teamScopeId,
    :defKindId,
    :defId,
    :actionId,
    :message,
    :eventType,
    :causedEvent,
    :tag,
    :ts
)