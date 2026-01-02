UPDATE OA_ELEMENT_DATA 
SET is_active = 0 
WHERE element_id IN (
    SELECT id FROM OA_ELEMENT WHERE game_id = :gameId
)
