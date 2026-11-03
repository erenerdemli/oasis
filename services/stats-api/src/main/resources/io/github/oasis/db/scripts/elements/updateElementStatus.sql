UPDATE
    OA_ELEMENT
SET
    status = :status,
    updated_at = :ts
WHERE
    id = :id
