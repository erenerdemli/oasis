UPDATE
    OA_ELEMENT
SET
    name = :name,
    description = :description,
    icon_url = :iconUrl,
    weight = :weight,
    version = version + 1,
    updated_at = :ts
WHERE
    def_id = :defId
    AND
    version = :version
