SELECT
    id as id,
    title as title,
    description as description,
    price as price,
    image_ref as imageRef,
    scope as scope,
    level as level,
    for_hero AS forHero,
    max_available AS maxAvailableItems,
    limited_amount AS limitedAmount,
    expiration_at as expirationAt

FROM OA_SHOP_ITEM
WHERE
    id = :itemId AND is_active = 1