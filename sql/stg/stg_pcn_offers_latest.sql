with
    source as (select * from {{ source("TEMP_ARGO_RAW", "PCN_OFFERS_LATEST") }})

select
    "NOTIFICATIONTYPE" as notificationtype,
    "UNIQUE_ID" as unique_id,
    "PUBLISH_TIME" as publish_time,
    "COMPETITIVE_PRICE_THRESHOLD" as competitive_price_threshold,
    "MARKETPLACE_ID" as marketplace_id,
    "ASIN" as asin,
    "ITEM_CONDITION" as item_condition,
    "TIME_OFFER_CHANGE" as time_offer_change,
    "OFFER_CHANGE_TYPE" as offer_change_type,
    "SELLER_ID" as seller_id,
    "MINIMUM_HOURS" as minimum_hours,
    "MAXIMUM_HOURS" as maximum_hours,
    "AVAILABILITY_TYPE" as availability_type,
    "AVAILABLE_DATE" as available_date,
    "SHIPS_DOMESTICALLY" as ships_domestically,
    "IS_FULFILLED_BY_AMAZON" as is_fulfilled_by_amazon,
    "IS_BUY_BOX_WINNER" as is_buy_box_winner,
    "IS_PRIME" as is_prime,
    "IS_NATIONAL_PRIME" as is_national_prime,
    "IS_FEATURED_MERCHANT" as is_featured_merchant,
    "LISTING_PRICE_CURRENCY_CODE" as listing_price_currency_code,
    "LISTING_PRICE_AMOUNT" as listing_price_amount,
    "SHIPPING_CURRENCY_CODE" as shipping_currency_code,
    "SHIPPING_AMOUNT" as shipping_amount,
    "SUB_CONDITION" as sub_condition,
    "SHIPS_FROM_STATE" as ships_from_state,
    "SHIPS_FROM_COUNTRY" as ships_from_country,
    "SELLER_FEEDBACK_RATING_COUNT" as seller_feedback_rating_count,
    "SELLER_POSITIVE_FEEDBACK_RATING" as seller_positive_feedback_rating

from source