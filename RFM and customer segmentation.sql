
WITH registrations AS (
    SELECT 
        user_pseudo_id,
        DATE_TRUNC(PARSE_DATE('%Y%m%d', MIN(event_date)), WEEK) AS registration_week
    FROM 
        turing_data_analytics.raw_events
    GROUP BY 
        user_pseudo_id
),

first_week_registration_count AS (
    SELECT 
        COUNT(user_pseudo_id) AS cohort_size,
        registration_week
    FROM 
        registrations
    GROUP BY 
        registration_week
),

weekly_revenue AS (
    SELECT 
        reg.registration_week,
        fw.cohort_size,
        
        -- Week 0 (current week revenue)
        SUM(CASE WHEN DATE_TRUNC(PARSE_DATE('%Y%m%d', re.event_date), WEEK) = reg.registration_week THEN re.purchase_revenue_in_usd ELSE 0 END) / fw.cohort_size AS revenue_week_0,

        -- Week 1
        SUM(CASE WHEN DATE_TRUNC(PARSE_DATE('%Y%m%d', re.event_date), WEEK) = reg.registration_week + INTERVAL 1 WEEK THEN re.purchase_revenue_in_usd ELSE 0 END) / fw.cohort_size AS revenue_week_1,

        -- Week 2
        SUM(CASE WHEN DATE_TRUNC(PARSE_DATE('%Y%m%d', re.event_date), WEEK) = reg.registration_week + INTERVAL 2 WEEK THEN re.purchase_revenue_in_usd ELSE 0 END) / fw.cohort_size AS revenue_week_2,

        -- Week 3
        SUM(CASE WHEN DATE_TRUNC(PARSE_DATE('%Y%m%d', re.event_date), WEEK) = reg.registration_week + INTERVAL 3 WEEK THEN re.purchase_revenue_in_usd ELSE 0 END) / fw.cohort_size AS revenue_week_3,

        -- Week 4
        SUM(CASE WHEN DATE_TRUNC(PARSE_DATE('%Y%m%d', re.event_date), WEEK) = reg.registration_week + INTERVAL 4 WEEK THEN re.purchase_revenue_in_usd ELSE 0 END) / fw.cohort_size AS revenue_week_4,

        -- Week 5
        SUM(CASE WHEN DATE_TRUNC(PARSE_DATE('%Y%m%d', re.event_date), WEEK) = reg.registration_week + INTERVAL 5 WEEK THEN re.purchase_revenue_in_usd ELSE 0 END) / fw.cohort_size AS revenue_week_5,

        -- Week 6
        SUM(CASE WHEN DATE_TRUNC(PARSE_DATE('%Y%m%d', re.event_date), WEEK) = reg.registration_week + INTERVAL 6 WEEK THEN re.purchase_revenue_in_usd ELSE 0 END) / fw.cohort_size AS revenue_week_6,

        -- Week 7
        SUM(CASE WHEN DATE_TRUNC(PARSE_DATE('%Y%m%d', re.event_date), WEEK) = reg.registration_week + INTERVAL 7 WEEK THEN re.purchase_revenue_in_usd ELSE 0 END) / fw.cohort_size AS revenue_week_7,

        -- Week 8
        SUM(CASE WHEN DATE_TRUNC(PARSE_DATE('%Y%m%d', re.event_date), WEEK) = reg.registration_week + INTERVAL 8 WEEK THEN re.purchase_revenue_in_usd ELSE 0 END) / fw.cohort_size AS revenue_week_8,

        -- Week 9
        SUM(CASE WHEN DATE_TRUNC(PARSE_DATE('%Y%m%d', re.event_date), WEEK) = reg.registration_week + INTERVAL 9 WEEK THEN re.purchase_revenue_in_usd ELSE 0 END) / fw.cohort_size AS revenue_week_9,

        -- Week 10
        SUM(CASE WHEN DATE_TRUNC(PARSE_DATE('%Y%m%d', re.event_date), WEEK) = reg.registration_week + INTERVAL 10 WEEK THEN re.purchase_revenue_in_usd ELSE 0 END) / fw.cohort_size AS revenue_week_10,

        -- Week 11
        SUM(CASE WHEN DATE_TRUNC(PARSE_DATE('%Y%m%d', re.event_date), WEEK) = reg.registration_week + INTERVAL 11 WEEK THEN re.purchase_revenue_in_usd ELSE 0 END) / fw.cohort_size AS revenue_week_11

    FROM 
        registrations AS reg
    LEFT JOIN 
        turing_data_analytics.raw_events AS re 
        ON reg.user_pseudo_id = re.user_pseudo_id
    JOIN 
        first_week_registration_count AS fw
        ON reg.registration_week = fw.registration_week
    GROUP BY 
        reg.registration_week, fw.cohort_size
)

-- Final Selection: Weekly Revenue per Registration Cohort
SELECT 
    registration_week,
    revenue_week_0,
    revenue_week_1,
    revenue_week_2,
    revenue_week_3,
    revenue_week_4,
    revenue_week_5,
    revenue_week_6,
    revenue_week_7,
    revenue_week_8,
    revenue_week_9,
    revenue_week_10,
    revenue_week_11
FROM 
    weekly_revenue
ORDER BY 
    registration_week;
