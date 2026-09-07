CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.visits_per_day
(
    `id` UInt32,
    `visit_date` Date,
    `visits` UInt32,
    `created_at` DateTime,
    `feature_version` String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(visit_date)
ORDER BY (visit_date);

CREATE TABLE IF NOT EXISTS analytics.views_per_day
(
    `id` UInt32,
    `visit_date` Date,
    `sum_page_views` UInt32,
    `created_at` DateTime,
    `feature_version` String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(visit_date)
ORDER BY (visit_date);

CREATE TABLE IF NOT EXISTS analytics.bounce_rate_per_source
(
    `id` UInt32,
    `utm_source` String,
    `bounce_rate` UInt32 COMMENT 'To get correct shares divide by 10000',
    `created_at` DateTime,
    `feature_version` String
)
ENGINE = MergeTree
PARTITION BY (utm_source)
ORDER BY (utm_source);

CREATE TABLE IF NOT EXISTS analytics.visit_duration_per_source
(
    `id` UInt32,
    `utm_source` String,
    `avg_visit_duration` UInt32,
    `created_at` DateTime,
    `feature_version` String
)
ENGINE = MergeTree
PARTITION BY (utm_source)
ORDER BY (utm_source);