create schema health_plans;

CREATE TABLE health_plans.uhg_plans (
    reporting_entity_name TEXT,
    reporting_entity_type TEXT,
    plan_name TEXT,
    plan_id INTEGER,
    plan_id_type TEXT,
    plan_market_type TEXT,
    description TEXT,
    location TEXT,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);