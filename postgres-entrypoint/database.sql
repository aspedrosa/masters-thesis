CREATE TABLE pipeline (
    id SERIAL PRIMARY KEY,
    name VARCHAR UNIQUE NOT NULL,
    filter VARCHAR,
    status VARCHAR NOT NULL
);

CREATE TABLE pipeline_selection (
    id SERIAL PRIMARY KEY,
    pipeline INTEGER NOT NULL REFERENCES pipeline,
    selection_column VARCHAR NOT NULL,
    selection_order INTEGER DEFAULT 0
);

CREATE TABLE subscription (
    id SERIAL PRIMARY KEY,
    name VARCHAR UNIQUE NOT NULL,
    status VARCHAR NOT NULL
);

CREATE TABLE subscription_request (
    id SERIAL PRIMARY KEY,
    subscription INTEGER NOT NULL REFERENCES subscription,
    request_arguments_template VARCHAR,
    success_condition_template VARCHAR,
    request_order INTEGER DEFAULT 0
);

INSERT INTO pipeline (name, filter, status) VALUES ('montra', 'analysis_id = 0', 'ACTIVE');
INSERT INTO pipeline_selection (pipeline, selection_column, selection_order) VALUES (1, 'count_value', 0);
