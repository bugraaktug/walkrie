-- insert_candidates.sql
--
-- Synthetic CV dataset for the walkrie CV/HR semantic search demo.
-- All names, skills, and career narratives are randomly assembled from
-- templates below — no real personal data of any kind.
--
-- Run against the demo database (see demo/README.md for full setup):
--   psql -d hr_demo -f demo/insert_candidates.sql

CREATE TABLE IF NOT EXISTS candidates (
    id               bigserial PRIMARY KEY,
    full_name        text NOT NULL,
    years_experience int NOT NULL,
    location         text NOT NULL,
    cv_text          text NOT NULL,
    created_at       timestamptz NOT NULL DEFAULT now()
);


WITH random_candidates AS (
    SELECT
        (ARRAY[
            'Alex','Jordan','Taylor','Can','Morgan','Casey','Riley','Jamie','Avery',
            'Cameron','Drew','Ata','Elliot','Harper','Kai','Logan','Parker','Reese',
            'Sam','Skyler','Eke','Quinn','Rowan','Emerson','Finley','Hayden','Micah'
        ])[1 + floor(random() * 27)::int]
        || ' ' ||
        (ARRAY[
            'Chen','Patel','Garcia','Kim','Novak','Silva','Andersson','Yilmaz',
            'Rossi','Kowalski','Nguyen','Okafor','Haddad','Larsen','Petrov','Diaz'
        ])[1 + floor(random() * 16)::int] AS full_name,

        (1 + floor(random() * 12)::int) AS role_idx,

        (2 + floor(random() * 14))::int AS years_experience,

        (ARRAY[
            'Berlin','Amsterdam','Austin','Toronto','Warsaw','Lisbon','Singapore',
            'Remote','London','Barcelona','Krakow','Denver','Budapest','Istanbul'
        ])[1 + floor(random() * 14)::int] AS location
    FROM generate_series(1, 300) AS s
),
with_role AS (
    SELECT
        full_name,
        years_experience,
        location,
        CASE role_idx
            WHEN 1  THEN 'Backend Engineer'
            WHEN 2  THEN 'Frontend Engineer'
            WHEN 3  THEN 'Data Engineer'
            WHEN 4  THEN 'DevOps Engineer'
            WHEN 5  THEN 'Machine Learning Engineer'
            WHEN 6  THEN 'Full Stack Engineer'
            WHEN 7  THEN 'Site Reliability Engineer'
            WHEN 8  THEN 'Database Administrator'
            WHEN 9  THEN 'Mobile Engineer'
            WHEN 10 THEN 'Security Engineer'
            WHEN 11 THEN 'Systems C++ Engineer'
            WHEN 12 THEN 'Product Manager'
        END AS title,
        CASE role_idx
            WHEN 1  THEN ARRAY['Python','PostgreSQL','Django','Docker','AWS']
            WHEN 2  THEN ARRAY['React','TypeScript','CSS','Webpack','Figma']
            WHEN 3  THEN ARRAY['Spark','Airflow','SQL','Kafka','dbt']
            WHEN 4  THEN ARRAY['Kubernetes','Terraform','CI/CD','Linux','Prometheus']
            WHEN 5  THEN ARRAY['PyTorch','scikit-learn','MLflow','Python','NLP']
            WHEN 6  THEN ARRAY['Node.js','React','PostgreSQL','GraphQL','Docker']
            WHEN 7  THEN ARRAY['Kubernetes','Go','Monitoring','Incident Response','AWS']
            WHEN 8  THEN ARRAY['PostgreSQL','MySQL','Replication','Backup and Recovery','Performance Tuning']
            WHEN 9  THEN ARRAY['Swift','Kotlin','iOS','Android','REST APIs']
            WHEN 10 THEN ARRAY['Penetration Testing','SIEM','Network Security','Python','Compliance']
            WHEN 11 THEN ARRAY['C++','Multithreading','Linux','Networking','Performance Optimization']
            WHEN 12 THEN ARRAY['Roadmapping','Stakeholder Management','SQL','Agile','User Research']
        END AS skills
    FROM random_candidates
)
INSERT INTO candidates (full_name, years_experience, location, cv_text)
SELECT
    full_name,
    years_experience,
    location,
    full_name || ' — ' || title || ' with ' || years_experience || ' years of experience, based in ' || location || '. ' ||
    'Core skills: ' || array_to_string(skills, ', ') || '. ' ||
    'Proven track record delivering production systems, collaborating with cross-functional teams, ' ||
    'and mentoring junior engineers. Seeking roles that leverage strong ' || skills[1] || ' and ' || skills[2] || ' expertise.'
FROM with_role;

