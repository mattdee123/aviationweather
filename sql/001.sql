CREATE TABLE metars (
    station text,
    observation_time timestamptz,
    csv_parts text[],
    primary key (station, observation_time)
)
