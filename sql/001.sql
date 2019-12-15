CREATE TABLE metars (
    station text,
    time timestamptz,
    csv_parts text[],
    primary key (station, time)
)
