SELECT
    -- Convertendo a data do voo para o tipo DATE
    CAST("FlightDate" AS DATE) AS flight_date,

    -- Selecionando e renomeando colunas de identificação
    "Reporting_Airline" AS airline_code,
    "Tail_Number" AS tail_number,
    "Flight_Number_Reporting_Airline" AS flight_number,

    -- Selecionando e renomeando colunas de origem e destino
    "Origin" AS origin_airport,
    "Dest" AS destination_airport,

    -- Convertendo atrasos para inteiros (tratando nulos como 0)
    CAST(COALESCE("DepDelay", 0) AS INTEGER) AS departure_delay,
    CAST(COALESCE("ArrDelay", 0) AS INTEGER) AS arrival_delay,

    -- Convertendo flags para booleanos (1=true, 0=false)
    CAST("Cancelled" AS BOOLEAN) AS is_cancelled,
    CAST("Diverted" AS BOOLEAN) AS is_diverted,

    -- Selecionando a distância do voo
    "Distance" AS distance

FROM
    read_csv('/opt/airflow/data/*.csv', header=true)
