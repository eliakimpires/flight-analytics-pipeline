WITH flights AS (
    SELECT * FROM {{ ref('stg_flights') }}
),

aggregated_flights AS (
    SELECT
        -- Dimensões
        flight_date,
        airline_code,
        
        -- Métricas
        COUNT(*) AS total_flights,
        
        -- Contar voos que chegaram no horário ou adiantados (atraso <= 0)
        SUM(CASE WHEN arrival_delay <= 0 THEN 1 ELSE 0 END) AS on_time_flights,
        
        -- Contar voos cancelados
        SUM(CASE WHEN is_cancelled THEN 1 ELSE 0 END) AS cancelled_flights,
        
        -- Contar voos desviados
        SUM(CASE WHEN is_diverted THEN 1 ELSE 0 END) AS diverted_flights,
        
        -- Calcular o atraso médio de chegada para voos não cancelados
        AVG(CASE WHEN NOT is_cancelled THEN arrival_delay ELSE NULL END) AS avg_arrival_delay
        
    FROM
        flights
    GROUP BY
        flight_date,
        airline_code
)

-- Query final que seleciona os dados da CTE e calcula a taxa de pontualidade
SELECT
    flight_date,
    airline_code,
    total_flights,
    on_time_flights,
    
    -- Calcula a taxa de pontualidade evitando divisão por zero
    CASE
        WHEN total_flights > 0 THEN (on_time_flights * 1.0 / total_flights) * 100
        ELSE 0
    END AS on_time_percentage,
    
    cancelled_flights,
    diverted_flights,
    avg_arrival_delay
    
FROM
    aggregated_flights
ORDER BY
    flight_date,
    airline_code