"""
SQL Queries for Traffic Stops Database
All SQL queries are stored here for easy maintenance
"""

def get_drop_table_query(table_name):
    """Get query to drop table if exists"""
    return f"DROP TABLE IF EXISTS {table_name}"

def get_create_table_query(table_name):
    """Get query to create traffic_stops table"""
    return f"""
    CREATE TABLE {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        stop_date DATE NOT NULL,
        stop_time TIME NOT NULL,
        country_name VARCHAR(100),
        driver_gender CHAR(1),
        driver_age_raw INT,
        driver_age INT,
        driver_race VARCHAR(50),
        violation_raw VARCHAR(100),
        violation VARCHAR(100),
        search_conducted BOOLEAN,
        search_type VARCHAR(100),
        stop_outcome VARCHAR(100),
        is_arrested BOOLEAN,
        stop_duration VARCHAR(50),
        drugs_related_stop BOOLEAN,
        vehicle_number VARCHAR(20),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_stop_date (stop_date),
        INDEX idx_country (country_name),
        INDEX idx_violation (violation),
        INDEX idx_vehicle (vehicle_number)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """

def get_insert_query(table_name):
    """Get query to insert data into table"""
    return f"""
    INSERT INTO {table_name} (
        stop_date, stop_time, country_name, driver_gender, driver_age_raw,
        driver_age, driver_race, violation_raw, violation, search_conducted,
        search_type, stop_outcome, is_arrested, stop_duration,
        drugs_related_stop, vehicle_number
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """

def get_count_query(table_name):
    """Get query to count total rows"""
    return f"SELECT COUNT(*) FROM {table_name}"

def get_sample_data_query(table_name, limit=3):
    """Get query to fetch sample data"""
    return f"SELECT * FROM {table_name} LIMIT {limit}"

def get_statistics_query(table_name):
    """Get query to fetch database statistics"""
    return f"""
        SELECT
            COUNT(*) as total_stops,
            COUNT(DISTINCT country_name) as countries,
            COUNT(DISTINCT violation) as violation_types,
            SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0 END) as arrests
        FROM {table_name}
    """

# Dashboard-specific queries

def get_vehicle_logs_query(table_name):
    """Get vehicle logs statistics"""
    return f"""
        SELECT
            COUNT(DISTINCT vehicle_number) as total_vehicles,
            COUNT(*) as total_stops,
            SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0 END) as arrests,
            SUM(CASE WHEN search_conducted = 1 THEN 1 ELSE 0 END) as searches
        FROM {table_name}
    """

def get_violations_stats_query(table_name):
    """Get violations statistics"""
    return f"""
        SELECT
            violation,
            COUNT(*) as count,
            SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0 END) as arrests,
            ROUND(SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as arrest_rate
        FROM {table_name}
        GROUP BY violation
        ORDER BY count DESC
    """

def get_officer_reports_query(table_name):
    """Get officer reports statistics by country"""
    return f"""
        SELECT
            country_name,
            COUNT(*) as total_stops,
            COUNT(DISTINCT vehicle_number) as unique_vehicles,
            SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0 END) as arrests,
            SUM(CASE WHEN search_conducted = 1 THEN 1 ELSE 0 END) as searches,
            SUM(CASE WHEN drugs_related_stop = 1 THEN 1 ELSE 0 END) as drug_related
        FROM {table_name}
        GROUP BY country_name
        ORDER BY total_stops DESC
    """

def get_vehicle_lookup_query(table_name, vehicle_number):
    """Get all records for a specific vehicle"""
    return f"""
        SELECT
            stop_date, stop_time, country_name, driver_gender, driver_age,
            driver_race, violation, search_conducted, search_type,
            stop_outcome, is_arrested, stop_duration, drugs_related_stop
        FROM {table_name}
        WHERE vehicle_number = '{vehicle_number}'
        ORDER BY stop_date DESC, stop_time DESC
    """

def get_all_vehicle_numbers_query(table_name):
    """Get all unique vehicle numbers"""
    return f"""
        SELECT DISTINCT vehicle_number
        FROM {table_name}
        ORDER BY vehicle_number
    """

def get_recent_stops_query(table_name, limit=10):
    """Get recent traffic stops"""
    return f"""
        SELECT
            stop_date, stop_time, vehicle_number, country_name,
            violation, stop_outcome, is_arrested
        FROM {table_name}
        ORDER BY stop_date DESC, stop_time DESC
        LIMIT {limit}
    """

def get_unique_values_query(table_name, column_name):
    """Get unique values for a column"""
    return f"SELECT DISTINCT {column_name} FROM {table_name} WHERE {column_name} IS NOT NULL ORDER BY {column_name}"

def get_prediction_stats_query(table_name, violation, driver_age, driver_race, driver_gender):
    """Get statistics for prediction based on similar cases"""
    return f"""
        SELECT
            COUNT(*) as similar_cases,
            ROUND(AVG(is_arrested) * 100, 1) as arrest_probability,
            ROUND(AVG(search_conducted) * 100, 1) as search_probability,
            ROUND(AVG(drugs_related_stop) * 100, 1) as drug_probability
        FROM {table_name}
        WHERE violation = '{violation}'
        AND driver_age BETWEEN {driver_age - 5} AND {driver_age + 5}
        AND driver_race = '{driver_race}'
        AND driver_gender = '{driver_gender}'
    """

def get_most_common_outcome_query(table_name, violation):
    """Get most common outcome for a violation"""
    return f"""
        SELECT stop_outcome, COUNT(*) as count
        FROM {table_name}
        WHERE violation = '{violation}' AND stop_outcome IS NOT NULL
        GROUP BY stop_outcome
        ORDER BY count DESC
        LIMIT 1
    """

def insert_new_log_query(table_name):
    """Get query to insert a new traffic stop log"""
    return f"""
        INSERT INTO {table_name}
        (stop_date, stop_time, country_name, driver_gender, driver_age_raw, driver_age,
         driver_race, violation_raw, violation, search_conducted, search_type,
         stop_outcome, is_arrested, stop_duration, drugs_related_stop, vehicle_number)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
