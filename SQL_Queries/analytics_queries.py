"""
Analytics SQL Queries for Traffic Stops Dashboard
Contains medium and complex level analytical queries
"""

# ============================================================
# MEDIUM LEVEL QUERIES
# ============================================================

# 🚗 VEHICLE-BASED QUERIES
# ============================================================

def get_top_10_drug_vehicles_query(table_name):
    """Top 10 vehicles involved in drug-related stops"""
    return f"""
        SELECT 
            vehicle_number,
            COUNT(*) as drug_stops,
            SUM(is_arrested) as arrests,
            SUM(search_conducted) as searches
        FROM {table_name}
        WHERE drugs_related_stop = 1
        GROUP BY vehicle_number
        ORDER BY drug_stops DESC
        LIMIT 10
    """

def get_most_searched_vehicles_query(table_name):
    """Vehicles most frequently searched"""
    return f"""
        SELECT 
            vehicle_number,
            COUNT(*) as total_stops,
            SUM(search_conducted) as times_searched,
            ROUND(SUM(search_conducted) * 100.0 / COUNT(*), 2) as search_rate
        FROM {table_name}
        WHERE search_conducted = 1
        GROUP BY vehicle_number
        ORDER BY times_searched DESC
        LIMIT 10
    """

# 🧍 DEMOGRAPHIC-BASED QUERIES
# ============================================================

def get_age_group_arrest_rate_query(table_name):
    """Driver age group with highest arrest rate"""
    return f"""
        SELECT 
            CASE 
                WHEN driver_age < 25 THEN 'Under 25'
                WHEN driver_age BETWEEN 25 AND 34 THEN '25-34'
                WHEN driver_age BETWEEN 35 AND 44 THEN '35-44'
                WHEN driver_age BETWEEN 45 AND 54 THEN '45-54'
                WHEN driver_age >= 55 THEN '55+'
                ELSE 'Unknown'
            END as age_group,
            COUNT(*) as total_stops,
            SUM(is_arrested) as arrests,
            ROUND(SUM(is_arrested) * 100.0 / COUNT(*), 2) as arrest_rate
        FROM {table_name}
        GROUP BY age_group
        ORDER BY arrest_rate DESC
    """

def get_gender_by_country_query(table_name):
    """Gender distribution of drivers stopped in each country"""
    return f"""
        SELECT 
            country_name,
            driver_gender,
            COUNT(*) as stops,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY country_name), 2) as percentage
        FROM {table_name}
        GROUP BY country_name, driver_gender
        ORDER BY country_name, stops DESC
    """

def get_race_gender_search_rate_query(table_name):
    """Race and gender combination with highest search rate"""
    return f"""
        SELECT 
            driver_race,
            driver_gender,
            COUNT(*) as total_stops,
            SUM(search_conducted) as searches,
            ROUND(SUM(search_conducted) * 100.0 / COUNT(*), 2) as search_rate
        FROM {table_name}
        GROUP BY driver_race, driver_gender
        ORDER BY search_rate DESC
        LIMIT 15
    """

# 🕒 TIME & DURATION BASED QUERIES
# ============================================================

def get_stops_by_time_of_day_query(table_name):
    """Time of day with most traffic stops"""
    return f"""
        SELECT 
            HOUR(stop_time) as hour,
            COUNT(*) as stops,
            SUM(is_arrested) as arrests,
            ROUND(SUM(is_arrested) * 100.0 / COUNT(*), 2) as arrest_rate
        FROM {table_name}
        GROUP BY hour
        ORDER BY hour
    """

def get_avg_duration_by_violation_query(table_name):
    """Average stop duration for different violations"""
    return f"""
        SELECT 
            violation,
            COUNT(*) as total_stops,
            AVG(CASE 
                WHEN stop_duration LIKE '%-%' THEN 
                    CAST(SUBSTRING_INDEX(stop_duration, '-', 1) AS UNSIGNED)
                ELSE 0
            END) as avg_duration_minutes,
            SUM(is_arrested) as arrests
        FROM {table_name}
        WHERE stop_duration IS NOT NULL AND stop_duration != 'Unknown'
        GROUP BY violation
        ORDER BY avg_duration_minutes DESC
    """

def get_night_arrest_rate_query(table_name):
    """Night vs day arrest rates"""
    return f"""
        SELECT 
            CASE 
                WHEN HOUR(stop_time) BETWEEN 6 AND 17 THEN 'Day (6AM-5PM)'
                ELSE 'Night (6PM-5AM)'
            END as time_period,
            COUNT(*) as total_stops,
            SUM(is_arrested) as arrests,
            ROUND(SUM(is_arrested) * 100.0 / COUNT(*), 2) as arrest_rate,
            SUM(search_conducted) as searches,
            ROUND(SUM(search_conducted) * 100.0 / COUNT(*), 2) as search_rate
        FROM {table_name}
        GROUP BY time_period
    """

# ⚖️ VIOLATION-BASED QUERIES
# ============================================================

def get_violations_search_arrest_query(table_name):
    """Violations most associated with searches or arrests"""
    return f"""
        SELECT 
            violation,
            COUNT(*) as total_stops,
            SUM(search_conducted) as searches,
            ROUND(SUM(search_conducted) * 100.0 / COUNT(*), 2) as search_rate,
            SUM(is_arrested) as arrests,
            ROUND(SUM(is_arrested) * 100.0 / COUNT(*), 2) as arrest_rate
        FROM {table_name}
        GROUP BY violation
        ORDER BY search_rate DESC, arrest_rate DESC
    """

def get_young_driver_violations_query(table_name):
    """Most common violations among younger drivers (<25)"""
    return f"""
        SELECT 
            violation,
            COUNT(*) as stops,
            SUM(is_arrested) as arrests,
            ROUND(SUM(is_arrested) * 100.0 / COUNT(*), 2) as arrest_rate
        FROM {table_name}
        WHERE driver_age < 25
        GROUP BY violation
        ORDER BY stops DESC
        LIMIT 10
    """

def get_low_risk_violations_query(table_name):
    """Violations that rarely result in search or arrest"""
    return f"""
        SELECT 
            violation,
            COUNT(*) as total_stops,
            SUM(search_conducted) as searches,
            ROUND(SUM(search_conducted) * 100.0 / COUNT(*), 2) as search_rate,
            SUM(is_arrested) as arrests,
            ROUND(SUM(is_arrested) * 100.0 / COUNT(*), 2) as arrest_rate
        FROM {table_name}
        GROUP BY violation
        HAVING COUNT(*) > 100
        ORDER BY search_rate ASC, arrest_rate ASC
        LIMIT 10
    """

# 🌍 LOCATION-BASED QUERIES
# ============================================================

def get_drug_stops_by_country_query(table_name):
    """Countries with highest rate of drug-related stops"""
    return f"""
        SELECT 
            country_name,
            COUNT(*) as total_stops,
            SUM(drugs_related_stop) as drug_stops,
            ROUND(SUM(drugs_related_stop) * 100.0 / COUNT(*), 2) as drug_stop_rate
        FROM {table_name}
        GROUP BY country_name
        ORDER BY drug_stop_rate DESC
    """

def get_arrest_rate_by_country_violation_query(table_name):
    """Arrest rate by country and violation"""
    return f"""
        SELECT 
            country_name,
            violation,
            COUNT(*) as stops,
            SUM(is_arrested) as arrests,
            ROUND(SUM(is_arrested) * 100.0 / COUNT(*), 2) as arrest_rate
        FROM {table_name}
        GROUP BY country_name, violation
        HAVING COUNT(*) > 10
        ORDER BY country_name, arrest_rate DESC
    """

def get_search_rate_by_country_query(table_name):
    """Countries with most stops with search conducted"""
    return f"""
        SELECT 
            country_name,
            COUNT(*) as total_stops,
            SUM(search_conducted) as searches,
            ROUND(SUM(search_conducted) * 100.0 / COUNT(*), 2) as search_rate
        FROM {table_name}
        GROUP BY country_name
        ORDER BY searches DESC
    """

# ============================================================
# COMPLEX LEVEL QUERIES
# ============================================================

def get_yearly_breakdown_by_country_query(table_name):
    """Yearly breakdown of stops and arrests by country with window functions"""
    return f"""
        SELECT 
            YEAR(stop_date) as year,
            country_name,
            COUNT(*) as stops,
            SUM(is_arrested) as arrests,
            ROUND(SUM(is_arrested) * 100.0 / COUNT(*), 2) as arrest_rate,
            SUM(COUNT(*)) OVER (PARTITION BY country_name ORDER BY YEAR(stop_date)) as cumulative_stops
        FROM {table_name}
        GROUP BY year, country_name
        ORDER BY year DESC, stops DESC
    """

def get_violation_trends_by_age_race_query(table_name):
    """Driver violation trends based on age and race"""
    return f"""
        SELECT 
            driver_race,
            CASE 
                WHEN driver_age < 25 THEN 'Under 25'
                WHEN driver_age BETWEEN 25 AND 44 THEN '25-44'
                WHEN driver_age >= 45 THEN '45+'
                ELSE 'Unknown'
            END as age_group,
            violation,
            COUNT(*) as stops,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY driver_race), 2) as pct_of_race
        FROM {table_name}
        GROUP BY driver_race, age_group, violation
        HAVING COUNT(*) > 20
        ORDER BY driver_race, stops DESC
    """

def get_time_period_analysis_query(table_name):
    """Number of stops by year, month, hour of the day"""
    return f"""
        SELECT 
            YEAR(stop_date) as year,
            MONTH(stop_date) as month,
            HOUR(stop_time) as hour,
            COUNT(*) as stops,
            SUM(is_arrested) as arrests,
            SUM(search_conducted) as searches
        FROM {table_name}
        GROUP BY year, month, hour
        ORDER BY year DESC, month DESC, stops DESC
    """

def get_high_search_arrest_violations_query(table_name):
    """Violations with high search and arrest rates using window functions"""
    return f"""
        SELECT 
            violation,
            total_stops,
            searches,
            search_rate,
            arrests,
            arrest_rate,
            RANK() OVER (ORDER BY search_rate DESC) as search_rank,
            RANK() OVER (ORDER BY arrest_rate DESC) as arrest_rank
        FROM (
            SELECT 
                violation,
                COUNT(*) as total_stops,
                SUM(search_conducted) as searches,
                ROUND(SUM(search_conducted) * 100.0 / COUNT(*), 2) as search_rate,
                SUM(is_arrested) as arrests,
                ROUND(SUM(is_arrested) * 100.0 / COUNT(*), 2) as arrest_rate
            FROM {table_name}
            GROUP BY violation
            HAVING COUNT(*) > 50
        ) as violation_stats
        ORDER BY (search_rank + arrest_rank) ASC
        LIMIT 15
    """

def get_demographics_by_country_query(table_name):
    """Driver demographics by country (age, gender, race)"""
    return f"""
        SELECT 
            country_name,
            ROUND(AVG(driver_age), 1) as avg_age,
            SUM(CASE WHEN driver_gender = 'M' THEN 1 ELSE 0 END) as male_count,
            SUM(CASE WHEN driver_gender = 'F' THEN 1 ELSE 0 END) as female_count,
            COUNT(DISTINCT driver_race) as race_diversity,
            COUNT(*) as total_stops
        FROM {table_name}
        GROUP BY country_name
        ORDER BY total_stops DESC
    """

def get_top_5_violations_arrest_rate_query(table_name):
    """Top 5 violations with highest arrest rates"""
    return f"""
        SELECT 
            violation,
            COUNT(*) as total_stops,
            SUM(is_arrested) as arrests,
            ROUND(SUM(is_arrested) * 100.0 / COUNT(*), 2) as arrest_rate,
            SUM(search_conducted) as searches,
            ROUND(SUM(search_conducted) * 100.0 / COUNT(*), 2) as search_rate
        FROM {table_name}
        GROUP BY violation
        HAVING COUNT(*) > 100
        ORDER BY arrest_rate DESC
        LIMIT 5
    """

