import streamlit as st
import mysql.connector
import pandas as pd
from sql_queries import *
from analytics_queries import *

DB_CONFIG = {
    'host': "gateway01.eu-central-1.prod.aws.tidbcloud.com",
    'port': 4000,
    'user': "4TQcB5BpE3ADNsN.root",
    'password': "NG8gFRIsGag8xXRv",
    'database': "test",
    'ssl_verify_cert': False,
    'ssl_verify_identity': False
}

TABLE_NAME = 'traffic_stops'

@st.cache_resource
def get_connection():
    return mysql.connector.connect(**DB_CONFIG)

def execute_query(query):
    return pd.read_sql(query, get_connection())

def main():
    st.set_page_config(page_title="Police Digital Ledger", layout="wide")
    st.title("🚔 Police Digital Ledger Dashboard")

    page = st.sidebar.radio("Navigation", ["Vehicle Logs & Reports", "Vehicle Lookup", "Analytics", "Add New Log"])

    if page == "Vehicle Logs & Reports":
        show_vehicle_logs_page()
    elif page == "Vehicle Lookup":
        show_vehicle_lookup_page()
    elif page == "Analytics":
        show_analytics_page()
    elif page == "Add New Log":
        show_add_new_log_page()

def show_vehicle_logs_page():
    st.header("Vehicle Logs & Reports")

    # Vehicle Logs
    vehicle_logs_df = execute_query(get_vehicle_logs_query(TABLE_NAME))
    if vehicle_logs_df is not None and not vehicle_logs_df.empty:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Vehicles", f"{vehicle_logs_df['total_vehicles'].iloc[0]:,}")
        col2.metric("Total Stops", f"{vehicle_logs_df['total_stops'].iloc[0]:,}")
        col3.metric("Total Arrests", f"{vehicle_logs_df['arrests'].iloc[0]:,}")
        col4.metric("Total Searches", f"{vehicle_logs_df['searches'].iloc[0]:,}")

    # Violations
    st.subheader("Violations")
    violations_df = execute_query(get_violations_stats_query(TABLE_NAME))
    if violations_df is not None and not violations_df.empty:
        col1, col2 = st.columns([2, 1])
        col1.dataframe(violations_df, hide_index=True, use_container_width=True)
        col2.bar_chart(violations_df.set_index('violation')['count'])

    # Officer Reports
    st.subheader("Officer Reports")
    officer_reports_df = execute_query(get_officer_reports_query(TABLE_NAME))
    if officer_reports_df is not None and not officer_reports_df.empty:
        st.dataframe(officer_reports_df, hide_index=True, use_container_width=True)

def show_vehicle_lookup_page():
    st.header("Vehicle Lookup")

    vehicle_number = st.text_input("Enter Vehicle Number", placeholder="e.g., UP76DY3473")
    search_button = st.button("Search")

    if search_button and vehicle_number:
        vehicle_data_df = execute_query(get_vehicle_lookup_query(TABLE_NAME, vehicle_number))

        if vehicle_data_df is not None and not vehicle_data_df.empty:
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Stops", len(vehicle_data_df))
            col2.metric("Arrests", vehicle_data_df['is_arrested'].sum())
            col3.metric("Searches", vehicle_data_df['search_conducted'].sum())
            col4.metric("Drug Related", vehicle_data_df['drugs_related_stop'].sum())

            st.subheader("Stop History")
            st.dataframe(vehicle_data_df, hide_index=True, use_container_width=True)

            csv = vehicle_data_df.to_csv(index=False)
            st.download_button("Download CSV", csv, f"vehicle_{vehicle_number}.csv", "text/csv")
        else:
            st.warning(f"No records found for: {vehicle_number}")

def show_analytics_page():
    st.header("Analytics & Trends")

    # Category selection
    category = st.selectbox("Select Analysis Category", [
        "Vehicle-Based Analysis",
        "Demographic Analysis",
        "Time & Duration Analysis",
        "Violation Analysis",
        "Location Analysis",
        "Complex Analytics"
    ])

    if category == "Vehicle-Based Analysis":
        show_vehicle_analytics()
    elif category == "Demographic Analysis":
        show_demographic_analytics()
    elif category == "Time & Duration Analysis":
        show_time_analytics()
    elif category == "Violation Analysis":
        show_violation_analytics()
    elif category == "Location Analysis":
        show_location_analytics()
    elif category == "Complex Analytics":
        show_complex_analytics()

def show_vehicle_analytics():
    st.subheader("Vehicle-Based Analysis")

    col1, col2 = st.columns(2)

    with col1:
        st.write("**Top 10 Vehicles in Drug-Related Stops**")
        df = execute_query(get_top_10_drug_vehicles_query(TABLE_NAME))
        st.dataframe(df, hide_index=True, use_container_width=True)

    with col2:
        st.write("**Most Frequently Searched Vehicles**")
        df = execute_query(get_most_searched_vehicles_query(TABLE_NAME))
        st.dataframe(df, hide_index=True, use_container_width=True)

def show_demographic_analytics():
    st.subheader("Demographic Analysis")

    st.write("**Arrest Rate by Age Group**")
    df = execute_query(get_age_group_arrest_rate_query(TABLE_NAME))
    st.dataframe(df, hide_index=True, use_container_width=True)
    st.bar_chart(df.set_index('age_group')['arrest_rate'])

    st.write("**Gender Distribution by Country**")
    df = execute_query(get_gender_by_country_query(TABLE_NAME))
    st.dataframe(df, hide_index=True, use_container_width=True)

    st.write("**Search Rate by Race and Gender**")
    df = execute_query(get_race_gender_search_rate_query(TABLE_NAME))
    st.dataframe(df, hide_index=True, use_container_width=True)

def show_time_analytics():
    st.subheader("Time & Duration Analysis")

    st.write("**Stops by Hour of Day**")
    df = execute_query(get_stops_by_time_of_day_query(TABLE_NAME))
    st.dataframe(df, hide_index=True, use_container_width=True)
    st.line_chart(df.set_index('hour')['stops'])

    st.write("**Average Duration by Violation**")
    df = execute_query(get_avg_duration_by_violation_query(TABLE_NAME))
    st.dataframe(df, hide_index=True, use_container_width=True)

    st.write("**Night vs Day Arrest Rates**")
    df = execute_query(get_night_arrest_rate_query(TABLE_NAME))
    st.dataframe(df, hide_index=True, use_container_width=True)

def show_violation_analytics():
    st.subheader("Violation Analysis")

    st.write("**Violations Associated with Searches/Arrests**")
    df = execute_query(get_violations_search_arrest_query(TABLE_NAME))
    st.dataframe(df, hide_index=True, use_container_width=True)

    st.write("**Common Violations Among Young Drivers (<25)**")
    df = execute_query(get_young_driver_violations_query(TABLE_NAME))
    st.dataframe(df, hide_index=True, use_container_width=True)

    st.write("**Low-Risk Violations**")
    df = execute_query(get_low_risk_violations_query(TABLE_NAME))
    st.dataframe(df, hide_index=True, use_container_width=True)

def show_location_analytics():
    st.subheader("Location Analysis")

    st.write("**Drug-Related Stops by Country**")
    df = execute_query(get_drug_stops_by_country_query(TABLE_NAME))
    st.dataframe(df, hide_index=True, use_container_width=True)

    st.write("**Arrest Rate by Country and Violation**")
    df = execute_query(get_arrest_rate_by_country_violation_query(TABLE_NAME))
    st.dataframe(df, hide_index=True, use_container_width=True)

    st.write("**Search Rate by Country**")
    df = execute_query(get_search_rate_by_country_query(TABLE_NAME))
    st.dataframe(df, hide_index=True, use_container_width=True)

def show_complex_analytics():
    st.subheader("Complex Analytics")

    st.write("**Yearly Breakdown by Country**")
    df = execute_query(get_yearly_breakdown_by_country_query(TABLE_NAME))
    st.dataframe(df, hide_index=True, use_container_width=True)

    st.write("**Violation Trends by Age and Race**")
    df = execute_query(get_violation_trends_by_age_race_query(TABLE_NAME))
    st.dataframe(df, hide_index=True, use_container_width=True)

    st.write("**High Search/Arrest Rate Violations**")
    df = execute_query(get_high_search_arrest_violations_query(TABLE_NAME))
    st.dataframe(df, hide_index=True, use_container_width=True)

    st.write("**Demographics by Country**")
    df = execute_query(get_demographics_by_country_query(TABLE_NAME))
    st.dataframe(df, hide_index=True, use_container_width=True)

    st.write("**Top 5 Violations by Arrest Rate**")
    df = execute_query(get_top_5_violations_arrest_rate_query(TABLE_NAME))
    st.dataframe(df, hide_index=True, use_container_width=True)

def show_add_new_log_page():
    st.header("Add New Police Log")

    # Get unique values for dropdowns
    countries = execute_query(get_unique_values_query(TABLE_NAME, 'country_name'))['country_name'].tolist()
    violations = execute_query(get_unique_values_query(TABLE_NAME, 'violation'))['violation'].tolist()
    races = execute_query(get_unique_values_query(TABLE_NAME, 'driver_race'))['driver_race'].tolist()

    # Form
    with st.form("new_log_form"):
        st.subheader("Stop Details")

        col1, col2 = st.columns(2)
        with col1:
            stop_date = st.date_input("Stop Date")
            stop_time = st.time_input("Stop Time")
            vehicle_number = st.text_input("Vehicle Number", placeholder="e.g., ABC123")
            country = st.selectbox("Country", countries)

        with col2:
            driver_gender = st.selectbox("Driver Gender", ["M", "F"])
            driver_age = st.number_input("Driver Age", min_value=16, max_value=100, value=30)
            driver_race = st.selectbox("Driver Race", races)
            violation = st.selectbox("Violation", violations)

        st.subheader("Additional Information")
        col3, col4 = st.columns(2)
        with col3:
            search_conducted = st.checkbox("Search Conducted")
            search_type = st.text_input("Search Type", value="None") if search_conducted else "None"
            is_arrested = st.checkbox("Arrested")

        with col4:
            drugs_related = st.checkbox("Drug Related Stop")
            stop_duration = st.selectbox("Stop Duration", ["0-15 Min", "16-30 Min", "30+ Min"])
            stop_outcome = st.text_input("Stop Outcome", placeholder="e.g., Citation, Warning")

        submitted = st.form_submit_button("Add Log & Predict Outcome")

        if submitted:
            # Get prediction
            st.subheader("Prediction Based on Similar Cases")

            pred_df = execute_query(get_prediction_stats_query(TABLE_NAME, violation, driver_age, driver_race, driver_gender))

            if pred_df is not None and not pred_df.empty and pred_df['similar_cases'].iloc[0] > 0:
                col5, col6, col7, col8 = st.columns(4)
                col5.metric("Similar Cases", f"{pred_df['similar_cases'].iloc[0]:,.0f}")
                col6.metric("Arrest Probability", f"{pred_df['arrest_probability'].iloc[0]:.1f}%")
                col7.metric("Search Probability", f"{pred_df['search_probability'].iloc[0]:.1f}%")
                col8.metric("Drug Stop Probability", f"{pred_df['drug_probability'].iloc[0]:.1f}%")

                # Get most common outcome
                outcome_df = execute_query(get_most_common_outcome_query(TABLE_NAME, violation))
                if outcome_df is not None and not outcome_df.empty:
                    st.info(f"Most Common Outcome for '{violation}': **{outcome_df['stop_outcome'].iloc[0]}**")
            else:
                st.warning("No similar cases found for prediction")

            # Insert into database
            try:
                conn = get_connection()
                cursor = conn.cursor()

                values = (
                    stop_date.strftime('%Y-%m-%d'),
                    stop_time.strftime('%H:%M:%S'),
                    country,
                    driver_gender,
                    driver_age,
                    driver_age,
                    driver_race,
                    violation,
                    violation,
                    search_conducted,
                    search_type,
                    stop_outcome,
                    is_arrested,
                    stop_duration,
                    drugs_related,
                    vehicle_number
                )

                cursor.execute(insert_new_log_query(TABLE_NAME), values)
                conn.commit()
                cursor.close()

                st.success(f"✅ New log added successfully! Vehicle: {vehicle_number}")

            except Exception as e:
                st.error(f"Error adding log: {str(e)}")

if __name__ == "__main__":
    main()

