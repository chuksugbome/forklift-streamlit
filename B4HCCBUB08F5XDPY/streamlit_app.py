import streamlit as st
import pandas as pd
from snowflake.snowpark.functions import col, current_timestamp
from snowflake.snowpark.types import *
from datetime import datetime, date, timedelta
import plotly.express as px
from snowflake.snowpark.context import get_active_session
# ---------------------------------------------------------
# PAGE SETUP
# ---------------------------------------------------------
st.set_page_config(
    page_title="NNL Electric Forklift Fleet Management",
    page_icon="🚛",
    layout="wide",
)

# ---------------------------------------------------------
# SNOWFLAKE SESSION
# ---------------------------------------------------------
@st.cache_resource
def create_session():
    return get_active_session()

# CREATE SESSION PROPERLY
session = create_session()

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------
def load_reference_data(table_name):
    return session.table(f"REFERENCE.{table_name}").collect()

def get_forklift_list():
    return session.table("OPERATIONS.FORKLIFTS").select("FORKLIFT_ID", "MODEL", "STATUS").collect()

def get_operator_list():
    return session.table("OPERATIONS.OPERATORS").filter(col("IS_ACTIVE") == True).collect()

# ---------------------------------------------------------
# SIDEBAR NAVIGATION
# ---------------------------------------------------------
st.sidebar.title("Navigation")
page = st.sidebar.selectbox(
    "Select Page",
    ["Dashboard", "Fleet Management", "Operator Management", "Battery Management", "Usage Logging",
     "Maintenance", "Maintenance History", "Fault Reporting", "Analytics"]
)

# ---------------------------------------------------------
# DASHBOARD
# ---------------------------------------------------------
if page == "Dashboard":
    st.header("📊 Fleet Dashboard")

    total = session.table("OPERATIONS.FORKLIFTS").count()
    operational = session.table("OPERATIONS.FORKLIFTS").filter(col("STATUS") == "Operational").count()
    maintenance = session.table("OPERATIONS.FORKLIFTS").filter(col("STATUS") == "In Maintenance").count()
    charging = session.table("OPERATIONS.FORKLIFTS").filter(col("STATUS") == "Charging").count()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Forklifts", total)
    c2.metric("Operational", operational)
    c3.metric("In Maintenance", maintenance)
    c4.metric("Charging", charging)

    st.subheader("Fleet Status Distribution")
    status_data = session.table("OPERATIONS.FORKLIFTS").group_by("STATUS").count().collect()

    if status_data:
        df_status = pd.DataFrame([(r["STATUS"], r["COUNT"]) for r in status_data],
                                 columns=["Status", "Count"])
        st.plotly_chart(px.pie(df_status, values="Count", names="Status"))

# ---------------------------------------------------------
# FLEET MANAGEMENT
# ---------------------------------------------------------
elif page == "Fleet Management":
    st.header("🚛 Fleet Management")

    tab1, tab2, tab3 = st.tabs(["View Fleet", "Add New Forklift", "Update Forklift"])

    # ---------------- VIEW FLEET ----------------
    with tab1:
        fleet = session.table("OPERATIONS.FORKLIFTS").collect()
        if fleet:
            df = pd.DataFrame([
                {
                    "Forklift ID": x["FORKLIFT_ID"],
                    "Serial": x["SERIAL_NUMBER"],
                    "Model": x["MODEL"],
                    "Year": x["MANUFACTURE_YEAR"],
                    "Capacity": x["RATED_CAPACITY_KG"],
                    "Location": x["LOCATION_SITE"],
                    "Status": x["STATUS"]
                } for x in fleet
            ])
            st.dataframe(df, use_container_width=True)

    # ---------------- ADD NEW FORKLIFT ----------------
    with tab2:
        st.subheader("Add New Forklift")

        with st.form("add_forklift_form"):
            left, right = st.columns(2)

            with left:
                forklift_id = st.text_input("Forklift ID*", placeholder="FL-001")
                serial_number = st.text_input("Serial Number*")
                model = st.text_input("Model*", placeholder="Toyota 8FBE25")
                manufacture_year = st.number_input("Manufacture Year", min_value=2000, max_value=2030)
                purchase_date = st.date_input("Purchase Date")

                # WARRANTY CHECKBOX
                has_warranty = st.checkbox("Add Warranty Expiry?")
                warranty_expiry = st.date_input("Warranty Expiry") if has_warranty else None

            with right:
                capacity = st.number_input("Rated Capacity (kg)", min_value=500, max_value=10000)
                location = st.text_input("Location Site")
                status_options = [r["STATUS"] for r in load_reference_data("FORKLIFT_STATUS")]
                status = st.selectbox("Status", status_options)
                notes = st.text_area("Notes")

            submitted = st.form_submit_button("Add Forklift")

            if submitted:
                try:
                    import datetime

                    # Use pure Python datetime - NO Snowflake expressions
                    created_at = datetime.datetime.now()
                    updated_at = None

                    df = session.create_dataframe(
                        [
                            (
                                forklift_id,
                                serial_number,
                                model,
                                manufacture_year,
                                purchase_date,
                                capacity,
                                None,               # BATTERY_ID
                                location,
                                status,
                                warranty_expiry,
                                notes,
                                created_at,
                                updated_at
                            )
                        ],
                        schema=StructType([
                            StructField("FORKLIFT_ID", StringType()),
                            StructField("SERIAL_NUMBER", StringType()),
                            StructField("MODEL", StringType()),
                            StructField("MANUFACTURE_YEAR", IntegerType()),
                            StructField("PURCHASE_DATE", DateType()),
                            StructField("RATED_CAPACITY_KG", IntegerType()),
                            StructField("BATTERY_ID", IntegerType()),
                            StructField("LOCATION_SITE", StringType()),
                            StructField("STATUS", StringType()),
                            StructField("WARRANTY_EXPIRY", DateType()),
                            StructField("NOTES", StringType()),
                            StructField("CREATED_AT", TimestampType()),
                            StructField("UPDATED_AT", TimestampType())
                        ])
                    )

                    df.write.mode("append").save_as_table("OPERATIONS.FORKLIFTS")
                    st.success("Forklift added successfully!")
                    st.rerun()

                except Exception as e:
                    st.error(f"❌ Error: {e}")

# -------------------------------------------------------------------
#OPERATOR MANAGEMENT
# -------------------------------------------------------------------

elif page == "Operator Management":
    st.header("👷 Operator Management")

    tab1, tab2, tab3 = st.tabs(["View Operators", "Add Operator", "Delete Operator"])

    # ---------------------- VIEW OPERATORS ----------------------
    with tab1:
        data = session.table("OPERATIONS.OPERATORS").collect()
        if data:
            df = pd.DataFrame([row.as_dict() for row in data])
            st.dataframe(df, use_container_width=True)
        else:
            st.info("No operators found in database.")

    # ---------------------- ADD OPERATOR ------------------------
    with tab2:
        st.subheader("➕ Add New Operator")

        full_name = st.text_input("Full Name*")
        employee_id = st.text_input("Employee ID*")
        employment_year = st.date_input("Employment Year", value=None)
        is_active = st.checkbox("Active", value=True)

        if st.button("Add Operator"):
            if not full_name or not employee_id:
                st.error("Full name, employee ID and Employment Year are required.")
            else:
                df = session.create_dataframe(
                [(employee_id, full_name, employment_year, is_active)],
                schema=[
                    "EMPLOYEE_ID",
                    "FULL_NAME",
                    "EMPLOYMENT_YEAR",
                    "IS_ACTIVE"
                ]
            )

            df.write.mode("append").save_as_table("OPERATIONS.OPERATORS")
            st.success(f"Operator '{full_name}' added successfully!")
            st.rerun()



    # ---------------------- DELETE OPERATOR ---------------------
    with tab3:
        st.subheader("🗑 Delete Operator")

        operators = session.table("OPERATIONS.OPERATORS").collect()
        if operators:
            op_map = {
                f"{row.FULL_NAME} (ID: {row.EMPLOYEE_ID})": row.EMPLOYEE_ID
                for row in operators
            }

            selected = st.selectbox("Select Operator to Delete", list(op_map.keys()))
            op_id = op_map[selected]

            if st.button("Delete Operator"):
                session.sql(
                    f"DELETE FROM OPERATIONS.OPERATORS WHERE OPERATOR_ID = {op_id}"
                ).collect()

                st.warning(f"Operator '{selected}' deleted successfully!")
                st.rerun()

        else:
            st.info("No operators available to delete.")


    
# ---------------------------------------------------------
# BATTERY MANAGEMENT
# ---------------------------------------------------------
elif page == "Battery Management":
    st.header("🔋 Battery Management")

    tab1, tab2, tab3 = st.tabs(["Battery Status", "Add Battery", "Charging Sessions"])

    # ---------------------- BATTERY STATUS ----------------------
    with tab1:
        data = session.table("OPERATIONS.BATTERIES").collect()
        if data:
            df = pd.DataFrame([row.as_dict() for row in data])
            st.dataframe(df, use_container_width=True)
    
    # ---------------------- ADD NEW BATTERY ----------------------
    with tab2:
        st.subheader("Add New Battery")

        with st.form("add_battery_form"):
            left, right = st.columns(2)

            with left:
                battery_id = st.number_input("Battery ID*", min_value=1, step=1)
                serial = st.text_input("Battery Serial Number*")

                forklift_choices = ["None"] + [
                    x["FORKLIFT_ID"] for x in get_forklift_list()
                ]
                assigned = st.selectbox("Assigned Forklift", forklift_choices)

                voltage = st.number_input("Voltage", min_value=12.0, max_value=80.0, step=0.1)

            with right:
                ah = st.number_input("Capacity (Ah)", min_value=100, max_value=1000)
                purchase = st.date_input("Purchase Date")
                health = st.number_input("Health (%)", min_value=0.0, max_value=100.0)

                status_options = [r["STATUS"] for r in load_reference_data("BATTERY_STATUS")]
                status = st.selectbox("Status", status_options)

            submitted = st.form_submit_button("Add Battery")

            if submitted:
                try:
                    import datetime
                    from snowflake.snowpark.types import (
                        StructType, StructField,
                        IntegerType, StringType, DoubleType,
                        DateType, TimestampType
                    )

                    # Convert "None" → NULL
                    forklift_id = None if assigned == "None" else assigned

                    created_at = datetime.now()
                    updated_at = None   # allowed NULL

                    # Build dataframe row
                    df = session.create_dataframe(
                        [
                            (
                                battery_id,          # 1 BATTERY_ID
                                serial,              # 2 BATTERY_SERIAL
                                forklift_id,         # 3 FORKLIFT_ID
                                voltage,             # 4 VOLTAGE
                                ah,                  # 5 CAPACITY_AH
                                purchase,            # 6 PURCHASE_DATE
                                0,                   # 7 TOTAL_CYCLES
                                health,  # 8 HEALTH_PERCENTAGE
                                 None,                # 9 LAST_FULL_CHARGE
                                status,              # 10 STATUS
                                created_at,          # 11 CREATED_AT
                                updated_at           # 12 UPDATED_AT
                            )
                        ],
                        schema=StructType([
                            StructField("BATTERY_ID", IntegerType()),
                            StructField("BATTERY_SERIAL", StringType()),
                            StructField("FORKLIFT_ID", StringType()),
                            StructField("VOLTAGE", DoubleType()),
                            StructField("CAPACITY_AH", IntegerType()),
                            StructField("PURCHASE_DATE", DateType()),
                            StructField("TOTAL_CYCLES", IntegerType()),
                            StructField("HEALTH_PERCENTAGE", DoubleType()),
                            StructField("LAST_FULL_CHARGE", DateType()),
                            StructField("STATUS", StringType()),
                            StructField("CREATED_AT", TimestampType()),
                            StructField("UPDATED_AT", TimestampType())
                        ])
                    )

                    df.write.mode("append").save_as_table("OPERATIONS.BATTERIES")

                    st.success("Battery added successfully!")
                    st.rerun()

                except Exception as e:
                    st.error(f"❌ Error: {e}")


# ---------------------------------------------------------
# USAGE LOGGING
# ---------------------------------------------------------
elif page == "Usage Logging":
    st.header("📝 Daily Usage Logging")

    tab1, tab2 = st.tabs(["Log Usage", "View History"])

    submit = False  # defensive initialization


# ====================== LOG USAGE ======================
    with tab1:
        with st.form("usage_form"):
    # Input fields for logging usage
            forklift_id = st.text_input("Forklift ID*", max_chars=10)
            shift_date = st.date_input("Shift Date*", value=date.today())
            start_hour_meter = st.number_input("Start Hour Meter", min_value=0.0, step=0.1)
            end_hour_meter = st.number_input("End Hour Meter", min_value=0.0, step=0.1)
            battery_start_soc = st.number_input("SOC Start (%)", min_value=0.0, max_value=100.0)
            battery_end_soc = st.number_input("SOC End (%)", min_value=0.0, max_value=100.0)
            energy_consumed_kwh = st.number_input("Energy Consumed (kWh)", min_value=0.0, step=0.1)
            notes = st.text_area("Notes")

    # Submit button
            submit_button = st.form_submit_button("Log Usage")

        # -------- processing OUTSIDE form --------
        if submit_button:
            if end_hour_meter < start_hour_meter:
                st.error("End Hour Meter cannot be less than Start Hour Meter.")
                st.stop()

    # Create a DataFrame with the logged usage data
            df = session.create_dataframe(
            [
                (
                    forklift_id,
                    shift_date,
                    start_hour_meter,
                    end_hour_meter,
                    battery_start_soc,
                    battery_end_soc,
                    energy_consumed_kwh,
                    notes,
                    datetime.now()
                )
            ],
            schema=StructType([
                StructField("FORKLIFT_ID", StringType()),
                StructField("SHIFT_DATE", DateType()),
                StructField("START_HOUR_METER", DoubleType()),
                StructField("END_HOUR_METER", DoubleType()),
                StructField("BATTERY_START_SOC", DoubleType()),
                StructField("BATTERY_END_SOC", DoubleType()),
                StructField("ENERGY_CONSUMED_KWH", DoubleType()),
                StructField("NOTES", StringType()),
                StructField("LOGGED_AT", TimestampType())
            ])
        )

    # Save the DataFrame to the specified table
            df.write.mode("append").save_as_table("OPERATIONS.USAGE_LOG")
            st.success("✅ Usage logged successfully!")
            st.rerun()  # Refresh the app to show updated data

# ====================== VIEW LOGGED USAGE ======================
    with tab2:

# Fetch and display logged usage data
        log_data = session.table("usage_log").to_pandas()

        if not log_data.empty:
            st.dataframe(log_data)
        else:
            st.write("No usage logs found.")



# ====================== MAINTENANCE & SERVICE SCHEDULE ==============
elif page == "Maintenance":
    st.header("Maintenance Schedule")

    tab1, tab2 = st.tabs(["View Schedule", "Add Maintenance Task"])

    submit = False

# ----------------------------------------------------------------
# VIEW MAINTENANCE SCHEDULE
# ----------------------------------------------------------------

    with tab1:
        st.subheader("Maintenance Schedule")
        data = session.table("OPERATIONS.MAINTENANCE_SCHEDULE").collect()
        if data:
            df = pd.DataFrame([row.as_dict() for row in data])
            st.dataframe(df, use_container_width=True)
        else:
            st.info("No maintenance schedules found in database.")


# ------------------------------------------------------------------
# ADD MAINTENANCE SCHEDULE
# ------------------------------------------------------------------

    with tab2:
        st.subheader("Add Maintenance Task")
        with st.form("add_maintenance"):
            forklift_id = st.text_input("FORKLIFT ID")
            task_name = st.text_input("TASK NAME")
            type = st.selectbox("Maintenance Type", ["PREVENTIVE", "INSPECTION", "REPAIR"])
            frequency_hours = st.number_input("Frequency (Hours)", min_value=0, step=10)
            frequency_months = st.number_input("Frequency (Months)", min_value=0, step=1)
            last_date = st.date_input("Last Performed Date", value=datetime.now())
            last_hours = st.number_input("Last Performed Hour Meter", min_value=0.0, step=0.5)
            next_date = st.date_input("Next Due Date", value=datetime.now())
            next_hours = st.number_input("Next Due Hours", min_value=0.0, step=0.5)
            is_active = st.checkbox("Active", value=True)
            notes = st.text_area("Notes")

            submit_button = st.form_submit_button("Save Maintenance Task")

 # -------- processing OUTSIDE form --------
        if submit_button:
            if not forklift_id or not task_name:
                st.error("Forklift ID and Task Name are required.")
            else:
                next_due_date = (last_date + timedelta(days=frequency_months * 30)
                                if frequency_months > 0 
                                else None)
                next_due_hours = (last_hours + frequency_hours
                                 if frequency_hours > 0
                                 else None)
                
           # Create a DataFrame with the logged usage data
            df = session.create_dataframe(
            [
                (
                    forklift_id,
                    task_name,
                    type,
                    frequency_hours,
                    frequency_months,
                    last_date,
                    last_hours,
                    next_date,
                    next_hours,
                    is_active,
                    notes
                )
            ],
            schema=StructType([
                StructField("FORKLIFT_ID", StringType()),
                StructField("TASK_NAME", StringType()),
                StructField("TYPE", StringType()),
                StructField("FREQUENCY_HOURS", IntegerType()),
                StructField("FREQUENCY_MONTHS", IntegerType()),
                StructField("LAST_PERFORMED_DATE", DateType()),
                StructField("LAST_PERFORMED_HOUR_METER", DoubleType()),
                StructField("NEXT_DUE_DATE", DateType()),
                StructField("NEXT_DUE_HOURS", DoubleType()),
                StructField("IS_ACTIVE", BooleanType()),
                StructField("NOTES", StringType())
            ])
        ) 

    # Save the DataFrame to the specified table
            df.write.mode("append").save_as_table("OPERATIONS.MAINTENANCE_SCHEDULE")
            st.success("✅ Maintenance Scheduled successfully!")
            st.rerun()  # Refresh the app     
        

# -------------------------------------------------------------------
# MAINTENANCE HISTORY
# -------------------------------------------------------------------

elif page == "Maintenance History":
    st.header("Maintenance Record")
    
    tab1, tab2 = st.tabs(["📋 View Records", "➕ Log Maintenance"])

    submit = False

# =========================================================
# VIEW RECORDS
# =========================================================
    with tab1:
        st.subheader("📋 Maintenance History")
        data = session.table("OPERATIONS.MAINTENANCE_RECORDS").collect()
        if data:
            df = pd.DataFrame([row.as_dict() for row in data])
            st.dataframe(df, use_container_width=True)
        else:
            st.info("No maintenance history found in database.")

        
# =========================================================
# ADD MAINTENANCE RECORD
# =========================================================
    with tab2:
        st.subheader("➕ Log Maintenance Activity")

        with st.form("maintenance_form"):
            forklift_id = st.text_input("Forklift ID *")
            work_order_number = st.text_input("Work Order Number")

            performed_date = st.date_input(
                "Performed Date *",
                value=date.today()
            )

            hour_meter = st.number_input(
                "Hour Meter Reading",
                min_value=0.0,
                step=0.5
            )

            technician = st.text_input("Technician Name")
            task_description = st.text_area("Task Description")
            parts_used = st.text_area("Parts Used")

            labor_hours = st.number_input(
                "Labor Hours",
                min_value=0.0,
                step=0.25
            )

            cost = st.number_input(
                "Maintenance Cost",
                min_value=0.0,
                step=100.0
            )

            downtime_hours = st.number_input(
                "Downtime (Hours)",
                min_value=0.0,
                step=0.25
            )

            status = st.selectbox(
                "Status",
                ["Completed", "In Progress", "Cancelled"]
            )

            submit = st.form_submit_button("💾 Save Record")


# ---------------------------------------------------------
# PROCESS SUBMISSION
# ---------------------------------------------------------
        if submit:
            if not forklift_id:
                st.error("Forklift ID is required.")
            else:
                df_insert = session.create_dataframe(
                    [
                        (
                            forklift_id,
                            work_order_number or None,
                            performed_date,
                            hour_meter or None,
                            technician or None,
                            task_description or None,
                            parts_used or None,
                            labor_hours or None,
                            cost or None,
                            downtime_hours or None,
                            status,
                            datetime.now(),   # ✅ created_at
                        )
                    ],
                    schema=StructType([
                        StructField("FORKLIFT_ID", StringType()),
                        StructField("WORK_ORDER_NUMBER", StringType()),
                        StructField("PERFORMED_DATE", DateType()),
                        StructField("HOUR_METER", DoubleType()),
                        StructField("TECHNICIAN", StringType()),
                        StructField("TASK_DESCRIPTION", StringType()),
                        StructField("PARTS_USED", StringType()),
                        StructField("LABOR_HOURS", DoubleType()),
                        StructField("COST", DoubleType()),
                        StructField("DOWNTIME_HOURS", DoubleType()),
                        StructField("STATUS", StringType()),
                        StructField("CREATED AT", DateType())
                    ])
                )

                df_insert.write.mode("append").save_as_table("OPERATIONS.MAINTENANCE_RECORDS")
                st.success("✅ Maintenance record logged successfully!")
                st.rerun()


# ---------------------------------------------------------
# FAULT REPORTING
# ---------------------------------------------------------

elif page == "Fault Reporting":
    st.header("Fault Reporting")
    
    tab1, tab2 = st.tabs(["📋 View Faults", "➕ Report Fault"])

    submit = False

# =========================================================
# VIEW FAULTS
# =========================================================
    with tab1:
        st.subheader("📋 View Faults")
        data = session.table("OPERATIONS.FAULTS").collect()
        if data:
            df = pd.DataFrame([row.as_dict() for row in data])
            st.dataframe(df, use_container_width=True)
        else:
            st.info("No faults Log found in database.")

# =========================================================
# REPORT FAULT
# =========================================================
    with tab2:
        st.subheader("➕ Report New Fault")

        with st.form("fault_form"):
            forklift_id = st.text_input("Forklift ID *")
            reported_date = st.date_input("Reported Date", value=None)
            reported_by = st.text_input(
                "Reported By (Operator ID)",
            )

            fault_code = st.text_input("Fault Code")

            description = st.text_area(
                "Fault Description *"
            )

            priority = st.selectbox(
                "Priority",
                ["Low", "Medium", "High", "Critical"],
                index=1
            )

            status = st.selectbox(
                "Status",
                ["Open", "In Progress", "Resolved"],
                index=0
            )

            resolved_date = st.date_input(
                "Resolved Date",
                value=None
            )

            root_cause = st.text_area("Root Cause")
            resolution_notes = st.text_area("Resolution Notes")

            downtime_hours = st.number_input(
                "Downtime Hours",
                min_value=0.0,
                step=0.25
            )

            submit = st.form_submit_button("🚨 Submit Fault")

# ---------------------------------------------------------
# PROCESS SUBMISSION
# ---------------------------------------------------------
   
        if submit:
            if not forklift_id or not description:
                st.error("Forklift ID and Description are required.")
            else:
                reported_ts = datetime.now()
                resolved_ts = (
                    datetime(
                        resolved_date.year,
                        resolved_date.month,
                        resolved_date.day,
                        0, 0
                    )
                    if resolved_date else None
                )

                df_insert = session.create_dataframe(
                    [
                        (
                            forklift_id,
                            reported_ts,
                            reported_by if reported_by else None,
                            fault_code or None,
                            description,
                            priority,
                            status,
                            resolved_ts,
                            root_cause or None,
                            resolution_notes or None,
                            downtime_hours or None
                        )
                    ],
                    schema=StructType([
                        StructField("FORKLIFT_ID", StringType()),
                        StructField("REPORTED_DATE", TimestampType()),
                        StructField("REPORTED_BY", StringType()),
                        StructField("FAULT_CODE", StringType()),
                        StructField("DESCRIPTION", StringType()),
                        StructField("PRIORITY", StringType()),
                        StructField("STATUS", StringType()),
                        StructField("RESOLVED_DATE", TimestampType()),
                        StructField("ROOT_CAUSE", StringType()),
                        StructField("RESOLUTION_NOTES", StringType()),
                        StructField("DOWNTIME_HOURS", DoubleType())
                    ])
                )

                df_insert.write.mode("append").save_as_table(
                    "NNL_FORKLIFT_FLEET_DB.OPERATIONS.FAULTS"
                )

                st.success("✅ Fault reported successfully!")
                st.rerun()



# ---------------------------------------------------------
# ANALYTICS
# ---------------------------------------------------------
elif page == "Analytics":
    st.header("📈 Fleet Analytics")

    st.subheader("Utilization (Last 30 Days)")
    util = session.sql("""
        SELECT 
            FORKLIFT_ID,
            AVG(END_HOUR_METER - START_HOUR_METER) AS AVG_HOURS,
            COUNT(*) AS DAYS_LOGGED
        FROM OPERATIONS.USAGE_LOG
        WHERE SHIFT_DATE >= DATEADD(day, -30, CURRENT_DATE())
        GROUP BY 1
        ORDER BY AVG_HOURS DESC
    """).collect()

    if util:
        df = pd.DataFrame([{
            "Forklift": x["FORKLIFT_ID"],
            "Avg Hours/Day": round(x["AVG_HOURS"], 2),
            "Days Logged": x["DAYS_LOGGED"]
        } for x in util])
        st.dataframe(df, use_container_width=True)

    st.subheader("Energy Consumption (Last 30 Days)")
    energy = session.sql("""
        SELECT 
            FORKLIFT_ID,
            SUM(ENERGY_CONSUMED_KWH) AS TOTAL_ENERGY,
            AVG(ENERGY_CONSUMED_KWH) AS AVG_ENERGY
        FROM OPERATIONS.USAGE_LOG
        WHERE SHIFT_DATE >= DATEADD(day, -30, CURRENT_DATE())
        GROUP BY 1
        ORDER BY TOTAL_ENERGY DESC
    """).collect()

    if energy:
        df = pd.DataFrame([{
            "Forklift": x["FORKLIFT_ID"],
            "Total kWh": round(x["TOTAL_ENERGY"], 2),
            "Avg kWh": round(x["AVG_ENERGY"], 2),
        } for x in energy])
        st.dataframe(df, use_container_width=True)
    
    st.subheader("💰 Maintenance Cost Summary by Forklift")

    from snowflake.snowpark.functions import col, sum as sf_sum, expr

    maintenance_df = session.table(
        "NNL_FORKLIFT_FLEET_DB.OPERATIONS.MAINTENANCE_RECORDS"
    )

    # Total maintenance cost (all time)
    total_cost_df = (
        maintenance_df
        .group_by(col("FORKLIFT_ID"))
        .agg(
            sf_sum(col("COST")).alias("TOTAL_MAINTENANCE_COST")
        )
    )

    # Maintenance cost in last 30 days
    last_30_days_cost_df = (
        maintenance_df
        .filter(
            col("PERFORMED_DATE") >= expr("DATEADD(day, -30, CURRENT_DATE())")
        )
        .group_by(col("FORKLIFT_ID"))
        .agg(
            sf_sum(col("COST")).alias("LAST_30_DAYS_COST")
        )
    )

    # Combine results
    summary_df = (
        total_cost_df
        .join(
            last_30_days_cost_df,
            on="FORKLIFT_ID",
            how="left"
        )
        .fillna({"LAST_30_DAYS_COST": 0})
        .sort(col("TOTAL_MAINTENANCE_COST").desc())
    )

    st.dataframe(
        summary_df.to_pandas(),
        use_container_width=True
    )

# 🏆 Top 5 Most Expensive Forklifts

    st.subheader("🏆 Top 5 Most Expensive Forklifts (All-Time)")

    top5_df = (
        summary_df
        .select(
            col("FORKLIFT_ID"),
            col("TOTAL_MAINTENANCE_COST")
        )
        .limit(5)
        .to_pandas()
    )

# Bar chart
    st.bar_chart(
        data=top5_df,
        x="FORKLIFT_ID",
        y="TOTAL_MAINTENANCE_COST",
        use_container_width=True
    )

# 📊 MONTHLY MAINTENANCE COST TREND

    st.subheader("📊 Monthly Maintenance Cost Trend per Forklift")

    from snowflake.snowpark.functions import col, sum as sf_sum, expr

# Optional forklift filter
    forklift_list = (
        session.table("NNL_FORKLIFT_FLEET_DB.OPERATIONS.MAINTENANCE_RECORDS")
        .select("FORKLIFT_ID")
        .distinct()
        .sort("FORKLIFT_ID")
        .to_pandas()["FORKLIFT_ID"]
        .tolist()
    )

    selected_forklifts = st.multiselect(
        "Select Forklift(s)",
        options=forklift_list,
        default=forklift_list[:1]
    )

    if selected_forklifts:

        monthly_df = (
            session.table("NNL_FORKLIFT_FLEET_DB.OPERATIONS.MAINTENANCE_RECORDS")
            .filter(col("FORKLIFT_ID").isin(selected_forklifts))
            .with_column(
                "MONTH",
                expr("DATE_TRUNC('month', PERFORMED_DATE)")
            )
            .group_by("MONTH", "FORKLIFT_ID")
            .agg(
                sf_sum(col("COST")).alias("MONTHLY_COST")
            )
            .sort("MONTH")
        )

        pdf = monthly_df.to_pandas()

    # Pivot for Streamlit chart
        pivot_df = pdf.pivot(
            index="MONTH",
            columns="FORKLIFT_ID",
            values="MONTHLY_COST"
        ).fillna(0)

        st.line_chart(
            pivot_df,
            use_container_width=True
        )

    else:
        st.info("Select at least one forklift to view trends.")




    

# ---------------------------------------------------------
# FOOTER
# ---------------------------------------------------------
st.markdown("---")
st.markdown("**NNL Electric Forklift Fleet Management System** | Powered by Snowflake + Streamlit")