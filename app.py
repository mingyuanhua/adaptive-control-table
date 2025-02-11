# Import necessary packages
import streamlit as st
import pandas as pd
import snowflake.connector

# Function to connect to Snowflake
@st.cache_resource
def get_snowflake_connection():
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"]
    )

# Connect to Snowflake
conn = get_snowflake_connection()
cursor = conn.cursor()

# App Title
st.title("📊 Adaptive Version Name & Schedule Frequency Input")

# Default table with editable fields
default_data = pd.DataFrame({
    "Version Name": ["", "", ""],
    "Schedule Frequency": ["", "", ""]
})

# Editable table (Users can input values)
st.write("### Enter Version Details Below 👇")
edited_data = st.data_editor(
    default_data, 
    num_rows="dynamic",
    column_config={
        "Version Name": st.column_config.TextColumn("Version Name"),
        "Schedule Frequency": st.column_config.SelectboxColumn(
            "Schedule Frequency", 
            options=["Daily", "Weekly", "Monthly"]
        )
    }
)

# Submit Button
if st.button("✅ Submit Data"):
    if not edited_data.empty:
        try:
            for _, row in edited_data.iterrows():
                version_name = row["Version Name"]
                schedule_frequency = row["Schedule Frequency"]

                if version_name and schedule_frequency:
                    cursor.execute(f"""
                        MERGE INTO DEV_COMMON.UTILITIES.ADAPTIVE_CONTROL_TABLE AS target
                        USING (SELECT '{version_name}' AS version_name, '{schedule_frequency}' AS schedule_frequency) AS source
                        ON target.version_name = source.version_name
                        WHEN MATCHED THEN
                            UPDATE SET target.schedule_frequency = source.schedule_frequency
                        WHEN NOT MATCHED THEN
                            INSERT (version_name, schedule_frequency) VALUES (source.version_name, source.schedule_frequency);
                    """)
            conn.commit()
            st.success("🎉 Data successfully inserted into Table!")

        except Exception as e:
            st.error(f"❌ Error: {e}")

    else:
        st.warning("⚠️ Please enter at least one row of data!")

# Display existing data
st.write("### 📋 Current Records in Table")
try:
    query_result = pd.read_sql("SELECT * FROM DEV_COMMON.UTILITIES.ADAPTIVE_CONTROL_TABLE", conn)
    st.dataframe(query_result)
except Exception as e:
    st.error(f"⚠️ Could not retrieve data: {e}")
