import streamlit as st
import pandas as pd
import redshift_connector
import altair as alt

conn = redshift_connector.connect(
    host=st.secrets["redshift"]["host"],
    database=st.secrets["redshift"]["database"],
    port=st.secrets["redshift"]["port"],
    user=st.secrets["redshift"]["user"],
    password=st.secrets["redshift"]["password"],
)
print(conn)
# Set the title of the Streamlit app
st.title("Global Partners Metrics")

# Use Streamlit's caching to connect to Redshift.
# This keeps the connection open and prevents re-connecting on every interaction.
@st.cache_resource
def get_redshift_connection():
    conn = redshift_connector.connect(
        host=st.secrets["redshift"]["host"],
        database=st.secrets["redshift"]["database"],
        port=st.secrets["redshift"]["port"],
        user=st.secrets["redshift"]["user"],
        password=st.secrets["redshift"]["password"],
    )
    print("---")
    print(conn)
    print("---")
    return conn

@st.cache_data
def run_query(query):
    with get_redshift_connection().cursor() as cursor:
        cursor.execute(query)
        df = cursor.fetch_dataframe()
    return df

def get_customer_lifetime_value():
    sql_query = """
        SELECT * FROM "dev"."public"."clv";
    """

    # Fetch the data from Redshift
    df = run_query(sql_query)

    df['customer_lifetime_value'] = df['customer_lifetime_value'].astype(float)

    print(df)
    st.header("Customer Life Time Value")


    st.bar_chart(
        df,
        x='user_id',
        y='customer_lifetime_value',
    )

    st.dataframe(df)

def get_churn_indicators():
    sql_query = """
        SELECT * FROM "dev"."public"."churn_indicators";
    """

    # Fetch the data from Redshift
    df = run_query(sql_query)

    #df['customer_lifetime_value'] = df['customer_lifetime_value'].astype(float)

    print(df)
    st.header("Churn Indicators")


    st.bar_chart(
        df,
        x='user_id',
        y='days_since_last_order',
    )

    st.dataframe(df)

def main():
    get_customer_lifetime_value()
    get_churn_indicators()
if __name__ == "__main__":
    main()