# Import Python packages
import streamlit as st
from snowflake.snowpark import Session

st.title('❄️ How to connect Streamlit to a Snowflake database')

# Establish Snowflake session
@st.cache_resource
def create_session():
    return Session.builder.configs(st.secrets.snowflake).create()

session = create_session()
st.success("Connected to Snowflake!")

# Write directly to the app
st.title("Example Streamlit App :balloon:")
st.write(
    """Replace this example with your own code!
    **And if you're new to Streamlit,** check
    out our easy-to-follow guides at
    [docs.streamlit.io](https://docs.streamlit.io).
    """
)

# Define a function to establish a Snowflake session
@st.cache_resource
def create_session():
    # Load Snowflake credentials from Streamlit secrets
    try:
        connection_parameters = {
            "account": st.secrets["snowflake"]["account"],
            "user": st.secrets["snowflake"]["user"],
            "password": st.secrets["snowflake"]["password"],
            "role": st.secrets["snowflake"]["role"],
            "warehouse": st.secrets["snowflake"]["warehouse"],
            "database": st.secrets["snowflake"]["database"],
            "schema": st.secrets["snowflake"]["schema"],
        }
        return Session.builder.configs(connection_parameters).create()
    except SnowparkException as e:
        st.error(f"Failed to create Snowflake session: {e}")
        return None


# Establish the session
session = create_session()

# Verify connection
if session:
    st.success("Connected to Snowflake!")
    # Example query using Snowflake session
    example_data = session.create_dataframe(
        [[50, 25, "Q1"], [20, 35, "Q2"], [60, 30, "Q3"]],
        schema=["HIGH_FIVES", "FIST_BUMPS", "QUARTER"]
    ).to_pandas()

    # Display the queried data and chart
    st.subheader("Example Data")
    st.dataframe(example_data)
    st.bar_chart(data=example_data, x="QUARTER", y="HIGH_FIVES")
else:
    st.error("Could not connect to Snowflake. Check your credentials.")
