import streamlit as st
import pandas as pd
import pydeck as pdk
import requests
import time

# Configure the page to use the full screen width
st.set_page_config(page_title="Real-Time Ride Demand", layout="wide")

st.title("🚖 Real-Time Ride-Hailing Demand Map (NYC)")
st.markdown("This map updates automatically every 3 seconds from the FastAPI backend.")

map_placeholder = st.empty()

# The API URL
API_URL = "http://localhost:8000/demand"

# NYC Coordinates for the camera angle
INITIAL_VIEW_STATE = pdk.ViewState(
    latitude=40.75,
    longitude=-73.98,
    zoom=11,
    pitch=45, # Tilt the camera for a 3D view
    bearing=0
)

# Infinite loop to auto-refresh the Streamlit app
while True:
    try:
        # 1. Fetch live data from FastAPI
        response = requests.get(API_URL)
        data = response.json().get("data", [])
        
        if data:
            df = pd.DataFrame(data)
            
            # Normalize demand for color scaling (max demand gets pure red, low gets green/yellow)
            max_demand = df["demand"].max() if df["demand"].max() > 0 else 1
            
            # Simple color logic: [R, G, B, Alpha]
            # High demand = more Red, less Green
            df["r"] = 255
            df["g"] = ((1 - (df["demand"] / max_demand)) * 255).astype(int)
            df["b"] = 0
            df["a"] = 180 # Transparency
            
            # Create the PyDeck H3HexagonLayer
            layer = pdk.Layer(
                "H3HexagonLayer",
                df,
                pickable=True,
                stroked=True,
                filled=True,
                extruded=True,
                get_hexagon="h3_cell",
                get_fill_color="[r, g, b, a]",
                get_elevation="demand",
                elevation_scale=30,
            )
            
            r = pdk.Deck(
                layers=[layer], 
                initial_view_state=INITIAL_VIEW_STATE, 
                tooltip={"text": "H3 Cell: {h3_cell}\nCurrent Demand: {demand} rides"}
            )
            
            with map_placeholder:
                st.pydeck_chart(r)
        else:
            with map_placeholder:
                st.info("Waiting for data stream to begin...")
                
    except Exception as e:
        with map_placeholder:
            st.error(f"Error connecting to API: {e}")

    time.sleep(3)