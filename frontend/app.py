import streamlit as st
import requests
import folium
from streamlit_folium import st_folium
from shapely import wkt
from datetime import datetime

API_URL = 'http://localhost:8000/search'

st.title("SDI-Search")
st.markdown('''Searching :blue-background[Spatial Data Infrastructures (SDIs)] using natural language.''')

def render_datasets(datasets):
    """Render dataset information in expanders."""
    if datasets:
        st.subheader("📊 Relevant Datasets")
        for i, dataset in enumerate(datasets, 1):
            if dataset.get('metadata'):
                metadata = dataset["metadata"]
                with st.expander(metadata.get("title", "Unknown Title")):
                    if metadata.get("title"):
                        st.write(f"**Title**: {metadata['title']}")
                    if metadata.get('description'):
                        st.write(f"**Description**: {metadata['description']}")
                    if metadata.get("keywords"):
                        st.write("**Keywords:**")
                        st.write(", ".join(metadata["keywords"]))
                    if metadata.get("access_urls"):
                        st.write("**Access URLs:**")
                        st.write("\n".join(metadata["access_urls"]))

                    # create a map that shows the dataset's extent
                    m = folium.Map(location=[51.9607, 7.62], zoom_start=14)

                    extent_wkt = metadata.get("spatial_extent")
                    if extent_wkt:
                        try:
                            geom = wkt.loads(extent_wkt)
                            if geom.geom_type == 'Polygon':
                                geojson_data = folium.GeoJson(
                                    data=geom.__geo_interface__,
                                    style_function=lambda x: {
                                        "color": "blue",
                                        "fillColor": "blue",
                                        "fillOpacity": 0.2,
                                        "weight": 2
                                    }
                                )
                                geojson_data.add_to(m)

                                # Zoom to the bounds of the polygon
                                minx, miny, maxx, maxy = geom.bounds
                                m.fit_bounds([[miny, minx], [maxy, maxx]])
                                
                        except Exception as e:
                            st.warning(f"Could not parse the spatial extent: {e}")
                             
                    st_folium(m, width=725, returned_objects=[], key=f"map-{i}-{dataset.get('dataset_id', str(i))}-{datetime.now().isoformat()}")
    else:
        st.info("No datasets found for this query")

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# display messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

    # Display datasets from the API response - only for assistant messages
    if message["role"] == "assistant":
        render_datasets(message.get("datasets", []))

# accept user prompt
if prompt := st.chat_input("What data are you looking for?"):
    #add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})

    # display user message in chat message container
    with st.chat_message("user"):
        st.markdown(prompt)

    # fetch the response from the backend
    with st.spinner("Searching for datasets..."):
        try:
            response = requests.post(
                API_URL,
                json={"query": prompt, "max_results": 5},
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 200:
                result = response.json()

                #display assistant response and datasets in chat message container
                with st.chat_message("assistant"):
                    st.markdown(result["answer"])
                    render_datasets(result.get("source_datasets", []))

                st.session_state.messages.append({
                    "role": "assistant",
                    "content": result["answer"],
                    "datasets": result.get("source_datasets", [])
                })
            else:
                st.error(f"API Error: {response.status_code} - {response.text}")
        except Exception as e:
            st.error(f"Request failed: {e}")