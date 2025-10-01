import streamlit as st
import requests
import json

API_URL = 'http://localhost:8000/search'

st.title("Spatial Data Search")

def render_datasets(datasets):
    """Render dataset information in expanders."""
    if datasets:
        st.subheader("📊 Found Datasets")
        for i, dataset in enumerate(datasets, 1):
            with st.expander(f"Dataset {i}: {dataset.get('dataset_id', 'Unknown ID')}"):
                if dataset.get("content"):
                    st.write("**Description**")
                    st.write(dataset["content"])

                if dataset.get("metadata"):
                    metadata = dataset["metadata"]
                    if metadata.get("title"):
                        st.write(f"**Title**: {metadata['title']}")
                    if metadata.get("keywords"):
                        st.write("**Keywords:**")
                        st.write(", ".join(metadata["keywords"]))
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