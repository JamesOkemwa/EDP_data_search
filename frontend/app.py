import streamlit as st
import requests
import json

API_URL = 'http://localhost:8000/search'

st.title("Spatial Data Search")

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# display messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# accept user prompt
if prompt := st.chat_input("What data are you looking for?"):
    #add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})

    # display user message in chat message container
    with st.chat_message("user"):
        st.markdown(prompt)

    # fetch the response from the backend
    try:
        response = requests.post(
            API_URL,
            json={"query": prompt, "max_results": 5},
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 200:
            result = response.json()

            #display assistant response in chat message container
            with st.chat_message("assistant"):
                st.markdown(result["answer"])

            st.session_state.messages.append({
                "role": "assistant",
                "content": result["answer"]
            })
        else:
            st.error(f"API Error: {response.status_code} - {response.text}")
    except Exception as e:
        st.error(f"Request failed: {e}")