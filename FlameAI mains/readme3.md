🔥 FlameAI – Intelligent Campus Assistant
Overview
FlameAI is an AI-powered intelligent assistant designed to provide real-time, structured, and context-aware information for Claremont Graduate University (CGU) students, faculty, and visitors. Developed by a team of graduate students, the assistant leverages advanced LLMs (OpenAI’s GPT-4o & GPT-4o mini), LangChain, and Tavily Search API to offer conversational support on course registration, student services, events, and campus resources via a user-friendly Streamlit interface.

Key Features

🧠 GPT-4o-Powered Conversational Agent: Developed using LangChain’s ChatOpenAI module with prompt templates and temperature tuning to control response behavior.
🌐 Real-Time Web Search: Integrated Tavily Search API through LangChain tools for up-to-date campus information.
🧭 Structured Reasoning: Uses PromptTemplate, output parsers, and document structures for reliable and organized replies.
⚙️ Backend Architecture: Implements haversine distance-based location matching and SQLite-powered campus data retrieval via folium, sqlite3, and vectorized calculations.
🎛️ Web Interface: Intuitive Streamlit app allows users to enter queries and visualize mapped answers.

Tech Stack

LangChain, OpenAI GPT-4o
Tavily API
Streamlit
Folium (for map visualization)
SQLite3 (for backend geolocation data)
Python (asyncio, os, dotenv)

🚀 Future Work

Add more industry-specific resume templates
Integrate job search API
Improve AI feedback tracing and memory
