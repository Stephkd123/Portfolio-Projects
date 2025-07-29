🔥 FlameAI – Intelligent Campus Assistant <br>
Overview
FlameAI is an AI-powered intelligent assistant designed to provide real-time, structured, and context-aware information for Claremont Graduate University (CGU) students, faculty, and visitors. Developed by a team of graduate students, the assistant leverages advanced LLMs (OpenAI’s GPT-4o & GPT-4o mini), LangChain, and Tavily Search API to offer conversational support on course registration, student services, events, and campus resources via a user-friendly Streamlit interface.

Key Features

🧠 GPT-4o-Powered Conversational Agent: Developed using LangChain’s ChatOpenAI module with prompt templates and temperature tuning to control response behavior.<br>
🌐 Real-Time Web Search: Integrated Tavily Search API through LangChain tools for up-to-date campus information.<br>
🧭 Structured Reasoning: Uses PromptTemplate, output parsers, and document structures for reliable and organized replies.<br>
⚙️ Backend Architecture: Implements haversine distance-based location matching and SQLite-powered campus data retrieval via folium, sqlite3, and vectorized calculations.
🎛️ Web Interface: Intuitive Streamlit app allows users to enter queries and visualize mapped answers.<br>

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

📘 Document Ingestion & Knowledge Embedding Pipeline - (doc_vect_db.ipynb) for code samples
To ensure FlameAI delivers highly accurate, department-specific responses to university-related queries, we implemented a document vectorization pipeline that transforms official student handbooks from various CGU departments into searchable vector representations.

This pipeline follows current GenAI best practices:

📂 Document Loading: Departmental handbooks (PDF/Docx format) are programmatically loaded and parsed using LangChain's document loaders.

✂️ Document Chunking: We utilized RecursiveCharacterTextSplitter to divide long documents into coherent chunks with overlap, preserving context for more accurate retrieval.

🧠 Embedding Generation: Using OpenAIEmbeddings, each chunk was converted into high-dimensional vector representations optimized for semantic similarity search.

🗃️ Vector Store Creation: These vectors were stored in a PGVector-powered vector database, allowing efficient retrieval of relevant chunks using cosine similarity.

🔍 RAG Integration: At runtime, user queries are matched to relevant document chunks, retrieved in real time, and injected into the prompt template, ensuring FlameAI's responses are grounded in accurate and verifiable source material.

This document-processing system provides the foundation for Retrieval-Augmented Generation (RAG) in FlameAI, enhancing factual accuracy and allowing scalable updates as more academic content is integrated. This architecture reflects a deep understanding of knowledge grounding in modern AI systems, ensuring trustworthiness and relevance in every response.
