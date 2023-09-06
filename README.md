# YouTube_Data_Harvesting_and_Warehousing_using_SQL_MongoDB_and_Streamlit
#### Domain : Social Media

### Problem Statement :
This project aims to develop a user-friendly Streamlit application that utilizes the Google API to extract information on a YouTube channel, stores it in a MongoDB database, migrates it to a SQL data warehouse, and enables users to search for channel details and join tables to view data in the Streamlit app

### Technologies used :
- [Python](https://www.python.org/)
- [MongoDB](https://www.mongodb.com/atlas/database)
- [MySQL](https://www.mysql.com/)
- [YouTube Data API](https://developers.google.com/youtube/v3)
- [Streamlit](https://docs.streamlit.io/library/api-reference)
- [Pandas](https://pandas.pydata.org/)

### Overview : 
This project aims to develop a user-friendly Streamlit application that utilizes the Google API to extract information on a YouTube channel, stores it in a MongoDB database, migrates it to a SQL data warehouse, and enables users to search for channel details and join tables to view data in the Streamlit app.

### Workflow :
- Using the Google API client library for Python, I connected to the YouTube API to access channel, video, and comment data.
- The API can be used to extract data from a YouTube channel by providing the Channel ID.
- After retrieving data from the YouTube API, I stored it in a MongoDB data lake. MongoDB is ideal for storing unstructured and semi-structured data.
- After collecting data from multiple sources, it is migrated and transformed into a structured MySQL data warehouse.
- I utilized SQL queries to join tables in the data warehouse and extract specific channel data based on user input.
- After executing a SQL query, I gained interesting insights about various YouTube channels.
- The data that was retrieved is then showcased within the Streamlit application for visual display.
- In summary, the process entails creating a basic user interface using Streamlit, obtaining information from the YouTube API, saving it in a MongoDB data repository, transferring it to a SQL data warehouse, using SQL to extract data from the data warehouse, and presenting the data in the Streamlit application.
