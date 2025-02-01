# Solar Energy Forecasting with Real-Time Weather Data

## Introduction
A couple of months ago, I worked on a project to build a solar energy forecasting model aimed at aiding Germany’s transition to clean energy. While the results were promising, the model’s performance was limited by a crucial factor: the lack of weather data, which is a strong indicator of available solar energy.

This experience highlighted an essential lesson — real-time data is crucial for practical forecasting models. However, collecting such data requires a robust data engineering solution.

This project proposes a workflow leveraging AWS to build a high-performing and scalable data pipeline for real-time weather data collection. The tools and techniques applied here can also be used for other real-time analytics challenges.

## Data
The pipeline retrieves weather data using OpenWeatherMap, an online service providing forecasts and nowcasts for locations worldwide.

- OpenWeatherMap API enables quick and easy access to weather data.
- The API updates its nowcast every 10 minutes.
- The pipeline pulls data at the same frequency and stores it in a database.
- The project focuses on weather data for Berlin, Germany, for simplicity.

## Proposed Workflow
The data collection workflow is designed to update the database with real-time weather data. Since the OpenWeatherMap API updates every 10 minutes, API calls are made at the same frequency, with responses stored immediately in the database.

Whenever the data is needed for downstream tasks such as visualization or machine learning, it is read, transformed into a tabular format, and exported to a flat file.

## Technologies Used
- **AWS Lambda**: For serverless execution of data processing functions.
- **OpenWeatherMap API**: For real-time weather data retrieval.
- **Database**: For storing and managing weather data.
- **Data Transformation**: Converting data into a structured format for analysis.

## Repository Contents
This repository contains the AWS Lambda functions used in the data pipeline. These functions handle API requests, data processing, and storage.

## Usage
1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/your-repo.git
   ```
2. Set up API keys for OpenWeatherMap.
3. Deploy AWS Lambda functions.
4. Configure AWS services for data storage and processing.
5. Run the data pipeline to collect and store real-time weather data.

## Future Work
- Extend the pipeline to multiple locations.
- Enhance data storage and processing efficiency.
- Integrate advanced machine learning models for improved forecasting.

## Contributing
Contributions are welcome! Feel free to fork this repository, create a new branch, and submit a pull request.

## License
This project is licensed under the MIT License.

