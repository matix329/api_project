# FastAPI + Kafka Project

## Project Description

This project integrates **FastAPI** with **Kafka** for real-time data processing. **Kafka** is used for message brokering between different systems, and **FastAPI** serves as the API interface to send and receive data. The project demonstrates the basics of how data can be sent to Kafka topics and consumed by a consumer service in real time.

## Requirements

- Docker
- Docker Compose
- Python 3.9+
- Kafka
- Zookeeper

## Project Structure

* app.py            - FastAPI application with endpoints to send data to Kafka
* consumer.py       - Kafka consumer to consume messages from Kafka topics
* db.py             - Database interaction logic (if applicable)
* generate.py       - Script to generate data for testing or simulation
* config.py         - Configuration file for Kafka and other settings
* requirements.txt  - Python dependencies
* Dockerfile        - Dockerfile to containerize FastAPI application
* docker-compose.yml - Docker Compose file to manage services (Kafka, Zookeeper, FastAPI)

## Basic Flow

1. **Kafka Producer**: The FastAPI app sends data to specific Kafka topics like `topic_1`, `topic_2`, or `default_topic`.
2. **Kafka Consumer**: The consumer script listens to the Kafka topics and processes the incoming messages.
