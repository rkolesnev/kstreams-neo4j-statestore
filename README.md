# Neo4j State Store for Kafka Streams

A proof of concept demonstrating Neo4j embedded as a state store engine for Apache Kafka Streams applications.

## Overview

This repository explores innovative approaches to state management in Kafka Streams by replacing the default RocksDB state store with Neo4j. This enables leveraging graph database capabilities directly within stream processing applications.

## Key Features

- **Graph-based State Management**: Utilize Neo4j's graph capabilities for complex state relationships
- **Embedded Mode**: Neo4j runs embedded within your Kafka Streams application
- **Alternative State Store**: Demonstrates moving beyond RocksDB for specialized use cases
- **Event-driven Architecture**: Combines the power of event streams with graph database semantics

## Conference Talk

This project accompanies the conference talk **"Kafka - No Rocks, Please: Using Kafka Streams with Alternative State Stores"** presented at [Confluent Current 2024](https://current.confluent.io/2024-sessions/kafka---no-rocks-please-using-kafka-streams-with-alternative-state-stores).

The talk explores why and when you might want to use alternative state stores instead of RocksDB, with live demonstrations and best practices for production deployments.

## Sister Repository

See also [@rkolesnev/lucene-kstreams](https://github.com/rkolesnev/lucene-kstreams) - another alternative state store implementation showcasing Lucene for full-text search capabilities within Kafka Streams. Both repositories are companion implementations presented at the same conference talk.

## Technology Stack

- **Language**: Java
- **Kafka Streams**: Event streaming library
- **Neo4j**: Embedded graph database
- **License**: MIT

## Getting Started

See the [End2EndTest](./src/test/java/End2EndTest.java) in the tests folder for a demonstration of the Neo4j state store in action.
