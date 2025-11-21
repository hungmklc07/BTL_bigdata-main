# Real-Time Streaming Analytics with Apache Spark and Python
## GitHub Repository Analytics System

**Author:** Shachak Rozenblat  
**Student ID:** 216141459

---

## Table of Contents

1. [System Architecture](#1-system-architecture)
   - 1.1 [Overview](#11-overview)
   - 1.2 [Component Architecture](#12-component-architecture)
   - 1.3 [Data Flow Architecture](#13-data-flow-architecture)
   - 1.4 [Container Orchestration](#14-container-orchestration)

2. [Service Interactions](#2-service-interactions)
   - 2.1 [Data Source to Spark Communication](#21-data-source-to-spark-communication)
   - 2.2 [Spark to Webapp Communication](#22-spark-to-webapp-communication)
   - 2.3 [Webapp to Frontend Communication](#23-webapp-to-frontend-communication)
   - 2.4 [Inter-Service Dependencies](#24-inter-service-dependencies)

3. [Spark Application Streaming Data Processing](#3-spark-application-streaming-data-processing)
   - 3.1 [Streaming Context Initialization](#31-streaming-context-initialization)
   - 3.2 [Data Ingestion and Transformation](#32-data-ingestion-and-transformation)
   - 3.3 [Batch Processing Workflow](#33-batch-processing-workflow)
   - 3.4 [Core Analysis Tasks (Requirements 3.1-3.4)](#34-core-analysis-tasks-requirements-31-34)
   - 3.5 [Advanced Analytics (Requirements 3.5, 4.1-4.3)](#35-advanced-analytics-requirements-35-41-43)
   - 3.6 [State Management and Deduplication](#36-state-management-and-deduplication)
   - 3.7 [Output and Visualization](#37-output-and-visualization)
   - 3.8 [Fault Tolerance](#38-fault-tolerance)

4. [System Deployment and Execution](#4-system-deployment-and-execution)

5. [Key Design Decisions and Trade-offs](#5-key-design-decisions-and-trade-offs)

6. [Conclusion](#6-conclusion)

---

## Executive Summary

This report describes a real-time streaming analytics system designed to analyze GitHub repository data in real-time. The system implements a three-tier architecture consisting of a data ingestion service, an Apache Spark streaming cluster, and a web visualization dashboard. Data flows from GitHub API through a TCP socket stream to Spark, which processes data in 60-second micro-batches, performing eight distinct analytical tasks including repository counting, star averages, text analysis, and advanced machine learning-based insights. Results are transmitted to a Flask web application that generates visualizations and serves them to end users through an interactive dashboard. The entire system is containerized using Docker Compose, enabling seamless deployment and scalability. This report details the system architecture, service interactions, and Spark streaming data processing mechanisms.

---

## 1. System Architecture

### 1.1 Overview

The system is a distributed, containerized big data streaming analytics platform designed to perform real-time analysis on GitHub repository data. The architecture follows a microservices pattern with three main components: a data source service, an Apache Spark streaming cluster, and a web visualization application. All components are orchestrated using Docker and Docker Compose, enabling seamless deployment and scalability.

### 1.2 Component Architecture

The system consists of five main services, each running in separate Docker containers:

#### 1.2.1 Data Source Service (`data-source`)
- **Technology:** Python 3.9
- **Purpose:** Continuously fetches repository data from GitHub API
- **Key Features:**
  - Queries GitHub API for the most recently-pushed repositories in Python, Java, and JavaScript
  - Retrieves up to 50 repositories per language per request
  - Operates on a 15-second interval cycle
  - Establishes a TCP socket server on port 9999
  - Streams JSON-formatted repository data to Spark via TCP socket connection
  - Reads GitHub Personal Access Token (PAT) from environment variable `TOKEN`
  - Filters repositories to ensure only the three target languages are processed

#### 1.2.2 Apache Spark Cluster
The Spark cluster consists of two containers:

**Spark Master (`spark`)**
- Runs in master mode
- Exposes ports: 8080 (Web UI), 7077 (Spark Master), 6066 (REST API)
- Manages job scheduling and resource allocation
- Hosts the Spark Streaming application

**Spark Worker (`spark-worker`)**
- Runs in worker mode
- Connects to master at `spark://spark:7077`
- Allocated 1GB memory and 1 core
- Executes distributed processing tasks
- Exposes ports: 28081 (Worker UI), 24040-24041 (Spark UI)

#### 1.2.3 Spark Streaming Application (`spark_app.py`)
- **Batch Interval:** 60 seconds
- **Processing Model:** Micro-batch streaming
- **Data Source:** TCP socket stream from data-source service
- **Storage:** Uses checkpointing and MEMORY_AND_DISK storage level
- **Analytics:** Performs 8 distinct analysis tasks on streaming data

#### 1.2.4 Web Application Service (`webapp`)
- **Technology:** Flask (Python), Redis, Matplotlib
- **Port:** 5000
- **Purpose:** Receives analysis results and serves interactive dashboard
- **Features:**
  - RESTful API endpoints for data updates and retrieval
  - Real-time visualization generation using Matplotlib
  - Redis-based data caching for efficient data serving
  - HTML/JavaScript frontend with 60-second auto-refresh

#### 1.2.5 Redis Service (`redis`)
- **Purpose:** In-memory data store for caching analysis results
- **Port:** 6379
- **Usage:** Webapp stores and retrieves Spark analysis results

### 1.3 Data Flow Architecture

The system implements a unidirectional data flow pipeline:

```
GitHub API → Data Source Service → TCP Socket → Spark Streaming → HTTP POST → Webapp → Redis → Frontend Dashboard
```

1. **Data Ingestion:** Data source service queries GitHub API every 15 seconds
2. **Data Streaming:** Repository data is sent as JSON lines over TCP socket (port 9999)
3. **Stream Processing:** Spark receives data, buffers for 60-second batches, and processes
4. **Result Distribution:** Spark sends analysis results via HTTP POST to webapp
5. **Data Caching:** Webapp stores results in Redis
6. **Visualization:** Frontend polls webapp every 60 seconds for updated visualizations

### 1.4 Container Orchestration

Docker Compose manages the entire system with the following configuration:
- **Network:** Default bridge network enabling service discovery by container name
- **Volumes:** Shared volume mounting for code synchronization
- **Environment Variables:** Secure token management via environment variables
- **Port Mapping:** Strategic port exposure for external access and monitoring
- **Dependency Management:** Automatic service startup and health management

---

## 2. Service Interactions

### 2.1 Data Source to Spark Communication

**Protocol:** TCP Socket Stream  
**Connection Details:**
- Data source binds to `0.0.0.0:9999` and listens for Spark connection
- Spark connects to `data-source:9999` using `socketTextStream`
- Data format: Newline-delimited JSON (NDJSON)
- Transmission: Continuous streaming, one repository per JSON object

**Data Structure Transmitted:**
```json
{
  "id": <repository_id>,
  "name": <repository_name>,
  "language": "Python|Java|JavaScript",
  "description": <repository_description>,
  "stargazers_count": <star_count>,
  "pushed_at": "YYYY-MM-DDTHH:MM:SSZ"
}
```

**Interaction Flow:**
1. Data source establishes TCP server and waits for connection
2. Spark initiates connection during application startup
3. Data source enters infinite loop: query GitHub API → filter repositories → send JSON → sleep 15 seconds
4. For each language, up to 50 repositories are queried and streamed
5. Spark receives data as text stream and parses JSON

### 2.2 Spark to Webapp Communication

**Protocol:** HTTP REST API  
**Endpoint:** `http://webapp:5000/updateData`  
**Method:** POST  
**Content-Type:** application/json

**Data Structure Sent:**
```json
{
  "req1": [{"language": "Python", "count": 1234}, ...],  // Total repo counts
  "req2": [{"batch_time": "HH:MM:SS", "language": "Python", "count": 45}, ...],  // Recent pushes
  "req3": [{"language": "Python", "avg_stargazers_count": 123.45}, ...],  // Avg stars
  "req4": [{"language": "Python", "top_ten_words": [["word", count], ...]}, ...],  // Top words
  "req5": [...],  // Topic modeling
  "req6": [...],  // Named entity recognition
  "req7": [...],  // TF-IDF similarity
  "req8": [...]   // Time-series analysis
}
```

**Interaction Flow:**
1. After each 60-second batch processing, Spark calls `generateData()`
2. `generateData()` aggregates all analysis results into a single dictionary
3. `sendToClient()` function sends HTTP POST request to webapp
4. Webapp's `/updateData` endpoint receives data and stores in Redis
5. Redis key `'data'` is updated with JSON stringified results
6. Webapp responds with `{"msg": "success"}`

**Error Handling:**
- Spark catches exceptions during HTTP POST and logs errors
- Webapp gracefully handles malformed JSON
- Redis connection failures are logged but don't crash the service

### 2.3 Webapp to Frontend Communication

**Protocol:** HTTP REST API  
**Endpoints:**
- `GET /` - Serves HTML dashboard
- `GET /getData` - Returns current analysis results

**Interaction Flow:**
1. Frontend JavaScript runs `loadData()` every 60 seconds
2. AJAX GET request to `/getData` endpoint
3. Webapp retrieves data from Redis
4. Webapp processes data:
   - Generates line chart for requirement 3.2 (recent pushes)
   - Generates bar chart for requirement 3.3 (average stars)
   - Formats text data for requirements 3.1, 3.4, and advanced analytics
5. Returns JSON response with image paths and data
6. Frontend updates DOM elements with new data
7. Images are refreshed with timestamp query parameter to bypass cache

### 2.4 Inter-Service Dependencies

**Startup Sequence:**
1. Docker Compose starts all containers simultaneously
2. Redis initializes first (lightweight, no dependencies)
3. Data source waits for Spark connection (blocking `s.accept()`)
4. Spark worker connects to Spark master
5. Spark application must be manually started via `spark-submit`
6. Webapp starts Flask server and connects to Redis
7. Once Spark connects to data source, streaming begins

**Service Discovery:**
- Containers resolve each other by service name (e.g., `data-source`, `webapp`, `spark`)
- Docker's internal DNS enables name-based networking
- No hardcoded IP addresses required

**Failure Resilience:**
- Data source reconnects if Spark disconnects (waits for new connection)
- Spark checkpointing enables recovery from failures
- Redis persistence ensures data survives webapp restarts
- Frontend continues polling even if webapp temporarily unavailable

---

## 3. Spark Application Streaming Data Processing

### 3.1 Streaming Context Initialization

The Spark application initializes with the following configuration:

```python
BATCH_INTERVAL = 60  # 60-second micro-batches
ssc = StreamingContext(sc, BATCH_INTERVAL)
ssc.checkpoint("checkpoint_EECS4415_Porject_3")
data = ssc.socketTextStream(DATA_SOURCE_IP, DATA_SOURCE_PORT, 
                           storageLevel=StorageLevel.MEMORY_AND_DISK)
```

**Key Aspects:**
- **StreamingContext:** Created with 60-second batch interval
- **Checkpointing:** Enables fault tolerance and state recovery
- **Storage Level:** MEMORY_AND_DISK balances performance and reliability
- **Socket Stream:** Connects to data-source service for continuous input

### 3.2 Data Ingestion and Transformation

**Stream Processing Pipeline:**

1. **Text Stream Reception:**
   ```python
   data = ssc.socketTextStream("data-source", 9999, 
                               storageLevel=StorageLevel.MEMORY_AND_DISK)
   ```
   - Receives newline-delimited JSON strings over TCP socket
   - Each line represents one repository as a JSON object
   - Storage level ensures data persistence across partitions
   - Stream is continuous and unbounded

2. **JSON Parsing and Transformation:**
   ```python
   repos = data.flatMap(lambda json_str: [json.loads(json_str)])
   ```
   - `flatMap` transforms each JSON string into a Python dictionary
   - Parses repository fields: id, name, language, description, stargazers_count, pushed_at
   - Returns list with single dictionary (or empty list on parse failure)
   - Creates DStream of repository dictionaries

3. **RDD Processing Trigger:**
   ```python
   repos.foreachRDD(processRdd)
   ```
   - `foreachRDD` is an output operation that triggers `processRdd()` for each batch
   - Called every 60 seconds with a new RDD containing all data in that window
   - Each RDD may contain 0 to ~600 repositories (15 sec intervals × 3 languages × ~50 repos)
   - RDD is collected to Python list for processing (small enough for in-memory operations)

### 3.3 Batch Processing Workflow

The `processRdd(time, rdd)` function orchestrates each batch:

**Step 1: Repository Deduplication and Storage**
```python
repositories = getAllRepositories()  # Global state: all unique repos
batch_repositories = {}  # Current batch repos

for repo in rdd.collect():
    if repo['id'] not in batch_repositories:
        batch_repositories[repo['id']] = repo
    if repo['id'] not in repositories:
        repositories[repo['id']] = repo
```

**Key Logic:**
- Maintains global dictionary of all unique repositories by ID
- Each repository counted only once across entire stream lifetime
- Batch-specific dictionary tracks repositories in current 60-second window
- Prevents duplicate counting in cumulative analyses

**Step 2: State Management**
- `updateAllRepositories()`: Updates global repository cache
- `updateBatchRepositories()`: Appends batch with timestamp for time-series analysis
- Global state persists across batches using Python globals

**Step 3: Analysis Execution**
`generateData()` triggers all 8 analysis functions:

### 3.4 Core Analysis Tasks (Requirements 3.1-3.4)

#### 3.4.1 Requirement 3.1: Total Repository Count by Language
```python
def getTotalRepoCountByLanguage():
    repos_list = list(getAllRepositories().values())
    df = spark.createDataFrame(repos_list)
    counts_df = df.groupBy('language').count()
```

**Processing:**
- Converts global repository dictionary to Spark DataFrame
- Groups by `language` column and counts occurrences
- Each repository counted exactly once (enforced by ID-based deduplication)
- Returns list of `{language, count}` dictionaries

#### 3.4.2 Requirement 3.2: Recent Pushes (Last 60 Seconds)
```python
def getBatchedRepoLanguageCountsLast60Seconds():
    current_batch = getBatchRepositories()[-1]
    batch_time = current_batch['time']
    
    for repo in current_batch['repos'].values():
        pushed_at = datetime.strptime(repo['pushed_at'], '%Y-%m-%dT%H:%M:%SZ')
        delta = batch_time - pushed_at
        if delta.total_seconds() <= 60:
            current_batch_repos_last_60[repo['id']] = repo
```

**Processing:**
- Retrieves latest batch from `getBatchRepositories()[-1]` with UTC timestamp
- Filters repositories where `pushed_at` datetime is within 60 seconds of batch processing time
- Calculates time delta: `delta = batch_time - pushed_at` and checks `delta.total_seconds() <= 60`
- Deduplicates within batch using repository ID as key
- Converts filtered repos to RDD: `sc.parallelize(current_batch_repos_last_60.values())`
- Uses RDD operations: `map(lambda repo: (repo['language'], 1))` then `reduceByKey(lambda a, b: a+b)`
- Fills missing languages with count 0 to ensure all three languages appear in results
- Appends to global `BatchedRepoLanguageCounts` list for historical tracking
- Maintains complete history of batch counts for line chart time-series visualization

#### 3.4.3 Requirement 3.3: Average Stars by Language
```python
def getAvgStargazersByLanguage():
    df = spark.createDataFrame(repos_list)
    avg_df = df.groupBy('language').agg(round(avg("stargazers_count"), 2)
                                       .alias('avg_stargazers_count'))
```

**Processing:**
- Creates DataFrame from all unique repositories
- Groups by language and calculates mean of `stargazers_count`
- Rounds to 2 decimal places
- Each repository contributes to average only once

#### 3.4.4 Requirement 3.4: Top 10 Frequent Words
```python
def getTopTenFrequentWordsByLanguage():
    repos = sc.parallelize(repositories.values())
    words_by_language = repos.flatMap(
        lambda repo: ((repo['language'], word) 
                     for word in re.sub('[^a-zA-Z ]', '', str(repo['description']))
                                        .lower().split() 
                     if repo['description'] is not None)
    ).groupByKey()
    
    word_count_by_language = words_by_language.mapValues(
        lambda words: Counter(words)
    ).mapValues(
        lambda word_count: sorted(word_count.items(), 
                                 key=lambda x: x[1], reverse=True)[:10]
    )
```

**Processing:**
- Converts repository dictionary to RDD using `sc.parallelize()`
- Uses RDD operations for distributed text processing:
  - `flatMap`: Extracts words from descriptions, emits `(language, word)` pairs
  - Text preprocessing: `re.sub('[^a-zA-Z ]', '', ...)` removes non-alphabetic, `.lower().split()` tokenizes
  - Filters out None descriptions
  - `groupByKey`: Groups words by language, creating `(language, iterable_of_words)` pairs
  - `mapValues`: Applies Python Counter to count word frequencies per language
  - Second `mapValues`: Sorts by frequency (descending) and takes top 10
- `collect()` brings results to driver for formatting
- Each repository's description processed only once (enforced by global deduplication)

### 3.5 Advanced Analytics (Requirements 3.5, 4.1-4.3)

#### 3.5.1 Topic Modeling (LDA)
- **Technology:** Spark MLlib LDA (Latent Dirichlet Allocation)
- **Process:**
  - Tokenizes repository descriptions using `Tokenizer`
  - Removes stop words with `StopWordsRemover`
  - Creates term frequency vectors with `CountVectorizer` (vocabSize=1000, minDF=2)
  - Applies IDF transformation for TF-IDF weighting
  - Trains LDA model with k=5 topics, maxIter=10, optimizer="online"
  - Extracts top 5 words per topic with term weights
- **Output:** Discovered topics and associated keywords per language
- **Purpose:** Identify emerging technology trends and themes in repository descriptions

#### 3.5.2 Named Entity Recognition
- **Technology:** Pattern-based matching with predefined dictionaries
- **Entity Categories:**
  - Companies: Google, Microsoft, Amazon, Facebook, etc.
  - Frameworks: React, Django, Spring, TensorFlow, etc.
  - Tools: Docker, Kubernetes, Redis, PostgreSQL, etc.
  - Technologies: AI/ML, blockchain, IoT, microservices, etc.
- **Process:**
  - Scans repository descriptions for entity pattern matches (case-insensitive)
  - Counts occurrences of each entity per language
  - Ranks entities by frequency, returns top 5 per category
- **Output:** Technology ecosystem mapping showing popular tools/frameworks per language
- **Purpose:** Understand technology adoption and ecosystem composition

#### 3.5.3 TF-IDF Cosine Similarity
- **Technology:** Spark MLlib TF-IDF with manual cosine similarity calculation
- **Process:**
  - Applies same text preprocessing as topic modeling (tokenization, stop word removal)
  - Creates TF-IDF feature vectors using `CountVectorizer` and `IDF`
  - Collects vectors to driver and computes pairwise cosine similarity
  - Cosine similarity: `dot_product / (magnitude1 * magnitude2)`
  - Filters similarities > 0.1 threshold
  - Compares each repo with next 5 repos to limit computation
  - Returns top 5 most similar repository pairs per language
- **Output:** Similar repository pairs with similarity scores and descriptions
- **Purpose:** Identify related projects, enable recommendation systems, detect duplicates

#### 3.5.4 Time-Series Analysis
- **Technology:** Python datetime processing with Spark DataFrames
- **Process:**
  - Extracts `created_at` timestamps from repositories (or generates synthetic dates if missing)
  - Groups repositories by year-month buckets (YYYY-MM format)
  - Calculates monthly metrics: repository count, total stars, average stars
  - Computes growth rate: `((current - previous) / previous) * 100`
  - Determines trend direction: "growing" (>10% avg), "declining" (<-10% avg), "stable" (otherwise)
  - Identifies peak month with maximum repository count
  - Returns last 6 months of data for visualization
- **Output:** Monthly trends, growth rates, peak periods per language
- **Purpose:** Track technology evolution, identify growth patterns, forecast trends

### 3.6 State Management and Deduplication

**Global State Variables:**
- `repositories`: Dictionary of all unique repositories by ID
- `batch_repositories`: List of batch dictionaries with timestamps
- `spark_session`: Shared SparkSession for DataFrame operations
- `SparkContext`: Shared SparkContext for RDD operations
- `startTime`: Application start timestamp for uptime tracking

**Deduplication Strategy:**
- Repository ID used as unique key
- Check `if repo['id'] not in repositories` before adding
- Applied consistently across all cumulative analyses (req 3.1, 3.3, 3.4)
- Batch-specific deduplication for requirement 3.2

### 3.7 Output and Visualization

**Console Output:**
- Each batch prints requirement-specific sections
- Shows DataFrame results using `.show()`
- Displays uptime, RDD count, and batch timestamp
- Human-readable format for TA verification

**Webapp Transmission:**
- `sendToClient()` called after each batch
- HTTP POST with complete analysis results
- Webapp generates static images for charts
- Frontend receives JSON with image paths and data

### 3.8 Fault Tolerance

**Checkpointing:**
- StreamingContext checkpoint directory: `checkpoint_EECS4415_Porject_3`
- Enables recovery from driver failures
- Preserves DStream lineage and state

**Error Handling:**
- Try-except blocks in `processRdd()` catch processing errors
- Continues streaming even if individual batch fails
- Logs traceback for debugging
- Graceful degradation if analysis functions fail

**Storage Level:**
- `MEMORY_AND_DISK` ensures data persistence
- Spills to disk if memory insufficient
- Balances performance and reliability

---

## 4. System Deployment and Execution

### 4.1 Deployment Steps

1. **Clone Repository:**
   ```bash
   git clone https://github.com/KaizaZaika/BTL_bigdata.git
   cd BTL_bigdata/streaming
   ```

2. **Configure Environment:**
   - Set `TOKEN` environment variable in `docker-compose.yaml` with GitHub PAT
   - Replace `your_api_token_here` with actual token (or dummy value for submission)

3. **Start Services:**
   ```bash
   docker-compose up -d
   ```

4. **Launch Spark Application:**
   ```bash
   docker exec streaming-spark-1 /opt/spark/bin/spark-submit /streaming/spark_app.py
   ```

5. **Access Dashboard:**
   - Open browser to `http://localhost:5000`
   - Dashboard updates every 60 seconds

### 4.2 System Monitoring

- **Spark Master UI:** `http://localhost:8080`
- **Spark Worker UI:** `http://localhost:28081`
- **Spark Application UI:** `http://localhost:24040`
- **Web Dashboard:** `http://localhost:5000`

### 4.3 Data Flow Timing

- **Data Source Interval:** 15 seconds (queries GitHub API)
- **Spark Batch Interval:** 60 seconds (processes accumulated data)
- **Frontend Refresh:** 60 seconds (polls webapp for updates)
- **Result Latency:** ~60-75 seconds from GitHub push to visualization

---

## 5. Key Design Decisions and Trade-offs

### 5.1 State Management
- **Choice:** Python global dictionaries for state persistence
- **Rationale:** Simple, effective for single-driver streaming application
- **Trade-off:** Not distributed; state lost on driver failure (mitigated by checkpointing)
- **Alternative Considered:** External state store (Redis/DB) for true distributed state

### 5.2 Deduplication Strategy
- **Choice:** Repository ID-based deduplication in Python dictionaries
- **Rationale:** O(1) lookup time, simple implementation
- **Trade-off:** In-memory state grows over time; suitable for long-running but not infinite streams
- **Enforcement:** Applied at ingestion point in `processRdd()` before any analysis

### 5.3 Batch vs. Record Processing
- **Choice:** Micro-batch processing with 60-second intervals
- **Rationale:** Balances latency (60s) with processing efficiency
- **Trade-off:** Not true real-time (sub-second), but enables complex analytics
- **Alternative:** Continuous processing with lower latency but higher complexity

### 5.4 DataFrame vs. RDD Usage
- **Choice:** Mixed approach - DataFrames for aggregations, RDDs for text processing
- **Rationale:** DataFrames provide SQL-like operations; RDDs offer fine-grained control
- **Trade-off:** Some code complexity from mixing paradigms
- **Benefits:** Leverages best of both Spark APIs

## 6. Conclusion

This system successfully implements a distributed, real-time streaming analytics pipeline for GitHub repository data. The architecture demonstrates:

1. **Scalability:** Containerized microservices enable horizontal scaling of Spark workers
2. **Reliability:** Checkpointing and state management ensure fault tolerance and recovery
3. **Real-time Processing:** 60-second batch intervals provide near-real-time insights with manageable latency
4. **Comprehensive Analytics:** Eight distinct analysis tasks provide multi-dimensional insights from basic counts to advanced ML-based topic modeling
5. **User Experience:** Interactive dashboard with automatic 60-second updates and rich visualizations
6. **Maintainability:** Clean separation of concerns, modular design, and containerized deployment
7. **Data Integrity:** Robust deduplication ensures accurate cumulative metrics
8. **Distributed Processing:** Effective use of Spark's distributed computing capabilities for text analysis and aggregations

The system effectively processes streaming GitHub data, performs complex distributed analytics using Apache Spark's RDD and DataFrame APIs, and presents results through an intuitive web interface. The implementation demonstrates proficiency in big data streaming architectures, distributed systems design, microservices orchestration, and real-time data visualization.

---

## Appendix: Key Implementation Details

### Data Source Service Timing
- **Query Interval:** 15 seconds per cycle
- **Languages Processed:** Python, Java, JavaScript (sequentially)
- **Repositories per Query:** Up to 50 per language (GitHub API limit)
- **Total Potential Throughput:** ~150 repositories per 15-second cycle
- **Connection Model:** Blocking TCP server, waits for Spark connection before streaming

### Spark Batch Processing Characteristics
- **Batch Duration:** 60 seconds (4 data source cycles per batch)
- **Expected Batch Size:** 0-600 repositories (varies with GitHub activity)
- **Processing Approach:** Collect RDD to driver for Python-based state management
- **State Persistence:** In-memory Python dictionaries with checkpointing backup
- **Output Frequency:** Results generated and sent to webapp every 60 seconds

### Deduplication Guarantees
- **Requirement 3.1 (Total Count):** Each repository ID counted exactly once globally
- **Requirement 3.2 (Recent Pushes):** Each repository ID counted at most once per 60-second batch
- **Requirement 3.3 (Avg Stars):** Each repository contributes to average exactly once globally
- **Requirement 3.4 (Top Words):** Each repository's description processed exactly once globally
- **Implementation:** Repository ID-based dictionary lookups before processing

### Network Communication Summary
| From | To | Protocol | Port | Data Format | Frequency |
|------|-----|----------|------|-------------|-----------|
| GitHub API | Data Source | HTTPS | 443 | JSON | Every 15s |
| Data Source | Spark | TCP Socket | 9999 | NDJSON | Continuous |
| Spark | Webapp | HTTP POST | 5000 | JSON | Every 60s |
| Webapp | Redis | Redis Protocol | 6379 | JSON String | On update |
| Frontend | Webapp | HTTP GET | 5000 | JSON | Every 60s |

---

**Report Format:** This report is provided in Markdown format (REPORT.md) and can be easily converted to PDF using tools such as Pandoc, Markdown-to-PDF converters, or rendered directly in GitHub/GitLab viewers.

