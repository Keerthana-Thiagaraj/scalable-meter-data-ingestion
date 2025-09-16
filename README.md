# scalable-meter-data-ingestion

## Production-Grade NEM12 Data Ingestion Pipeline

This repository contains my solution for the Flo Energy Tech Assessment 2025. The project implements a robust, production-grade pipeline for parsing NEM12 files and ingesting interval meter data into a PostgreSQL database. The solution is designed for large-scale data, with a focus on validation, error handling, and best engineering practices.

---

## Problem Statement

**Challenge:**
- Parse NEM12 files (hierarchical 100, 200, 300, 500, 900 records).
- Map and validate data for insertion into a normalized PostgreSQL `meter_readings` table.
- Efficiently handle very large files (streaming, batching).
- Ensure robust error handling and auditability.

**Sample Input (NEM12):**
```
100,HEADER
200,NMI123,3
300,2025-09-12,1.1,2.2,3.3
900,FOOTER
```

**Sample Output (SQL Insert):**
```sql
INSERT INTO meter_readings (nmi, timestamp, consumption)
VALUES
  ('NMI123', '2025-09-12', 1.1),
  ('NMI123', '2025-09-12', 2.2),
  ('NMI123', '2025-09-12', 3.3);
```

---

## Solution Overview

- **Streaming Parser:** Reads NEM12 files line-by-line to minimize memory usage and support very large files.
- **Validation:** Validates file structure, record types, and interval counts before any database operation.
- **Batch Inserts:** Uses prepared statements and batch execution for efficient, atomic database writes.
- **Database Schema:**
  - PostgreSQL table: `meter_readings`
  - Columns: `nmi`, `timestamp`, `consumption`
  - Constraints: primary key, foreign keys, not null, unique constraints
  - Partitioning: (recommended for production) by date or NMI for scalability
- **Idempotency & Security:**
  - Duplicate detection and safe re-runs
  - Parameterized queries to prevent SQL injection
  - Audit logging for traceability

---

## Key Features & Best Practices

- **File Validation:** Ensures only well-formed, valid data is inserted.
- **Batch Processing:** Efficiently inserts large volumes of data using JDBC batch operations.
- **Error Handling:**
  - Malformed rows are logged to a CSV error log with context.
  - Processing continues for valid rows.
- **Security:**
  - All SQL uses parameterized queries.
  - Audit logs for all data loads.
- **Scalability:**
  - Handles files of arbitrary size via streaming.
  - Designed for easy partitioning and horizontal scaling.
- **No Infra/Deployment Code:**
  - Focuses on core ingestion logic as per assessment requirements.

---

## Setup Instructions

1. **Clone the Repository**
   ```sh
   git clone https://github.com/yourusername/scalable-meter-data-ingestion.git
   cd scalable-meter-data-ingestion
   ```

2. **Dependencies**
   - Java 11+ (tested with OpenJDK)
   - Maven (for build and dependency management)
   - PostgreSQL (for data storage)
   - JDBC Driver for PostgreSQL

3. **Build the Project**
   ```sh
   mvn clean package
   ```

4. **Run the Parser**
   ```sh
   java -cp target/FloTest-1.0-SNAPSHOT.jar org.parser.Main
   ```
   - By default, parses `test_nem12.csv` in the project root.
   - Update `Main.java` to point to your input file as needed.

---

## Example Run

**Sample NEM12 File:**
```
100,HEADER
200,NMI123,2
300,2025-09-12,1.1,2.2
900,FOOTER
```

**Generated SQL:**
```sql
INSERT INTO meter_readings (nmi, timestamp, consumption)
VALUES
  ('NMI123', '2025-09-12', 1.1),
  ('NMI123', '2025-09-12', 2.2);
```

---

## Testing

- **Unit Tests:**
  - Validate parser logic, error handling, and SQL generation.
  - Located in `src/test/java/org/parser/`.
- **Integration Tests:**
  - Use sample files (`test_nem12.csv`, `integration_nem12.csv`) to verify end-to-end ingestion.
  - Run with Maven:
    ```sh
    mvn test
    ```

---

## Q&A

**Q1: Why these technologies?**
- Java is robust for high-performance, production-grade data pipelines.
- PostgreSQL is reliable, supports advanced features (partitioning, constraints).
- Maven ensures reproducible builds and dependency management.

**Q2: What would you do with more time?**
- Implement advanced partitioning and indexing for even larger datasets.
- Add monitoring, alerting, and automated archival.
- Build a REST API for ingestion and status reporting.
- Integrate with CI/CD for automated testing and deployment.

**Q3: Why these design choices?**
- Streaming and batching ensure scalability and efficiency.
- Strict validation and error logging guarantee data quality and auditability.
- Security is enforced via parameterized queries and audit trails.

---

## Future Improvements

- **Archival Policies:** Automate archival of old meter readings to cold storage.
- **Caching:** Add caching for read-heavy analytics queries.
- **CI/CD Integration:** Set up pipelines for automated testing and deployment (out of scope for this assessment).

---

## Design Patterns Used

### Singleton Pattern
- **Code Demonstration:**  
  The `Config` class uses static members and a static initializer to load configuration from `config.properties` once, providing global access via static fields (e.g., `Config.DB_URL`).


### Factory Pattern
- **Code Demonstration:**  
  The parsing logic in `NEM12Parser` uses factory methods to instantiate domain objects (`MeterReading`) based on the type of NEM12 record.


### Strategy Pattern
- **Code Demonstration:**  
  Error handling and logging are encapsulated in the `ErrorLogger` class, allowing flexible error management strategies.


### Template Method Pattern
- **Code Demonstration:**  
  The main workflow in `Main.java` follows a fixed sequence: read file, validate, transform, batch insert, log errors.

### Data Access Object (DAO) Pattern
- **Code Demonstration:**  
  The `MeterReadingSqlGenerator` class encapsulates all SQL generation and database interaction logic.


### Layered Architecture / Separation of Concerns
- **Code Demonstration:**  
  Classes are organized by responsibility: parsing (`NEM12Parser`), domain modeling (`MeterReading`), configuration (`Config`), error handling (`ErrorLogger`), and database access (`MeterReadingSqlGenerator`).

---
