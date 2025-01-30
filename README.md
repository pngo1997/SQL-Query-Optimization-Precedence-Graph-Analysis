# 🏗️ SQL Query Optimization & Precedence Graph Analysis  

## 📜 Overview  
This project explores **SQL query execution, indexing, and serializability in database transactions** using **SQLite** and **Python**. It includes **performance benchmarking**, **SQL vs. Python query execution time comparisons**, and **precedence graph analysis** for database scheduling.  

## 🎯 Problem Explanation  

### **Part 1: Query Execution and Performance Benchmarking**  
This section compares **SQL query execution** with equivalent **Python-based queries** and measures **runtime performance**.  

#### **1. Expanding the Database Schema**  
- Created a **new Geo table** in addition to the existing `Tweet` and `User` tables.  
- The `Geo` table includes:  
  - **Primary Key (`geo_id`)** – Assigned based on location uniqueness.  
  - **Type**  
  - **Longitude**  
  - **Latitude**  
- **Linked `Geo` table to the `Tweet` table** via **foreign key**.  

#### **2. Query Execution & Benchmarking**  
- **SQL Query Execution & Timing:**  
  a. Find tweets where **tweet ID (`id_str`) contains "89" or "78"** anywhere in the column.  
  b. Find the number of **unique values in the `friends_count` column**.  

- **Equivalent Python-Based Query Execution & Timing:**  
  - Queries executed **without using SQL** by reading from the **CSV file**.  
  - Execution time compared against **SQL query performance**.  

#### **3. Visualization (Scatter Plot)**  
- Plotted **tweet lengths (first 60 tweets) vs. username lengths**.  
- Created a **scatterplot** to analyze correlation.  

### **Part 2: Query Optimization Using Indexes & Materialized Views**  
This section improves query performance using **indexes and materialized views** in **SQLite**.  

- **Indexes Created:**  
  a. **Index on `userid`** in the `Tweet` table.  
  b. **Composite index on (`friends_count`, `screen_name`)** in the `User` table.  
- **Materialized View for Query Optimization:**  
  - Since SQLite lacks **materialized view support**, created an **optimized table (`CREATE TABLE AS`)** to store precomputed query results for **faster retrieval**.  

### **Part 3: Precedence Graph & Serializability Analysis**  
This section evaluates **database transaction schedules** for **conflict serializability** using **precedence graphs**.  

- **Precedence Graph for Schedule 1:**  
  - The schedule is **serializable**.  
  - **Equivalent Serial Schedule:** `<T3, T1, T2>`.  

- **Precedence Graph for Schedule 2:**  
  - The schedule is **NOT serializable** due to a **conflict schedule**.  

## 🚀 Technologies Used  
- **SQLite** (for database management & indexing).  
- **Python (Pandas, Matplotlib)** (for data querying, visualization & performance benchmarking).  
- **SQL Query Optimization** (Indexes & Materialized Views).  
