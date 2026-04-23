
# Global Retail ETL Pipeline with Real-Time Currency Normalization

## 🚀 Overview
This project implements a modular ETL (Extract, Transform, Load) pipeline designed to process multi-source retail data. It integrates real-time exchange rates using a REST API and normalizes all financial data into a single currency (INR) for consistent analysis.

The pipeline focuses on data reliability, scalability, and real-world engineering practices such as validation, logging, and fault tolerance.

---

## 🛠 Tech Stack
- **Languages:** Python, SQL  
- **Libraries:** pandas, numpy, requests, matplotlib  
- **Database:** SQLite  
- **API:** ExchangeRate API  

---

## ⚙️ Key Features
- Multi-source data ingestion (CSV files + API integration)  
- Real-time currency normalization using external API  
- Data cleaning, validation, and transformation  
- Fault-tolerant API handling with fallback mechanism  
- Logging for debugging and monitoring pipeline execution  
- SQL-based analytics and insights generation  

---

## 🏗 Architecture
The pipeline follows a modular architecture:

- `extract.py` → Data ingestion (CSV + API)  
- `transform.py` → Data cleaning, validation, merging, normalization  
- `load.py` → Store data in SQLite and export CSV  
- `main.py` → Orchestrates full pipeline  
- `config.py` → Centralized configuration  
- `plot_insights.py` → Generates visual analytics  

---

## 📊 Outputs
- Cleaned dataset exported as CSV  
- SQLite database (`retail.db`)  
- Revenue trend visualization  
- Product category distribution chart  

---

## ▶️ How to Run

### 1. Install dependencies
```bash
pip install -r requirements.txt
