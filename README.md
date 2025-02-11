# pumpfetch

**pumpfetch** is an open-source coin investment analysis bot designed to monitor, analyze, and categorize investment activities in real time. It provides insights into who is investing in a coin, whether investments occur in bundles, if there is any copycat behavior, and if specific strategies—such as sniper trades, developer transactions, or automated bot deployments—are at play.

pumpfetch comprises multiple specialized modules that work together to deliver a complete picture of coin investment dynamics.

---

## Table of Contents

- [Overview](#overview)
- [Modules and Functionality](#modules-and-functionality)
  - [WebSocket Listener](#websocket-listener)
  - [Dev Sold](#dev-sold)
  - [Dev Bought](#dev-bought)
  - [Bot Deployer](#bot-deployer)
  - [Copy Cat](#copy-cat)
  - [Sniper](#sniper)
  - [Sniper Bot](#sniper-bot)
  - [Sniper Wallets](#sniper-wallets)
  - [Bundle API](#bundle-api)
- [System Workflow](#system-workflow)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)
- [Legal Notice](#legal-notice)
- [Contact](#contact)

---

## Overview

pumpfetch is engineered to give you a complete, real-time overview of coin investment activities. By processing both live and historical transaction data, pumpfetch can determine:

- **Who** is investing in a coin.
- Whether investments occur as **bundles** (grouped transactions) or individually.
- The overall **investment amounts** and whether they meet specified thresholds.
- The presence of **copycat behavior**, where tokens mimic other tokens.
- Rapid trading activities (**sniping**), including analysis of both sniper trades and repeated sniper wallet behavior.
- **Developer activity** (buying or selling by the coin creator).
- **Automated behavior** by identifying wallets that deploy bots.
- Which wallets are repeatedly executing sniper transactions (**sniper wallets**).

Each module of pumpfetch is responsible for a specific aspect of this analysis, and together they provide a detailed picture of market activity.

---

## Modules and Functionality

### WebSocket Listener
- **File:** `websocket_listener.py`
- **Functionality:**  
  Establishes a persistent WebSocket connection to an external data source, subscribes to token updates (such as creation, buy, or sell events), and writes the data into a database for subsequent analysis by other modules.

---

### Dev Sold
- **File:** `dev_sold.py`
- **Functionality:**  
  Analyzes the database to detect transactions where the coin’s creator (developer) is selling tokens. It records these “Dev Sold” events in a separate table to help identify potential insider selling.

---

### Dev Bought
- **File:** `dev_bought.py`
- **Functionality:**  
  Detects transactions where the coin’s creator buys tokens. Tracking “Dev Bought” events provides insights into whether the developer is supporting the token value or potentially engaging in market manipulation.

---

### Bot Deployer
- **File:** `bot_deployer.py`
- **Functionality:**  
  Identifies wallets that deploy bots to automatically invest in the coin. This module aggregates transaction data and flags wallets that exhibit repeated automated behavior, storing these records in a dedicated table.

---

### Copy Cat
- **File:** `copy_cat.py`
- **Functionality:**  
  Scans for tokens that imitate others by comparing key attributes such as name or symbol. If a token appears to be a copycat, it is flagged and logged into a separate table for further review.

---

### Sniper
- **File:** `sniper.py`
- **Functionality:**  
  Monitors rapid trading activities by defining a short (e.g., 5-second) window for each token. It aggregates buy and sell transactions within this window, calculates total amounts for each, and determines the net result (profit, loss, or break-even).

---

### Sniper Bot
- **File:** `sniper_bot.py`
- **Functionality:**  
  Focuses on detecting wallets that consistently execute sniper trades. By processing new transaction records and grouping them by wallet, this module flags those with repeated sniper activity, suggesting potential automated sniper behavior.

---

### Sniper Wallets
- **File:** `sniper_wallets.py`
- **Functionality:**  
  Analyzes the database to identify wallets that repeatedly perform sniper transactions. This module groups transactions by wallet and records those with multiple entries, helping to highlight potentially suspicious trading behavior.

---

### Bundle API
- **File:** `bundle.py`
- **Functionality:**  
  Provides a Flask-based REST API endpoint for trade bundle analysis. It:
  - Retrieves all trades for a given token.
  - Filters out trades below a certain SOL threshold.
  - Groups trades by slot (a time or sequence identifier).
  - Calculates aggregated metrics (total buys, total sells, and net results).
  - Returns the analyzed data as a JSON response.

---

## System Workflow

1. **Data Ingestion:**  
   The WebSocket Listener continuously gathers live token updates from an external source and stores them in the database.

2. **Transaction Analysis:**  
   - **Developer Analysis:**  
     The Dev Sold and Dev Bought modules detect insider trading by monitoring the token creator’s transactions.
   - **Copycat Detection:**  
     The Copy Cat module compares token attributes to flag imitations.
   - **Sniper Analysis:**  
     The Sniper and Sniper Bot modules detect rapid, short-window trades, while the Sniper Wallets module identifies wallets with repeated sniper activity.
   - **Bot Deployment:**  
     The Bot Deployer module aggregates transaction data to identify wallets that deploy bots for automated investing.

3. **Aggregation and Reporting:**  
   The Bundle API aggregates trade data into bundles, computes overall metrics (e.g., total buy/sell amounts, net results), and makes these insights available through a RESTful interface.

---

## Requirements

- **Python 3.8+**
- **MySQL** database for storing transaction data
- Required Python libraries (see `requirements.txt`):
  - `mysql-connector-python`
  - `requests`
  - `flask`
  - Additional standard libraries: `decimal`, `asyncio`, etc.

---

## Installation

1. **Clone the Repository:**

   ```
   git clone https://github.com/yourusername/pumpfetch.git
   cd pumpfetch
   ```

2. **Set Up a Virtual Environment (Recommended):**

   ```
   python3 -m venv venv
   source venv/bin/activate  # For Windows: venv\Scripts\activate
   ```

3. **Install Dependencies:**

   ```
   pip install -r requirements.txt
   ```

4. **Configure Environment Variables:**

   Create a `.env` file or set the following environment variables:
   - `DB_HOST`
   - `DB_USER`
   - `DB_PASSWORD`
   - `DB_DATABASE`
   - `WEBSOCKET_URI` (if applicable)

---

## Usage

Each module in pumpfetch can be run independently or as part of the overall system. For example:

- **Start the WebSocket Listener:**

   ```
   python websocket_listener.py
   ```

- **Run Developer Transaction Modules:**

   ```
   python dev_sold.py
   python dev_bought.py
   ```

- **Run Copy Cat Detection:**

   ```
   python copy_cat.py
   ```

- **Run Sniper Analysis:**

   ```
   python sniper.py
   ```

- **Run Sniper Bot Detection:**

   ```
   python sniper_bot.py
   ```

- **Analyze Sniper Wallets:**

   ```
   python sniper_wallets.py
   ```

- **Start the Bundle API:**

   ```
   python bundle.py
   ```
   The API will be available at `http://0.0.0.0:5000/`.

- **Run Bot Deployer Analysis:**

   ```
   python bot_deployer.py
   ```

pumpfetch continuously processes incoming data and updates its internal state to provide real-time insights into coin investment activities.

---

## Contributing

pumpfetch is open to contributions from the community. We welcome code suggestions, bug fixes, and feature enhancements. Please submit your pull requests or open an issue on GitHub. All contributions will be reviewed, and contributors will be credited for their efforts.

---

## License

This project is licensed under the MIT License:

MIT License  
A short and simple permissive license with conditions only requiring preservation of copyright and license notices.
Licensed works, modifications, and larger works may be distributed under different terms and without source code.

---

## Legal Notice

**IMPORTANT:**  
While pumpfetch is open-source and available under the MIT License, the contents of this repository are proprietary. Unauthorized reuse, redistribution, or modification of this code beyond the terms of the MIT License is strictly prohibited.

---

## Contact

For updates and inquiries, please refer to the Twitter profile linked in the GitHub description.

---

pumpfetch is a modular solution for in-depth coin investment analysis. By integrating real-time data ingestion, comprehensive transaction analysis, and specialized modules (such as developer activity, copycat detection, sniper trading, and bot deployment), pumpfetch offers a powerful tool for understanding and acting on investment trends.
