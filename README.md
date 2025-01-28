# Order Book Simulation and Backtesting Framework

---
#### This branch provides a framework for backtesting trading strategies using MBO (Market-by-Order) data. The system simulates real-time order book activity by processing historical market data, enabling the analysis and validation of trading strategies in a controlled environment. It integrates with the Databento API and supports operations like adding, modifying, canceling, and matching orders. It also calculates aggregate trading metrics and visualizes the state of the order book.

## Features

### 1. Order Book Management

#### Core Operations:
- Add, modify, and cancel orders.
- Match and execute trades dynamically based on the best available price.

#### Trade Metrics:
- Tracks filled and unfilled orders.
- Calculates total trade volume and average execution price.

#### Dynamic Order Placement:
- Automatically places market-reactive orders to match the best bid or ask.

### 2. Backtesting Framework
- Processes historical market data in the Databento DBN format.
- Simulates order book actions based on historical data:
  - Adds new orders.
  - Modifies existing orders.
  - Cancels or clears the book when required.
  - Matches orders to execute trades dynamically.
- Provides detailed trade metrics and a final snapshot of the order book after processing.

### 3. Debugging Support
- Debug mode (`#define DEBUG`) allows detailed logging of all order book actions.
- Enables testing of specific scenarios with hardcoded sample data.

---

## How It Works

### Initialization:
- Load a historical MBO data file (`.dbn.zst`) using the Databento SDK.
- Set a record limit for processing to focus on specific time ranges or data samples.

### Replay and Simulate:
- Replay historical market data:
  - Orders are added, modified, canceled, or matched based on the data.
  - The order book is updated dynamically to reflect these actions.

### Order Matching and Execution:
- Matches orders dynamically based on the best available prices.
- Records executed trades and updates order sizes for partial fills.

### Metrics Calculation:
- Calculates the total traded volume and the weighted average price for executed trades.

### Final Analysis:
- Outputs the final state of the order book, providing insights into the order flow and strategy performance.

---

## How to Run

### The framework requires **C++17** or later.

#### Install Dependencies:  
On macOS, install OpenSSL and Zstandard with:  
`brew install openssl@3 zstd`  

On Windows, use vcpkg to install the dependencies with:  
`vcpkg install openssl:x64-windows zstd:x64-windows`  

On Ubuntu, run:  
`sudo apt update`  
`sudo apt install libssl-dev libzstd-dev`  

#### Build and Run:  
Create a build directory, configure the project, and compile it:  
`mkdir build`  
`cd build`  
`cmake ..`  
`make`  

Return to the project root and execute the program with your Databento API key:  
`cd ..`  
`./build/MBO "api_key"`

---

## Future Updates
Instead of using a .dbn file, implement a live data pipeline from Databento

Add support for additional order types, such as stop-loss and market orders

Include visualization for order book depth and trade execution

Combine MBO data with market-by-price (MBP) for broader analysis
