#include <databento/dbn.hpp>
#include <databento/historical.hpp>
#include <databento/enums.hpp> // For ToString
#include <iostream>
#include <string>
#include "OrderBook.h"

// Uncomment the following line to enable debug logging
// #define DEBUG

using namespace databento;

const int record_limit = 100; // Maximum number of records to process in this backtest
int processed_count = 0; // Tracks the number of processed records

std::string file_path = "./xnas-itch-20241224.mbo.dbn.zst";

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <API_KEY>" << std::endl;
        return 1;
    }

    std::string api_key = argv[1];

    // Initialize an order book instance for managing market data and trades
    OrderBook orderBook;

    #ifdef DEBUG
    Debugging: Display the memory address of the order book instance
    std::cout << "[Debug] OrderBook instance address (main): " << &orderBook << "\n";
    #endif

    try {
        // Open the historical market data file
        DbnFileStore dbn(file_path);

        // Callback to handle metadata, providing general information about the dataset
        auto metadata_callback = [](const Metadata& metadata) {
            std::cout << "Metadata: " << metadata << std::endl;
        };

        int display_count = 0; // Limits the number of unknown action warnings displayed

        // Callback to process individual records from the data file
        auto record_callback = [&orderBook, &display_count](const Record& record) {
            // Stop processing if the record limit has been reached
            if (processed_count >= record_limit) {
                return KeepGoing::Stop;
            }

            // Extract the order book message from the record
            const auto& mbo_msg = record.Get<MboMsg>();
            uint64_t order_id = mbo_msg.order_id;
            int64_t price = mbo_msg.price;  // Price in nanodollars
            uint32_t size = mbo_msg.size;
            char action = mbo_msg.action; // Action type (e.g., Add, Modify, Cancel)
            std::string side_str = databento::ToString(mbo_msg.side); // Convert enum to string
            char side = (side_str == "Ask") ? 'A' : (side_str == "Bid" ? 'B' : '\0'); // Determine the side (Ask or Bid)

            // Process the action based on its type
            switch (action) {
                case 'A': // Add a new order
                    #ifdef DEBUG
                        std::cout << "[Add] Order ID: " << order_id << ", Price: " << price
                                  << ", Size: " << size << ", Side: " << side << "\n";
                    #endif
                    orderBook.placeLimitOrder(order_id, price, size, side);
                    break;

                case 'M': // Modify an existing order
                    #ifdef DEBUG
                        std::cout << "[Modify] Order ID: " << order_id << ", New Size: " << size << "\n";
                    #endif
                    orderBook.modifyOrder(order_id, size);
                    break;

                case 'C': // Cancel an order
                    #ifdef DEBUG
                        std::cout << "[Cancel] Order ID: " << order_id << "\n";
                    #endif
                    orderBook.cancelOrder(order_id);
                    break;

                case 'R': // Clear the order book
                    #ifdef DEBUG
                        std::cout << "[Clear Book]\n";
                    #endif
                    orderBook.clearBook();
                    break;

                case 'F': // Fill an order
                    orderBook.matchOrder(order_id, size);
                    break;

                case 'T': // Simulate a trade
                    if (side == 'B') {
                        // Reactively place an Ask order to match the best Bid
                        orderBook.dynamicOrderPlacement('A', size);
                    } else if (side == 'A') {
                        // Reactively place a Bid order to match the best Ask
                        orderBook.dynamicOrderPlacement('B', size);
                    }
                    break;

                default: // Handle unknown actions
                    if (display_count < 10) {
                        std::cerr << "Unknown action: " << action << "\n";
                        display_count++;
                    }
            }

            // Increment the processed record count
            processed_count++;

            return KeepGoing::Continue; // Continue processing records
        };

        // Replay the file: process metadata and records using the specified callbacks
        dbn.Replay(metadata_callback, record_callback);

        // Calculate aggregate metrics from executed trades
        double total_volume = 0;
        double total_value = 0;
        for (const auto& trade : trades) {
            total_volume += trade.execution_size;
            total_value += trade.execution_price * trade.execution_size;
        }
        double average_price = (total_value / total_volume);

        std::cout << "Total Volume: " << total_volume
                  << ", Average Price: " << average_price / 1e9 << "\n";

        // Display the final state of the order book
        std::cout << "Final Order Book\n";
        orderBook.displayOrderBook();

        std::cout << "Filled Orders: " << orderBook.getFilledOrders() << "\n";
        std::cout << "Unfilled Orders: " << orderBook.getUnfilledOrders() << "\n";

    } catch (const std::exception& ex) {
        // Catch and display any errors encountered during processing
        std::cerr << "Error: " << ex.what() << std::endl;
        return 1;
    }

    #ifdef DEBUG
    // Test and display various operations on the order book when in debug mode

    // Add sample Ask orders
    orderBook.addOrder(1, 100, 10, 'A');
    orderBook.addOrder(2, 101, 20, 'A');
    orderBook.addOrder(3, 102, 15, 'A');

    // Add sample Bid orders
    orderBook.addOrder(4, 99, 25, 'B');
    orderBook.addOrder(5, 98, 30, 'B');
    orderBook.addOrder(6, 97, 35, 'B');

    // Display the initial order book
    std::cout << "=== Initial Order Book ===\n";
    orderBook.displayOrderBook();

    // Modify an order
    std::cout << "\n=== Modify Order ===\n";
    orderBook.modifyOrder(3, 25);
    orderBook.displayOrderBook();

    // Cancel an order
    std::cout << "\n=== Cancel Order ===\n";
    orderBook.cancelOrder(5);
    orderBook.displayOrderBook();

    // Match an order
    std::cout << "\n=== Match Order ===\n";
    orderBook.matchOrder(1, 10);
    orderBook.displayOrderBook();
    #endif

    return 0;
}
