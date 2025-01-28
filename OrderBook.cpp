#include "OrderBook.h"
#include <iostream>
#include <algorithm>
#include <iomanip>


// Define the global trades vector
std::vector<Trade> trades;

// Uncomment the following line to enable debug logging
// #define DEBUG


void OrderBook::addOrder(uint64_t order_id, int64_t price, uint32_t size, char side) {
    // Adds a new order to the order book. 
    // Determines if the order is a bid ('B') or ask ('A') and places it in the appropriate price level.

    #ifdef DEBUG
        std::cout << "[Add] Order ID: " << order_id
                << ", Price: " << price
                << ", Size: " << size
                << ", Side: " << side << "\n"; // Debug: Log order addition details
    #endif

        // Invalid side provided; display an error message and return.
    if (side != 'A' && side != 'B') { 
        std::cerr << "[Error] Invalid side for Order ID: " << order_id << "\n";
        return;
    }
    
    #ifdef DEBUG
    std::cout << "[Debug] OrderBook instance address (addOrder): " << this << "\n"; //orderbook address
    #endif

    // Create the new order and add it to the orders map
    Order order = {order_id, price, size, side, std::chrono::steady_clock::now()};
    orders[order_id] = order;

    // Add the order to the respective price level map (asks or bids)
    if (side == 'A') {
        asks[price].push_back(order_id);
        unfilled_orders++;
    } else if (side == 'B') {
        bids[price].push_back(order_id);
        unfilled_orders++;
    } else {
        std::cerr << "[Error] Invalid side for Order ID: " << order_id << "\n";
    }

}

void OrderBook::modifyOrder(uint64_t order_id, uint32_t new_size) {
        // Modifies the size of an existing order identified by its order ID.
    // This is useful for adjusting the size of unfilled orders without canceling them.
    if (orders.find(order_id) != orders.end()) {

        #ifdef DEBUG
        std::cout << "[Modify] Order ID: " << order_id
                  << ", New Size: " << new_size << "\n"; // logging
        #endif

        orders[order_id].size = new_size;
    } else {
        std::cerr << "[Error] Attempt to modify non-existent Order ID: " << order_id << "\n";
    }

}


void OrderBook::cancelOrder(uint64_t order_id) {
        // Cancels an existing order identified by its order ID.
    // Removes the order from the orders map and the corresponding price level (asks or bids).

    if (orders.find(order_id) != orders.end()) {
        auto& order = orders[order_id];

        #ifdef DEBUG
        std::cout << "[Cancel] Order ID: " << order_id
            << ", Price: " << order.price
            << ", Side: " << order.side << "\n";//logging
        #endif

        // Remove the order from the appropriate price level map
        if (order.side == 'A') {
            auto& levels = asks[order.price];
            levels.erase(std::remove(levels.begin(), levels.end(), order_id), levels.end());
            if (levels.empty()) {
                asks.erase(order.price);

            }
        } else if (order.side == 'B') {
            auto& levels = bids[order.price];
            levels.erase(std::remove(levels.begin(), levels.end(), order_id), levels.end());
            if (levels.empty()) {
                bids.erase(order.price);

            }
        }
        // Remove the order from the orders map and update the unfilled orders count
        orders.erase(order_id);
        unfilled_orders--; 
    }else {
        std::cerr << "[Error] Attempt to cancel non-existent Order ID: " << order_id << "\n";
    }

}

void OrderBook::matchOrder(uint64_t order_id, uint32_t size) {
        // Matches an existing order with an incoming order of a specified size.
    // Executes trades and updates the size of partially filled orders if necessary.

    if (orders.find(order_id) != orders.end()) {
        auto& order = orders[order_id];

        #ifdef DEBUG
        std::cout << "[Match] Order ID: " << order_id
                  << ", Size to Match: " << size << "\n";
        #endif

        uint32_t matched_size = std::min(order.size, size);
        trades.push_back({order_id, order.price, matched_size});

            // Fully match the order and remove it
        if (order.size <= size) {
            cancelOrder(order_id);
            filled_orders++;

            // Partially match the order and update its size
        } else { 
            order.size -= size;
            #ifdef DEBUG
            std::cout << "[Match] Remaining Size for Order ID: " << order_id
                      << " is " << order.size << "\n";
            #endif
        }
    } else {
        std::cerr << "[Error] Attempt to match non-existent Order ID: " << order_id << "\n";
    }

}

void OrderBook::clearBook() {
        // Clears all orders from the order book.
    // Resets the ask, bid, and order maps, as well as the unfilled order count.
    std::cout << "[Clear] Clearing the order book.\n";
    asks.clear();
    bids.clear();
    orders.clear();
    unfilled_orders = 0;

}

void OrderBook::placeLimitOrder(uint64_t order_id, int64_t price, uint32_t size, char side) {
       // Places a limit order in the order book.
    // If a matching order exists in the opposing side of the book, it is matched immediately.
    // Otherwise, the order is added to the book.

    #ifdef DEBUG
    std::cout << "[PlaceLimitOrder] Placing limit order - ID: " << order_id
              << ", Price: " << price
              << ", Size: " << size
              << ", Side: " << side << "\n";//logging
    #endif

    if (side == 'A') { // Ask
        auto it = bids.begin();
        if (it != bids.end() && it->first >= price) {
            matchOrder(it->second.front(), size); // Match with best bid
        } else {
            addOrder(order_id, price, size, side); // Add to the book
        }
    } else if (side == 'B') { // Bid
        auto it = asks.begin();
        if (it != asks.end() && it->first <= price) {
            matchOrder(it->second.front(), size); // Match with best ask
        } else {
            addOrder(order_id, price, size, side); // Add to the book
        }
    }
}

void OrderBook::dynamicOrderPlacement(char side, uint32_t size) {
        // Dynamically places an order on the best available price level on the opposing side of the book.
        // meant to mimic market order 

    if (side == 'B' && !asks.empty()) {
        auto best_ask = asks.begin();
        placeLimitOrder(orders.size() + 1, best_ask->first, size, side); // Buy at best ask
    } else if (side == 'A' && !bids.empty()) {
        auto best_bid = bids.begin();
        placeLimitOrder(orders.size() + 1, best_bid->first, size, side); // Sell at best bid
    }
}

void OrderBook::displayOrderBook() const {
    // Displays the current state of the order book.
    // Lists all orders, as well as the aggregated price levels for asks and bids.

        #ifdef DEBUG
        // std::cout << "[Debug] OrderBook instance address (displayOrderBook): " << this << "\n";
        #endif

    std::cout << "Orders: (Total: " << orders.size() << ")\n";
    if (orders.empty()) {
        std::cout << "No orders in the order book.\n";
    } else {
        for (const auto& [order_id, order] : orders) {
            double price_in_dollars = static_cast<double>(order.price) / 1e9;
            std::cout << "Order ID: " << order_id
                      << ", Price: $" << std::fixed << std::setprecision(2) << price_in_dollars
                      << ", Size: " << order.size
                      << ", Side: " << order.side << "\n";
        }
    }

    std::cout << "Asks: (Total Price Levels: " << asks.size() << ")\n";
    if (asks.empty()) {
        std::cout << "No ask levels in the order book.\n";
    } else {
        for (const auto& [price, order_ids] : asks) {
            double price_in_dollars = static_cast<double>(price) / 1e9;
            std::cout << "Price: $" << std::fixed << std::setprecision(2) << price_in_dollars
                      << " | Orders: ";
            for (const auto& id : order_ids) {
                std::cout << id << " ";
            }
            std::cout << "\n";
        }
    }

    std::cout << "Bids: (Total Price Levels: " << bids.size() << ")\n";
    if (bids.empty()) {
        std::cout << "No bid levels in the order book.\n";
    } else {
        for (const auto& [price, order_ids] : bids) {
            double price_in_dollars = static_cast<double>(price) / 1e9;
            std::cout << "Price: $" << std::fixed << std::setprecision(2) << price_in_dollars
                      << " | Orders: ";
            for (const auto& id : order_ids) {
                std::cout << id << " ";
            }
            std::cout << "\n";
        }
    }
}
