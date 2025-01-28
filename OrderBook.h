#ifndef ORDERBOOK_H
#define ORDERBOOK_H

#include <iostream>
#include <map>
#include <vector>
#include <cstdint>
#include <string>

struct Order {
    uint64_t order_id;
    int64_t price; // Stored in nanodollars for precision
    uint32_t size;// Asset size being traded
    char side; // 'A' for Ask, 'B' for Bid
    std::chrono::steady_clock::time_point timestamp; // Time of order placement

};

struct Trade {
    uint64_t order_id;
    int64_t execution_price;
    uint32_t execution_size;
};

// Externally declared vector to store all executed trades
extern std::vector<Trade> trades;


// The OrderBook class manages buy and sell orders for an asset
class OrderBook {
public:
    void addOrder(uint64_t order_id, int64_t price, uint32_t size, char side);
    void modifyOrder(uint64_t order_id, uint32_t new_size);
    void cancelOrder(uint64_t order_id);
    void matchOrder(uint64_t order_id, uint32_t size);
    void clearBook();
    void placeLimitOrder(uint64_t order_id, int64_t price, uint32_t size, char side);
    void displayOrderBook() const;
    void dynamicOrderPlacement(char side, uint32_t size);

    // Metrics
    uint32_t getFilledOrders() const { return filled_orders; }
    uint32_t getUnfilledOrders() const { return unfilled_orders; }

private:
    std::map<uint64_t, Order> orders; // All active orderes keyed by order ID
    std::map<int64_t, std::vector<uint64_t>> asks; // Ask orders Price -> Order IDs (ascending)
    std::map<int64_t, std::vector<uint64_t>, std::greater<>> bids; // Bid orders Price -> Order IDs (descending)

    uint32_t filled_orders = 0;
    uint32_t unfilled_orders = 0;

};

#endif // ORDERBOOK_H
