import databento as db

# Path to your downloaded .dbn file
file_path = "xnas-itch-20241224.mbo.dbn.zst"

# Load the file
store = db.DBNStore.from_file(file_path)

# Display metadata
print("Metadata:")
print(store.metadata)

# Display the first few records
print("\nSample Records:")
count = 0

# Iterate through records directly
for record in store:  # Iterate through the DBNStore object itself
    print(record)
    count += 1
    if count > 1:  # Limit to the first 5 records
        break
