///

// Switch to your database (replace 'crypto' if you used a different name)
use('crypto');

// View all collections in the database
show(collections)

// View documents in your btc-price-zscore collection (replace with your actual collection name)
db.btc_price_zscore.find().pretty()

// View first 5 documents with nice formatting
db.btc_price_zscore.find().limit(5).pretty()

// Count total documents
db.btc_price_zscore.countDocuments()