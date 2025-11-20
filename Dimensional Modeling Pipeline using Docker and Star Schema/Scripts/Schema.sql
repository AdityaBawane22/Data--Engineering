CREATE TABLE Dim_Customer (
    customer_id INT NOT NULL,
    age INT,
    gender VARCHAR(10),
    location VARCHAR(50),
    subscription_status VARCHAR(10),
    frequency_of_purchases VARCHAR(50),
    PRIMARY KEY (customer_id)
);

CREATE TABLE Dim_Item (
    item_name VARCHAR(50) NOT NULL,
    category VARCHAR(50) NOT NULL,
    size VARCHAR(5),
    color VARCHAR(20),
    season VARCHAR(20),
    PRIMARY KEY (item_name, category)
);

CREATE TABLE Fact_Purchase (
    purchase_transaction_id INT NOT NULL,
    customer_id INT NOT NULL,
    item_name VARCHAR(50) NOT NULL,
    category VARCHAR(50) NOT NULL,
    purchase_amount_usd DECIMAL(10, 2), -- UPDATED DATA TYPE
    review_rating DECIMAL(3, 2),
    payment_method VARCHAR(50),
    shipping_type VARCHAR(50),
    discount_applied VARCHAR(5),
    promo_code_used VARCHAR(5),
    previous_purchases INT,
    preferred_payment_method VARCHAR(50),
    
    PRIMARY KEY (purchase_transaction_id),
    
    FOREIGN KEY (customer_id) REFERENCES Dim_Customer(customer_id),
    FOREIGN KEY (item_name, category) REFERENCES Dim_Item(item_name, category)
);