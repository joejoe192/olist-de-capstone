--1. Customers per state (top 5)
--1.How many customers are there in each state? Show the top 5 states.

SELECT customer_state, COUNT(*) AS customers
FROM customers
GROUP BY customer_state
ORDER BY customers DESC
LIMIT 5;
--2.Distinct real customers
--2.How many distinct real customers (customer_unique_id) are there?

SELECT COUNT(DISTINCT customer_unique_id) AS real_customers
FROM customers;
--3. Most-used payment type
--3.Which payment_type is used most, and what is its total payment_value?

SELECT payment_type, COUNT(*) AS uses
FROM order_payments op 
GROUP BY payment_type
ORDER BY uses DESC;

SELECT payment_type,
       COUNT(*)                            AS uses,
       ROUND(SUM(payment_value)::numeric, 2) AS total_payment_value
FROM order_payments
GROUP BY payment_type
ORDER BY uses DESC
LIMIT 1;
--4. Reviews by score and overall average
--How many reviews are there for each review_score (1 to 5)? What is the overall average score?

SELECT review_score, COUNT(*) AS reviews
FROM order_reviews
GROUP BY review_score
ORDER BY review_score;

SELECT ROUND(AVG(review_score)::numeric, 2) AS avg_score
FROM order_reviews t ;
--5. Seller states with more than 50 sellers
--5.Sellers by state: which states have more than 50 sellers?

SELECT seller_state, COUNT(*) AS sellers
FROM sellers s 
GROUP BY seller_state
HAVING COUNT(*) > 50
ORDER BY sellers DESC;
--6.Average price and freight per order item
--6.Average price and freight per order item, rounded to 2 decimals.

SELECT ROUND(AVG(price)::numeric, 2)         AS avg_price,
       ROUND(AVG(freight_value)::numeric, 2) AS avg_freight
FROM order_items oi 