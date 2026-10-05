--1. Customers per state (top 5)

SELECT customer_state, COUNT(*) AS customers
FROM customers
GROUP BY customer_state
ORDER BY customers DESC
LIMIT 5;
--2.Distinct real customers
SELECT COUNT(DISTINCT customer_unique_id) AS real_customers
FROM customers;
--3. Most-used payment type
SELECT payment_type, COUNT(*) AS uses
FROM order_payments op 
GROUP BY payment_type
ORDER BY uses DESC;
--4. Reviews by score and overall average
SELECT review_score, COUNT(*) AS reviews
FROM order_reviews
GROUP BY review_score
ORDER BY review_score;

SELECT ROUND(AVG(review_score)::numeric, 2) AS avg_score
FROM order_reviews t ;
--5. Seller states with more than 50 sellers
SELECT seller_state, COUNT(*) AS sellers
FROM sellers s 
GROUP BY seller_state
HAVING COUNT(*) > 50
ORDER BY sellers DESC;
--6.Average price and freight per order item
SELECT ROUND(AVG(price)::numeric, 2)         AS avg_price,
       ROUND(AVG(freight_value)::numeric, 2) AS avg_freight
FROM order_items oi 