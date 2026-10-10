--1.--Revenue (SUM of price) by customer_state for delivered orders. Top 10 states.

SELECT c.customer_state,
       SUM(oi.price) AS revenue
FROM raw.orders o
JOIN raw.customers   c  ON c.customer_id = o.customer_id
JOIN raw.order_items oi ON oi.order_id   = o.order_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_state
ORDER BY revenue DESC
LIMIT 10;

--2.--How many orders have no review at all? (anti-join)

SELECT COUNT(*) AS orders_without_review
FROM raw.orders o
LEFT JOIN raw.order_reviews r ON r.order_id = o.order_id
WHERE r.order_id IS NULL;

-- Q3: some categories have no translation (and some products have
--     no category) — INNER JOIN would silently drop those items
--Top 10 product categories in English by items sold. Why LEFT JOIN to the translation?

SELECT COALESCE(t.product_category_name_english,
                p.product_category_name, 'unknown') AS category,
       COUNT(*) AS items_sold
FROM raw.order_items oi
JOIN raw.products p ON p.product_id = oi.product_id
LEFT JOIN raw.category_translation t
       ON t.product_category_name = p.product_category_name
GROUP BY 1
ORDER BY items_sold DESC
LIMIT 10;

--What share of order items were sold by a seller in the same state as the customer?
SELECT ROUND(100.0 * SUM(CASE WHEN s.seller_state = c.customer_state
                              THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_same_state
FROM raw.order_items oi
JOIN raw.sellers   s ON s.seller_id   = oi.seller_id
JOIN raw.orders    o ON o.order_id    = oi.order_id
JOIN raw.customers c ON c.customer_id = o.customer_id;

--For 5 orders, show item revenue and payment total side by side — without fan-out.
SELECT i.order_id, i.item_revenue, p.paid
FROM (SELECT order_id, SUM(price + freight_value) AS item_revenue
      FROM raw.order_items GROUP BY order_id) i
JOIN (SELECT order_id, SUM(payment_value) AS paid
      FROM raw.order_payments GROUP BY order_id) p
  ON p.order_id = i.order_id
LIMIT 5;
--Find orders where payments differ from price + freight by more than 1.00.
SELECT i.order_id, i.item_revenue, p.paid,
       p.paid - i.item_revenue AS diff
FROM (SELECT order_id, SUM(price + freight_value) AS item_revenue
      FROM raw.order_items GROUP BY order_id) i
JOIN (SELECT order_id, SUM(payment_value) AS paid
      FROM raw.order_payments GROUP BY order_id) p
  ON p.order_id = i.order_id
WHERE ABS(p.paid - i.item_revenue) > 1.00
ORDER BY ABS(p.paid - i.item_revenue) DESC;


