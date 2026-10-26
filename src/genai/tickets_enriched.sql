-- Support tickets from Olist review comments, enriched with AI functions (Week 7)
-- Run on a SQL warehouse. Statements separated by ';;' lines (for run_sql.ps1).
--
-- Olist has no support-ticket table, so review comments stand in for tickets. Tickets are
-- mostly complaints, so the sample takes up to 2,000 reviews scored 1-2 and 1,000 others.
-- ai_translate (pt -> en), ai_analyze_sentiment and ai_classify run on Databricks-hosted models.

CREATE OR REPLACE TABLE shopsphere_dev.silver.tickets_enriched
COMMENT 'Review comments used as support tickets: English translation, sentiment and topic from AI functions'
TBLPROPERTIES (delta.enableChangeDataFeed = true)
AS
WITH comments AS (
  SELECT r.review_id AS ticket_id, r.order_id, r.review_score, r.review_creation_ts AS created_ts,
         trim(concat_ws(' - ', r.review_comment_title, r.review_comment_message)) AS text_pt
  FROM shopsphere_dev.silver.reviews r
  WHERE r.review_comment_message IS NOT NULL AND length(r.review_comment_message) >= 20
),
sample AS (
  SELECT * FROM (SELECT * FROM comments WHERE review_score <= 2 ORDER BY xxhash64(ticket_id) LIMIT 2000)
  UNION ALL
  SELECT * FROM (SELECT * FROM comments WHERE review_score >= 3 ORDER BY xxhash64(ticket_id) LIMIT 1000)
),
translated AS (
  SELECT *, ai_translate(text_pt, 'en') AS text_en FROM sample
)
SELECT ticket_id, order_id, review_score, created_ts, text_pt, text_en,
       ai_analyze_sentiment(text_en) AS sentiment,
       ai_classify(text_en, ARRAY('delivery delay', 'product quality', 'wrong or missing item',
                                  'customer service', 'price or value', 'praise')) AS topic,
       current_timestamp() AS _enriched_ts
FROM translated
;;
SELECT topic, sentiment, count(*) AS tickets, round(avg(review_score), 2) AS avg_score
FROM shopsphere_dev.silver.tickets_enriched GROUP BY ALL ORDER BY tickets DESC
