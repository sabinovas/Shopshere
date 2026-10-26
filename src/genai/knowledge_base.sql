-- Knowledge base for the ShopSphere assistant (Week 7). Run on a SQL warehouse.
-- Statements separated by ';;' lines (for run_sql.ps1).
--
-- 1. ai.policy_chunks: policy PDFs in /Volumes/shopsphere_dev/ai/raw_docs/policies parsed with
--    ai_parse_document, one chunk per section (heading + the text under it).
-- 2. ai.knowledge_chunks: policy chunks + product descriptions + support tickets, the source
--    table of the Vector Search index (Delta Sync needs Change Data Feed).

CREATE OR REPLACE TABLE shopsphere_dev.ai.parsed_documents
COMMENT 'Raw ai_parse_document output per file'
AS SELECT path, ai_parse_document(content) AS parsed, current_timestamp() AS _parsed_ts
FROM READ_FILES('/Volumes/shopsphere_dev/ai/raw_docs/policies/', format => 'binaryFile')
;;
CREATE OR REPLACE TABLE shopsphere_dev.ai.policy_chunks
COMMENT 'Policy PDFs chunked by section'
AS
WITH elements AS (
  SELECT regexp_extract(path, '([^/]+)\\.pdf$', 1) AS doc_id,
         e.pos AS element_pos,
         e.col:type::string AS element_type,
         regexp_replace(e.col:content::string, '\\s+', ' ') AS content
  FROM shopsphere_dev.ai.parsed_documents
  LATERAL VIEW posexplode(variant_get(parsed, '$.document.elements', 'array<variant>')) e AS pos, col
),
titled AS (
  SELECT *,
         first_value(CASE WHEN element_type IN ('title', 'section_header') THEN content END, true)
           OVER (PARTITION BY doc_id ORDER BY element_pos) AS doc_title,
         sum(CASE WHEN element_type = 'section_header' THEN 1 ELSE 0 END)
           OVER (PARTITION BY doc_id ORDER BY element_pos) AS section_no
  FROM elements
  WHERE content NOT LIKE 'Synthetic document created%'  -- disclaimer line and page footer
),
sections AS (
  SELECT doc_id, section_no, max(doc_title) AS doc_title,
         max(CASE WHEN element_type = 'section_header' THEN content END) AS heading,
         array_join(collect_list(CASE WHEN element_type NOT IN ('section_header', 'title') THEN content END), ' ') AS body
  FROM titled
  GROUP BY doc_id, section_no
)
SELECT concat(doc_id, '#', section_no) AS chunk_id, doc_id, doc_title, heading, body,
       concat(doc_title, ' > ', heading, ': ', body) AS text
FROM sections
WHERE heading IS NOT NULL AND length(body) > 0  -- the parser labels some document titles 'title', not 'section_header'
;;
CREATE OR REPLACE TABLE shopsphere_dev.ai.knowledge_chunks (
  chunk_id    STRING NOT NULL COMMENT 'policy:<doc>#<section>, product:<product_id> or ticket:<review_id>:<order_id>',
  source_type STRING COMMENT 'policy, product or ticket',
  title       STRING,
  text        STRING COMMENT 'Text that is embedded and returned to the agent',
  order_id    STRING COMMENT 'Tickets only',
  product_id  STRING COMMENT 'Products only',
  CONSTRAINT knowledge_chunks_pk PRIMARY KEY (chunk_id)
)
COMMENT 'Source table of the Vector Search index ai.knowledge_index'
TBLPROPERTIES (delta.enableChangeDataFeed = true)
;;
INSERT OVERWRITE shopsphere_dev.ai.knowledge_chunks
SELECT concat('policy:', chunk_id), 'policy', concat(doc_title, ' - ', heading), text, NULL, NULL
FROM shopsphere_dev.ai.policy_chunks
UNION ALL
SELECT concat('product:', p.product_id), 'product', concat('Product ', p.product_id),
       concat('Product ', p.product_id, ' in category ', coalesce(p.category, 'unknown'),
              ' (Portuguese category name: ', coalesce(p.category_pt, 'unknown'), '). ',
              'Sold ', coalesce(pp.units_sold, 0), ' units in ', coalesce(pp.orders, 0), ' orders at an average price of R$',
              coalesce(format_number(pp.avg_price, 2), 'n/a'), '. Average review score ',
              coalesce(format_number(pp.avg_review_score, 1), 'n/a'), ' out of 5; late delivery rate ',
              coalesce(format_number(pp.late_delivery_rate * 100, 1), 'n/a'), '%. ',
              'Weight ', coalesce(cast(p.product_weight_g AS INT), 0), ' g, package ',
              coalesce(cast(p.product_length_cm AS INT), 0), ' x ', coalesce(cast(p.product_width_cm AS INT), 0), ' x ',
              coalesce(cast(p.product_height_cm AS INT), 0), ' cm, ', coalesce(p.product_photos_qty, 0), ' photos.'),
       NULL, p.product_id
FROM shopsphere_dev.gold.dim_product p
JOIN shopsphere_dev.gold.product_performance pp USING (product_sk)
-- Top 3,000 products by units sold: all 32,729 made the first embedding sync take over an hour,
-- and the agent can look up any product with SQL anyway
QUALIFY row_number() OVER (ORDER BY pp.units_sold DESC, p.product_id) <= 3000
UNION ALL
-- Olist reuses some review ids across orders, so the ticket key includes the order id
SELECT concat('ticket:', ticket_id, ':', order_id), 'ticket', concat('Ticket on order ', order_id, ' (', topic, ', ', sentiment, ')'),
       concat('Customer ticket about order ', order_id, ', review score ', review_score, '/5, topic: ', topic,
              ', sentiment: ', sentiment, '. Customer wrote: ', text_en),
       order_id, NULL
FROM shopsphere_dev.silver.tickets_enriched
;;
SELECT source_type, count(*) AS chunks, round(avg(length(text))) AS avg_chars
FROM shopsphere_dev.ai.knowledge_chunks GROUP BY ALL ORDER BY 1
;;
SELECT chunk_id, left(text, 160) AS text FROM shopsphere_dev.ai.knowledge_chunks WHERE source_type = 'policy' ORDER BY 1 LIMIT 6

;;
-- Embeddings computed in one batch with ai_query. A Delta Sync index with managed embeddings
-- embedded only about 50 rows a minute (two hours for 6,019 chunks), while batch ai_query did
-- 500 rows in 30 seconds. The index uses these vectors (self-managed embeddings), and the agent
-- embeds each question with the same model.
CREATE OR REPLACE TABLE shopsphere_dev.ai.knowledge_embeddings
COMMENT 'knowledge_chunks with databricks-gte-large-en embeddings (1024 dims); source of ai.knowledge_index'
TBLPROPERTIES (delta.enableChangeDataFeed = true)
AS SELECT *, CAST(ai_query('databricks-gte-large-en', text) AS ARRAY<FLOAT>) AS embedding  -- Vector Search needs float
FROM shopsphere_dev.ai.knowledge_chunks
;;
SELECT count(*) AS rows, min(size(embedding)) AS dim, count_if(embedding IS NULL) AS missing FROM shopsphere_dev.ai.knowledge_embeddings