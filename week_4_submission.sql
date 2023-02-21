/** C_CUSTKEY [c]

LAST_ORDER_DATE - the date of the last URGENT order placed [c]

ORDER_NUMBERS - a comma-separated list of the order_keys for the three highest dollar urgent orders [c]

TOTAL_SPENT - the total dollar amount of the three highest orders [c]

PART_1_KEY - the identifier of the part with the highest dollar amount spent, across all urgent orders 

PART_1_QUANTITY - the quantity ordered

PART_1_TOTAL_SPENT - total dollars spent on the part 

PART_2_KEY - the identifier of the part with the second-highest dollar amount spent, across all urgent orders  

PART_2_QUANTITY - the quantity ordered

PART_2_TOTAL_SPENT - total dollars spent on the part 

PART_3_KEY - the identifier of the part with the third-highest dollar amount spent, across all urgent orders 

PART_3_QUANTITY - the quantity ordered

PART_3_TOTAL_SPENT - total dollars spent on the part 

Gather all orders marked urgent. Rank orders by the total spent and partition on each customer window and order by order total... only allowing 
a rank of #1 to #3. 
Group urgent orders by customer key.
Create a table called order_quanity_sum by grouping line items by order id, sum(line item cost)
Create a table called top_3_urgent_order_details -> joining marked urgent and order line item
UNpivot the row of 3 into columns of 

Gather all orders marked urgent and join with line items. **/
with urgent_order_summary as  (
select 
	o_custkey
    ,o_orderkey
    ,o_totalprice
    ,o_orderdate
    ,rank() OVER ( PARTITION BY o_custkey ORDER BY o_totalprice) as order_amt_rank
from snowflake_sample_data.tpch_sf1.orders
	where o_orderpriority ='1-URGENT' 
    qualify order_amt_rank < 4
LIMIT 500),
customer_urgent_order_summary as (
select
	o_custkey
    ,max(o_orderdate) as last_order_date
    ,sum(o_totalprice) as total_spent
    ,array_agg(o_orderkey) as order_numbers
from urgent_order_summary
	group by o_custkey),
top_3_order_detail_summary as (
select 
	o_custkey
    ,l.l_partkey
    ,l.l_quantity
    ,l.l_extendedprice
    ,rank () over (PARTITION BY o_custkey order by l.l_extendedprice) as line_item_rank
from urgent_order_summary
 left join snowflake_sample_data.tpch_sf1.lineitem l
on urgent_order_summary.o_orderkey = l.l_orderkey
qualify line_item_rank < 4
),
answer as (
select *
from top_3_order_detail_summary
	pivot (count(l_partkey) for line_item_rank in (1,2,3)) as pivot_values(
    o_custkey
    ,PART_1_KEY
    ,PART_1_QUANTITY
    ,PART_1_TOTAL_SPENT
    ,PART_2_KEY
  --  ,PART_2_QUANTITY
-- ,PART_2_TOTAL_SPENT
    ))
select * from answer
