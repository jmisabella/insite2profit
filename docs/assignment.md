# INSITE2PROFIT Take-Home Assignment

## 1. Data Loading
Load the provided three files using PySpark.
- Name each table with a raw_ prefix to differentiate between original and transformed data.

## 2. Data Review and Storage
1. Review the loaded data and assign appropriate data types based on your best judgment.
2. Identify primary and foreign keys for each table.
3. Store the transformed data using a store_ prefix.

## 3. Product Master Transformations
Perform the following transformations on the product master data and write the results into a table named publish_product:
1. Replace NULL values in the Color field with N/A.
2. Enhance the ProductCategoryName field when it is NULL using the following logic:
    a. If ProductSubCategoryName is in ('Gloves', 'Shorts', 'Socks', 'Tights', 'Vests'), set ProductCategoryName to 'Clothing'.
    b. If ProductSubCategoryName is in ('Locks', 'Lights', 'Headsets', 'Helmets', 'Pedals', 'Pumps'), set ProductCategoryName to 'Accessories'.
    c. If ProductSubCategoryName contains the word 'Frames' or is in ('Wheels', 'Saddles'), set ProductCategoryName to 'Components'.

## 4. Sales Order Transformations
1. Join SalesOrderDetail with SalesOrderHeader on SalesOrderId and apply the following transformations:
2. Calculate LeadTimeInBusinessDays as the difference between OrderDate and ShipDate, excluding Saturdays and Sundays.
3. Calculate TotalLineExtendedPrice using the formula: OrderQty * (UnitPrice - UnitPriceDiscount).
4. Write the results into a table named publish_orders, including:
    - All fields from SalesOrderDetail.
    - All fields from SalesOrderHeader except SalesOrderId, and rename Freight to TotalOrderFreight.

## 5. Analysis Questions
Provide answers to the following questions based on the transformed data:
- Which color generated the highest revenue each year?
- What is the average LeadTimeInBusinessDays by ProductCategoryName?