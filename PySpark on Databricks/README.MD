%md
## Project Description
**Objective**:  
- Clean and prepare sales and store data
- Add derived columns for advanced insights
- Store performance and product trends

**Datasets**  
>  **Sales data**(CSV file)  
> - sale_id: unique id for each sale
> - store_id: store where the sale occured
> - product_id: product sold
> - sale_date: date of the sale
> - quantity: numbe rof items sold
> - total_amount: total amount of the sales(USD)

> **Store data**(CSV file)   
> - store_id: unique id of the store
> - store_region: region where the store is located
> - store_size: size of the store in square feet
> - open_date: date when the store was opened

**Project Requirements**  
- Handle null values in both of the datsets
- Remove duplicates and invalid rows

**Data Tranformations:**  
- Join the sales and store datasets  
- Create new columns
- per square foot : calculate total sales divided by the store size
- year: extract the year from the date col
- Aggregate sales and quantity by store and region

**Analysis**:
- Identify the top five stores based on total sales
- Find the top five products by total quantity sold

**Save results:**
- Save the transformed and aggregated results as parquet files
