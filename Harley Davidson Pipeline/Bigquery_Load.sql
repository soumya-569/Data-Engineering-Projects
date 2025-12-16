
-- I have just created the table only, all the transformation is done in pysaprk on dataproc cluster and later on power bi

CREATE OR REPLACE TABLE `budget-cloud-465616.Harley_Davidson_sales.customers`(
  CustomerKey INT64,
  Prefix STRING,
  FirstName STRING,
  LastName STRING,
  BirthDate DATE,
  MaritalStatus STRING,
  Gender STRING,
  EmailAddress STRING,
  AnnualIncome FLOAT64,
  TotalChildren INT64,
  EducationLevel STRING,
  Occupation STRING,
  HomeOwner STRING,
  FullName STRING,
  Age FLOAT64,
  Priority STRING,
  IsParent STRING
);

CREATE OR REPLACE TABLE `budget-cloud-465616.Harley_Davidson_sales.products_whole`(
  ProductCategoryKey INT64,
  ProductSubcategoryKey INT64,
  ProductKey INT64,
  ProductSKU STRING,
  ProductName STRING,
  ModelName STRING,
  ProductDescription STRING,
  ProductColor STRING,
  ProductSize STRING,
  ProductStyle STRING,
  ProductCost FLOAT64,
  ProductPrice FLOAT64,
  SubcategoryName STRING,
  CategoryName STRING
);

CREATE OR REPLACE TABLE `budget-cloud-465616.Harley_Davidson_sales.products`(
  ProductKey INT64,
  ProductSubcategoryKey INT64,
  ProductSKU STRING,
  ProductName STRING,
  ModelName STRING,
  ProductDescription STRING,
  ProductColor STRING,
  ProductSize STRING,
  ProductStyle STRING,
  ProductCost FLOAT64,
  ProductPrice FLOAT64
);

CREATE OR REPLACE TABLE `budget-cloud-465616.Harley_Davidson_sales.product_subcategory`(
  ProductSubcategoryKey INT64,
  SubcategoryName STRING,
  ProductCategoryKey INT64
);

CREATE OR REPLACE TABLE `budget-cloud-465616.Harley_Davidson_sales.product_category`(
  ProductCategoryKey INT64,
  CategoryName STRING
);

CREATE OR REPLACE TABLE `budget-cloud-465616.Harley_Davidson_sales.sales`(
  OrderDate DATE,
  StockDate DATE,
  OrderNumber STRING,
  ProductKey INT64,
  CustomerKey INT64,
  TerritoryKey INT64,
  OrderLineItem INT64,
  OrderQuantity INT64,
  order_month INT64,
  order_year INT64,
  order_week INT64,
  is_weekend STRING
);

CREATE OR REPLACE TABLE `budget-cloud-465616.Harley_Davidson_sales.product_sales`(
  ProductKey INT64,
  OrderDate DATE,
  StockDate DATE,
  OrderNumber STRING,
  CustomerKey INT64,
  TerritoryKey INT64,
  OrderLineItem INT64,
  OrderQuantity INT64,
  order_month INT64,
  order_year INT64,
  order_week INT64,
  is_weekend STRING,
  ProductSubcategoryKey INT64,
  ProductSKU STRING,
  ProductName STRING,
  ModelName STRING,
  ProductDescription STRING,
  ProductColor STRING,
  ProductSize STRING,
  ProductStyle STRING,
  ProductCost FLOAT64,
  ProductPrice FLOAT64,
  Revenue FLOAT64,
  Expense FLOAT64,
  Profit FLOAT64
);

CREATE OR REPLACE TABLE `budget-cloud-465616.Harley_Davidson_sales.location`(
  SalesTerritoryKey INT64,
  Region STRING,
  Country STRING,
  Continent STRING
);

CREATE OR REPLACE TABLE `budget-cloud-465616.Harley_Davidson_sales.returns`(
  ReturnDate DATE,
  TerritoryKey INT64,
  ProductKey INT64,
  ReturnQuantity INT64
);
