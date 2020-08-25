CREATE TABLE App.Sales(
id int,
customerId int,
BranchId int,
ProductId int,
OrderId int,
region string
 )
using delta
PARTITIONED BY (partition_bin)
location '$LAKE_PATH/App/Sales'