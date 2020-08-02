CREATE TABLE App.Customer
(
    Id int not null,
    name string,
    phonenumber string,
    haha string
)
USING DELTA
LOCATION '$LAKE_PATH/App/Customer'