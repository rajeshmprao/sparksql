CREATE TABLE App.Customer
(
    Id int not null,
    name string comment 'awesome name',
    phonenumber string,
    haha string
)
USING DELTA
LOCATION '$LAKE_PATH/App/Customer'