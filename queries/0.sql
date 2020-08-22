DELIMITER //
create procedure refresh_function1(sf decimal)
begin
    declare iterations1 int default 0;
    declare iterations2 int default 0;
    declare i int default 0;
    declare j int default 0;
    set iterations1 = floor(1500 * sf);

    start transaction;
    while i < iterations1 do
        insert into ORDERS (O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT) values (3022, "O", 20000.00, "5-LOW", "Clerk#000000616", 0, "ly special requests");        
        set iterations2 = floor(RAND() * (7-1)+1);
        while j < iterations2 do
            insert into CUSTOMER (C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT) values ("Customer#00000000663", "IVhzIApeRb ot,c,E", 12, "25-989-741-2988", 711.56, "BUILDING", "to the even, regular platelets. regular, ironic epitaphs nag e");
            set j = j + 1;
        end while;
        set i = i + 1;
    end while;
    commit;
end;
DELIMITER ;