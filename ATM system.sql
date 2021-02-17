---Part1: Create tables
CREATE TABLE hw8_account (
    account_pk        NUMBER NOT NULL,
    balance           NUMBER,
    customer_fk       NUMBER NOT NULL,
    account_type_fk   NUMBER NOT NULL
);

ALTER TABLE hw8_account ADD CONSTRAINT hw8_account_pk PRIMARY KEY ( account_pk );

CREATE TABLE hw8_account_type (
    account_type_pk   NUMBER NOT NULL,
    account_type      VARCHAR2(30)
);

ALTER TABLE hw8_account_type ADD CONSTRAINT hw8_account_type_pk PRIMARY KEY ( account_type_pk );

CREATE TABLE hw8_customer (
    customer_pk   NUMBER NOT NULL,
    first_name    VARCHAR2(50),
    last_name     VARCHAR2(50),
    pin           VARCHAR2(10)
);

ALTER TABLE hw8_customer ADD CONSTRAINT hw8_customer_pk PRIMARY KEY ( customer_pk );

CREATE TABLE hw8_log (
    log_pk           NUMBER NOT NULL,
    log_type         VARCHAR2(25),
    description      VARCHAR2(4000),
    transaction_fk   NUMBER NOT NULL
);

ALTER TABLE hw8_log ADD CONSTRAINT hw8_log_pk PRIMARY KEY ( log_pk );

CREATE TABLE hw8_transaction (
    transaction_pk   NUMBER NOT NULL,
    customer_fk      NUMBER,
    pin              VARCHAR2(10),
    account_fk       NUMBER,
    amount           NUMBER,
    type             VARCHAR2(15)
);

ALTER TABLE hw8_transaction ADD CONSTRAINT hw8_transaction_pk PRIMARY KEY ( transaction_pk );

ALTER TABLE hw8_account
    ADD CONSTRAINT hw8_account_account_type_fk FOREIGN KEY ( account_type_fk )
        REFERENCES hw8_account_type ( account_type_pk );

ALTER TABLE hw8_account
    ADD CONSTRAINT hw8_account_customer_fk FOREIGN KEY ( customer_fk )
        REFERENCES hw8_customer ( customer_pk );

ALTER TABLE hw8_log
    ADD CONSTRAINT hw8_log_hw8_transaction_fk FOREIGN KEY ( transaction_fk )
        REFERENCES hw8_transaction ( transaction_pk );

--Part 2: Insert data into tables
----insert records into tables
insert into  hw8_customer (customer_pk,first_name,last_name,pin) 
values(1,'Jiaoyu','Guo','1234')
insert into  hw8_customer (customer_pk,first_name,last_name,pin) 
values(2,'Takk','Yamaguchi','2345')
insert into  hw8_customer (customer_pk,first_name,last_name,pin) 
values(3,'Yuxiao','Huo','3456')
insert into  hw8_customer (customer_pk,first_name,last_name,pin) 
values(4,'Jingyu','Tian','4567')
insert into  hw8_customer (customer_pk,first_name,last_name,pin) 
values(5,'Zhiwei','Qiao','5678')
select * from hw8_customer

INSERT INTO hw8_account_type (account_type_pk,account_type) 
values (1,'checking_account')
INSERT INTO hw8_account_type (account_type_pk,account_type) 
values (2,'saving_account')
select * from hw8_account_type


insert into hw8_account (account_pk,balance,account_type_fk,customer_fk)
values (1,5000, 2,1)
insert into hw8_account (account_pk,balance,account_type_fk,customer_fk)
values (2,6000, 2,2)
insert into hw8_account (account_pk,balance,account_type_fk,customer_fk)
values (3,7000, 2,3)
insert into hw8_account (account_pk,balance,account_type_fk,customer_fk)
values (4,8000, 2,4)
insert into hw8_account (account_pk,balance,account_type_fk,customer_fk)
values (5,9000, 2,5)
select * from hw8_account



insert into hw8_transaction(transaction_pk, customer_fk, pin, account_fk, amount, type)
values(1,25,'2222',2,7500,'Withdraw')
insert into hw8_transaction(transaction_pk, customer_fk, pin, account_fk, amount, type)
values(2,1,'3456',2,7500,'Withdraw')
insert into hw8_transaction(transaction_pk, customer_fk, pin, account_fk, amount, type)
values(3,1,'1234',5,7500,'Withdraw')
insert into hw8_transaction(transaction_pk, customer_fk, pin, account_fk, amount, type)
values(4,1,'1234',1,7500,'Withdraw')
insert into hw8_transaction(transaction_pk, customer_fk, pin, account_fk, amount, type)
values(5,1,'1234',1,500,'Withdraw')
insert into hw8_transaction(transaction_pk, customer_fk, pin, account_fk, amount, type)
values(6,2,'2345',2,100,'Deposit')
insert into hw8_transaction(transaction_pk, customer_fk, pin, account_fk, amount, type)
values(7,2,'2345',2,500,'Withdraw')
insert into hw8_transaction(transaction_pk, customer_fk, pin, account_fk, amount, type)
values(8,3,'3456',3,-500,'Deposit')
insert into hw8_transaction(transaction_pk, customer_fk, pin, account_fk, amount, type)
values(9,3,'3456',3,-500,'Withdraw')
insert into hw8_transaction(transaction_pk, customer_fk, pin, account_fk, amount, type)
values(10,4,'4567',4,500,'Withdraw')
insert into hw8_transaction(transaction_pk, customer_fk, pin, account_fk, amount, type)
values(11,4,'4567',2,500,'Deposit')
insert into hw8_transaction(transaction_pk, customer_fk, pin, account_fk, amount, type)
values(12,5,'5678',5,14000,'Withdraw')
insert into hw8_transaction(transaction_pk, customer_fk, pin, account_fk, amount, type)
values(13,5,'5678',5,300,'Deposit')

select * from hw8_transaction


--Part 3: Write Algorithms 
---create sequence
create sequence seq_hw8_log

DECLARE 
  ---List of all transaction that I need to process 
    CURSOR cur_transaction IS 
    SELECT * 
    FROM   hw8_transaction; 
    v_count_nr       NUMBER; 
    v_log_cd         VARCHAR(25); 
    v_log_message_tx VARCHAR(150); 
    rec_customer hw8_customer%ROWTYPE; 
    rec_account hw8_account%ROWTYPE; 
    rec_customer_account_info   hw8_customer_account_view%ROWTYPE; 
    
BEGIN 
  ----get a list of each transaction 
  FOR each_trans IN cur_transaction LOOP 
  null;
    --dbms_output.put_line(each_trans.transaction_pk||' : '||each_trans.customer_fk||' : '||each_trans.pin||' : '||each_trans.account_fk||' : '||each_trans.amount);
    --start processing each transaction 
    BEGIN 
    ----filter customers who has a valid id
                  SELECT * 
                  INTO   rec_customer_account_info 
                  FROM   hw8_customer_account_view
                  WHERE  customer_pk=each_trans.customer_fk;     
            EXCEPTION WHEN no_data_found THEN NULL; 
            END; 
            --dbms_output.put_line('pin:'||rec_customer.pin);
            
            IF rec_customer_account_info.customer_pk IS NOT NULL THEN 
    ---When Customer Id is valid, check the pin number
               IF rec_customer_account_info.pin=each_trans.pin THEN 
                --DBMS_OUTPUT.PUT_LINE(rec_customer.customer_pk||rec_customer.PIN);
    ---When Customer Id and pin are all good, check the account ID
                    select count(*)
                    into v_count_nr
                    from hw8_account
                    where customer_fk=each_trans.customer_fk
                    and account_pk=each_trans.account_fk;
                    --DBMS_OUTPUT.put_line(v_count_nr);
                          If v_count_nr=1 then
                            null;  
                    ---check if account is good, account found, check if the amount is negative
                            if each_trans.amount>0 THEN
                             null;  
                             
                               If each_trans.type='Deposit' Then
                                    UPDATE hw8_account 
                                    SET    balance =balance+each_trans.amount 
                                    WHERE  account_pk=each_trans.account_fk;  
                                    ---the deposit is success
                                    v_log_cd :='SUCCESS'; 
                                    v_log_message_tx :='Success Deposit'; 
                                ELSIF each_trans.type='Withdraw' Then                                    
                    ---account is found, amount is postive, check if the balance is enough for the withdraw
                                    IF rec_customer_account_info.balance >=each_trans.amount THEN 
                                        --fund is sufficient 
                                        UPDATE hw8_account 
                                        SET    balance =balance-each_trans.amount 
                                        WHERE  account_pk=each_trans.account_fk 
                                        AND    balance >=each_trans.amount; 
                                       ---the withdraw is success
                                        v_log_cd :='SUCCESS'; 
                                        v_log_message_tx :='Success Withdraw'; 
                                    ELSE 
                                    --Insufficient Funds, fail the withdraw money 
                                    v_log_cd :='ERROR'; 
                                    v_log_message_tx :='Insufficient Funds'; 
                                    END IF; 
                                End If;
                              ELSE 
                              --Amount is negative, reject the withdraw or deposit process
                              v_log_cd :='ERROR'; 
                              v_log_message_tx :='Invalid Process: Negative amount'; 
                            END IF;       
                                                                             
                            ELSE 
                              --invalid account number, fail the withdraw money 
                              v_log_cd :='ERROR'; 
                              v_log_message_tx :='Invalid Account Id'; 
                            END IF; 
                      ELSE 
                      --Invalid Pin, fail the withdraw money 
                        v_log_cd :='ERROR'; 
                        v_log_message_tx :='Invalid Pin'; 
                      END IF; 
                ELSE 
                --Customer does not exist in our system, fail the withdraw money 
                  v_log_cd :='ERROR'; 
                  v_log_message_tx :='Customer Does Not Exist'; 
                END IF; 
                
 ----keep track all records into log table           
    INSERT INTO hw8_log 
                ( 
                            log_pk, 
                            log_type, 
                            description, 
                            transaction_fk 
                ) 
                VALUES 
                ( 
                            seq_hw8_log.NEXTVAL, 
                            v_log_cd, 
                            v_log_message_tx, 
                            each_trans.transaction_pk 
                ); 

 END LOOP; 
END;

---Part 4: Check log table
select * from hw8_log
---check if the account balance is correct
select * from hw8_account


