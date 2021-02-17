
create sequence seq_hw9_log;

create or replace package pkg_atm_jg
as
    --main progran
    procedure p_run_all;
    function f_account_exists_yn(i_customer_fk number, i_pin_nr varchar2,i_account_fk number) return varchar2;
end pkg_atm_jg;

create or replace package body pkg_atm_jg
as
v_count_nr number;
v_customer_exist_tx varchar2(50);
v_pin_valid_tx varchar2(50);
customer_account_info   hw8_customer_account_view%ROWTYPE; 

    ---Function to check if customer exists
    function f_customer_exists_yn(i_customer_fk number) return varchar2
    is 
    v_customer_exist_tx varchar2(50);
    begin
        select count(*)
        into v_count_nr
        from hw8_customer_account_view
        where customer_pk = i_customer_fk;
    --END; 
        If v_count_nr>0 then
           v_customer_exist_tx :='Y';
        else
            v_customer_exist_tx :='N';
        end if;
    return v_customer_exist_tx;
    EXCEPTION WHEN no_data_found THEN 
        Return NULL; 
    End;  
   
    ---Function to check if pin is valid
    function f_customer_pin_valid_yn(i_customer_fk number,i_pin_nr varchar2) return varchar2
    is
    begin
        select count(*)
        into v_count_nr
        from hw8_customer_account_view
        where customer_pk = i_customer_fk
        and pin = i_pin_nr;
    --END; 
        If v_count_nr>0 then
           v_pin_valid_tx :='Y';
        else
            v_pin_valid_tx :='N';
        end if;
    return v_pin_valid_tx;
    End; 
    
    ---Function to check if account exist
    function f_account_exists_yn(i_customer_fk number, i_pin_nr varchar2,i_account_fk number) return varchar2
    is 
    v_account_exist_tx varchar2(50);
    begin
        select count(*)
        into v_count_nr
        from hw8_customer_account_view
        where customer_pk = i_customer_fk
        and pin = i_pin_nr
        and account_pk = i_account_fk;
    --END; 
        If v_count_nr>0 then
           v_account_exist_tx :='Y';
        else
            v_account_exist_tx :='N';
        end if;
    return v_account_exist_tx;
    End;  
 
    --log each transaction result
    procedure p_log(i_transaction_fk number, i_log_cd varchar2, i_log_tx varchar2)
    is
    begin
         insert into hw9_log(LOG_PK, LOG_TYPE_CD, DESCRIPTION, TRANSACTION_FK)
         values
         (seq_hw9_log.nextval, i_log_cd,i_log_tx,i_transaction_fk); 
    --commit;        
    end;
    
    ---handle deibt trasaction
    procedure p_handle_debit(i_transaction_fk number,i_customer_fk number,i_pin_nr varchar2,i_account_fk number,i_amount_nr number)
    is
    begin
    --END;
                if f_customer_exists_yn(i_customer_fk) ='Y' then
            if f_customer_pin_valid_yn(i_customer_fk,i_pin_nr) ='Y' then
                if f_account_exists_yn(i_customer_fk, i_pin_nr,i_account_fk) ='Y' then
                    p_log(i_transaction_fk, '','Success Deposit');
                    UPDATE hw8_account 
                    SET    balance =balance+i_amount_nr
                    WHERE  account_pk=i_amount_nr;
                END IF;
            END IF;
        END IF;
    end;   
  
    ---handle credit trasaction
    procedure p_handle_credit(i_transaction_fk number,i_customer_fk number,i_pin_nr varchar2,i_account_fk number,i_amount_nr number)
    is
    begin             
    --END;
           
        if f_customer_exists_yn(i_customer_fk) ='Y' then
            if f_customer_pin_valid_yn(i_customer_fk,i_pin_nr) ='Y' then
                if f_account_exists_yn(i_customer_fk, i_pin_nr,i_account_fk) ='Y' then
                    if customer_account_info.balance>=i_amount_nr then 
                        p_log(i_transaction_fk, '', 'Success Withdraw');
                        UPDATE hw8_account 
                        SET    balance =balance-i_amount_nr
                        WHERE  account_pk=i_amount_nr
                        AND    balance >=i_amount_nr; 
                    else 
                    p_log(i_transaction_fk, 'ERROR','Insufficient Amount');
                    END IF;
                END IF;
            END IF;
        END IF;
    end;    
    --handle transaction (DEBIT or CREDIT)
    procedure p_handle_transaction(i_transaction_fk number, i_customer_fk number,i_pin_nr varchar2,  i_account_fk number,i_action_cd varchar2,i_amount_nr number)
    is
    begin
        if i_action_cd ='Deposit' then
            p_handle_debit(i_transaction_fk,i_customer_fk,i_pin_nr,i_account_fk, i_amount_nr);
        elsif i_action_cd ='Withdraw' then
            p_handle_credit(i_transaction_fk,i_customer_fk,i_pin_nr,i_account_fk,i_amount_nr);
        end if;
    end;   

    --main progran
    procedure p_run_all
    is
    cursor cur_all is select * from hw8_transaction;
    begin
        for each_trans in cur_all loop
              if f_customer_exists_yn( each_trans.customer_fk) ='Y' then
                   if f_customer_pin_valid_yn(each_trans.customer_fk,each_trans.pin) ='Y' then
                        if f_account_exists_yn(each_trans.customer_fk,each_trans.pin,each_trans.account_fk) ='Y' then
                    --handle transaction
                    --p_handle_transaction(i_transaction_fk number, i_customer_fk number,i_pin_nr varchar2,  i_account_fk number,i_action_cd varchar2,i_amount_nr number)
                        p_handle_transaction(each_trans.transaction_pk,each_trans.customer_fk,each_trans.pin,each_trans.ACCOUNT_FK,each_trans.action_cd,each_trans.amount);
                        else
                        --log error
                            p_log(each_trans.transaction_pk, 'ERROR','account does not exist');
                        end if;
                    ELSE
            
                        p_log(each_trans.transaction_pk, 'ERROR','Invalid Pin');
                    end if; 
            else
                --log error
                p_log(each_trans.transaction_pk, 'ERROR','Customer Does not Exist');
            end if;
        end loop;
    end; 
End pkg_atm_jg;




---call the function to check if the customer exist
declare
   v_customer_exist_tx varchar2(50);
begin
   v_customer_exist_tx := pkg_atm_jg.f_customer_exists_yn(1);
   DBMS_OUTPUT.PUT_LINE(v_customer_exist_tx);
end;

---call the function to check if the pin is valid 
declare
   v_pin_valid_tx varchar2(50);
begin
   v_pin_valid_tx := pkg_atm_jg.f_customer_pin_valid_yn(1,'2345');
   DBMS_OUTPUT.PUT_LINE(v_pin_valid_tx);
end;





---call the function to check if the pin is valid 
declare
   v_pin_valid_tx varchar2(50);
begin
   v_pin_valid_tx := pkg_atm_jg.f_account_exists_yn(1,'1234',2);
   DBMS_OUTPUT.PUT_LINE(v_pin_valid_tx);
end;

---call the procedure to insert into LOG table
begin
  pkg_atm_jg.p_run_all;
END;


---take a look at my transaction table
select * from hw8_transaction

--check the log table result 
select * from hw9_log

















