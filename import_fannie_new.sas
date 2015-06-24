** 02/04/2015 Code Modified by York **;

option compress=yes;
options mprint;

libname folder "./";
libname source "../data_source/fannie/";
libname fannie "../sas_dataset/fannie/";
libname txtds "../data/fannie/";
libname sasds "../sas_dataset/";
libname result "../sas_dataset_final";

%let source = ../data_source/fannie/;
%let txtds = ../data/fannie/;
%let startyr = 2000;

%include "./download.sas";
* input macro: %FILE_IMPORT(2000Q1);

*%let endyr = 2013;
%macro unzip_file();
filename gzs pipe "ls &source.*.gz";
data gz_list;
  infile gzs truncover;
  input gz_name $50.;
  format gz_file $30. file $20. txtfile $25.;
  gz_file = scan(gz_name,-1,'/');
  file = scan(gz_file,1,'.');
  txtfile = cats(file,'.txt');
run;
proc sort; by file; run;

filename txts pipe "ls &txtds.*.txt";
data txt_list1;
  infile txts truncover;
  input txt_name $50.;
  format file $20. period $10.;
  file = scan(scan(txt_name,-1,'/'),1,'.');
  period = scan(file,-1,'_');
run;
proc sort; by file; run;
proc print; run;

data new_gz;
  merge gz_list(in=a) txt_list1(in=b);
  by file;
  if a=1 and b=0;
run;
proc print; run;

data _NULL_;
  set new_gz;
  call system("gunzip -c -d &source."||strip(gz_file)||" >  &txtds."||strip(txtfile));
run;
%mend unzip_file;


%macro import(file);
%if %index(&file.,Acquisition) ^= 0 %then %do; %put ~~~~~acq;
  data &file.;        
    infile "&txtds.&file..txt" dlm= '|' MISSOVER DSD lrecl=32767 firstobs=1 ;
    input 	id_loan : $22. 
		channel : $22. 
		seller_name : $82. 
		Int_rt : 16. 
		orig_upb : 13. 
		orig_loan_term : 8.
    		Orig_date : $10. 
		dt_first_pi : $10. 
		ltv : 16. cltv : 16. 
		cnt_borr : 5. 
		dti: 16. fico : 8. 
		flag_fthb : $12.
    		loan_purpose : $22. 
		prop_type : $12. 
		cnt_units : 12. 
		occpy_sts : $22. 
		st : $22. 
		zipcode : $12. 
    		mi_pct : 16.  
		prod_type : $22.  ;
    label 	  id_loan = "loan indentifier"
      	  	  seller_name = "seller name"
      	  	  Int_rt = "original interest rate"
	  	  orig_upb = "original unpaid principal balance"
	  	  orig_loan_term = "original loan term"
	  	  orig_date = "origination date"
	  	  dt_first_pi = "first payment date"
	  	  ltv = "original loan-to-value"
	  	  cltv = "original combined loan-to-value"
	  	  cnt_borr = "number of borrowers"
	  	  dti = "original dect-to-income-ratio"
	 	  fico = "credit score"
	 	  flag_fthb = "first-time home buyer indicator"
		  loan_purpose = "loan purpose"
		  prop_type = "property type"
		  cnt_units = "number of units"
		  occpy_sts = "occupancy state"
		  st = "property state"
		  zipcode = "zip"
		  mi_pct = "mortgage insurance percentage"
		  prod_type = "product type";

  run;
%end;
%else %do;%put ~~~~perf;
  data &file.;
	infile "&txtds.&file..txt" dlm= '|' MISSOVER DSD lrecl=32767 firstobs=1 ;
	input 	ID_loan : $20. 
		Period : mmddyy10. 
		servicer_name : $80. 
		new_Int_rt : 14. 
		Act_endg_upb : 11.  
		loan_age : 10. 
		mths_remng : 3. 
		adj_mths_remng: 3. 
		dt_matr : $8. msa : $5. 
		delq_sts : $5. 
		flag_mod : $2. 
		CD_Zero_BAL : $3.
		Dt_zero_BAL : $8.  ;
	label     id_loan = "loan indentifier"
    	 	  period = "monthly reporting period"
		  servicer_name = "servicer name"
		  new_int_rt = "current interest rate"
		  act_endg_upb = "current actual unpaid principal balance"
		  loan_age = "loan age"
		  mths_remng = "remaining months to legal maturity"
		  adj_mths_remng = "adjusted remaining months to legal maturity"
		  dt_matr = "maturity date"
		  msa = "metropolitan statistical area"
		  delq_sts = "current loan delinquency status"
		  flag_mod = "modification flag"
		  cd_zero_bal = "zero balance code"
		  dt_zero_bal = "zerop balance effective date";

  run;
%end;
mend import;




%macro combine(period);
%put ~~~~acquisition&period.~~~~performance&period.;

  data id_loan_ac;
    set fannie.acquisition_&period.;
    *if st = 'IL' and zipcode = 606;
  run;
  proc sort; by LOAN_ID; run;


  data id_loan_pf;
    set fannie.performance_&period.;
   keep LOAN_ID CURRENT_RATE X_MATURITY_DATE SERVICER X_ACTIVITY_PERIOD CURRENT_DLQ_STATUS REMAINING_MONTHS ZERO_BALANCE_CODE X_ZERO_BALANCE_DATE;
  run;
  proc sort; by LOAN_ID; run;

  data loan;
    merge id_loan_ac(in=a) id_loan_pf;
    by LOAN_ID;
    if a = 1;
  run;

  data loan_float_p;
    set loan;
    if strip(CURRENT_DLQ_STATUS) ^= '0' or X_ZERO_BALANCE_DATE ^= .;
    if strip(CURRENT_DLQ_STATUS) = 'X' or strip(CURRENT_DLQ_STATUS) = 'x' then CURRENT_DLQ_STATUS = '10';
   
  run;
  
  data loan_zero_bal;
  retain ZERO_BALANCE_CODE X_ZERO_BALANCE_DATE;
    set loan_float_p(keep=LOAN_ID ZERO_BALANCE_CODE X_ZERO_BALANCE_DATE rename=(ZERO_BALANCE_CODE=cd_tmp X_ZERO_BALANCE_DATE=dt_tmp));
    by LOAN_ID;
   if first.LOAN_ID then do;
     ZERO_BALANCE_CODE = cd_tmp;
     X_ZERO_BALANCE_DATE = dt_tmp;
   end;
   else do; = '' and dt_tmp ^= '' then do; CD_Zero_BAL = cd_tmp; Dt_zero_BAL = dt_tmp; end;
     if X_ZERO_BALANCE_DATE = '' and dt_tmp ^= '' then do; ZERO_BALANCE_CODE = cd_tmp; X_ZERO_BALANCE_DATE = dt_tmp; end;
   if last.LOAN_ID;
   drop cd_tmp dt_tmp;
   label ZERO_BALANCE_CODE = 'ZERO BALANCE CODE'
         X_ZERO_BALANCE_DATE = 'ZERO BALANCE EFFECTIVE DATE';
  run; 

  proc sort nodupkey data=loan_float_p; by LOAN_ID CURRENT_DLQ_STATUS; run;

  data loan_fixed_p;
    set loan_float_p;
    drop period CURRENT_DLQ_STATUS REMAINING_MONTHS ZERO_BALANCE_CODE X_ZERO_BALANCE_DATE;
  run;
  proc sort nodupkey; by LOAN_ID; run;
  
  proc transpose data=loan_float_p(keep=LOAN_ID CURRENT_DLQ_STATUS X_ACTIVITY_PERIOD) out=dt_delq prefix =dt_delq_;
    by LOAN_ID;
    id CURRENT_DLQ_STATUS;
    var X_ACTIVITY_PERIOD;
  run;

  proc transpose data=loan_float_p(keep=LOAN_ID CURRENT_DLQ_STATUS REMAINING_MONTHS) out=mths_remng_delq prefix =mths_remng_delq_;
    by LOAN_ID;
    id CURRENT_DLQ_STATUS;
    var REMAINING_MONTHS;
  run;

  data float;
    merge dt_delq mths_remng_delq;
    by LOAN_ID;
    format dt_delq_1 - dt_delq_6 mmddyy10.
           dt_delq_10 mmddyy10.;
    rename dt_delq_10 = dt_delq_x
           mths_remng_delq_10 = mths_remng_delq_x;
    attrib _ALL_ label = '';
    drop _NAME_ _LABEL_;
  run;


  data fannie_all_&period.;
    merge loan_fixed_p loan_zero_bal float;
    by id_loan;
  run; 


%mend combine;







%macro import_txt();
filename txt pipe "ls &txtds.*.txt";
data txt_list;
  infile txt truncover;
  input txt_name $50.;
  format  file_period $15. file_year $5. file_qt $2.;
  txt_file = scan(scan(txt_name,-1,'/'),1,'.');
  file_period = scan(txt_file,-1,'_');
  file_year = substr(file_period,1,4);
  file_qt = substr(file_period,6,1);
run;

proc sort; by txt_file file_year file_qt; run;

data new_f;
  merge new_gz(keep=file rename=(file=txt_file) in=a) txt_list;
  by txt_file;
  if a=1;
run;
proc sort out=txt_period nodupkey; by file_period; run;
proc print; run;
data _NULL_;
  set txt_period;
  call symputx('all_p',_N_);
  call symput(cats('t',_N_),file_period);
run;

proc sort data=new_f; by file_year file_qt; run;
proc print; run;
data _NULL_;
  set new_f;
  %let maxobs = 0;
  call symputx('maxobs',_N_);
  call symput(cats('f',_N_),txt_file);
  call symput(cats('year',_N_),file_year);
  call symput(cats('qt',_N_),file_qt);
run;

%if %eval(&maxobs.) ^= 0 %then %do;
%do i=1 %to &maxobs.;
%put ~~~~~year=&&year&i.~~~~qt=&&qt&i.~~~file=&&f&i.;
  *%import(&&f&i.);
  %FILE_IMPORT(&&f&i.);
  %if %index(&&f&i.,Acquisition) ^= 0 %then %do;%put ~~~~acq~~again;
    %if %eval(&&qt&i.) = 1 %then %do;%put ~~~first~acq;
	  data sasds.fannie_Acquisition&&year&i.;
	    set &&f&i.;
	  run;
	%end;
	%else %do;
	  data sasds.fannie_Acquisition&&year&i.;
	    set sasds.fannie_Acquisition&&year&i. &&f&i.;
	  run;
	%end;
  %end;

  %else %do;
    %if %eval(&&qt&i.) = 1 %then %do;
	  data sasds.fannie_Performance&&year&i.;
	    set &&f&i.;
	  run;
	%end;
	%else %do;
	  data sasds.fannie_Performance&&year&i.;
	    set sasds.fannie_Performance&&year&i. &&f&i.;
	  run;
	%end;
  %end;

%end;
%end;
%do j=1 %to &all_p.;
  %put ~~~~~&&t&j.;
  %combine(&&t&j.);
  %if %index(&&t&j.,&startyr.Q1) ^= 0  %then %do; %put ~~~&&t&j.~~~new;
     data result.fannie_all ;
      set fannie_all_&&t&j.;
      rename current_int_rt = new_int_rt;
    run;
  %end;
  %else %do;%put ~~~&&t&j.~~~update;
    data result.fannie_all ;
      set result.fannie_all fannie_all_&&t&j.;
      rename current_int_rt = new_int_rt;
    run;
  %end;
%end;

data result.fannie_606;
  set result.fannie_all;
  if st = 'IL' and zipcode = 606;
run;
  	


%mend import_txt;

%unzip_file;
%import_txt;
