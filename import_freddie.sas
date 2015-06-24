option compress=yes;
options mprint;

libname folder "./";
libname source "../data_source/freddie/";
libname txtds "../data/freddie/";
libname sasds "/home/yzhang96/GSE/sas_dataset/";
libname result "../sas_dataset_final/";

%let source = ../data_source/freddie/;
%let txtds = ../data/freddie/;
%let  sasds = /home/yzhang96/GSE/sas_dataset/;
%let startyr = 1999;
%let endyr = 1999;


%macro unzip_file();
filename zips pipe "ls &source.*.zip";
data zip_list;
  infile zips truncover;
  input zip_name $50.;
  format zip_file $30. file $30. period $10.;
  zip_file = scan(zip_name,-1,'/');
  file = scan(zip_file,1,'.');
  period = scan(file,-1,'_');
run;
proc sort; by file; run;
proc print; run;

filename txts pipe "ls &txtds.*.txt";
data txt_list1;
  infile txts truncover;
  input txt_name $50.;
  format file $30.;
  file = scan(scan(txt_name,-1,'/'),1,'.');
run;
proc sort; by file; run;
proc print; run;

data unzip;
  merge zip_list(in=a) txt_list1(in=b);
  by file;
  if a=1 and b=0;
run;
proc print; run;

data _NULL_;
  set unzip;
  call system("unzip -u &source."||strip(zip_file)||" -d &txtds.");
run;
%mend unzip_file;


%macro import(file,out_f);
%if %index(&file.,time) ^= 0 %then %do;%put ~~~~~performance;
  data &out_f.;*performance;
    infile "&txtds.&file..txt" dlm= '|' MISSOVER DSD lrecl=32767 firstobs=1 ;
    input ID_loan : $12. Period : 8. Act_endg_upb : 8. delq_sts : 8. loan_age : 8. mths_remng : 8. repch_flag : $1.
      flag_mod : $1. CD_Zero_BAL : $3. Dt_zero_BAL : 8. New_Int_rt : 8. ;
    label id_loan = "loan indentifier"
          period = "monthly reporting period"
          act_endg_upb = "current actual unpaid principal balance"
          delq_sts = "current loan delinquency status"
          loan_age = "loan age"
          mths_remng = "remaining months to legal maturity"
          repch_flag = "repurchase flag"
          flag_mod = "modification flag"
          cd_zero_bal = "zero balance code"
          dt_zero_bal = "zero balance effective date"
  	  new_int_rt = "current interest rate";
  run;
%end;
%else %do;%put ~~~~origination;
  data &out_f.;*origination;
    infile "&txtds.&file..txt" dlm= '|' MISSOVER DSD lrecl=32767 firstobs=1 ;
    input fico : 8. dt_first_pi : 8. flag_fthb : $1. dt_matr : 8. cd_msa : 8. mi_pct : 8. cnt_units : 8. occpy_sts : $1.
      cltv : 8. dti : 8. orig_upb : 8. ltv : 8. int_rt : 8. channel : $1. ppmt_pnlty : $1. prod_type : $5. st : $2. 
      prop_type : $2. zipcode : $5. id_loan : $16. loan_purpose : $5. orig_loan_term : 8. cnt_borr : $2. seller_name : $30.
      servicer_name : $30. ;
    label fico = "credit score"
          dt_first_pi = "first payment date"
          flag_fthb = "first-time home buyer indicator"
          dt_matr = "maturity date"
          cd_msa = "metropolitan statistical area or metropolitan division"
          mi_pct = "mortgage insurance percentage"
          cnt_units = "number of units"
          occpy_sts = "occupancy state"
          cltv = "original combined loan-to-value"
          dti = "original dect-to-income-ratio"
          orig_upb = "original unpaid principal balance"
          ltv = "original loan-to-value"
          Int_rt = "original interest rate"
          ppmt_pnlty="prepayment penalty mortgage flag"
          prod_type = "product type"
          st = "property state"
          prop_type = "property type"
          zipcode = "zip"
          id_loan = "loan indentifier"
          loan_purpose = "loan purpose"
          orig_loan_term = "original loan term"        
          cnt_borr = "number of borrowers"
          seller_name = "seller name"
          servicer_name = "servicer name";
  run;
%end;

%mend import;



%macro combine(period);
%put ~~~~&period.~~~origination_&period.~~~~performance_&period.;
  data id_loan_ac;
    set sasds.freddie_origination&period.;
    *if st = 'IL' and zipcode = 60600;
  run;
  proc sort; by id_loan; run;


  data id_loan_pf;
    set sasds.freddie_performance&period.;
    keep id_loan New_Int_rt period delq_sts mths_remng CD_Zero_BAL Dt_zero_BAL;
  run;
  proc sort; by id_loan; run;

  data loan;
    merge id_loan_ac(in=a) id_loan_pf;
    by id_loan;
    if a = 1;
  run;

  data loan_float_p;
    set loan;
    if delq_sts ^= 0 or Dt_zero_BAL ^= .;
    if delq_sts = . then delq_sts = 10;
  run;
  
  data loan_zero_bal;
  retain CD_Zero_BAL Dt_zero_BAL;
    set loan_float_p(keep=id_loan CD_Zero_BAL Dt_zero_BAL rename=(CD_Zero_BAL=cd_tmp Dt_zero_BAL=dt_tmp));
    by id_loan;
   if first.id_loan then do;
     CD_Zero_BAL = cd_tmp;
     Dt_zero_BAL = dt_tmp;
   end;
   else do;
     if Dt_zero_BAL = '' and dt_tmp ^= '' then do; CD_Zero_BAL = cd_tmp; Dt_zero_BAL = dt_tmp; end;
   end;
   if last.id_loan;
   drop cd_tmp dt_tmp;
   label CD_Zero_BAL = 'zero balance code'
         Dt_zero_BAL = 'zero balance effective date';
  run; 

  proc sort nodupkey data=loan_float_p; by id_loan delq_sts; run;

  data loan_fixed_p;
    set loan_float_p;
    drop period delq_sts mths_remng CD_Zero_BAL Dt_zero_BAL;
  run;
  proc sort nodupkey; by id_loan; run;
  
  proc transpose data=loan_float_p(keep=id_loan delq_sts period where=(delq_sts^=0)) out=dt_delq prefix =dt_delq_;
    by id_loan;
    id delq_sts;
    var period;
  run;


  proc transpose data=loan_float_p(keep=id_loan delq_sts mths_remng where=(delq_sts^=0)) out=mths_remng_delq prefix =mths_remng_delq_;
    by id_loan;
    id delq_sts;
    var mths_remng;
  run;

  data float;
    merge dt_delq mths_remng_delq;
    by id_loan;
    attrib _ALL_ label = '';
    drop _NAME_ _LABEL_;
  run;


  data freddie_all_&period.;
    merge loan_fixed_p loan_zero_bal float;
    by id_loan;
    rename dt_delq_10 = dt_delq_x
           mths_remng_delq_10 = mths_remng_delq_x;
  run; 
data folder.freddie_all_&period.; set freddie_all_&period.; run;

%mend combine;




%macro import_txt();
filename txt pipe "ls &txtds.*.txt";
data txt_list;
  infile txt truncover;
  input txt_name $50.;
  format txt_file $30. file_period $10. period $10. year $8. qt $3. output_file $20.;
  txt_file = scan(scan(txt_name,-1,'/'),1,'.');
  file_period = scan(txt_file,-1,'_');
  period = cats(substr(file_period,3,4),substr(file_period,1,2));
  year = substr(file_period,3,4);
  qt = substr(file_period,2,1);
  if index(txt_file,'time') ^= 0 then output_file = cats('performance','_',period);
  else output_file = cats('origination','_',period);

run;
proc sort; by file_period; run;
proc sort data=unzip; by period; run;
data new_f;
  merge unzip(keep=period rename=(period=file_period) in=a) txt_list(in=b);
  by file_period;
  if a=1;
run; 

proc sort out=txt_period nodupkey; by period; run;
data _NULL_;
  set txt_period;
  call symputx('all_p',_N_);
  call symput(cats('period',_N_),period);
run;

proc sort data=new_f; by year qt txt_file; run;
proc print; run;
data _NULL_;
  set new_f;
  call symput('maxobs',_N_);
  call symput(cats('input',_N_), txt_file);
  call symput(cats('output',_N_), output_file);
  call symput(cats('year',_N_), year);
  call symput(cats('qt',_N_),qt);
run;

%do i=1 %to &maxobs.;
%put ~~~~~year=&&year&i.~~~~qt=&&qt&i.~~~input=&&input&i~~~~output=&&output&i.;
  %import(&&input&i.,&&output&i.);

  %if %index(&&output&i.,origination) ^= 0 %then %do;%put ~~~~ori~~again;
    %if %eval(&&qt&i.) = 1 %then %do;%put ~~~first~acq;
	  data sasds.freddie_origination&&year&i.;
	    set &&output&i.;
	  run;
	%end;
	%else %do;
	  data sasds.freddie_origination&&year&i.;
	    set sasds.freddie_origination&&year&i. &&output&i.;
	  run;
	%end;
  %end;

  %else %do;
    %if %eval(&&qt&i.) = 1 %then %do;
	  data sasds.freddie_Performance&&year&i.;
	    set &&output&i.;
	  run;
	%end;
	%else %do;
	  data sasds.freddie_Performance&&year&i.;
	    set sasds.freddie_Performance&&year&i. &&output&i.;
	  run;
	%end;
  %end;
%end;

%do j=1 %to &all_p.;
  %put ~~~~&&year&j.;
  %combine(&&year&j.);
  %if %index(&&period&j.,Q1&startyr.) ^= 0  %then %do; %put ~~~&&period&j.~~~new;
    data result.freddie_all;
      set freddie_all_&&year&j.;
      label servicer_name ="servicer name"
            new_int_rt = "current interest rate";
    run;
  %end;
  %else %do; %put ~~~&&period&j.~~~update;
    data result.freddie_all;
      set result.freddie_all freddie_all_&&year&j.;
      label servicer_name ="servicer name"
            new_int_rt = "current interest rate";
    run;
  %end;
%end;

    
%mend import_txt;

%unzip_file;
%import_txt;
