

/* Import data from csv files. Download the sample file via the link above from the CBOE-Datashop and adapt the path.*/
data data; 
	infile "C:\abc\Code_publish\UnderlyingOptionsTrades_2016-06-01.csv" 
	delimiter = ',' 
	MISSOVER 
	DSD 
	lrecl=32767 
	firstobs=2 ;

	informat underlying_symbol $6.;
	informat quote_datetime anydtdtm23.;
	informat sequence_number BEST16.;
	informat root $6.;
	informat expiration yymmdd8.;
	informat strike BEST8.3;
	informat option_type $1.;
	informat exchange_id BEST8.;
	informat trade_size BEST8.4;
	informat trade_price BEST8.4;
	informat trade_condition_id BEST8.;
	informat canceled_trade_condition_id BEST8.;
	informat best_bid BEST8.4;
	informat best_ask BEST8.4;
	informat underlying_bid BEST8.4;
	informat underlying_ask BEST8.4;
	informat number_of_exchanges BEST8.;
	informat exchange BEST8.;
	informat bid_size BEST8.;
	informat bid BEST8.4;
	informat ask_size BEST8.;
	informat ask BEST8.4;

	format underlying_symbol $6.;
	format quote_datetime datetime.;
	format sequence_number BEST16.;
	format root $6.;
	format expiration date9.;
	format strike BEST8.3;
	format option_type $1.;
	format exchange_id BEST8.;
	format trade_size BEST8.4;
	format trade_price BEST8.4;
	format trade_condition_id BEST8.;
	format canceled_trade_condition_id BEST8.;
	format best_bid BEST8.4;
	format best_ask BEST8.4;
	format underlying_bid BEST8.4;
	format underlying_ask BEST8.4;
	format number_of_exchanges BEST8.;
	format exchange BEST8.;
	format bid_size BEST8.;
	format bid BEST8.4;
	format ask_size BEST8.;
	format ask BEST8.4;

	input
		underlying_symbol $
		quote_datetime 
		sequence_number
		root $
		expiration 
		strike 
		option_type $
		exchange_id 
		trade_size 
		trade_price 
		trade_condition_id 
		canceled_trade_condition_id 
		best_bid 
		best_ask 
		underlying_bid 
		underlying_ask 
		number_of_exchanges 
		exchange 
		bid_size 
		bid 
		ask_size 
		ask
	; 
run;

data data;
	set data;
	format date date9.;
	date=datepart(quote_datetime);
run;

/* Step 1) filter out	
	- large volumes > 10 mio
	- negative or 0 volumes
	- negative or 0 prices
*/
data step1;
	set data;
	if trade_size>10000000 or trade_size<=0 or trade_price>10000000 or trade_price<=0 then delete;
run;

/* Step 2) Check for multiple underlying_symbols for the same root
	- If the same root, for the same strike, option type, and ex-date has multiple underlying_symbols, filter out those that have fewer observations and 
	use an alphabetical sort as a tie breaker.*/

proc sql;
	create table step2_key as
	select distinct datepart(quote_datetime) as date format=date9., root, expiration, strike, option_type, underlying_symbol,  count(*) as nobs
	from  step1
	group by datepart(quote_datetime), root, expiration, strike, option_type, underlying_symbol ;
quit;

proc sort data=step2_key; by date root expiration strike option_type descending nobs underlying_symbol; run;

proc sort data=step2_key out=step2_key_noDupl nodupkey; by date root expiration strike option_type; run;

proc sql;
	create table step2 as
	select s1.*
	from step1 s1, step2_key_noDupl k
	where datepart(s1.quote_datetime)=k.date and s1.root=k.root and s1.expiration =k.expiration 
		and s1.strike =k.strike and s1.option_type =k.option_type and s1.underlying_symbol = k.underlying_symbol
	;
quit;

/* Step 3)	Filter out other duplicates
	- Before Aug 3, 2015 (quote_datetime sequence_number root expiration strike option_type) should be a distinct key for each observation (including canceled ones)*/

data step2_15a step2_15b;
	set step2;
	if datepart(quote_datetime)>='03Aug2015'd then output step2_15b;
	else output step2_15a;
run;

proc sort data=step2_15a; by quote_datetime sequence_number root expiration strike option_type descending canceled_trade_condition_id; run; /*so that the 0 is deleted in the next step*/

proc sort data=step2_15a out=step3_15a nodupkey; by quote_datetime sequence_number root expiration strike option_type; run;

/* Step 4) Filter out canceled trades and cancel message */
data data_clean;
	set step3_15a step2_15b;
	if canceled_trade_condition_id>=1 then delete;
run;

/*For reverse tick test*/
/* Get first different trade price from succeeding trade of option series across all exchanges*/
proc sort data= data_clean; by root expiration strike option_type desending date desending quote_datetime desending sequence_number;run;

data data_clean;
	set data_clean;
	by root expiration strike option_type;
	
	retain _trade_price;
	retain _trade_price_lead;

	if first.option_type then price_all_lead=.;  
	else if trade_price ^= _trade_price then price_all_lead= _trade_price;
	else price_all_lead= _trade_price_lead; 
			
	_trade_price = trade_price;
	_trade_price_lead = price_all_lead;
		
	drop _trade_price _trade_price_lead;
run;

/* Implementation of recommended trade classification rule 

Note: 	The classification rules are based on the comparison of trade prices and sizes with quote data or preceding/succeding trade price information. 
		To account for the numeric representation and precision of SAS potentially affecting such comparisons, we add a small epsilon (e) to deal with 
		values that are almost, but not exactly equal. We therefor replace "x<y" with "x<=y-e" and "x=y" with "abs(x-y)<=e".
*/

data recommended_rule;
	set data_clean;
	call streaminit(123); /* seed to initialize stream of pseudorandom numbers */

	/* refined trade size rule: Applied to standard single-leg trades (trade condition id 0 and 18, see footnote 9 in the paper) for which the trade price is not outside the bid-ask spread. 
	   Classifies trades for which the trade size is equal to the quoted bid (ask) size as buys (sells).*/
	if 		bid_size ne . and ask_size ne . and trade_size ne . and trade_price ne . and ask ne . and bid ne . and bid <= ask -0.00001 and
			trade_price <= ask + 0.00001 and trade_price >= bid -0.00001 and trade_condition_id in (18, 0) and 
			abs(trade_size - bid_size)<= 0.00001 and abs(trade_size - ask_size)>= 0.00001 then rule = 1; 
	else if bid_size ne . and ask_size ne . and trade_size ne . and trade_price ne . and ask ne . and bid ne . and bid <= ask -0.00001 and 
			trade_price <= ask + 0.00001 and trade_price >= bid -0.00001 and trade_condition_id in (18, 0) and
			abs(trade_size - ask_size)<= 0.00001 and abs(trade_size - bid_size)>= 0.00001 then rule = -1; 

	/* quote rule applied to BBO (from trading venue): classifies a trade as a buy (sell) if its trade price is above (below) the midpoint of the bid and ask spread.*/
	else if bid ne . and ask ne . and bid <= ask -0.00001 and trade_price ne . and trade_price >= (ask + bid)/2 + 0.00001 then rule= 1; 
	else if bid ne . and ask ne . and bid <= ask -0.00001 and trade_price ne . and trade_price <= (ask + bid)/2 - 0.00001 then rule= -1;

	/* quote rule applied to NBBO: classifies a trade as a buy (sell) if its trade price is above (below) the midpoint of the bid and ask spread.*/
	else if best_bid ne . and best_ask ne . and best_bid <= best_ask -0.00001 and trade_price ne . and trade_price >= (best_ask + best_bid)/2 + 0.00001 then rule= 1; 
	else if best_bid ne . and best_ask ne . and best_bid <= best_ask -0.00001 and trade_price ne . and trade_price <= (best_ask + best_bid)/2 - 0.00001 then rule= -1;

	/* depth rule: classifies a midspread trade as a buy (sell) if the quoted ask (bid) size exceeds the bid (ask) size.*/
	else if ask_size ne . and bid_size ne . and bid_size <= ask_size -0.00001 then rule = 1;
	else if ask_size ne . and bid_size ne . and bid_size >= ask_size +0.00001 then rule = -1; 

	/* reverse tick test (applied to trade prices across all option exchanges): classifies a trade as a buy (sell) if its trade price is above (below) the closest different price of a following trade.*/
	else if trade_price ne . and price_all_lead ne . and price_all_lead + 0.00001 <= trade_price then rule=1;
	else if trade_price ne . and price_all_lead ne . and price_all_lead - 0.00001 >= trade_price then rule=-1; 

	/* random assignment if non of the previous rules is applicable.*/ 
	else rule= 2 * round(rand("Uniform")) - 1; 

run;

/***********************************************************************/
