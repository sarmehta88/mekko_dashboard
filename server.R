library(shiny)
library(shinyjs)
library(plyr)
library(dplyr)
library(tidyr)
## Compatibility change for tidyr
if(packageVersion("tidyr") >= "0.4.0") gather = function(...) tidyr:::gather(..., factor_key = TRUE)
library(grid)
library(ggvis)
library(scales)
library(reshape)
library(stringr)
library(ggplot2)
library(RMySQL)
library(data.table)
library(DT)

shinyServer(function(input, output, session) {
  # end all mysql connections for the user
  all_cons <- dbListConnections(MySQL())
  for(con in all_cons){
    dbDisconnect(con)
  }
  
  # Add code to use Node 136 if 32 is down. Use try/catch for the dbConnect- had this code in another version 
  # start a new memsql connection for the user of this session
  # memdb = dbConnect(MySQL(), user= 'tableau1',password = 'tableau1pw',dbname='merged_mekko_partner1', host='10.155.96.32', port= 3307)
  # memdb = dbConnect(MySQL(), user= 'root', password= 'mem$ql123',dbname='merged_mekko_partner1', host='10.155.96.136', port= 3306)
  # memdb2 = dbConnect(MySQL(), user= 'root',password= 'mem$ql123',dbname='merged_mekko_partner1', host='10.155.96.136', port= 3306)
  # memdb3 = dbConnect(MySQL(), user= 'root',password= 'mem$ql123',dbname='merged_mekko_partner1', host='10.155.96.136', port= 3306)
  
  # DATABASE CONNECTIONS
  memdb = dbConnect(MySQL(), user= 'tableau1',password = 'tableau1pw',dbname='merged_mekko_partner1', host='10.155.96.32', port= 3307)
  memdb2 = dbConnect(MySQL(), user= 'tableau1',password = 'tableau1pw',dbname='merged_mekko_partner1', host='10.155.96.32', port= 3307)
  memdb3 = dbConnect(MySQL(), user= 'tableau1',password = 'tableau1pw',dbname='merged_mekko_partner1', host='10.155.96.32', port= 3307)
  
  # DETAILED DATABASE TABLE- which is the Original Mekko table
  # Mekko_Q3FY17- this table is used to calculate the static, bic, unc wallet at SalesRepName, SAV Group Name, and levels further down
  
  #  AGGREGATED DATABASE TABLES
  # 1) Agg_SL1_to_SL3_Q*FY**: table used to calculate the static, bic, unc wallet at the TOP, LEVEL-2, LEVEL-3
  # VerticalMarket	varchar(50)	YES			
  # SAV_Segment	varchar(50)	YES			
  # LEVEL_1	varchar(300)	YES			
  # LEVEL_2	varchar(300)	YES			
  # LEVEL_3	varchar(300)	YES			
  # Technology	varchar(70)	YES			
  # TechnologyGroup	varchar(10)	YES			
  # BE_GEO_ID	int(11)	YES	MUL		
  # Partner_Name	varchar(500)	YES			
  # BE_ID	int(11)	YES			
  # PTNR_NAME_ID	varchar(300)	YES	MUL		
  # FY	varchar(5)	YES	MUL		
  # Wallet	int(11)	YES			
  # BIC_WS_Opportunity	int(11)	YES			
  # Static_WS_Opportunity	int(11)	YES			
  # Value	int(11)	- this is the % of the static that is High end products
  # Volume	int(11)	- this is the % of the static that is Low end products		
  # NC	int(11)	- this is the % of the static that is Not captured products

  
  # 2) Agg_SL3_to_SL6_Q*FY**: table used to calculate the static, bic, unc wallet at Level-3 to Level-6, just before Acct Managers
  # VerticalMarket	varchar(50)	YES			
  # SAV_Segment	varchar(50)	YES			
  # LEVEL_3	varchar(300)	YES			
  # LEVEL_4	varchar(300)	YES			
  # LEVEL_5	varchar(300)	YES			
  # LEVEL_6	varchar(300)	YES			
  # Technology	varchar(70)	YES			
  # TechnologyGroup	varchar(10)	YES			
  # BE_GEO_ID	int(11)	YES	MUL		
  # Partner_Name	varchar(500)	YES			
  # BE_ID	int(11)	YES			
  # PTNR_NAME_ID	varchar(300)	YES	MUL		
  # FY	varchar(5)	YES	MUL		
  # Wallet	int(11)	YES			
  # BIC_WS_Opportunity	int(11)	YES			
  # Static_WS_Opportunity	int(11)	YES			
  # Value	int(11)	YES			
  # Volume	int(11)	YES			
  # NC	int(11)	YES			
  
  
  # 3) Mekko_Account_Counts_Q*FY**- aggregated table used to get the number of SAV accounts and/or number of sites -  doesnt contain info at the SAV Group Name level
  # SAV_Segment	varchar(50)	YES			
  # SalesRepName	varchar(300)	YES			
  # VerticalMarket	varchar(50)	YES			
  # LEVEL_6	varchar(300)	YES			
  # LEVEL_5	varchar(300)	YES			
  # LEVEL_4	varchar(300)	YES			
  # LEVEL_3	varchar(300)	YES			
  # LEVEL_2	varchar(300)	YES			
  # LEVEL_1	varchar(300)	YES			
  # cnt_savs	int(11)	- this is the number of distinct savids at a given level
  # Count_Accounts	int(11)	- this is the count of distinct sites for unnamed and prospect levels which occurs in sales level 3 and down		
  
  # 4) cnt_partners - aggregated table used to calculate the count distinct Partners; script to create this table is under 
  # file name create_indices_memsql.txt. Run this script everytime you have a new mekko file loaded to memsql (new Quarter)
  # screenlevel_num	bigint(20)	- this is the current level number we are calculating the count distinct partners		
  # curr_levels	varchar(300)	- this is the current sales level string		
  # break_levels	text	- this is the next sales levels or the xcategories in the current level		
  # FY	varchar(5)	YES			
  # cnt_partners	bigint(21)	- this is the count distinct partners		
  # view_type	varchar(15)	- this is filter that determines if you are in Sales Hierarchy, Verticals, or SAV Segments view			
  
  # 5) Download_for_mekko_Q*F**- this table is called when the high/low end product is shown and contains info about the 
  # Technologies, SAVID, Product family, Bookings for a given SAV Group Name
  # SAV_Name	varchar(200)	NO			
  # SAV_ID	int(11)	NO			
  # Technology	varchar(45)	NO			
  # BUSINESS_UNIT_ID	varchar(200)	NO			
  # BUSINESS_UNIT_DESCRIPTION	varchar(200)	NO			
  # PRODUCT_FAMILY_ID	varchar(100)	NO			
  # Bookings_L4Yrs_HE	int(11)	NO			
  # Bookings_L4Yrs_LE	int(11)	NO			
  # Bookings_L4Yrs_NC	int(11)	NO			
  # Bookings_CurrYr_HE	int(11)	NO			
  # Bookings_CurrYr_LE	int(11)	NO			
  # Bookings_CurrYr_NC	int(11)	NO			
  
  # load the product famimly csv file into a data table
  #csvdata <- fread(input = "Product_family_pct_Q3FY17_csv.csv", na.strings = "NULL")
  csvdata = read.table("Product_family_pct_Q3FY17_csv.csv", header = TRUE, sep = ",")
  
  # initialize the aggregated table names in order to get the most recent version, since we have multiple versions of the table with different Quarter, FY
  query <- dbSendQuery(memdb,"SELECT TABLE_NAME 
                              FROM information_schema.TABLES
                              WHERE TABLE_NAME LIKE '%Mekko_Acc%'
                              ORDER BY create_time DESC
                              LIMIT 1")
  table_of_accts = fetch(query, n=-1)
  # get the most recent SL1- SL3 aggreg table
  query <- dbSendQuery(memdb,"SELECT TABLE_NAME 
                              FROM information_schema.TABLES
                              WHERE TABLE_NAME LIKE '%Agg_SL1_to_SL3%'
                              ORDER BY create_time DESC
                              LIMIT 1")
  table_of_sl1_3 = fetch(query, n=-1)
  
  # get the most recent SL3- S6 aggreg table
  query <- dbSendQuery(memdb,"SELECT TABLE_NAME 
                       FROM information_schema.TABLES
                       WHERE TABLE_NAME LIKE '%Agg_SL3_to_SL6%'
                       ORDER BY create_time DESC
                       LIMIT 1")
  table_of_sl3_6 = fetch(query, n=-1)
  
  # get the most recent detailed, original mekko table. There are different versions in the database
  query <- dbSendQuery(memdb,"SELECT TABLE_NAME 
                       FROM information_schema.TABLES
                       WHERE TABLE_NAME LIKE '%Mekko_Q%'
                       ORDER BY create_time DESC
                       LIMIT 1")
  table_mekko = fetch(query, n=-1)
  
  # send a query to get all the FYs for the FY dropdown
  query <- dbSendQuery(memdb,sprintf("select FY from %s group by 1 order by FY DESC", table_of_sl3_6)) # biggest year first, which is the selected
  fys = fetch(query, n=-1)
  fields = fys 
  # Append the Prefix FY to each year
  fields$FY = paste0("FY", fields$FY)
  
  
  # helper function that proper cases the dfm1 like US_COMMERCIAL becomes US Commercial
  simpleCap <- function(x) {
      # remove the BE GEO ID in the xaxis labels
      x1 <- gsub(" \\(BE-G.*","",x)
      # remove punctuation and replace with space
      removed_punct_x = gsub("[[:punct:]]", " ", x1)
      st <- strsplit(removed_punct_x, " ")[[1]]
      # if number of chars in split string- first token- is less than 4 chars, then  keep each token as is
      new_str = ""
      for(i in st){
        if(nchar(i) <4){
          new_str = paste(new_str,i)
        }else{
          prop = paste(substring(i, 1, 1), tolower(substring(i, 2)),sep="", collapse=" ")
          new_str = paste(new_str,prop)
        }
      }
      trimws(new_str)
  }
  
  # help function to remove duplicate words found in the dataframe column containing the x_categories
  shorten.labels <- function(dfm1.copy, thresh=3){
    print("Shorten Labels...")
    # first get unique labels from the column in data frame
    # threshold is the number of times we see a duplicate token in the labels before we remove it
    # ie. Emear south, Emear north, Emear East becomes south, north, east
    df = dfm1.copy[dfm1.copy$variable=="SWOpct",]$cleanx
    # create one long string from the dataframe column containing text
    df.strings = paste(df, collapse=" ")
    # split the string on whitespace to get tokens and store in a one level list
    d <- unlist(strsplit(df.strings, split=" "))
    
    dat1 = NULL
    # while dat1 has empty columns, keep changing the word count threshold until results(dat1) have no missing data
    while (length(unique(dat1[,1])) !=  length(df)) {
      # get the duplicated words- words that appear at least 3 times or words that occur in all rows
      dw = NULL
      for(word1 in d){ 
        word = gsub( " *\\(.*?\\) *", "", word1)
        wc = length(grep(paste("\\b",word,"\\b",sep=""), d))
        # if word occurs 3+ times or word occurs in all segments given the number of segments is greater than 1, 
        # then store this word in duplicate word dw list
        if (wc >=thresh || (wc == length(df) & length(df)>1)){ 
          dw = c(dw, word)
        }
      }
     
      # query the dataframe to remove duplicated words from text column
      dat1 <- as.data.frame(sapply(dfm1.copy$cleanx, function(x) 
        gsub(paste0("\\b", paste(dw, collapse = '\\b|\\b'),"\\b"), '', x)))
      
      thresh= thresh +1
    } # end while
    
    return(dat1[,1])
  } #end shorten.labels
  
  # helper function used in Technology view to remove redundant words such as Collaboration A, Collaboration B, which then becomes A, B
  remove_first_word <- function(dfm1.copy){
    # remove the first word (duplicate word in Tech view)
    dup_words = sapply(strsplit(dfm1.copy$cleanx, ","), `[`, 1) # get the first word of each x-category
    dup_word = dup_words[duplicated(dup_words)] # get the real duplicated words based on the first words
    dfm1.copy$cleanx <- gsub(paste0(dup_word[1],", "),"",dfm1.copy$cleanx)
    return(dfm1.copy$cleanx)
  } 
  
  # Mapping of the Group by or Mekko Levels with the pcurrent.level. For instance we show Level_1 as x_categoris when pcurrent.level =2
  # and we show LeveL_2 when pcurrent.level (index in this list) = 3
  # ignore the first element since the old vs of mekko used to start at SAV_Segment; that is why we now start at pcurrent.level =2
  mekko.level.vars <- list( "SAV_Segment","Level_1","Level_2","Level_3","Level_4","Level_5","Level_6","SalesRepName","SALES_ACCOUNT_GROUP_NAME","TechnologyGroup","Technology")
  
  # Prettier version of of the above mentioned levels
  mekko.level.str <- list( "Account View Segments","Level 1","Level 2","Level 3","Level 4","Level 5","Level 6","Representative Names","SAV Names","Technology Groups", "Technologies")
 
  # Function that takes in a number and converts into a string that rounds the number to nearest tenth of Billion or nearest whole Million
  num.to.string <- function(dfm1,col.name, unit){
    print("Num to string")
    i <- 1
    if(unit=="B"){ # if units are in Billions
      for (amount in dfm1[,col.name]) {
        if(is.na(amount)){
          dfm1[,col.name][[i]]= paste("NA")
        }else{ # amount is not NA
          num.billions = amount/1000
          if(num.billions < .1){ # if number is less than 100M round to 2 decimal places
            dfm1[,col.name][[i]]= paste(round(num.billions,2),"B",sep="")
          }else{ # round to 1 decimal place of B if number is >= 100M, add the comma format for thousands place
            dfm1[,col.name][[i]]= paste(format(round(num.billions,1),big.mark=",",scientific=FALSE, trim=TRUE),"B",sep="")
          }
        }
        i = i +1
      }# end forloop
    }else if(unit == "K"){ # amount is in millions, need to convert to thousands
      
      for (amount in dfm1[,col.name]) {
        if(is.na(amount)){
          dfm1[,col.name][[i]]= paste("NA")
        }else{ # amount exists
          amount = signif(amount*1000, 3)
          dfm1[,col.name][[i]]= paste(format(amount,big.mark=",",scientific=FALSE, trim=TRUE),"K",sep="")
        }
        i =i +1 #increment loop
      }# end for loop
    }
    else{ # units are in Millions
      for (amount in dfm1[,col.name]) {
        if(is.na(amount)){
          dfm1[,col.name][[i]]= paste("NA")
        }else{ # amount exists
          # if amount is less than 1M, round amount 2 decimal places
          if(amount < 1){
            amount = round(amount, 2)
            dfm1[,col.name][[i]]= paste(amount,"M",sep="")
          }else{ # if amount is >=1M, round to nearest whole Million
            dfm1[,col.name][[i]]= paste(format(round(amount,0),big.mark=",",scientific=FALSE, trim=TRUE),"M",sep="")
          }
        }
        i =i +1 #increment loop
      }# end for loop
    }# end else
    return(dfm1[,col.name])
  }# end function
  
  # Set the reactive variables: default to values of page load
  pv <- reactiveValues(
    pcurrent.level = 2,  # Represents the first mouse click, if any
    # Sel.list stores the filters for the where_clause, used in any sql statement
    # the where_clauses uses the elements in this list to contruct the string "where Level_1 = xx AND"
    sel.list = list(),
    clean.sel.list = list(), # the cleaner string version of the sel.list(), this is used in the arrow header on top of the mekko
    where_clause = "",
    tech_breakdown = 0,
    goback_techgroups = 0,
    tech_grp = c('All Products and Services'), # this is the default selected value on page load
    savseg = c('All SAV Segments'), # this is the default selected value on page load,
    vert = c('All Verticals'),
    technologies = c("All Technologies"),
    no_bic = 0, # turn flag for no bic off, this is for FY 2016 for ex
    partner_tech_grps = 0,
    partner_technologies = 0,
    partner_SAVID = 0,
    is_unnamed_prospects = 0,
    beid = "All PartnerNames (BE-ID)",
    varstring = "",
    table_name = table_of_sl1_3,
    all_years = fields$FY,
    fy = as.numeric(gsub("FY","",fields$FY[1])),
    redraw_without_unk_checkbox = FALSE
  )
  
  # Helper function to calculate number of partners/wallet for a each x_category in a mekko graph
  calculate_num_partner <- function(df){
    print("Calc Num Partners avg- no query")
    # Calculate the ave wallet per account for each segment
    df$avg.acct.wallet =ifelse(df$numaccts ==0.0, 0.0, df$tot.tmp/df$numaccts)
    # If ave wallet is less than .01 M then  convert hose rows to K
    cond = df$avg.acct.wallet < .01
    df$avg.acct.wallet[cond] <- df$avg.acct.wallet[cond] *1000
    df$avg.acct.wallet = num.to.string(df,"avg.acct.wallet", "M") # unit is Millions
    # for the Unnamed accts, turn units to K from M
    df$avg.acct.wallet[cond] <-  gsub('M', 'K', df$avg.acct.wallet[cond])
    return(df)
  }
  
  # Helper function to calculate number of partners/wallet for a each x_category in a mekko graph
  calculate_num_account <- function(df){
    print("calc num accts...")
    # Calculate the ave wallet per account for each segment; tot.tmp is total wallet for that column
    df$avg.acct.walletsav =ifelse(df$numaccts2 ==0.0, 0.0, df$tot.tmp/df$numaccts2)
    # If ave wallet is less than .01 M then  convert hose rows to K
    cond = df$avg.acct.walletsav < .01
    df$avg.acct.walletsav[cond] <- df$avg.acct.walletsav[cond] *1000
    df$avg.acct.walletsav = num.to.string(df,"avg.acct.walletsav", "M") # unit is Millions
    # for the Unnamed accts, turn units to K from M
    df$avg.acct.walletsav[cond] <-  gsub('M', 'K', df$avg.acct.walletsav[cond])
    return(df)
  }
  
  # Helper function which writes the query to get number of Unique Partners or Unique SAV Accts
  create_string <- function(varstring, where_clause, calc_sav, calc_part){
    print("create string .... Query 2  ")
    # if calculate number of Sav or number of sites flag is on, then create the appropriate query
    if(calc_sav){
      # remove the FY from the query since the Number of distinct SAV accts is same for all FYs
      where_clause =  unlist(strsplit(where_clause, "AND FY"))[1]
      where_clause =  unlist(strsplit(where_clause, "AND  FY"))[1] # notice the extra space between 2 words, catch this is it comes up
      # if there is only an FY clause as in current level 2, then remove it
      where_clause =  trimws(unlist(strsplit(where_clause, "FY"))[1])
      # if there is just a  where in the where_clause after removing the FY filter, then append the VerticalMarket filter to the where clause in the below query
      if(where_clause =='where'){
            # get the number of sav accounts for either regular (not unnamed/prospects) levels and number of sites (Count_Accounts) for unnamed/prospects accounts
            qstr = sprintf("select * from (
                       select %s, sum(cnt_savs) as numaccts2,'reg' as type_acct
                       from %s %s
                       VerticalMarket !='' group by %s
                       UNION
                       select %s, sum(Count_Accounts) as numaccts2, 'unnamed' as type_acct
                       from %s %s
                       VerticalMarket = '' group by %s)x group by 1"
                       ,varstring,table_of_accts ,where_clause, varstring,varstring, table_of_accts, where_clause, varstring)
      }
      else{ # there is no where and need to append an AND to include the VerticalMarket filter
            # get the number of sav accounts for either regular (not unnamed/prospects) levels and number of sites (Count_Accounts) for unnamed/prospects accounts
            qstr = sprintf("select * from (
                       select %s, sum(cnt_savs) as numaccts2,'reg' as type_acct
                       from %s %s
                       AND VerticalMarket !='' group by %s
                       UNION
                       select %s, sum(count_Accounts) as numaccts2, 'unnamed' as type_acct
                       from %s %s
                       AND VerticalMarket = '' group by %s)x group by 1"
                       ,varstring,table_of_accts, where_clause, varstring,varstring,table_of_accts, where_clause, varstring)
      }
      
      print("starting CALC number of SAV/Sites QUERY 11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111")
      print(qstr)
      query2 = dbSendQuery(memdb3,qstr)
      
    }
    # if calculate number of partner flag is on, then create the appropriate query
    else if(calc_part){
      print("starting CALC/NUM PArtners QUERY 00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")
       #if current level is before SalesRepName Level and there is no other filters from the dropdowns selects like Techgroup/SAVSeg, then use aggregated count_partners table
       if(pv$pcurrent.level <9 & pv$tech_grp == "All Products and Services" & pv$savseg =="All SAV Segments" & pv$technologies== "All Technologies" & pv$beid == "All PartnerNames (BE-ID)" & pv$vert== "All Verticals" & input$pview != 'Partners'){
          # get the prev_level
          if(pv$pcurrent.level ==2){
                prev_lev = "\"TOP\""
          }
          else{
            tok = unlist(str_split(where_clause, " AND  FY" ))[1]
            # if token contains FY still due to extra space between AND and FY, then use another way to tokenize
            if( grepl("FY", tok)){
              tok = unlist(str_split(where_clause, " AND FY" ))[1]
            }
            # extract from the tok, the last token after AND
            tok2 = tail(unlist(str_split(tok, " AND " )),1)
            if(length(tok2) ==0){
              tok2 = unlist(str_split(tok, " AND " ))[1] # must be the first one, if no more tokens
            }
            prev_lev = unlist(str_split(tok2, " =" ))[2]
          }
          
          # get the number of distinct partners from the aggregated table: break_levels are the x_categories for which we have the count distinct partners
          # screenlevel_num is the pcurrent.level and curr_levels is the variable that maps to pcurrent.level, view_type is either SalesHierarchy, Verticals, or Sav Segments
          # ie. select break_levels as Level_1, cnt_partners as numaccts from cnt_partners where screenlevel_num =2 and curr_level= "TOP" and view_type = "SalesHierarchy"
          qstr =  sprintf("select break_levels as %s, cnt_partners as numaccts
                          from cnt_partners 
                          where screenlevel_num = %s and curr_levels= %s and FY= %s and view_type = '%s' ",varstring ,pv$pcurrent.level, prev_lev, pv$fy, input$pview )
       }
       else{ # current level is on sales level 3+
          qstr = sprintf("select %s, count(distinct BE_GEO_ID) as numaccts from %s %s
                                         group by %s", varstring, pv$table_name, where_clause, varstring)
       }
       print(qstr)
       query2= dbSendQuery(memdb2, qstr)
      
    }
    
    return(query2)
    }# end function
  
  # helper function that calculate what % of the static wallet is value(highend), volume(lowend), and nonclassifed for the x_categories. 
  create_val_vol_rows <- function(trow, dfm3) {
    print("create val_vol....")
    # get the value, vol, unk %
    val_pct = as.numeric(trow['Val_pct'])
    vol_pct = as.numeric(trow['Vol_pct'])
    unk_pct = as.numeric(trow['Unk_pct'])
    ymax = as.numeric(trow['ymax'])
    # calculate the Value which is trow$xmin + .8*trow$xsub
    value.start <- as.numeric(trow['xmin'])
    value.end <- as.numeric(trow['xmin']) + val_pct*as.numeric(trow['xsub'])/100.0
    # calculate the Volume which is trow$xmin + .8*trow$xsub
    vol.start <- value.end
    vol.end <- value.end + vol_pct*as.numeric(trow['xsub'])/100.0
    na.start <- vol.end
    na.end <- as.numeric(trow['xmax'])
    # create columnar vectors for xstart, xend, etc
    xstart = c(value.start, vol.start, na.start)
    xend = c(value.end, vol.end, na.end)
    three_row_append = data.frame(TechnologyGroup = c(trow[1], trow[1], trow[1]) , type = c('Value','Volume','Unk'), pct_Static = c(val_pct,vol_pct, unk_pct),
                                  xs = xstart, xe = xend, ys = c(0,0,0), ye = c(ymax, ymax, ymax))
    
    return(three_row_append)
  }
  
  # The main function that calculates the dimension of the rectangles of mekko graph. Runs sql queries to get the static,bic,uncaptured % and numbers.
  pmekko.blueprint <- function(){
    print("Mekko Blueprint...**********************************************************************************************************************************************************************************************************")
    # print("CURRENT LEVEL **********&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
    # print(pv$pcurrent.level)
    # print("BEID Partner Name")
    # print(input$partner_beid)
    # print("Partner Tech Groups Page")
    # print(pv$partner_tech_grps)
    # print("Partner Technologies")
    # print(pv$partner_technologies)
    # print("Partner SAVID")
    # print(pv$partner_SAVID)
    # print("Input View 1")
    # print(input$pview)
    # print("Input View 2")
    # print(input$pview2)
    # print("**********&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
    # 
    
    # if dfm1 is null- which means it still is being computed, disable the pview and pview2
    #shinyjs::disable('pview')
    
    # Initialize any null dropdowns. This happens when shiny has not reached the dropdown part of the code before executing this function; thus the dropdown values are null.
    if(is.null(isolate(input$pselect_tech_grps))){ # need to isolate this input otherwise, the graph will keep refreshing after each dropdown selection
      pv$tech_grp = c("All Products and Services")
    }
    if(is.null(isolate(input$pselect_savseg))){ # need to isolate this input otherwise, the graph will keep refreshing after each dropdown selection
      pv$savseg = c("All SAV Segments")
    }
    if(is.null(isolate(input$pselect_vertical))){ # need to isolate this input otherwise, the graph will keep refreshing after each dropdown selection
      pv$vert = c("All Verticals")
    }
    if(is.null(isolate(input$pselect_technologies))){ # need to isolate this input otherwise, the graph will keep refreshing after each dropdown selection
      pv$technologies = c("All Technologies")
    }
  
    # if on tech breakdown page, then set the switch to go back to techgroups on
    if(pv$tech_breakdown ==1){
      pv$goback_techgroups = 1
    }else{
      pv$goback_techgroups = 0
    }
    # save the fy in local variable, you can just use pv$fy directly but I do this so it doesn't refresh the functions since this is reactive value
    fy = pv$fy
    
    # Get the pcurrent.level- which is the numerical index that maps to the mekko.level.vars list. Top starts at pcurrent.level =2 which is the 2nd element in the mekko.level.vars
    pcurrent.level = pv$pcurrent.level
    
    #################################### Logic to Choose the Variable to Group the Mekko by wjich are the x_categories in the mekko graph
    ### The varstring will be the group by statement in sql such as Level_1
    
    # pview are the main views you see in SL1 to the end of the mekko
    # pview2 are views you see when you are in the Partner view, which is an radio option of pview
    if(input$pview =='Technologies'){
      varstring = 'TechnologyGroup'
      # if the flag for individual technologies is on, the varstring is Technology instead of TechnologyGroup
      if(pv$tech_breakdown==1 ){
        varstring = "Technology"
      }
    }
    else if(input$pview == "Acct Mngrs"){
      varstring = "SalesRepName"
    }
    else if(input$pview == "SAV Group"){
      varstring = "SALES_ACCOUNT_GROUP_NAME"
    }
    else if((input$pview2 == "Verticals" || input$pview2 == "SAV Segments" ) & input$pview == "Sales Hierarchy"){
      # this is jumping from Verticals in Partner to Partner Names to SLNodes
      varstring = mekko.level.vars[[pcurrent.level]]
    }
    else if((input$pview2 == "Verticals" || input$pview2 == "SAV Segments" ) & input$pview == "SAV Segments"){
      # this is jumping from Verticals in Partner to Partner Names to SLNodes
      varstring = "SAV_Segment"
    }
    else if((input$pview2 == "Verticals" || input$pview2 == "SAV Segments" ) & input$pview =="Verticals"){
      # this is jumping from Verticals in Partner to Partner Names to SLNodes
      varstring = "VerticalMarket"
    }
    else if(input$pview2 == "Verticals" & pv$partner_SAVID== 1){ 
      varstring = "VerticalMarket"
    }
    else if(input$pview2 == "SAV Segments" & pv$partner_SAVID== 1){ 
      varstring = "SAV_Segment"
    }
    else if(input$pview2 == "Verticals" & pv$partner_tech_grps == 0){ # this is jumping from Verticals in Partner to another Sales Level, then go to Partner_Names for that SL
      varstring = "Partner_Name"
    }
    else if(input$pview2 == "SAV Segments" & pv$partner_tech_grps == 0){ # this is jumping from Verticals in Partner to another Sales Level, then go to Partner_Names for that SL
      varstring = "Partner_Name"
    }
    else if(input$pview2 == "Verticals" & input$pview =="Partners"){
      varstring = "VerticalMarket"
    }
    else if(input$pview2 == "SAV Segments" & input$pview =="Partners"){
      varstring = "SAV_Segment"
    }
    else if(pv$partner_SAVID == 1){
      varstring = "SAVID"
    }
    else if(pv$partner_tech_grps == 1){
      varstring = "TechnologyGroup"
    }else if(pv$partner_technologies == 1){
      varstring = "Technology"
    }
    else if(input$pview == "Partners"){ # not in techgroup or technologies page
      varstring = "Partner_Name"
    }
    else if(input$pview =='SAV Segments'){
      varstring = 'SAV_Segment'
    }else if(input$pview =='Verticals'){
      varstring = 'VerticalMarket'
    }
    else if(pcurrent.level ==11){
      varstring = "Technology"
    }
    else{
      varstring = mekko.level.vars[[pcurrent.level]]
    }
  
    pv$varstring = varstring
    
    
    ################## Start Building the Where Clause used in the sql queries
  
    #### 1)  create where_clause strings for All the dropdowns ie. Technology Group, SAV Segment, Verticals, and Technologies
    ####     This will be appended at the end of where_clause after the main filter(same as arrow header)are added.
    ####     The tech_grp_str contains part of the where_clause for all dropdown selections: TechnologyGroup, Verticals, SAV Segments, and individual Technologies
    
    # if no Techgrps is chosen in the landing page, then choose ALL TechGrps
    if(any(pv$tech_grp == "All Products and Services") || is.null(pv$tech_grp)){
      tech_grp_str = ""
    }else{  # otherwise, there are Techgroups selected from the dropdown, build a where_clause like 
            # where FY= 2018 AND (TechnologyGroup = 'Collab' OR TechnologyGroup = 'DC'  OR TechnologyGroup = 'Switches')
      # if All Products in the list, then simply exclude Services 
      tech_grp_str =  ""
      ctg = 1 # this is flag for the first element selected in this multiple selection dropdown list; first element is joined by AND, following elements are joined by OR
      for(tg in pv$tech_grp){
        if(ctg ==1){
          if(tg =='All Products'){
            tech_grp_str =  paste0(tech_grp_str," AND (TechnologyGroup != 'SERVICES'")
          }else{
            tech_grp_str =  paste0(tech_grp_str," AND (TechnologyGroup = ","\"",tg, "\"")
          }
        }else{
          if(tg =='All Products'){
            tech_grp_str =  paste0(tech_grp_str," OR TechnologyGroup != 'SERVICES'")
          }else{
            tech_grp_str =  paste0(tech_grp_str," OR TechnologyGroup = ","\"",tg, "\"")
          }
        }
        ctg = ctg +1
      }
      # add the closing parenthesis and append to tech_grp_str
      tech_grp_str =  paste0(tech_grp_str,")")
    }
    
    # if no SAV Segments are chosen in landing page, then choose ALL
    if(any(pv$savseg == "All SAV Segments") || is.null(pv$savseg)){
      savseg_str = ""
    }
    else{  # Sav Segements are chosen from the dropdown
      savseg_str =  ""
      ctg = 1
      for(tg in pv$savseg){
        if(ctg ==1){
            savseg_str =  paste0(savseg_str," AND (SAV_Segment = ","\"",tg, "\"")
          
        }else{
            savseg_str =  paste0(savseg_str," OR SAV_Segment = ","\"",tg, "\"")
          
        }
        ctg = ctg +1
      }
      # add the closing parenthesis and append to tech_grp_str
      tech_grp_str =  paste(tech_grp_str, savseg_str, ")")
    }
    
    # if no Verticals are chosen in landing page, then choose ALL
    if(any(pv$vert == "All Verticals") || is.null(pv$vert)){
      vert_str = ""
    }
    else{  
      vert_str =  ""
      ctg = 1
      for(tg in pv$vert){
        if(ctg ==1){
          vert_str =  paste0(vert_str," AND (VerticalMarket = ","\"",tg, "\"")
          
        }else{
          vert_str =  paste0(vert_str," OR VerticalMarket = ","\"",tg, "\"")
          
        }
        ctg = ctg +1
      }
      # add the closing parenthesis
      tech_grp_str =  paste(tech_grp_str, vert_str, ")")
    }
    
    # if no Technologies are chosen in landing page, then choose ALL
    if(any(pv$technologies == "All Technologies") || is.null(pv$technologies)){
      techn_str = ""
    }
    else{  
      techn_str =  ""
      ctg = 1
      for(tg in pv$technologies){
        if(ctg ==1){
          techn_str =  paste0(techn_str," AND (Technology = ","\"",tg, "\"")
          
        }else{
          techn_str =  paste0(techn_str," OR Technology = ","\"",tg, "\"")
          
        }
        ctg = ctg +1
      }
      # add the closing parenthesis
      tech_grp_str =  paste(tech_grp_str, techn_str, ")")
    }
     
    
    ############ 2) create the Main Where_Clause that is based on the elements in the sel.list()
    # pcurrent.level =2 is the TOP, meaning no filters
    # create a string of all the selected filters - the WHERE clause- only applicable after SAV segment view
    # Everytime a user double clicks, the token is stored in sel.list() and if the user selects from a dropdown, then the filter value is
    # stored in pv$sel.dropdown1
    # Sel.list() only stores the full set of values of the filter such as c("AMERICAS", "US COMMERCIAL", ..) logic is used to deduce the filter variable
    if(pcurrent.level > 2){
      # Create the where statement which concats all the previous selected filters 
      upto = pcurrent.level -1
      if(upto >=9){ # manually insert the selections for pcurrent.level 9 or upto level 8
        upto = 8
      }
      filter_list = list()
      for (i in 2:upto) {
        seg_level = mekko.level.vars[[i]] # this is the mapping between the pcurrent.level and the actual variable that represents that level, ie. "LEVEL_1"
        seg_value = pv$sel.list[[i]] # ex. "AMERICAS", this contains the actual token
        f_string = paste(seg_level, " = ","\"" ,seg_value,"\"", sep = "") # need to escape quotes with backslash
        filter_list[i] = f_string
      }
      if(pcurrent.level ==9){
        filter_list[8] = paste0("SalesRepName" ," = ", "\"" ,pv$sel.dropdown1,"\"") 
      }
      else if(pcurrent.level ==10){
        
        # if not in Partners view, use dropdown value
        # if in Partners view, use sel.list value
        filter_list[9] = paste0("SALES_ACCOUNT_GROUP_NAME" ," = ", "\"", pv$sel.list[[9]],"\"")
        
      } 
      if(pv$goback_techgroups == 0 & pcurrent.level >=9 ){
        filter_list[8] = paste0("SalesRepName" ," = ", "\"" ,pv$sel.list[[8]],"\"")
      }
      # Replace the last item in list with TechnologyGroup if in techbreakdown page
      if(pv$tech_breakdown == 1 || pcurrent.level>=11){
        #last page
        if(pcurrent.level ==11){
          filter_list[8] = paste0("SalesRepName" ," = ", "\"" ,pv$sel.list[[8]],"\"")
          filter_list[9] = paste0("SALES_ACCOUNT_GROUP_NAME" ," = ", "\"", pv$sel.list[[9]],"\"")
          # if on last page, click the Others category in technology group, then add the list of techs in others
          if(pv$sel.list[[10]] == "ETC"){
            filter_list[10] <- paste0("TechnologyGroup IN ('", paste(pv$others,collapse = "','"),"')")
            
          }
          else{
            filter_list[10] <- paste0("TechnologyGroup" ," = ", "\"" , pv$sel.list[[10]],"\"")
          }
        }
        else if(pcurrent.level ==12 & ( pv$partner_tech_grps == 1 || pv$partner_technologies==1)){
          
          filter_list[9] = paste0("SALES_ACCOUNT_GROUP_NAME" ," = ", "\"", pv$sel.list[[9]],"\"")
          filter_list[10] = paste0("Partner_Name" ," = ", "\"", pv$sel.list[[10]],"\"")
          
          # if on last page, click the Others category in technology group, then add the list of techs in others
          if(pv$sel.list[[11]] == "ETC"){
            filter_list[11] <- paste0("TechnologyGroup IN ('", paste(pv$others,collapse = "','"),"')")
            
          }
          else{
            filter_list[11] <- paste0("TechnologyGroup" ," = ", "\"" , pv$sel.list[[11]],"\"")
          }
        }
        else if(pcurrent.level ==13 & ( pv$partner_technologies==1)){
          filter_list[9] = paste0("SALES_ACCOUNT_GROUP_NAME" ," = ", "\"", pv$sel.list[[9]],"\"")
          filter_list[10] = paste0("Partner_Name" ," = ", "\"", pv$sel.list[[10]],"\"")
          filter_list[11] = paste0("Sales_Account_ID_INT" ," = ", "\"", pv$sel.list[[11]],"\"")
          filter_list[12] = paste0("TechnologyGroup" ," = ", "\"", pv$sel.list[[12]],"\"")
        }
        
        else{ # not last page, so replace the last item in sel list
          
          if(pcurrent.level== 10){
            seg_value = pv$sel.list[[length(pv$sel.list)]] #get the last item in list which is the technology group selected
          }
          if(seg_value== "ETC" & input$pview == 'Technologies'){
            filter_list[length(filter_list)] <- paste0("TechnologyGroup IN ('", paste(pv$others,collapse = "','"),"')")
          }else{
            filter_list[length(filter_list)] <- paste0("TechnologyGroup" ," = ", "\"" , seg_value,"\"")
          }
        }
      }
      joined_string = paste(filter_list[-1], collapse = " AND ") # dont start at sav segment
      where_clause = paste(joined_string, " AND FY = ", fy)
      where_clause = paste("where",where_clause) # this is reactive
    }
    else{ # if pcurrent.level ==2
      where_clause = paste("where FY = ", fy, " ", tech_grp_str)
    }
    
    
    ############################## After configuring where_clause, determine the appropriate table_name
    
    # if SAVID or SalesRepName is in where_clause,  table name is the original mekko table
    if(grepl("Sales_Account_ID_INT", where_clause) | grepl("Level_6", where_clause) | grepl("Sales_Account_Group_Name", where_clause)){
        table_name = table_mekko
    }else if(grepl("Level_3", where_clause) ){
        table_name = table_of_sl3_6
    }else{
      table_name = table_of_sl1_3
    }
    
    pv$table_name = table_name
    print("TABLE NAME IN MEKKO BLUEPRINT")
    print(table_name)
    if( table_name == table_of_sl3_6 ){
      # skip the first 2 levels since they are not in the dB table
      where_list = unlist(strsplit(where_clause," AND"))
      where_clause <- paste( paste0("where" , where_list[3]), "AND" ,paste(where_list[4:length(where_list)], collapse = " AND "))
    }
    
    
    ########################### Change Where statements in Partner Views
    # if in Partner SAVID view or  Partner Tech Group or Partner Technonologies, then remove extra (empty) filters
    # this is due to current level not matching up with the number of where params in the Partner View
    if( pv$partner_SAVID ==1 || pv$partner_tech_grps == 1 || pv$partner_technologies == 1){
      # If in Partner SAVID view, remove the last Sales Level and replace with Partner_Name Param
      if(pv$partner_SAVID == 1){
        where_list = unlist(strsplit(where_clause," AND"))
        second_last = where_list[length(where_list)-2]
        remove_where = unlist(strsplit(second_last,"where")) # remove the where
        second_last <- remove_where[length(remove_where)]
        token_to_replace = unlist(strsplit(second_last,"="))[1]
        new_replace = gsub(token_to_replace, " Partner_Name ", second_last)
        # replace this string back in where list
        where_clause <- gsub(second_last, new_replace,where_clause)
        # add FY and Tech group at the very end of where clause
        where_clause = paste(where_clause, tech_grp_str )
      }
      # if in Partner Tech Grp view, change last 2 params in where clause to Partner_Name and TechnologyGroup
      else if(pv$partner_tech_grps == 1){
        where_list = unlist(strsplit(where_clause," AND"))
        second_last = where_list[length(where_list)-1] # this will be SAVID
        third_last = where_list[length(where_list)-3] # this will be Partner_Name
        remove_where = unlist(strsplit(third_last,"where")) # remove the where
        third_last <- remove_where[length(remove_where)]
        sec_token_to_replace = unlist(strsplit(second_last,"="))[1]
        third_token_to_replace = unlist(strsplit(third_last,"="))[1]
        sec_new_replace = gsub(sec_token_to_replace, " Sales_Account_ID_INT ", second_last)
        third_new_replace = gsub(third_token_to_replace, " Partner_Name ", third_last)
        # replace this string back in where list
        where_clause <- gsub(third_last, third_new_replace,where_clause)
        where_clause <- gsub(second_last, sec_new_replace,where_clause)
        # add FY and Tech group at the very end of where clause
        where_clause = paste(where_clause, tech_grp_str )
      }
      else{ # in Partner Technologies view, change last 3 params in where clause to 
        where_list = unlist(strsplit(where_clause," AND"))
        # switch the token before '=' with the right Category
        delete_this = where_list[length(where_list)-1] # this will be removed
        switch_to_tg = where_list[length(where_list)-2] # this will be TechGroup
        switch_to_savid = where_list[length(where_list)-3] # this will be SAVID
        switch_to_pn = where_list[length(where_list)-5] # this will be Partner Name
        
        where_clause <- gsub(delete_this, "", where_clause)
        where_clause <- gsub(unlist(strsplit(switch_to_tg,"="))[1], " TechnologyGroup ", where_clause)
        where_clause <- gsub(unlist(strsplit(switch_to_savid,"="))[1], " Sales_Account_ID_INT ", where_clause)
        where_clause <- gsub(unlist(strsplit(switch_to_pn,"="))[1], " Partner_Name ", where_clause)
        # remove the extra AND
        where_clause <- gsub("AND AND","AND",where_clause)
        # if no where, then add to the start
        if(grepl('where', where_clause) ==0){
          where_clause <- paste("where", where_clause)
        }
        # add FY and Tech group at the very end of where clause
        where_clause <- paste(where_clause, tech_grp_str)
      }
      
    }
    # add the beid techgroup selection to the where_clause if selected
    if(input$partner_beid != "All PartnerNames (BE-ID)" && !is.null(input$partner_beid)){
      beid_str = paste0(" AND PTNR_NAME_ID = \'", input$partner_beid,"\'")
      where_clause <- paste(where_clause, beid_str)
    }
    
    # add the savsegment selection to the where_clause if selected
    if(input$partner_savsegment != "All SAV Segments" && !is.null(input$partner_savsegment)){
      savseg_str = paste0(" AND SAV_Segment = ", "\"",input$partner_savsegment,"\"")
      where_clause <- paste(where_clause, savseg_str)
    }
    
    
    # After changing the where_clause for the partner view, sumbit the appropriate query to get the static, bic, and uncaptured wallet numbers and %ages
    
    # if in Partner SAVID Level and in SAV Segments and Verticals view, then do a simple group by to get the static,bic,and unc wallet.
    if(pv$partner_SAVID ==1 ){
          if(input$pview2 == 'SAV Segments' || input$pview2 == 'Verticals'){
          
            query1_1 <- dbSendQuery(memdb,sprintf("select %s,sum(Static_WS_Opportunity)/1e6 as SWO, sum(BIC_WS_Opportunity-Static_WS_Opportunity)/1e6 as AdditionalBIC,
                                                  sum(Wallet - BIC_WS_Opportunity)/1e6 as Uncaptured,
                                                  sum(Wallet)/1e6 as Total from %s %s group by %s",varstring, table_name,where_clause, varstring))
            
            cdata = fetch(query1_1, n=-1)
          }
          else{ # if in Partner SAVID Level and the main view, then get staic,bic,and unc for the top 10 SAVIDs with highest wallet, the next 40, and the rest
            
            query_str = sprintf("select SAVID2 as SAVID, sum(SWO)/1e6 as SWO,                          
                                sum(AdditionalBIC)/1e6 as AdditionalBIC,                          
                                sum(Uncaptured)/1e6 as Uncaptured,  
                                sum(Total)/1e6 as Total 
                                from(
                                select case
                                when rank() over(order by Sum(Wallet) DESC) <11 then CONCAT(Sales_Account_Group_Name,' (SAVID: ', Sales_Account_ID_INT, ')') 
                                when rank() over( order by sum(Wallet) DESC) <51 then 'Next Top 40'
                                else 'Rest'
                                end AS SAVID2, 
                                sum(Static_WS_Opportunity) as SWO,                          
                                sum(BIC_WS_Opportunity-Static_WS_Opportunity) as AdditionalBIC,                          
                                sum(Wallet - BIC_WS_Opportunity) as Uncaptured,  
                                sum(Wallet) as Total, rank() over(order by sum(Wallet) DESC) as rank 
                                from %s                       
                                %s
                                group by Sales_Account_ID_INT 
                                order by Rank 
                                )x
                                group by SAVID2
                                order by Total Desc", table_mekko, where_clause)
             
            
            query1 <- dbSendQuery(memdb,query_str)
            cdata1 = fetch(query1,n=-1)
            cdata = na.omit(cdata1) # get rid of NA values
            # move the Next 40, Rest to back of the df since it is originally at the start of the df
            # to do this, subset the data frame to remove the Rest and Next top 40 (top 2 rows) and append to back of the dataframe
            sub.40 = subset(cdata, SAVID == "Next Top 40")
            sub.rest = subset(cdata, SAVID == "Rest")
            cdata <- subset(cdata, SAVID != "Next Top 40" & SAVID != "Rest")
            cdata <- rbind(cdata, sub.40, sub.rest)
          }
    }
    # if not in Partner SAVID level but in Partner Technologies/Tech Groups level, then simply get the static,bic, and unc wallet at this level
    else if(pv$partner_tech_grps == 1 || pv$partner_technologies == 1){
          # Now write the query to get the techs or techgroups
          query1_1 <- dbSendQuery(memdb,sprintf("select %s,sum(Static_WS_Opportunity)/1e6 as SWO, sum(BIC_WS_Opportunity-Static_WS_Opportunity)/1e6 as AdditionalBIC,
                                                sum(Wallet - BIC_WS_Opportunity)/1e6 as Uncaptured,
                                                sum(Wallet)/1e6 as Total from %s %s group by %s",varstring,table_mekko, where_clause, varstring))
          
          cdata = fetch(query1_1, n=-1)
    }
    # if in PartnerNames level, then get staic,bic,and unc for the top 10 partnernames with the highest wallet, the next 40, and the rest
    else if(input$pview =="Partners"){ # not in technologies or Tech groups of Partner
          # add FY and Tech group at the very end of where clause
          if(pcurrent.level >2){
            where_clause = paste(where_clause, tech_grp_str )
          }
          # if remove Unkown value checkbox is checked, then add an additional filter in the where_clause to remove those values
          if(pv$redraw_without_unk_checkbox){
              remove_unk_str = " AND BE_GEO_ID !=0"
          }else{
              remove_unk_str = ""
          }
      
          ############# Group Partners by 1-10 individually, 11-50, 51- rest
          query_str = sprintf("select Partner_Name2 as Partner_Name, sum(SWO)/1e6 as SWO,                          
                              sum(AdditionalBIC)/1e6 as AdditionalBIC,                          
                              sum(Uncaptured)/1e6 as Uncaptured,  
                              sum(Total)/1e6 as Total 
                              from(
                              select case
                              when rank() over(order by Sum(Wallet) DESC) <11 then CONCAT(Partner_Name,' (BE-GEO-ID: ', BE_GEO_ID, ')')
                              when rank() over( order by sum(Wallet) DESC) <51 then 'Next Top 40'
                              else 'Rest'
                              end AS Partner_Name2, 
                              sum(Static_WS_Opportunity) as SWO,                          
                              sum(BIC_WS_Opportunity-Static_WS_Opportunity) as AdditionalBIC,                          
                              sum(Wallet - BIC_WS_Opportunity) as Uncaptured,  
                              sum(Wallet) as Total, rank() over(order by sum(Wallet) DESC) as rank 
                              from %s                       
                              %s %s
                              group by BE_GEO_ID, Partner_Name 
                              order by Rank 
                              )x
                              group by Partner_Name
                              order by Total Desc",pv$table_name,where_clause, remove_unk_str)
    
          
          query1 <- dbSendQuery(memdb,query_str)
          print(query_str)
          cdata1 = fetch(query1,n=-1)
          cdata = na.omit(cdata1)
          # Subset the data frame to remove the Rest and Next top 40 (top 2 rows) and append to back of the dataframe
          sub.40 = subset(cdata, Partner_Name== "Next Top 40")
          sub.rest = subset(cdata, Partner_Name== "Rest")
          cdata <- subset(cdata, Partner_Name != "Next Top 40" & Partner_Name != "Rest")
          cdata <- rbind(cdata, sub.40, sub.rest)
          print("CDATA FOR PARTNERNAMES")
          print(cdata)
          # # if BE-GEO-ID =0 in the results, turn a flag on to notify the user, they can click a button called "Remove Unknown Values" to exclude this value and recalculat/redraw  the mekko
          # if(length(grep("(BE-GEO-ID: 0)", cdata[,1])) >0 ){
          #     pv$remove_unk_val = 1
          # }
          # print("remove unk values flag value")
          # print(pv$remove_unk_val)
    }
    # if not in Partners view, then get the static, bic, and unc wallet at this sales level
    else{ 
          # add techgroup string at the end of the where clause
          if(pcurrent.level >2){
            where_clause = paste(where_clause,tech_grp_str )
          }
          print("start QUery 1) The sums of Wallets 22222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222")
          print(where_clause)
          # if varstring is TechnologyGroup or Technology, sum the Value, volume, NC,otherwise, do not.
          query1_1 <- dbSendQuery(memdb2,sprintf("select %s,sum(Static_WS_Opportunity)/1e6 as SWO, sum(BIC_WS_Opportunity-Static_WS_Opportunity)/1e6 as AdditionalBIC,
                                                sum(Wallet - BIC_WS_Opportunity)/1e6 as Uncaptured,
                                                sum(Wallet)/1e6 as Total from %s %s group by %s",varstring, table_name, where_clause, varstring))
          cdata = fetch(query1_1, n=-1)
          print("end Query1 22222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222")
    }
     
    # set where clause as reactive for gameboard
    pv$where_clause = where_clause
    
    # if all x categories have BIC of 0, if so, turn no_bic flag on; once set, cannot change unless on level 2 again 
    if(pcurrent.level ==2 & all(cdata$AdditionalBIC == 0 )){
        pv$no_bic = 1 #  this will tell the renderPlot to use Bookings and Uncaptured Wallet 
    }
    # Get each segment's opportunity in %, this is the width of each xcategory in the mekko. Round to nearest whole number 
    cdata$Segpct <- round(100*(cdata$Total / sum(cdata$Total)), 0) # the segment or xcategory % = segment's Total Wallet/Grand Total Wallet of the whole level
    # remove rows that have 0 segpct
    cdata <- cdata[cdata$Segpct >0,]
    # recalculate the percents after those insignificant rows are removed
    cdata$Segpct <- round(100*(cdata$Total / sum(cdata$Total)), 0)
    # calculate the height of each segment's opportunity, this is the height of each rectangle in a column of the mekko
    cdata$SWOpct <- round(100*(cdata$SWO / cdata$Total), 0) # static percent per xcategory = static wallet/ Total wallet for that xcategory
    cdata$AdditionalBICpct <- round(100*(cdata$AdditionalBIC / cdata$Total), 0) # calc BIC
    cdata$Uncapturedpct <- round(100*(cdata$Uncaptured / cdata$Total), 0) # calc Unc
    
    if(input$pview != "Partners" || pv$partner_tech_grps ==1 || pv$partner_technologies ==1){
        if(input$psortby == "wallet"){
          cdata <- cdata[order(cdata$Total, decreasing = TRUE),]
        }else{
          cdata <- cdata[order(cdata$AdditionalBIC, decreasing = TRUE),]
        }
    }
    
    # Use a new dataframe, containing relevant columns, called cdatanew that contains the percents of each segment's static,bic, and unc wallet
    # cdata contains only the raw numbers and is a helper dataframe
    cdatanew <- cdata[,c(1,6,7,8,9,3)]
    cdata <- cdata[,c(1,6,2,3,4)]
    # Delete this column so that melt doesnt transpose this column as rows
    cdata$Segpct <- NULL
    
    ##################################################################################### Calculate the xmin, xmax, ymin, ymax
    # Calculate the xmin, xmax for each segment based on segment percents
    cdatanew$xmax <- cumsum(cdatanew$Segpct) # this gives the percent of that segment, this is the xmax value
    cdatanew$xmin <- cdatanew$xmax - cdatanew$Segpct # this is the x starting coord for each column
    # Remove this column to avoid creating a row for segment percent
    cdatanew$Segpct <-NULL
    cdatanew$AdditionalBIC <-NULL
    
    # Create a helper dataframe used to calculate the ymin and ymax for each opportunity for each segment type. 
    # Melt reshapes the dataframe to convert column fields to rows.
    dfm1 <- melt(cdatanew, id = c(varstring, "xmin", "xmax"))
    dfm1$xsub <- dfm1$xmax - dfm1$xmin
    dfm1$Product <- with(dfm1, xmin + (xmax - xmin)/2)
    
    dfm1$Number <- melt(cdata,id = c(varstring))$value # get the $ opportunity for each opp type for each segment
    # Reorder rows by the segment type; put all opportunity of the same segment in  adjacent rows; this line is optional
    dfm1 <- dfm1[order(dfm1[,1]),]
    
    # Clean up the labels in another column
    dfm1$cleanx <-gsub("_", " ", dfm1[,1]) # remove punctuation like underscores
    x <- gsub("VERTICAL", "", dfm1$cleanx) # remove certain words that are pre/suffixes
    x <- gsub("AREA", "", x)
    
    dfm1$cleanx <- x
    
  
    # if technology radio button is selected group last 16% of rows as 'Other' and change dfm1
    if((input$pview =='Technologies' & pv$tech_breakdown==0) || (pcurrent.level ==8 && input$pview == 'Acct Mngrs') || (pcurrent.level ==9 && input$pview == 'SAV Group')){
      
      if(nrow(dfm1) > 30  ){ # if number of rows is more than 10, than group rest as ETC
        # Subset the dataframe that contains "other" for the last 16% of the techs
        # get the top 10 x-categories, and group the rest under ETC 
        # order the dfm1 by xsub Desc and then limit by 6 rows
        dfm1 <- dfm1[order(-dfm1$xsub),]
        df.reg = dfm1[1:30,] # get the top 10 x-categorical rows
        df.other = dfm1[31:nrow(dfm1),] # subset the rest
      
        # aggregate rows by opportunity type ie. SWO, BIC, Uncaptured for the last 16% of technologies= OTHER
        df3 = ddply(df.other, c("variable"), summarise,
                      xmin = min(xmin),
                      xmax = max(xmax), Number = sum(Number))
        
        
        # If sum of the opportunities is close to 0M, then set % of each opportunity(value) as 0 to avoid a divide by 0 error.
        if(sum(df3$Number==0.0)){
            # Assume the values for the "other" tech opportunities are the max values found in df.other,
            # which contains all the techs used to create           
            df3$value = df.other[df.other$xsub == max(df.other$xsub),][1,c("value")] # get seg name with the largest xsub, first one
        }else{
          df3$value = round((df3$Number / sum(df3$Number)),2) *100
        }
        if(input$pview == 'Technologies' || pcurrent.level==10){
          df3$TechnologyGroup = "ETC"
          pv$others = unique(df.other[,1])
        }
        # Calculate y and x boundaries for the OTHER row which will be appended to the first 84% of techs or Sales Account Managers
        else if(pcurrent.level == 8){
          df3$SalesRepName = "ETC"
        }else if(pcurrent.level == 9){
          df3$SALES_ACCOUNT_GROUP_NAME = "ETC"
        }
        else{
          df3$TechnologyGroup = "ETC"
        }
        df3$cleanx = "ETC"
        df3$xsub = 100 - df3$xmin
        df3$xmax = 100
        df3$Product <- (df3$xmin + df3$xmax)/2
        # Append this Other row to main dfm1
        dfm1 = rbind(df.reg, df3)
      
      }# end if more than 10 rows
    }
    
    # save the list of all SalesRepNames (or SAV Names) for the selected Level 6 filter. This will be used in dropdown and before 
    # condensed into OTHERS category.
    if(pcurrent.level == 8 || pcurrent.level == 9){
      ordered.dfm1 = dfm1[order(-dfm1$xsub),] 
      all.sales.rep.names = ordered.dfm1[,1]
      pv$all.sales.rep.names = all.sales.rep.names[all.sales.rep.names!="ETC"]
    }
    # Calculate and add a total col (tot.tmp before string processing) to dfm1 which is the sum of the wallet opportunities for each segment
    dfm1 <- ddply(dfm1, varstring, transform, tot.tmp = sum(Number)) # get the sum of this rounded number for each segment
    # Now that we have the wallet amount (Number) for each opportunity type of each segment, let calculate the grand totals for each
    # opportunity(all segments).
    total.SWO = sum(dfm1[dfm1$variable=='SWOpct',]$Number)
    total.BIC = sum(dfm1[dfm1$variable=='AdditionalBICpct',]$Number)
    total.Unc = sum(dfm1[dfm1$variable=='Uncapturedpct',]$Number)
    total.all= total.SWO + total.BIC + total.Unc
    
    # If grand total of wallet is 0M due to rounding the Number so that the totals bar matches the sum of the mekko rectangles,
    # then copy the values of the row in dfm1 that has the largest xsub (total wallet %).
    if(total.all == 0.0){ #
      seg = dfm1[dfm1$xsub == max(dfm1$xsub),][1,1] # get seg name with the largest xsub
      values = dfm1[dfm1[,1]==seg,]$value # get the values % for each opportunity for that segment
      
    }else{
      values = c(round(100*total.SWO/total.all, 0),round(100 *total.BIC/total.all, 0),round(100 * total.Unc/total.all,0))
    }
    
    # Calculate the percents of each opportunity by dividing it by the total.all
    dfm2 = data.frame(variable = c('SWOpct','AdditionalBICpct','Uncapturedpct'),Number= c(total.SWO,total.BIC,total.Unc),
                      value = values
    )
    # Add columns in this dataframe that calculate the ymin and ymax and Percentage which is the ycoord to put the labels in the totals bar
    dfm2$ymax = cumsum(dfm2$value)
    dfm2$ymin = dfm2$ymax - dfm2$value
    dfm2$Percentage <- dfm2$ymin + (dfm2$ymax - dfm2$ymin)/2
    
    # Convert the Numbers in dfm1 and dfm2 to string to output in hover and mekko tables
    
    # Every number in a mekko view has to be consistent, either all in M or all in B
    # Get the number of rows in dfm1 that is less 100M. If number of those rows is greater than 20%, units should be in Millions
    # Otherwise unit should be Billions
    unit = "B"
    if(nrow(dfm1[dfm1$Number < 1,])/nrow(dfm1) >.5 ){
      unit = "K"
    }else if(nrow(dfm1[dfm1$Number < 100,])/nrow(dfm1) >.2 ){
      unit = "M"
    }
    
    # Convert the Mekko Column Numbers to rounded number and string
    dfm1$total.col = num.to.string(dfm1,"tot.tmp", unit) # this function rounds the numbers
    
    # Clean up values in dfm1 so that they match the values in dfm2,
    # dfm1 need to be calculated using rounded opportunity M /rounded total column wallet M
    dfm1$value <- ifelse(dfm1$tot.tmp >0.0,round(dfm1$Number/dfm1$tot.tmp*100,1),dfm1$value)
    # Calculate the ymin,ymax values based on the value, this is the height of each rectangle of a column
    dfm1<- ddply(dfm1, c(varstring), transform, ymax = cumsum(value))
    dfm1$ymin = dfm1$ymax - dfm1$value
    dfm1$Percentage <- dfm1$ymin + (dfm1$ymax - dfm1$ymin)/2
    # Make sure ymax is 100 for the last rectangle (Uncaptured pct) of each column
    dfm1[dfm1$variable=="Uncapturedpct",]$ymax <- 100
    
    # if TechGroup, create the blueprint of where the Value, Volume, NC line segment should be placed in main mekko graph
    # for each Technology Static Wallet row, append 3 rows to the new dataframe 
    if(pv$draw_vvn & (varstring == "TechnologyGroup" | varstring == "Technology")){
      # if in Partner's View, the Tech page may have a 'Sales_Account_ID_INT' as a filter.If so, use orig mekko table
      if(grepl('Sales_Account_ID_INT', where_clause)){
            table_name1 = table_mekko
      }else{
            table_name1 = pv$table_name
      }
      query3 <- dbSendQuery(memdb3,sprintf("select %s,sum(Value)/1e6 as Value, sum(Volume)/1e6 as Volume, sum(NC)/1e6 as NC from %s %s group by %s",varstring,
                                           table_name1,where_clause, varstring))
      q3data = fetch(query3, n=-1)
      vvn_data = q3data
      # exclude rows where value, volume, and nc are all 0
      #vvn_data = q3data[q3data$Value >0 | q3data$Volume>0 | q3data$NC>0, ]
      # create dfm3 which contains the Val/Vol/NA for Technologies in the Static Wallet Opp
      # get the rows in dfm1 where variable is static wallet
      swo_rows = dfm1[dfm1$variable == "SWOpct",]
      # Join this data with swo_rows from dfm1 on the first column- varstring
      swo_merge = merge(swo_rows,vvn_data, by = 1)
      # if after merge, there are no rows, then set dfm3 = NULL and thus no line segments
      if(nrow(swo_merge)==0){
        pv$dfm3 <-NULL
      }else{
        # get the Value, Vol, NC % of Static
        swo_merge$Val_pct  = round((swo_merge$Value *100.0) / swo_merge$Number)
        swo_merge$Vol_pct  = round((swo_merge$Volume *100.0) / swo_merge$Number)
        swo_merge$Unk_pct  = round((swo_merge$NC *100.0) / swo_merge$Number)
        dfm3 = do.call(rbind, apply(swo_merge,1,create_val_vol_rows))
        #replace NA values with 0??
        #dfm3[is.na(dfm3)] <- 0
        pv$dfm3 <- dfm3
      }
    }else{
      pv$dfm3 <-NULL
    }
    # Convert each Mekko rectangle to string- first round appropriately by unit
    dfm1$Number = num.to.string(dfm1,"Number", unit) # this function rounds as well
    
    # For the totals dataframe, dfm2, sum up the total wallet types and create a string field called total.col
    dfm2$tot.tmp <- sum(dfm2$Number)
    # Convert these columns to strings
    dfm2$Number <- num.to.string(dfm2,"Number", unit)
    dfm2$total.col <-num.to.string(dfm2,"tot.tmp", unit)   
    
    # Calculate the # unique Partners and #partner/Wallet 
    # Then calculate the # unique SAV Accts and SAV Accts/Wallet for SH,VM, and SAV Seg only
    if(input$pview !="Technologies" & pcurrent.level <9 &  input$pview!= "Partners"){
      
      # if in Partners view, use the accts.df created from the first query
      # Add the Number Unique Account ids and the Avg. Number of Accounts/Wallet for each segment to dfm1 to output to the table
      query2 =  create_string(varstring, where_clause,  calc_sav=0, calc_part=1)
      accts.df <- fetch(query2, n=-1) 
      print("QUery2 results 33333333333333333333333333333333333333333333333333333333333333333333333333333333333333")
      
      # Convert number to string and merge with main dataframe, dfm1
      # format the Number with commas
      accts.df$num.accts.str = format(accts.df,big.mark=",",scientific=FALSE, trim=TRUE)$numaccts
      dfm1 <- merge(dfm1,accts.df,all.x = TRUE) #left join since "OTHER" is not a real technology and will not be in accts.df
      
      if(pcurrent.level == 8 || pcurrent.level ==9){
        remaining_num_accts = sum(accts.df$numaccts) - (sum(dfm1$numaccts, na.rm= TRUE)/3)
        dfm1$numaccts[is.na(dfm1$numaccts)] <- remaining_num_accts
        dfm1$num.accts.str = format(dfm1,big.mark=",",scientific=FALSE, trim=TRUE)$numaccts
      }
      
      # Call function to get the number of accounts/wallet for each x axis category
      dfm1 = calculate_num_partner(dfm1)
      
      # Now calculate the  # SAV Accts
      query2 =  create_string(varstring,  where_clause, calc_sav=1, calc_part=0)
      accts.df2 <- fetch(query2, n=-1)
      print("QUery2 results 333333333333333333333333333333333333333333333333333333333333333333333333333333333333333")
     
      accts.df2$num.accts.str2 = format(accts.df2,big.mark=",",scientific=FALSE, trim=TRUE)$numaccts2
      
      dfm1 <- merge(dfm1, accts.df2, all.x = TRUE) #left join since "OTHER" is not a real technology and will not be in accts.df
      
      # Since some columns might not match for example SAV Segment US COmmercial not in Accounts Table when it should be
      dfm1[is.na(dfm1)] <- 0
      
      if(pcurrent.level == 8 || pcurrent.level ==9){
        remaining_num_accts = sum(accts.df$numaccts2) - (sum(dfm1$numaccts2, na.rm= TRUE)/3)
        dfm1$numaccts2[is.na(dfm1$numaccts2)] <- remaining_num_accts
        dfm1$num.accts.str2 = format(dfm1, big.mark=",",scientific=FALSE, trim=TRUE)$numaccts2
      }
      # Call function to get the number of accounts/wallet for each x axis category
      dfm1 = calculate_num_account(dfm1)
      
      # In dfm1 when on Level_3, append a * to the number Sav accts to show that we are calculating the #sites since it contains an unamed acct
      #if(varstring =='Level_3' || varstring =='Level_4'){
      dfm1$num.accts.str2 <- ifelse(dfm1$type_acct =='unnamed',paste0(dfm1$num.accts.str2,'*'), dfm1$num.accts.str2)
      #}
      print("QUery to Count distinct BE GEO IDS 44444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444")
      ################### Add query to get total count distinct be-geo-ids grouped by the varstring
      #where_clause = unlist(strsplit(where_clause, "AND .*\\(Techno"))[1]
      query_total_str = sprintf("select count( distinct BE_GEO_ID) as cnt_ids
                                from %s
                                %s", pv$table_name, where_clause)
      
      
      total_res_q = dbSendQuery(memdb,query_total_str)
      total_res = fetch(total_res_q, n=-1)
      print("END count distinct BE GEO ID 4444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444")
      # Add the total Number of unique Account ids for all segments in the mekko
      dfm2$numaccts = total_res$cnt_ids #ignore NA values from "OTHER" and divide by 3 
      # Since num accounts is being counted for the 3 opportunity types
      # convert this to a string with commas
      dfm2$num.accts.str = format(dfm2$numaccts,big.mark=",",scientific=FALSE, trim=TRUE)
      # Calculate the Avg wallet for all segments
      dfm2 = calculate_num_partner(dfm2)
      
      ################### Add query to get total sum of distinct sav ids  grouped by the varstring
      # if dfm1 has Unnamed or Prospects in the first column, then use count_Accounts
      # else use cnt_savs
      
      # remove the FY from the query since the Number of distinct SAV accts is same for 2 FYs
      where_clause =  unlist(strsplit(where_clause, "AND FY"))[1]
      where_clause =  unlist(strsplit(where_clause, "AND  FY"))[1]
      # if there is only an FY clause as in current level 2, then remove it
      where_clause =  trimws(unlist(strsplit(where_clause, "FY"))[1])
      if(where_clause =='where'){
        where_clause = ""
      }
      # # remove the Technology Group from the where_clause since it is not a field
      # where_clause = unlist(strsplit(where_clause, "AND .*\\(Techno"))[1]
      # # remove the PartnerName Id if exists in the where clause
      # where_clause = unlist(strsplit(where_clause, "AND PTNR_NAME_ID"))[1]
      print("Query to get DFM2 NUM OF SAV ACCTS 55555555555555555555555555555555555555555555555555555555555555555555555555")
      query_total_str = sprintf("select ALL
                                CASE when VerticalMarket != '' THEN sum(cnt_savs)
                                else sum(Count_Accounts) 					
                                END AS cnt_ids_sav,
                                CASE when VerticalMarket != '' THEN 'regular'
                                else 'unnamed'					
                                END AS acct_type
                                from %s
                                %s",table_of_accts, where_clause)
      
      
      total_res_q = dbSendQuery(memdb,query_total_str)
      total_res = fetch(total_res_q, n=-1)
      print("ENd 55555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555")
      #store he flag from the query of whether we calculated #Accounts(Unnamed/prospects) or #SAV_ids
      pv$is_unnamed_prospects = total_res$acct_type
      # Add the total Number of unique Account ids for all segments in the mekko
      dfm2$numaccts2 = total_res$cnt_ids_sav #ignore NA values from "OTHER" and divide by 3 
      # Since num accounts is being counted for the 3 opportunity types
      # convert this to a string with commas
      dfm2$num.accts.str2 = format(dfm2$numaccts2,big.mark=",",scientific=FALSE, trim=TRUE)
      # Calculate the Avg wallet for all segments
      dfm2 = calculate_num_account(dfm2)
    }
    ################ END BAD
    
    # Remove OT and other very small segments  ~0 % segment pct (xsub) by keeping the relevant rows
    dfm1 <- dfm1[dfm1$xsub >0.0,]
    
    # If tech is not selected and current level is not on last page, then shorten long, redundant titled labels (x-axis)
    if(input$pview =="Sales Hierarchy" & pcurrent.level<8){
      
      # Remove duplicates from cleanx labels such as prefixes, suffixes,not in technologies
      dfm1$cleanx <- gsub("-", " ", dfm1$cleanx)
      dfm1$clean.short.x <- shorten.labels(dfm1)
      
      # check if any strings are empty, if so replace the empty string with a slightly uncleaned label in column cleanx 
      find.empty <- str_trim(dfm1$clean.short.x) %in% ""
      dfm1$clean.short.x <- as.character(dfm1$clean.short.x)
      dfm1[find.empty, "clean.short.x"] <- as.character(dfm1[find.empty, "cleanx"])
      
    }else if(varstring == "Technology"){ # if technology or last view, then dont shorten labels by removing prefixes etc.
      # Remove duplicates from cleanx labels such as prefixes, suffixes,not in technologies
      dfm1$clean.short.x <- remove_first_word(dfm1)
      
      # check if any strings are empty, if so replace the empty string with a slightly uncleaned label in column cleanx 
      find.empty <- str_trim(dfm1$clean.short.x) %in% ""
      dfm1$clean.short.x <- as.character(dfm1$clean.short.x)
      dfm1[find.empty, "clean.short.x"] <- as.character(dfm1[find.empty, "cleanx"])
    }else{ 
      dfm1$clean.short.x <- dfm1$cleanx
    }
    
    # Proper case the clean.short.x
    dfm1$clean.short.x <- sapply(dfm1$clean.short.x, simpleCap)
    
    # Split the shortened clean x labels by the length of xsub- add space every xsub characters so that shorten.labels can break up very long strings 
    dfm1$clean.short.x <-apply(dfm1,1, function(x) 
      ifelse(length(strsplit(x['clean.short.x']," "))[[1]] == 1 ,gsub( paste('(.{',str_trim((ceiling(1.5*as.numeric(x['xsub']))-1)),'})',sep=""), 
                                                                       '\\1 ',x['clean.short.x']),x['clean.short.x'])
    )
    
    # Next string wrap- add newline char- on width of xsub
    dfm1$clean.short.x <- apply(dfm1,1, function(x) {
      str_wrap(x['clean.short.x'], width = ceiling(1.5*as.numeric(x['xsub']))-1)
    }
    )
    num.lines = 3
    # Instantiate the number of lines of text(x labels)
    if(input$pview =="Partners"){
      num.lines = 1
    }
    else if(input$pview !="Technologies" & pcurrent.level <9){
      num.lines = 3
    }
    else{ 
      num.lines = 1
    }
    # Lastly, shorten any long height strings- that have more than num.lines newline chars
    dfm1$clean.short.x <- apply(dfm1,1, function(x) {
      substr(x['clean.short.x'], 0, ifelse( is.na(gregexpr('\n', x['clean.short.x'])[[1]][num.lines]) |gregexpr('\n', x['clean.short.x'])[[1]][num.lines] <0 
                                            , 100,gregexpr('\n', x['clean.short.x'])[[1]][num.lines]))
    })
    
    # make sure the dfm2 reaches a ymax of 100, could be slightly more bc of rounding so cutoff at 100
    dfm2[dfm2$variable=="Uncapturedpct",]$ymax <- 100
    
    #Update the reaction values
    pv$dfm1 <- dfm1
    pv$dfm2 <- dfm2
    print(pv$table_name)
    #shinyjs::enable('pview')
    print("End mekko *****************************************************************************************************************************************************************************************************************************************************")
  } #end reactive function
  
  
  observe({
    print("In observe....")
    # if checkbox is marked, then remove be-geo-id =0 or any othe unk values
    if(!is.null(input$redraw_without_unk_checkbox)){
        pv$redraw_without_unk_checkbox = input$redraw_without_unk_checkbox
    }
    
    # When pcurrent.level is 14 which is after the last page, reset the pcurrent.level to 1 and filtered.data to newdata
    if(pv$pcurrent.level == 14){
      pv$pcurrent.level = 2
      pv$clean.sel.list = NULL
      pv$where_clause = ""
      shinyjs::hide('pview2')
    }
    if(pv$pcurrent.level == 2){
      #when current level is on Level1 segment, reinitialize the list that store the clicked segments on the mekko 
      pv$sel.list = list()
      pv$count_clicks = 0 # reset number of clicks
      pv$no_bic = 0 # reset flag when there is no bic
      
    }
    #if pcurrent.level is 10 or is in techgbreakdown view, hide technologies 
    if(pv$pcurrent.level >=11 || pv$tech_breakdown ==1 ){
      shinyjs::hide('pview')
    }else{
      shinyjs::show('pview') # show the radio buttons if not on last page
    }
    
    
    # if there is an input view already selected, use that; else if null, select first
    if(pv$pcurrent.level == 8 & (input$pview == "Sales Hierarchy" ||input$pview=="SAV Group")){
      prev_radio_button = 'Acct Mngrs'
    }else if(pv$pcurrent.level == 9 & input$pview == "Acct Mngrs"){
      prev_radio_button = 'SAV Group'
    }
    else if(pv$pcurrent.level <=7 & (input$pview=="SAV Group" || input$pview == "Acct Mngrs")){
      prev_radio_button = 'Sales Hierarchy'
    }
    else{
      prev_radio_button = input$pview
    }
    if(input$pview == "Partners" & pv$partner_tech_grps ==0 & pv$partner_technologies ==0){
      shinyjs::hide("psortby")
    }else{
      shinyjs::show("psortby")
    }
    
    if(pv$partner_tech_grps ==0){
      shinyjs::hide('pview2')
    }
    
    if(pv$partner_technologies ==1){
      shinyjs::hide('pview')
    }
    
    if(is.null(input$draw_vvn)){
      
      pv$draw_vvn = FALSE
    }
    else if(input$draw_vvn){
      pv$draw_vvn = TRUE
    }else{
      pv$draw_vvn = FALSE
    }
    
    # In Partners and Techgroup page, show the option of VM, SAV_Segment, and default selected is Technologies
    if( pv$pcurrent.level >=2 & (pv$partner_SAVID)){
      shinyjs::hide('pview')
      shinyjs::show('pview2')
      updateRadioButtons(session, "pview2", "Views", choices =
                           c("SAV Names" = "Technologies", "Verticals" = "Verticals","SAV Segments" ="SAV Segments"))
      
    }
    else if( pv$pcurrent.level >=2 & (pv$partner_tech_grps)){
      shinyjs::hide('pview')
      shinyjs::show('pview2')
      updateRadioButtons(session, "pview2", "Views", choices =
                           c("Technologies" = "Technologies", "Verticals" = "Verticals","SAV Segments" ="SAV Segments"))
      
    }
    else if(pv$pcurrent.level == 8){
      updateRadioButtons(session, "pview", "Views", selected= prev_radio_button, choices =
                           c("Acct Mngrs" = "Acct Mngrs","Verticals" = "Verticals","SAV Segments" ="SAV Segments", "Partners" = "Partners",
                             "Technologies" = "Technologies"))
      
    }else if(pv$pcurrent.level == 9){
      
      updateRadioButtons(session, "pview", "Views", selected= prev_radio_button, choices =
                           c("SAV Group" = "SAV Group","Verticals" = "Verticals","SAV Segments" ="SAV Segments","Partners" = "Partners",
                             "Technologies" = "Technologies"))
      
    }
    else if( pv$partner_tech_grps==0 & pv$pcurrent.level ==10){
      
      if(prev_radio_button == 'SAV Group'){
        prev_radio_button = 'Technologies'
        
      }
      
      updateRadioButtons(session, "pview", "Views",  selected = prev_radio_button, choices = 
                           c("Technologies" = "Technologies", "Verticals" = "Verticals","SAV Segments" ="SAV Segments","Partners" = "Partners"))
    }
    else if(pv$pcurrent.level >=2 & pv$pcurrent.level < 8){
      
      updateRadioButtons(session, "pview", "Views",  selected = prev_radio_button,choices = 
                           c("Sales Hierarchy" = "Sales Hierarchy","Verticals" = "Verticals","SAV Segments" ="SAV Segments","Partners" = "Partners",
                             "Technologies" = "Technologies"))
      
    }
    print("end observe*************************************************************8")
  })
  
  
  # Draw the MEKKO by first calling the reactive function pmekko.blueprint(), 
  # any reactive values (starting with pv$ ) you put in the renderPlot function will cause a redraw when those values change. 
  output$pmekkograph <- renderPlot({
    print("DRAW PMekkograph......................................................................................................")
    if(is.null(input$partner_beid)|| is.null(pv$beid)){
      print("Beid is Null so Returnning in mekko")
      return()
    }
    
    # call mekko.bp() which gets the dataframe to create the mekko
    pmekko.blueprint()
    dfm1 = pv$dfm1 
    pv$redraw_without_unk_checkbox # put this here to tell the function to redraw the mekko graph when this value changes
    print("start drawing the Mekko")
    #print("Dfm1")
    #print(dfm1)
    p <- ggplot(dfm1, aes(ymin = ymin, ymax = ymax,
                          xmin = xmin, xmax = xmax, 
                          fill = variable)) 
    
    p <- p +  geom_rect(colour = I("black")) 
    # if the High end low end dataframe exists, then display the bars
    if(!is.null(pv$dfm3)){
      dfm3 = pv$dfm3
      dfm3$fill[dfm3$type =='Value'] ='lightcyan'
      dfm3$fill[dfm3$type =='Volume'] ='lightblue'
      dfm3$fill[dfm3$type =='Unk'] ='lightblue1'
      dfm3$alpha[dfm3$type =='Value'] = 1.0
      dfm3$alpha[dfm3$type =='Volume'] = 1.0
      dfm3$alpha[dfm3$type =='Unk'] = 1.0
      
      #if number of rows in dfm3 != number of rows in dfm1, then we need to add NC colors to dfm3 so nrows are the same
      ff = dfm3$fill
      diff_r = abs(nrow(dfm1) - nrow(dfm3))
      if(diff_r > 0){
        fill_factor = append(ff ,rep("lightblue1", diff_r))
      }else{
        fill_factor = ff
      }
      p <- p + geom_rect( aes(fill = fill_factor))
      p <- p + geom_rect(data= dfm3, aes(xmin=xs, xmax=xe, ymin=ys, ymax=ye), fill= dfm3$fill, alpha=dfm3$alpha)
      
      
      static_df = dfm1[dfm1$variable == 'SWOpct',]
      p <- p +geom_rect(data= static_df, aes(ymin = ymin, ymax = ymax,xmin = xmin, xmax = xmax), color = "black", alpha=0.0)
      p <- p +geom_rect(data= dfm1, aes(ymin = ymin, ymax = ymax,xmin = xmin, xmax = xmax), color = "black", alpha=0.0)
     
    }
    
    if(input$pview !="Technologies" & pv$pcurrent.level <9  & input$pview != "Partners"){
      # Draw the pink table at the bottom representing the number unique partners and the ave partners/wallet
      p <- p + geom_rect(data=dfm1, aes(xmin=xmin, xmax=xmax, ymin=-40, ymax=-20), fill='grey95', alpha=0.2,colour="black") 
      # Add a horizontal divider to the number of partners and av partners/wallet table in the middle of the table
      p <- p +geom_segment(aes(x = 0, y = -30,xend = max(dfm1$xmax), yend = -30),color = "black") 
      # Add text: number partners for each rectangle in the pink table if width is bigger than threshold
      p <- p + geom_text(aes(x = Product, y = -25,label = ifelse(xsub>=4.0, paste(num.accts.str2),"...")), size = 3.5) 
      # Add text: avg. number partnes/wallet for each rectangle in the table if there is space else ...
      p <- p +geom_text(aes(x = Product, y = -35,label = ifelse(xsub>=3.0,
                                                                paste("$",avg.acct.walletsav,sep=""),
                                                                "...")), size = 3.5) 
      
      # Draw 2 additional rows in the pink table at the bottom representing the number accounts and the ave accounts/wallet
      p <- p + geom_rect(data=dfm1, aes(xmin=xmin, xmax=xmax, ymin=-60, ymax=-40), fill='grey95', alpha=0.2,colour="black") 
      # Add a horizontal divider to the number of accounts and avgg accounts/wallet table in the middle of the table
      p <- p +geom_segment(aes(x = 0, y = -50,xend = max(dfm1$xmax), yend = -50),color = "black") 
      # Add text: number accounts for each rectangle in the pink table if width is bigger than threshold
      p <- p + geom_text(aes(x = Product, y = -45,label = ifelse(xsub>=4.0, paste(num.accts.str),"...")), size = 3.5) 
      # Add text: avg. number accts/wallet for each rectangle in the table if there is space else ...
      p <- p +geom_text(aes(x = Product, y = -55,label = ifelse(xsub>=3.0,
                                                                paste("$",avg.acct.wallet,sep=""),
                                                                "...")), size = 3.5)
    }
    
    
    # Print the percents and totals in each Rectangle only if width and height is greater than threshold
    # if Wallet number is less than 1M, print less than 1M, else print round the number(in M) to 100s place
    # Number is a char string not numeric
    p <- p + geom_text(aes(x = Product, y = Percentage,label = ifelse((xsub>=3.5 & value>=10.3),
                                                                      paste("$", Number,"\n",round(value,0),"%", sep=""),
                                                                      ifelse(value >=2.0, paste("..."),"")
    )), size = 3.5) 
    
    # Print the X Labels at the bottom of mekko
    p <- p + geom_text(aes(x = Product, y = -2, label = ifelse(variable =="SWOpct", clean.short.x,
                                                               ""),vjust = 1), size = 3.3, fontface = "bold", colour ="black") 
    
    # Print the Column total wallet for each segment  
    p <- p + geom_text(aes(x = Product, y = 105, label = ifelse((xsub>=4.0),paste("$", total.col,sep="")
                                                                ,"..."),vjust=-.5), size = 3.5,colour="black") 
    # Print the Column total Wallet % for each segment
    p <- p + geom_text(aes(x = Product, y = 100, label =paste(round_any(xsub,1),"%", sep = ""),vjust=-.3), size = 3.5 ,colour="black") 
    
    
    
    
    # Add themes to the graph such as margins for y tick mark labels and legend position
    p <- p + theme(panel.grid.minor = element_blank(), panel.background = element_blank(),axis.text.x=element_blank(),axis.ticks=element_blank(),
                   axis.text.y = element_blank()) 
    
    p <- p +theme(legend.position = "top", legend.direction = "horizontal") + theme(axis.title.y=element_blank()) +  
      theme(legend.text = element_text(size = 10)) +
      theme(legend.key.size = unit(.45, "cm")) + theme(plot.margin=unit(c(0,0,0,0),"mm")) #top,right,bottom, left
    
    if(input$pview !="Technologies" & pv$pcurrent.level <9 & input$pview != "Partners"){
      # Add table headings to the side of the table describing the rows
      
      p <- p + annotation_custom(grob = textGrob("#Partners ",gp = gpar(fontsize = 10)), xmin = 2,xmax=-7, ymin = -50, ymax = -41) 
      p <- p +annotation_custom(grob = textGrob("Avg Partner",gp = gpar(fontsize = 10)), xmin = 2,xmax=-7, ymin = -55, ymax = -50) 
      p <- p +annotation_custom(grob = textGrob("Wallet ",gp = gpar(fontsize = 10)), xmin = 2,xmax=-7, ymin = -60, ymax = -58)
      if(pv$is_unnamed_prospects == 'unnamed'){
        
        p <- p + annotation_custom(grob = textGrob("#Sites ",gp = gpar(fontsize = 10)), xmin = 2,xmax=-7, ymin = -30, ymax = -21) 
        p <- p +annotation_custom(grob = textGrob("Avg Site",gp = gpar(fontsize = 10)), xmin = 2,xmax=-7, ymin = -35, ymax = -30) 
        p <- p +annotation_custom(grob = textGrob("Wallet ",gp = gpar(fontsize = 10)), xmin = 2,xmax=-7, ymin = -40, ymax = -38 )   
      }
      else{
        p <- p + annotation_custom(grob = textGrob("#Accts ",gp = gpar(fontsize = 10)), xmin = 2,xmax=-7, ymin = -30, ymax = -21) 
        p <- p +annotation_custom(grob = textGrob("Avg Acct",gp = gpar(fontsize = 10)), xmin = 2,xmax=-7, ymin = -35, ymax = -30) 
        p <- p +annotation_custom(grob = textGrob("Wallet ",gp = gpar(fontsize = 10)), xmin = 2,xmax=-7, ymin = -40, ymax = -38 )
      }
    }
    
    p <- p + xlab("")+ ylab("")
    # if no_bic is on, then legend should have 2 fields where light blue is Bookings and white is uncaptured
    print("BEST IN CLASS ON??? IN VERT?SAV OF PARTNER VIEW")
    print(pv$no_bic)
    if(pv$no_bic ==1){
      if(!is.null(pv$dfm3)){
        p <- p + scale_fill_manual(name="", breaks = c( "lightcyan","Uncapturedpct","AdditionalBICpct", "lightblue1"),
                                   values=c("lightblue1", "#56B4E9", "white", "lightcyan","lightblue", "lightblue"),
                                   labels=c( "Bookings HE  ", "Bookings LE  ", "Bookings NC ","Uncaptured"
                                   ),guide = guide_legend(nrow = 1))
      }else{
          p <- p + scale_fill_manual(name="",breaks = c("SWOpct", "Uncapturedpct"), values=c("lightblue1","#56B4E9","white"),labels=c("Bookings   ","Uncaptured   "))
      }
    }else{
      if(!is.null(pv$dfm3)){
        
        # dont understand the logic of the colors- this is a manual setup since we dont want to include Static here
        p <- p + scale_fill_manual(name="", breaks = c( "lightcyan", "SWOpct","Uncapturedpct", "AdditionalBICpct", "lightblue1"),
                                  values=c("#56B4E9", "#56B4E9", "white", "lightcyan", "lightblue","lightblue1"),
                                  labels=c( "Static HE  ", "Static LE  ", "Static NC ", "Incr. Best-In-Class   ","Uncaptured"
                                  ),guide = guide_legend(nrow = 1))
      }
      else{
        
        p <- p + scale_fill_manual(name="",values=c("lightblue1", "#56B4E9", "white"),labels=c("Static Share   ", "Incr. Best-In-Class   ",
                                                                                         "Uncaptured   "))
      }
    }
    print("end drawing the mkko *********(((((((((((((((((((****************8")
    
    p
    
  }) # end renderPlot mekko
  
  # Totals bar Reactive Plot
  output$pbar <- renderPlot({
    print("Draw Pbar............................................................................................................................................")
    if(is.null(input$partner_beid) || is.null(pv$beid)){
      print("Beid is Null so Returnning in bar")
      return()
    }
    #pmekko.blueprint() # call reactive function to update the reactive values
    dfm2 = pv$dfm2
    
    p <- ggplot(dfm2, aes(ymin = ymin, ymax = ymax,
                          xmin = 0, xmax = 100,
                          fill = variable)) 
    
    p <- p +  geom_rect(colour = I("black")) 
    
    if(input$pview !="Technologies" & pv$pcurrent.level <9 & input$pview != "Partners" ){   
      # Draw the pink table at the bottom representing the number partners and the ave partners/wallet
      p <- p +  geom_rect(data=dfm2, aes(xmin=0, xmax=100, ymin=-40, ymax=-20), fill='grey95', alpha=0.2,colour="black") 
      # Add a horizontal divider to the number of accounts and avgg accounts/wallet table
      p <- p + geom_segment(aes(x = 0, y = -30,xend = 100, yend = -30),color = "black") 
      # Add text: number partners for each rectangle in the bottom table 
      p <- p + geom_text(aes(x = 50, y = -25,label = paste(num.accts.str2)), size = 3.5) 
      # Add text: avg. number partners/wallet for each rectangle in the bottom table
      p <- p + geom_text(aes(x = 50, y = -35,label = paste("$",avg.acct.walletsav,sep="")), size = 3.5)
      
      # Draw the pink table at the bottom representing the number accounts and the ave accounts/wallet
      p <- p +  geom_rect(data=dfm2, aes(xmin=0, xmax=100, ymin=-60, ymax=-40), fill='grey95', alpha=0.2,colour="black") 
      # Add a horizontal divider to the number of accounts and avgg accounts/wallet table
      p <- p + geom_segment(aes(x = 0, y = -50,xend = 100, yend = -50),color = "black") 
      # Add text: number accounts for each rectangle in the bottom table 
      p <- p + geom_text(aes(x = 50, y = -45,label = paste(num.accts.str)), size = 3.5) 
      # Add text: avg. number accts/wallet for each rectangle in the bottom table
      p <- p + geom_text(aes(x = 50, y = -55,label = paste("$",avg.acct.wallet,sep="")), size = 3.5)
      
    }
    # For each rectangle, print the total wallet for that opportunity and the % if the height is large enough, else reduce the size
    p <- p +  geom_text(aes(x = 50, y = Percentage,label = ifelse(value >=10.0, paste("$", Number,
                                                                                      "\n",round(value,0),"%", sep = ""),"...")), size = 3.5)
    
    
    p <- p +  geom_text(aes(x = 50, y = 0, label = paste("Totals"), vjust = 1.5), size = 3.3, colour = "black") 
    # print the grand total of all wallets of all segments
    p <- p +  geom_text(aes(x = 50, y = 105, label = paste("$",total.col,sep=""),
                            vjust=-.5), size = 3.5,colour="black") 
    
    p <- p +  geom_text(aes(x = 50, y = 100, label = paste("100%"),vjust=-.4), size = 3.5,colour="black")+
      theme(panel.grid.minor = element_blank(), panel.background = element_blank(),axis.text.x=element_blank(),
            axis.text.y=element_blank(),axis.ticks=element_blank()) +
      theme(legend.position = "none") + 
      
      xlab("")+  
      ylab("")+
      scale_fill_manual(values=c("#56B4E9","lightblue1", "white"))
      
    p
  })
  
  # Create a checkbox to allow users to see the High/Low end markers in the mekko
  output$val_vol_lines <- renderUI({
    print("Val/Vol/checkbox...")
    if(pv$varstring == 'Technology' || pv$varstring=='TechnologyGroup') {
      # remember the state of the checkbox
      checkboxInput("draw_vvn", "Show High/Low end", pv$draw_vvn)
    }else{
      
      return()
    }
  })
  
  
  # Create a checkbox to allow users to remove the unknow values in the mekko ie. BE-GEO-ID = 0
  output$remove_unk_checkbox <- renderUI({
    print("remove_unk_checkbox...")
    # in Partner_Name level, give the users the option to exclude certain unknown values
    if(pv$varstring == "Partner_Name"){
      # intialize value as unchecked
      div(checkboxInput("redraw_without_unk_checkbox", "Redraw Mekko Without Non Allocated Partners", value = pv$redraw_without_unk_checkbox), style= "color:white;")
    }else{
      return()
    }
  })
  
  
  # Create a download button in all Sales Levels 
  output$pdownloadData <- renderUI({
    print("DownloadData....")
    if( pv$pcurrent.level >= 2) {
      downloadButton('pdownload', 'Download SAV Accts',style= "color:black;")
    }else{
      return()
    }
  })
  
  # create a dropdown of a list of Account Managers/SAV Names when pcurrent.level is 8 and 9 
  output$pdropdown <- renderUI({
    print("Pdropdown...")
    
    if(input$pview == 'Technologies' || input$pview =='SAV Segments' || input$pview =='Verticals' || input$pview2 != 'Technologies'){
      
      return()
    }
    # if on Partners view only
    if(input$pview == "Partners" & pv$partner_SAVID ==0 & pv$partner_tech_grps ==0 & pv$partner_technologies==0){
      
      # create the query to get the partner names ordered by total wallet in desc order
      print("TABLE NAME **")
      print(pv$table_name)
      query_str = sprintf("select concat(rank, '-', Partner_Name, ' (BE-GEO-ID: ', BE_GEO_ID, ')') as partner_rank
                          from(
                          select Partner_Name, BE_GEO_ID, rank() over(order by sum(Wallet) DESC) as rank
                          from %s
                          %s
                          group by Partner_Name,BE_GEO_ID 
                          limit 100
                          )a", pv$table_name,pv$where_clause)
         
      query_pr =  dbSendQuery(memdb,query_str) # biggest year first, which is the selected
      pr = fetch(query_pr, n=-1)
      fields = pr$partner_rank
      nfields <- rbind(c("All PartnerNames (BE-GEO-ID)"), fields)
      div(
        tags$style(type='text/css', ".selectize-input { padding: 1px; min-height: 0;} .selectize-dropdown-content { font-size: 8px; line-height: 15px; };"),
        selectInput("pselect", label = NULL, 
                    choices = nfields, 
                    selected = 1))
      
      
    }
    # if on Partners- SAVID view only
    else if(input$pview == "Partners" & pv$partner_SAVID ==1 & pv$partner_tech_grps ==0 & pv$partner_technologies==0){
      print("SAVID PARTNER VIEW*****************")
      # create the query to get the partner names ordered by total wallet in desc order
      # table name will always be orig mekko table since it has the SAVIDs
      table_name = table_mekko
      query_str = sprintf("select concat(rank, '-', Sales_Account_Group_Name, ' (SAVID: ', Sales_Account_ID_INT, ')') as savid_rank
                          from(
                          select Sales_Account_Group_Name, Sales_Account_ID_INT, rank() over(order by sum(Wallet) DESC) as rank
                          from %s
                          %s
                          group by Sales_Account_Group_Name, Sales_Account_ID_INT
                          limit 100
                          )a",table_name,  pv$where_clause)
        
      
      query_pr =  dbSendQuery(memdb,query_str) 
      pr = fetch(query_pr, n=-1)
      print("DONE SAVID PARTNER VIEW ***********")
      fields = pr$savid_rank
      nfields <- rbind(c("Select a SAV Name"), fields)
      div(
        tags$style(type='text/css', ".selectize-input { padding: 1px; min-height: 0;} .selectize-dropdown-content { font-size: 8px; line-height: 15px; }"),
        selectInput("pselect", label = NULL, 
                    choices = nfields, 
                    selected = 1))
    }
    else if( pv$pcurrent.level == 8 & input$pview != "Partners") {
      
      # send a query to get all the levels of S6- dfm1
      fields = pv$all.sales.rep.names # choose all rows of 1st column, get this list from mekko.blueprint()
      nfields <- rbind(c("Select an Account Manager"), fields)
      div(
        tags$style(type='text/css', ".selectize-input { padding: 1px; min-height: 0;} .selectize-dropdown-content { font-size: 8px; line-height: 15px; }"),
        selectInput("pselect", label = NULL, 
                    choices = nfields, 
                    selected = 1))
    }else if( pv$pcurrent.level == 9 & input$pview != "Partners") {
      
      # send a query to get all the levels of S6- dfm1
      fields = pv$all.sales.rep.names
      nfields <- rbind(c("Select a SAV Name"), fields)
      div(
        tags$style(type='text/css', ".selectize-input { padding: 1px; min-height: 0;} .selectize-dropdown-content { font-size: 8px; line-height: 15px; }"),
        selectInput("pselect", label = NULL, 
                    choices = nfields, 
                    selected = 1))
    }else{
      print("DONE with Pdropdown ********************")
      return()
    }
  })
  
  # # Create a download button to output at SAV ID - Tech level and display the BUnit and Product Family at
  output$pdownloadBU_data <- renderUI({
    print("Download BU data")
    if(input$pview =="Partners"){ # dont show download for Partners view
      return()
    }
    if( pv$pcurrent.level == 9 & pv$varstring == "Technology" ) { # if we are in Technology in SL6 and there is a dfm3 (val/volume graph), then dont show download
        return()
    }
    if( pv$pcurrent.level >= 9 & !is.null(pv$dfm3)) { # this is the Technology Group and Tecnhology levels, SAV Group name has been Chosen
          downloadButton('pdownloadBU', 'Download BU-Products Info',style= "color:black;")
    }else{
        return()
    }
  })
  
  # Function to download the Accounts in each level in mekko db, if action button is clicked
  output$pdownloadBU <- downloadHandler(
    
    # This function returns a string which tells the client
    # browser what name to use when saving the file.
    filename = function() {
      filter.str = paste(pv$sel.list[-1],collapse = "->")
      if(filter.str== ""){ # if empty because 1st page, no filters, then enter No filters
        filter.str= "NoFilters"
      }
      filter.str = paste0(filter.str,"_BUProducts")
      paste(filter.str, 'csv', sep = ".")
    },
    
    # This function should write data to a file given to it by
    # the argument 'file'.
    content = function(file) {
      # Wallet is in raw numbers, not converted to Billions
      # Include Wallet, SAVID, BEID
      down_str = sprintf("select D.* from Download_for_mekko_Q3FY17 D 
                         join 
                         (select Sales_Account_ID_INT 
                         from %s %s
                         group by Sales_Account_ID_INT
                         ) M
                         on D.SAV_ID = M.Sales_Account_ID_INT",table_mekko, pv$where_clause)
      
      print("Download Query BU Products")
      print(down_str)
      query <- dbSendQuery(memdb,down_str)
      results = fetch(query, n=-1)
      
      # Write to a file specified by the 'file' argument
      write.table(results, file, sep = ',',row.names = FALSE)
    })
  
  
  # Dropdown to select the FY in Current Level 2- landing page
  output$pfy_dropdown <- renderUI({
    print('FY Dropdown render UI')
    if( pv$pcurrent.level >= 2) {
      
      div(
        tags$style(type='text/css', ".selectize-input { padding: 1px; min-height: 0;} .selectize-dropdown-content { font-size: 8px; line-height: 15px; }"),
        selectInput("pselect_fy", label = NULL, 
                    choices = pv$all_years, 
                    selected = paste0("FY",pv$fy)))
    }
    else{
      return()
    }
  })
  
  # Function to process select_fy input from the FY dropdown and set the reactive value
  observeEvent(input$pselect_fy,{
    print("Observe FY Dropdown")
    a = gsub("FY","",input$pselect_fy) # set the fy as reactive value in current level 2
    pv$fy = as.numeric(a)
    pv$no_bic = 0  # reset everytime dropdown changes
  })
  
  # func --------------------------------------------------------------------
  
  dropdownButton <- function(label = "", status = c("default", "primary", "success", "info", "warning", "danger"), ...,ml = 32, width = NULL) {
    dmenu_str = ''
    if (!is.null(width)){
      dmenu_str <- paste0(dmenu_str,"width: ", validateCssUnit(width), ";")
    }
    dmenu_str <- paste0(dmenu_str,"background-color:white;overflow-y:auto; max-height:120px;color:black;")
    status <- match.arg(status)
    # dropdown button content
    html_ul <- list(
      class = "dropdown-menu",
      style = dmenu_str,
      lapply(X = list(...), FUN = tags$li, style = "margin-left: 10px; margin-right: 10px; font-size: 10px;margin-top:-20px;")
    )
    # dropdown button appearence
    html_button <- list(
      class = paste0("btn btn-", status," dropdown-toggle"),
      type = "button", 
      `data-toggle` = "dropdown",
      `style` = 'height:24px; padding-top:0px; padding-left:5px;width:93%; text-align:left;overflow:hidden;'
    )
    html_button <- c(html_button, list(label))
    #"float:right; margin-top:8px;border-width:5px;" 
    caret_style <- paste0("float:right; margin-top:8px;border-width:5px;")
    html_button <- c(html_button, list(tags$span(class = "caret",style=caret_style)))
    # final result
    tags$div(
      class = "dropdown",
      do.call(tags$button, html_button), 
      do.call(tags$ul, html_ul),
      tags$script(
        "$('.dropdown-menu').click(function(e) {
                  e.stopPropagation();
          });"),
      tags$script(
        ' // first time, check all techgroups
          var all_tg = $("input[value=\'All Products and Services\']").is(":checked");
          // if All Products and Services are checked, then select all

          if(all_tg){
                    $("input[name=\'pselect_tech_grps\']").prop("checked", true);
          }
          // select all sav segs,
          var all_sav = $("input[value=\'All SAV Segments\']").is(":checked");
          if(all_sav){
                    $("input[name=\'pselect_savseg\']").prop("checked", true);
          }

          // select all verticals,
          var all_vert = $("input[value=\'All Verticals\']").is(":checked");
          if(all_vert){
                    $("input[name=\'pselect_vertical\']").prop("checked", true);
          }

          // select all technologies at first 
          var all_techn = $("input[value=\'All Technologies\']").is(":checked");
          if(all_techn){
          $("input[name=\'pselect_technologies\']").prop("checked", true);
          }


          // if All Products and Services is checked off, then unselect all
          // else select all
          $("input[value=\'All Products and Services\']").change(function() {
                  var all_tg = $("input[value=\'All Products and Services\']").is(":checked");
                  // if All Products and Services are checked, then select all
                  if(all_tg){
                        $("input[name=\'pselect_tech_grps\']").prop("checked", true);
                  }else{
                        $("input[name=\'pselect_tech_grps\']").prop("checked",false);
                  }
          });

          // if All tech prod is checked on, then select all but services and All Products and Services
          $("input[value=\'All Products\']").change(function() {
                  var all_tp = $("input[value=\'All Products\']").is(":checked");
                  // if rod are checked, then select all
                  if(all_tp){
                      $("input[name=\'pselect_tech_grps\']").not("input[value=\'SERVICES\']").not("input[value=\'All Products and Services\']").prop("checked", true);
                      $("input[value=\'SERVICES\']").prop("checked", false); //uncheck Services
                      $("input[value=\'All Products and Services\']").prop("checked", false); //uncheck All Products and Services
                  }
          });

          // if All SAV SEG is checked off, then unselect all
          // else select all
          $("input[value=\'All SAV Segments\']").change(function() {
                  var all_tg = $("input[value=\'All SAV Segments\']").is(":checked");
                  // if All Products and Services are checked, then select all
                  if(all_tg){
                      $("input[name=\'pselect_savseg\']").prop("checked", true);
                  }else{
                      $("input[name=\'pselect_savseg\']").prop("checked",false);
                  }
          });

          // if All verticals is checked off, then unselect all
          // else select all
          $("input[value=\'All Verticals\']").change(function() {
              var all_tg = $("input[value=\'All Verticals\']").is(":checked");
              // if all verticals are checked, then select all
              if(all_tg){
              $("input[name=\'pselect_vertical\']").prop("checked", true);
              }else{
              $("input[name=\'pselect_vertical\']").prop("checked",false);
              }
          });

          // if All technologies is checked off, then unselect all
          // else select all
          $("input[value=\'All Technologies\']").change(function() {
          var all_techn = $("input[value=\'All Technologies\']").is(":checked");
          // if all techs are checked, then select all
          if(all_techn){
          $("input[name=\'pselect_technologies\']").prop("checked", true);
          }else{
          $("input[name=\'pselect_technologies\']").prop("checked",false);
          }
          });
        '
          
          
        )
      )
  } # end dropdownButton
  
  #####################################################################
  output$ptechgrps_dropdown <- renderUI({
    print("TechGroup Dropdown...")
    if( pv$pcurrent.level >= 2) {
      if(grepl('Sales_Account_ID_INT', pv$where_clause) | grepl('SalesRepName', pv$where_clause)){
        table_name = table_mekko
      }else{
        table_name = pv$table_name
      }
      print("Start TECH 666666666666666666666666666666666666666666666666666666666666666666666666666666")
      query <- dbSendQuery(memdb3,sprintf("select TechnologyGroup as tech_grps from %s %s group by 1", table_name, pv$where_clause)) 
      tg = fetch(query, n=-1)
      print("END TECH 66666666666666666666666666666666666666666666666666666666666666666666666666666666666")
      fields =  rbind( data.frame(tech_grps= c('All Products and Services','All Products')), tg)
      # remove any selected choices that are no longer valid fields due a filter being used in the Technologies dropdown
      tmp = c()
      for(i in pv$tech_grp){
        if( i %in% fields$tech_grps){
          tmp <- c(tmp, i)
        }
      }
      pv$tech_grp <- tmp
      dropdownButton(
        width = 200,
        ml = 32,
        label = paste(pv$tech_grp, collapse = ","), status = "default", 
        checkboxGroupInput(inputId = "pselect_tech_grps", label= "",choices = fields$tech_grps, selected= pv$tech_grp) # keep the existing selection
        ,actionButton(inputId = "submit_techgrps", label = "Apply",style= "font-size:8px;padding-top:6px; margin-top:10px;")
      )
      
    }else{
      return()
    }
  })
  
  # when Submit Tech Grps button is clicked, then update the checkbox
  # reset the tech groups if page refreshes else keep the existing selected tech grp
  observeEvent(input$submit_techgrps,{
    print("Submit Techgroups ")
    if(! is.null(input$submit_techgrps)){
        if("All Products and Services" %in% input$pselect_tech_grps){
          pv$tech_grp = c("All Products and Services")
        }else if("All Products" %in% input$pselect_tech_grps & "SERVICES" %in% input$pselect_tech_grps ){
          pv$tech_grp = c("All Products","SERVICES")
        }
        else if("All Products" %in% input$pselect_tech_grps){
          pv$tech_grp = c("All Products")
        }else{
          pv$tech_grp = input$pselect_tech_grps
        }
    }
  })
  
  output$psavseg_dropdown <- renderUI({
    print("SAVSEG Dropdown")
    if( pv$pcurrent.level >= 2) {
     print("Get All SAVSEG dropdown 7777777777777777777777777777777777777777777777777777777777777777777777777")
      if(grepl('Sales_Account_ID_INT', pv$where_clause) | grepl('SalesRepName', pv$where_clause)){
        table_name = table_mekko
      }else{
        table_name = pv$table_name
      }
      query <- dbSendQuery(memdb2,sprintf("select SAV_Segment from %s %s group by 1", table_name,pv$where_clause)) 
      tg = fetch(query, n=-1)
      print("7777777777777777777777777777777777777777777777777777777777777777777777777777")
      fields =  c( 'All SAV Segments', tg$SAV_Segment)
      dropdownButton(
        width = 200,
        ml= 27,
        label =  paste(pv$savseg, collapse = ","), status = "default", 
        checkboxGroupInput(inputId = "pselect_savseg", label= "",choices = fields, selected= pv$savseg) # keep the existing selection
        ,actionButton(inputId = "submit_savseg", label = "Apply", style= "font-size:8px;padding-top:1px; margin-top:10px;")
      )
      
    }
    else{
        return()
    }
  })
  # after the submit sav segment is clicked, save the selected sav segment(s)
  observeEvent(input$submit_savseg,{
    print("Submit SAVSEG")
    if(! is.null(input$submit_savseg)){
      if("All SAV Segments" %in% input$pselect_savseg){
        print("ALL sav segments chosen ")
          pv$savseg = c('All SAV Segments')
      }else{
        print("NOT ALL sav segments chosen ")
          pv$savseg = input$pselect_savseg
      }
    }
  })
  
  output$pvert_dropdown <- renderUI({
    print("Vertical Dropdown")
    if( pv$pcurrent.level >= 2) {
      print("Veticals 888888888888888888888888888888888888888888888888888888888888888888888888888888888888888888888")
      if(grepl('Sales_Account_ID_INT', pv$where_clause) | grepl('SalesRepName', pv$where_clause)){
        table_name = table_mekko
      }else{
        table_name = pv$table_name
      }
      query <- dbSendQuery(memdb,sprintf("select VerticalMarket from %s %s group by 1", table_name,pv$where_clause)) 
      tg = fetch(query, n=-1)
      print("end Verticals 888888888888888888888888888888888888888888888888888888888888888888888888888888")
      fields =  c( 'All Verticals', tg$VerticalMarket)
      dropdownButton(
        width = 200,
        ml= 27,
        label =  paste(pv$vert, collapse = ","), status = "default", 
        checkboxGroupInput(inputId = "pselect_vertical", label= "",choices = fields, selected= pv$vert) # keep the existing selection
        ,actionButton(inputId = "submit_vert", label = "Apply",style= "font-size:8px;padding-top:1px; margin-top:10px;")
      )
      
    }
    else{
      return()
    }
  })
  # after the submit sav segment is clicked, save the selected sav segment(s)
  observeEvent(input$submit_vert,{
    print("Observe SUbmit Vertical")
    if(! is.null(input$submit_vert)){
      if("All Verticals" %in% input$pselect_vertical){
          pv$vert = c("All Verticals")
      }else{
          pv$vert = input$pselect_verticalkRISH.123
          
      }
    }
  })
  # multiple select dropdown for Technologies
  output$ptechnologies_dropdown <- renderUI({
    print("Technoloiges Dropdown")
    if( pv$pcurrent.level >= 2) {
      print("technologies 101010100101001010100100101001010100101010010100101")
      # change the table name to the detailed table if SAVID or SalesRepName is in the where clause
      if(grepl('Sales_Account_ID_INT', pv$where_clause) | grepl('SalesRepName', pv$where_clause)){
        table_name = table_mekko
      }else{
        table_name = pv$table_name
      }
      query <- dbSendQuery(memdb,sprintf("select Technology from %s %s group by 1 order by Technology", table_name, pv$where_clause)) 
      tg = fetch(query, n=-1)
      print("end Technologies 10101010100101010010010010101010100101010010101001010")
      # remove any previous selected choices that are no longer valid choices due to a Tech Group being used a filter
      fields =  c( 'All Technologies', tg$Technology)
      tmp = c()
      for(i in pv$technologies){
        if( i %in% fields){
          tmp <- c(tmp, i)
        }
      }
      pv$technologies <- tmp
      dropdownButton(
        width = 200,
        ml= 27,
        label =  paste(pv$technologies, collapse = ","), status = "default", 
        checkboxGroupInput(inputId = "pselect_technologies", label= "",choices = fields, selected= pv$technologies) # keep the existing selection
        ,actionButton(inputId = "submit_technologies", label = "Apply",style= "font-size:8px;padding-top:1px; margin-top:10px;")
      )
      
    }
    else{
      return()
    }
  })
  # after the submit sav segment is clicked, save the selected sav segment(s)
  observeEvent(input$submit_technologies,{
    print("Observe SUbmit Technologies")
    if(! is.null(input$submit_vert)){
      if("All Technologies" %in% input$pselect_technologies){
        pv$technologies = c("All Technologies")
      }else{
        pv$technologies = input$pselect_technologies
      }
    }
  })

  
  output$pbeid_partnername <- renderUI({
    print(" START Beid Partner name Dropdown&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
    if( pv$pcurrent.level >= 2 ) {
      
      
      if(pv$pcurrent.level == 2){
        print("BEID Dropdown For Current level ==2")
        # need to change to mekkoqn since sav account name is needed
        if(is.null(pv$fy)){
          print("FY IS NULLLLLLLLLLLLL*****************************************************************************************************************************************************************************************************************************************************************")
          fy_str = pv$fy
        }
        else{
          fy_str = pv$fy
        }
        
        query <- dbSendQuery(memdb3,sprintf("select PTNR_NAME_ID
                                           from %s
                                           where PTNR_NAME_ID != '' AND FY = %s
                                           group by 1 limit 1000",pv$table_name,fy_str)) 
      }
      else{
        # the where_clause may be in a Partner's view and may have 'Sales_Account_ID_INT' as a filter. If so, use original mekko table
        if(grepl('Sales_Account_ID_INT', pv$where_clause) | grepl('SalesRepName', pv$where_clause)){
          table_name = table_mekko
        }else{
          table_name = pv$table_name
        }
        query <- dbSendQuery(memdb3,sprintf("select PTNR_NAME_ID
                                            from %s
                                            %s AND PTNR_NAME_ID != ''
                                            group by 1 limit 1000", table_name,pv$where_clause))
      }
      tg = fetch(query, n=-1)
      print("END PNAME QUery..&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
      fields_beid =  c("All PartnerNames (BE-ID)", tg$PTNR_NAME_ID)
      div(
        tags$style(type='text/css', ".selectize-input { padding: 2px; min-height: 0;} .selectize-dropdown-content { font-size: 8px; line-height: 15px; }"),
        selectInput("partner_beid", label = NULL, 
                    choices = fields_beid,
                    selected = pv$beid)
      )
      }else{
        return()
      }
  })
  
  
  # Function to process select_beid input from the Partner Name- BEID dropdown and set the reactive value
  observeEvent(input$partner_beid,{
    print("Observe Dropdown for Partner Beid")
    pv$beid = input$partner_beid # save the selection in a global var
    print("observe Event pselect beid")
    print(pv$beid)
  })
  
  #########################################################################  
  
  # When mekko is clicked, update selected segments and pcurrent.level
  observeEvent(input$pplot1_click,{
   print("PLOT CLICK...")
    # if previously, the unnamed flag is on, the when current.level is 7, disable click
    # since there is no salesrepname and SAV account group name for those account types
    if(pv$is_unnamed_prospects == 'unnamed' & pv$pcurrent.level==7 & input$pview == "Sales Hierarchy"){
      print("Setting CL 7 to 11........................................................")
      return()
    }
    dfm1 = pv$dfm1
    max.x = max(dfm1$xmax)
    # then get the selected segment where click occurred
    x_coord = input$pplot1_click$x
    y_coord = input$pplot1_click$y
    # Make sure user can click in boundary of the mekko 
    if(x_coord <= 0 || x_coord>= max.x || y_coord <=0 || y_coord>=100){
      return() #click is meaningless 
    }
    # get the segment that is clicked on
    row = dfm1[dfm1$xmin <=x_coord & dfm1$xmax>= x_coord,]
    sel.seg = row[1,1] # this contains the x_category that the user clicked 
    istg = 0
    if(input$pview == "Partners" & pv$partner_SAVID == 0){ # remove (Be geo id from string)
      uncleaned_p = sel.seg
      pn <- gsub("\\s*\\([^\\)]+\\)","",sel.seg)
      bgid = gsub(")","" ,unlist(strsplit(sel.seg,"BE-GEO-ID: "))[2])
      cleaned_partner <- paste0(pn,"\"", " AND BE_GEO_ID = ","\"",bgid)
      istg = 1
    }
    else if(input$pview == "Partners" & pv$partner_SAVID == 1){ # remove (Be geo id from string)
      
      uncleaned_p = sel.seg
      bgid = gsub(")","" ,unlist(strsplit(uncleaned_p,"SAVID: "))[2])
      cleaned_partner <- bgid
      istg = 1
      
    }
    
    #if Next Top 40 or Rest in Partners, then no click
    if(input$pview== "Partners" & (sel.seg == 'Next Top 40' || sel.seg =='Rest')){
      return()
    }
    
    if((input$pview2 == 'SAV Segments' || input$pview2 == 'Verticals') & pv$partner_tech_grps==1){
      return()
    }
    
    if((input$pview2 == 'SAV Segments' || input$pview2 == 'Verticals') & pv$partner_SAVID==1){
      return()
    }
    
    if(pv$goback_techgroups == 1 || (pv$pcurrent.level ==11 & input$pview=='Technologies')){
      if(input$pview =="Partners" & pv$partner_technologies == 1){
        pv$partner_tech_grps = 1
        pv$partner_technologies = 0
      }
      pv$pcurrent.level = pv$pcurrent.level -1
      pv$tech_breakdown = 0 
      # remove the last item
      pv$sel.list <- pv$sel.list[-length(pv$sel.list)]
      pv$clean.sel.list <- pv$clean.sel.list[-length(pv$clean.sel.list)]
      return()
    }
    
    if(input$pview == 'SAV Segments' || input$pview == 'Verticals' || pv$tech_breakdown ==1 ){
      #if tech is chosen or currnt level is on Sale Rep or SAV Names, click is meaningless
      return()
    }
    
    if(input$pview == "Technologies"){
      pv$tech_breakdown = 1
    }
    ############################################# TURN Partner Flags On/Off#######################################
    
    #if in Partnerview- SAVID view and there is a click, then you will be in TechGrp view next
    if(input$pview == "Partners" & pv$partner_SAVID ==1){
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 1
      pv$partner_technologies = 0
    }
    # if in Partner Tech Grp view and there is a click, that means you will be Technologies view next
    else if(input$pview == "Partners" & pv$partner_tech_grps ==1){ # if in Partner view and in Techgroup view, then you will be in Technologies
      pv$partner_SAVID = 0
      pv$partner_technologies = 1
      pv$partner_tech_grps = 0
    }
    # if in Partner Technologies view and there is a click, that means you will go back to Tech Grp view
    else if(input$pview =="Partners" & pv$partner_technologies == 1){ # if in Partner view, and in Technologies view, then you will be in Tech Groups back again
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 1
      pv$partner_technologies = 0
      pv$pcurrent.level = pv$pcurrent.level -1 # decrease the level
      sel.list = pv$sel.list 
      pv$sel.list <- sel.list[-length(sel.list)] # remove the last item- Technology Grp
      # remove the last item from clean.sel.list
      pv$clean.sel.list <- pv$clean.sel.list[-length(pv$clean.sel.list)]
      return()
    }
    # if in Partner  view only  and there is a click, that means you will be in SAVID view next
    else if(input$pview == "Partners"){ # if in Partners view and not in techgrp, then go to techgroup
      pv$partner_SAVID = 1
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    
    # if others in a view besides technologies, no click
    if((input$pview == "SAV Group" || input$pview == "Acct Mngrs") & sel.seg == "ETC" & pv$pcurrent.level <10){
      return()
    }
    
    #if x_coord located in OTHERS segment, get the companies in OTHERS as a list
    if(length(sel.seg) ==0){
      sel.seg = pv$others
    }
    if(pv$pcurrent.level == 8 || pv$pcurrent.level ==9){
      
      #if there is a dropdown selected or a double click, 
      pv$sel.dropdown1 = sel.seg
    }
    clean.seg = row$cleanx[1]
    
    if(input$pview == "Partners" & istg==1){
      pv$sel.dropdown1 = cleaned_partner
      print("IN ISTG!!!")
      print(cleaned_partner)
      pv$clean.sel.list[[pv$pcurrent.level]]= uncleaned_p
      # also save the selected segment that was clicked by user for each level in the mekko
      pv$sel.list[[pv$pcurrent.level]]= cleaned_partner
    }else{
      # everytime a filter is made, save the clean version of the filter selection in a list to display to UI
      pv$clean.sel.list[[pv$pcurrent.level]]=clean.seg
      # also save the selected segment that was clicked by user for each level in the mekko
      pv$sel.list[[pv$pcurrent.level]]= sel.seg
    }
    pv$pcurrent.level <- pv$pcurrent.level + 1
  })  
  
  # Output the Filters as links only for current levels >=2
  output$pfiltered_sel <- renderUI({
    print("Filtered Selection....")
    if(pv$pcurrent.level >=2){
      # for each item in the clean filter list, create an action list
      L <- vector("list",10)  
      if(length(pv$clean.sel.list) <=1){
        action.link.id = paste0("pfilter_",1)
        action.link.label = paste("TOP")
        L[[1]] <- actionLink(action.link.id, action.link.label)
      }else{ # list not empty
        for(i in 1:(length(pv$clean.sel.list)-1)){ # skip the first value which is NULL
          
          action.link.id = paste0("pfilter_",i)
          # replace the first item (NULL) with No Filters to signify top
          if(is.null(pv$clean.sel.list[[i]]) || is.na(pv$clean.sel.list[[i]])){
            action.link.label = "TOP -->"
          }else{
            action.link.label = paste(pv$clean.sel.list[[i]], "-->")
          }
          L[[i]] <- actionLink(action.link.id, action.link.label)
        }
        # add the last item to filtered list
        i = length(pv$clean.sel.list)
        action.link.id = paste0("filter_",i)
        action.link.label = paste(pv$clean.sel.list[[i]])
        L[[i]] <- div(id = action.link.id, action.link.label, style = "display:inline;")
      }
      
      return(L)     
      #return(paste(paste(pv$clean.sel.list[-1], collapse = ' --> '))) # exclude sav_segment
    }
  })
  
  # Output the Current Level string, the FY, and the Selected Tech Group
  output$pcurr_level <- renderText({
    print("Print Current Level")
    ptechgrp_str = paste(pv$tech_grp, collapse = ",")
    # append the Technologies selection to the ptechgrp_str
    ptechgrp_str <- paste0(ptechgrp_str, " Technologies: ")
    ptechgrp_str <- paste0(ptechgrp_str, paste(pv$technologies, collapse = ","))
    # append the PartnerName Beid selection to the ptechgrp_str
    ptechgrp_str <- paste0(ptechgrp_str,", ", input$partner_beid)
    # append the SAV Segments selection to the ptechgrp_str
    ptechgrp_str <- paste0(ptechgrp_str,", ", paste(pv$savseg, collapse = ","))
    # append the Verticals selection to the ptechgrp_str
    ptechgrp_str <- paste0(ptechgrp_str,", ", paste(pv$vert, collapse = ","))
    
    #if input view is on tech, then display a different sales level which is the previous sales level for those technologies
    if(input$pview =="Technologies"){
      return(paste("Technologies ","," ,"    FY ",pv$fy, ",  ", ptechgrp_str,sep = ""))
    }
    else if(input$pview =="Verticals"){
      return(paste("Verticals ","," ,"    FY ",pv$fy, ",  ", ptechgrp_str,sep = ""))
    }
    else if(input$pview =="SAV Segments"){
      return(paste("SAV Segments ","," ,"    FY ",pv$fy, ",  ", ptechgrp_str,sep = ""))
    }else if(pv$partner_SAVID ==1 & input$pview2 =="Verticals" ){
      return(paste("Verticals for the Selected Partner ","," ,"    FY ",pv$fy, ",  ", ptechgrp_str,sep = ""))
    }else if(pv$partner_SAVID ==1 & input$pview2 =="SAV Segments" ){
      return(paste("SAV Segments for the Selected Partner ","," ,"    FY ",pv$fy, ",  ", ptechgrp_str,sep = ""))
    }else if(pv$partner_SAVID ==1){
      return(paste("SAV Names for Selected Partner ","," ,"    FY ",pv$fy, ",  ", ptechgrp_str,sep = ""))
    }
    # if going from Partner Verticals/SAV Seg to another LEVEL, then show Partner Names
    else if(input$pview =="Partners" & input$pview2 =="Verticals" & pv$partner_tech_grps ==0){
      return(paste("Partner Names ","," ,"    FY ",pv$fy, ",  ", ptechgrp_str,sep = ""))
    }
    else if(input$pview =="Partners" & input$pview2 =="SAV Segments" & pv$partner_tech_grps ==0){
      return(paste("Partner Names ","," ,"    FY ",pv$fy, ",  ", ptechgrp_str,sep = ""))
    }
    else if(input$pview =="Partners" & input$pview2 =="SAV Segments" & pv$partner_tech_grps ==1){
      return(paste("SAV Segments for Selected Partner,SAV Name ","," ,"    FY ",pv$fy, ",  ", ptechgrp_str,sep = ""))
    }
    else if(input$pview =="Partners" & input$pview2 =="Verticals" & pv$partner_tech_grps ==1){
      return(paste("Verticals for Selected Partner,SAV Name ","," ,"    FY ",pv$fy, ",  ",ptechgrp_str,sep = ""))
    }
    else if(input$pview =="Partners" & input$pview2 =="SAV Segments"){
      return(paste("SAV Segments for Selected Partner ","," ,"    FY ",pv$fy, ",  ", ptechgrp_str,sep = ""))
    }
    else if(input$pview =="Partners" & input$pview2 =="Verticals" ){
      return(paste("Verticals for Selected Partner ","," ,"    FY ",pv$fy, ",  ",ptechgrp_str,sep = ""))
    }
    else if(pv$partner_tech_grps ==1){
      return(paste("Technology Groups for Selected Partner,SAV Name ","," ,"    FY ",pv$fy, ",  ", ptechgrp_str,sep = ""))
    }else if(pv$partner_technologies ==1){
      return(paste("Technology for Selected Partner, SAV Name ","," ,"    FY ",pv$fy, ",  ", ptechgrp_str,sep = ""))
    }
    else if(input$pview== "Partners"){
      return(paste("Partner Names ","," ,"    FY ",pv$fy, ",  ", ptechgrp_str,sep = ""))
    }
    
    return(paste("Sales ",mekko.level.str[[pv$pcurrent.level]],"," ,"    FY ",pv$fy, ",  ", ptechgrp_str,sep = "")) 
  })
  
  
  # when dropdown selection is made, save the selection as a reactive value
  observeEvent(input$pselect,{
    print(" SAV Name, BEGEOID PArtner Name, Sales rep dropdown")
    if(input$pselect =="Select an Account Manager"){ # if no selection is made, then dont do anything
      return()
    }else if(input$pselect == "All PartnerNames (BE-GEO-ID)"){
      return()
    }else if(input$pselect =="Select a SAV Name"){
      return()
    }
    # if a partner name is chosen from the dropdown, add it to the sel.list and increase the pcurrent.level
    if(input$pview == "Partners" & pv$partner_SAVID ==0 & pv$partner_tech_grps==0 & pv$partner_technologies ==0){
      print('Partner Name Dropdown to be Placed********************')
      pv$partner_SAVID = 1
      pv$partner_tech_grps=0
      pv$partner_technologies =0
      uncleaned_partner = input$pselect
      remove_num = unlist(strsplit(uncleaned_partner,"-"))[1]
      sel.seg = gsub(paste0(remove_num,"-"),"",uncleaned_partner)
      pn =  gsub("\\s*\\([^\\)]+\\)","",sel.seg)
      bgid = gsub(")","" ,unlist(strsplit(sel.seg,"BE-GEO-ID: "))[2])
      cleaned_partner <- paste0(pn,"\"", " AND BE_GEO_ID = ","\"",bgid)
      # everytime a filter is made, save the clean version of the filter selection in a list to display to UI
      pv$clean.sel.list[[pv$pcurrent.level]]= sel.seg
      # also save the selected segment that was clicked by user for each level in the mekko
      pv$sel.list[[pv$pcurrent.level]]= cleaned_partner
      print(cleaned_partner)
    }
    else if(input$pview == "Partners" & pv$partner_SAVID ==1){
      print("SAVID Dropdown to be Placed************************************** ")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 1
      pv$partner_technologies =0
      uncleaned_partner = input$pselect
      bgid = gsub(")","" ,unlist(strsplit(uncleaned_partner,"SAVID: "))[2])
      cleaned_partner <- bgid
      print('Cleaned Partner')
      print(cleaned_partner)
      # everytime a filter is made, save the clean version of the filter selection in a list to display to UI
      pv$clean.sel.list[[pv$pcurrent.level]]= uncleaned_partner
      # also save the selected segment that was clicked by user for each level in the mekko
      pv$sel.list[[pv$pcurrent.level]]= cleaned_partner
      print(cleaned_partner)
    }
    else{ # Not in Partner Views
      print("Not in Partners dropwdown to be placed ********************")
      pv$sel.dropdown1 = input$pselect
      # everytime a filter is made, save the clean version of the filter selection in a list to display to UI
      pv$clean.sel.list[[pv$pcurrent.level]]= input$pselect
      # also save the selected segment that was clicked by user for each level in the mekko
      pv$sel.list[[pv$pcurrent.level]]= input$pselect
      print(input$pselect)
    }
    
    
    pv$pcurrent.level <- pv$pcurrent.level + 1 # increment the current level
  })
  
  # when user hovers over a mekko rectangle, display the info
  output$phover_info <- renderUI({
    print("Hover info...")
    if(!is.null(input$pplot_hover)){ #if there is a hover on mekko
      dfm1 = pv$dfm1
      max.x = max(dfm1$xmax)
      # then get the selected segment where hover occurred
      x_coord = input$pplot_hover$x
      y_coord = input$pplot_hover$y
      
      # Make sure no hover message is shown when hover on the white space between the mekko and pink table
      if(y_coord <=0 & y_coord >-10){
        return("Hover or Double Click the Mekko to view more details ...")
      }
      if(x_coord > max.x || x_coord <0.0){
        return("Hover or Double Click the Mekko to view more details ...")
      }
      
      if(y_coord >=101){ #if at the top of the mekko, display only the totals for that column
        sel.seg = dfm1[dfm1$xmin <=x_coord & dfm1$xmax>=x_coord,][1,] #use the placement of x_coord to find which segment is hovered on.
        num = sel.seg$total.col
        return(paste(sel.seg$cleanx ,": $",num," in"," Total Wallet, which is ",round_any(sel.seg$xsub,1),"% of the Grand Total Wallet",sep = ""))
      }
      else if(y_coord<=-10){ #if hovered on the pink table, display the number of accounts
        sel.seg = dfm1[dfm1$xmin <=x_coord & dfm1$xmax>=x_coord,][1,] #use the placement of x_coord to find which segment is hovered on.
        return(paste(sel.seg$cleanx ," #Partners: ", sel.seg$num.accts.str,", Avg. Wallet/Partner: ",sel.seg$avg.acct.wallet,
                     "  #Accounts: ", sel.seg$num.accts.str2,", Avg. Wallet/Acct: ",sel.seg$avg.acct.walletsav,
                     sep = ""))
      }
      else{
        sel.seg = dfm1[dfm1$xmin <= x_coord & dfm1$xmax >=x_coord & dfm1$ymin <=y_coord & dfm1$ymax>=y_coord,]
        sel.seg = sel.seg[1,] #get only first row
        if(sel.seg$variable == "SWOpct"){
          wallet.type= "Static Opportunity"
        }else if(sel.seg$variable == "AdditionalBICpct"){
          wallet.type= "Incr. Best-In-Class"
        }else{
          wallet.type= "Uncaptured"
        }
        num = sel.seg$Number
        sentence = paste(sel.seg$cleanx ,": $",num," in ",wallet.type , ", which is ", round(sel.seg$value,0) ,"% of its Total Wallet",sep = "")
        # create sentence for the Val/Vol/Unk if dfm3 exists
        if(is.null(pv$dfm3)  ){
          return(HTML(sentence))
        }
        if(wallet.type != "Static Opportunity"){
          return(HTML(sentence))
        }
        dfm3 = pv$dfm3
        val_sent = NULL
        vol_sent = NULL
        unk_sent = NULL
        
        #if value >0, create value sentence, repeat for volume and unk
        sel_seg_dfm3 = dfm3[dfm3[1]== sel.seg[1,1],]
        # if sel segment does not have corresponding row in dfm3, then that Tech has no information and we should only display static wallet
        if(nrow(sel_seg_dfm3)==0){
          return(HTML(sentence))
        }
        value = sel_seg_dfm3[sel_seg_dfm3$type =="Value", "pct_Static"]
        if(value > 0){
          val_sent = sprintf("%s%% in High-end products", value) 
        }
        volume = sel_seg_dfm3[sel_seg_dfm3$type =="Volume", "pct_Static"]
        if(volume > 0){
          vol_sent = sprintf("%s%% in Low-end products", volume) 
        }
        unk = sel_seg_dfm3[sel_seg_dfm3$type =="Unk", "pct_Static"]
        if(unk > 0){
          unk_sent = sprintf("%s%% in Non Classified products", unk) 
        }
        # join 3 parts with comma
        ll = data.frame(s = c(val_sent, vol_sent, unk_sent))
        sent2 = paste(ll$s, collapse = ", ")
        return(HTML(paste(sentence, sent2, sep="<br/>")))
      } 
    }else{
      if(!is.null(input$pplot2_hover)){ # if over the totals bar, return info
        dfm2 = pv$dfm2
        y_coord = round(input$pplot2_hover$y)
        if(y_coord>99 || y_coord <0){
          return("Hover or Double Click the Mekko to view more details ...")
        }
        
        sel.seg = dfm2[dfm2$ymin <=y_coord & dfm2$ymax>=y_coord,]
        sel.seg = sel.seg[1,] #get only first row
        if(sel.seg$variable == "SWOpct"){
          wallet.type= "Static Opportunity"
        }else if(sel.seg$variable == "AdditionalBICpct"){
          wallet.type= "Incr. Best-In-Class"
        }else{
          wallet.type= "Uncaptured"
        }
        num = sel.seg$Number
        
        return(paste(wallet.type ,": $",num,", which is ",round(sel.seg$value,0) ,"% of the Grand Total Wallet",sep = ""))
        
      }else{
        return("Hover or Double Click the Mekko to view more details ...")
      }  
    }
  }) #end hover ui output
  
  # Function to download the Accounts in each level in mekko db, if action button is clicked
  output$pdownload <- downloadHandler(
    
    # This function returns a string which tells the client
    # browser what name to use when saving the file.
    filename = function() {
      filter.str = paste(pv$sel.list[-1],collapse = "->")
      if(filter.str== ""){ # if empty because 1st page, no filters, then enter No filters
        filter.str= "NoFilters"
      }
      
      paste(filter.str, 'csv', sep = ".")
    },
    
    # This function should write data to a file given to it by
    # the argument 'file'.
    content = function(file) {
      # Wallet is in raw numbers, not converted to Billions
      # Include Wallet, SAVID, BEID
      down_str = sprintf("select FY, SAV_Segment, VerticalMarket, LEVEL_1, LEVEL_2,
                         LEVEL_3, LEVEL_4, LEVEL_5, LEVEL_6, TechnologyGroup, Technology,
                         Sales_Account_ID_INT,SALES_ACCOUNT_GROUP_NAME,
                         BE_GEO_ID, BE_ID,Partner_Name,
                         sum(Static_WS_Opportunity) as Static_Wallet_Opportunity, 
                         sum(BIC_WS_Opportunity-Static_WS_Opportunity) as BIC_Opportunity,
                         sum(Wallet - BIC_WS_Opportunity) as Uncaptured_Wallet,
                         sum(Wallet) as Total_Wallet 
                         from %s 
                         %s
                         group by FY, SAV_Segment, VerticalMarket, LEVEL_1, LEVEL_2,
                         LEVEL_3, LEVEL_4, LEVEL_5, LEVEL_6, TechnologyGroup, Technology,
                         Sales_Account_ID_INT,SALES_ACCOUNT_GROUP_NAME,
                         BE_GEO_ID, BE_ID,Partner_Name
                         order by Sales_Account_ID_INT desc, Total_Wallet desc
                         ",table_mekko, pv$where_clause)
      query <- dbSendQuery(memdb,down_str)
      results = fetch(query, n=-1)
      
      # Write to a file specified by the 'file' argument
      write.table(results, file, sep = ',',row.names = FALSE)
    })
  
  #### FUnction to go back to a selected filter level- do this for levels 2 -10######
  # function to go back to top of mekko - No filters when link is clicked
  observeEvent(input$pfilter_1, {
    prev_cl = pv$pcurrent.level
    # if on tech breakdown, switch off
    if(pv$tech_breakdown == 1){
      pv$tech_breakdown =0
    }
    pv$sel.list = NULL
    pv$clean.sel.list = NULL
    pv$pcurrent.level = 2
    filter_num = 1
    pv$beid = "All PartnerNames (BE-ID)"
    #when in partner technologies, if pcurrent.level -1 == filter number, then turn partner_techgrps on
    if(pv$partner_technologies ==1 & (prev_cl - filter_num == 2)){
      print("Filter: Go to Tech Grps")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 1
      pv$partner_technologies = 0
    }
    else if(pv$partner_technologies ==1 & (prev_cl - filter_num ==3)){
      print("Filter: Go to SAVID")
      pv$partner_SAVID = 1
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_technologies ==1 & (prev_cl - filter_num >3)){
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_tech_grps ==1 & (prev_cl - filter_num == 2 )){
      print("Filter: Go to SAVID")
      pv$partner_SAVID = 1
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_tech_grps ==1 & (prev_cl - filter_num > 2 )){
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_SAVID ==1 & (prev_cl - filter_num > 1 )){
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    
  })  
  
  # function to go back to go to SL2 (current level 3) - No filters when link is clicked
  observeEvent(input$pfilter_2, {
    prev_cl = pv$pcurrent.level
    # if on tech breakdown, switch off
    if(pv$tech_breakdown == 1){
      pv$tech_breakdown =0
    }
    
    # reset everything, go back to pcurrent.level =2
    new.sel.list = list()
    new.clean.sel.list = list()
    
    for(i in 1:2){
      new.sel.list[i] = pv$sel.list[i]
      new.clean.sel.list[i] = pv$clean.sel.list[i]
    }
    # replace the reactive values with the new lists
    pv$sel.list = new.sel.list
    pv$clean.sel.list = new.clean.sel.list
    pv$pcurrent.level = 3
    filter_num = 2
    #when in partner technologies, if pcurrent.level -1 == filter number, then turn partner_techgrps on
    if(pv$partner_technologies ==1 & (prev_cl - filter_num == 2)){
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 1
      pv$partner_technologies = 0
    }
    else if(pv$partner_technologies ==1 & (prev_cl - filter_num ==3)){
      pv$partner_SAVID = 1
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_technologies ==1 & (prev_cl - filter_num >3)){
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_tech_grps ==1 & (prev_cl - filter_num == 2 )){
      pv$partner_SAVID = 1
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_tech_grps ==1 & (prev_cl - filter_num > 2 )){
      
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_SAVID ==1 & (prev_cl - filter_num > 1 )){
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
  }) 
  # function to go back to go to SL3 (current level 4) - No filters when link is clicked
  observeEvent(input$pfilter_3, {
    prev_cl = pv$pcurrent.level
    print("Filter 3")
    print('prev_cl')
    print(prev_cl)
    # if on tech breakdown, switch off
    if(pv$tech_breakdown == 1){
      pv$tech_breakdown =0
    }
    # reset everything, go back to pcurrent.level =2
    new.sel.list = list()
    new.clean.sel.list = list()
    
    for(i in 1:3){
      new.sel.list[i] = pv$sel.list[i]
      new.clean.sel.list[i] = pv$clean.sel.list[i]
    }
    # replace the reactive values with the new lists
    pv$sel.list = new.sel.list
    pv$clean.sel.list = new.clean.sel.list
    pv$pcurrent.level = 4
    filter_num = 3
    #when in partner technologies, if pcurrent.level -1 == filter number, then turn partner_techgrps on
    if(pv$partner_technologies ==1 & (prev_cl - filter_num == 2)){
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 1
      pv$partner_technologies = 0
    }
    else if(pv$partner_technologies ==1 & (prev_cl - filter_num ==3)){
      pv$partner_SAVID = 1
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_technologies ==1 & (prev_cl - filter_num >3)){
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_tech_grps ==1 & (prev_cl - filter_num == 2 )){
      pv$partner_SAVID = 1
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_tech_grps ==1 & (prev_cl - filter_num > 2 )){
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_SAVID ==1 & (prev_cl - filter_num > 1 )){
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
  }) 
  
  # function to go back to go to SL4 (current level 5) - No filters when link is clicked
  observeEvent(input$pfilter_4, {
    prev_cl = pv$pcurrent.level
    print("Filter 4")
    # if on tech breakdown, switch off
    if(pv$tech_breakdown == 1){
      pv$tech_breakdown =0
    }
    new.sel.list = list()
    new.clean.sel.list = list()
    
    for(i in 1:4){
      new.sel.list[i] = pv$sel.list[i]
      new.clean.sel.list[i] = pv$clean.sel.list[i]
    }
    # replace the reactive values with the new lists
    pv$sel.list = new.sel.list
    pv$clean.sel.list = new.clean.sel.list
    pv$pcurrent.level = 5
    filter_num = 4
    #when in partner technologies, if pcurrent.level -1 == filter number, then turn partner_techgrps on
    if(pv$partner_technologies ==1 & (prev_cl - filter_num == 2)){
      print("Filter: Go to Tech Grps")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 1
      pv$partner_technologies = 0
    }
    else if(pv$partner_technologies ==1 & (prev_cl - filter_num ==3)){
      print("Filter: Go to SAVID")
      pv$partner_SAVID = 1
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_technologies ==1 & (prev_cl - filter_num >3)){
      print("Filter: Turn of All Partner Extra Flags")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_tech_grps ==1 & (prev_cl - filter_num == 2 )){
      print("Filter: Go to SAVID")
      pv$partner_SAVID = 1
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_tech_grps ==1 & (prev_cl - filter_num > 2 )){
      print("Filter: Turn of All Partner Extra Flags")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_SAVID ==1 & (prev_cl - filter_num > 1 )){
      print("Filter: Turn of All Partner Extra Flags")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    
  }) 
  
  # function to go back to go to SL5 (current level 6) - No filters when link is clicked
  observeEvent(input$pfilter_5, {
    prev_cl = pv$pcurrent.level
    print("Filter 5")
    # if on tech breakdown, switch off
    if(pv$tech_breakdown == 1){
      pv$tech_breakdown =0
    }
    new.sel.list = list()
    new.clean.sel.list = list()
    
    for(i in 1:5){
      new.sel.list[i] = pv$sel.list[i]
      new.clean.sel.list[i] = pv$clean.sel.list[i]
    }
    # replace the reactive values with the new lists
    pv$sel.list = new.sel.list
    pv$clean.sel.list = new.clean.sel.list
    pv$pcurrent.level = 6
    filter_num = 5
    #when in partner technologies, if pcurrent.level -1 == filter number, then turn partner_techgrps on
    if(pv$partner_technologies ==1 & (prev_cl - filter_num == 2)){
      print("Filter: Go to Tech Grps")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 1
      pv$partner_technologies = 0
    }
    else if(pv$partner_technologies ==1 & (prev_cl - filter_num ==3)){
      print("Filter: Go to SAVID")
      pv$partner_SAVID = 1
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_technologies ==1 & (prev_cl - filter_num >3)){
      print("Filter: Turn of All Partner Extra Flags")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_tech_grps ==1 & (prev_cl - filter_num == 2 )){
      print("Filter: Go to SAVID")
      pv$partner_SAVID = 1
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_tech_grps ==1 & (prev_cl - filter_num > 2 )){
      print("Filter: Turn of All Partner Extra Flags")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_SAVID ==1 & (prev_cl - filter_num > 1 )){
      print("Filter: Turn of All Partner Extra Flags")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
  }) 
  # function to go back to go to SL6 (current level 7) - No filters when link is clicked
  observeEvent(input$pfilter_6, {
    prev_cl = pv$pcurrent.level
    print("Filter 6")
    if(pv$tech_breakdown == 1){
      pv$tech_breakdown =0
    }
    new.sel.list = list()
    new.clean.sel.list = list()
    
    for(i in 1:6){
      new.sel.list[i] = pv$sel.list[i]
      new.clean.sel.list[i] = pv$clean.sel.list[i]
    }
    # replace the reactive values with the new lists
    pv$sel.list = new.sel.list
    pv$clean.sel.list = new.clean.sel.list
    pv$pcurrent.level = 7
    filter_num = 6
    #when in partner technologies, if pcurrent.level -1 == filter number, then turn partner_techgrps on
    if(pv$partner_technologies ==1 & (prev_cl - filter_num == 2)){
      print("Filter: Go to Tech Grps")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 1
      pv$partner_technologies = 0
    }
    else if(pv$partner_technologies ==1 & (prev_cl - filter_num ==3)){
      print("Filter: Go to SAVID")
      pv$partner_SAVID = 1
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_technologies ==1 & (prev_cl - filter_num >3)){
      print("Filter: Turn of All Partner Extra Flags")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_tech_grps ==1 & (prev_cl - filter_num == 2 )){
      print("Filter: Go to SAVID")
      pv$partner_SAVID = 1
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_tech_grps ==1 & (prev_cl - filter_num > 2 )){
      print("Filter: Turn of All Partner Extra Flags")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_SAVID ==1 & (prev_cl - filter_num > 1 )){
      print("Filter: Turn of All Partner Extra Flags")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
  }) 
  # function to go back to go to SRepName (current level 8) - No filters when link is clicked
  observeEvent(input$pfilter_7, {
    prev_cl = pv$pcurrent.level
    print("Filter 7")
    if(pv$tech_breakdown == 1){
      pv$tech_breakdown =0
    }
    new.sel.list = list()
    new.clean.sel.list = list()
    
    for(i in 1:7){
      new.sel.list[i] = pv$sel.list[i]
      new.clean.sel.list[i] = pv$clean.sel.list[i]
    }
    # replace the reactive values with the new lists
    pv$sel.list = new.sel.list
    pv$clean.sel.list = new.clean.sel.list
    pv$pcurrent.level = 8
    filter_num = 7
    #when in partner technologies, if pcurrent.level -1 == filter number, then turn partner_techgrps on
    if(pv$partner_technologies ==1 & (prev_cl - filter_num == 2)){
      print("Filter: Go to Tech Grps")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 1
      pv$partner_technologies = 0
    }
    else if(pv$partner_technologies ==1 & (prev_cl - filter_num ==3)){
      print("Filter: Go to SAVID")
      pv$partner_SAVID = 1
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_technologies ==1 & (prev_cl - filter_num >3)){
      print("Filter: Turn of All Partner Extra Flags")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_tech_grps ==1 & (prev_cl - filter_num == 2 )){
      print("Filter: Go to SAVID")
      pv$partner_SAVID = 1
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_tech_grps ==1 & (prev_cl - filter_num > 2 )){
      print("Filter: Turn of All Partner Extra Flags")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_SAVID ==1 & (prev_cl - filter_num > 1 )){
      print("Filter: Turn of All Partner Extra Flags")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
  }) 
  
  # function to go back to go to Sales rep name (current level 9) - No filters when link is clicked
  observeEvent(input$pfilter_8, {
    prev_cl = pv$pcurrent.level
    print("Filter 8")
    pv$sel.dropdown1 = pv$sel.list[[8]]
    
    if(pv$tech_breakdown == 1){
      pv$tech_breakdown =0
    }
    new.sel.list = list()
    new.clean.sel.list = list()
    
    for(i in 1:8){
      new.sel.list[i] = pv$sel.list[i]
      new.clean.sel.list[i] = pv$clean.sel.list[i]
    }
    # replace the reactive values with the new lists
    pv$sel.list = new.sel.list
    pv$clean.sel.list = new.clean.sel.list
    pv$pcurrent.level = 9
    filter_num = 8
    #when in partner technologies, if pcurrent.level -1 == filter number, then turn partner_techgrps on
    if(pv$partner_technologies ==1 & (prev_cl - filter_num == 2)){
      print("Filter: Go to Tech Grps")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 1
      pv$partner_technologies = 0
    }
    else if(pv$partner_technologies ==1 & (prev_cl - filter_num ==3)){
      print("Filter: Go to SAVID")
      pv$partner_SAVID = 1
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_technologies ==1 & (prev_cl - filter_num >3)){
      print("Filter: Turn of All Partner Extra Flags")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_tech_grps ==1 & (prev_cl - filter_num == 2 )){
      print("Filter: Go to SAVID")
      pv$partner_SAVID = 1
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_tech_grps ==1 & (prev_cl - filter_num > 2 )){
      print("Filter: Turn of All Partner Extra Flags")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_SAVID ==1 & (prev_cl - filter_num > 1 )){
      print("Filter: Turn of All Partner Extra Flags")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
  }) 
  
  # function to go back to go to Tech (current level 10) - No filters when link is clicked
  observeEvent(input$pfilter_9, {
    prev_cl = pv$pcurrent.level
    print("Filter 9")
    if(pv$tech_breakdown == 1){
      pv$tech_breakdown =0
    }
    new.sel.list = list()
    new.clean.sel.list = list()
    
    for(i in 1:9){
      new.sel.list[i] = pv$sel.list[i]
      new.clean.sel.list[i] = pv$clean.sel.list[i]
    }
    # replace the reactive values with the new lists
    pv$sel.list = new.sel.list
    pv$clean.sel.list = new.clean.sel.list
    pv$pcurrent.level = 10
    filter_num = 9
    #when in partner technologies, if pcurrent.level -1 == filter number, then turn partner_techgrps on
    if(pv$partner_technologies ==1 & (prev_cl - filter_num == 2)){
      print("Filter: Go to Tech Grps")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 1
      pv$partner_technologies = 0
    }
    else if(pv$partner_technologies ==1 & (prev_cl - filter_num ==3)){
      print("Filter: Go to SAVID")
      pv$partner_SAVID = 1
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_technologies ==1 & (prev_cl - filter_num >3)){
      print("Filter: Turn of All Partner Extra Flags")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_tech_grps ==1 & (prev_cl - filter_num == 2 )){
      print("Filter: Go to SAVID")
      pv$partner_SAVID = 1
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_tech_grps ==1 & (prev_cl - filter_num > 2 )){
      print("Filter: Turn of All Partner Extra Flags")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_SAVID ==1 & (prev_cl - filter_num > 1 )){
      print("Filter: Turn of All Partner Extra Flags")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
  }) 
  
  # function to go back to go to Tech (current level 10) - No filters when link is clicked
  observeEvent(input$pfilter_10, {
    prev_cl = pv$pcurrent.level
    print("Filter 10")
    if(pv$tech_breakdown == 1){
      pv$tech_breakdown =0
    }
    new.sel.list = list()
    new.clean.sel.list = list()
    
    for(i in 1:10){
      new.sel.list[i] = pv$sel.list[i]
      new.clean.sel.list[i] = pv$clean.sel.list[i]
    }
    # replace the reactive values with the new lists
    pv$sel.list = new.sel.list
    pv$clean.sel.list = new.clean.sel.list
    pv$pcurrent.level = 11
    filter_num = 10
    #when in partner technologies, if pcurrent.level -1 == filter number, then turn partner_techgrps on
    if(pv$partner_technologies ==1 & (prev_cl - filter_num == 2)){
      print("Filter: Go to Tech Grps")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 1
      pv$partner_technologies = 0
    }
    else if(pv$partner_technologies ==1 & (prev_cl - filter_num ==3)){
      print("Filter: Go to SAVID")
      pv$partner_SAVID = 1
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_technologies ==1 & (prev_cl - filter_num >3)){
      print("Filter: Turn of All Partner Extra Flags")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_tech_grps ==1 & (prev_cl - filter_num == 2 )){
      print("Filter: Go to SAVID")
      pv$partner_SAVID = 1
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_tech_grps ==1 & (prev_cl - filter_num > 2 )){
      print("Filter: Turn of All Partner Extra Flags")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_SAVID ==1 & (prev_cl - filter_num > 1 )){
      print("Filter: Turn of All Partner Extra Flags")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
  }) 
  
  # function to go back to go to Tech (current level 10) - No filters when link is clicked
  observeEvent(input$pfilter_11, {
    prev_cl = pv$pcurrent.level
    print("Filter 11")
    if(pv$tech_breakdown == 1){
      pv$tech_breakdown =0
    }
    new.sel.list = list()
    new.clean.sel.list = list()
    
    for(i in 1:10){
      new.sel.list[i] = pv$sel.list[i]
      new.clean.sel.list[i] = pv$clean.sel.list[i]
    }
    # replace the reactive values with the new lists
    pv$sel.list = new.sel.list
    pv$clean.sel.list = new.clean.sel.list
    pv$pcurrent.level = 11
    filter_num = 10
    #when in partner technologies, if pcurrent.level -1 == filter number, then turn partner_techgrps on
    if(pv$partner_technologies ==1 & (prev_cl - filter_num == 2)){
      print("Filter: Go to Tech Grps")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 1
      pv$partner_technologies = 0
    }
    else if(pv$partner_technologies ==1 & (prev_cl - filter_num ==3)){
      print("Filter: Go to SAVID")
      pv$partner_SAVID = 1
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_technologies ==1 & (prev_cl - filter_num >3)){
      print("Filter: Turn of All Partner Extra Flags")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_tech_grps ==1 & (prev_cl - filter_num == 2 )){
      print("Filter: Go to SAVID")
      pv$partner_SAVID = 1
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_tech_grps ==1 & (prev_cl - filter_num > 2 )){
      print("Filter: Turn of All Partner Extra Flags")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
    else if(pv$partner_SAVID ==1 & (prev_cl - filter_num > 1 )){
      print("Filter: Turn of All Partner Extra Flags")
      pv$partner_SAVID = 0
      pv$partner_tech_grps = 0
      pv$partner_technologies = 0
    }
  }) 
  
  output$pftable = DT::renderDataTable({
    
    DT::datatable(csvdata, options = list(scrollX = TRUE, pageLength = 20, rowCallback = JS('
                                                                              function(nRow, aData, iDisplayIndex, iDisplayIndexFull) {
                                                                                          // turn searching off at the top
                                                                                          $("#DataTables_Table_0_filter").css("display", "none");
                                                                                          // Bold and green cells for conditions
                                                                                          if (aData[4] == "High-End"){
                                                                                                $("td:eq(4)", nRow).css("color", "green");
                                                                                                $("td:eq(3)", nRow).css("color", "green");
                                                                                          }else{
                                                                                                $("td:eq(4)", nRow).css("color", "red");
                                                                                                $("td:eq(3)", nRow).css("color", "red");
                                                                                          }
  }')),filter = 'top', rownames = F, caption = 'Products that are considered High and Low-end for Static Share Calculations.',
                  selection = "none")
  })
  
  
  
}) # end of shinyServer()
