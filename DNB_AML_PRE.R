### DNB AML DATA ### 

### Preliminary data inspection ###

## Set directory
setwd("/nr/project/stat/BigInsight/Projects/Fraud/Subprojects/DNB-AML-Master/Data") # Data
load("/nr/project/stat/BigInsight/Projects/Fraud/Subprojects/DNB-AML-Master/Data/masterStudentAnonymizedDataUseThis.RData")

library("doParallel", lib.loc="/usr/local/lib/R/site-library")
library("data.table", lib.loc="/usr/local/lib/R/site-library")



## Make all data.frames into data.table
datAnonymizedVar = data.table(datAnonymizedVar)
datFullNameVar = data.table(datFullNameVar, key = 'CaseID')
datPartyCaseRelation = data.table(datPartyCaseRelation, key = c('CaseID','PartyID'))
## Make PartyID/AccountID into character in datTransactions data, such that it corresponds to datPartyCaseRelation
datTransactions$PartyID = as.character(datTransactions$PartyID)
datTransactions$AccountID = as.character(datTransactions$AccountID)
datTransactions = data.table(datTransactions)
## In datAnonymizedVar create CaseID variable for further merging
datAnonymizedVar[, CaseID := 1:nrow(datAnonymizedVar)]





## Remove duplicates in transactions(the exact same TransID)
## 123 rows with duplicated TransID
datTransNoDup = datTransactions[!duplicated(datTransactions$TransID),]
setkey(datTransNoDup,PartyID)

## Assign creationDate and CaseStatus from datFullNameVar to corresponding PartyID in datPartyCaseRelation and 
datPartyCaseRelation = merge(datPartyCaseRelation, datFullNameVar, by='CaseID', all.x=T)

## Find PartyID in datPartyCaseRelation which do not have transactions in datTransNoDup
u1 = subset(data.table(PartyID = unique(datPartyCaseRelation$PartyID) , key = 'PartyID'), PartyID != 'None')
u2 = data.table(PartyID = unique(datTransNoDup$PartyID), flagg = 1, key = 'PartyID')
nonTransPartyID = merge(u1,u2,by='PartyID', all.x=T)
nonTransPartyID = subset(nonTransPartyID, is.na(flagg), PartyID) 
## In addition to these PartyIDs, we need to find all PartyIDs whose Accounts are not in the network in period
## [0, creationDate + 14] + these whose Accounts connect only to 'Unknown' nodes.
## 1.) Get temporary creationDate -- PartyID list
dates = sort(unique(datPartyCaseRelation$creationDate))
tmp_relation = datPartyCaseRelation[!PartyID %in% c('None',nonTransPartyID$PartyID)]
## Make PostingDate in datTransNoDup into Date format
datTransNoDup$PostingDate = as.Date(datTransNoDup$PostingDate)
setkey(datTransNoDup,PostingDate)

#cl = makeCluster(4, type = 'FORK')
registerDoParallel(7)
res = foreach(time = iter(1:length(dates))) %dopar% {
  tmp1 =  tmp_relation[creationDate == dates[time],PartyID]
  tmp_dat = datTransNoDup[PartyID %in% tmp1 & PostingDate <= (dates[time]+14),c('PartyID','AccountID','CounterPartyAccountID'),with=F]
  d = tmp_dat$AccountID %in% c('-1','None') | tmp_dat$CounterPartyAccountID %in% c('-1','None')
  w_in = tmp1[tmp1%in%tmp_dat$PartyID]
  w_unk = tmp1[tmp1%in%tmp_dat$PartyID[d]]
  w_unk = w_unk[!w_unk%in%w_in]
  check = c(tmp1[!tmp1 %in% w_in],w_unk)
  check
}  
#stopCluster(cl)
names(res) = dates
l = unlist(res)
c_ID = datPartyCaseRelation[PartyID%in%l,CaseID]
count_rows = 0
for(c in 1:length(c_ID)){
  count_rows[c] = nrow(datPartyCaseRelation[CaseID == c_ID[c]])
}
### None of the CaseIDs involves only one PartyID!
### So that means, only CaseIDs in nonTransCaseID(see below) need to be removed

## Set TransFlag to 1
datPartyCaseRelation[, TransFlag := 1]
## If PartyID in (no transaction,'None') ==> TransFlag = 0
datPartyCaseRelation[datPartyCaseRelation$PartyID%in%nonTransPartyID$PartyID | 
                       datPartyCaseRelation$PartyID == 'None','TransFlag'] = 0
### Now find PartyID/creationDate combinations for which we do not have transactions in datTransNoDup



## Find CaseID for which we cannot construct a network
nonTransCaseID = subset(aggregate(datPartyCaseRelation$TransFlag, by=list(CaseID=datPartyCaseRelation$CaseID), 
                                   FUN=sum, na.rm = T), x == 0)
nonTransCaseID = datPartyCaseRelation[, .(tot = sum(TransFlag, na.rm = T)), by = CaseID][tot == 0]
## 39 such CaseID !!

## Find PartyID which have many transactions associated with
tab = table(datTransNoDup$PartyID)
#quantile(tab, probs=c(0,0.05,0.1,0.25,0.5,0.75,0.9,0.95,1))
#head(sort(tab, decreasing = T),10)

## Find all AccountIDs associated with all PartyIDs in datPartyCaseRelation
## 87s with data.frame --> 3s with data.table
setkey(datTransNoDup,PartyID)
partyID = c(names(sort(tab, decreasing = T)),nonTransPartyID$PartyID)
cl = makeCluster(detectCores()-1, type = 'FORK')
registerDoParallel(cl)
getAccountIDPar = function(){
  res = foreach(i = iter(partyID)) %dopar% {
    if(i %in% nonTransPartyID$PartyID){accountsIDtmp = 'None';numTransactionstmp = 0}
    else{
      tmp = datTransNoDup[i,AccountID]
      accountsIDtmp = unique(tmp)
      numTransactionstmp = length(tmp) + length(datTransNoDup[CounterPartyAccountID%in%accountsIDtmp,CounterPartyAccountID])
    }
    list(PartyID = i, accountsID =accountsIDtmp, numTransactions = numTransactionstmp)
  }
  names(res) = partyID
  res
}
PartyIDSummary = getAccountIDPar()
stopCluster(cl)
## AccountIDs to all PartyIDs
partyIDaccounts = unlist(lapply(PartyIDSummary, function(x){x$accountsID})) ## 7780 unique AccountIDs
partyIDaccounts = partyIDaccounts[partyIDaccounts!='None']

## Find all unique AccountIDs except {'-1','None'}
setkey(datTransNoDup,AccountID)
u = unique(c(datTransNoDup[!AccountID%in%c('None','-1'),AccountID],datTransNoDup[!CounterPartyAccountID%in%c('None','-1'),CounterPartyAccountID]))
## 375 473 CounterPartyAccountIDs with no corresponding PartyID
otherAccounts = u[!u%in%partyIDaccounts]

## Find CounterPartyAccountID not belonging to any PartyID which have most transactions.
CounterPartyAccountIDSummary = table(datTransNoDup$CounterPartyAccountID[datTransNoDup$CounterPartyAccountID%in%otherAccounts])
# head(sort(CounterPartyAccountIDSummary, decreasing = T))

## PartyID = '11001272' occurs in 1 808 338 rows, (~ 42.5%) of all rows and have 391 AccountIDs associated with. 60 095 rows within its own accounts.
# This indicates that this ID corresponds to some authority account. 
## PartyID = '4001275' occurs in 311 955 rows, (~7%) of all rows and have 795 AccountIDs associated with. 
# This indicates that this ID corresponds to some authority account. 

## CounterPartyAccountID = '13601604' occurs in 431 274 rows (~10%) of all rows. Other CounterPartyAccountIDs are {'14801303','6901367','9501545','2501326','9101379'}

## Check whether one AccountID can be accessed by multiple PartyIDs
tab = table(datTransNoDup$AccountID)
accountID = names(sort(tab, decreasing = T))

cl = makeCluster(detectCores()-1, type = 'FORK')
registerDoParallel(cl)
getAccountIDPar2 = function(){
  res = foreach(i = iter(accountID)) %dopar% {
    tmp = datTransNoDup[i,'PartyID']
    partyIDtmp = unique(tmp)
    list(AccountID = i, PartyID =partyIDtmp)
  }
  res
}
AccountIDSummary = getAccountIDPar2()
stopCluster(cl)
## Check whether one AccountID can be attached to many PartyID
f = 0
for(i in 1:length(AccountIDSummary)){
  if(length(AccountIDSummary[[i]]$PartyID) > 1){f[i] = T}
}
sum(f)
## There are no AccountIDs that are connected to more than one PartyID !!!!

## Find transactions that are duplicated(i.e. two rows corresponding to the same transaction. That is two rows with
## different PartyID and CreditDebitCode = 'D' and CreditDebitCode = 'C' in these two rows.)

## The transactions occurs twice <=> both AccountID and CounterAccountPartyID belongs to one of PartyIDs
## Restrict transactions to only these
PartyIDTransactions = datTransNoDup[datTransNoDup$CounterPartyAccountID%in%partyIDaccounts,]
nonPartyIDTransactions = datTransNoDup[!datTransNoDup$CounterPartyAccountID%in%partyIDaccounts,]
## Find duplicates within PartyIDTransactions
## If CreditDebitCode == 'C' ==> switch AccountID and CounterPartyAccountID
row.credit = which(PartyIDTransactions$CreditDebitCode=='C')
col.order = colnames(datTransNoDup)
col.order[3] = colnames(datTransNoDup)[7]
col.order[7] = colnames(datTransNoDup)[3]

PartyIDTransactions[row.credit,] = PartyIDTransactions[row.credit,col.order,with=F]
PartyIDTransactions = PartyIDTransactions[order(PartyIDTransactions$CreditDebitCode),]


## Find duplicates 
d1 = duplicated(substr(PartyIDTransactions$TransID, 1, nchar(PartyIDTransactions$TransID)-1))
temp = PartyIDTransactions[,c('AccountID','PostingDate','Amount','CounterPartyAccountID'),with=F]
d2 = duplicated(temp)
## We do not use d2 to remove duplicates, since it will remove also rows that actually are not duplicates,
## but just coincide in 'AccountID','PostingDate','Amount','CounterPartyAccountID'!!
## Here we get 12 rows that according to d1&d2 are not duplicates, because they differ in Amount. However this difference is marginal and therefore
## is probably an error.
## Therefore to remove duplicates only condition d1 is used !! ##
dups = PartyIDTransactions[d1,]
nonDupsTmp = PartyIDTransactions[!d1,]
## Switch AccountID and CounterPartyAccountID also in nonPartyIDTransactions data
row.credit = which(nonPartyIDTransactions$CreditDebitCode=='C')
nonPartyIDTransactions[row.credit,] = nonPartyIDTransactions[row.credit,col.order,with=F]

### Final dataset without duplicated rows. 
### CAUTION !!! Rows with CreditDebitCode = 'C' have AccountID and CounterPartyAccountID interchanged !!! ###
datTransNoDup = rbind(nonDupsTmp,nonPartyIDTransactions)
## Left with 4 199 927 unique transactions


## Remove unnecessary objects before saving
rm(list=setdiff(ls(), c('datAnonymizedVar','datFullNameVar','datPartyCaseRelation','datTransNoDup','nonTransCaseID',
                        'nonTransPartyID','PartyIDSummary','partyIDaccounts')))

## Since PartyID and AccountID have some duplicated values(i.e. PartyID = AccountID) add prefix
setkeyv(datTransNoDup,c("AccountID",'CounterPartyAccountID'))
datTransNoDup[AccountID!='Unknown',AccountID := paste0('A',AccountID)] 
datTransNoDup[CounterPartyAccountID!='Unknown', CounterPartyAccountID := paste0('A',CounterPartyAccountID)]
## Make 'Unknown' node to appear first, for easier subsetting later on
setorder(datTransNoDup, -AccountID)


## Save image for future work
save.image("/nr/project/stat/BigInsight/Projects/Fraud/Subprojects/DNB-AML-Master/temp.RData")

