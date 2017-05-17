## Summarize transation data
library("moments", lib.loc="/usr/local/lib/R/site-library")
library("doParallel", lib.loc="/usr/local/lib/R/site-library")
library("data.table", lib.loc="/usr/local/lib/R/site-library")
library("igraph", lib.loc="/usr/local/lib/R/library")
load("/nr/project/stat/BigInsight/Projects/Fraud/Subprojects/DNB-AML-Master/temp.RData")
t = lapply(PartyIDSummary, function(x){x$accountsID}) ## can be precomputed
AccountVsPartyID = data.table(PartyID = rep(names(t), sapply(t, length)), AccountID = paste('A',unlist(t),sep=''), key='AccountID') ## can be precomputed

setkeyv(datTransNoDup, c('AccountID','CounterPartyAccountID'))
dataTransKnown = datTransNoDup[AccountID!='Unknown' & CounterPartyAccountID != 'Unknown']
dataTransUnKnown = datTransNoDup[AccountID=='Unknown' | CounterPartyAccountID == 'Unknown']

dataTransBigParty = datTransNoDup[AccountID%in%paste0('A',PartyIDSummary$`11001272`$accountsID) | CounterPartyAccountID %in%paste0('A',PartyIDSummary$`11001272`$accountsID)]

## Find Hub AccountIDs
## Find PartyID which have many transactions associated with
net = graph.data.frame(datTransNoDup[,c('AccountID','CounterPartyAccountID'),with=F])
deg = degree(net,mode='all')
deg = data.table(AccountID = rep(names(deg), sapply(deg, length)), N = unlist(deg)) ## can be precomputed
deg=deg[order(-N)]
deg[,"prop":=N/nrow(datTransNoDup)]
t = lapply(PartyIDSummary, function(x){x$accountsID}) ## can be precomputed
AccountVsPartyID = data.table(PartyID = rep(names(t), sapply(t, length)), AccountID = paste('A',unlist(t),sep=''), key='AccountID') ## can be precomputed
## Find corresponding PartyID
degTop =head(deg,5)
merge(degTop,AccountVsPartyID,by='AccountID',all.x=T)
## Similar for in-degree and out-degree
indeg = degree(net,mode='in')
indeg = data.table(AccountID = rep(names(indeg), sapply(indeg, length)), indeg = unlist(indeg)) ## can be precomputed
indeg=indeg[order(-indeg)]
indeg[,"prop":=indeg/nrow(datTransNoDup)]
indegTop =head(indeg,5)
merge(indegTop,AccountVsPartyID,by='AccountID',all.x=T)
outdeg = degree(net,mode='out')
outdeg = data.table(AccountID = rep(names(outdeg), sapply(outdeg, length)), outdeg = unlist(outdeg)) ## can be precomputed
outdeg=outdeg[order(-outdeg)]
outdeg[,"prop":=outdeg/nrow(datTransNoDup)]
outdegTop =head(outdeg,5)
merge(outdegTop,AccountVsPartyID,by='AccountID',all.x=T)
#quantile(tab, probs=c(0,0.05,0.1,0.25,0.5,0.75,0.9,0.95,1))
#head(sort(tab, decreasing = T),10)

## Currency
#kn_curr = head(round(sort(prop.table(table(dataTransKnown$CurrencyCodeOrig)),decreasing=T)*100,2))
#unk_curr = head(round(sort(prop.table(table(dataTransUnKnown$CurrencyCodeOrig)),decreasing=T)*100,2))
## Find average amount of transcation per Currency
kn_curr = dataTransKnown[,list(TotAmount = sum(Amount), MeanAmount = mean(Amount), Count = length(Amount)), by = list(Currency = CurrencyCodeOrig)]
kn_curr[,'AmountProp':= TotAmount/sum(dataTransKnown$Amount)*100]
kn_curr[,'CountProp':= Count/nrow(dataTransKnown)*100]
head(kn_curr[order(-AmountProp)])

unk_curr = dataTransUnKnown[,list(TotAmount = sum(Amount), MeanAmount = mean(Amount), Count = length(Amount)), by = list(Currency = CurrencyCodeOrig)]
unk_curr[,'AmountProp':= TotAmount/sum(dataTransUnKnown$Amount)]
unk_curr[,'CountProp':= Count/nrow(dataTransUnKnown)*100]
head(unk_curr[order(-AmountProp)])

## TransType
#sort(prop.table(table(dataTransKnown$TransTypeID)),decreasing=T)
kn_trType = dataTransKnown[,list(TotAmount = sum(Amount), MeanAmount = mean(Amount), Count = length(Amount)), by = list(TranType = TransTypeID)]
kn_trType[,'AmountProp':= TotAmount/sum(dataTransKnown$Amount)*100]
kn_trType[,'CountProp':= Count/nrow(dataTransKnown)*100]
kn_trType[order(-CountProp)]

sort(prop.table(table(dataTransUnKnown$TransTypeID)),decreasing=T)
unk_trType = dataTransUnKnown[,list(TotAmount = sum(Amount), MeanAmount = mean(Amount), Count = length(Amount)), by = list(TranType = TransTypeID)]
unk_trType[,'AmountProp':= TotAmount/sum(dataTransUnKnown$Amount)*100]
unk_trType[,'CountProp':= Count/nrow(dataTransUnKnown)*100]
unk_trType[order(-CountProp)]
## Looks like Transtype=2 is a cash transaction, since it is most prevelant in Unknown transactions
## TransChannel
round(sort(prop.table(table(dataTransKnown$TransactionChannelID)),decreasing=T)*100,2)
kn_trChannel = dataTransKnown[,list(TotAmount = sum(Amount), MeanAmount = mean(Amount), Count = length(Amount)), by = list(TranChannel = TransactionChannelID)]
kn_trChannel[,'AmountProp':= TotAmount/sum(dataTransKnown$Amount)*100]
kn_trChannel[,'CountProp':= Count/nrow(dataTransKnown)*100]
head(kn_trChannel[order(-CountProp)])
head(kn_trChannel[order(-AmountProp)])

round(sort(prop.table(table(dataTransUnKnown$TransactionChannelID)),decreasing=T)*100,2)
unk_trChannel = dataTransUnKnown[,list(TotAmount = sum(Amount), MeanAmount = mean(Amount), Count = length(Amount)), by = list(TranChannel = TransactionChannelID)]
unk_trChannel[,'AmountProp':= TotAmount/sum(dataTransUnKnown$Amount)*100]
unk_trChannel[,'CountProp':= Count/nrow(dataTransUnKnown)*100]
head(unk_trChannel[order(-CountProp)])
head(unk_trChannel[order(-AmountProp)])

## In addition TransactionChannel=6 looks like channel for cash transactions(ATM?, bank counter,...)
## Check combination
tab1 = table(dataTransKnown$TransTypeID,dataTransKnown$TransactionChannelID)
round(prop.table(tab1,1)*100,2)
round(prop.table(tab1,2)*100,2)
kn_trChannelType = dataTransKnown[,list(TotAmount = sum(Amount), MeanAmount = mean(Amount), Count = length(Amount)),
                                        by = list(TranChannel = TransactionChannelID, TranType = TransTypeID)]
kn_trChannelType[,'AmountProp':= TotAmount/sum(dataTransKnown$Amount)*100]
kn_trChannelType[,'CountProp':= Count/nrow(dataTransKnown)*100]
tmpType = kn_trChannelType[,list(TotAmountType = sum(TotAmount), CountType = sum(Count)), by = list(TranType = TranType)]
tmpChannel = kn_trChannelType[,list(TotAmountChannel = sum(TotAmount), CountChannel = sum(Count)), by = list(TranChannel = TranChannel)]
kn_trChannelType = merge(kn_trChannelType,tmpChannel,by='TranChannel')
kn_trChannelType = merge(kn_trChannelType,tmpType,by='TranType')
kn_trChannelType[,'AmountPropChannel':= TotAmount/TotAmountChannel*100]
kn_trChannelType[,'CountPropChannel':= Count/CountChannel*100]
kn_trChannelType[,'AmountPropType':= TotAmount/TotAmountType*100]
kn_trChannelType[,'CountPropType':= Count/CountType*100]

head(kn_trChannelType[order(-CountProp)])
head(kn_trChannelType[order(-AmountProp)])

tab2 = table(dataTransUnKnown$TransTypeID,dataTransUnKnown$TransactionChannelID)
round(prop.table(tab2,1)*100,2)
round(prop.table(tab2,2)*100,2)
unk_trChannelType = dataTransUnKnown[,list(TotAmount = sum(Amount), MeanAmount = mean(Amount), Count = length(Amount)),
                                  by = list(TranChannel = TransactionChannelID, TranType = TransTypeID)]
unk_trChannelType[,'AmountProp':= TotAmount/sum(dataTransUnKnown$Amount)*100]
unk_trChannelType[,'CountProp':= Count/nrow(dataTransUnKnown)*100]
tmpType = unk_trChannelType[,list(TotAmountType = sum(TotAmount), CountType = sum(Count)), by = list(TranType = TranType)]
tmpChannel = unk_trChannelType[,list(TotAmountChannel = sum(TotAmount), CountChannel = sum(Count)), by = list(TranChannel = TranChannel)]
unk_trChannelType = merge(unk_trChannelType,tmpChannel,by='TranChannel')
unk_trChannelType = merge(unk_trChannelType,tmpType,by='TranType')
unk_trChannelType[,'AmountPropChannel':= TotAmount/TotAmountChannel*100]
unk_trChannelType[,'CountPropChannel':= Count/CountChannel*100]
unk_trChannelType[,'AmountPropType':= TotAmount/TotAmountType*100]
unk_trChannelType[,'CountPropType':= Count/CountType*100]

head(unk_trChannelType[order(-CountProp)])
head(unk_trChannelType[order(-AmountProp)])


prop.table(tab1,1)
prop.table(tab2,1)
prop.table(tab1,2)
prop.table(tab2,2)
## Check amount distribution
summary(dataTransKnown$Amount)
summary(dataTransUnKnown$Amount)
skewness(dataTransKnown$Amount)
skewness(dataTransUnKnown$Amount)

### Check Fraudulent vs. Legitimate
### remove CaseIDs with no transactions
datPartyCaseRelationTr = datPartyCaseRelation[!CaseID%in%nonTransCaseID$CaseID]
datAnonymizedVarTr = datAnonymizedVar[!CaseID%in%nonTransCaseID$CaseID]
datFullNameVarTr = datFullNameVar[!CaseID%in%nonTransCaseID$CaseID]
## Class distribution on CaseID level
prop.table(table(datFullNameVarTr$CaseStatus))
CID_F = unique(datPartyCaseRelationTr[ CaseStatus == '1',CaseID])
### remove CaseIDs with no transactions
PID_F = unique(datPartyCaseRelationTr[CaseStatus == '1',PartyID])
## Class distribution on PartyID level
length(PID_F)/(length(unique(datPartyCaseRelationTr$PartyID))-1)

net = graph.data.frame(datTransNoDup[,c("AccountID", "CounterPartyAccountID"),with=F], directed = T)

AID_F = AccountVsPartyID[PartyID %in% PID_F & !PartyID %in% nonTransPartyID$PartyID,AccountID]
## Class distribution on AccountID level
length(AID_F)/(vcount(net)-1)

###
FF = datTransNoDup[(AccountID %in% AID_F & CounterPartyAccountID %in% AID_F)]
# only within Fraudulent
FL = datTransNoDup[(AccountID %in% AID_F & !CounterPartyAccountID %in% AID_F) | 
                                     (CounterPartyAccountID %in% AID_F & !AccountID %in% AID_F)]
# cross labeled
LL = datTransNoDup[(!AccountID %in% AID_F & !CounterPartyAccountID %in% AID_F)]
# only within Legitimate: L-L,L-Unknown
Fr = datTransNoDup[(AccountID %in% AID_F | CounterPartyAccountID %in% AID_F)]
# at least one side is Fraudulent(i.e. FL+FF)
Le = dataTransNoDup[(!AccountID %in% AID_F | !CounterPartyAccountID %in% AID_F)]
# at least one side is Legitimate(i.e. LL+FL)

#### Check homophily on undirected simplified network
net_sim = simplify(as.undirected(net))
e_list = get.edgelist(net_sim)
## Fraudulent connections
ff = e_list[,1] %in% AID_F & e_list[,2] %in% AID_F
## Cross-labeled connections
fl = (e_list[,1] %in% AID_F & !e_list[,2] %in% AID_F) | 
  (!e_list[,1] %in% AID_F & e_list[,2] %in% AID_F)
## Legitimate connections
ll = !ff & !fl
## Fraudulent accounts
N = vcount(net_sim); e=ecount(net_sim)
f = length(AID_F)/N
## Legitimate accounts
l = 1-f
### Test 1.
sample=sum(fl)/e
test = 2*f*l
z = (sample-test)/sqrt(test*(1-test)/e)
pnorm(z)
## We observe much more cross-labeled edges than expected for a random network
## Test 2.
D = 2*e/(N*(N-1))
ff_exp = choose(length(AID_F),2)*D
fl_exp = f*N*l*N*D

d=sum(ff)/ff_exp
h=sum(fl)/fl_exp

### First check pure fraudulent transactions vs. pure legitimate
## Currency
kn_curr = FF[,list(TotAmount = sum(Amount), MeanAmount = mean(Amount), Count = length(Amount)), by = list(Currency = CurrencyCodeOrig)]
kn_curr[,'AmountProp':= TotAmount/sum(FF$Amount)*100]
kn_curr[,'CountProp':= Count/nrow(FF)*100]
head(kn_curr[order(-AmountProp)])

kn_curr = LL[,list(TotAmount = sum(Amount), MeanAmount = mean(Amount), Count = length(Amount)), by = list(Currency = CurrencyCodeOrig)]
kn_curr[,'AmountProp':= TotAmount/sum(LL$Amount)*100]
kn_curr[,'CountProp':= Count/nrow(LL)*100]
head(kn_curr[order(-AmountProp)])

kn_curr = FL[,list(TotAmount = sum(Amount), MeanAmount = mean(Amount), Count = length(Amount)), by = list(Currency = CurrencyCodeOrig)]
kn_curr[,'AmountProp':= TotAmount/sum(FL$Amount)*100]
kn_curr[,'CountProp':= Count/nrow(FL)*100]
head(kn_curr[order(-AmountProp)])

kn_curr = Fr[,list(TotAmount = sum(Amount), MeanAmount = mean(Amount), Count = length(Amount)), by = list(Currency = CurrencyCodeOrig)]
kn_curr[,'AmountProp':= TotAmount/sum(Fr$Amount)*100]
kn_curr[,'CountProp':= Count/nrow(Fr)*100]
head(kn_curr[order(-AmountProp)])

kn_curr = Le[,list(TotAmount = sum(Amount), MeanAmount = mean(Amount), Count = length(Amount)), by = list(Currency = CurrencyCodeOrig)]
kn_curr[,'AmountProp':= TotAmount/sum(Le$Amount)*100]
kn_curr[,'CountProp':= Count/nrow(Le)*100]
head(kn_curr[order(-AmountProp)])

### Looks like much higher ratio of foreign transactions(i.e. in USD) for pure fraudulent
## TransType
head(sort(prop.table(table(FF$TransTypeID)),decreasing=T))
kn_trType = FF[,list(TotAmount = sum(Amount), MeanAmount = mean(Amount), Count = length(Amount)), by = list(TranType = TransTypeID)]
kn_trType[,'AmountProp':= TotAmount/sum(FF$Amount)*100]
kn_trType[,'CountProp':= Count/nrow(FF)*100]
kn_trType[order(-CountProp)]

head(sort(prop.table(table(LL$TransTypeID)),decreasing=T))
kn_trType = LL[,list(TotAmount = sum(Amount), MeanAmount = mean(Amount), Count = length(Amount)), by = list(TranType = TransTypeID)]
kn_trType[,'AmountProp':= TotAmount/sum(LL$Amount)*100]
kn_trType[,'CountProp':= Count/nrow(LL)*100]
kn_trType[order(-CountProp)]

head(sort(prop.table(table(Fr$TransTypeID)),decreasing=T))
kn_trType = Fr[,list(TotAmount = sum(Amount), MeanAmount = mean(Amount), Count = length(Amount)), by = list(TranType = TransTypeID)]
kn_trType[,'AmountProp':= TotAmount/sum(Fr$Amount)*100]
kn_trType[,'CountProp':= Count/nrow(Fr)*100]
kn_trType[order(-CountProp)]

head(sort(prop.table(table(Le$TransTypeID)),decreasing=T))
kn_trType = Le[,list(TotAmount = sum(Amount), MeanAmount = mean(Amount), Count = length(Amount)), by = list(TranType = TransTypeID)]
kn_trType[,'AmountProp':= TotAmount/sum(Le$Amount)*100]
kn_trType[,'CountProp':= Count/nrow(Le)*100]
head(kn_trType[order(-CountProp)])

### Almost all pure fraudulent connections are TransTypeID = 7, while for pure legitimate its TransType=1
### that dominates, with TransTypeID=2 following
head(sort(prop.table(table(FF$TransactionChannelID)),decreasing=T))
kn_trType = FF[,list(TotAmount = sum(Amount), MeanAmount = mean(Amount), Count = length(Amount)), by = list(TranChannel = TransactionChannelID)]
kn_trType[,'AmountProp':= TotAmount/sum(FF$Amount)*100]
kn_trType[,'CountProp':= Count/nrow(FF)*100]
head(n_trType[order(-CountProp)])

head(sort(prop.table(table(LL$TransactionChannelID)),decreasing=T))
kn_trType = LL[,list(TotAmount = sum(Amount), MeanAmount = mean(Amount), Count = length(Amount)), by = list(TranChannel = TransactionChannelID)]
kn_trType[,'AmountProp':= TotAmount/sum(LL$Amount)*100]
kn_trType[,'CountProp':= Count/nrow(LL)*100]
head(kn_trType[order(-CountProp)])

head(sort(prop.table(table(Fr$TransactionChannelID)),decreasing=T))
kn_trType = Fr[,list(TotAmount = sum(Amount), MeanAmount = mean(Amount), Count = length(Amount)), by = list(TranChannel = TransactionChannelID)]
kn_trType[,'AmountProp':= TotAmount/sum(Fr$Amount)*100]
kn_trType[,'CountProp':= Count/nrow(Fr)*100]
head(kn_trType[order(-CountProp)])

head(sort(prop.table(table(Le$TransactionChannelID)),decreasing=T))
kn_trType = Le[,list(TotAmount = sum(Amount), MeanAmount = mean(Amount), Count = length(Amount)), by = list(TranChannel = TransactionChannelID)]
kn_trType[,'AmountProp':= TotAmount/sum(Le$Amount)*100]
kn_trType[,'CountProp':= Count/nrow(Le)*100]
head(kn_trType[order(-CountProp)])
### TransactionChannelID={8,9,30} dominates in pure fraudulent compared to {4,6,32} in pure legitimate
## In addition TransactionChannel=6 looks like channel for cash transactions(ATM?, bank counter,...)
## Check combination
tab1 = table(FF$TransTypeID,FF$TransactionChannelID)
tab2 = table(LL$TransTypeID,LL$TransactionChannelID)
prop.table(tab1,1)
prop.table(tab2,1)
prop.table(tab1,2)
prop.table(tab2,2)
## Check amount distribution
summary(FF$Amount)
summary(LL$Amount)
summary(FL$Amount)
summary(Fr$Amount)
summary(Le$Amount)
skewness(FF$Amount)
skewness(LL$Amount)
skewness(FL$Amount)
skewness(Fr$Amount)
skewness(Le$Amount)
## Much higher skewness for pure legitimate transactions then pure fraud or cross labeled
## However IQR is much bigger for pure fraudulent transactions
### Check cross-labeled
## Currency
sort(prop.table(table(FL$CurrencyCodeOrig)),decreasing=T)
### Not that much of foreign currencies
## TransType
sort(prop.table(table(FL$TransTypeID)),decreasing=T)
##
sort(prop.table(table(FL$TransactionChannelID)),decreasing=T)
## Check combination
tab1 = table(FL$TransTypeID,FL$TransactionChannelID)
prop.table(tab1,1)
prop.table(tab1,2)
### Check whether fraudulent nodes "communicate" more heavily with 'Unknown' node than legitimate nodes
### Measure by number of links and also by amount
## F-U vs. (F-U+F-L+F-F)
nrow(Fr[AccountID=='Unknown'|CounterPartyAccountID=='Unknown'])/nrow(Fr)
sum(Fr[AccountID=='Unknown'|CounterPartyAccountID=='Unknown',Amount])/sum(Fr[,Amount])
## L-U vs. (L-U+L-F+L-L)
nrow(Le[AccountID=='Unknown'|CounterPartyAccountID=='Unknown'])/nrow(Le)
sum(Le[AccountID=='Unknown'|CounterPartyAccountID=='Unknown',Amount])/sum(Le[,Amount])
### Looks like Fraudulent Nodes communicate much more heavily with Unknown node than Legitimate nodes
### Especially in terms of total amount
## Create indicator
datTransNoDup[,UnInd := ifelse(AccountID=='Unknown'|CounterPartyAccountID=='Unknown','Yes','No')]
datTransNoDup[,CurrInd := ifelse(CurrencyCodeOrig!='NOK','Foreign','NOK')]
FF = datTransNoDup[(AccountID %in% AID_F & CounterPartyAccountID %in% AID_F)]
# only within Fraudulent
FL = datTransNoDup[(AccountID %in% AID_F & !CounterPartyAccountID %in% AID_F) | 
                     (CounterPartyAccountID %in% AID_F & !AccountID %in% AID_F)]
# cross labeled
LL = datTransNoDup[(!AccountID %in% AID_F & !CounterPartyAccountID %in% AID_F)]
# only within Legitimate: L-L,L-Unknown
Fr = datTransNoDup[(AccountID %in% AID_F | CounterPartyAccountID %in% AID_F)]
# at least one side is Fraudulent(i.e. FL+FF)
Le = datTransNoDup[(!AccountID %in% AID_F | !CounterPartyAccountID %in% AID_F)]


## 
kn_curr = Fr[,list(TotAmount = sum(Amount), MeanAmount = mean(Amount), Count = length(Amount)), by = list(Unknown = UnInd)]
kn_curr[,'AmountProp':= TotAmount/sum(Fr$Amount)*100]
kn_curr[,'CountProp':= Count/nrow(Fr)*100]
head(kn_curr[order(-AmountProp)])

kn_curr = Le[,list(TotAmount = sum(Amount), MeanAmount = mean(Amount), Count = length(Amount)), by = list(Unknown = UnInd)]
kn_curr[,'AmountProp':= TotAmount/sum(Le$Amount)*100]
kn_curr[,'CountProp':= Count/nrow(Le)*100]
head(kn_curr[order(-AmountProp)])

##
kn_curr = Fr[,list(TotAmount = sum(Amount), MeanAmount = mean(Amount), Count = length(Amount)), by = list(Unknown = UnInd, Currency = CurrInd)]
kn_curr[,'AmountProp':= TotAmount/sum(Fr$Amount)*100]
kn_curr[,'CountProp':= Count/nrow(Fr)*100]
head(kn_curr[order(-AmountProp)])

prop.table(table(Fr$CurrInd,Fr$UnInd),2)*100

kn_curr = Le[,list(TotAmount = sum(Amount), MeanAmount = mean(Amount), Count = length(Amount)), by = list(Unknown = UnInd, Currency = CurrInd)]
kn_curr[,'AmountProp':= TotAmount/sum(Le$Amount)*100]
kn_curr[,'CountProp':= Count/nrow(Le)*100]
head(kn_curr[order(-AmountProp)])

prop.table(table(Le$CurrInd,Le$UnInd),2)*100

##
kn_curr = Fr[,list(TotAmount = sum(Amount), MeanAmount = mean(Amount), Count = length(Amount)), by = list(Unknown = UnInd, TrType = TransTypeID)]
kn_curr[,'AmountProp':= TotAmount/sum(Fr$Amount)*100]
kn_curr[,'CountProp':= Count/nrow(Fr)*100]
head(kn_curr[order(-AmountProp)])

prop.table(table(Fr$UnInd,Fr$TransTypeID),1)

kn_curr = Le[,list(TotAmount = sum(Amount), MeanAmount = mean(Amount), Count = length(Amount)), by = list(Unknown = UnInd, TrType = TransTypeID)]
kn_curr[,'AmountProp':= TotAmount/sum(Le$Amount)*100]
kn_curr[,'CountProp':= Count/nrow(Le)*100]
head(kn_curr[order(-AmountProp)])

prop.table(table(Le$UnInd,Le$TransTypeID),1)

kn_curr = Fr[,list(TotAmount = sum(Amount), MeanAmount = mean(Amount), SkewAmount = skewness(Amount), IQR = IQR(Amount)), by = list(Unknown = UnInd)]
kn_curr

kn_curr = Le[,list(TotAmount = sum(Amount), MeanAmount = mean(Amount), SkewAmount = skewness(Amount), IQR = IQR(Amount)), by = list(Unknown = UnInd)]
kn_curr
