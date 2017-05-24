### Try to create PartyID -- AccountID graph

setwd("/nr/project/stat/BigInsight/Projects/Fraud/Subprojects/DNB-AML-Master")
load("/nr/project/stat/BigInsight/Projects/Fraud/Subprojects/DNB-AML-Master/temp.RData")

### Data files loaded frin temp.RData ###
## 1.) datAnonymizedVar - data.table: 1498 rows(CaseIDs) with explanatory variables(background vars)
## 2.) datFullNameVar - data.table: 1498 rows(CaseIDs) with response {0,1} for {non-fraud,fraud}
## 3.) datPartyCaseRelation - data.table: 2721 rows with CaseID -- PartyID relation + variable Implicated
##     which says whether it is the main PartyID in CaseID
## 4.) datTransNoDup - data.table: 4 199 927 rows with unique transactions
## 5.) nonTransCaseID - data.table: 39 rows with unique CaseIDs which have no transactions
## 6.) nonTransPartyID - data.table: 231 rows with unique PartyIDs which have no transactions
## 7.) partyIDaccounts - char vector with 7780 unique AccountIDs belonging to PartyIDs
## 8.) PartyIDSummary - list: with 2483 elements(lists) containing: char: PartyID, char vector: accountsID
##                                                                  int numberTransactions

## Load packages
library("igraph", lib.loc="~/Downloads/R-3.3.1/library")
library("moments", lib.loc="/usr/local/lib/R/site-library")
library("Matrix", lib.loc="/usr/local/lib/R/library")
library("data.table", lib.loc="/usr/local/lib/R/site-library")
library("doParallel", lib.loc="/usr/local/lib/R/site-library")

## Get unique TransTypeID
uniqueTransTypeID = unique(datTransNoDup$TransTypeID) ## Outside program !!!!
## Custom min/max function
Min = function(vec,def=NaN){
  x=suppressWarnings(min(vec,na.rm=T))
  if(is.finite(x)){x}
  else{def}
}
Max = function(vec,def=NaN){
  x=suppressWarnings(max(vec,na.rm=T))
  if(is.finite(x)){x}
  else{def}
}

## Create data.frame with Col1 = "AccountID", Col2 = "PartyID"
t = lapply(PartyIDSummary, function(x){x$accountsID}) ## can be precomputed
AccountVsPartyID = data.table(PartyID = rep(names(t), sapply(t, length)), AccountID = paste('A',unlist(t),sep=''), key='AccountID') ## can be precomputed

########################################################################################
createMixedGraph = function(dir=T, shift_v=0, alpha=0.85){
  ## dir -- boolean, whether to consider edge directions
  ## PartyIDSummary -- a list of lists(Char - PartyID, CharVec - accountsID, Int - numTransactions)
  
  ########################################################################################
  # Loop through creationDate(CaseID) and grow the network in each iteration
  # Extract features(scores + 'clique' + shortest path + RFM...) in each iteration
  ########################################################################################
  dates = sort(unique(datFullNameVar$creationDate)) + shift_v # Here we shift creationDate by 2 weeks
  l = length(dates)
  #l=26
  ## PageRank score combinations
  pr_non_weighted = data.frame(personalized = c("fraud","fraudSTw","fraudMTw","fraudMLTw","fraudLTw",
                                                "fraudD","fraudSTwd","fraudMTwd","fraudMLTwd","fraudLTwd")
                               ,weights=NA,stringsAsFactors = F)
  pr_weighted = data.frame(personalized = c(rep(c("fraudSTw","fraudMTw","fraudMLTw","fraudLTw"),4),
                                            rep(c("fraudSTwd","fraudMTwd","fraudMLTwd","fraudLTwd"),4))
                           ,weights=rep(c("weightSTE","weightMTE","weightMLTE","weightLTE","weightSTL",
                                          "weightMTL","weightMLTL","weightLTL","weightSTLOG","weightMTLOG",
                                          "weightMLTLOG","weightLTLOG",'weightSTLOGAmount','weightMTLOGAmount',
                                          'weightMLTLOGAmount','weightLTLOGAmount'),2),stringsAsFactors = F)
  pr_comb = rbind(pr_non_weighted,pr_weighted)
  weights_dist = unique(pr_comb$weights)
  
  ## Create empty instance of data.table Features
  Features=data.table(NULL)
  
  time=0
  ### During each iteration create two types of network:
  ## 1.) Unipartite AccountID network without unknown nodes
  ## - for this network add vertex attributes(i.e. personalized restart vectors) and edge attributes(i.e. edge weights)
  ## 2.) Bipartite PartyID/AccountID network with unknown AccountID node
  ## - for this network, keep vertex attributes(name, color, shape, size) and edge attributes(edge weights,
  ##   amount, currency, transtype, transchannel)
  for(d in 1:l){
    print(d)
    ## Initiate doChange indicator
    doChange=F
    time[d]=Sys.time()
    # Get all CaseIDs for given creationDate
    caseID = datFullNameVar[creationDate == (dates[d]-shift_v) & !CaseID %in% nonTransCaseID$CaseID,CaseID]
    # Get all corresponding PartyIDs
    partyID = subset(datPartyCaseRelation, CaseID %in% caseID & PartyID != 'None')
    partyID = partyID[order(as.numeric(PartyID))]
    # Get all corresponding AccountIDs
    accountsID = unlist(lapply(PartyIDSummary[unique(partyID$PartyID[!partyID$PartyID%in%nonTransPartyID$PartyID])], FUN=function(x){x$accountsID}))
    if(d == 1){
      tmpTrans = datTransNoDup[PostingDate <= dates[d]] # Other features, add 14 days
      # Extract within accounts network first
      tmpA_network = graph.data.frame(tmpTrans[,c("AccountID", "CounterPartyAccountID"),with=F], directed = dir)
      ## Set edge attributes within AccountID links
      ## Much faster assignment: Use external list for edge attributes instead of igraph object
      e_attr = list(Amount=tmpTrans[,Amount], TransChannelID = tmpTrans[,TransactionChannelID], TransTypeID = tmpTrans[,TransTypeID], PostingDate =
                      tmpTrans[,PostingDate], Currency = tmpTrans[,CurrencyCodeOrig])
      
      ### Add AccountIDs of PartyIDs which are not in current network(i.e. Manual cases)
      manual_PID = partyID[Manual==1,PartyID]
      manual_AID = AccountVsPartyID[PartyID %in% manual_PID]
      manual_AID = manual_AID[!manual_AID$AccountID%in%vertex_attr(tmpA_network, 'name')]
      tmpA_network = tmpA_network + manual_AID$AccountID
      #################################################
      ## Set vertex attributes
      tmp = AccountVsPartyID[AccountID %in% vertex_attr(tmpA_network, name = 'name')]
      tmp = merge(tmp,partyID,by='PartyID')
      tmp = tmp[order(as.numeric(substr(tmp$AccountID,2,nchar(tmp$AccountID))))]
      ind = which(vertex_attr(tmpA_network, 'name') %in% tmp$AccountID)
      ord = order(as.numeric(substr(vertex_attr(tmpA_network, 'name')[ind],2,nchar(vertex_attr(tmpA_network, 'name')[ind]))))
      ## Set initial fraud vectors and fraudDate
      
      N = vcount(tmpA_network)
      zero = rep(0,N)
      vert_attr = list(fraud = zero, fraudDate = zero, fraudSTw = zero, fraudMTw=zero, fraudMLTw=zero,
                       fraudLTw=zero, fraudD = zero, fraudSTwd=zero, fraudMTwd=zero, fraudMLTwd=zero,
                       fraudLTwd=zero, investigated=zero)
      vert_attr[['fraudDate']][ind] = dates[d]
      vert_attr[['fraud']][ind][ord] = abs(as.numeric(tmp$CaseStatus)-2)*ifelse(tmp$Implicated!="Y",0.5,1)
      vert_attr[['investigated']][ind] = 1
      ##################################  
      # Try to add PartyID nodes preserving all attributes
      col.order = colnames(tmpTrans)
      col.order[3] = colnames(tmpTrans)[7]
      col.order[7] = colnames(tmpTrans)[3]
      tmpTrans[CreditDebitCode=='C',] = tmpTrans[CreditDebitCode=='C',col.order,with=F]
      tmpP = merge(tmpTrans,AccountVsPartyID,by.x='CounterPartyAccountID',by.y='AccountID',all.x=T)
      u = unique(tmpP[,c('PartyID.x','AccountID'),with=F])
      tmpP = tmpP[!is.na(PartyID.y)]
      u2 = unique(tmpP[,c("PartyID.y",'CounterPartyAccountID'),with=F]); colnames(u2) = colnames(manual_AID) = colnames(u)
      u = unique(rbind(rbind(u,u2),manual_AID)); 
      ## u contains all PartyID -- AccountID combinations in current network
      u=u[order(as.numeric(substr(u[[2]],2,nchar(u[[2]])))),]
      ### Add PartyID vertices and corresponding edges and attributes(vertex/edge)
      pid_nodes = unique(u$PartyID.x)
      pid_nodes = pid_nodes[order(as.numeric(pid_nodes))]
      tmpA_network = tmpA_network + pid_nodes
      ## Add vertex type to separte nodes
      vert_attr[['type']] = ifelse(vertex_attr(tmpA_network, 'name')%in%u$PartyID.x,T,F)
      ## Add fraud indicator to PartyID nodes
      new_P = rev(seq(vcount(tmpA_network),by=-1,length.out = length(pid_nodes)))
      vert_attr[['fraud']][new_P] = ifelse(pid_nodes%in%partyID[CaseStatus=='1' & Implicated =='Y',PartyID],1,ifelse(pid_nodes%in%partyID[CaseStatus=='1' & Implicated =='N',PartyID],0.5,0))
      ## Set fraudDate
      vert_attr[['fraudDate']][new_P] = ifelse(pid_nodes%in%partyID[CaseStatus=='1',PartyID],dates[d],0)
      vert_attr[['fraudSTw']][new_P] = NA
      vert_attr[['fraudMTw']][new_P] = NA
      vert_attr[['fraudMLTw']][new_P] = NA
      vert_attr[['fraudLTw']][new_P] = NA
      vert_attr[['fraudD']][new_P] = NA
      vert_attr[['fraudSTwd']][new_P] = NA
      vert_attr[['fraudMTwd']][new_P] = NA
      vert_attr[['fraudMLTwd']][new_P] = NA
      vert_attr[['fraudLTwd']][new_P] = NA
      ## Investigated -- same as for AccountID nodes
      vert_attr[['investigated']][new_P] = ifelse(pid_nodes%in%partyID[,PartyID], 1, 0)
      
      ##################################  
      # Add edges connecting PartyID nodes to corresponding AccountID nodes, make them undirected
      # Network is directed, therefore we add edges in both direction between PartyID and AccountID
      e = as.vector(t(cbind(u,u[,c('AccountID','PartyID.x'),with=F])))
      tmpA_network = add.edges(tmpA_network, e)
      ones = rep(1,ecount(tmpA_network))
      new_PAe = seq(ecount(tmpA_network),by=-1,length.out = length(e)/2)
      e_attr[['Amount']][new_PAe] = 1
      e_attr[['TransChannelID']][new_PAe] = NA
      e_attr[['TransTypeID']][new_PAe] = NA
      e_attr[['PostingDate']][new_PAe] = NA
      e_attr[['Currency']][new_PAe] = NA
      e_attr[['weightSTE']] = ones
      e_attr[['weightMTE']] = ones
      e_attr[['weightMLTE']] = ones
      e_attr[['weightLTE']] = ones
      e_attr[['weightSTL']] = ones
      e_attr[['weightMTL']] = ones
      e_attr[['weightMLTL']] = ones
      e_attr[['weightLTL']] = ones
      e_attr[['weightSTLOG']] = ones
      e_attr[['weightMTLOG']] = ones
      e_attr[['weightMLTLOG']] = ones
      e_attr[['weightLTLOG']] = ones
      e_attr[['weightSTLOGAmount']] = ones
      e_attr[['weightMTLOGAmount']] = ones
      e_attr[['weightMLTLOGAmount']] = ones
      e_attr[['weightLTLOGAmount']] = ones
      ##################################
    }
    else if(d<=17){# burn-in
      ###############################################################
      ## Check investigated
      w_investigatedPID = which(partyID$PartyID%in%vertex_attr(tmpA_network)[['name']][vert_attr[['investigated']]!=0])
      if(length(w_investigatedPID)){
        w_investigatedPID = partyID[w_investigatedPID,PartyID]
        ## Do change <==> there is a change in fraud indicator(i.e. t_0:F=0 --> t_1:F=1 or the other way around).
        ## Make sure that we do not have multiple PartyIDs here (i.e. PartyID in two CaseIDs at the same time)
        now = data.table(PartyID = w_investigatedPID,now=abs(as.numeric(partyID[PartyID%in%w_investigatedPID,CaseStatus])-2)*ifelse(partyID[PartyID%in%w_investigatedPID,Implicated]!="Y",0.5,1))
        now = now[,list(now=mean(now)),by=PartyID]$now
        w_investigatedPID = unique(w_investigatedPID)
        doChange = now!= vert_attr[['fraud']][w_investigatedPID]
        if(any(doChange)){## Get AccountIDs
          w_investigatedPID = w_investigatedPID[doChange]
          fraud_DiffPID = as.numeric(dates[d]-vert_attr[['fraudDate']][w_investigatedPID])
          originalFraudPID = vert_attr[['fraud']][w_investigatedPID]
          w_investigatedAID = names(unlist(lapply(w_investigatedPID,function(x){neighbors(tmpA_network,v=x,'out')})))
          fraud_DiffAID = as.numeric(dates[d]-vert_attr[['fraudDate']][w_investigatedAID])
          originalFraudAID = vert_attr[['fraud']][w_investigatedAID]
        }
      }
      ###############################################################
      
      ###############################################################
      ## Subset transactional data for newly added transactions
      tmpTrans = datTransNoDup[dates[d-1] < PostingDate & PostingDate <= dates[d]]
      # Extract within accounts network first
      A_network = graph.data.frame(tmpTrans[,c("AccountID", "CounterPartyAccountID"),with=F], directed = dir)
      ###############################################################
      ## Find new AccountID nodes
      # nodes to add, take care no to add duplicate!!
      manual_PID = unique(partyID[Manual==1,PartyID])
      manual_AID = AccountVsPartyID[PartyID %in% manual_PID]
      aid_nodes = vertex_attr(A_network, 'name')[!vertex_attr(A_network, 'name')%in%vertex_attr(tmpA_network, 'name')] 
      if(!is.null(aid_nodes)){
        tmpA_network = tmpA_network + aid_nodes # add new AccountID nodes
      }
      manual_AID = manual_AID[!manual_AID$AccountID%in%vertex_attr(tmpA_network, 'name')]
      tmpA_network = tmpA_network + manual_AID$AccountID
      new_A = rev(seq(vcount(tmpA_network),by=-1,length.out = length(aid_nodes)+length(manual_AID$AccountID))) ## Vertex ids for new vertices
      e = as.vector(t(get.edgelist(A_network))) ## edgelist in vector
      ## Add new edges together with their attributes from data
      tmpA_network = add.edges(tmpA_network, e)
      new_Aedges = rev(seq(ecount(tmpA_network),by=-1,length.out = length(e)/2))
      e_attr[['Amount']][new_Aedges]=tmpTrans$Amount
      e_attr[['TransTypeID']][new_Aedges]=tmpTrans$TransTypeID
      e_attr[['TransChannelID']][new_Aedges]=tmpTrans$TransactionChannelID
      e_attr[['PostingDate']][new_Aedges]=tmpTrans$PostingDate
      e_attr[['Currency']][new_Aedges]=tmpTrans$CurrencyCodeOrig
      ## Add new PartyIDs if any
      ## We do this directly after adding AccountID nodes such that degree gives correct number
      ###############################################################
      ## Here manipulate tmpTrans data by reordering columns AccountID, CounterPartyAccountID in order to find
      ## correct CounterPartyID
      col.order = colnames(tmpTrans)
      col.order[3] = colnames(tmpTrans)[7]
      col.order[7] = colnames(tmpTrans)[3]
      tmpTrans[CreditDebitCode=='C',] = tmpTrans[CreditDebitCode=='C',col.order,with=F]
      tmpP = merge(tmpTrans,AccountVsPartyID,by.x='CounterPartyAccountID',by.y='AccountID',all.x=T)
      u = unique(tmpP[,c('PartyID.x','AccountID'),with=F])
      tmpP = tmpP[!is.na(PartyID.y)]
      u2 = unique(tmpP[,c("PartyID.y",'CounterPartyAccountID'),with=F]); colnames(u2) = colnames(manual_AID) = colnames(u)
      u = unique(rbind(rbind(u,u2),manual_AID)); 
      ## u contains all PartyID -- AccountID combinations in current network
      u=u[order(as.numeric(substr(u[[2]],2,nchar(u[[2]])))),]
      ## Add PartyID vertices and corresponding edges and attributes(vertex/edge)
      ## Be careful not to add duplicated vertices already in network!!!
      pid_nodes = unique(u$PartyID.x)
      ## Find new PartyID nodes
      pid_nodes = pid_nodes[!pid_nodes%in%vertex_attr(tmpA_network, 'name')]
      ###############################################################
      tmpA_network = tmpA_network + pid_nodes # add new PartyID nodes
      new_P = rev(seq(vcount(tmpA_network),by=-1,length.out = length(pid_nodes))) ## Vertex ids for new vertices
      ##################################  
      ## Add vertex type to separte nodes
      vert_attr[['type']][new_A] = F
      vert_attr[['fraudDate']][c(new_A,new_P)] = vert_attr[['fraud']][c(new_A,new_P)] = vert_attr[['investigated']][c(new_A,new_P)] = 
      vert_attr[['fraudSTw']][c(new_A,new_P)] = vert_attr[['fraudMTw']][c(new_A,new_P)] = vert_attr[['fraudMLTw']][c(new_A,new_P)] = 
      vert_attr[['fraudLTw']][c(new_A,new_P)] =  vert_attr[['fraudD']][c(new_A,new_P)] = vert_attr[['fraudSTwd']][c(new_A,new_P)] = 
      vert_attr[['fraudMTwd']][c(new_A,new_P)] = vert_attr[['fraudMLTwd']][c(new_A,new_P)] = vert_attr[['fraudLTwd']][c(new_A,new_P)] =  0 ## replace NA with 0
      vert_attr[['type']][new_P] = T # partyID
      ## Add node names to fraud/fraudDate attribute vectors for easier subsetting: increases list size(as object)
      names(vert_attr[['fraud']]) = names(vert_attr[['fraudDate']]) = vertex_attr(tmpA_network)[['name']] 
      ##########################################
      ### CHECK IF SOME OF THE NEW ACCOUNTID NODES DOES NOT BELONG TO ALREADY FRAUDULENT PARTYID
      ### IF YES, THEN WE ASSIGN TO IT THE SAME INITIAL FRAUD SCORES AS OTHER ACCOUNTIDs belonging to PARTYID
      u_new = u[PartyID.x%in%pid_nodes] ## Completely new PartyID nodes
      u_exist = u[(AccountID%in%aid_nodes | AccountID%in%manual_AID$AccountID) & !PartyID.x %in% u_new$PartyID.x] ## New AccountID nodes, belonging to already existing PartyID nodes
      ##########################################
      w_PID = which(u_exist$PartyID.x %in% vertex_attr(tmpA_network)[['name']][vert_attr[['fraud']]!=0])
      if(length(w_PID)){# i.e. some of the new AccountIDs belongs to previously fraudulent PartyID
        w_assignFraud = u_exist[w_PID,AccountID]
        ## Get fraud indicator of corresponding AccountIDs
        ## No need to check AccountID nodes, since PartyID nodes have the same initial score
        fraud_vec=vert_attr[['fraud']][u_exist[w_PID,PartyID.x]]
        fraudDate_vec=vert_attr[['fraudDate']][u_exist[w_PID,PartyID.x]]
        vert_attr[['fraud']][w_assignFraud] = fraud_vec
        vert_attr[['fraudDate']][w_assignFraud] = fraudDate_vec
      }
      # Add edges connecting PartyID nodes to corresponding AccountID nodes, make them undirected
      # Network is directed, therefore we add edges in both direction between PartyID and AccountID
      e_new = as.vector(t(cbind(u_new,u_new[,c('AccountID','PartyID.x'),with=F])))
      e_exist = as.vector(t(cbind(u_exist,u_exist[,c('AccountID','PartyID.x'),with=F])))
      # No edge attributes for these edges
      tmpA_network = add.edges(tmpA_network, c(e_new,e_exist))
      new_PAe = seq(ecount(tmpA_network), by=-1, length.out = length(c(e_new,e_exist))/2)
      e_attr[['Amount']][new_PAe] = 1
      e_attr[['TransChannelID']][new_PAe] = NA
      e_attr[['TransTypeID']][new_PAe] = NA
      e_attr[['PostingDate']][new_PAe] = NA
      e_attr[['Currency']][new_PAe] = NA
      e_attr[['weightSTE']][new_PAe] = 1
      e_attr[['weightMTE']][new_PAe] = 1
      e_attr[['weightMLTE']][new_PAe] = 1
      e_attr[['weightLTE']][new_PAe] = 1
      e_attr[['weightSTL']][new_PAe] = 1
      e_attr[['weightMTL']][new_PAe] = 1
      e_attr[['weightMLTL']][new_PAe] = 1
      e_attr[['weightLTL']][new_PAe] = 1
      e_attr[['weightSTLOG']][new_PAe] = 1
      e_attr[['weightMTLOG']][new_PAe] = 1
      e_attr[['weightMLTLOG']][new_PAe] = 1
      e_attr[['weightLTLOG']][new_PAe] = 1
      e_attr[['weightSTLOGAmount']][new_PAe] = 1
      e_attr[['weightMTLOGAmount']][new_PAe] = 1
      e_attr[['weightMLTLOGAmount']][new_PAe] = 1
      e_attr[['weightLTLOGAmount']][new_PAe] = 1
      ###############################################################
      ## Set vertex attributes
      ## First find AccountIDs considered in current CaseIDs
      tmp = AccountVsPartyID[AccountID %in% vertex_attr(tmpA_network, 'name')]
      tmp = merge(tmp,partyID,by='PartyID')
      tmp = tmp[order(as.numeric(substr(tmp$AccountID,2,nchar(tmp$AccountID))))]
      ind = which(vertex_attr(tmpA_network, 'name') %in% tmp$AccountID)
      ord = order(as.numeric(substr(vertex_attr(tmpA_network, 'name',ind),2,nchar(vertex_attr(tmpA_network, 'name',ind)))))
      
      ind_P = which(vertex_attr(tmpA_network, 'name')%in%partyID$PartyID)
      w_P = which(partyID$PartyID%in%vertex_attr(tmpA_network, 'name'))## Might happen that some PartyID is not in network(manual case)
      ord_P = order(as.numeric(vertex_attr(tmpA_network, 'name',ind_P)))
      
      ## First set all new AccountIDs to 0
      vert_attr[['fraudSTw']][new_A] = vert_attr[['fraudMTw']][new_A] = vert_attr[['fraudMLTw']][new_A] = vert_attr[['fraudLTw']][new_A] =  vert_attr[['fraudD']][new_A] = vert_attr[['fraudSTwd']][new_A] = vert_attr[['fraudMTwd']][new_A] = vert_attr[['fraudMLTwd']][new_A] = vert_attr[['fraudLTwd']][new_A] =  0 ## replace NA with 0
      ################################################
      ## Set fraudDate
      vert_attr[['fraudDate']][ind_P] = dates[d]
      vert_attr[['fraudDate']][ind] = dates[d]
      ##################################  
      ## Investigated
      vert_attr[['investigated']][ind_P] = vert_attr[['investigated']][ind_P]+1
      vert_attr[['investigated']][ind] = vert_attr[['investigated']][ind]+1
      ## Fraud
      insert=partyID[,c('CaseStatus'):= list(abs(as.numeric(partyID$CaseStatus)-2)*ifelse(Implicated=='Y',1,0.5)),with=F][, .SD, .SDcols = c('PartyID','CaseStatus')]
      insert=insert[,lapply(.SD,mean),by=PartyID, .SDcols=sapply(insert, is.numeric)]
      w_P = which(insert$PartyID%in%vertex_attr(tmpA_network, 'name'))
      vert_attr[['fraud']][ind_P][ord_P] = insert[w_P,CaseStatus]
      ## AccountID
      insert = tmp[,c('CaseStatus'):= list(abs(as.numeric(CaseStatus)-2)*ifelse(Implicated=='Y',1,0.5)),with=F][, .SD, .SDcols = c('AccountID','CaseStatus')]
      insert=insert[,lapply(.SD,mean),by=AccountID, .SDcols=sapply(insert, is.numeric)]
      print(length(ind)); print(length(insert$CaseStatus))
      vert_attr[['fraud']][ind][ord] = insert$CaseStatus
      ### Take care of P_ID and A_ID nodes that must be changed (doChange) due to previously being in network
      ## if(any(doChange))
      if(any(doChange)){
        ## Linear weights
        w1 = ifelse(fraud_DiffPID<730,-fraud_DiffPID*(1/730)+1,exp(-7/730*fraud_DiffPID))
        w2 = ifelse(fraud_DiffAID<730,-fraud_DiffAID*(1/730)+1,exp(-7/730*fraud_DiffAID))
        vert_attr[['fraud']][w_investigatedPID] = (originalFraudPID*w1 + vert_attr[['fraud']][w_investigatedPID])/(w1+1)
        vert_attr[['fraud']][w_investigatedAID] = (originalFraudAID*w2 + vert_attr[['fraud']][w_investigatedAID])/(w2+1)
      }
      ##################################  
    }
    else{# extraction starts
      ###############################################################
      ## Check investigated
      w_investigatedPID = which(partyID$PartyID%in%vertex_attr(tmpA_network)[['name']][vert_attr[['investigated']]!=0])
      if(length(w_investigatedPID)){
        w_investigatedPID = partyID[w_investigatedPID,PartyID]
        ## Do change <==> there is a change in fraud indicator(i.e. t_0:F=0 --> t_1:F=1 or the other way around).
        ## Make sure that we do not have multiple PartyIDs here (i.e. PartyID in two CaseIDs at the same time)
        now = data.table(PartyID = w_investigatedPID,now=abs(as.numeric(partyID[PartyID%in%w_investigatedPID,CaseStatus])-2)*ifelse(partyID[PartyID%in%w_investigatedPID,Implicated]!="Y",0.5,1))
        now = now[,list(now=mean(now)),by=PartyID]$now
        w_investigatedPID = unique(w_investigatedPID)
        doChange = now!= vert_attr[['fraud']][w_investigatedPID]
        if(any(doChange)){## Get AccountIDs
          w_investigatedPID = w_investigatedPID[doChange]
          fraud_DiffPID = as.numeric(dates[d]-vert_attr[['fraudDate']][w_investigatedPID])
          originalFraudPID = vert_attr[['fraud']][w_investigatedPID]
          w_investigatedAID = names(unlist(lapply(w_investigatedPID,function(x){neighbors(tmpA_network,v=x,'out')})))
          fraud_DiffAID = as.numeric(dates[d]-vert_attr[['fraudDate']][w_investigatedAID])
          originalFraudAID = vert_attr[['fraud']][w_investigatedAID]
        }
      }
      ###############################################################
      # Subset transactional data for newly added transactions
      tmpTrans = datTransNoDup[dates[d-1] < PostingDate & PostingDate <= dates[d]]
      # Extract within accounts network first
      A_network = graph.data.frame(tmpTrans[,c("AccountID", "CounterPartyAccountID"),with=F], directed = dir)
      ###############################################################
      ## Find new AccountID nodes
      # nodes to add, take care no to add duplicate!!
      manual_PID = unique(partyID[Manual==1,PartyID])
      manual_AID = AccountVsPartyID[PartyID %in% manual_PID]
      aid_nodes = vertex_attr(A_network, 'name')[!vertex_attr(A_network, 'name')%in%vertex_attr(tmpA_network, 'name')] 
      if(!is.null(aid_nodes)){
        tmpA_network = tmpA_network + aid_nodes # add new AccountID nodes
      }
      manual_AID = manual_AID[!manual_AID$AccountID%in%vertex_attr(tmpA_network, 'name')]
      tmpA_network = tmpA_network + manual_AID$AccountID
      new_A = rev(seq(vcount(tmpA_network),by=-1,length.out = length(aid_nodes)+length(manual_AID$AccountID))) ## Vertex ids for new vertices
      e = as.vector(t(get.edgelist(A_network))) ## edgelist in vector
      ## Add new edges together with their attributes from data
      tmpA_network = add.edges(tmpA_network, e)
      new_Aedges = rev(seq(ecount(tmpA_network),by=-1,length.out = length(e)/2))
      e_attr[['Amount']][new_Aedges]=tmpTrans$Amount
      e_attr[['TransTypeID']][new_Aedges]=tmpTrans$TransTypeID
      e_attr[['TransChannelID']][new_Aedges]=tmpTrans$TransactionChannelID
      e_attr[['PostingDate']][new_Aedges]=tmpTrans$PostingDate
      e_attr[['Currency']][new_Aedges]=tmpTrans$CurrencyCodeOrig
      ## Add new PartyIDs if any
      ## We do this directly after adding AccountID nodes such that degree gives correct number
      ###############################################################
      ## Here manipulate tmpTrans data by reordering columns AccountID, CounterPartyAccountID in order to find
      ## correct CounterPartyID
      col.order = colnames(tmpTrans)
      col.order[3] = colnames(tmpTrans)[7]
      col.order[7] = colnames(tmpTrans)[3]
      tmpTrans[CreditDebitCode=='C',] = tmpTrans[CreditDebitCode=='C',col.order,with=F]
      tmpP = merge(tmpTrans,AccountVsPartyID,by.x='CounterPartyAccountID',by.y='AccountID',all.x=T)
      u = unique(tmpP[,c('PartyID.x','AccountID'),with=F])
      tmpP = tmpP[!is.na(PartyID.y)]
      u2 = unique(tmpP[,c("PartyID.y",'CounterPartyAccountID'),with=F]); colnames(u2) = colnames(manual_AID) = colnames(u)
      u = unique(rbind(rbind(u,u2),manual_AID)); 
      ## u contains all PartyID -- AccountID combinations in current network
      u=u[order(as.numeric(substr(u[[2]],2,nchar(u[[2]])))),]
      ## Add PartyID vertices and corresponding edges and attributes(vertex/edge)
      ## Be careful not to add duplicated vertices already in network!!!
      pid_nodes = unique(u$PartyID.x)
      ## Find new PartyID nodes
      pid_nodes = pid_nodes[!pid_nodes%in%vertex_attr(tmpA_network, 'name')]
      ###############################################################
      tmpA_network = tmpA_network + pid_nodes # add new PartyID nodes
      new_P = rev(seq(vcount(tmpA_network),by=-1,length.out = length(pid_nodes))) ## Vertex ids for new vertices
      ##################################  
      ## Add vertex type to separte nodes
      vert_attr[['type']][new_A] = F
      vert_attr[['fraudDate']][c(new_A,new_P)] = vert_attr[['fraud']][c(new_A,new_P)] = vert_attr[['investigated']][c(new_A,new_P)] = 
      vert_attr[['fraudSTw']][c(new_A,new_P)] = vert_attr[['fraudMTw']][c(new_A,new_P)] = vert_attr[['fraudMLTw']][c(new_A,new_P)] = 
      vert_attr[['fraudLTw']][c(new_A,new_P)] =  vert_attr[['fraudD']][c(new_A,new_P)] = vert_attr[['fraudSTwd']][c(new_A,new_P)] = 
      vert_attr[['fraudMTwd']][c(new_A,new_P)] = vert_attr[['fraudMLTwd']][c(new_A,new_P)] = vert_attr[['fraudLTwd']][c(new_A,new_P)] =  0 ## replace NA with 0
      vert_attr[['type']][new_P] = T # partyID
      ## Add node names to fraud/fraudDate attribute vectors for easier subsetting: increases list size(as object)
      names(vert_attr[['fraud']]) = names(vert_attr[['fraudDate']]) = vertex_attr(tmpA_network)[['name']] 
      ### CHECK IF SOME OF THE NEW ACCOUNTID NODES DOES NOT BELONG TO ALREADY FRAUDULENT PARTYID
      ### IF YES, THEN WE ASSIGN TO IT THE SAME INITIAL FRAUD SCORES AS OTHER ACCOUNTID BELONGING TO PARTYID
      u_new = u[PartyID.x%in%pid_nodes] ## Completely new PartyID nodes
      u_exist = u[(AccountID%in%aid_nodes | AccountID%in%manual_AID$AccountID) & !PartyID.x %in% u_new$PartyID.x] ## New AccountID nodes, belonging to already existing PartyID nodes
      ##########################################
      ## Find indeces to new AccountID nodes belonging to previously fraudulent PartyID nodes
      w_PID = which(u_exist$PartyID.x %in% vertex_attr(tmpA_network)[['name']][vert_attr[['fraud']]!=0])
      if(length(w_PID)){# i.e. some of the new AccountIDs belongs to previously fraudulent PartyID
        w_assignFraud = u_exist[w_PID,AccountID]
        ## Get fraud indicator of corresponding AccountIDs
        ## No need to check AccountID nodes, since PartyID nodes have the same initial score
        fraud_vec=vert_attr[['fraud']][u_exist[w_PID,PartyID.x]]
        fraudDate_vec=vert_attr[['fraudDate']][u_exist[w_PID,PartyID.x]]
        vert_attr[['fraud']][w_assignFraud] = fraud_vec
        vert_attr[['fraudDate']][w_assignFraud] = fraudDate_vec
      }
      # Add edges connecting PartyID nodes to corresponding AccountID nodes, make them undirected
      # Network is directed, therefore we add edges in both direction between PartyID and AccountID
      e_new = as.vector(t(cbind(u_new,u_new[,c('AccountID','PartyID.x'),with=F])))
      e_exist = as.vector(t(cbind(u_exist,u_exist[,c('AccountID','PartyID.x'),with=F])))
      # No edge attributes for these edges, only color
      tmpA_network = add.edges(tmpA_network, c(e_new,e_exist))
      new_PAe = seq(ecount(tmpA_network), by=-1, length.out = length(c(e_new,e_exist))/2)
      e_attr[['Amount']][new_PAe] = 1
      e_attr[['TransChannelID']][new_PAe] = NA
      e_attr[['TransTypeID']][new_PAe] = NA
      e_attr[['PostingDate']][new_PAe] = NA
      e_attr[['Currency']][new_PAe] = NA
      e_attr[['weightSTE']][new_PAe] = 1
      e_attr[['weightMTE']][new_PAe] = 1
      e_attr[['weightMLTE']][new_PAe] = 1
      e_attr[['weightLTE']][new_PAe] = 1
      e_attr[['weightSTL']][new_PAe] = 1
      e_attr[['weightMTL']][new_PAe] = 1
      e_attr[['weightMLTL']][new_PAe] = 1
      e_attr[['weightLTL']][new_PAe] = 1
      e_attr[['weightSTLOG']][new_PAe] = 1
      e_attr[['weightMTLOG']][new_PAe] = 1
      e_attr[['weightMLTLOG']][new_PAe] = 1
      e_attr[['weightLTLOG']][new_PAe] = 1
      e_attr[['weightSTLOGAmount']][new_PAe] = 1
      e_attr[['weightMTLOGAmount']][new_PAe] = 1
      e_attr[['weightMLTLOGAmount']][new_PAe] = 1
      e_attr[['weightLTLOGAmount']][new_PAe] = 1
      ###############################################################
      ## Set vertex attributes
      ## First find AccountIDs considered in current CaseIDs
      tmp = AccountVsPartyID[AccountID %in% vertex_attr(tmpA_network, 'name')]
      tmp = merge(tmp,partyID,by='PartyID')
      tmp = tmp[order(as.numeric(substr(tmp$AccountID,2,nchar(tmp$AccountID))))]
      ind = which(vertex_attr(tmpA_network, 'name') %in% tmp$AccountID)
      ord = order(as.numeric(substr(vertex_attr(tmpA_network, 'name',ind),2,nchar(vertex_attr(tmpA_network, 'name',ind)))))
      
      ## Based on time span we are interested in, we might construct different degree-adapted vectors
      ## since some time spans might introduce isolated fraudulent nodes(i.e. degree(v)=0). That means they will
      ## not propagate its fraud through the network.
      ## We need to subset full network by edges to find corresponding degree
      diff = as.numeric(dates[d] - e_attr[['PostingDate']]) ## PartyID -- AccountID edges are not here
      # 
      st_net = delete.vertices(simplify(as.undirected(subgraph.edges(tmpA_network, eids = which(diff <= 30), delete.vertices = F)),edge.attr.comb = 'ignore'), v='Unknown')
      mt_net = delete.vertices(simplify(as.undirected(subgraph.edges(tmpA_network, eids = which(diff <= 90), delete.vertices = F)),edge.attr.comb = 'ignore'), v='Unknown')
      mlt_net = delete.vertices(simplify(as.undirected(subgraph.edges(tmpA_network, eids = which(diff <= 183), delete.vertices = F)),edge.attr.comb = 'ignore'), v='Unknown')
      lt_net = delete.vertices(simplify(as.undirected(subgraph.edges(tmpA_network, eids = which(diff <= 365), delete.vertices = F)),edge.attr.comb = 'ignore'), v='Unknown')
      tmp_full = delete.vertices(simplify(as.undirected(tmpA_network,edge.attr.comb = 'ignore')),v='Unknown')
  
      ## Add fraud indicator to PartyID nodes
      ind_P = which(vertex_attr(tmpA_network, 'name')%in%partyID$PartyID)
      w_P = which(partyID$PartyID%in%vertex_attr(tmpA_network, 'name'))## Might happen that some PartyID is not in network(manual case)
      ord_P = order(as.numeric(vertex_attr(tmpA_network, 'name',ind_P)))
      ## Before we insert current fraud to the network, we need to extract features
      ## So we first set edge weights, since these are needed for calculation of features
      ## Set edge attributes
      ############################
      ### Set different weights based on time difference
      ## ST: short-term(1 month); MT: medium-term(3months); MLT: medium-long-term(6months); LT: long-term(1year)
      ## E - exponential decay, with exponential decay constant chosen s.t. weight is ~ 0 after k-days(30,90,180,365)
      ## L - linear decay, s.t. weight ~ 0 after k-days(30,90,180,365)
      ## LOG - logistic decay, with slow concave decay at the beginning s.t. after 1/2 of period it still remains
      ##       90% of weight, and weight ~ 0 after k-days(30,90,180,365)
      ############################
      w_Edge = which(!is.na(diff))
      diff = diff[w_Edge]
        
      e_attr[['weightSTE']][w_Edge] = exp(-(7/30)*diff)
      e_attr[['weightMTE']][w_Edge] = exp(-(7/90)*diff)
      e_attr[['weightMLTE']][w_Edge] = exp(-(7/183)*diff)
      e_attr[['weightLTE']][w_Edge] = exp(-(7/365)*diff)
      #
      e_attr[['weightSTL']][w_Edge] = ifelse(diff<30,-diff*(1/30)+1,exp(-(7/30)*diff))
      e_attr[['weightMTL']][w_Edge] = ifelse(diff<90,-diff*(1/90)+1,exp(-(7/90)*diff))
      e_attr[['weightMLTL']][w_Edge] = ifelse(diff<183,-diff*(1/183)+1,exp(-(7/183)*diff))
      e_attr[['weightLTL']][w_Edge] = ifelse(diff<365,-diff*(1/365)+1,exp(-(7/365)*diff)) 
      #
      e_attr[['weightSTLOG']][w_Edge] = 1.000012/(1+0.000012*exp(log((1.000012-0.9)/(0.000012*0.9))/15*(diff)))
      e_attr[['weightMTLOG']][w_Edge] = 1.000012/(1+0.000012*exp(log((1.000012-0.9)/(0.000012*0.9))/45*(diff)))
      e_attr[['weightMLTLOG']][w_Edge] = 1.000012/(1+0.000012*exp(log((1.000012-0.9)/(0.000012*0.9))/91.5*(diff)))
      e_attr[['weightLTLOG']][w_Edge] = 1.000012/(1+0.000012*exp(log((1.000012-0.9)/(0.000012*0.9))/182.5*(diff))) 
      ##################################  
      amount = e_attr[['Amount']][w_Edge]
      e_attr[['weightSTLOGAmount']][w_Edge] = e_attr[['weightSTLOG']][w_Edge]*amount
      e_attr[['weightMTLOGAmount']][w_Edge] = e_attr[['weightMTLOG']][w_Edge]*amount
      e_attr[['weightMLTLOGAmount']][w_Edge] = e_attr[['weightMLTLOG']][w_Edge]*amount
      e_attr[['weightLTLOGAmount']][w_Edge] = e_attr[['weightLTLOG']][w_Edge]*amount
      ##################################  
      ## Now we need to update restart fraud vectors
      ## If some of the new AccountIDs in network belong to PartyID that has already been part of a fraud
      ## this new account get the same restart score(current) as other accounts that belong to the same PartyID
      ## Moreover, fraudulent nodes from past must be updated
      w = which(vert_attr[['fraud']] != 0 & !vert_attr[['type']]) # past fraud
      f_diff = as.numeric(dates[d]-vert_attr[['fraudDate']][w])
      
      vert_attr[['fraudSTw']][w] = vert_attr[['fraud']][w]*1.000012/(1+0.000012*exp(log((1.000012-0.9)/(0.000012*0.9))/15*(f_diff)))
      vert_attr[['fraudMTw']][w] = vert_attr[['fraud']][w]*1.000012/(1+0.000012*exp(log((1.000012-0.9)/(0.000012*0.9))/45*(f_diff)))
      vert_attr[['fraudMLTw']][w] = vert_attr[['fraud']][w]*1.000012/(1+0.000012*exp(log((1.000012-0.9)/(0.000012*0.9))/91.5*(f_diff)))
      vert_attr[['fraudLTw']][w] = vert_attr[['fraud']][w]*1.000012/(1+0.000012*exp(log((1.000012-0.9)/(0.000012*0.9))/182.5*(f_diff))) 
      ## Degree-adapted
      vert_attr[['fraudD']][w] = (degree(tmp_full, v=w-1 , mode='all')-1)*vert_attr[['fraud']][w]
      
      vert_attr[['fraudSTwd']][w] = vert_attr[['fraudSTw']][w]*degree(st_net, v=w-1 , mode='all')
      vert_attr[['fraudMTwd']][w] = vert_attr[['fraudMTw']][w]*degree(mt_net, v=w-1 , mode='all')
      vert_attr[['fraudMLTwd']][w] = vert_attr[['fraudMLTw']][w]*degree(mlt_net, v=w-1 , mode='all')
      vert_attr[['fraudLTwd']][w] = vert_attr[['fraudLTw']][w]*degree(lt_net, v=w-1 , mode='all')
      ######################################################
      ### Evaluate PageRank
      ## Release memory before parallel calculation
      if(sum(vertex_attr(tmpA_network)[['name']] %in% paste0('A',accountsID)) != length(ind)){print('Break-unequal length');break}
      rm(list=setdiff(ls(), c('dates','l','d','dir','datFullNameVar','nonTransCaseID','datPartyCaseRelation','PartyIDSummary',
                              'datAnonymizedVar','tmpA_network','datTransKnown','datTransNoDup','partyIDaccounts',
                              'nonTransPartyID','pr_comb','AccountVsPartyID','tmp','caseID','partyID','ind_P','ord_P',
                              'ind','ord','w_P','time','doChange','w_investigatedPID','w_investigatedAID',
                              'fraud_DiffPID','fraud_DiffAID','originalFraudAID','originalFraudPID',
                              'uniqueTransTypeID','Min','Max','Features','e_attr','vert_attr','weights_dist',
                              'shift_v','alpha')))
      gc()
      ###############################################################################
      ## The main extraction part of the progrm
      ###############################################################################
      ## 1.) PageRank on AccountID level
      ## Unipartite version for PageRank calculation
      ## Make it parallel
      ## PRPACK ca. 1.5-7 times faster than power depending on network structure
      ## For all PageRank combinations ca. 2.5-3 times faster
      ## We need PageRank network without PartyID nodes, remove 'Unknown' node and all nodes that connect only 
      ## to 'Unknown' node
      
      ## Remove 'Unknown'
      known_net = delete.vertices(tmpA_network,v='Unknown')
      ## Remove PartyID nodes
      pr_net = delete.vertices(known_net, v=names(vert_attr[['fraud']])[vert_attr[['type']]])
      ## Only PageRank of these accounts, to make it more memory efficient
      ## i.e. we calculate PageRank for full network but return only those nodes which are of current interest
      pr_a_id = unique(tmp$AccountID[tmp$AccountID%in%vertex_attr(pr_net)[['name']]])
      
      ## Find 'Hub' nodes. We define hub node as an AccountID node with more transactions than current
      ## 0.999 percentile
      tmp_deg = degree(known_net,mode='all')
      w_hubs = vertex_attr(known_net)[['name']][tmp_deg>=quantile(tmp_deg,probs=0.999) &
                                                  !vert_attr[['type']][-1]]
      ## We extract only PageRank to nodes which are not considered as Hubs, cos these accumulate most of the
      ## fraud score
      pr_a_id = pr_a_id[!pr_a_id%in%w_hubs]
      ## Get edglist
      e_tmpA = get.edgelist(tmpA_network)
      ## Edge within only known vertices
      w_EdgeKnown = (e_tmpA[,1] !=  'Unknown' & e_tmpA[,2] !=  'Unknown')
      ## Edges within only AccountIDs
      w_Edge = (substr(e_tmpA[,1],1,1) ==  'A' & substr(e_tmpA[,2],1,1) ==  'A')
      ## Vertices for PageRank
      w_Vert = names(vert_attr[[1]]) %in% vertex_attr(pr_net, 'name')
      ## Parallelized version ca. 3-4x faster
      ## Handle empty pr_a_id
      if(length(pr_a_id)){
        registerDoParallel(cores=4)
        pr_l = foreach(p=1:nrow(pr_comb), .packages = c('igraph','Matrix')) %dopar% {
          a=pr_iter2(pr_net, directed = F, weights = e_attr[[pr_comb[p,2]]][w_Edge], beta = vert_attr[[pr_comb[p,1]]][w_Vert], alpha=0.85)$vector
          a=a/max(a)
          a[pr_a_id]
        }
        names(pr_l) = paste0('PageRank',pr_comb[,1], pr_comb[,2])
        ## Aggregate PageRank scores first on PartyID level
        ## From AccountID level to PartyID level we assign max score
        if(length(pr_a_id)==1){
          pr_l = data.table(t(sapply(pr_l, `[`)), AccountID = names(pr_l[[1]]))
          colnames(pr_l)[1:42] = paste0('PageRank',pr_comb[,1], pr_comb[,2])
        } else{
          pr_l = data.table(sapply(pr_l, `[`), AccountID = names(pr_l[[1]]))
        }
        pr_l = merge(pr_l,tmp[,c('PartyID','AccountID'), with=F],by = 'AccountID')
        pr_l = pr_l[,lapply(.SD,max),by="PartyID",.SDcols=sapply(pr_l, is.numeric)]
        pr_l = merge(pr_l,partyID[,c('PartyID','Implicated','CaseID'),with=F],by='PartyID', all.x=T)
        ## Then on CaseID level
        ## From PartyID level to CaseID level we take weighted (by Implicated) average, 
        ## with double the weight on Implicated
        pr_l[,c('PartyID', 'weight'):= list(NULL, ifelse(Implicated=='Y',1,0.5))]
        pr_l=pr_l[,lapply(.SD,weighted.mean,w=weight),by=CaseID, .SDcols=sapply(pr_l, is.numeric)]
        ncols = ncol(pr_l)
        pr_l = pr_l[,-c(ncols-1,ncols),with=F]
        ###################################
        ### HITS algorithm applied to the same network as PageRank
        registerDoParallel(cores=4)
        AUTH = foreach(w = 1:length(weights_dist), .combine = cbind, .packages = c('igraph')) %dopar% {
          auth = authority_score(pr_net,weights=e_attr[[weights_dist[[w]]]][w_Edge])$vector[pr_a_id]
          hubs = hub_score(pr_net,weights=e_attr[[weights_dist[[w]]]][w_Edge])$vector[pr_a_id]
          cbind(auth,hubs)
        }
        ## Aggregate HITS scores first on PartyID level
        ## From AccountID level to PartyID level we assign max score
        colnames(AUTH) = paste0(colnames(AUTH), c(rbind(weights_dist,weights_dist)))
        AUTH = as.data.table(AUTH, keep.rownames = T)
        AUTH = merge(AUTH,tmp[,c('PartyID','AccountID'), with=F],by.x = 'rn',by.y='AccountID')
        AUTH = AUTH[,lapply(.SD,max),by="PartyID",.SDcols=sapply(AUTH, is.numeric)]
        AUTH = merge(AUTH,partyID[,c('PartyID','Implicated','CaseID'),with=F],by='PartyID', all.x=T)
        ## Then on CaseID level
        ## From PartyID level to CaseID level we take weighted(by Implicated) average, 
        ## with double the weight on Implicated
        AUTH[,c('PartyID', 'weight'):= list(NULL, ifelse(Implicated=='Y',1,0.5))]
        AUTH=AUTH[,lapply(.SD,weighted.mean,w=weight),by=CaseID, .SDcols=sapply(AUTH, is.numeric)]
        ncols = ncol(AUTH)
        AUTH = AUTH[,-c(ncols-1,ncols),with=F]
      } else{## i.e. CaseID that connects only to 'Unknown' node
        pr_l = data.table(matrix(NA,nrow=length(caseID),ncol=43))
        colnames(pr_l) = c('CaseID',paste0('PageRank',pr_comb[,1], pr_comb[,2]))
        pr_l[,CaseID := caseID]
        AUTH = data.table(matrix(NA,nrow=length(caseID),ncol=35))
        colnames(AUTH) = c('CaseID',paste0(rep(c('auth','hubs'),17),c(rbind(weights_dist,weights_dist))))
        AUTH[,CaseID := caseID]
      }
      ##########################################################
      ## Make sure that all CaseIDs in current CaseIDs are in pr_l (i.e. even those who are not in
      ## PageRank network)
      w_Case = caseID[!caseID%in%pr_l$CaseID]
      if(length(w_Case)){
        add_pr_l = data.table(matrix(c(w_Case,rep(NaN,length(w_Case)*(ncol(pr_l)-1))),nrow=length(w_Case),ncol=ncol(pr_l)))
        add_auth = data.table(matrix(c(w_Case,rep(NaN,length(w_Case)*(ncol(AUTH)-1))),nrow=length(w_Case),ncol=ncol(AUTH)))
        colnames(add_pr_l) = colnames(pr_l)
        colnames(add_auth) = colnames(AUTH)
        pr_l = rbind(pr_l,add_pr_l)
        AUTH = rbind(AUTH,add_auth)
      }
      #### 2.) Check shortest-paths to fraudulent PartyIDs
      ## Remove hubs, i.e. nodes with degree >=quantile(0.999)
      w_EdgeKnownNoHubs = w_EdgeKnown & !(e_tmpA[,1] %in% w_hubs | e_tmpA[,2] %in% w_hubs)
      known_netNoHubs = delete.vertices(known_net, v=w_hubs)
      w_fPID = vertex_attr(known_net)[['name']][vert_attr[['fraud']][-1] != 0 & vert_attr[['type']][-1]]
      ## Parallelize
      registerDoParallel(cores=4)
      dist = foreach(di=1:length(weights_dist), .combine = cbind, .packages = c('igraph')) %dopar% {
        if(di == 1){
          d_Hubs = distances(known_net,v=partyID$PartyID[w_P],to=w_fPID,mode='all',weights = e_attr[[weights_dist[di]]][w_EdgeKnown])
          d_NoHubs = distances(known_netNoHubs,v=partyID$PartyID[w_P],to=w_fPID,mode='all',weights = e_attr[[weights_dist[di]]][w_EdgeKnownNoHubs])
          d_Hubs=data.table(PartyID=rownames(d_Hubs) ,dist=apply(d_Hubs,1,function(x){mean(x[!x%in%c(Inf)])}))
          d_NoHubs=data.table(PartyID=rownames(d_NoHubs) ,distNoHubs=apply(d_NoHubs,1,function(x){mean(x[!x%in%c(Inf)])}))
          d_Hubs[,c('PartyID','CaseID','weight'):=list(NULL,partyID$CaseID[w_P],ifelse(partyID$Implicated[w_P]=='Y',1,0.5))]
          d_NoHubs[,c('PartyID','CaseID','weight'):=list(NULL,partyID$CaseID[w_P],ifelse(partyID$Implicated[w_P]=='Y',1,0.5))]
          d_Hubs = d_Hubs[,lapply(.SD,weighted.mean,w=weight,na.rm=T),by=CaseID, .SDcols=c('dist','weight')]
          d_NoHubs = d_NoHubs[,lapply(.SD,weighted.mean,w=weight,na.rm=T),by=CaseID, .SDcols=c('distNoHubs','weight')]
          cbind(d_Hubs[,-3,with=F],d_NoHubs[,2,with=F])
        }else{
          d_Hubs = distances(known_net,v=partyID$PartyID[w_P],to=w_fPID,mode='all',weights = 1/e_attr[[weights_dist[di]]][w_EdgeKnown])
          d_NoHubs = distances(known_netNoHubs,v=partyID$PartyID[w_P],to=w_fPID,mode='all',weights = 1/e_attr[[weights_dist[di]]][w_EdgeKnownNoHubs])
          d_Hubs=data.table(PartyID=rownames(d_Hubs) ,dist=apply(d_Hubs,1,function(x){mean(x[!x%in%c(Inf)])}))
          d_NoHubs=data.table(PartyID=rownames(d_NoHubs) ,distNoHubs=apply(d_NoHubs,1,function(x){mean(x[!x%in%c(Inf)])}))
          d_Hubs[,c('PartyID','CaseID','weight'):=list(NULL,partyID$CaseID[w_P],ifelse(partyID$Implicated[w_P]=='Y',1,0.5))]
          d_NoHubs[,c('PartyID','CaseID','weight'):=list(NULL,partyID$CaseID[w_P],ifelse(partyID$Implicated[w_P]=='Y',1,0.5))]
          d_Hubs = d_Hubs[,lapply(.SD,weighted.mean,w=weight,na.rm=T),by=CaseID, .SDcols=c('dist','weight')]
          d_NoHubs = d_NoHubs[,lapply(.SD,weighted.mean,w=weight,na.rm=T),by=CaseID, .SDcols=c('distNoHubs','weight')]
          cbind(d_Hubs[,2,with=F],d_NoHubs[,2,with=F])
        }
      }
      names(dist)[seq(2,ncol(dist),by=2)] = paste0('dist',weights_dist)
      names(dist)[seq(3,ncol(dist),by=2)] = paste0('distNoHubs',weights_dist)
      ################################################
      
      ### CaseID egonet features
      ego2 = make_ego_graph(tmpA_network,order=2, nodes = partyID$PartyID[w_P])
      ## Assign CaseID to each ego
      names(ego2) = partyID$CaseID[w_P]
      caseEgo2 = list()
      t = sort(table(names(ego2)))
      names(ego2) = make.unique(names(ego2))
      ## Here we create actual CaseID egonet
      for(c in names(t)){
        if(t[c] == 1){caseEgo2[[c]] = ego2[[c]]} ## one PartyID per CaseID
        else{
          ind_e = substr(names(ego2),1,nchar(c))%in%c
          vert = unique(unlist(lapply(ego2[ind_e], function(x){V(x)$name})))
          caseEgo2[[c]] = induced.subgraph(tmpA_network,vids = vert)
        }
      }
      ego2 = caseEgo2
      ## If CaseID egonet is empty(i.e. manual case or no transactions at all) remove it!
      ## Create several types of CaseID egonet {without Unknown node, without PartyID nodes, both,
      ## without hubs...}
      ## Make sure that 'Unknown' actually exists in Egonet
      ego2_Known = lapply(ego2, function(x){
        if('Unknown'%in%vertex_attr(x)[['name']]){
          delete.vertices(x,v='Unknown')
        }
        else{x}
      })
      
      ### This is necessary to be sure that edges correspond to their attributes in e_attr list!!
      ego2_wo = lapply(ego2, function(x){delete.vertices(x,substr(vertex_attr(x)[['name']],1,1) != 'A' 
                                                         & vertex_attr(x)[['name']]!='Unknown')})
      e_List_wo = lapply(ego2_wo, get.edgelist)
      w_Edge_wo = lapply(e_List_wo, function(x){which(e_tmpA[,1] %in% x[,1] & e_tmpA[,2] %in% x[,2])}) 
      ego2_wo = lapply(w_Edge_wo, function(x){graph_from_data_frame(matrix(c(t(e_tmpA[x,])),byrow=T,ncol=2))})
      nonempty_ego = unlist(lapply(ego2_wo, ecount)) != 0
      ###
      ego2_woKnown = lapply(ego2_Known, function(x){delete.vertices(x,substr(vertex_attr(x)[['name']],1,1) 
                                                                    != 'A')})
      e_List_woKnown = lapply(ego2_woKnown, get.edgelist)
      w_Edge_woKnown = lapply(e_List_woKnown, function(x){e_tmpA[,1] %in% x[,1] & e_tmpA[,2] %in% x[,2]}) 
      ego2_woKnown = lapply(w_Edge_woKnown, function(x){graph_from_data_frame(matrix(c(t(e_tmpA[x,])),byrow=T,ncol=2))})
      nonempty_ego2 = unlist(lapply(ego2_woKnown, ecount)) != 0
      ###
      ego2_woKnownNoHubs = lapply(ego2_woKnown, function(x){delete.vertices(x,
                                                                  vertex_attr(x)[['name']]%in%w_hubs)})
      e_List_woKnownNoHubs = lapply(ego2_woKnownNoHubs, get.edgelist)
      w_Edge_woKnownNoHubs = lapply(e_List_woKnownNoHubs, function(x){e_tmpA[,1] %in% x[,1] & e_tmpA[,2] %in% x[,2]}) 
      ego2_woKnownNoHubs = lapply(w_Edge_woKnownNoHubs, function(x){graph_from_data_frame(matrix(c(t(e_tmpA[x,])),byrow=T,ncol=2))})
      nonempty_ego3 = unlist(lapply(ego2_woKnownNoHubs, ecount)) != 0
      ### We extract features for different periods [0,t], [t-30,t], [t-90,t], [t-183,t], [t-365,t]
      ## And some of the features are calculated per TransTypeID
      RFM=matrix(0,nrow=length(ego2),ncol=191)
      
      ### Not much sense to parallelize here, since for most of the Egonets, this is fast. Only exception
      ### is Egonet to PartyIDs with many hub-accounts, but this does not occur often in CaseIDs
      ### The overhead costs might prolong the computation time compared to sequential looping
      for(i in 1:length(ego2)){
        if(!nonempty_ego[i]){RFM[i,] = rep(0,191)} ## no transactions => proportions are 0
        else{
          diff = as.numeric(dates[d]-e_attr[['PostingDate']][w_Edge_wo[[i]]])
          TransTypeID = e_attr[['TransTypeID']][w_Edge_wo[[i]]]
          TimeWindow = data.frame(ST = diff <= 30, MT = diff <= 90, MLT = diff <= 183, LT = diff <= 365,
                                  FULL = T)
          skew = iqr = propCurrWin = propCurrTrTypeID = MeanWin = MeanCurrWin = MeanTrTypeID =
          MedianWin = MedianCurrWin = MedianTrTypeID = 0
          propUnkWin = propUnkTrTypeID = AmountpropUnkWin = AmountpropCurrTrTypeID = AmountpropUnkTrTypeID = 0
          # 
          propTrTypeID = AmountpropTrTypeID = matrix(0,ncol=8,nrow=5)
          # 
          recent = diff<=7
          propRecentWin = propRecentTrTypeID = 0
          e_List = get.edgelist(ego2_wo[[i]])
          w_Unknown = e_List[,1] == 'Unknown' | e_List[,2] == 'Unknown'
          # 
          amount = e_attr[['Amount']][w_Edge_wo[[i]]]
          ## Take care of 0 amount
          amount[amount == 0] = 0.01
          n = apply(TimeWindow,2,sum)
          w_Foreign = e_attr[['Currency']][w_Edge_wo[[i]]] != 'NOK'
          ## Find 95% quantile transaction
          q_95 = quantile(amount,0.95)
          HighMonetary = any(amount[recent] >= q_95)
          if(!nonempty_ego2[i]){
            cliq = 0; trans = 0
          }
          else{
            cliq = length(suppressWarnings(max_cliques(ego2_woKnown[[i]],min=3)))/vcount(ego2_woKnown[[i]])
            trans = transitivity(ego2_woKnown[[i]], isolates = 'zero')
          }
          if(!nonempty_ego3[i]){
            cliqNoHubs = 0; transNoHubs = 0
          }
          else{
            cliqNoHubs = length(suppressWarnings(max_cliques(ego2_woKnownNoHubs[[i]],min=3)))/vcount(ego2_woKnownNoHubs[[i]])
            transNoHubs = transitivity(ego2_woKnownNoHubs[[i]], isolates = 'zero')
          }
          
          for(p in 1:5){# Time window
            if(n[p] == 0){
              skew[p] = iqr[p] = propCurrWin[p] = propUnkWin[p] = AmountpropUnkWin[p] = propRecentWin[p] =
                MeanWin[p] = MeanCurrWin[p] = MedianWin[p] = MedianCurrWin[p] = 0
            }
            else {
              skew[p] = skewness(amount[TimeWindow[,p]])
              iqr[p] = IQR(amount[TimeWindow[,p]])
              MeanWin[p] = mean(amount[TimeWindow[,p]])
              MedianWin[p] = quantile(amount[TimeWindow[,p]],0.5)
              MeanCurrWin[p] = mean(amount[w_Foreign & TimeWindow[,p]])
              MedianCurrWin[p] = quantile(amount[w_Foreign & TimeWindow[,p]],0.5)
              propCurrWin[p] = sum(w_Foreign & TimeWindow[,p])/n[p]
              propUnkWin[p] = sum(w_Unknown & TimeWindow[,p])/n[p]
              AmountpropUnkWin[p] = sum(amount[w_Unknown & TimeWindow[,p]])/sum(amount[TimeWindow[,p]])
              propRecentWin[p] = sum(recent)/n[p]
              if(sum(w_Foreign & TimeWindow[,p]) == 0){
                MeanCurrWin[p] = MedianCurrWin[p] = 0
              }
            }
            if(n[p] == 1){skew[p] = 0}
          }
          for(t in 1:8){# TransTypeID
            tmp_id = (TransTypeID == uniqueTransTypeID[t])
            den = sum(tmp_id)
            if(den == 0){
              propCurrTrTypeID[t] = 0
              propUnkTrTypeID[t] = 0
              MeanTrTypeID[t] = 0
              MedianTrTypeID[t] = 0
              AmountpropCurrTrTypeID[t] = 0
              AmountpropUnkTrTypeID[t] = 0
              propRecentTrTypeID[t] = 0
            }
            else{
              propCurrTrTypeID[t] = sum(w_Foreign & tmp_id)/den
              propUnkTrTypeID[t] = sum(w_Unknown & tmp_id)/den
              MeanTrTypeID[t] = mean(amount[tmp_id])
              MedianTrTypeID[t] = quantile(amount[tmp_id],0.5)
              AmountpropCurrTrTypeID[t] = sum(amount[w_Foreign & tmp_id])/sum(amount[tmp_id])
              AmountpropUnkTrTypeID[t] = sum(amount[w_Unknown & tmp_id])/sum(amount[tmp_id])
              propRecentTrTypeID[t] = sum(recent & tmp_id)/den
            }
            for(p in 1:5){# Time window
              tmp_id = (TransTypeID == uniqueTransTypeID[t] & TimeWindow[,p])
              den = sum(tmp_id)
              if(n[p] == 0){
                propTrTypeID[p,t] = AmountpropTrTypeID[p,t] = 0
              }
              else {
                propTrTypeID[p,t] = den/n[p]
                AmountpropTrTypeID[p,t] = sum(amount[tmp_id])/sum(amount[TimeWindow[,p]])
              }
            }  
          }
          RFM[i,] = c(skew, iqr, MeanWin, MedianWin, MeanCurrWin, MedianCurrWin, propCurrWin,
                          propUnkWin, AmountpropUnkWin, propRecentWin, propCurrTrTypeID, propUnkTrTypeID,
                          MeanTrTypeID, MedianTrTypeID, AmountpropCurrTrTypeID, AmountpropUnkTrTypeID,
                          propRecentTrTypeID, HighMonetary, cliq, trans, cliqNoHubs, transNoHubs,
                          c(propTrTypeID), c(AmountpropTrTypeID))
        }
      }
      # 
      Deg_centr = unlist(lapply(ego2_wo, function(x){
          if(vcount(x) < 3){0}
          else{
            centr_degree(simplify(x),mode=c('all'),loops = F)$centralization
          }
        }
      ))
      Deg_centrIN = unlist(lapply(ego2_wo, function(x){
          if(vcount(x) < 2){0}
          else{
            centr_degree(simplify(x),mode=c('in'),loops = F)$centralization
          }
        }
      ))
      Deg_centrOUT = unlist(lapply(ego2_wo, function(x){
          if(vcount(x) < 2){0}
          else{
            centr_degree(simplify(x),mode=c('out'),loops = F)$centralization
          }
        }
      ))
      
      ## Closeness and betweenness centralities are not feasible to calculate for huge Egonets
      ## We calculate it over Egonets without 'Unknown' and Hub nodes
      ## Here it makes sense, since considering shortest paths (or distances) through Hubs/'Uknown' nodes 
      ## might creates suspicious connections in such a Egonet
      Bet_centr = unlist(lapply(ego2_woKnownNoHubs, function(x){
          if(vcount(x) < 3){0}
          else{
            centr_betw(simplify(x),directed=T)$centralization
          }
        }
      ))
      Bet_centrUnDir = unlist(lapply(ego2_woKnownNoHubs, function(x){
          if(vcount(x) < 3){0}
          else{
            centr_betw(simplify(x),directed=F)$centralization
          }
        }
      ))
      Clo_centr = unlist(lapply(ego2_woKnownNoHubs, function(x){
          if(vcount(x) < 3){0}
          else{
            centr_clo(simplify(x),mode='all')$centralization
          }
        }
      ))
      Clo_centrIN = unlist(lapply(ego2_woKnownNoHubs, function(x){
          if(vcount(x) < 3){0}
          else{
            centr_clo(simplify(x),mode='in')$centralization
          }
        }
      ))
      Clo_centrOUT = unlist(lapply(ego2_woKnownNoHubs, function(x){
          if(vcount(x) < 3){0}
          else{
            centr_clo(simplify(x),mode='out')$centralization
          }
        }
      ))
      
      Dens = unlist(lapply(ego2_wo, function(x){
          if(vcount(x) < 2){0}
          else{
            edge_density(x)
          }
        }
      ))
      
      PropFraud = unlist(lapply(ego2_wo, function(x){
        if(vcount(x) == 0){0}
        else{
          w_Vert_wo = names(vert_attr[['fraud']]) %in% vertex_attr(x)[['name']]
          sum(vert_attr[['fraud']][w_Vert_wo] != 0)/vcount(x)
        }
      }
      ))
      
      TimeWindow = c('ST','MT','MLT','LT','')
      colnames(RFM) = c(paste0('SkewWin',TimeWindow),paste0('IqrWin',TimeWindow),
                        paste0('MeanWin',TimeWindow),paste0('MedianWin',TimeWindow),
                        paste0('MeanCurrWin',TimeWindow),paste0('MedianCurrWin',TimeWindow),
                        paste0('PropCurrWin',TimeWindow),paste0('PropUnkWin',TimeWindow),
                        paste0('AmountPropUnkWin',TimeWindow),paste0('PropRecentWin',TimeWindow),
                        paste0('PropCurrTrType',uniqueTransTypeID),paste0('PropUnkTrType',uniqueTransTypeID),
                        paste0('MeanTrType',uniqueTransTypeID),paste0('MedianTrType',uniqueTransTypeID),
                        paste0('AmountPropCurrTrType',uniqueTransTypeID),
                        paste0('AmountPropUnkTrType',uniqueTransTypeID),
                        paste0('PropRecentTrType',uniqueTransTypeID),'HighMonetary',
                        'PropCliq', 'Transitivity', 'PropCliqNoHubs', 'TransitivityNoHubs',
                        paste0('prop',rep(uniqueTransTypeID,each=length(TimeWindow)),
                               rep(TimeWindow,length(uniqueTransTypeID))),
                        paste0('Amountprop',rep(uniqueTransTypeID,each=length(TimeWindow)),
                               rep(TimeWindow,length(uniqueTransTypeID))))
      
      RFM=cbind(RFM,Deg_centr,Deg_centrIN,Deg_centrOUT,Bet_centr,Bet_centrUnDir,Clo_centr,Clo_centrIN,
                Clo_centrOUT,Dens,PropFraud)
      RFM = data.table(RFM)
      RFM[,CaseID := as.numeric(names(ego2))]
      ####
      ## If missing in RFM
      w_Case = caseID[!caseID%in%RFM$CaseID]
      if(length(w_Case)){
        add_RFM = data.table(matrix(c(rep(NaN,length(w_Case)*(ncol(RFM)-1)),w_Case),nrow=length(w_Case),ncol=ncol(RFM)))
        colnames(add_RFM) = colnames(RFM)
        RFM = rbind(RFM,add_RFM)
      }
      ### Check whether all attribute data.tables have the same number of rows, i.e. == length(caseID)
      print('Number of missing inputs')
      print('PageRank'); print(cbind(pr_l$CaseID,apply(pr_l,1,function(x){sum(is.na(x))})))
      print('AUTH'); print(cbind(AUTH$CaseID,apply(AUTH,1,function(x){sum(is.na(x))})))
      print('dist'); print(cbind(dist$CaseID,apply(dist,1,function(x){sum(is.na(x))})))
      print('RFM'); print(cbind(RFM$CaseID,apply(RFM,1,function(x){sum(is.na(x))})))
      
      if(nrow(pr_l)!= length(caseID)){stop('Stopping because of error in PageRank!')}
      if(nrow(AUTH)!= length(caseID)){stop('Stopping because of error in AUTH!')}
      if(nrow(dist)!= length(caseID)){stop('Stopping because of error in dist!')}
      if(nrow(RFM)!= length(caseID)){stop('Stopping because of error in RFM!')}
      Features_tmp = merge(pr_l,AUTH,by='CaseID')
      Features_tmp = merge(Features_tmp,dist,by='CaseID')
      Features_tmp = merge(Features_tmp,RFM,by='CaseID')
      Features = rbind(Features,Features_tmp)
      ################################################
      ## After feature extraction, insert new fraud to the network
      ## Set fraudDate
      vert_attr[['fraudDate']][ind_P] = dates[d]
      vert_attr[['fraudDate']][ind] = dates[d]
      ## Investigated
      vert_attr[['investigated']][ind_P] = vert_attr[['investigated']][ind_P]+1
      vert_attr[['investigated']][ind] = vert_attr[['investigated']][ind]+1
      ## Fraud
      insert=partyID[,c('CaseStatus'):= list(abs(as.numeric(partyID$CaseStatus)-2)*ifelse(Implicated=='Y',1,0.5)),with=F][, .SD, .SDcols = c('PartyID','CaseStatus')]
      insert=insert[,lapply(.SD,mean),by=PartyID, .SDcols=sapply(insert, is.numeric)]
      w_P = which(insert$PartyID%in%vertex_attr(tmpA_network, 'name'))
      vert_attr[['fraud']][ind_P][ord_P] = insert[w_P,CaseStatus]
      insert = tmp[,c('CaseStatus'):= list(abs(as.numeric(CaseStatus)-2)*ifelse(Implicated=='Y',1,0.5)),with=F][, .SD, .SDcols = c('AccountID','CaseStatus')]
      insert=insert[,lapply(.SD,mean),by=AccountID, .SDcols=sapply(insert, is.numeric)]
      print(length(ind)); print(length(insert$CaseStatus))
      if(length(ind)!= length(insert$CaseStatus)){stop('Error!')}
      vert_attr[['fraud']][ind][ord] = insert$CaseStatus
      ### Take care of P_ID and A_ID nodes that must be changed(doChange) due to previously being in network
      ## if(any(doChange))
      if(any(doChange)){
        ## Linear weights
        w1 = ifelse(fraud_DiffPID<730,-fraud_DiffPID*(1/730)+1,exp(-7/730*fraud_DiffPID))
        w2 = ifelse(fraud_DiffAID<730,-fraud_DiffAID*(1/730)+1,exp(-7/730*fraud_DiffAID))
        vert_attr[['fraud']][w_investigatedPID] = (originalFraudPID*w1 + vert_attr[['fraud']][w_investigatedPID])/(w1+1)
        vert_attr[['fraud']][w_investigatedAID] = (originalFraudAID*w2 + vert_attr[['fraud']][w_investigatedAID])/(w2+1)
      }
      ##################################  
      ## In every 10th iteration save objects
      if(d%%10 == 0 | d == l){
        print(paste('Iteration nr: ',d))
        print('Saving objects: vert_attr,e_attr,tmpA_network,Features,d,time')
        save(list=c('vert_attr','e_attr','tmpA_network','Features','d','time'),
             file=paste0('Features_',as.Date(Sys.time()),'.Rdata'))
      }
    }
  }
  time[d+1]=Sys.time()
  #return(list(vert_attr=vert_attr,e_attr=e_attr,net=tmpA_network,time=time,Features=Features))
}
