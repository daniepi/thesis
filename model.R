## Model fitting
library("data.table", lib.loc="/usr/local/lib/R/site-library")
library("caret", lib.loc="/usr/local/lib/R/site-library")
library("C50", lib.loc="/usr/local/lib/R/site-library")
library("randomForest", lib.loc="/usr/local/lib/R/site-library")
library("gbm", lib.loc="/usr/local/lib/R/site-library")
library("doParallel", lib.loc="/usr/local/lib/R/site-library")
library("mice", lib.loc="/usr/local/lib/R/site-library")
library("ROCR", lib.loc="/usr/local/lib/R/site-library")
library("Matrix", lib.loc="/usr/local/lib/R/library")
library("glmnet", lib.loc="/usr/local/lib/R/site-library")

setwd("/nr/project/stat/BigInsight/Projects/Fraud/Subprojects/DNB-AML-Master")
## Load features
#load("/nr/project/stat/BigInsight/Projects/Fraud/Subprojects/DNB-AML-Master/Features_2017-04-19.Rdata")
load("RFM_Trans.Rdata")
#load("/nr/project/stat/BigInsight/Projects/Fraud/Subprojects/DNB-AML-Master/Features_2017-04-20.Rdata")
load("/nr/project/stat/BigInsight/Projects/Fraud/Subprojects/DNB-AML-Master/Features_2017-05-08.Rdata")
#load("/nr/project/stat/BigInsight/Projects/Fraud/Subprojects/DNB-AML-Master/Features_Dir_2017-05-07.Rdata")

## Load data
load("/nr/project/stat/BigInsight/Projects/Fraud/Subprojects/DNB-AML-Master/temp.RData")
#load("/nr/project/stat/BigInsight/Projects/Fraud/Subprojects/DNB-AML-Master/models.Rdata")



## Subset to only CaseIDs for which we extract network features
datAnonymizedVar = datAnonymizedVar[!CaseID%in%nonTransCaseID$CaseID & CaseID %in% Features$CaseID]
datFullNameVar = datFullNameVar[!CaseID%in%nonTransCaseID$CaseID & CaseID %in% Features$CaseID]
## We used all CaseID with creationDate <= "2015-01-26" as a burn-in for fraud insertion. We are then left
## with 1314 instances (i.e. CaseIDs) to train and test the model.

## Replace missing values in factor variables for third level
datAnonymizedVar$Var_37_factor = addNA(datAnonymizedVar$Var_37_factor)
levels(datAnonymizedVar$Var_37_factor) = c("Level_1","Level_2","Unknown")
datAnonymizedVar$Var_39_factor = addNA(datAnonymizedVar$Var_39_factor)
levels(datAnonymizedVar$Var_39_factor) = c("Level_1","Level_2","Unknown")
datAnonymizedVar$Var_40_factor = addNA(datAnonymizedVar$Var_40_factor)
levels(datAnonymizedVar$Var_40_factor) = c("Level_1","Level_2","Unknown")

## Missing values in Var_6_Numeric - Var_23_Numeric + Var_38_Numeric
## Try pmm imputation for numeric data, using the rest of the data set as predictors
predMatrix = (1 - diag(1, ncol(datAnonymizedVar)))
predMatrix[c(6:23,38,43),c(6:23,38,43)] = 0
imp1 = mice(datAnonymizedVar,m=1,method = 'pmm',seed=1, predictorMatrix = predMatrix, maxit = 20)
datAnonymizedVarCompl = data.table(complete(imp1,1))

## Var_8/21_Numeric contains only 0 and NAs. Thus imputation makes no sense and we drop this variable.
datAnonymizedVarCompl[,Var_21_numeric := NULL]
datAnonymizedVarCompl[,Var_8_numeric := NULL]
## Check for regressor wit low variance
nearZeroVar(datAnonymizedVarCompl, saveMetrics= TRUE)



### Create response
Y = datFullNameVar$CaseStatus
levels(Y) = c("Fraud",'Legitimate')


## Function that takes type of the imput data as string, also type of the RFM data to use as a string
## In addition one specifies the model, the metric over which one optimize
fit_time = function(data_type="BASE", rfm_type = 'CASE', model='C5.0', metric='ROC', scale = F, save.plot = F,
                    SMOTE = F, cv.seed = 8, corr_cutoff = 0){
  
  if(corr_cutoff < 0 | corr_cutoff > 1){stop('Correlation cut-off must be in [0,1]!')}
  
  ## data_type: specifies what kind of input data is used
  ## 1.) 'BASE' uses only base covariates
  ## 2.) 'BASE_RFM' uses both BASE and RFM covariates
  ## 3.) 'BASE_NET' uses both BASE and NET covariates
  ## 4.) 'BASE_RFM_NET' uses both BASE, RFM and NET covariates
  ############################################################
  ## rfm_type: specifies what kind of rfm data is used
  ## 1.) 'CASE' summarizes case-centric egonet 
  ## 2.) 'PARTY' summarizes involved parties
  ## 3.) 'IMPLICATED' summarizes implicated party only
  ## NOTE THAT if grepl('RFM', data_type) = FALSE => rfm_type has no effect
  ############################################################
  ## model: specifies ML algorithm used to fit the data
  ## metric: specifies metric which is about to be optimized
  ## scale: boolean, whether to scale the input matrix (always TRUE for `net)
  ## save.plot: boolean, whether to save resulting ROC plots as .pdf
  ## SMOTE: boolean, whether to perform SMOTE before model fitting
  ## cv.seed: Set seed for reproducible results (i.e. seed for CV-folds)
  ## corr_cutoff: Cutoff value for removing highly correlated RFM/Network features
  ## If corr_cutoff = 0 => full data.
  
  data_type = toupper(data_type)
  rfm_type = toupper(rfm_type)
  ind_net = grepl('NET',data_type)
  ind_rfm = grepl('RFM',data_type)
  
  if(ind_net){
    w_NET = grepl('PageRank', colnames(Features)) | grepl('auth', colnames(Features)) | grepl('hubs', colnames(Features)) |
      grepl('dist', colnames(Features)) | grepl('centr', colnames(Features)) | grepl('Cliq', colnames(Features)) |
      grepl('Trans', colnames(Features))  | grepl('Dens', colnames(Features)) | grepl('Fraud', colnames(Features))
    w_NET[1] = T
    NET = Features[,w_NET,with=F]
    mean_repl = apply(NET,2,function(x){mean(x,na.rm = T)})
    max_repl = apply(NET,2,function(x){max(x,na.rm = T)})
    ## NB! 
    ## Missing PageRank and HITS scores are replaces by mean value. These are missing because when extracting
    ## we do not aggregate for HUBS!!!!
    ## Missing distances means that case is disconnected from the rest of the network => we set it to max(dist)
    ## Missing in other network variables (centralities, cliques, transitivity..) => we set to 0, since these
    ## are missing because of anomalous graphs (i.e. no transactions, less than 3 nodes ....)
    for (i in seq_along(NET)){ 
      if(grepl('PageRank',colnames(NET)[i]) | grepl('auth',colnames(NET)[i]) |
         grepl('hubs',colnames(NET)[i])){
        val = mean_repl[i]
      }
      else if(grepl('dist',colnames(NET)[i])){val = max_repl[i]}
      else{val = 0}
      set(NET, i=which(is.na(NET[[i]])), j=i, value=val)
    }
    
    ## Check for regressor wit zero variance
    nzv = nearZeroVar(NET,saveMetrics = T)
    nzv = rownames(nzv)[nzv$zeroVar==F]
    NET = NET[,nzv,with=F]
    
    if(corr_cutoff > 0){
      ## Find columns with infinite variance (i.e. huge number)
      ## This happens only for glmnet, where we do not log transform distances
      w_InfVar = which(apply(NET, 2, var) == Inf)
      if(length(w_InfVar)){
        tmp_NET = copy(NET)
        ## Sweep mean from these columns to be able to find correlation
        for(c in w_InfVar){
          tmp_NET[[c]] = tmp_NET[[c]]/mean(tmp_NET[[c]])
        }
      }
      descrCor <-  cor(tmp_NET,use='complete.obs')
      highlyCorDescr <- findCorrelation(descrCor, cutoff = corr_cutoff, names=T)
      NET <- NET[,-highlyCorDescr,with=F]
    }
    
    ## Distance features are numerically too big for some algorithms (C5.0) to handle them.
    ## We therefore log transform them before model training
    if(model!='glmnet'){
      w_Dist = which(grepl('dist',colnames(NET)))
      for(c in w_Dist){
        NET[[c]] = log(NET[[c]])
      }
    }
  }
  
  
  if(data_type=='BASE'){
    dat = copy(datAnonymizedVarCompl)
    dat[,CaseID := NULL]
  }
  else if(data_type%in%c('BASE_RFM','BASE_RFM_NET')){
    if(rfm_type == 'CASE'){
      RFM = RFM_case[CaseID %in% datAnonymizedVarCompl$CaseID]
    }
    else if(rfm_type == 'PARTY'){
      RFM = RFM_party[CaseID %in% datAnonymizedVarCompl$CaseID]
    }
    else if(rfm_type == 'IMPLICATED'){
      RFM = RFM_implicated[CaseID %in% datAnonymizedVarCompl$CaseID]
    }
    else{
      stop('Must specify a valid rfm data!')
    }
    ### For cases with no transactions (i.e. either whole case is manual or implicated party does not have
    ### transactions or implicated party is 'None') we set RFM variables = 0, since these are proportions and 
    ### mean/median
    for (j in seq_len(ncol(RFM)))
      set(RFM,which(is.na(RFM[[j]])),j,0)
    ## Check for regressor wit zero variance
    nzv = nearZeroVar(RFM,saveMetrics = T)
    nzv = rownames(nzv)[nzv$zeroVar==F]
    RFM = RFM[,nzv,with=F]
    
    ## Find pairwise high correlations and remove variables
    if(corr_cutoff > 0){
      descrCor <-  cor(RFM,use='complete.obs')
      highlyCorDescr <- findCorrelation(descrCor, cutoff = corr_cutoff, names=T)
      RFM <- RFM[,-highlyCorDescr,with=F]
    }
    
    dat = merge(datAnonymizedVarCompl, RFM, by='CaseID')
    ## If HighMonetary in dat => make it factor
    if(!is.null(dat$HighMonetary)){
      dat[, HighMonetary := factor(HighMonetary)]
    }
    
    if(ind_net){
      dat = merge(dat, NET, by = 'CaseID')
    }
    
    dat[,CaseID := NULL]
  }
  else if(data_type=='BASE_NET'){
    dat = merge(datAnonymizedVarCompl, NET, by='CaseID')
    dat[,CaseID := NULL]
  } else{stop('Must specify a valid input data!')}
  
  if(SMOTE){library("DMwR", lib.loc="/usr/local/lib/R/site-library")}
  VarImp = list()
  
  ## Create date sequence to iterate over
  iter=seq(as.Date("2015-04-01"), length=9, by="1 month")-1
  len_iter = length(iter)
  
  ## Initialize objects
  AUC = Brier = rep(0, len_iter)
  
  pred = list()
  pred_vec = 0
  
  models = list()
  
  if(model=='C5.0'){
    paramGrid = expand.grid( .winnow = c(TRUE, FALSE), .trials=c(5,10,20,50,100),
                             .model=c("tree","rules"), .minCases = c(2,3,5,10))
    set.seed(cv.seed)
    
    ## length is = number of folds in k-folds cv + 1 for full sample
    seeds <- vector(mode = "list", length = 11)
    
    ## Create seeds for each resampling iteration, i.e. for each of the folds, create vector of length
    ## equal to the number unique combinations in parameter grid.
    for(i in 1:10) seeds[[i]]<- sample.int(n=1000, nrow(paramGrid))
    
    ##for the last model (i.e. full training set)
    seeds[[11]]<-sample.int(1000, 1)
  }
  else if(model=='glmnet'){
    ## Need to scale features before model fitting
    scale = T
    paramGrid = expand.grid( .alpha = seq(0,1,0.1), .lambda = c(seq(0,1,0.1),2:10))
    set.seed(cv.seed)
    
    ## length is = number of folds in k-folds cv + 1 for full sample
    seeds <- vector(mode = "list", length = 11)
    
    ## Create seeds for each resampling iteration, i.e. for each of the folds, create vector of length
    ## equal to the number unique combinations in parameter grid.
    for(i in 1:10) seeds[[i]]<- sample.int(n=1000, nrow(paramGrid))
    
    ##for the last model (i.e. full training set)
    seeds[[11]]<-sample.int(1000, 1)
  }
  else if(model=='rf' | model == 'parRF'){
    m_seq = seq(floor(sqrt(ncol(dat)))-9,length.out = 7,by=3)
    m_seq = m_seq[m_seq>0]
    paramGrid = expand.grid( .mtry = m_seq, 
                             .ntree = c(100,500,1000,2000),.nodesize=c(1,5,10))
    set.seed(cv.seed)
    
    ## length is = number of folds in k-folds cv + 1 for full sample
    seeds <- vector(mode = "list", length = 11)
    
    ## Create seeds for each resampling iteration, i.e. for each of the folds, create vector of length
    ## equal to the number unique combinations in parameter grid.
    for(i in 1:10) seeds[[i]]<- sample.int(n=1000, nrow(paramGrid))
    
    ##for the last model (i.e. full training set)
    seeds[[11]]<-sample.int(1000, 1)
  }
  else if(model=='gbm'){
    paramGrid = expand.grid( .n.trees = seq(50,250,by=50), .interaction.depth = 1:3, .shrinkage = 
                                       c(0.001,0.01,0.1,0.3), .n.minobsinnode = seq(5,20,5))
    set.seed(cv.seed)
    
    ## length is = number of folds in k-folds cv + 1 for full sample
    seeds <- vector(mode = "list", length = 11)
    
    ## Create seeds for each resampling iteration, i.e. for each of the folds, create vector of length
    ## equal to the number unique combinations in parameter grid.
    for(i in 1:10) seeds[[i]]<- sample.int(n=1000, nrow(paramGrid))
    
    ##for the last model (i.e. full training set)
    seeds[[11]]<-sample.int(1000, 1)
  } else {stop('Must specify a valid model!')}
  
  if(save.plot){
    if(ind_rfm){
      name = toupper(paste0(model,"_",data_type,"_",rfm_type,'_',metric,".pdf"))
    } else{
      name = toupper(paste0(model,"_",data_type,"_",metric,".pdf"))
    }
    
    pdf(name, width = 7.5, height = 5)
  }
  for(d in seq_along(iter)){
    print(d)
    ## Train/Test split
    w_Train = datFullNameVar$creationDate <= iter[d]
    Y.train = Y[w_Train]
    Y.test = Y[!w_Train]
    X.train = dat[w_Train]
    X.test = dat[!w_Train]
    
    ## Reproducible CV, need to set.seed when splitting data into folds
    set.seed(cv.seed)
    folds = createFolds(Y.train, returnTrain = T)
    
    if(metric=='ROC'){
      Control = trainControl(method='cv', number=10, classProbs = T, summaryFunction = twoClassSummary,
                             index=folds, seeds = seeds, savePredictions = 'all', returnResamp = 'all',
                             allowParallel = T)
    } else{ Control = trainControl(method='cv', number=10, index=folds, seeds = seeds)}
  
    if(SMOTE){
      tmp_dat = data.table(cbind(X.train,Y.train))
      tmp_SMOTE = SMOTE(Y.train~.,data=tmp_dat)
      Y.train = tmp_SMOTE$Y.train
      X.train = tmp_SMOTE[-ncol(tmp_SMOTE)]
    }
    ## Remove variables with zero-variance in current training set
    nzv = nearZeroVar(X.train,saveMetrics = T)
    nzv = rownames(nzv)[nzv$zeroVar==F]
    #nzv = rownames(nzv)[nzv$nzv==F]
    X.train = X.train[,nzv,with=F]
    X.test = X.test[,nzv,with=F]
    
    if(scale){
      ## Scale inputs if scale == TRUE
      preProc = preProcess(X.train,method=c('center','scale'))
      X.train = predict(preProc, X.train)
      X.test = predict(preProc, X.test)
    }
    
    ## Glmnet does not work for factor inputs. We create dummy variables for factor variables and remove
    ## the intercept column, since it is implicitly assumed in glmnet call.
    if(model=='glmnet'){
      ## glmnet needs matrix as an input
      X.train = model.matrix(~.,X.train)[,-1]
      X.test = model.matrix(~.,X.test)[,-1]
    }
    if(d == 1){
      Y.testFull = Y.test
      n=sum(!w_Train)
      ind = 1:n
    } else{ind = (n-sum(!w_Train)+1):n}
    
    ## Register parallel backend
    registerDoParallel(cores=6)
    if(model%in%c("C5.0",'rf','parRF')){
      if(model == 'C5.0'){
        ## Custom C5.0 model tuning
        train_model = train(x=X.train,y=Y.train,method=customC50,metric=metric,tuneGrid = paramGrid, 
                            trControl = Control)
      }
      else if(model == 'rf'){
        ## Custom randomForest model tuning
        train_model = train(x=X.train,y=Y.train,method=customrf,metric=metric,tuneGrid = paramGrid, 
                            trControl = Control)
      }
      else{
        ## Custom parallel randomForest tuning
        train_model = train(x=X.train,y=Y.train,method=customparRF,metric=metric,tuneGrid = paramGrid, 
                            trControl = Control)
      }
    }
    else{
      train_model = train(x=X.train,y=Y.train,method=model,metric=metric,tuneGrid = paramGrid, 
                          trControl = Control)  
    }
    ## Save each model
    models[[as.character(iter[d])]] = train_model
    ## Fetch variable importance
    VarImp[[as.character(iter[d])]] = varImp(train_model)
    ## Fetch trained tuning parameters
    best_tune = train_model$bestTune
    ## Produce predicted probabilities
    pred_model = predict(train_model, newdata = X.test, type='prob')['Fraud']
    ## Save them
    pred[[as.character(iter[d])]] = pred_model
    ## Overwrite previously calculated probabilities when moving in time
    pred_vec[ind] = as.numeric(unlist(pred_model))
    ## Calculate AUC, Brier score and plot ROC curve
    predictions = prediction(pred_model,Y.test, label.ordering = c("Legitimate",'Fraud'))
    AUC[d] = as.numeric(performance(predictions,"auc")@y.values)
    Brier[d] = mean((pred_model - abs(as.numeric(Y.test)-2))**2)
    ROC = performance(predictions, measure = "tpr", x.measure = "fpr")
    
    plot(ROC,colorize=TRUE,main='Receiver operator characteristics curve',print.cutoffs.at=seq(0.1,1,0.1),
         text.adj = c(-0.2,1.7),xlab='1-Specificity',ylab='Sensitivity')
    legend('bottomright',bty='n',legend=c(paste('AUC = ',round(AUC[d],3)), paste("Brier = ", round(Brier[d],3))),
           pch=rep(16,2),col=c('red','blue'), title = 'Performance')
    legend('topleft',bty='n',legend=paste(names(best_tune),best_tune, sep = ' = '),
           pch=rep(16,2),col=c('red','blue'), title = 'Tuning parameters')
    segments(0,0,1,1,lty=2)
    mtext(toupper(paste0("Data: ", data_type,'   Model: ', model, "   Date: ", iter[d])))
    
  }
  ## After iteration is done. Compute the performance over the year.
  ## Plot corresponding ROC.
  predictions = prediction(pred_vec, Y.testFull, label.ordering = c("Legitimate",'Fraud'))
  auc_tot = as.numeric(performance(predictions,"auc")@y.values)
  brier_tot = mean((pred_vec - abs(as.numeric(Y.testFull)-2))**2)
  ROC = performance(predictions, measure = "tpr", x.measure = "fpr")
  
  plot(ROC,colorize=TRUE,main='Receiver operator characteristics curve',print.cutoffs.at=seq(0.1,1,0.1),
       text.adj = c(-0.2,1.7),xlab='1-Specificity',ylab='Sensitivity')
  legend('bottomright',bty='n',legend=c(paste('AUC = ',round(auc_tot,3)), paste("Brier = ", round(brier_tot,3))),
         pch=rep(16,2),col=c('red','blue'))
  segments(0,0,1,1,lty=2)
  mtext(toupper(paste0("Data: ", data_type,'   Model: ', model, "   Metric: ", metric)))
  if(save.plot){
    dev.off()
  }
  names(AUC) = names(Brier) = iter
  return(list(AUC=AUC, predictions = pred, predictions_rolling = pred_vec, AUC_tot = auc_tot,
              Brier_tot = brier_tot, VarImp = VarImp, Models = models))
}

## Run the code for all models and all possible combinations of data sources.
## Save the results in 4 (model) .Rdata files.

setwd("/nr/project/stat/BigInsight/Projects/Fraud/Subprojects/DNB-AML-Master/ROCs")

a=fit_time(data_type = 'BASE', model = 'glmnet', save.plot = T)
a_RFM_case=fit_time(data_type ='base_rfm', rfm_type = 'case', model = 'glmnet', save.plot = T, corr_cutoff = 0.85)
a_RFM_implicated=fit_time(data_type ='base_rfm', rfm_type = 'implicated', model = 'glmnet', save.plot = T, corr_cutoff = 0.85)
a_RFM_party=fit_time(data_type ='base_rfm', rfm_type = 'party', model = 'glmnet', save.plot = T, corr_cutoff = 0.85)
a_NET=fit_time(data_type = 'base_net', model = 'glmnet', save.plot = T, corr_cutoff = 0.85)
a_RFM_case_NET=fit_time(data_type ='base_rfm_net', rfm_type = 'case', model = 'glmnet', save.plot = T, corr_cutoff = 0.85)
a_RFM_party_NET=fit_time(data_type ='base_rfm_net', rfm_type = 'party', model = 'glmnet', save.plot = T, corr_cutoff = 0.85)
a_RFM_implicated_NET=fit_time(data_type ='base_rfm_net', rfm_type = 'implicated', model = 'glmnet', save.plot = T, corr_cutoff = 0.85)

setwd("/nr/project/stat/BigInsight/Projects/Fraud/Subprojects/DNB-AML-Master")
save(list=c('a','a_RFM_case','a_RFM_party','a_RFM_implicated',
            'a_NET','a_RFM_case_NET','a_RFM_party_NET','a_RFM_implicated_NET'),file='glmnet_model.Rdata')

setwd("/nr/project/stat/BigInsight/Projects/Fraud/Subprojects/DNB-AML-Master/ROCs")
b=fit_time(data_type = 'BASE', model = 'C5.0', save.plot = T)
b_RFM_case = fit_time(data_type = 'base_rfm', rfm_type='case', model = 'C5.0', save.plot = T, corr_cutoff = 0.85)
b_RFM_implicated=fit_time(data_type ='base_rfm', rfm_type = 'implicated', model = 'C5.0', save.plot = T, corr_cutoff = 0.85)
b_RFM_party=fit_time(data_type ='base_rfm', rfm_type = 'party', model = 'C5.0', save.plot = T, corr_cutoff = 0.85)
b_NET=fit_time(data_type = 'base_net', model = 'C5.0', save.plot = T, corr_cutoff = 0.85)
b_RFM_case_NET=fit_time(data_type ='base_rfm_net', rfm_type = 'case', model = 'C5.0', save.plot = T, corr_cutoff = 0.85)
b_RFM_party_NET=fit_time(data_type ='base_rfm_net', rfm_type = 'party', model = 'C5.0', save.plot = T, corr_cutoff = 0.85)
b_RFM_implicated_NET=fit_time(data_type ='base_rfm_net', rfm_type = 'implicated', model = 'C5.0', save.plot = T, corr_cutoff = 0.85)

setwd("/nr/project/stat/BigInsight/Projects/Fraud/Subprojects/DNB-AML-Master")
save(list=c('b','b_RFM_case','b_RFM_party','b_RFM_implicated',
            'b_NET','b_RFM_case_NET','b_RFM_party_NET','b_RFM_implicated_NET'),file='C50_model.Rdata')

setwd("/nr/project/stat/BigInsight/Projects/Fraud/Subprojects/DNB-AML-Master/ROCs")
c=fit_time(data_type = 'BASE', model = 'rf', save.plot = T)
c_RFM_case = fit_time(data_type = 'base_rfm', rfm_type='case', model = 'rf', save.plot = T, corr_cutoff = 0.85)
c_RFM_implicated = fit_time(data_type = 'base_rfm', rfm_type='implicated', model = 'rf', save.plot = T, corr_cutoff = 0.85)
c_RFM_party = fit_time(data_type = 'base_rfm', rfm_type='party', model = 'rf', save.plot = T, corr_cutoff = 0.85)
c_NET=fit_time(data_type = 'base_net', model = 'rf', save.plot = T, corr_cutoff = 0.85)
c_RFM_case_NET=fit_time(data_type ='base_rfm_net', rfm_type = 'case', model = 'rf', save.plot = T, corr_cutoff = 0.85)
c_RFM_party_NET=fit_time(data_type ='base_rfm_net', rfm_type = 'party', model = 'rf', save.plot = T, corr_cutoff = 0.85)
c_RFM_implicated_NET=fit_time(data_type ='base_rfm_net', rfm_type = 'implicated', model = 'rf', save.plot = T, corr_cutoff = 0.85)

setwd("/nr/project/stat/BigInsight/Projects/Fraud/Subprojects/DNB-AML-Master")
save(list=c('c','c_RFM_case','c_RFM_party','c_RFM_implicated',
            'c_NET','c_RFM_case_NET','c_RFM_party_NET','c_RFM_implicated_NET'),file='rf_model.Rdata')

setwd("/nr/project/stat/BigInsight/Projects/Fraud/Subprojects/DNB-AML-Master/ROCs")
d=fit_time(data_type = 'BASE', model = 'gbm', save.plot = T)
d_RFM_case = fit_time(data_type = 'base_rfm', rfm_type='case', model = 'gbm', save.plot = T, corr_cutoff = 0.85)
d_RFM_implicated = fit_time(data_type = 'base_rfm', rfm_type='implicated', model = 'gbm', save.plot = T, corr_cutoff = 0.85)
d_RFM_party = fit_time(data_type = 'base_rfm', rfm_type='party', model = 'gbm', save.plot = T, corr_cutoff = 0.85)
d_NET=fit_time(data_type = 'base_net', model = 'gbm', save.plot = T, corr_cutoff = 0.85)
d_RFM_case_NET=fit_time(data_type ='base_rfm_net', rfm_type = 'case', model = 'gbm', save.plot = T, corr_cutoff = 0.85)
d_RFM_party_NET=fit_time(data_type ='base_rfm_net', rfm_type = 'party', model = 'gbm', save.plot = T, corr_cutoff = 0.85)
d_RFM_implicated_NET=fit_time(data_type ='base_rfm_net', rfm_type = 'implicated', model = 'gbm', save.plot = T, corr_cutoff = 0.85)

setwd("/nr/project/stat/BigInsight/Projects/Fraud/Subprojects/DNB-AML-Master")
save(list=c('d','d_RFM_case','d_RFM_party','d_RFM_implicated',
            'd_NET','d_RFM_case_NET','d_RFM_party_NET','d_RFM_implicated_NET'),file='gbm_model.Rdata')