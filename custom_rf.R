## Custom rf

nodesize_seq = function(n, classification=F, len = 3){
  ## n: number of instances
  print(len)
  if (len == 1) {# default
    tuneSeq <- if (classification) 
      1
    else 5
  }
  else {
    if (n <= len) {
      tuneSeq <- floor(seq(1, to = n, length = n))
    }
    else {
      if (n < 100) 
        tuneSeq <- floor(seq(1, to = n/2, length = len))
      else tuneSeq <- floor(2^seq(0, to = log(n/2, base = 2), 
                                  length = len))
    }
  }
  if (any(table(tuneSeq) > 1)) {
    tuneSeq <- unique(tuneSeq)
    cat("note: only", length(tuneSeq), "unique complexity parameters in default grid.", 
        "Truncating the grid to", length(tuneSeq), ".\n\n")
  }
  tuneSeq
}

customrf <- list(label = "Random Forest",
                  library = "randomForest",
                  loop = NULL,
                  type = c("Classification", "Regression"),
                  parameters = data.frame(parameter = c("mtry","ntree","nodesize"),
                                          class = rep("numeric",3),
                                          label = c("#Randomly Selected Predictors","#Number of bagged trees", "#Minimum node size")),
                  grid = function(x, y, len = NULL, search = "grid"){
                    if(search == "grid") {
                      out <- data.frame(mtry = caret::var_seq(p = ncol(x), 
                                                              classification = is.factor(y), 
                                                              len = len),
                                        ntree = c(100,500,1000,1500,2000,2500),
                                        nodesize = nodesize_seq(n=nrow(x),
                                                                classification = is.factor(y),
                                                                len = len))
                    } else {
                      out <- data.frame(mtry = unique(sample(1:ncol(x), size = len, replace = TRUE)),
                                        ntree = unique(sample(c(100,500,1000,1500,2000,2500),size = len, replace = TRUE)),
                                        nodesize = unique(sample(1:(nrow(x)/2), size = len, replace = TRUE)))
                    }
                    out
                  },
                  fit = function(x, y, wts, param, lev, last, classProbs, ...) 
                    randomForest(x, y, mtry = param$mtry, ntree = param$ntree, nodesize = param$nodesize, ...),
                  predict = function(modelFit, newdata, submodels = NULL) 
                    if(!is.null(newdata)) predict(modelFit, newdata) else predict(modelFit),
                  prob = function(modelFit, newdata, submodels = NULL)
                    if(!is.null(newdata)) predict(modelFit, newdata, type = "prob") else predict(modelFit, type = "prob"),
                  predictors = function(x, ...) {
                    ## After doing some testing, it looks like randomForest
                    ## will only try to split on plain main effects (instead
                    ## of interactions or terms like I(x^2).
                    varIndex <- as.numeric(names(table(x$forest$bestvar)))
                    varIndex <- varIndex[varIndex > 0]
                    varsUsed <- names(x$forest$ncat)[varIndex]
                    varsUsed
                  },
                  varImp = function(object, ...){
                    varImp <- randomForest::importance(object, ...)
                    if(object$type == "regression")
                      varImp <- data.frame(Overall = varImp[,"%IncMSE"])
                    else {
                      retainNames <- levels(object$y)
                      if(all(retainNames %in% colnames(varImp))) {
                        varImp <- varImp[, retainNames]
                      } else {
                        varImp <- data.frame(Overall = varImp[,1])
                      }
                    }
                    
                    out <- as.data.frame(varImp)
                    if(dim(out)[2] == 2) {
                      tmp <- apply(out, 1, mean)
                      out[,1] <- out[,2] <- tmp  
                    }
                    out
                  },
                  levels = function(x) x$classes,
                  tags = c("Random Forest", "Ensemble Model", "Bagging", "Implicit Feature Selection"),
                  sort = function(x) x[order(x[,1]),],
                  oob = function(x) {
                    out <- switch(x$type,
                                  regression =   c(sqrt(max(x$mse[length(x$mse)], 0)), x$rsq[length(x$rsq)]),
                                  classification =  c(1 - x$err.rate[x$ntree, "OOB"],
                                                      e1071::classAgreement(x$confusion[,-dim(x$confusion)[2]])[["kappa"]]))
                    names(out) <- if(x$type == "regression") c("RMSE", "Rsquared") else c("Accuracy", "Kappa")
                    out
                  })