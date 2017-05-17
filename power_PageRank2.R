### Power iteration method for PageRank 
pr_iter2 = function(net, directed=T, weights=NULL, tol=10, max.iter=3000, beta = NULL, alpha=0.85, loop=T, scale=T){
  ## net -- an igraph object
  ## directed -- boolean, saying whether the network should be considered as directed
  ##             If is.directed(net) == F then this is ignored
  ## weights -- vector of edge weights, must have the same length as ecount(net)
  ##            If is.weighted(net) == T then weighted PageRank is calculated automatically unless weights=NA
  ## tol -- tolerance for convergence(i.e. 1/10^tol)
  ## max.iter -- maximum number of iterations in power method
  ## beta -- restart vector, default is NULL (non-personalized PageRank)
  ##         If not NULL then it must be of length(beta) = vcount(net) # of nodes in net
  ## alpha -- restart probability in PageRank(teleportation probability), must be in [0,1]
  ## loops -- boolean, whether to consider loops in PageRank calculation. This is considered only in the case that
  ##          net actually contains loops.
  ## scale -- boolean saying whether to scale scores such that they sum to 1
  ##          Shouldn't be necessary if PageRank matrix is correct
  
  if(!is.igraph(net)){stop(paste('Object' , deparse(substitute(net)),  'is not an igraph network!'))}
  N = vcount(net); n=ecount(net)
  
  if(!is.null(beta) & length(beta)!=N){stop('Restart vector beta must either NULL or with the length equal to number of vertices!')}
  if(any(beta<0)){stop('Restart vector beta contains negative values!')}
  
  if(alpha<0 | alpha>1){stop('Restart probability alpha must be in [0,1]!')}
  
  w_IND = is.weighted(net)
  
  if(anyNA(weights)){
    weighted=F
    warning('Weights contain NA. Unweighted version will be calculated!')    
  } else if(is.null(weights)){
    if(w_IND){
      weighted=T
      warning('No weights supplied but network is weighted. Weighted version will be calculated!')
    } else{
      weighted=F
      warning('No weights supplied and network is non-weighted. Unweighted version will be calculated!')
    }
  } else if(any(weights < 0) | length(weights)!=n){
    stop('Invalid length of weights vector or negative weights!')
  } else{
    weighted=T
  }
  
  ##
  directed = directed & is.directed(net)
  if(!directed){## Undirected version handled here
    netUN = as.undirected(net,'each')
    A = get.adjacency(netUN)
    loop = loop & sum(diag(A)) != 0 # Whether to consider loops
    if(!loop){diag(A)=0}
    if(!weighted){D = Diagonal(N,1/rowSums(A))}## Unweighted version
    else{## Weighted version
      if(w_IND){
        tmp_net = simplify(netUN, remove.loops = !loop, edge.attr.comb = list('weight' = 'sum','ignore'))
        tmp=edge_attr(tmp_net)[['weight']]
      }
      else{
        edge_attr(netUN, 'weight') = weights
        #l=list('sum','ignore');names(l)=c(w_name,'')
        tmp_net = simplify(netUN, remove.loops = !loop, edge.attr.comb = list('weight' = 'sum','ignore'))
        #tmp = edge_attr(tmp_net)[[w_name]]
        tmp=edge_attr(tmp_net)[['weight']]
      }
      ind = which(triu(A)!=0, arr.ind=T)
      ind = ind[order(ind[,1],ind[,2]),]
      ind2 = ind[,c(2,1)]
      A[ind] = tmp
      A[ind2] = tmp
      D = Diagonal(N,1/rowSums(A))
    }
  }
  else{## Directed version handled here
    A = get.adjacency(net)
    loop = loop & sum(diag(A)) != 0 # Whether to consider loops
    if(!loop){diag(A)=0}
    if(!weighted){D = Diagonal(N,1/rowSums(A))}## Unweighted version
    else{## Weighted version
      if(w_IND){
        tmp_net = simplify(net, remove.loops = !loop, edge.attr.comb = list('weight' = 'sum','ignore'))
        tmp=edge_attr(tmp_net)[['weight']]
      }
      else{
        edge_attr(net, 'weight') = weights
        #l=list('sum','ignore');names(l)=c(w_name,'')
        tmp_net = simplify(net, remove.loops = !loop, edge.attr.comb = list('weight' = 'sum','ignore'))
        #tmp = edge_attr(tmp_net)[[w_name]]
        tmp=edge_attr(tmp_net)[['weight']]
      }
      ind = which(A!=0, arr.ind=T)
      ind = ind[order(ind[,1],ind[,2]),]
      A[ind] = tmp
      D = Diagonal(N,1/rowSums(A))
    }
  }
  if(is.null(beta)){
    warning('Non-personalized version will be calculated!')
    Beta_vec = (1-alpha)*rep(1/N,N)
  }
  else{
    Beta_vec=(1-alpha)*beta/sum(beta)
  }
  A_norm = D%*%A
  
  ## Sink nodes(directed) or isolates
  ## na.rm = T in order to handle isolates which introduces NAs into A_norm matrix
  zero_rows = rowSums(A_norm, na.rm = T) == 0
  
  
  x0 = rep(0,N)
  x1 = rep(1/N,N)
  eps = 1/10^tol
  iter = 0
  #prob = 0
  ## Iterate PageRank equation until convergence or max.iter reached
  ## Two different versions based on sink nodes presence(directed) and isolates(directed/undirected)
  if(length(zero_rows)>0){
    A_norm = A_norm[!zero_rows,]
    while(sum(abs(x0-x1)) > eps){
      if(iter < max.iter){
        x0 = x1
        x1 = alpha*(x1[!zero_rows]%*%A_norm + 1/N*sum(x1[zero_rows]))+Beta_vec
        iter = iter + 1
        #prob[iter] = sum(x1)
      }
      else{warning('Maximum number of iterations reached!')
        #if(scale){x1=x1/sum(x1)} 
        x1 = as.vector(x1)
        names(x1) = vertex_attr(net)[['name']]
        return(x1)
      }
    }
  }
  else{
    while(sum(abs(x0-x1)) > eps){
      if(iter < max.iter){
        x0 = x1
        x1 = as.vector(alpha*x1%*%A_norm)+Beta_vec
        iter = iter + 1
        #prob[iter] = sum(x1)
      }
      else{warning('Maximum number of iterations reached!')
        #if(scale){x1=x1/sum(x1)} 
        x1 = as.vector(x1)
        names(x1) = vertex_attr(net)[['name']]
        return(x1)
      }
    }
  }
  x1 = as.vector(x1)
  names(x1) = vertex_attr(net)[['name']]
  return(list(vector=x1,n.iter=iter))#,prob=prob))
}