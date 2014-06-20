
jobcost<-function(directory,file){
  
  df-<getdr(jobcost)
}


getdf<-function(directory,file){
  name<-createName(directory,file)
  df<-read.csv(name)
  return (df)
}

debug<-function(lable,value,nl=FALSE){
  if(nl){
    cat(lable,value,"\n")
  }else{
    cat(lable,value)
  }
}