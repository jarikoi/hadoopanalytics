setwd("/Users/jkoister/Work/Analysis")

jobs<-c("oink_pymk_digests-daily-production",
        "oink_spam_systems-daily",
        "oink_tweet_event_details-daily",
        "oink_email_top_stories-daily-weekly_production",
        "oink_email_invites-daily",
        "frigate_mobile_sdk_version")

compare1<-function(job){
  perd1<-"~/Work/Analysis/jobcost/2-1"
  perd2<-"~/Work/Analysis/jobcost/5-15"
  return(compareComputecost(perd1,perd2,job))
}

compareComputecost<-function(per1dir,per2dir,job){
  debug("In Compare Cost","")
  per1File_df<-getdf(per1dir,job)
  per2File_df<-getdf(per2dir,job)
  per1<-jobcost(per1dir,per1File_df)
  per2<-jobcost(per2dir,per2File_df)
  sper1<-jobHDFSCost(per1dir,per1File_df)
  sper2<-jobHDFSCost(per2dir,per2File_df)
  
  print("-----------------------------")
  print(job)
  cat("Period 1 HDFS Read Cost : ",sper1,"\n")
  cat("Period 2 HDFS Read Cost : ",sper2,"\n")
  analyzehbr(sper1,sper2,job)
  cat("Period 1 Compute Cost : ",per1,"\n")
  cat("Period 2 Compute Cost : ",per2,"\n")
  return(analyzembm(per1,per2,job))
}

jobcost<-function(directory,df){
  mbm<-averageMBM(df)
  #print(mbm)
  return(mbm)
}

jobHDFSCost<-function(directory,df){
  hdr<-averageHDFSBRead(df)
  #print(mbm)
  return(hdr)
}

averageMBM<-function(df){
  mbm<-vector("numeric")
  mbm<-data.matrix(df["megabyteMillis"])
  return(mean(mbm))
}

averageHDFSBRead<-function(df){
  hbr<-vector("numeric")
  hbr<-data.matrix(df["hdfsBytesRead"])
  return(mean(hbr))
}

getdf<-function(directory,file){
  name<-createName(directory,file)
  df<-read.csv(name)
  return (df)
}

analyzembm<-function(per1,per2,job){
  if(per2 < per1) {
    improve<-(1-per2/per1)*100
    cat("Megabyte Millis decreased by ",improve,"%\n")
    return(-improve)
  } else if (per2 > per1) {
    increase<-(1-per1/per2)*100
    cat("Megabyte Millis Increased by ",increase,"%\n")
    return(increase)
  } else {
    cat("Megabyte Millis unchanged")
    return(0)
  }
}


analyzehbr<-function(per1,per2,job){
  if(per2 < per1) {
    improve<-(1-per2/per1)*100
    cat("HDFS Bytes Read decreased by ",improve,"%\n")
    return(-improve)
  } else if (per2 > per1) {
    increase<-(1-per1/per2)*100
    cat("HDFS Bytes Read Increased by ",increase,"%\n")
    return(increase)
  } else {
    cat("HDFS Bytes Read unchanged")
    return(0)
  }
}
createName<- function(directory,id) {
  directory<-paste(directory,"/",sep="")
  name<-paste(directory,id,sep="")
  name<-paste(name,".csv",sep="")
  return(name)
}

debug<-function(lable,value="",nl=FALSE){
  if(nl){
    cat(lable,value,"\n")
  }else{
    cat(lable,value)
  }
}