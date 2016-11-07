#Version recorded on 9/26/2016 by Cen Hu
#Update new output file name




#Version recorded on 9/21/2016 by Cen Hu
#Updates aggregation for report_log and all vendors


#version recorded on 9/19/2016 by Cen Hu
##update robarts vendor
library(xml2)
library(dplyr)
library(docxtractr)
library(RODBC)
library(sqldf)

rm(list=ls())
gc(reset=TRUE)

start=Sys.time()

#===================LOAD XLSX PACKAGE
if (Sys.getenv("JAVA_HOME")!="")
       Sys.setenv(JAVA_HOME="")
library(xlsx)

#sample
#write.xlsx(test$site_staff,file="C:/Users/hucen/GitHub/pro/python/Gilead_XML/
#vendor_tracker/Vendor_Spreadsheets/tttt.xlsx",sheetName = "Site_staff",append=TRUE,row.names=FALSE)


#number of tables in a word document
###docx_xml=read_docx(f)
###ns=docx_xml$ns
###tbls_num=docx_tbl_count(docx_xml)
  
#check which tables have checkbox
###sapply(docx_xml$tbls, function(x) grepl('checkBox', toString(x)))

#check which table have value--will check after compose dataframe,
##by columns name if with val2 or not.

#if with checkbox replace checkbox text to checked value(this should
##be a nested function)
country_code=data.frame(Country=c("Argentina", "Australia", "Austria", "Belgium",	"Bulgaria", "Bahamas",	"Brazil",	"Canada",	"Switzerland",	"Chile",
                                  "China",	"Colombia",	"Costa Rica",	"Czech Republic",	"Germany",	"Denmark",	"Dominican Republic",	"Ecuador",	"Egypt",
                                  "Spain",	"Estonia",	"Finland",	"France",	"United Kingdom",	"Greece",	"Guatemala",	"Hong Kong",	"Croatia",	"Haiti",
                                  "Hungary",	"Indonesia",	"India",	"Ireland",	"Iceland",	"Israel",	"Italy",	"Jamaica",	"Japan",	"Korea",	"Lebanon",
                                  "Lithuania",	"Luxembourg",	"Latvia",	"Morocco",	"Moldova",	"Mexico",	"Malta",	"Montenegro",	"Malawi",	"Malaysia",	"Netherlands",
                                  "Norway",	"New Zealand",	"Oman",	"Pakistan",	"Panama",	"Peru",	"Philippines",	"Poland",	"Puerto Rico",	"Portugal",
                                  "Romania",	"Russia",	"Singapore",	"Serbia",	"Slovakia",	"Slovenia",	"Sweden",	"Thailand",	"Tunisia",	"Turkey",	"Taiwan",
                                  "Uganda",	"Ukraine",	"Uruguay",	"United States",	"Venezuela",	"Viet Nam",	"South Africa"),
                        Code=c(54,	61,	43,	32,	359,	1,	55,	1,	41,	56,	86,	57,	506,	42,	49,	45,	1,	593,	20,	34,	372,	358,	33,	44,	30,	502,	852,	385,	1,	36,
                               62,	91,	353,	354,	972,	39,	1,	81,	82,	961,	370,	351,	371,	212,	373,	52,	356,	381,	265,	60,	31,	47,	64,	968,	92,	507,	51,
                               63,	48,	1, 351,	40,	7,	65,	381,	421,	386,	46,	66,	216,	90,	886,	256,	380,	598,	1,	58,	84,	27))




#======================================================function for trim
trim <- function (x) gsub("^\\s+|\\s+$", "", x)

#======================================================function for Upper and Lower country
country_rename <- function(x) {

  s <- strsplit(x, " ")[[1]]
  paste(toupper(substring(s, 1,1)), tolower(substring(s, 2)),
        sep="", collapse=" ")

}

#=====================================test file
#For the first table, it always be the project info and site info
##1. Get protocol id 2. save as site table
###tst_tb=docx_xml$tbls[3]

#xml_find_all(tst_tb,".//w:checkBox/w:default")
#checkbox_value=lapply(xml_find_all(tst_tb,".//w:checkBox/w:default"), 
#                      function(x) grep(1,toString(x)))


#=======================================================function for a single table=============
  
gilead_docx_tb=function(tst_xml, tst_tb)
{
  
#add flag for minors wrong info
flag={}
  
ns=tst_xml$ns
  
tst_rows=xml_find_all(tst_tb,"./w:tr", ns=ns)

df=bind_rows(lapply(tst_rows,function(row)
  {
    vals=xml_text(xml_find_all(row,".//w:tc", ns=ns), trim=TRUE)
    
    
    if(length(grep("\u2610|\u2612|FORMCHECKBOX", vals)))
    { 
      #find index of the cell with checkbox value
      cb_index=grep("\u2610|\u2612|FORMCHECKBOX",vals)
      
      #find checkbox value "1" or "0"
      if(length(grep("\u2610|\u2612",vals)))
      {
        checkbox_value=lapply(xml_find_all(row,".//w14:checkbox/w14:checked"),
                            function(x) grep('"1"',toString(x)))
      }else{
        checkbox_value=lapply(xml_find_all(row,".//w:checkBox/w:default"),
                              function(x) grep(1,toString(x)))
      }
      #find the index of checkbox with value = "1"
      checked_index=grep(1, checkbox_value)
      
      #list of value in the cell
      lc=trim(unlist(strsplit(vals[cb_index],"\u2610|\u2612|FORMCHECKBOX")))
      
      #get rid of empty value in the list
      lc_clean=lc[lc!=""]
      
      #detect if the checkbox row is for studyid or vendor
      if(length(grep('Yes',lc_clean)))
        {
          #by default set all checkbox cell to "No"
          vals[cb_index]="No"
      
          #return checked checkbox value to the cell
          if(length(grep('Yes, PI Access',lc_clean[1])))
            {
              vals[ceiling((checked_index+3)/2)]=lc_clean[checked_index]
            }else
            {
              vals[(checked_index+3)/2]=lc_clean[checked_index]
            }
        }else
          {
            if(length(grep('All Protocols',lc_clean[checked_index])))
            {
              vals[cb_index]=paste((lc_clean[-1]), collapse=' and ')
            }else
            {
              vals[cb_index]=paste((lc_clean[checked_index]), collapse=' and ')
            }
        
          }
    }
    
    names(vals)=sprintf("v%d",1:length(vals))
    data.frame(as.list(vals),stringsAsFactors = FALSE)
  }

))

#for site 
if(ncol(df)==2 && nrow(df)==17)
{

  #get rid of ":"
  df[,1]=gsub("\\:.*","",df[,1])
  df=data.frame(df,row.names = NULL, check.names = FALSE)
  colnames(df)=c('Para',"Value")
  
  t_df=t(df)
  colnames(t_df)=paste("site ", t_df[1,], sep="")
  t_df=data.frame(t_df,check.names = FALSE)
  t_df=t_df[-1,]
  t_df=t_df[,colSums(is.na(t_df))==0]
  rownames(t_df)=NULL
  extract_df=t_df
}else #for site stuff
  if(ncol(df)==5)
{
  t_df=t(df)
  #get rid of explainations
  colnames(t_df)=gsub("\\:.*","",t_df[1,])
  t_df=data.frame(t_df[-1,],row.names=NULL, check.names = FALSE)
  colnames(t_df)[1]="Specify Role"
  
  #removce column with no value
  t_df=t_df[, colSums(is.na(t_df)) == 0] 
  
  #remove rows if "Specify Role" has no value
  t_df=t_df[!(t_df$`Specify Role`)==""&!(is.na(t_df$`Specify Role`)),]

  #remove rows if "Name" has no value
  if(sum(t_df$`Last Name`=="")>0 & sum(t_df$`First Name`=="")>0)
  {
    flag=paste(flag,"Site staff table roles without name, deleted; ", sep="")
    t_df=t_df[!((t_df$`Last Name`=="") + (t_df$`First Name`=="")),]
  }
  
  extract_df=t_df
  #if there is exception, add flag column

}else #drug
  if(ncol(df)==2 && nrow(df)==14)
{
  t_df=t(df)
  colnames(t_df)=paste("Drug Delivery ",t_df[1,],sep="")
  t_df=data.frame(t_df,check.names=FALSE)
  t_df=t_df[,-1]
  
  colnames(t_df)=gsub("\\:.*","",colnames(t_df))
  t_df=data.frame(t_df[-1,],row.names=NULL, check.names = FALSE)
  rownames(t_df)=NULL
  
  extract_df=t_df
  }else #for update log
    if(ncol(df)==4)
    {
     colnames(df)=df[1,]
     extract_df=df[-1,]
    }else{
    df[,1]=gsub("\\:.*","",df[,1])
    df=data.frame(df,row.names=NULL, check.names = FALSE)
    extract_df=df
}



return(list(extract_df=extract_df, flag=flag))
}

#========================================function for a single docx
gilead_docx=function(docx_xml)
{
  if(docx_tbl_count(docx_xml)<1)
  {
    return(list())
  }
    ns=docx_xml$ns
  
  

  t_site={}
  staff={}
  drug={}
  compby={}
  update_log={}
  
  t_site_dedu={}
  site_staff={}
  site_drug={}
  
  flag={}
  
  for(i in 1:docx_tbl_count(docx_xml))
  {
    tbl=gilead_docx_tb(docx_xml,docx_xml$tbls[i])$extract_df
    flag=tbl$flag
    #site
    if( ncol(tbl)==16)
    {
      t_site=tbl
    }else #staff 
      if(ncol(tbl)==22 | ncol(tbl)==21 | ncol(tbl)==20)
      {
        if(!length(staff))
        {
          staff=rbind(staff,tbl)
        }else{
          colnames(tbl)=colnames(staff)
          staff=rbind(staff,tbl)
        }
      }else #drug
        if(ncol(tbl)==13)
        {
          drug=tbl
        }else# completed by
          if(nrow(tbl)==4 & ncol(tbl)==2)
          {
            compby=tbl
          }else #log
          {update_log=tbl}
    
  }
  
  
  #==========================detect how many protocols in this word document
  protocol_no=t_site$`site Protocol No`
  list_protocol=trim(unlist(strsplit(toString(protocol_no), "and")))
  
  if(length(list_protocol)==0)
  {
    flag=paste("Protocol No Checkbox fatal error.")
  }
  
  if(length(list_protocol)>0)
  {
    for(i in 1:length(list_protocol))
    {
     t_site$`site Protocol No`=list_protocol[i]
     t_site_dedu=rbind(t_site_dedu,t_site)
    }
  }
  site_staff=merge(t_site_dedu,staff)
  site_drug=merge(t_site_dedu,drug)
  
  return(list(site_staff=site_staff, site_drug=site_drug, compby=compby, update_log=update_log,flag=flag))
}

#===================================Main function

#input_path="//chofile/Applications/ETLKCI/ETLUserSource/Gilead/Gilead_Diversity_and_Selection_Study/D&S_Input_Folder"
input_path="C:/Users/hucen/GitHub/pro/python/Gilead_XML/input_file_new"
output_path="C:/Users/hucen/GitHub/pro/python/Gilead_XML/output_file_new"
finished_path="C:/Users/hucen/GitHub/pro/python/Gilead_XML/finished_file_new"
onhold_path="C:/Users/hucen/GitHub/pro/python/Gilead_XML/onhold_file_new"
test_path="C:/Users/hucen/GitHub/pro/python/Gilead_XML/badass"

setwd(input_path)
list_files=list.files(pattern='.docx$')

total_site_staff={}
total_site_drug={}
new_report_log={}
new_report_log=data.frame(matrix(ncol=8, nrow=length(list_files)))
colnames(new_report_log)=c("Date", "FileName","Site","PI","UniqueSitePI", "StudyId", "Flag", "Result")

if(length(list_files)>0)
{
for(i in 1:length(list_files))
{
  temp_site_staff={}
  temp_site_drug={}
  
  temp_xml=read_docx(list_files[i])
  temp_result=gilead_docx(temp_xml)
  
  if(length(temp_result$flag)==0)
  {
    flag=""
  }else{flag=temp_result$flag  }
  

  ##for PI's name not match in differnet tables
  ##1. delect title in name
  temp_site_staff=as.data.frame(sapply(temp_result$site_staff,function(x)
    trim(gsub("Dr.ssa|MD|Professor|Prof.|Dr.","",x))))
  temp_site_drug=as.data.frame(sapply(temp_result$site_drug,function(x)
    trim(gsub("Dr.ssa|MD|Professor|Prof.|Dr.","",x))))
  
 
  
  #report fill in
  new_report_log[i,1]=toString(Sys.time())
  new_report_log[i,2]=list_files[i]
  new_report_log[i,3]=ifelse(nrow(temp_site_staff)==0,"Fatal Error" ,unique(paste(temp_site_staff$`site Site Number`)))
  new_report_log[i,4]=ifelse(nrow(temp_site_staff)==0,"Fatal Error", unique(paste(temp_site_staff$`site Investigator Last Name`, temp_site_staff$`site Investigator First Name`)))
  new_report_log[i,6]=paste(unique(temp_site_staff$`site Protocol No`), collapse=" and ")
  
  
  diff_pi=paste("    select *
                  from  temp_site_staff
                  where `Specify Role` like '%Principal Investigator%'
                  and (`site Investigator Last Name`!= `Last Name`
                  or `site Investigator First Name` !=`First Name`)",sep="")
  
  #flag protocol no checkbox erro
  if(grepl("Protocol No Checkbox fatal error; ", flag))
  {
    new_report_log[i,7]=paste("Fatal: ", flag, sep="")
    new_report_log[i,8]="On Hold"
  }else #flag site and drug table with different rows.
    if(nrow(temp_site_staff)==0 | nrow(temp_site_drug)==0)
    {
      new_report_log[i,7]=paste(flag, "Fatal: Site or Drug table with different rows; ", sep="")
      new_report_log[i,8]="On Hold"
    }else#flag file with no study id
  if(length(new_report_log[i,6])==0)
  {
    new_report_log[i,7]=paste(flag,"Major: No Study ID; ",sep="")
    new_report_log[i,8]="On Hold"
  }else#flag PI's name not match in tables
    if(nrow(sqldf(diff_pi))>0)  
  {
    new_report_log[i,7]=paste(flag,"Major: PI's name doesnt'match between tables; ",sep="")
    new_report_log[i,8]="On Hold"
  }else#flag site infomation not complete
    if(sum(temp_site_staff[,c(1:10,13:16)]=="")>0)
  {
    new_report_log[i,7]=paste(flag,"Major: Site Information not Complete; ",sep="")
    new_report_log[i,8]="On Hold"
  }else#flag Drug delivery info complete
    if(sum(temp_site_drug[,c(17:23,26:29)]=="")>0)
    {
      new_report_log[i,7]=paste(flag,"Major: Drug Delivery info not complete; ",sep="")
      new_report_log[i,8]="On Hold"
    }else#flag PI has no checked checkbox value
      if(sum(temp_site_staff[1,c(31:length(temp_site_staff))]=="No")==8)
      {
        new_report_log[i,7]=paste(flag,"Major: PI has no checked value; ", sep="")
        new_report_log[i,8]="On Hold"
      }else#name not complete
        if((sum(temp_site_staff$`Last Name`=="")+sum(temp_site_staff$`First Name`==""))>0)
        {
          new_report_log[i,7]=paste(flag,"Major: Staff's Name not Complete; ")
          new_report_log[i,8]="On Hold"
        }else#email address not complete
          if(sum(temp_site_staff$`Specify Role`!='Principal Investigator' & temp_site_staff$`E-mail`=="")>0)
          {
            new_report_log[i,7]=paste(flag,"Major: Staff's E-mail is missing; ")
            new_report_log[i,8]="On Hold"
          }else
           {
             new_report_log[i,8]="Pass"

    
    ###########################################################
    #                     DATA IMPUTE AND CLEAN   for STAFF   #
    ###########################################################
    #record missing condition for blanks
    temp_flag={}
    
    #add levels to the blank columns
    for(lvl in 6:16)
    {
      levels(temp_site_staff[,lvl+14])=append(levels(temp_site_staff[,lvl+14]),levels(temp_site_staff[,lvl]))
    }
    
    #add levels to checkbox
    for(clvl in 31:length(temp_site_staff))
    {
      levels(temp_site_staff[,clvl])=append(levels(temp_site_staff[,clvl]),c("Yes","No","Yes, PI Access"))
    }
    
    for(j in 1:nrow(temp_site_staff))
    {
      
      if(temp_site_staff[j,]$`Specify Role`=='Principal Investigator')
      {
        temp_site_staff[j,c(20:30)]=temp_site_staff[j,c(6:16),drop=TRUE]
      }
      
      #check phone number
      if(temp_site_staff[j,]$`Specify Role`!='Principal Investigator' & temp_site_staff[j,]$Phone=="")
      {
        temp_flag=append(temp_flag,"Phone Number Missing; ")
        temp_site_staff[j,]$Phone=temp_site_staff[j,]$`site Phone`
      }
      
      #check Fax
      if(temp_site_staff[j,]$`Specify Role`!='Principal Investigator' & temp_site_staff[j,]$Fax=="")
      {
        temp_flag=append(temp_flag,"Fax Number Missing; ")
        temp_site_staff[j,]$Fax=temp_site_staff[j,]$`site Fax`
      }
      
      
      #check Site Name
      if(temp_site_staff[j,]$`Specify Role`!='Principal Investigator' & temp_site_staff[j,]$`Site Name`=="")
      {
        temp_flag=append(temp_flag,"Site Name Missing; ")
        temp_site_staff[j,]$`Site Name`=temp_site_staff[j,]$`site Site Name`
      }
      
      #check Address 1
      if(temp_site_staff[j,]$`Specify Role`!='Principal Investigator' & temp_site_staff[j,]$`Address 1`=="")
      {
        temp_flag=append(temp_flag,"Address 1 Missing; ")
        temp_site_staff[j,]$`Address 1`=temp_site_staff[j,]$`site Address 1`
      }
      
      #check Address 2
      if(temp_site_staff[j,]$`Specify Role`!='Principal Investigator' & temp_site_staff[j,]$`Address 2`=="")
      {
        temp_flag=append(temp_flag,"Address 2 Missing; ")
        temp_site_staff[j,]$`Address 2`=temp_site_staff[j,]$`site Address 2`
      }
      
      #check City
      if(temp_site_staff[j,]$`Specify Role`!='Principal Investigator' & temp_site_staff[j,]$City=="")
      {
        temp_flag=append(temp_flag,"City Missing; ")
        temp_site_staff[j,]$City=temp_site_staff[j,]$`site City`
      }
      
      #check State/Province
      if(temp_site_staff[j,]$`Specify Role`!='Principal Investigator' & temp_site_staff[j,]$`State/Province`=="")
      {
        temp_flag=append(temp_flag,"State Missing; ")
        temp_site_staff[j,]$`State/Province`=temp_site_staff[j,]$`site State/Province`
      }
      
      #check zip/postal code
      if(temp_site_staff[j,]$`Specify Role`!='Principal Investigator' & temp_site_staff[j,]$`Zip/Postal Code`=="")
      {
        temp_flag=append(temp_flag,"Zipcode Missing; ")
        temp_site_staff[j,]$`Zip/Postal Code`=temp_site_staff[j,]$`site Zip/Postal Code`
      }
      
      #check country
      if(temp_site_staff[j,]$`Specify Role`!='Principal Investigator' & temp_site_staff[j,]$Country=="")
      {
        temp_flag=append(temp_flag,"Country Missing; ")
        temp_site_staff[j,]$Country=temp_site_staff[j,]$`site Country`
      }
      
      #check Covance checkbox
      if(('Yes' %in% temp_site_staff$`Covance e-Site Access`+'Yes' %in% temp_site_staff$`Covance Lab Reports` +'Yes' %in% temp_site_staff$`Covance Lab Supplies`)!=3)
      {
        if('Study Coordinator' %in% temp_site_staff$`Specify Role`)
        {
          temp_flag=append(temp_flag,"Covance checkboxes haven't checked completely. Force SC to check it; ")
          temp_site_staff[temp_site_staff$`Specify Role`=='Study Coordinator',c(34,35,36)]="Yes"
        }else
          {
            temp_flag=append(temp_flag,"Covance checkboxes haven't checked completely. Force PI to check it; ")
            temp_site_staff[temp_site_staff$`Specify Role`=='Principal Investigator',c(34,35,36)]="Yes"
          }
        
        
      }
    }
    

    
    if(flag=="" & length(temp_flag)==0)
    {flag="Good condition"}else{
   flag=paste(flag,"Minors: ",paste(unique(temp_flag),collapse = ""),sep="")}
    
    new_report_log[i,7]=flag
    

    
    #combine temp tables 
    
    total_site_staff=rbind(total_site_staff,temp_site_staff)
   
    total_site_drug=rbind(total_site_drug,temp_site_drug)
   
    print(paste(i, " complete"))
  }
  

}
  
#check if total_site tables are exist or not  
if(is.data.frame(total_site_staff) && is.data.frame(total_site_drug))  
{
#add system date
total_site_staff$Date=toString(Sys.Date())
total_site_drug$Date=toString(Sys.Date())

#===============================all about country
combined=union(levels(total_site_staff$Country),levels(country_code$Country))

total_site_staff$`site Country`=factor(total_site_staff$`site Country`,levels=combined)
total_site_staff$Country=factor(total_site_staff$Country,levels=combined)
total_site_drug$`site Country`=factor(total_site_drug$`site Country`,levels=combined)
total_site_drug$`Drug Delivery Drug Country`=factor(total_site_drug$`Drug Delivery Drug Country`,levels=combined)


#formalize US country name

total_site_staff[grep("USA|US",total_site_staff$`site Country`),c("site Country", "Country")]="United States"

total_site_drug[grep("USA|US",total_site_drug$`Drug Delivery Drug Country`),"Drug Delivery Drug Country"]="United States"
total_site_drug[grep("USA|US",total_site_drug$`site Country`),"site Country"]="United States"
#clean country name
total_site_staff$`site Country`=unlist(lapply(as.character(total_site_staff$`site Country`), country_rename))
total_site_staff$Country=unlist(lapply(as.character(total_site_staff$Country), country_rename))


#add country code

total_site_staff=left_join(mutate(total_site_staff,Country=factor(Country,levels=combined)),
                           mutate(country_code,Country=factor(Country,levels=combined)), 
                          by=c('Country'='Country'), copy=FALSE)


#create new  5 digit siteid for drug table and staff table
new_siteid={}
for(si in 1:nrow(total_site_staff))
{
  if(nchar(as.character(total_site_staff$`site Site Number`[si]))==5)
  {
    new_siteid=append(new_siteid,paste(total_site_staff$`site Site Number`[si]))
  }
  
  if(nchar(as.character(total_site_staff$`site Site Number`[si]))==4)
  {
    new_siteid=append(new_siteid,paste("0",total_site_staff$`site Site Number`[si],sep=""))
  }
  
  if(nchar(as.character(total_site_staff$`site Site Number`[si]))==3)
  {
    new_siteid=append(new_siteid,paste("00",total_site_staff$`site Site Number`[si],sep=""))
  }
}
total_site_staff=cbind(total_site_staff,new_siteid)

new_dsiteid={}
for(sid in 1:nrow(total_site_drug))
{
  if(nchar(as.character(total_site_drug$`site Site Number`[sid]))==5)
  {
    new_dsiteid=append(new_dsiteid, paste(total_site_drug$`site Site Number`[sid]))
  }
  
  if(nchar(as.character(total_site_drug$`site Site Number`[sid]))==4)
  {
    new_dsiteid=append(new_dsiteid, paste("0", total_site_drug$`site Site Number`[sid], sep=""))
  }
  
  if(nchar(as.character(total_site_drug$`site Site Number`[sid]))==3)
  {
    new_dsiteid=append(new_dsiteid, paste("00", total_site_drug$`site Site Number`[sid], sep=""))
  }
  
}
total_site_drug=cbind(total_site_drug,new_dsiteid)
}
#========================================================
#move files to folders
for(f in 1:nrow(new_report_log))
{
  if(new_report_log$Result[f]=='Pass')
  {
    file.rename(paste(input_path,'/',new_report_log$FileName[f],sep=''), paste(finished_path,'/',new_report_log$FileName[f],sep=''))
  }else
  {
    file.rename(paste(input_path,'/',new_report_log$FileName[f],sep=''), paste(onhold_path,'/',new_report_log$FileName[f],sep=''))
  }
}

#=======================================================================================================================

  #=======================================================================================  
  #===============aggregation part========================================================  
  #=======================================================================================  
  setwd(output_path)
  aggregate_files=list.files(pattern='.xlsx$')  
  
#========================================split data into target csv
  
  
  if(is.data.frame(total_site_staff) && is.data.frame(total_site_drug))  
  {
  
list_ptcl=unique(total_site_staff$`site Protocol No`)


compare_col=function(a,b)
{
  levels(a)=unique(append(levels(a),levels(b)))
  levels(b)=unique(append(levels(b),levels(a)))
  result=ifelse(a==b,TRUE, FALSE)
  return(result)
}

for(i in 1:length(list_ptcl))
{
  temp_protocol=toString(list_ptcl[i])
  
#========Bracket_TXRS_Site User Import(1 tab)

  
  #sql_bracket_site_user=paste("select distinct Date as `Add/Update Date`, `site Country` as Country, 
  #                           `site Site Number` as SiteID, `First Name` as 'First Name',
  #                           `Last Name` as 'Last Name', 'Site User' as 'User Type',`E-mail` as Email, Fax ,Phone
  #                            from total_site_staff
  #                            where `Bracket/IWRS Notifications` like '%Yes%' and `site Protocol No`='",temp_protocol,"'",sep="")
  
  #bracket_site_user=sqldf(sql_bracket_site_user)
  if(length(grep("Bracket",colnames(total_site_staff)))>0)
  {
  new_bracket_site_user=mutate(filter(total_site_staff, grepl('Yes', total_site_staff$`Bracket/IWRS Notification`) , `site Protocol No`==temp_protocol),`User Type`='Site User')
  new_bracket_site_user=select(new_bracket_site_user,Date, `site Country`, `new_siteid`, `First Name`, `Last Name`, `User Type`, `E-mail`,Fax, Phone)
  colnames(new_bracket_site_user)=c('Add/Update Date', 'Country', 'SiteID', 'First Name', 'Last Name', 'User Type', 'Email', 'Fax', 'Phone')
#========Brackete_IXRS_Site Import(2 tabs)
  
  #sql_bracket_site_additional=paste("select distinct Date as `Add/Update Date`, `site Country` as Region, `site Site Number` as SiteID, 
  #                               case when `Specify Role` like '%Coordinator%' then 'Coordinator'
  #                                    when `Specify Role` like '%Principal Investigator%' then 'Investigaor'
  #                                    when `Specify Role` like '%Drug Delivery%' then 'Drug Delivery'
  #                                    when `Specify Role` like '%Pharmacy Technician%' then 'Drug Delivery'
  #                               else 'Coordinator'
  #                               end 'Contact Type',
  #                               `First Name` as 'Contact First Name',`Last Name` as 'Contact Last Name', `E-mail` as 'Email Address' 
  #                       from total_site_staff
  #                       where `Bracket/IWRS Access` like '%Yes%' and `site Protocol No`='",temp_protocol,"'",sep="")
  
  #bracket_site_additional=unique(sqldf(sql_bracket_site_additional))
  
  temp_new_bracket_site_additional=filter(total_site_staff,grepl('Yes',total_site_staff$`Bracket/IWRS Access`), `site Protocol No`==temp_protocol)
  temp_new_bracket_site_additional=mutate(temp_new_bracket_site_additional, 'New Role'=ifelse(grepl('Principal Investigator', `Specify Role`), 'Investigator',
                                                                                              ifelse(grepl('Drug Delivery',`Specify Role`)|grepl('Pharmacy Technician',`Specify Role`),'Drug Delivery','Coordinator')))
  new_bracket_site_additional=unique(select(temp_new_bracket_site_additional, Date, `site Country`, `new_siteid`, `New Role`, `First Name`, `Last Name`, `E-mail`))
  colnames(new_bracket_site_additional)=c('Add/Update Date', 'Region', 'SiteID', 'Contact Type', 'Contact First Name', 'Contact Last Name', 'Email Address')
  
  ##==================================================================
  #sql_bracket_site_drug=paste("select distinct Date as `Add/Update Date`, `site Country` as Country, `site Site Number` as SiteID, '' as 'Screening Status', '' as  'Randomization Status',
  #                                    '' as 'Site Type for Supply Strategy', '' as 'Threshold Resupply Status', '' as 'Predictive Resupply Status',
  #                                    `site Site Name` as Location, `site Investigator First Name` as 'Investigator First Name', `site Investigator Last Name` as 'Inverstigator Last Name',`site Address 1` as Address1,
  #                                     `site Address 2` as Address2, `site City` as City, `site State/Province` as 'State/Province', `site Zip/Postal Code` as 'Zip/Postal Code',
  #                                    '' as TimeZone, '' as TZID, '' as 'Adjust for Daylight Saving?', `site Phone` as 'Site Phone Number', `site Fax` as 'Site Fax Number', `site Email`as 'SiteEmail',
  #                                    `Drug Delivery Drug Location` as 'Drug Location', `Drug Delivery First Name` as 'Drug Delivery Contact First Name', `Drug Delivery Last Name` as 'Drug Delivery Contact Last Name',
  #                                    'Drug Delivery Drug Country' as 'DrugCountry', '' as 'Drug Delivery Address same as Site Address?', `Drug Delivery Drug Address 1` as DrugAdd1, `Drug Delivery Drug Address 2` as DrugAdd2, 
  #                                    `Drug Delivery Drug City` as DrugCity, `Drug Delivery Drug State/Province` as 'DrugState/Province', `Drug Delivery Drug Zip/Postal Code` as 'DrugZip/Postal Code', `Drug Delivery Drug Phone` as DrugPhone, 
  #                                    `Drug Delivery Drug Fax` as DrugFax, `Drug Delivery Drug E-mail` as DrugEmail,'' as 'Shipping Note'
  #                            from total_site_drug
  #                            where  `site Protocol No`='",temp_protocol,"'",sep="")
  
  #bracket_site_drug=sqldf(sql_bracket_site_drug)

  
  temp_new_bracket_site_drug=mutate(filter(total_site_drug, `site Protocol No`==temp_protocol),`Screening Status`='', `Randomization Status`='',`Site Type for Supply Strategy`='',
                                   `Threshold Resupply Status`='', `Predictive Resupply Status`='', `TimeZone`='', `TZID`='',`Adjust for Daylight Savings?`='', `Shipping Note`='',
                                   `Drug Delivery Address same as Site Address?`=ifelse(compare_col(`site Site Name`,`Drug Delivery Drug Location`) &
                                                                                  compare_col(`site Address 1`, `Drug Delivery Drug Address 1`) &
                                                                                  compare_col(`site Address 2`, `Drug Delivery Drug Address 2`) &
                                                                                  compare_col(`site City`, `Drug Delivery Drug City`), 'Yes', 'No'))
  
  new_bracket_site_drug=select(temp_new_bracket_site_drug,`Add/Update Date`=Date, Country=`site Country`, SiteID=`new_dsiteid`, `Screening Status`, `Randomization Status`,
                               `Site Type for Supply Strategy`, `Threshold Resupply Status`, `Predictive Resupply Status`, Location=`site Site Name`, `Investigator First Name`=`site Investigator First Name`,
                               `Investigator Last Name`=`site Investigator Last Name`, Address1=`site Address 1`, Address2=`site Address 2`, City=`site City`, `State/Province`=`site State/Province`,
                               `Zip/Postal Code`=`site Zip/Postal Code`, TimeZone, TZID, `Adjust for Daylight Savings?`, `Site Phone Number`=`site Phone`, `Site Fax Number`=`site Fax`, SiteEmail=`site Email`,
                               `Drug Location`=`Drug Delivery Drug Location`, `Drug Delivery Contact First Name`=`Drug Delivery First Name`, `Drug Delivery Contact Last Name`=`Drug Delivery Last Name`,
                               DrugCountry=`Drug Delivery Drug Country`, `Drug Delivery Address same as Site Address?`, DrugAdd1=`Drug Delivery Drug Address 1`, DrugAdd2=`Drug Delivery Drug Address 2`,
                               DrugCity=`Drug Delivery Drug City`, `DrugState/Province`=`Drug Delivery Drug State/Province`, `DrugZip/Postal Code`=`Drug Delivery Drug Zip/Postal Code`, DrugPhone=`Drug Delivery Drug Phone`,
                               DrugFax=`Drug Delivery Drug Fax`, DrugEmail=`Drug Delivery Drug E-mail`, `Shipping Note`)
  }
#=============================================================EDC(2 tabs)
 # sql_edc_site=paste("select distinct `site Site Number` || ' ' || `site Investigator Last Name` as 'Investigator Site Number & Name', 
 #                             `site Country` as 'Country of Site', 'Addition' as 'Type of Change', Date as 'Date of Change'
 #                      from total_site_staff
 #                      where `Specify Role` like '%Principal Investigator%' and `site Protocol No`='",temp_protocol,"'",sep="")
  
 #  edc_site_approval=sqldf(sql_edc_site)
  
  if(length(grep("EDC", colnames(total_site_staff)))>0)
  {
  new_edc_site_approval=select(mutate(filter(total_site_staff,grepl('Principal Investigator',`Specify Role`), `site Protocol No`==temp_protocol),`Type of Change`='Addition',`Investigator Site Number & Name`=paste(`site Site Number`," ",`site Investigator Last Name`,sep="")),
                                `Investigator Site Number & Name`,`Country of Site`=`site Country`, `Type of Change`, `Date of Change`=Date )
  
  ##=========================================================================
  #sql_edc_user=paste("select distinct `First Name`, '' as 'Middle Name(optional)', `Last Name`, `E-mail` as 'Email',
  #                           case when `Specify Role` like '%Principal Investigator%' then 'INV'
  #                                else 'CRC' end as 'Role in Rave System', `site Site Number` || ' ' || `site Investigator Last Name` as 'Investigator Site Number & Name', 'Addition' as 'Type of Change',
  #                           Date as 'Date of Change', '' as 'Requestor Name', '' as 'Request Completed By', '' as 'Gilead Notes'
  #                   from total_site_staff
  #                   where `Medidata/EDC Access` like '%Yes%' and `site Protocol No`='", temp_protocol,"'", sep="")
  
  # edc_user_approval=sqldf(sql_edc_user)
  
  
  new_edc_user_approval=unique(select(mutate(filter(total_site_staff, grepl('Yes', `Medidata/EDC Access`), `site Protocol No`==temp_protocol), `Middle Name(optional)`="", `Type of Change`='Addition',
                               `Requestor Name`='', `Request Completed By`='', `Gilead Notes`='',`Investigator Site Number & Name`=paste(`site Site Number`," ",`site Investigator Last Name`,sep=""),
                               `Role in Rave System`=ifelse(`Specify Role`=='Principal Investigator', 'INV', 'CRC')), `First Name`, `Middle Name(optional)`, `Last Name`, Email=`E-mail`,`Role in Rave System`,
                               `Investigator Site Number & Name`, `Type of Change`, `Data of Change`=Date, `Requestor Name`, `Request Completed By`, `Gilead Notes`))
  }
#============================ePRO/eRT  
#  sql_epro=paste("select distinct '' as Updated, Date as Added, `site Site Number` as 'Site Number',`site Investigator First Name` as 'Investigator First Name',
#                 `site Investigator Last Name` as 'Investigator Last Name',`First Name` as 'ePRO Site Admin First Name',`Last Name` as 'ePRO Site Admin Last Name', `E-mail` as `Email Address`,
#                 `Phone` as 'Contact Phone', '' as Language, '' as 'Requested Device Delivery Date', `Country`, `Address 1` as 'Address1', `Address 2` as 'Address2', `Address 3` as 'Address3',
#                  `City` , `State/Province`, `Zip/Postal Code`, '' as 'Shipped-see All Sites Tab for tracking & device information', '' as 'Additional Languages(Locales)', 
#                 '' as 'Date Scheduled for Locale Release on Device', '' as 'Actual Date Locale Added to Device', '' as 'Initial Site Admin Usename'
#                 from total_site_staff
#                 where `eRT/ePRO Shipments` like '%Yes%' and `site Protocol No`='", temp_protocol, "'", sep="")
  
#  epro_contacts=sqldf(sql_epro)
  
  if(length(grep("eRT",colnames(total_site_staff)))>0)
  {
  new_epro_contacts=select(mutate(filter(total_site_staff,grepl('Principal Investigator', `Specify Role`), `site Protocol No`==temp_protocol), Parent='CRO', `Domain Type`='Site', `Principal Investigator MIDDLE Name`='',
                                  `Site Address Line 4`='', 
                                  `State*(USA) 2 character limited` = ifelse(Country=='United States',levels(`State/Province`)[`State/Province`],''), 
                                  `Province*(Canada) 2 character limited`= ifelse(Country=='Canada',levels(`State/Province`)[`State/Province`],''),
                                  `Region`= ifelse( !Country %in% c('United States','Canada'), levels(`State/Province`)[`State/Province`],''), `Phone Extension`='', TimeZone='')
                                  , Parent,`Domain Type`, `Site Number`=`new_siteid`,
                           `Principal Investigator FIRST Name`=`site Investigator First Name`,  `Principal Investigator MIDDLE Name`,`Principal Investigator LAST Name`=`site Investigator Last Name`, 
                           `Site Company Organization Name`=`Site Name`, `Site Address Line 1`=`Address 1`, `Site Address Line 2`= `Address 2`, `Site Address Line 3`=`Address 3`, `Site Address Line 4`, 
                           City, `State*(USA) 2 character limited`, `Province*(Canada) 2 character limited`, Region, `Postal Code`=`Zip/Postal Code`, Country, `PhoneNumber`=Phone, `Phone Extension`, `FaxNumber`=Fax, TimeZone )
  }
#================================================Covance
 # test=sqldf("select distinct *,'Study Coordinator' as 'Role in Covance' from total_site_staff where `Covance e-Site Access`='Yes' or `Covance Lab Supplies`='Yes' or `Covance Lab Reports`='Yes'")
  ##create a new table for covance reqeust
  #covance_table_pi=mutate(filter(total_site_staff,`Specify Role`=='Principal Investigator'), `New Role`='Principal Investigator')
  #covance_table_sc=mutate(filter(total_site_staff,`Specify Role`!='Principal Investigator', `Covance e-Site Access`=='Yes'),`New Role`='Study Coordinator')
  #covance_table_supplies=mutate(filter(total_site_staff,`Specify Role`!='Principal Investigator',`Covance Lab Supplies`=='Yes'),`New Role`='Supplies Recipient')
  #covance_table_report=mutate(filter(total_site_staff,`Specify Role`!='Principal Investigator',`Covance Lab Reports`=='Yes'),`New Role`='Lab Report Recipient')
  
  if(length(grep("Covance",colnames(total_site_staff)))>0)
  {
  temp_staff=filter(total_site_staff,`site Protocol No`==temp_protocol)
  
  covance_staff={}
  for(s in 1:nlevels(temp_staff$`site Site Number`))
  {
    #get the rows for each site
    temp_site=filter(temp_staff,`site Site Number`==levels(temp_staff$`site Site Number`)[s])
    #get the rows for non PI staff
    temp_site_npi=filter(temp_site, `Specify Role`!='Principal Investigator')
    #test if Covance-access checked in these rows
    signal_a=length(grep('Yes',temp_site_npi$`Covance e-Site Access`))>0
    #test if Covance-supplies checked in these rows
    signal_s=length(grep('Yes',temp_site_npi$`Covance Lab Supplies`))>0
    #test if Covance-reports checked in these rows
    signal_r=length(grep('Yes', temp_site_npi$`Covance Lab Reports`))>0
    
    #logic magic begins
    #all these covance checkboxes have been checked by non-pi staff
    if(signal_a+signal_s+signal_r==3)
    {
      pi_row=mutate(filter(temp_site, `Specify Role`=='Principal Investigator'),`Covance Role`='Principal Investigator')
      
      a_row=mutate(filter(temp_site_npi, `Covance e-Site Access`=='Yes'),`Covance Role`='Study Coordinator')
      s_row=mutate(filter(temp_site_npi, `Covance Lab Supplies`=='Yes'),`Covance Role`='Supplies Recipient')
      r_row=mutate(filter(temp_site_npi, `Covance Lab Reports`=='Yes'),`Covance Role`='Lab Report Recipient')
      
      covance_temp_staff=rbind(pi_row,a_row, s_row, r_row)
    }
    #only two of these covanc checkboxes have been checked by non-pi staff
    if(signal_a+signal_s+signal_r==2)
    {
      pi_row=mutate(filter(temp_site, `Specify Role`=='Principal Investigator'),`Covance Role`='Principal Investigator')
      #check which two checked
      if(signal_a+signal_s==2)
      {
        a_row=mutate(filter(temp_site_npi, `Covance e-Site Access`=='Yes'),`Covance Role`='Study Coordinator')
        s_row=mutate(filter(temp_site_npi, `Covance Lab Supplies`=='Yes'),`Covance Role`='Supplies Recipient')
        r_row=mutate(filter(temp_site, `Specify Role`=='Principal Investigator'),`Covance Role`='Lab Report Recipient')
        
        covance_temp_staff=rbind(pi_row,a_row, s_row, r_row)
      }
      
      if(signal_a+signal_r==2)
      {
        a_row=mutate(filter(temp_site_npi, `Covance e-Site Access`=='Yes'),`Covance Role`='Study Coordinator')
        s_row=mutate(filter(temp_site, `Specify Role`=='Principal Investigator'),`Covance Role`='Supplies Recipient')
        r_row=mutate(filter(temp_site_npi, `Covance Lab Reports`=='Yes'),`Covance Role`='Lab Report Recipient')
        
        covance_temp_staff=rbind(pi_row,a_row, s_row, r_row)
      }
      
      if(signal_s+signal_r==2)
      {
        a_row=mutate(filter(temp_site, `Specify Role`=='Principal Investigator'),`Covance Role`='Study Coordinator')
        s_row=mutate(filter(temp_site_npi, `Covance Lab Supplies`=='Yes'),`Covance Role`='Supplies Recipient')
        r_row=mutate(filter(temp_site_npi, `Covance Lab Reports`=='Yes'),`Covance Role`='Lab Report Recipient')
        
        covance_temp_staff=rbind(pi_row,a_row, s_row, r_row)
      }
    }
    #only one checkbox checked by non-pi staff
    if(signal_a+signal_s+signal_r==1)
    {
      pi_row=mutate(filter(temp_site, `Specify Role`=='Principal Investigator'),`Covance Role`='Principal Investigator')
      #check which one
      if(signal_a==1)
      {
        a_row=mutate(filter(temp_site_npi, `Covance e-Site Access`=='Yes'),`Covance Role`='Study Coordinator')
        s_row=mutate(filter(temp_site, `Specify Role`=='Principal Investigator'),`Covance Role`='Supplies Recipient')
        r_row=mutate(filter(temp_site, `Specify Role`=='Principal Investigator'),`Covance Role`='Lab Report Recipient')
        
        covance_temp_staff=rbind(pi_row,a_row, s_row, r_row)
      }
      
      if(signal_s==1)
      {
        a_row=mutate(filter(temp_site, `Specify Role`=='Principal Investigator'),`Covance Role`='Study Coordinator')
        s_row=mutate(filter(temp_site_npi, `Covance Lab Supplies`=='Yes'),`Covance Role`='Supplies Recipient')
        r_row=mutate(filter(temp_site, `Specify Role`=='Principal Investigator'),`Covance Role`='Lab Report Recipient')
        
        covance_temp_staff=rbind(pi_row,a_row, s_row, r_row)
      }
      
      if(signal_r==1)
      {
        a_row=mutate(filter(temp_site, `Specify Role`=='Principal Investigator'),`Covance Role`='Study Coordinator')
        s_row=mutate(filter(temp_site, `Specify Role`=='Principal Investigator'),`Covance Role`='Supplies Recipient')
        r_row=mutate(filter(temp_site_npi, `Covance Lab Reports`=='Yes'),`Covance Role`='Lab Report Recipient')
        
        covance_temp_staff=rbind(pi_row,a_row, s_row, r_row)
      }
    }
    
    if(signal_a+signal_s+signal_r==0)
    {
      pi_row=mutate(filter(temp_site, `Specify Role`=='Principal Investigator'),`Covance Role`='Principal Investigator')
      
      a_row=mutate(filter(temp_site, `Specify Role`=='Principal Investigator'),`Covance Role`='Study Coordinator')
      s_row=mutate(filter(temp_site, `Specify Role`=='Principal Investigator'),`Covance Role`='Supplies Recipient')
      r_row=mutate(filter(temp_site, `Specify Role`=='Principal Investigator'),`Covance Role`='Lab Report Recipient')
      
      covance_temp_staff=rbind(pi_row,a_row, s_row, r_row)
    }
    covance_staff=rbind(covance_staff,covance_temp_staff)
  }
  
  new_covance=select(
                      mutate(covance_staff, `Distribution Code`='', `Title`='', `ISO Province`='',`Country Phone Code`='', `Telephone area Code`='', `Extension`='',
                     `Fax Country Code`='', `Fax area code`='', `Emergency number area code`='',`Emergency phone number`='',`Saturday Number Area Code`='',
                     `Saturday phone number`='', `Mobile Number Area Code`='', `Mobile or beeper number`='',`Sarstedt Monovette System Y N`='',
                     `Covance to arrange Dry IceY N`='', `Patient block numbers`='', `Faxing hours Start-EndTime`='', `Are you open normal office hrs? i.e 9:00 AM-5:00 PM Y N?`='',
                     `if NO at what time does the Site normally close ?`='',`Language of Manual`='', `Send Start-Up? Y N`='', `Database`='', `This Column is Internationally Blank`='',
                     `eSite Access Exceptions [Default to eSite only] Mark if eSite and Fax reporting required`=''),
              `Site Number`=`site Site Number`, `Distribution Code`, Role=`Covance Role`, Title, `Last Name`=`Last Name`, `First Name`=`First Name`, `Insititution Company`=`Site Name`,
              `DepartmentBuilding`=`Address 2`, `Street`=`Address 1`, `Postal Code`=`Zip/Postal Code`, City, `State Province`=`State/Province`, `ISO Province`, `Country`, 
              `Country Phone Code`=Code, `Telephone area Code`, `Telephone number`=Phone, Extension, `Fax Country Code`=Code, `Fax area code`, `Fax number`=Fax, `Emergency number area code`,
              `Emergency phone number`, `Saturday Number Area Code`, `Saturday phone number`, `Mobile Number Area Code`, `Mobile or beeper number`, `E-Mail`=`E-mail`,`Sarstedt Monovette System Y N`,
              `Covance to arrange Dry IceY N`,`Patient block numbers`,`Faxing hours Start-EndTime`,`Are you open normal office hrs? i.e 9:00 AM-5:00 PM Y N?`,`if NO at what time does the Site normally close ?`,
              `Language of Manual`,`Send Start-Up? Y N`,`Database`,`This Column is Internationally Blank`,`eSite Access Exceptions [Default to eSite only] Mark if eSite and Fax reporting required`)
  }
#=================================================================================  Robarts
  
  if(length(grep("Robarts",colnames(total_site_staff)))>0)
  {
  new_robarts=select(mutate(filter(total_site_staff, ((`Specify Role`=="Principal Investigator" | `Specify Role`=="Study Coordinator") & `site Protocol No`==temp_protocol) |
                                       ((`Specify Role`!="Principal Investigator" & `Specify Role`!="Study Coordinator") & `Robarts/Central Imaging Kit Shipments`=="Yes" & `site Protocol No`==temp_protocol)),
                     `Distribution Code`='', Role=ifelse(`Specify Role`=='Principal Investigator', 'Principal Investigator', ifelse(`Specify Role`=='Study Coordinator', 'Study Coordinator', 'Supplies Recipient')), 
                     Title='', `ISO Province`='', `Telephone area Code`='', Extension='', `Fax area code`=''),
              `Site number`=`site Site Number`, `Distribution Code`, Role, Title, `Last Name`, `First Name`, `Instituion Company`= `Site Name`,       
              `DepartmentBuilding`=`Address 2`, `Street`=`Address 1`, `Postal Code`=`Zip/Postal Code`, City, `State Province`=`State/Province`, `ISO Province`, `Country`, 
              `Country Phone Code`=Code, `Telephone area Code`, `Telephone number`=Phone, Extension, `Fax Country Code`=Code, `Fax area code`, `Fax number`=Fax, `E-Mail`=`E-mail`)
  }

#=======================================================================================  
    if(sum(grepl(temp_protocol, aggregate_files))>0)
    {
      if(length(grep("Bracket",colnames(total_site_staff)))>0)
      {
      Bracket_Site_User_Import_hist=read.xlsx2(paste(temp_protocol,"_Bracket_Site User Import Tracker",".xlsx",sep="") ,sheetName = "Site_User", check.names=FALSE)
      bracket_site_user_import=rbind(Bracket_Site_User_Import_hist, new_bracket_site_user)
      
      Bracket_Site_additional_hist=read.xlsx2(paste(temp_protocol, "_Bracket_Site Import Tracker",".xlsx",sep="") ,sheetName = "Additional Contacts", check.names=FALSE)
      bracket_site_additional=rbind(Bracket_Site_additional_hist, new_bracket_site_additional)
      
      Bracket_Site_drug_hist=read.xlsx2(paste(temp_protocol, "_Bracket_Site Import Tracker",".xlsx",sep="") ,sheetName = "Drug Delivery Contacts", check.names=FALSE)
      bracket_site_drug=rbind(Bracket_Site_drug_hist, new_bracket_site_drug)
      }
      
      if(length(grep("EDC", colnames(total_site_staff)))>0)
      {
      Edc_site_approval_hist=read.xlsx2(paste(temp_protocol,"_EDC_SUAW",".xlsx",sep="") ,sheetName = "Site Approval", check.names=FALSE)
      edc_site_approval=rbind(Edc_site_approval_hist, new_edc_site_approval)
      
      Edc_user_approval_hist=read.xlsx2(paste(temp_protocol,"_EDC_SUAW",".xlsx",sep="") ,sheetName = "User Approval", check.names=FALSE)
      edc_user_approval=rbind(Edc_user_approval_hist, new_edc_user_approval)
      }
      
      if(length(grep("eRT",colnames(total_site_staff)))>0)
      {
      Epro_contacts_hist=read.xlsx2(paste(temp_protocol,"_eRT_SiteBatchLoader",".xlsx",sep=""),sheetName = "ePRO", check.names=FALSE)
      epro_contacts=rbind(Epro_contacts_hist, new_epro_contacts)
      }
      
      if(length(grep("Covance",colnames(total_site_staff)))>0)
      {
      Covance_hist=read.xlsx2(paste(temp_protocol,"_Covance eSA_Investigator List Tracker", ".xlsx", sep=""),sheetName="Site Information", check.names=FALSE)
      covance=rbind(Covance_hist, new_covance)
      }
      
      if(length(grep("Robarts",colnames(total_site_staff)))>0)
      {
      Robarts_hist=read.xlsx2(paste(temp_protocol,"_Robarts_Site List Tracker", ".xlsx", sep=""),sheetName = "Site Information", check.names=FALSE)
      robarts=rbind(Robarts_hist, new_robarts)
      }
      
    }else{
      
      if(length(grep("Bracket",colnames(total_site_staff)))>0)
      {
      bracket_site_user_import= new_bracket_site_user
      
      bracket_site_additional=new_bracket_site_additional
      
      bracket_site_drug=new_bracket_site_drug
      }
      
      if(length(grep("EDC", colnames(total_site_staff)))>0)
      {
      edc_site_approval=new_edc_site_approval
      
      edc_user_approval=new_edc_user_approval
      }
      
      if(length(grep("eRT",colnames(total_site_staff)))>0)
      {
      epro_contacts=new_epro_contacts
      }
      
      if(length(grep("Covance",colnames(total_site_staff)))>0)
      {
      covance=new_covance
      }
      
      if(length(grep("Robarts",colnames(total_site_staff)))>0)
      {
      robarts=new_robarts
      }
    }

#=============================================================================================================    
  
  
  ####Bracket
  if(length(grep("Bracket",colnames(total_site_staff)))>0)
  {
  write.xlsx(bracket_site_user_import, paste(temp_protocol,"_Bracket_Site User Import Tracker",".xlsx",sep="") ,sheetName = "Site_User",append=FALSE,row.names=FALSE)
    
  write.xlsx(bracket_site_additional, paste(temp_protocol, "_Bracket_Site Import Tracker",".xlsx",sep="") ,sheetName = "Additional Contacts",append=FALSE,row.names=FALSE)
  
  write.xlsx(bracket_site_drug, paste(temp_protocol, "_Bracket_Site Import Tracker",".xlsx",sep="") ,sheetName = "Drug Delivery Contacts",append=TRUE,row.names=FALSE)
  }
  ####EDC
  if(length(grep("EDC",colnames(total_site_staff)))>0)
  {
  write.xlsx(edc_site_approval, paste(temp_protocol,"_EDC_SUAW",".xlsx",sep="") ,sheetName = "Site Approval",append=FALSE,row.names=FALSE)
  
  write.xlsx(edc_user_approval, paste(temp_protocol,"_EDC_SUAW",".xlsx",sep="") ,sheetName = "User Approval",append=TRUE,row.names=FALSE)
  }
  ###ePRO
  if(length(grep("eRT",colnames(total_site_staff)))>0)
  {
  write.xlsx(epro_contacts, paste(temp_protocol,"_eRT_SiteBatchLoader",".xlsx",sep=""),sheetName = "ePRO",append = FALSE, row.names = FALSE)
  }
  ###Covance
  if(length(grep("Covance",colnames(total_site_staff)))>0)
  {
  write.xlsx(covance, paste(temp_protocol,"_Covance eSA_Investigator List Tracker", ".xlsx", sep=""),sheetName="Site Information", append=FALSE, row.names = FALSE)
  }
  ###Robart
  if(length(grep("Robarts",colnames(total_site_staff)))>0)
  {
  write.xlsx(robarts, paste(temp_protocol,"_Robarts_Site List Tracker", ".xlsx", sep=""),sheetName = "Site Information", append = FALSE, row.names = FALSE)
  }
}
}
#=================================================================================report_log uniquesitepi&aggreate
if(sum(grepl("Report", aggregate_files))>0)
{
  report_log_his=read.xlsx2("Report_log.xlsx", sheetIndex = 1)
  report_log=rbind(report_log_his,new_report_log)
}else{report_log=new_report_log}

count_site=data.frame(table(unique(report_log[,c(3,4)])$Site))

combined_site=union(levels(report_log$Site),levels(count_site$Var1))
report_log=left_join(mutate(report_log,Site=factor(Site,levels=combined_site)),
                     mutate(count_site,Var1=factor(Var1,levels=combined_site)), 
                     by=c('Site'='Var1'), copy=FALSE)    

report_log$UniqueSitePI=ifelse(report_log$Freq>1,"No", "Yes")

report_log=report_log[,-ncol(report_log)]

report_log=report_log[order(report_log$FileName, report_log$Date),]

write.xlsx(report_log, paste("Report_log.xlsx"),sheetName = "Report",append = FALSE, row.names = FALSE)
  
}
end=Sys.time()

running_time=end-start
print(running_time)
