##Updated on 2/01/2017



library(xml2)
library(dplyr)
library(RODBC)
library(sqldf)

#===================LOAD XLSX PACKAGE
if (Sys.getenv("JAVA_HOME") != "")
  Sys.setenv(JAVA_HOME = "")
library(xlsx)
library(mailR)
#===================memory reset
rm(list = ls())
gc(reset = TRUE)

#===================start
start <- Sys.time()


setwd("//chofile/Applications/ETLKCI/ETLUserSource/Gilead/Script")
source("Main Function.R")


#===================================Main function
base.dir=c("//chofile/Applications/ETLKCI/ETLUserSource/Gilead/")
setwd(base.dir)



#locate project folder name
file.folder <- dir()

form.folder <- file.folder[-grep("Script|script|output", file.folder)]

#inicial body text message
body.msg <- ""
result <- ""
#length(form.folder)
for (pro in 1:length(form.folder))
{
  
  #if(result == "")
  
  #{
  result <- tryCatch({
    
    folder.name <- form.folder[pro]
    
    print(form.folder[pro])
    #input.path="//chofile/Applications/ETLKCI/ETLUserSource/Gilead/Gilead_Diversity_and_Selection_Study/D&S_Input_Folder"
    input.path <- paste0(
      "//chofile/Applications/ETLKCI/ETLUserSource/Gilead/",
      form.folder[pro],
      "/Input_Folder"
    )
    
    output.path <- paste0(
      "//chofile/Applications/ETLKCI/ETLUserSource/Gilead/",
      form.folder[pro],
      "/Output_Folder"
    )
    
    finished.path <- paste0(
      "//chofile/Applications/ETLKCI/ETLUserSource/Gilead/",
      form.folder[pro],
      "/Archive_Folder"
    )
    
    onhold.path <- paste0(
      "//chofile/Applications/ETLKCI/ETLUserSource/Gilead/",
      form.folder[pro],
      "/OnHold_Folder"
    )
    
    #test_path="C:/Users/hucen/GitHub/pro/python/Gilead_XML/badass"
    
    setwd(input.path)
    list.files <- list.files(pattern = '.docx$|.DOCX$')
    
    total.site.staff <- {}
    
    total.site.drug <- {}
    
    new.report.log <- {}
    
    new.report.log <- data.frame(matrix(ncol = 9, nrow = length(list.files)))
    
    colnames(new.report.log) <- c(
      "Date",
      "FileName",
      "Site",
      "PI",
      "UniqueSitePI",
      "StudyId",
      "Flag",
      "Result",
      "Status"
    )
    
    if (length(list.files) > 0)
    {
      for (i in 1:length(list.files))
      {
        temp.site.staff <- {}
        
        temp.site.drug <- {}
        
        temp.xml <- read_docx(list.files[i])
        temp.result <- LoadGileadDocx(temp.xml)
        
        if (length(temp.result$flag) == 0)
        {
          flag <- ""
        } else{
          flag <- temp.result$flag
        }
        
        
        ##for PI's name not match in differnet tables
        ##1. delect title in name
        #temp.site.staff=as.data.frame(sapply(temp.result$site_staff,function(x)
        # trim(gsub("Dr.ssa|MD|Professor|Prof.|Dr.","",x))))
        #fix the error for title MD clean, remove the state MD
        #temp.site.staff$`site State/Province`=temp.result$site_staff$`site State/Province`
        #temp.site.staff$`State/Province`=temp.result$site_staff$`State/Province`
        
        #temp.site.drug=as.data.frame(sapply(temp.result$site_drug,function(x)
        #trim(gsub("Dr.ssa|MD|Professor|Prof.|Dr.","",x))))
        #fix the error for title MD clean, remove the state MD
        #temp.site.drug$`site State/Province`=temp.result$site_drug$`site State/Province`
        #temp.site.drug$`Drug Delivery Drug State/Province`=temp.result$site_drug$`Drug Delivery Drug State/Province`
        
        
        #create temp.site.staff and temp.site.drug
        temp.site.staff <- temp.result$site_staff
        temp.site.drug <- temp.result$site_drug
        
        
        #report fill in
        new.report.log[i, 1] <- toString(Sys.time())
        new.report.log[i, 2] <- list.files[i]
        new.report.log[i, 3] <- ifelse(nrow(temp.site.staff) == 0, "Fatal Error" , unique(paste(temp.site.staff$`site Site Number`)))
        new.report.log[i, 4] <- ifelse(nrow(temp.site.staff) == 0, "Fatal Error", unique(
          paste(
            temp.site.staff$`site Investigator Last Name`,
            temp.site.staff$`site Investigator First Name`
          )
        ))
        new.report.log[i, 6] <- paste(unique(temp.site.staff$`site Protocol No`), collapse =
                                        " and ")
        
        
        diff.pi <- paste(
          "    select *
          from  `temp.site.staff`
          where `Specify Role` like '%Principal Investigator%'
          and (upper(`site Investigator Last Name`)!= upper(`Last Name`)
          or upper(`site Investigator First Name`) !=upper(`First Name`))",
          sep = ""
        )
        
        #flag protocol no checkbox erro
        if (grepl("Protocol No Checkbox fatal error or Table sturcture has been changed; ",
                  flag))
        {
          new.report.log[i, 7] <- paste("Fatal: ", flag, sep = "")
          new.report.log[i, 8] <- "On Hold"
        } else
          #flag site and drug table with different rows.
          if (nrow(temp.site.staff) == 0 | nrow(temp.site.drug) == 0)
          {
            new.report.log[i, 7] <- paste(flag,
                                          "Fatal: Site or Drug table with different rows; ",
                                          sep = "")
            new.report.log[i, 8] <- "On Hold"
          } else
            #flag file with no study id
            if (length(new.report.log[i, 6]) == 0)
            {
              new.report.log[i, 7] <- paste(flag, "Major: No Study ID; ", sep = "")
              new.report.log[i, 8] <- "On Hold"
            } else
              #flag PI's name not match in tables
              if (nrow(sqldf(diff.pi)) > 0)
              {
                new.report.log[i, 7] <- paste(flag,
                                              "Major: PI's name doesnt'match between tables; ",
                                              sep = "")
                new.report.log[i, 8] <- "On Hold"
              } else
                #flag site infomation not complete
                if (sum(temp.site.staff[, c(1:10, 13:16)] == "") > 0)
                {
                  new.report.log[i, 7] <- paste(flag, "Major: Site Information not Complete; ", sep =
                                                  "")
                  new.report.log[i, 8] <- "On Hold"
                } else
                  #flag Drug delivery info complete
                  if (sum(temp.site.drug[, c(17:23, 26:29)] == "") > 0)
                  {
                    new.report.log[i, 7] <- paste(flag, "Major: Drug Delivery info not complete; ", sep =
                                                    "")
                    new.report.log[i, 8] <- "On Hold"
                  } else
                    #flag PI has no checked checkbox value
                    if (sum(temp.site.staff[1, c(31:length(temp.site.staff))] ==
                            "No") == 8)
                    {
                      new.report.log[i, 7] <- paste(flag, "Major: PI has no checked value; ", sep =
                                                      "")
                      new.report.log[i, 8] <- "On Hold"
                    } else
                      #name not complete
                      if ((sum(temp.site.staff$`Last Name` == "") + sum(temp.site.staff$`First Name` ==
                                                                        "")) > 0)
                      {
                        new.report.log[i, 7] <- paste(flag, "Major: Staff's Name not Complete; ", sep =
                                                        "")
                        new.report.log[i, 8] <- "On Hold"
                      } else
                        #email address not complete
                        if (sum(
                          temp.site.staff$`Specify Role` != 'Principal Investigator' &
                          temp.site.staff$`E-mail` == ""
                        ) > 0)
                        {
                          new.report.log[i, 7] <- paste(flag, "Major: Staff's E-mail is missing; ", sep =
                                                          "")
                          new.report.log[i, 8] <- "On Hold"
                        } else
                          #unique email for members
                          if (length(unique(tolower(
                            paste(
                              temp.site.staff$`Last Name`,
                              " ",
                              temp.site.staff$`First Name`,
                              sep = ""
                            )
                          ))) != length(unique(tolower(temp.site.staff$`E-mail`))))
                          {
                            new.report.log[i, 7] <- paste(flag, "Major: E-mail is not unique; ", sep =
                                                            "")
                            new.report.log[i, 8] <- "On Hold"
                          } else
                            #more than one SC
                            if (sum(temp.site.staff$`Specify Role` == 'Study Coordinator') /
                                length(unique(temp.site.staff$`site Protocol No`)) > 1)
                            {
                              new.report.log[i, 7] <- paste(flag,
                                                            "Major: More than one Study Coordinators in this site; ",
                                                            sep = "")
                              new.report.log[i, 8] <- "On Hold"
                            } else
                              #multiple non PI SC memebers select Robarts
                              if (sum(
                                temp.site.staff$`Specify Role` != 'Principal Investigator' &
                                temp.site.staff$`Specify Role` != 'Study Coordinator' &
                                temp.site.staff$`Robarts/Central Imaging Kit Shipments` == 'Yes'
                              ) /
                              length(unique(temp.site.staff$`site Protocol No`)) >
                              1)
                              {
                                new.report.log[i, 7] <- paste(flag,
                                                              "Major: More than one non-PI, non-SC member have checked Robarts; ",
                                                              sep = "")
                                new.report.log[i, 8] <- "On Hold"
                              } else
                                if (nchar(unique(as.character(temp.site.staff$`site Site Number`))) >
                                    5)
                                {
                                  new.report.log[i, 7] <- paste(flag, "Major: Site Number has more than 5 digits; ", sep =
                                                                  "")
                                  new.report.log[i, 8] <- "On Hold"
                                } else
                                {
                                  new.report.log[i, 8] <- "Pass"
                                  
                                  
                                  ###########################################################
                                  #                     DATA IMPUTE AND CLEAN   for STAFF   #
                                  ###########################################################
                                  #record missing condition for blanks
                                  temp.flag <- {}
                                  
                                  #add levels to the blank columns
                                  for (lvl in 6:16)
                                  {
                                    levels(temp.site.staff[, lvl + 14]) <- append(levels(temp.site.staff[, lvl + 14]),
                                                                                  levels(temp.site.staff[, lvl]))
                                  }
                                  
                                  #add levels to checkbox
                                  for (clvl in 31:length(temp.site.staff))
                                  {
                                    levels(temp.site.staff[, clvl]) <- append(levels(temp.site.staff[, clvl]),
                                                                              c("Yes", "No", "Yes, PI Access"))
                                  }
                                  
                                  
                                  #flag minor errors for site
                                  for (j in 1:nrow(temp.site.staff))
                                  {
                                    #check phone number
                                    if (temp.site.staff[j, ]$Phone == "")
                                    {
                                      temp.flag <- append(temp.flag, "Phone Number Missing; ")
                                      temp.site.staff[j, ]$Phone <- temp.site.staff[j, ]$`site Phone`
                                    }
                                    
                                    #check Fax
                                    if (temp.site.staff[j, ]$Fax == "")
                                    {
                                      temp.flag <- append(temp.flag, "Fax Number Missing; ")
                                      temp.site.staff[j, ]$Fax <- temp.site.staff[j, ]$`site Fax`
                                    }
                                    
                                    #check email
                                    if (temp.site.staff[j, ]$`Specify Role` == 'Principal Investigator'
                                        && temp.site.staff[j, ]$`E-mail` == "")
                                    {
                                      temp.site.staff[j, ]$`E-mail` <- temp.site.staff[j, ]$`site Email`
                                    }
                                    
                                    #check Site Name
                                    if (temp.site.staff[j, ]$`Site Name` == "")
                                    {
                                      temp.flag <- append(temp.flag, "Site Name Missing; ")
                                      temp.site.staff[j, ]$`Site Name` <- temp.site.staff[j, ]$`site Site Name`
                                    }
                                    
                                    #check site Address 1
                                    if (temp.site.staff[j, ]$`Address 1` == "")
                                    {
                                      temp.flag <- append(temp.flag, "Address 1 Missing; ")
                                      temp.site.staff[j, ]$`Address 1` <- temp.site.staff[j, ]$`site Address 1`
                                    }
                                    
                                    #check site Address 2
                                    if (temp.site.staff[j, ]$`Address 2` == "")
                                    {
                                      temp.flag <- append(temp.flag, "Address 2 Missing; ")
                                      temp.site.staff[j, ]$`Address 2` <- temp.site.staff[j, ]$`site Address 2`
                                    }
                                    
                                    #check site City
                                    if (temp.site.staff[j, ]$City == "")
                                    {
                                      temp.flag <- append(temp.flag, "City Missing; ")
                                      temp.site.staff[j, ]$City <- temp.site.staff[j, ]$`site City`
                                    }
                                    
                                    #check site State/Province
                                    if (temp.site.staff[j, ]$`State/Province` == "")
                                    {
                                      temp.flag <- append(temp.flag, "State Missing; ")
                                      temp.site.staff[j, ]$`State/Province` <-
                                        temp.site.staff[j, ]$`site State/Province`
                                    }
                                    
                                    #check site zip/postal code
                                    if (temp.site.staff[j, ]$`Zip/Postal Code` ==
                                        "")
                                    {
                                      temp.flag <- append(temp.flag, "Zipcode Missing; ")
                                      temp.site.staff[j, ]$`Zip/Postal Code` <-
                                        temp.site.staff[j, ]$`site Zip/Postal Code`
                                    }
                                    
                                    #check site country
                                    if (temp.site.staff[j, ]$Country == "")
                                    {
                                      temp.flag <- append(temp.flag, "Country Missing; ")
                                      temp.site.staff[j, ]$Country <- temp.site.staff[j, ]$`site Country`
                                    }
                                    
                                    
                                    
                                    #check Covance checkbox
                                    if ((
                                      'Yes' %in% temp.site.staff$`Covance e-Site Access` + 'Yes' %in% temp.site.staff$`Covance Lab Reports` +
                                      'Yes' %in% temp.site.staff$`Covance Lab Supplies`
                                    ) != 3)
                                    {
                                      if ('Study Coordinator' %in% temp.site.staff$`Specify Role`)
                                      {
                                        temp.flag <- append(
                                          temp.flag,
                                          "Covance checkboxes haven't checked completely. Force SC to check it; "
                                        )
                                        temp.site.staff[temp.site.staff$`Specify Role` ==
                                                          'Study Coordinator', c(34, 35, 36)] <- "Yes"
                                      } else
                                      {
                                        temp.flag <- append(
                                          temp.flag,
                                          "Covance checkboxes haven't checked completely. Force PI to check it; "
                                        )
                                        temp.site.staff[temp.site.staff$`Specify Role` ==
                                                          'Principal Investigator', c(34, 35, 36)] <- "Yes"
                                      }
                                      
                                      
                                    }
                                    
                                    
                                  }
                                  
                                  
                                  #flag minor errors for drug
                                  for (k in 1:nrow(temp.site.drug))
                                  {
                                    if (temp.site.drug[k, ]$`Drug Delivery Drug Address 2` == "")
                                    {
                                      temp.flag <- append(temp.flag, "Drug Address 2 Missing; ")
                                    }
                                  }
                                  
                                  
                                  
                                  if (flag == "" &
                                      length(temp.flag) == 0)
                                  {
                                    flag <- "Good condition"
                                  } else{
                                    flag <- paste(flag, "Minors: ", paste(unique(temp.flag), collapse = ""), sep = "")
                                  }
                                  
                                  new.report.log[i, 7] <- flag
                                  
                                  if (length(grep(
                                    'Drug Delivery Drug Institution Name',
                                    colnames(total.site.drug)
                                  )))
                                  {
                                    colnames(total.site.drug)[grep('Drug Delivery Drug Institution Name',
                                                                   colnames(total.site.drug))] <- "Drug Delivery Drug Location"
                                  }
                                  
                                  if (length(grep(
                                    'Drug Delivery Drug Institution Name',
                                    colnames(temp.site.drug)
                                  )))
                                  {
                                    colnames(temp.site.drug)[grep('Drug Delivery Drug Institution Name',
                                                                  colnames(temp.site.drug))] <- "Drug Delivery Drug Location"
                                  }
                                  
                                  #combine temp tables
                                  
                                  total.site.staff <- rbind(total.site.staff, temp.site.staff)
                                  
                                  total.site.drug <- rbind(total.site.drug, temp.site.drug)
                                  
                                  print(paste(i, " complete"))
                                }
        
        
      }
      
      #check if total_site tables are exist or not
      if (is.data.frame(total.site.staff) &&
          is.data.frame(total.site.drug))
      {
        #add system date
        total.site.staff$Date <- toString(Sys.Date())
        total.site.drug$Date <- toString(Sys.Date())
        
        #===============================all about country
        combined <- union(levels(total.site.staff$Country),
                          levels(country.code$Country))
        
        total.site.staff$`site Country` <- factor(total.site.staff$`site Country`, levels =
                                                    combined)
        total.site.staff$Country <- factor(total.site.staff$Country, levels =
                                             combined)
        total.site.drug$`site Country` <- factor(total.site.drug$`site Country`, levels =
                                                   combined)
        total.site.drug$`Drug Delivery Drug Country` <- factor(total.site.drug$`Drug Delivery Drug Country`, levels =
                                                                 combined)
        
        
        #formalize US country name
        
        total.site.staff[grep("USA|US|United States of America",
                              total.site.staff$`site Country`), c("site Country", "Country")] <- "United States"
        
        total.site.drug[grep("USA|US|United States of America",
                             total.site.drug$`Drug Delivery Drug Country`), "Drug Delivery Drug Country"] <-
          "United States"
        total.site.drug[grep("USA|US|United States of America",
                             total.site.drug$`site Country`), "site Country"] <- "United States"
        #clean country name
        total.site.staff$`site Country` <- unlist(lapply(
          as.character(total.site.staff$`site Country`),
          RenameCountry
        ))
        total.site.staff$Country <- unlist(lapply(as.character(total.site.staff$Country), RenameCountry))
        
        
        #add country code
        
        total.site.staff <- left_join(
          mutate(total.site.staff, Country = factor(Country, levels = combined)),
          mutate(country.code, Country = factor(Country, levels =
                                                  combined)),
          by = c('Country' = 'Country'),
          copy = FALSE
        )
        
        
        #create new  5 digit siteid for drug table and staff table
        new.siteid <- {}
        
        for (si in 1:nrow(total.site.staff))
        {
          if (nchar(as.character(total.site.staff$`site Site Number`[si])) == 5)
          {
            new.siteid <- append(new.siteid,
                                 paste(total.site.staff$`site Site Number`[si]))
          }
          
          if (nchar(as.character(total.site.staff$`site Site Number`[si])) ==
              4)
          {
            new.siteid <- append(new.siteid,
                                 paste("0", total.site.staff$`site Site Number`[si], sep = ""))
          }
          
          if (nchar(as.character(total.site.staff$`site Site Number`[si])) ==
              3)
          {
            new.siteid <- append(new.siteid,
                                 paste("00", total.site.staff$`site Site Number`[si], sep = ""))
          }
        }
        total.site.staff <- cbind(total.site.staff, new.siteid)
        
        new.dsiteid <- {}
        for (sid in 1:nrow(total.site.drug))
        {
          if (nchar(as.character(total.site.drug$`site Site Number`[sid])) == 5)
          {
            new.dsiteid <- append(new.dsiteid,
                                  paste(total.site.drug$`site Site Number`[sid]))
          }
          
          if (nchar(as.character(total.site.drug$`site Site Number`[sid])) ==
              4)
          {
            new.dsiteid <- append(new.dsiteid,
                                  paste("0", total.site.drug$`site Site Number`[sid], sep = ""))
          }
          
          if (nchar(as.character(total.site.drug$`site Site Number`[sid])) ==
              3)
          {
            new.dsiteid <- append(new.dsiteid,
                                  paste("00", total.site.drug$`site Site Number`[sid], sep = ""))
          }
          
        }
        
        total.site.drug <- cbind(total.site.drug, new.dsiteid)
        
        
      }
      #========================================================
      #move files to folders
      for (f in 1:nrow(new.report.log))
      {
        if (new.report.log$Result[f] == 'Pass')
        {
          file.rename(
            paste(input.path, '/', new.report.log$FileName[f], sep = ''),
            paste(finished.path, '/', new.report.log$FileName[f], sep = '')
          )
        } else
        {
          file.rename(
            paste(input.path, '/', new.report.log$FileName[f], sep = ''),
            paste(onhold.path, '/', new.report.log$FileName[f], sep = '')
          )
        }
      }
      
      #=======================================================================================================================
      
      #=======================================================================================
      #===============aggregation part========================================================
      #=======================================================================================
      setwd(output.path)
      aggregate.files <- list.files(pattern = '.xlsx$')
      
      #========================================split data into target csv
      
      
      if (is.data.frame(total.site.staff) &&
          is.data.frame(total.site.drug))
      {
        list.ptcl <- unique(total.site.staff$`site Protocol No`)
        
        
        for (i in 1:length(list.ptcl))
        {
          temp.protocol <- toString(list.ptcl[i])
          
          #========Bracket_TXRS_Site User Import(1 tab)
          
          
          #sql_bracket_site_user=paste("select distinct Date as `Add/Update Date`, `site Country` as Country,
          #                           `site Site Number` as SiteID, `First Name` as 'First Name',
          #                           `Last Name` as 'Last Name', 'Site User' as 'User Type',`E-mail` as Email, Fax ,Phone
          #                            from total.site.staff
          #                            where `Bracket/IWRS Notifications` like '%Yes%' and `site Protocol No`='",temp.protocol,"'",sep="")
          
          #bracket_site_user=sqldf(sql_bracket_site_user)
          if (length(grep("Bracket", colnames(total.site.staff))) > 0)
          {
            new.bracket.site.user <- mutate(
              filter(
                total.site.staff,
                (
                  grepl('Yes', total.site.staff$`Bracket/IWRS Access`) |
                    grepl(
                      'Principal Investigator',
                      total.site.staff$`Specify Role`
                    )
                ) ,
                `site Protocol No` == temp.protocol
              ),
              `User Type` = ifelse(
                grepl('Principal Investigator', `Specify Role`),
                'Site Unblinder',
                'Site User'
              )
            )
            
            new.bracket.site.user <- select(
              new.bracket.site.user,
              Date,
              `site Country`,
              `new.siteid`,
              `First Name`,
              `Last Name`,
              `User Type`,
              `E-mail`,
              Fax,
              Phone
            )
            
            colnames(new.bracket.site.user) <- c(
              'Add/Update Date',
              'Country',
              'SiteID',
              'First Name',
              'Last Name',
              'User Type',
              'Email',
              'Fax',
              'Phone'
            )
            
            #========Brackete_IXRS_Site Import(2 tabs)
            
            #sql_bracket.site.additional=paste("select distinct Date as `Add/Update Date`, `site Country` as Region, `site Site Number` as SiteID,
            #                               case when `Specify Role` like '%Coordinator%' then 'Coordinator'
            #                                    when `Specify Role` like '%Principal Investigator%' then 'Investigaor'
            #                                    when `Specify Role` like '%Drug Delivery%' then 'Drug Delivery'
            #                                    when `Specify Role` like '%Pharmacy Technician%' then 'Drug Delivery'
            #                               else 'Coordinator'
            #                               end 'Contact Type',
            #                               `First Name` as 'Contact First Name',`Last Name` as 'Contact Last Name', `E-mail` as 'Email Address'
            #                       from total.site.staff
            #                       where `Bracket/IWRS Access` like '%Yes%' and `site Protocol No`='",temp.protocol,"'",sep="")
            
            #bracket.site.additional=unique(sqldf(sql_bracket.site.additional))
            
            temp.new.bracket.site.additional <- filter(
              total.site.staff,
              grepl(
                'Yes',
                total.site.staff$`Bracket/IWRS Notification`
              ),
              `site Protocol No` == temp.protocol,
              `Specify Role` != 'Principal Investigator'
            )
            
            temp.new.bracket.site.additional <- mutate(
              temp.new.bracket.site.additional,
              'New Role' = ifelse(
                grepl('Principal Investigator', `Specify Role`),
                'Investigator',
                ifelse(
                  grepl('Drug Delivery', `Specify Role`) |
                    grepl('Pharmacy Technician', `Specify Role`) |
                    grepl('Pharmacist', `Specify Role`),
                  'Drug Delivery',
                  'Coordinator'
                )
              )
            )
            
            new.bracket.site.additional <- unique(
              select(
                temp.new.bracket.site.additional,
                Date,
                `site Country`,
                `new.siteid`,
                `New Role`,
                `First Name`,
                `Last Name`,
                `E-mail`
              )
            )
            
            colnames(new.bracket.site.additional) <- c(
              'Add/Update Date',
              'Region',
              'SiteID',
              'Contact Type',
              'Contact First Name',
              'Contact Last Name',
              'Email Address'
            )
            
            ##==================================================================
            #sql_bracket.site.drug=paste("select distinct Date as `Add/Update Date`, `site Country` as Country, `site Site Number` as SiteID, '' as 'Screening Status', '' as  'Randomization Status',
            #                                    '' as 'Site Type for Supply Strategy', '' as 'Threshold Resupply Status', '' as 'Predictive Resupply Status',
            #                                    `site Site Name` as Location, `site Investigator First Name` as 'Investigator First Name', `site Investigator Last Name` as 'Inverstigator Last Name',`site Address 1` as Address1,
            #                                     `site Address 2` as Address2, `site City` as City, `site State/Province` as 'State/Province', `site Zip/Postal Code` as 'Zip/Postal Code',
            #                                    '' as TimeZone, '' as TZID, '' as 'Adjust for Daylight Saving?', `site Phone` as 'Site Phone Number', `site Fax` as 'Site Fax Number', `site Email`as 'SiteEmail',
            #                                    `Drug Delivery Drug Location` as 'Drug Location', `Drug Delivery First Name` as 'Drug Delivery Contact First Name', `Drug Delivery Last Name` as 'Drug Delivery Contact Last Name',
            #                                    'Drug Delivery Drug Country' as 'DrugCountry', '' as 'Drug Delivery Address same as Site Address?', `Drug Delivery Drug Address 1` as DrugAdd1, `Drug Delivery Drug Address 2` as DrugAdd2,
            #                                    `Drug Delivery Drug City` as DrugCity, `Drug Delivery Drug State/Province` as 'DrugState/Province', `Drug Delivery Drug Zip/Postal Code` as 'DrugZip/Postal Code', `Drug Delivery Drug Phone` as DrugPhone,
            #                                    `Drug Delivery Drug Fax` as DrugFax, `Drug Delivery Drug E-mail` as DrugEmail,'' as 'Shipping Note'
            #                            from total.site.drug
            #                            where  `site Protocol No`='",temp.protocol,"'",sep="")
            
            #bracket.site.drug=sqldf(sql_bracket.site.drug)
            
            
            temp.new.bracket.site.drug <- mutate(
              filter(total.site.drug, `site Protocol No` == temp.protocol),
              `Screening Status` = '',
              `Randomization Status` = '',
              `Site Type for Supply Strategy` = '',
              `Threshold Resupply Status` =
                '',
              `Predictive Resupply Status` = '',
              `TimeZone` = '',
              `TZID` = '',
              `Adjust for Daylight Savings?` = '',
              `Shipping Note` = '',
              `Drug Delivery Address same as Site Address?` =
                ifelse(
                  CompareColumnVal(`site Site Name`, `Drug Delivery Drug Location`) &
                    CompareColumnVal(`site Address 1`, `Drug Delivery Drug Address 1`) &
                    CompareColumnVal(`site Address 2`, `Drug Delivery Drug Address 2`) &
                    CompareColumnVal(`site City`, `Drug Delivery Drug City`),
                  'Yes',
                  'No'
                ),
              `siteAddress2` = ifelse(
                `site Address 3` == '',
                as.character(`site Address 2`),
                paste(`site Address 2`, `site Address 3`, sep = ", ")
              ),
              `drugAddress2` = ifelse(
                `Drug Delivery Drug Address 3` == '',
                as.character(`Drug Delivery Drug Address 2`),
                paste(
                  `Drug Delivery Drug Address 2`,
                  `Drug Delivery Drug Address 3`,
                  sep = ", "
                )
              )
            )
            
            new.bracket.site.drug <- select(
              temp.new.bracket.site.drug,
              `Add/Update Date` = Date,
              Country = `site Country`,
              SiteID = `new.dsiteid`,
              `Screening Status`,
              `Randomization Status`,
              `Site Type for Supply Strategy`,
              `Threshold Resupply Status`,
              `Predictive Resupply Status`,
              Location = `site Site Name`,
              `Investigator First Name` = `site Investigator First Name`,
              `Investigator Last Name` = `site Investigator Last Name`,
              Address1 = `site Address 1`,
              Address2 = `siteAddress2`,
              City = `site City`,
              `State/Province` = `site State/Province`,
              `Zip/Postal Code` = `site Zip/Postal Code`,
              TimeZone,
              TZID,
              `Adjust for Daylight Savings?`,
              `Site Phone Number` = `site Phone`,
              `Site Fax Number` = `site Fax`,
              SiteEmail = `site Email`,
              `Drug Location` = `Drug Delivery Drug Location`,
              `Drug Delivery Contact First Name` = `Drug Delivery First Name`,
              `Drug Delivery Contact Last Name` = `Drug Delivery Last Name`,
              DrugCountry = `Drug Delivery Drug Country`,
              `Drug Delivery Address same as Site Address?`,
              DrugAdd1 = `Drug Delivery Drug Address 1`,
              DrugAdd2 = `drugAddress2`,
              DrugCity = `Drug Delivery Drug City`,
              `DrugState/Province` = `Drug Delivery Drug State/Province`,
              `DrugZip/Postal Code` = `Drug Delivery Drug Zip/Postal Code`,
              DrugPhone = `Drug Delivery Drug Phone`,
              DrugFax = `Drug Delivery Drug Fax`,
              DrugEmail = `Drug Delivery Drug E-mail`,
              `Shipping Note`
            )
          }
          #=============================================================EDC(2 tabs)
          # sql_edc_site=paste("select distinct `site Site Number` || ' ' || `site Investigator Last Name` as 'Investigator Site Number & Name',
          #                             `site Country` as 'Country of Site', 'Addition' as 'Type of Change', Date as 'Date of Change'
          #                      from total.site.staff
          #                      where `Specify Role` like '%Principal Investigator%' and `site Protocol No`='",temp.protocol,"'",sep="")
          
          #  edc.site.approval=sqldf(sql_edc_site)
          
          if (length(grep("EDC", colnames(total.site.staff))) > 0)
          {
            new.edc.site.approval <- select(
              mutate(
                filter(
                  total.site.staff,
                  grepl('Principal Investigator', `Specify Role`),
                  `site Protocol No` == temp.protocol
                ),
                `Type of Change` = 'Addition',
                `Investigator Site Number & Name` = paste(
                  `site Site Number`,
                  " ",
                  `site Investigator Last Name`,
                  sep = ""
                )
              ),
              `Investigator Site Number & Name`,
              `Country of Site` = `site Country`,
              `Type of Change`,
              `Date of Change` = Date
            )
            
            ##=========================================================================
            #sql_edc_user=paste("select distinct `First Name`, '' as 'Middle Name(optional)', `Last Name`, `E-mail` as 'Email',
            #                           case when `Specify Role` like '%Principal Investigator%' then 'INV'
            #                                else 'CRC' end as 'Role in Rave System', `site Site Number` || ' ' || `site Investigator Last Name` as 'Investigator Site Number & Name', 'Addition' as 'Type of Change',
            #                           Date as 'Date of Change', '' as 'Requestor Name', '' as 'Request Completed By', '' as 'Gilead Notes'
            #                   from total.site.staff
            #                   where `Medidata/EDC Access` like '%Yes%' and `site Protocol No`='", temp.protocol,"'", sep="")
            
            # edc.user.approval=sqldf(sql_edc_user)
            
            
            new.edc.user.approval <- unique(
              select(
                mutate(
                  filter(
                    total.site.staff,
                    grepl('Yes', `Medidata/EDC Access`),
                    `site Protocol No` == temp.protocol
                  ),
                  `Middle Name(optional)` = "",
                  `Type of Change` = 'Addition',
                  `Requestor Name` =
                    '',
                  `Request Completed By` = '',
                  `Gilead Notes` = '',
                  `Investigator Site Number & Name` = paste(
                    `site Site Number`,
                    " ",
                    `site Investigator Last Name`,
                    sep = ""
                  ),
                  `Role in Rave System` =
                    ifelse(
                      `Specify Role` == 'Principal Investigator',
                      'INV',
                      'CRC'
                    )
                ),
                `First Name`,
                `Middle Name(optional)`,
                `Last Name`,
                Email = `E-mail`,
                `Role in Rave System`,
                `Investigator Site Number & Name`,
                `Type of Change`,
                `Data of Change` = Date,
                `Requestor Name`,
                `Request Completed By`,
                `Gilead Notes`
              )
            )
          }
          #============================ePRO/eRT
          #  sql_epro=paste("select distinct '' as Updated, Date as Added, `site Site Number` as 'Site Number',`site Investigator First Name` as 'Investigator First Name',
          #                 `site Investigator Last Name` as 'Investigator Last Name',`First Name` as 'ePRO Site Admin First Name',`Last Name` as 'ePRO Site Admin Last Name', `E-mail` as `Email Address`,
          #                 `Phone` as 'Contact Phone', '' as Language, '' as 'Requested Device Delivery Date', `Country`, `Address 1` as 'Address1', `Address 2` as 'Address2', `Address 3` as 'Address3',
          #                  `City` , `State/Province`, `Zip/Postal Code`, '' as 'Shipped-see All Sites Tab for tracking & device information', '' as 'Additional Languages(Locales)',
          #                 '' as 'Date Scheduled for Locale Release on Device', '' as 'Actual Date Locale Added to Device', '' as 'Initial Site Admin Usename'
          #                 from total.site.staff
          #                 where `eRT/ePRO Shipments` like '%Yes%' and `site Protocol No`='", temp.protocol, "'", sep="")
          
          #  epro.contacts=sqldf(sql_epro)
          
          if (length(grep("eRT", colnames(total.site.staff))) > 0)
          {
            new.epro.contacts <- select(
              mutate(
                filter(
                  total.site.staff,
                  grepl('Principal Investigator', `Specify Role`),
                  `site Protocol No` == temp.protocol
                ),
                Parent = 'CRO',
                `Domain Type` = 'Site',
                `Principal Investigator MIDDLE Name` = '',
                `Site Address Line 3` = '',
                `Site Address Line 4` = '',
                `State*(USA) 2 character limited` = ifelse(
                  Country == 'United States',
                  levels(`State/Province`)[`State/Province`],
                  ''
                ),
                `Province*(Canada) 2 character limited` = ifelse(Country ==
                                                                   'Canada', levels(`State/Province`)[`State/Province`], ''),
                `Region` = ifelse(
                  !Country %in% c('United States', 'Canada'),
                  levels(`State/Province`)[`State/Province`],
                  ''
                ),
                `Phone Extension` = '',
                TimeZone = '',
                `siteAddress2` = ifelse(
                  `site Address 3` == '',
                  as.character(`site Address 2`),
                  paste(`site Address 2`, `site Address 3`, sep = ", ")
                )
              )
              ,
              Parent,
              `Domain Type`,
              `Site Number` = `new.siteid`,
              `Principal Investigator FIRST Name` =
                `site Investigator First Name`,
              `Principal Investigator MIDDLE Name`,
              `Principal Investigator LAST Name` = `site Investigator Last Name`,
              `Site Company Organization Name` = `Site Name`,
              `Site Address Line 1` = `site Address 1`,
              `Site Address Line 2` = `siteAddress2`,
              `Site Address Line 3`,
              `Site Address Line 4`,
              City,
              `State*(USA) 2 character limited`,
              `Province*(Canada) 2 character limited`,
              Region,
              `Postal Code` = `Zip/Postal Code`,
              Country,
              `PhoneNumber` = Phone,
              `Phone Extension`,
              `FaxNumber` = Fax,
              TimeZone
            )
          }
          #================================================Covance
          # test=sqldf("select distinct *,'Study Coordinator' as 'Role in Covance' from total.site.staff where `Covance e-Site Access`='Yes' or `Covance Lab Supplies`='Yes' or `Covance Lab Reports`='Yes'")
          ##create a new table for covance reqeust
          #covance_table_pi=mutate(filter(total.site.staff,`Specify Role`=='Principal Investigator'), `New Role`='Principal Investigator')
          #covance_table_sc=mutate(filter(total.site.staff,`Specify Role`!='Principal Investigator', `Covance e-Site Access`=='Yes'),`New Role`='Study Coordinator')
          #covance_table_supplies=mutate(filter(total.site.staff,`Specify Role`!='Principal Investigator',`Covance Lab Supplies`=='Yes'),`New Role`='Supplies Recipient')
          #covance_table_report=mutate(filter(total.site.staff,`Specify Role`!='Principal Investigator',`Covance Lab Reports`=='Yes'),`New Role`='Lab Report Recipient')
          
          if (length(grep("Covance", colnames(total.site.staff))) > 0)
          {
            temp.staff <- filter(total.site.staff, `site Protocol No` == temp.protocol)
            
            covance.staff <- {}
            
            for (s in 1:nlevels(temp.staff$`site Site Number`))
            {
              #get the rows for each site
              temp.site <- filter(temp.staff,
                                  `site Site Number` == levels(temp.staff$`site Site Number`)[s])
              #get the rows for non PI staff
              temp.site.npi <- filter(temp.site,
                                      `Specify Role` != 'Principal Investigator')
              #test if Covance-access checked in these rows
              signal.a <- length(grep('Yes', temp.site.npi$`Covance e-Site Access`)) > 0
              #test if Covance-supplies checked in these rows
              signal.s <- length(grep('Yes', temp.site.npi$`Covance Lab Supplies`)) > 0
              #test if Covance-reports checked in these rows
              signal.r <- length(grep('Yes', temp.site.npi$`Covance Lab Reports`)) > 0
              
              #logic magic begins
              #all these covance checkboxes have been checked by non-pi staff
              if (signal.a + signal.s + signal.r == 3)
              {
                pi.row <- mutate(
                  filter(
                    temp.site,
                    `Specify Role` == 'Principal Investigator'
                  ),
                  `Covance Role` = 'Principal Investigator'
                )
                
                a.row <- mutate(
                  filter(temp.site.npi, `Covance e-Site Access` == 'Yes'),
                  `Covance Role` = 'Study Coordinator'
                )
                
                s.row <- mutate(filter(temp.site.npi, `Covance Lab Supplies` ==
                                         'Yes'),
                                `Covance Role` = 'Supplies Recipient')
                
                r.row <- mutate(filter(temp.site.npi, `Covance Lab Reports` ==
                                         'Yes'),
                                `Covance Role` = 'Lab Report Recipient')
                
                covance.temp.staff <- rbind(pi.row, a.row, s.row, r.row)
              }
              #only two of these covanc checkboxes have been checked by non-pi staff
              if (signal.a + signal.s + signal.r == 2)
              {
                pi.row <- mutate(
                  filter(
                    temp.site,
                    `Specify Role` == 'Principal Investigator'
                  ),
                  `Covance Role` = 'Principal Investigator'
                )
                #check which two checked
                if (signal.a + signal.s == 2)
                {
                  a.row <- mutate(
                    filter(temp.site.npi, `Covance e-Site Access` == 'Yes'),
                    `Covance Role` = 'Study Coordinator'
                  )
                  
                  s.row <- mutate(
                    filter(temp.site.npi, `Covance Lab Supplies` == 'Yes'),
                    `Covance Role` = 'Supplies Recipient'
                  )
                  
                  r.row <- mutate(
                    filter(
                      temp.site,
                      `Specify Role` == 'Principal Investigator'
                    ),
                    `Covance Role` = 'Lab Report Recipient'
                  )
                  
                  covance.temp.staff <- rbind(pi.row, a.row, s.row, r.row)
                }
                
                if (signal.a + signal.r == 2)
                {
                  a.row <- mutate(
                    filter(temp.site.npi, `Covance e-Site Access` == 'Yes'),
                    `Covance Role` = 'Study Coordinator'
                  )
                  s.row <- mutate(
                    filter(
                      temp.site,
                      `Specify Role` == 'Principal Investigator'
                    ),
                    `Covance Role` = 'Supplies Recipient'
                  )
                  r.row <- mutate(
                    filter(temp.site.npi, `Covance Lab Reports` == 'Yes'),
                    `Covance Role` = 'Lab Report Recipient'
                  )
                  
                  covance.temp.staff <- rbind(pi.row, a.row, s.row, r.row)
                }
                
                if (signal.s + signal.r == 2)
                {
                  a.row <- mutate(
                    filter(
                      temp.site,
                      `Specify Role` == 'Principal Investigator'
                    ),
                    `Covance Role` = 'Study Coordinator'
                  )
                  s.row <- mutate(
                    filter(temp.site.npi, `Covance Lab Supplies` == 'Yes'),
                    `Covance Role` = 'Supplies Recipient'
                  )
                  r.row <- mutate(
                    filter(temp.site.npi, `Covance Lab Reports` == 'Yes'),
                    `Covance Role` = 'Lab Report Recipient'
                  )
                  
                  covance.temp.staff <- rbind(pi.row, a.row, s.row, r.row)
                }
              }
              #only one checkbox checked by non-pi staff
              if (signal.a + signal.s + signal.r == 1)
              {
                pi.row <- mutate(
                  filter(
                    temp.site,
                    `Specify Role` == 'Principal Investigator'
                  ),
                  `Covance Role` = 'Principal Investigator'
                )
                #check which one
                if (signal.a == 1)
                {
                  a.row <- mutate(
                    filter(temp.site.npi, `Covance e-Site Access` == 'Yes'),
                    `Covance Role` = 'Study Coordinator'
                  )
                  
                  s.row <- mutate(
                    filter(
                      temp.site,
                      `Specify Role` == 'Principal Investigator'
                    ),
                    `Covance Role` = 'Supplies Recipient'
                  )
                  
                  r.row <- mutate(
                    filter(
                      temp.site,
                      `Specify Role` == 'Principal Investigator'
                    ),
                    `Covance Role` = 'Lab Report Recipient'
                  )
                  
                  covance.temp.staff <- rbind(pi.row, a.row, s.row, r.row)
                }
                
                if (signal.s == 1)
                {
                  a.row <- mutate(
                    filter(
                      temp.site,
                      `Specify Role` == 'Principal Investigator'
                    ),
                    `Covance Role` = 'Study Coordinator'
                  )
                  
                  s.row <- mutate(
                    filter(temp.site.npi, `Covance Lab Supplies` == 'Yes'),
                    `Covance Role` = 'Supplies Recipient'
                  )
                  
                  r.row <- mutate(
                    filter(
                      temp.site,
                      `Specify Role` == 'Principal Investigator'
                    ),
                    `Covance Role` = 'Lab Report Recipient'
                  )
                  
                  covance.temp.staff <- rbind(pi.row, a.row, s.row, r.row)
                }
                
                if (signal.r == 1)
                {
                  a.row <- mutate(
                    filter(
                      temp.site,
                      `Specify Role` == 'Principal Investigator'
                    ),
                    `Covance Role` = 'Study Coordinator'
                  )
                  
                  s.row <- mutate(
                    filter(
                      temp.site,
                      `Specify Role` == 'Principal Investigator'
                    ),
                    `Covance Role` = 'Supplies Recipient'
                  )
                  
                  r.row <- mutate(
                    filter(temp.site.npi, `Covance Lab Reports` == 'Yes'),
                    `Covance Role` = 'Lab Report Recipient'
                  )
                  
                  covance.temp.staff <- rbind(pi.row, a.row, s.row, r.row)
                }
              }
              
              if (signal.a + signal.s + signal.r == 0)
              {
                pi.row <- mutate(
                  filter(
                    temp.site,
                    `Specify Role` == 'Principal Investigator'
                  ),
                  `Covance Role` = 'Principal Investigator'
                )
                
                a.row <- mutate(
                  filter(
                    temp.site,
                    `Specify Role` == 'Principal Investigator'
                  ),
                  `Covance Role` = 'Study Coordinator'
                )
                
                s.row <- mutate(
                  filter(
                    temp.site,
                    `Specify Role` == 'Principal Investigator'
                  ),
                  `Covance Role` = 'Supplies Recipient'
                )
                
                r.row <- mutate(
                  filter(
                    temp.site,
                    `Specify Role` == 'Principal Investigator'
                  ),
                  `Covance Role` = 'Lab Report Recipient'
                )
                
                covance.temp.staff <- rbind(pi.row, a.row, s.row, r.row)
              }
              covance.staff <- rbind(covance.staff, covance.temp.staff)
            }
            
            new.covance <- select(
              mutate(
                covance.staff,
                `Distribution Code` = '',
                `Title` = '',
                `ISO Province` = '',
                `Country Phone Code` = '',
                `Telephone area Code` = '',
                `Extension` = '',
                `Fax Country Code` = '',
                `Fax area code` = '',
                `Emergency number area code` = '',
                `Emergency phone number` = '',
                `Saturday Number Area Code` = '',
                `Saturday phone number` = '',
                `Mobile Number Area Code` = '',
                `Mobile or beeper number` = '',
                `Sarstedt Monovette System Y N` = '',
                `Covance to arrange Dry IceY N` = '',
                `Patient block numbers` = '',
                `Faxing hours Start-EndTime` = '',
                `Are you open normal office hrs? i.e 9:00 AM-5:00 PM Y N?` = '',
                `if NO at what time does the Site normally close ?` = '',
                `Language of Manual` = '',
                `Send Start-Up? Y N` = '',
                `Database` = '',
                `This Column is Internationally Blank` = '',
                `eSite Access Exceptions [Default to eSite only] Mark if eSite and Fax reporting required` = '',
                `siteAddress2` = ifelse(
                  `site Address 3` == '',
                  as.character(`site Address 2`),
                  paste(`site Address 2`, `site Address 3`, sep = ", ")
                )
              ),
              `Site Number` = `site Site Number`,
              `Distribution Code`,
              Role = `Covance Role`,
              Title,
              `Last Name` = `Last Name`,
              `First Name` = `First Name`,
              `Insititution Company` = `Site Name`,
              `DepartmentBuilding` = `siteAddress2`,
              `Street` = `site Address 1`,
              `Postal Code` = `Zip/Postal Code`,
              City,
              `State Province` = `State/Province`,
              `ISO Province`,
              `Country` = `Abbreviations`,
              `Country Phone Code` = Code,
              `Telephone area Code`,
              `Telephone number` = Phone,
              Extension,
              `Fax Country Code` = Code,
              `Fax area code`,
              `Fax number` = Fax,
              `Emergency number area code`,
              `Emergency phone number`,
              `Saturday Number Area Code`,
              `Saturday phone number`,
              `Mobile Number Area Code`,
              `Mobile or beeper number`,
              `E-Mail` = `E-mail`,
              `Sarstedt Monovette System Y N`,
              `Covance to arrange Dry IceY N`,
              `Patient block numbers`,
              `Faxing hours Start-EndTime`,
              `Are you open normal office hrs? i.e 9:00 AM-5:00 PM Y N?`,
              `if NO at what time does the Site normally close ?`,
              `Language of Manual`,
              `Send Start-Up? Y N`,
              `Database`,
              `This Column is Internationally Blank`,
              `eSite Access Exceptions [Default to eSite only] Mark if eSite and Fax reporting required`
            )
            
          }
          #=================================================================================  Robarts
          
          if (length(grep("Robarts", colnames(total.site.staff))) > 0)
          {
            new.robarts <- {}
            
            for (r in 1:nlevels(total.site.staff$`site Site Number`))
            {
              temp.site.robarts <- filter(
                total.site.staff,
                `site Site Number` == levels(total.site.staff$`site Site Number`)[r]
              )
              
              if (sum(
                temp.site.robarts$`Specify Role` != 'Principal Investigator' &
                temp.site.robarts$`Specify Role` != 'Study Coordinator' &
                temp.site.robarts$`Robarts/Central Imaging Kit Shipments` == 'Yes'
              )
              &
              sum(
                temp.site.robarts$`Specify Role` == 'Study Coordinator' &
                temp.site.robarts$`Robarts/Central Imaging Kit Shipments` == 'No'
              ))
              {
                temp.robarts <- select(
                  mutate(
                    filter(
                      temp.site.robarts,
                      ((
                        `Specify Role` == "Principal Investigator" |
                          `Specify Role` == "Study Coordinator"
                      ) & `site Protocol No` == temp.protocol
                      ) |
                        ((
                          `Specify Role` != "Principal Investigator" &
                            `Specify Role` != "Study Coordinator"
                        ) &
                          `Robarts/Central Imaging Kit Shipments` == "Yes" &
                          `site Protocol No` == temp.protocol
                        )
                    ),
                    `Distribution Code` = '',
                    Role = ifelse(
                      `Specify Role` == 'Principal Investigator',
                      'Principal Investigator',
                      ifelse(
                        `Specify Role` == 'Study Coordinator',
                        'Study Coordinator',
                        'Supplies Recipient'
                      )
                    ),
                    Title = '',
                    `ISO Province` = '',
                    `Telephone area Code` = '',
                    Extension = '',
                    `Fax area code` = ''
                  ),
                  `Site number` = `site Site Number`,
                  `Distribution Code`,
                  Role,
                  Title,
                  `Last Name`,
                  `First Name`,
                  `Instituion Company` = `Site Name`,
                  `DepartmentBuilding` = `Address 2`,
                  `Street` = `Address 1`,
                  `Postal Code` = `Zip/Postal Code`,
                  City,
                  `State Province` = `State/Province`,
                  `ISO Province`,
                  `Country`,
                  `Country Phone Code` = Code,
                  `Telephone area Code`,
                  `Telephone number` = Phone,
                  Extension,
                  `Fax Country Code` = Code,
                  `Fax area code`,
                  `Fax number` = Fax,
                  `E-Mail` = `E-mail`
                )
              } else{
                temp.robarts = select(
                  mutate(
                    filter(
                      temp.site.robarts,
                      ((
                        `Specify Role` == "Principal Investigator" |
                          `Specify Role` == "Study Coordinator"
                      ) & `site Protocol No` == temp.protocol
                      )
                    ),
                    `Distribution Code` = '',
                    Role = ifelse(
                      `Specify Role` == 'Principal Investigator',
                      'Principal Investigator',
                      ifelse(
                        `Specify Role` == 'Study Coordinator',
                        'Study Coordinator',
                        'Supplies Recipient'
                      )
                    ),
                    Title = '',
                    `ISO Province` = '',
                    `Telephone area Code` = '',
                    Extension = '',
                    `Fax area code` = '',
                    `siteAddress2` = ifelse(
                      `site Address 3` == '',
                      as.character(`site Address 2`),
                      paste(`site Address 2`, `site Address 3`, sep = ", ")
                    )
                  ),
                  `Site number` = `site Site Number`,
                  `Distribution Code`,
                  Role,
                  Title,
                  `Last Name`,
                  `First Name`,
                  `Instituion Company` = `Site Name`,
                  `DepartmentBuilding` = `siteAddress2`,
                  `Street` = `site Address 1`,
                  `Postal Code` = `Zip/Postal Code`,
                  City,
                  `State Province` = `State/Province`,
                  `ISO Province`,
                  `Country`,
                  `Country Phone Code` = Code,
                  `Telephone area Code`,
                  `Telephone number` = Phone,
                  Extension,
                  `Fax Country Code` = Code,
                  `Fax area code`,
                  `Fax number` = Fax,
                  `E-Mail` = `E-mail`
                )
                
                temp.supplies = select(
                  mutate(
                    filter(
                      temp.site.robarts,
                      `Specify Role` == 'Study Coordinator' &
                        `site Protocol No` == temp.protocol
                    ),
                    `Distribution Code` = '',
                    Role = 'Supplies Recipient',
                    Title = '',
                    `ISO Province` = '',
                    `Telephone area Code` = '',
                    Extension = '',
                    `Fax area code` = '',
                    `siteAddress2` = ifelse(
                      `site Address 3` == '',
                      as.character(`site Address 2`),
                      paste(`site Address 2`, `site Address 3`, sep = ", ")
                    )
                  ),
                  `Site number` = `site Site Number`,
                  `Distribution Code`,
                  Role,
                  Title,
                  `Last Name`,
                  `First Name`,
                  `Instituion Company` = `Site Name`,
                  `DepartmentBuilding` = `siteAddress2`,
                  `Street` = `site Address 1`,
                  `Postal Code` = `Zip/Postal Code`,
                  City,
                  `State Province` = `State/Province`,
                  `ISO Province`,
                  `Country`,
                  `Country Phone Code` = Code,
                  `Telephone area Code`,
                  `Telephone number` = Phone,
                  Extension,
                  `Fax Country Code` = Code,
                  `Fax area code`,
                  `Fax number` = Fax,
                  `E-Mail` = `E-mail`
                )
                
                temp.robarts <- rbind(temp.robarts, temp.supplies)
                
              }
              new.robarts <- rbind(new.robarts, temp.robarts)
            }
          }
          
          #=======================================================================================
          if (sum(grepl(temp.protocol, aggregate.files)) > 0)
          {
            if (length(grep("Bracket", colnames(total.site.staff))) > 0)
            {
              bracket.site.user.import.hist <- read.xlsx2(
                paste(
                  temp.protocol,
                  "_Bracket_Site User Import Tracker",
                  ".xlsx",
                  sep = ""
                ) ,
                sheetName = "Site_User",
                check.names = FALSE
              )
              bracket.site.user.import <- rbind(bracket.site.user.import.hist,
                                                new.bracket.site.user)
              
              
              bracket.site.additional.hist <- tryCatch(
                read.xlsx2(
                  paste(
                    temp.protocol,
                    "_Bracket_Site Import Tracker",
                    ".xlsx",
                    sep = ""
                  ) ,
                  sheetName = "Additional Contacts",
                  check.names = FALSE
                ),
                error = function(e)
                  e
              )
              
              if (is.data.frame(bracket.site.additional.hist))
              {
                bracket.site.additional <- rbind(bracket.site.additional.hist,
                                                 new.bracket.site.additional)
              } else {
                bracket.site.additional <- new.bracket.site.additional
              }
              bracket.site.drug.hist <- read.xlsx2(
                paste(
                  temp.protocol,
                  "_Bracket_Site Import Tracker",
                  ".xlsx",
                  sep = ""
                ) ,
                sheetName = "Site Import",
                check.names = FALSE
              )
              
              bracket.site.drug <- rbind(bracket.site.drug.hist, new.bracket.site.drug)
            }
            
            if (length(grep("EDC", colnames(total.site.staff))) > 0)
            {
              edc.site.approval.hist <- read.xlsx2(
                paste(temp.protocol, "_EDC_SUAW", ".xlsx", sep = "") ,
                sheetName = "Site Approval",
                check.names = FALSE
              )
              edc.site.approval <- rbind(edc.site.approval.hist, new.edc.site.approval)
              
              edc.user.approval.hist <- read.xlsx2(
                paste(temp.protocol, "_EDC_SUAW", ".xlsx", sep = "") ,
                sheetName = "User Approval",
                check.names = FALSE
              )
              edc.user.approval <- rbind(edc.user.approval.hist, new.edc.user.approval)
            }
            
            if (length(grep("eRT", colnames(total.site.staff))) > 0)
            {
              epro.contacts.hist <- read.xlsx2(
                paste(
                  temp.protocol,
                  "_eRT_SiteBatchLoader",
                  ".xlsx",
                  sep = ""
                ),
                sheetName = "ePRO",
                check.names = FALSE
              )
              epro.contacts <- rbind(epro.contacts.hist, new.epro.contacts)
            }
            
            if (length(grep("Covance", colnames(total.site.staff))) > 0)
            {
              covance.hist <- read.xlsx2(
                paste(
                  temp.protocol,
                  "_Covance eSA_Investigator List Tracker",
                  ".xlsx",
                  sep = ""
                ),
                sheetName = "Site Information",
                check.names = FALSE
              )
              #when load history data, the fax country code turn to factor, need to transfer back to numeric first
              covance.hist$`Fax Country Code` <- as.numeric((levels(
                covance.hist$`Fax Country Code`
              )))[covance.hist$`Fax Country Code`]
              
              covance <- rbind(covance.hist, new.covance)
            }
            
            if (length(grep("Robarts", colnames(total.site.staff))) > 0)
            {
              robarts.hist <- read.xlsx2(
                paste(
                  temp.protocol,
                  "_Robarts_Site List Tracker",
                  ".xlsx",
                  sep = ""
                ),
                sheetName = "Site Information",
                check.names = FALSE
              )
              #when load history data, the fax country code turn to factor, need to transfer back to numeric first
              robarts.hist$`Fax Country Code` <- as.numeric(levels(robarts.hist$`Fax Country Code`))[robarts.hist$`Fax Country Code`]
              robarts <- rbind(robarts.hist, new.robarts)
            }
            
          } else{
            if (length(grep("Bracket", colnames(total.site.staff))) > 0)
            {
              bracket.site.user.import <- new.bracket.site.user
              
              bracket.site.additional <- new.bracket.site.additional
              
              bracket.site.drug <- new.bracket.site.drug
            }
            
            if (length(grep("EDC", colnames(total.site.staff))) > 0)
            {
              edc.site.approval <- new.edc.site.approval
              
              edc.user.approval <- new.edc.user.approval
            }
            
            if (length(grep("eRT", colnames(total.site.staff))) > 0)
            {
              epro.contacts <- new.epro.contacts
            }
            
            if (length(grep("Covance", colnames(total.site.staff))) > 0)
            {
              covance <- new.covance
            }
            
            if (length(grep("Robarts", colnames(total.site.staff))) > 0)
            {
              robarts <- new.robarts
            }
          }
          
          #=============================================================================================================
          
          
          ####Bracket
          if (length(grep("Bracket", colnames(total.site.staff))) > 0)
          {
            write.xlsx(
              bracket.site.user.import,
              paste(
                temp.protocol,
                "_Bracket_Site User Import Tracker",
                ".xlsx",
                sep = ""
              ) ,
              sheetName = "Site_User",
              append = FALSE,
              row.names = FALSE
            )
            
            write.xlsx(
              bracket.site.drug,
              paste(
                temp.protocol,
                "_Bracket_Site Import Tracker",
                ".xlsx",
                sep = ""
              ) ,
              sheetName = "Site Import",
              append = FALSE,
              row.names = FALSE
            )
            if (nrow(bracket.site.additional) > 0)
            {
              write.xlsx(
                bracket.site.additional,
                paste(
                  temp.protocol,
                  "_Bracket_Site Import Tracker",
                  ".xlsx",
                  sep = ""
                ) ,
                sheetName = "Additional Contacts",
                append = TRUE,
                row.names = FALSE
              )
            }
          }
          ####EDC
          if (length(grep("EDC", colnames(total.site.staff))) > 0)
          {
            write.xlsx(
              edc.site.approval,
              paste(temp.protocol, "_EDC_SUAW", ".xlsx", sep = "") ,
              sheetName = "Site Approval",
              append = FALSE,
              row.names = FALSE
            )
            
            write.xlsx(
              edc.user.approval,
              paste(temp.protocol, "_EDC_SUAW", ".xlsx", sep = "") ,
              sheetName = "User Approval",
              append = TRUE,
              row.names = FALSE
            )
          }
          ###ePRO
          if (length(grep("eRT", colnames(total.site.staff))) > 0)
          {
            write.xlsx(
              epro.contacts,
              paste(
                temp.protocol,
                "_eRT_SiteBatchLoader",
                ".xlsx",
                sep = ""
              ),
              sheetName = "ePRO",
              append = FALSE,
              row.names = FALSE
            )
          }
          ###Covance
          if (length(grep("Covance", colnames(total.site.staff))) > 0)
          {
            write.xlsx(
              covance,
              paste(
                temp.protocol,
                "_Covance eSA_Investigator List Tracker",
                ".xlsx",
                sep = ""
              ),
              sheetName = "Site Information",
              append = FALSE,
              row.names = FALSE
            )
          }
          ###Robart
          if (length(grep("Robarts", colnames(total.site.staff))) > 0)
          {
            write.xlsx(
              robarts,
              paste(
                temp.protocol,
                "_Robarts_Site List Tracker",
                ".xlsx",
                sep = ""
              ),
              sheetName = "Site Information",
              append = FALSE,
              row.names = FALSE
            )
          }
        }
      }
      #=================================================================================report.log uniquesitepi&aggreate
      if (sum(grepl("Report", aggregate.files)) > 0)
      {
        report.log.his <- read.xlsx2("Report_log.xlsx", sheetIndex = 1)
        report.log <- rbind(report.log.his, new.report.log)
      } else{
        report.log <- new.report.log
      }
      
      count.site <- data.frame(table(unique(report.log[, c(3, 4)])$Site))
      
      combined.site <- union(levels(report.log$Site), levels(count.site$Var1))
      
      report.log <- left_join(
        mutate(report.log, Site = factor(Site, levels = combined.site)),
        mutate(count.site, Var1 = factor(Var1, levels =
                                           combined.site)),
        by = c('Site' = 'Var1'),
        copy = FALSE
      )
      
      report.log$UniqueSitePI <- ifelse(report.log$Freq > 1, "No", "Yes")
      
      report.log <- report.log[, -ncol(report.log)]
      
      report.log <- report.log[order(report.log$FileName, report.log$Date), ]
      
      final.report <- {}
      
      #add a column to highlight current form conditions
      for (st in 1:length(unique(report.log$Site)))
      {
        #1. filter one site 2. add column status
        temp.report <- report.log[report.log$Site == unique(report.log$Site)[st],]
        temp.report$Status <- factor(temp.report$Status, levels = c("History", "Current"))
        
        temp.report$Status <- "History"
        
        if (nrow(temp.report) == 1)
        {
          temp.report$Status <- "Current"
        } else{
          temp.report$Status[nrow(temp.report)] <- "Current"
        }
        
        final.report <- rbind(final.report, temp.report)
      }
      
      final.report <- final.report[order(final.report$FileName, final.report$Date), ]
      
      write.xlsx(
        final.report,
        paste("Report_log.xlsx"),
        sheetName = "Report",
        append = FALSE,
        row.names = FALSE
      )
      
      
      temp.body.msg=paste("Gilead run for", folder.name, "Success!", 
                          length(list.files), "files have been processed.",
                          sum(new.report.log$Result=="Pass"), "files pass, ",
                          sum(new.report.log$Result=="On Hold"), "files onhold.\n")
      
    }
    if (exists("temp.body.msg"))
    {
      body.msg=paste(body.msg,temp.body.msg)
      rm(temp.body.msg)
    }
    
    #tryCatch end
  }, error = function(e) e )
  
  if(is.null(result))
  {
    result <- ""
  }
  #}
  
  
  if( result != "")
  {
    if (grepl("error|Error",as.character(result)))
    {
      body.msg <- paste(body.msg,"\n","Gilead run for", folder.name,
                        "with no forms or fail:\n", as.character(result),"\n")
    }
  }
}







#greenlight for email function, default is 1
greenlight <- 1


from <- "<Gilead-Server@prahs.com>"
to   <- "<hucen@prahs.com>"
subject <- "Gilead condition"
smtp <- "smtpgateway.prant.praintl.local"


if (nchar(body.msg) > 0 && greenlight == 1)
{
  send.mail(from=from, to=to, subject=subject, 
            body=body.msg,smtp=list(host.name = smtp))
}
#if (length(sink())==0){
#  body=paste("Run Success!")
#}else{
#  body=paste(sink())
#}


end <- Sys.time()

running.time <- end - start
print(running.time)

