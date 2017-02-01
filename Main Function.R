##Updated on 1/26/2017

library(docxtractr)


#======================================================function for trim
TrimSpaces <- function (x) {
  gsub("^\\s+|\\s+$", "", x)
}

#======================================================function for Upper and Lower country
RenameCountry <- function(x) {
  s <- strsplit(x, " ")[[1]]
  paste(toupper(substring(s, 1, 1)),
        tolower(substring(s, 2)),
        sep = "",
        collapse = " ")
  
}

#=======================================================function for a single table=============

LoadGileadTable <- function(tst_xml, tst_tb) {
  #add flag for minors wrong info
  flag <- {}
  
  ns <- tst_xml$ns
  
  tst_rows <- xml_find_all(tst_tb, "./w:tr", ns = ns)
  
  df <- bind_rows(lapply(tst_rows, function(row)
  {
    vals <- xml_text(xml_find_all(row, ".//w:tc", ns = ns), trim = TRUE)
    
    
    if (length(grep("\u2610|\u2612|FORMCHECKBOX", vals)))
    {
      #find index of the cell with checkbox value
      cb_index <- grep("\u2610|\u2612|FORMCHECKBOX", vals)
      
      #find checkbox value "1" or "0"
      if (length(grep("\u2610|\u2612", vals)))
      {
        checkbox_value <- lapply(xml_find_all(row, ".//w14:checkbox/w14:checked"),
                                function(x)
                                  grep('"1"', toString(x)))
      } else{
        checkbox_value <- lapply(xml_find_all(row, ".//w:checkBox/w:default"),
                                function(x)
                                  grep(1, toString(x)))
      }
      #find the index of checkbox with value = "1"
      checked_index <- grep(1, checkbox_value)
      
      #list of value in the cell
      lc <- TrimSpaces(unlist(strsplit(
        vals[cb_index], "\u2610|\u2612|FORMCHECKBOX"
      )))
      
      #get rid of empty value in the list
      lc_clean <- lc[lc != ""]
      
      #detect if the checkbox row is for studyid or vendor
      if (length(grep('Yes', lc_clean)))
      {
        #by default set all checkbox cell to "No"
        vals[cb_index] <- "No"
        
        #return checked checkbox value to the cell
        if (length(grep('Yes, PI Access', lc_clean[1])))
        {
          vals[ceiling((checked_index + 3) / 2)] <- lc_clean[checked_index]
        } else
        {
          vals[(checked_index + 3) / 2] <- lc_clean[checked_index]
        }
      } else
      {
        if (length(grep('All Protocols|Both Protocols', lc_clean[checked_index])))
        {
          vals[cb_index] <- paste((lc_clean[-1]), collapse = ' and ')
        } else
        {
          vals[cb_index] <- paste((lc_clean[checked_index]), collapse = ' and ')
        }
        
      }
    }
    
    names(vals) <- sprintf("v%d", 1:length(vals))
    data.frame(as.list(vals), stringsAsFactors = FALSE)
  }))
  
  #for site
  if (ncol(df) == 2 && nrow(df) == 17)
  {
    #get rid of ":"
    df[, 1] <- gsub("\\:.*", "", df[, 1])
    df <- data.frame(df, row.names = NULL, check.names = FALSE)
    colnames(df) <- c('Para', "Value")
    
    t_df <- t(df)
    colnames(t_df) <- paste("site ", t_df[1, ], sep = "")
    t_df <- data.frame(t_df, check.names = FALSE)
    t_df <- t_df[-1, ]
    t_df <- t_df[, colSums(is.na(t_df)) == 0]
    rownames(t_df) <- NULL
    extract_df <- t_df
  } else
    #for site stuff
    if (ncol(df) == 5)
    {
      t_df <- t(df)
      #get rid of explainations
      colnames(t_df) <- gsub("\\:.*", "", t_df[1, ])
      t_df <- data.frame(t_df[-1, ], row.names = NULL, check.names = FALSE)
      colnames(t_df)[1] <- "Specify Role"
      
      #removce column with no value
      t_df <- t_df[, colSums(is.na(t_df)) == 0]
      
      #remove rows if "Specify Role" has no value
      t_df <- t_df[!(t_df$`Specify Role`) == "" &
                    !(is.na(t_df$`Specify Role`)), ]
      
      #remove rows if "Name" has no value
      if (sum(t_df$`Last Name` == "") > 0 &
          sum(t_df$`First Name` == "") > 0)
      {
        flag <- paste(flag, "Site staff table roles without name, deleted; ", sep =
                       "")
        t_df <- t_df[!((t_df$`Last Name` == "") + (t_df$`First Name` == "")), ]
      }
      
      extract_df <- t_df
      #if there is exception, add flag column
      
    } else
      #drug
      if (ncol(df) == 2 && nrow(df) == 14)
      {
        t_df <- t(df)
        colnames(t_df) <- paste("Drug Delivery ", t_df[1, ], sep = "")
        t_df <- data.frame(t_df, check.names = FALSE)
        t_df <- t_df[, -1]
        
        colnames(t_df) <- gsub("\\:.*", "", colnames(t_df))
        t_df <- data.frame(t_df[-1, ], row.names = NULL, check.names = FALSE)
        rownames(t_df) <- NULL
        
        extract_df <- t_df
      } else
        #for update log
        if (ncol(df) == 4)
        {
          colnames(df) <- df[1, ]
          extract_df <- df[-1, ]
        } else{
          df[, 1] <- gsub("\\:.*", "", df[, 1])
          df <- data.frame(df, row.names = NULL, check.names = FALSE)
          extract_df <- df
        }
  
  
  
  return(list(extract_df = extract_df, flag = flag))
}

#========================================function for a single docx
LoadGileadDocx <- function(docx_xml)
{
  if (docx_tbl_count(docx_xml) < 1)
  {
    return(list())
  }
  ns <- docx_xml$ns
  
  
  
  t_site <- {}
  
  staff <- {}
  
  drug <- {}
  
  compby <- {}
  
  update_log <- {}
  
  t_site_dedu <- {}
  
  site_staff <- {}
  
  site_drug <- {}
  
  flag <- {}
  
  for (i in 1:docx_tbl_count(docx_xml))
  {
    tbl <- LoadGileadTable(docx_xml, docx_xml$tbls[i])$extract_df
    flag <- tbl$flag
    #site
    if (ncol(tbl) == 16)
    {
      t_site <- tbl
    } else
      #staff
      if (ncol(tbl) == 22 | ncol(tbl) == 21 | ncol(tbl) == 20)
      {
        if (!length(staff))
        {
          staff <- rbind(staff, tbl)
        } else{
          colnames(tbl) <- colnames(staff)
          staff <- rbind(staff, tbl)
        }
      } else
        #drug
        if (ncol(tbl) == 13)
        {
          drug <- tbl
        } else
          # completed by
          if (nrow(tbl) == 4 & ncol(tbl) == 2)
          {
            compby <- tbl
          } else
            #log
          {
            update_log <- tbl
          }
    
  }
  
  
  #==========================detect how many protocols in this word document
  protocol_no <- t_site$`site Protocol No`
  list_protocol <- TrimSpaces(unlist(strsplit(toString(protocol_no), "and")))
  
  if (length(list_protocol) == 0)
  {
    flag <- paste("Protocol No Checkbox fatal error or Table sturcture has been changed.")
  }
  
  if (length(list_protocol) > 0)
  {
    for (i in 1:length(list_protocol))
    {
      t_site$`site Protocol No` <- list_protocol[i]
      t_site_dedu <- rbind(t_site_dedu, t_site)
    }
  }
  site_staff <- merge(t_site_dedu, staff)
  site_drug <- merge(t_site_dedu, drug)
  
  return(
    list(
      site_staff = site_staff,
      site_drug = site_drug,
      compby = compby,
      update_log = update_log,
      flag = flag
    )
  )
}


#=============================== compare two columns if with the same value or not
CompareColumnVal <- function(a, b)
{
  levels(a) <- unique(append(levels(a), levels(b)))
  levels(b) <- unique(append(levels(b), levels(a)))
  result <- ifelse(a == b, TRUE, FALSE)
  return(result)
}


#Country code data table
country.code <- data.frame(
  Country = c(
    "Argentina",
    "Australia",
    "Austria",
    "Belgium",
    "Bulgaria",
    "Bahamas",
    "Brazil",
    "Canada",
    "Switzerland",
    "Chile",
    "China",
    "Colombia",
    "Costa Rica",
    "Czech Republic",
    "Germany",
    "Denmark",
    "Dominican Republic",
    "Ecuador",
    "Egypt",
    "Spain",
    "Estonia",
    "Finland",
    "France",
    "United Kingdom",
    "Greece",
    "Guatemala",
    "Hong Kong",
    "Croatia",
    "Haiti",
    "Hungary",
    "Indonesia",
    "India",
    "Ireland",
    "Iceland",
    "Israel",
    "Italy",
    "Jamaica",
    "Japan",
    "Korea",
    "Lebanon",
    "Lithuania",
    "Luxembourg",
    "Latvia",
    "Morocco",
    "Moldova",
    "Mexico",
    "Malta",
    "Montenegro",
    "Malawi",
    "Malaysia",
    "Netherlands",
    "Norway",
    "New Zealand",
    "Oman",
    "Pakistan",
    "Panama",
    "Peru",
    "Philippines",
    "Poland",
    "Puerto Rico",
    "Portugal",
    "Romania",
    "Russia",
    "Singapore",
    "Serbia",
    "Slovakia",
    "Slovenia",
    "Sweden",
    "Thailand",
    "Tunisia",
    "Turkey",
    "Taiwan",
    "Uganda",
    "Ukraine",
    "Uruguay",
    "United States",
    "Venezuela",
    "Viet Nam",
    "South Africa"
  ),
  Abbreviations = c(
    "ARG",
    "AUS",
    "AUT",
    "BEL",
    "BGR",
    "BHS",
    "BRA",
    "CAN",
    "CHE",
    "CHL",
    "CHN",
    "COL",
    "CRI",
    "CZE",
    "DEU",
    "DNK",
    "DOM",
    "ECU",
    "EGY",
    "ESP",
    "EST",
    "FIN",
    "FRA",
    "GBR",
    "GRC",
    "GTM",
    "HKG",
    "HRV",
    "HTI",
    "HUN",
    "IDN",
    "IND",
    "IRL",
    "ISL",
    "ISR",
    "ITA",
    "JAM",
    "JPN",
    "KOR",
    "LBN",
    "LTU",
    "LUX",
    "LVA",
    "MAR",
    "MDA",
    "MEX",
    "MLT",
    "MNE",
    "MWI",
    "MYS",
    "NLD",
    "NOR",
    "NZL",
    "OMN",
    "PAK",
    "PAN",
    "PER",
    "PHL",
    "POL",
    "PRI",
    "PRT",
    "ROU",
    "RUS",
    "SGP",
    "SRB",
    "SVK",
    "SVN",
    "SWE",
    "THA",
    "TUN",
    "TUR",
    "TWN",
    "UGA",
    "UKR",
    "URY",
    "USA",
    "VEN",
    "VNM",
    "ZAF"
  ),
  Code = c(
    54,
    61,
    43,
    32,
    359,
    1,
    55,
    1,
    41,
    56,
    86,
    57,
    506,
    42,
    49,
    45,
    1,
    593,
    20,
    34,
    372,
    358,
    33,
    44,
    30,
    502,
    852,
    385,
    1,
    36,
    62,
    91,
    353,
    354,
    972,
    39,
    1,
    81,
    82,
    961,
    370,
    351,
    371,
    212,
    373,
    52,
    356,
    381,
    265,
    60,
    31,
    47,
    64,
    968,
    92,
    507,
    51,
    63,
    48,
    1,
    351,
    40,
    7,
    65,
    381,
    421,
    386,
    46,
    66,
    216,
    90,
    886,
    256,
    380,
    598,
    1,
    58,
    84,
    27
  )
)

