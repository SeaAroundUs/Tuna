#
#====IATTC
#

# Load required packages and function
library(tidyverse)
library(reshape2)
source("proportion_disaggregator.R")
#--------------------------------------------------Nominal data--------------------------------------------------

#Read data
nominal_data <- read.csv("INPUT IATTC Nominal Catch For Formatting.csv", sep=",")
spp_data <- read.table("input_codes/INPUT IATTC Nominal Species Codes.txt", header=T)
gear_data <- read.table("input_codes/INPUT IATTC Gear Codes.txt", header=T, sep="\t")
country_data <- read.table("input_codes/INPUT IATTC Nominal Country Codes.txt", header=T, sep="\t")

#Exclude entries with no catch, or for years prior to 1950
nominal_data <- nominal_data[nominal_data$Year>=1950 & nominal_data$Catch!=0,]

#Match country and gear codes
combined_df <- nominal_data |> 
  left_join(gear_data, by = join_by(Gear)) |>
  left_join(country_data, by = join_by(Flag)) |> 
  left_join(spp_data, by = join_by(Species)) |>
  select(c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","Catch"))

#All countries split
#Allocate catch out from "All countries" fishing entity (FishingEntityID= 1000). 
unknown_df <- combined_df |> filter(FishingEntityID == 1000)
known_df <- combined_df |> filter(FishingEntityID!=1000)

# Match and group based on Year, TaxonKey, and Layer3GearID
grouping_cols <- c("Year","TaxonKey","Layer3GearID")
matching_result_1 <- proportional_catch(known_df, unknown_df, grouping_cols)

# Match and group based on Year, SpeciesGroupID, and Layer3GearID
grouping_cols <- c("Year", "SpeciesGroupID", "Layer3GearID")
matching_result_2 <- proportional_catch(known_df, matching_result_1$unmatched_catch, grouping_cols)

# Match and group based on Year and Layer3GearID
grouping_cols <- c("Year", "Layer3GearID")
matching_result_3 <- proportional_catch(known_df, matching_result_2$unmatched_catch, grouping_cols)

# Match and group based on Year 
grouping_cols <- c("Year")
matching_result_4 <- proportional_catch(known_df, matching_result_3$unmatched_catch, grouping_cols)

# Compile all in one df and export as csv
compiled_match <- bind_rows(known_df, matching_result_1$matched_catch, 
                            matching_result_2$matched_catch, matching_result_3$matched_catch,
                            matching_result_4$matched_catch) |> arrange(Year)

# Sanity check for if catch amount before and after formatting remains the same 
if(!all.equal(sum(compiled_match$Catch), sum(combined_df$Catch)))print("Catch is missing after formatting.")

write.csv(compiled_match, "Formatted IATTC Nominal Catch For Spatial Matching.csv", row.names = F)
#--------------------------------------------------Spatial data--------------------------------------------------

#Clear any stored data
rm(list=setdiff(ls(), "proportional_catch"))

#Read spatial catch data
#Purse Seine
iattc.psb <- read.csv("INPUT IATTC Spatial Catch For Billfish Purse Seine Formatting.csv", sep=",")
iattc.pss <- read.csv("INPUT IATTC Spatial Catch For Shark Purse Seine Formatting.csv", sep=",")
iattc.pst <- read.csv("INPUT IATTC Spatial Catch For Tuna Purse Seine Formatting.csv", sep=",")

#Longline
iattc.llt <- read.csv("INPUT IATTC Spatial Catch For Tuna and Billfish Longline Formatting.csv", sep=",")
iattc.lls <- read.csv("INPUT IATTC Spatial Catch For Shark Longline Formatting.csv", sep=",")

#Pole and Line 
iattc.pl <- read.csv("INPUT IATTC Spatial Catch For Tuna Pole and Line Formatting.csv", sep=",")

#Species Codes
iattc.psspp <- read.table("input_codes/INPUT IATTC Spatial Purse Seine Species Codes.txt", header=T)
iattc.llspp <- read.table("input_codes/INPUT IATTC Spatial Longline Species Codes.txt", header=T)
iattc.plspp <- read.table("input_codes/INPUT IATTC Spatial Pole and Line Species Codes.txt", header=T)

#Formatting
#Purse Seine Formatting
#Function to re-shape database, aggregate by year, flag, and match species codes

spp.match= function(x= iattc.psb, y= iattc.psspp)
{
  z= melt(x, id= c("Year","Flag","Lat","Lon") )
  
  names(z)[names(z)=="variable"] = "SpeciesName"
  names(z)[names(z)=="value"] = "Catch"
  
  z= z[ z$Catch!=0, ]	#Only keep entries where there is catch (i.e. catch does not equal zero)
  
  z= merge(z, y, by= c("SpeciesName"), all.x=T)	#Match species codes
  z= z[ , -c(1) ]	#Delete "SpeciesName" column (obsolete)
  
  z= aggregate(z$Catch, by= list(z$Year,z$Flag,z$Lat,z$Lon,z$TaxonKey,z$SpeciesGroupID), sum)
  
  colnames(z)= c("Year","Flag","Lat","Lon","TaxonKey","SpeciesGroupID","Catch")
  
  # Sanity Check for if catch before and after melting dataframe and assigning species name still remains the same
  original_sum <- x |> select(-c(Year, Flag, Lat, Lon)) |> sum()
  if(!all.equal(sum(z$Catch), original_sum))print("Catch differs after matching species names, double check that all species code in catch file present in species code file.")
    
  return(z)
}

iattc.psb2 <- spp.match(x=iattc.psb, y= iattc.psspp)
iattc.pss2 <- spp.match(x=iattc.pss, y= iattc.psspp)
iattc.pst2 <- spp.match(x=iattc.pst, y= iattc.psspp)

#Combine the purse seine data sets
iattc.ps <- rbind(iattc.psb2,iattc.pss2,iattc.pst2)

#Add GearGroupID, specify this is purse seine catch
iattc.ps$GearGroupID <- 41
#Add a BigCellTypeID, specify this is a 1x1 spatial scale
iattc.ps$BigCellTypeID <- 1
#Final order
iattc.ps <- iattc.ps[ , c("Year","GearGroupID","Flag","Lat","Lon","BigCellTypeID","TaxonKey","SpeciesGroupID","Catch")]

#Longline Formatting
iattc.llt2 <- spp.match(x=iattc.llt, y=iattc.llspp)
iattc.lls2 <- spp.match(x=iattc.lls, y=iattc.llspp)
#Combine the purse seine data sets
iattc.ll <- rbind(iattc.llt2,iattc.lls2)
#Add GearGroupID, specify this is longline catch
iattc.ll$GearGroupID <- 32
#Add a BigCellTypeID, specify this is a 5x5 spatial scale
iattc.ll$BigCellTypeID <- 2
#Final order
iattc.ll <- iattc.ll[ , c("Year","GearGroupID","Flag","Lat","Lon","BigCellTypeID","TaxonKey","SpeciesGroupID","Catch")]

#Pole and Line Formatting
iattc.pl <- spp.match(x=iattc.pl, y=iattc.plspp)
#Add GearGroupID, specify this is pole and line catch, gear group i.d. = 5, "Small-scale gears"
iattc.pl$GearGroupID <- 31
#Add a BigCellTypeID, specify this is a 1x1 spatial scale
iattc.pl$BigCellTypeID <- 1
#Final order
iattc.pl <- iattc.pl[ , c("Year","GearGroupID","Flag","Lat","Lon","BigCellTypeID","TaxonKey","SpeciesGroupID","Catch")]

#Combine gear data sets together
iattc.spat <- rbind(iattc.pl,iattc.ll,iattc.ps)

#Turn flags into CountryGroupID and	FishingEntityID 
#Country Codes
iattc.cou <- read.table("input_codes/INPUT IATTC Spatial Country Codes.txt", header=T)
#Merge the codes
iattc.spat <- merge(iattc.spat,iattc.cou, by= c("Flag"), all.x=T)
iattc.spat <- iattc.spat[ , -c(1) ]	#Delete "Flag" column (now obsolete)
#Make it prettier, change the order of the columns
iattc.spat <- iattc.spat[ , c("Year","FishingEntityID","CountryGroupID","GearGroupID","Lat","Lon","BigCellTypeID","TaxonKey","SpeciesGroupID","Catch")]

#Rename Lat=Y, Lon=X
names(iattc.spat)[names(iattc.spat)=="Lat"] <- "y"
names(iattc.spat)[names(iattc.spat)=="Lon"] <- "x"
#Reorder
iattc.spat <- iattc.spat[ , c("Year","FishingEntityID","CountryGroupID","GearGroupID","x","y","BigCellTypeID","TaxonKey","SpeciesGroupID","Catch")]

#Convert Lat-Lon to BigCellID
#Read cell code reference data
cellid <- read.csv("input_codes/INPUT CellTypeID.csv", sep=",")
#Match BigCellID to x,y,BigCellIDType, all.x=T shows NA values
iattc.spat <- merge(iattc.spat,cellid, by=c("x","y","BigCellTypeID"), all.x=T)

#Some data don't match, this is because it is erroneously reported on land
#Write a table so we can look at the data after
#write a table to see no matches
iattc.nomatch <- subset(iattc.spat, is.na(iattc.spat$BigCellID))
write.table(iattc.nomatch,"OUTPUT IATTC Spatial No Match.csv", sep=",", row.names=F)

#Remove the no match cells from our data
iattc.spat <- subset(iattc.spat, iattc.spat$BigCellID > 0)

#Fishing entity 1000 split
#Allocate catch out from "All countries" fishing entity (FishingEntityID= 1000). 
unknown_df= iattc.spat[iattc.spat$FishingEntityID==1000,]
known_df= iattc.spat[iattc.spat$FishingEntityID!=1000,]

# Match and group based on Year, TaxonKey, and GearGroup, and BigCellID
grouping_cols <- c("Year","TaxonKey","GearGroupID", "BigCellID")
matching_result_1 <- proportional_catch(known_df, unknown_df, grouping_cols, is_spat=TRUE)

# Match and group based on Year, TaxonKey, and GearGroup
grouping_cols <- c("Year", "TaxonKey", "GearGroupID")
matching_result_2 <- proportional_catch(known_df, matching_result_1$unmatched_catch, grouping_cols, is_spat=TRUE)

# Match and group based on Year, SpeciesGroup and GearGroup
grouping_cols <- c("Year", "GearGroupID", "SpeciesGroupID")
matching_result_3 <- proportional_catch(known_df, matching_result_2$unmatched_catch, grouping_cols, is_spat = TRUE)

# Compile all into one dataframe and export as csv
compiled_match <- bind_rows(known_df, matching_result_1$matched_catch,
                            matching_result_2$matched_catch,
                            matching_result_3$matched_catch) |> 
  arrange(Year) |> 
  select(Year, FishingEntityID, CountryGroupID,
         GearGroupID, TaxonKey, SpeciesGroupID,
         BigCellID, Catch)  |> 
  group_by(Year, FishingEntityID, CountryGroupID,
           GearGroupID, TaxonKey, SpeciesGroupID,
           BigCellID) |>
  summarise(Catch = sum(Catch), .groups = "keep")

# Sanity check for if catch amount before and after formatting remains the same 
if(!all.equal(sum(compiled_match$Catch), sum(iattc.spat$Catch)))print("Catch is missing after formatting.")

write.csv(compiled_match, "Formatted IATTC Spatial Catch For Spatial Matching.csv", row.names = F)

#=============================================================================================================
#==SECTION I: INITIAL DATA AND TRANSFORMATION
#=============================================================================================================
rm(list=ls())

#1.Read in databases of nominal and spatialized catch by ocean
nom=read.csv("Formatted IATTC Nominal Catch For Spatial Matching.csv",sep=",",header=T)	#Yearly nominal catch
spat= read.csv("Formatted IATTC Spatial Catch For Spatial Matching.csv", sep=",", header=T) #Yearly spatial catch

nom= nom[nom$Catch!=0,] #Exclude nominal records with zero catch
#nom= nom[nom$Catch>1,] #Exclude nominal catch records with less than 1 tonne of catch 

spat= spat[spat$Catch!=0,]
spat= spat[is.na(spat$BigCellID)==F,]

gearres= read.csv("input_codes/INPUT IATTC GearRestrictionTable.csv",sep=",")
areares= read.csv("input_codes/INPUT IATTC AreaRestrictionTable.csv",sep=",")

tot.nom.ct=sum(nom$Catch)	#Total catch in nominal database; this is used to compare catches after spatializing.
tot.spat.ct= sum(spat$Catch)	#Total catch in spatial database; this is not used in the routine but is available to look at.

#Add record IDs
nom$NID= 1:dim(nom)[1]

#Add ID categories everywhere
final.categ= c("NID","Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","SpatCatch","MatchID")
final.categ.names= c("NID","Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","Catch","MatchID")

#=============================================================================================================
#==SECTION II: PERFECT MATCH
#=============================================================================================================

#First match all the best-case scenarios (i.e. perfect data match)
#This doesn't need any spatial data subsetting

nom.data= nom
match.categ= list(spat$FishingEntityID, spat$CountryGroupID,spat$Year, spat$GearGroupID, spat$TaxonKey, spat$SpeciesGroupID)
merge.categ= c("FishingEntityID", "CountryGroupID","Year","GearGroupID","TaxonKey","SpeciesGroupID")
match.categ.names= c(merge.categ,"TotalCatch")
matchid= 1

#1.Sum up the total catch, by all categories, in all reported spatial cells.
ct.all= aggregate(spat$Catch,by= match.categ,sum)  #Total catch in all cells, all categories
colnames(ct.all)= match.categ.names #Add matching column names

#2.Transform reported catch per cell into proportions of the total for all cells, by all categories.
spat.all= merge(spat,ct.all,by= merge.categ, all.x=T)	#Spatialized catch total in all cells by Year/GearGroupID/TaxonKey
spat.all$Proportion= spat.all$Catch / spat.all$TotalCatch	#Proportion of catch per cell = catch / total catch in cells reported for that category combo
spat.all= spat.all[,c(merge.categ,"BigCellID","Proportion")]	#Remove the catch per cell in this dataframe (but leave the proportions)

#3.Match nominal and cell proportion catch by all categories.
new.db= merge(nom.data,spat.all,by= merge.categ,all.x=T)#Match and merge the spatialized (with proportions) and 
#nominal catch databases into a new database (db)
new.db$SpatCatch= new.db$Catch * new.db$Proportion	#New spatialized catch = nominal catch * spatial proportions
new.db$MatchID= matchid

#4.Split the new database into matched and non-matched databases (makes computation faster)
db.match= subset(new.db,is.na(BigCellID)==F)	#Separate the new database into matched (d for "data")
db.nomatch= subset(new.db,is.na(BigCellID))	#and non-matched (nd for "no data") records

#5.Check that all catch is accounted for (whether matched or not) and return proportion of catch that was not matched at this stage
print(c("Current refinement",sum(c(db.match$SpatCatch,db.nomatch$Catch)),"Nominal",tot.nom.ct))
print(c("Proportion Matched Tonnes", 1-( sum(db.nomatch$Catch)/tot.nom.ct) ) )

#6.Reset columns so that they match the original nominal and spatial databases (to avoid potential indexing screw-ups) 
db.match= db.match[,final.categ]	#Re-order columns for matched database
colnames(db.match)= final.categ.names

db.nomatch= db.nomatch[,colnames(nom)]	#Reset columns in non-matched database
rm(list=c("ct.all","spat.all","new.db","nom.data"))	#MEMORY CLEAN-UP

sum(db.match$Catch)/sum(nom$Catch)

#=============================================================================================================
#==SECTION III: 
#=============================================================================================================
#db.nomatch <- read.csv("OUTPUT IATTC No Match Catch 1 of 1.csv",header=T)

#This is the key function. Same as above but does not return the non-matched data as this is unnecessary
#given the functions below

data.match= function(nom.dataY, spatY, db.match, match.categ, merge.categ, match.categ.names, matchid )
{
  ct.all= aggregate(spatY$Catch,by= match.categ,sum)
  colnames(ct.all)= match.categ.names 
  spat.all= merge(spatY,ct.all,by= merge.categ,all.x=T)  
  spat.all$Proportion= spat.all$Catch / spat.all$TotalCatch  
  spat.all= spat.all[,c(merge.categ,"BigCellID","Proportion")]  
  new.db= merge(nom.dataY,spat.all,by= merge.categ,all.x=T)
  new.db$SpatCatch= new.db$Catch * new.db$Proportion	
  new.db$MatchID= matchid
  db.match.new= subset(new.db,is.na(BigCellID)==F)	
  #NM= subset(new.db,is.na(BigCellID))	
  #print(c("Current refinement",sum(c(db.match.new$SpatCatch,db.nomatch$Catch)),"Nominal",sum(nom.dataY$Catch)))
  db.match.new= db.match.new[,final.categ]	
  colnames(db.match.new)= final.categ.names
  db.match= rbind( db.match, db.match.new)
  #NM= NM[,colnames(nom)]	
  rm(list=c("ct.all","spat.all","new.db","db.match.new")) 
  
  return(list(db.match=db.match))
}

fids= sort(unique(nom$FishingEntityID)) #Vector of unique FishingEntityIDs in the nominal data
YRs= c(0,2,5) #Year ranges for subsetting (i.e. +/- years to match over) TC Removed 1, 10, 15 as these are saying that categories are more important than time. This will bias results towards future tuna fisheries rather than spatial expansion. 

for(i in 1:length(fids)) #Loop over countries
{
  ci= fids[i] #Test by changing index to desired FishingEntityID number
  
  nom.datai= subset(db.nomatch, FishingEntityID==ci) #Nominal data to match for a country
  
  gears= subset(gearres, FishingEntityID==ci)[,c("FishingEntityID","GearGroupID")] #Fishing gears used by that country
  if(dim(gears)[1] == 0) {gears= cbind("FishingEntityID"=ci, "GearGroupID"=unique(spat$GearGroupID))} #If no restriction, use all gears
  y= merge(spat, gears, by="GearGroupID",all.x=T) #Match country gears to spatial data
  
  cells= subset(areares, FishingEntityID==ci)[,c("FishingEntityID","BigCellID")] #Cells used by that country
  if(dim(cells)[1] == 0) {cells= cbind("FishingEntityID"=ci, "BigCellID"=unique(spat$BigCellID))} #If no restriction, use all cells
  y2= merge(y, cells, by="BigCellID",all.x=T)#Match country cells to spatial data
  
  spati= subset(y2, FishingEntityID.y==ci & FishingEntityID==ci) #Only use spatial data that matched to both the allowed country gears and cells
  spati= spati[,-c(12,13)]; colnames(spati)[7]= "FishingEntityID"#Keep necessary data and update column name UPDATE check this for errors
  
  rm(list=c("y","y2","gears","cells")) #Remove interim datasets
  
  yrs= sort(unique(nom.datai$Year)) #Unique years in the country nominal data
  
  NM= nom.datai #Non-matched data that will be used for the country. This should be the only time this nom.datai gets used each loop
  
  rni= dim(nom.datai)[1] #Rows in original country non-matched data
  
  for(z in 1:13) #Thirteen is the number of category combos in the ifelse statment below
  {

    for(j in 1:length(yrs)) #Loop over years
    {
      yrj=yrs[j] #Year j is pulled from the vector of year for that country
      
      for(w in 1:length(YRs)) #Loop over year ranges
      {
        YR=YRs[w] #Year range is pulled from the year ranges (+/- 0 to 20 year range)
        
        print("FishingEntityID"); print(i)
        
        print("Category"); print(z) #Current category for troubleshooting
        
        print("Year"); print(yrj) #Current year for troubleshooting
        
        print("Year range"); print(YR) #Current year range for troubleshooting
        
        nom.dataY= subset(NM, Year==yrj) #Only match data for a given year
        spatY= subset(spat, Year<=yrj+YR & Year>=yrj-YR ) #Only use data for a given year range
        #print("Years spatial");print(sort(unique(spatY$Year)))  #Current years in range for troubleshooting
        
        #Depending on the step z, match by decreasing number of categories (this must be updated manually)
        if(z==1){   match.categ= list(spatY$FishingEntityID, spatY$GearGroupID, spatY$TaxonKey) 
        merge.categ= c("FishingEntityID","GearGroupID","TaxonKey")
        
        } else if(z==2){   match.categ= list(spatY$FishingEntityID, spatY$GearGroupID, spatY$SpeciesGroupID) 
        merge.categ= c("FishingEntityID", "GearGroupID","SpeciesGroupID")
        
        } else if(z==3){   match.categ= list(spatY$FishingEntityID, spatY$TaxonKey) 
        merge.categ= c("FishingEntityID", "TaxonKey")
        
        } else if(z==4){   match.categ= list(spatY$FishingEntityID, spatY$SpeciesGroupID) 
        merge.categ= c("FishingEntityID", "SpeciesGroupID")
        
        } else if(z==5){   match.categ= list(spatY$FishingEntityID) 
        merge.categ= c("FishingEntityID")
        
        } else if(z==6){   match.categ= list(spatY$CountryGroupID, spatY$GearGroupID, spatY$TaxonKey) 
        merge.categ= c("CountryGroupID","GearGroupID","TaxonKey")
        
        } else if(z==7){   match.categ= list(spatY$CountryGroupID, spatY$GearGroupID, spatY$SpeciesGroupID) 
        merge.categ= c("CountryGroupID", "GearGroupID","SpeciesGroupID")
        
        } else if(z==8){   match.categ= list(spatY$CountryGroupID, spatY$TaxonKey) 
        merge.categ= c("CountryGroupID", "TaxonKey")
        
        } else if(z==9){   match.categ= list(spatY$CountryGroupID, spatY$SpeciesGroupID) 
        merge.categ= c("CountryGroupID", "SpeciesGroupID")
             
        } else if(z==10){   match.categ= list(spatY$TaxonKey) 
        merge.categ= c("TaxonKey")
        
        } else if(z==11){   match.categ= list(spatY$SpeciesGroupID) 
        merge.categ= c("SpeciesGroupID")
        
        } else if(z==12){   match.categ= list(spatY$GearGroupID) 
        merge.categ= c("GearGroupID")
        
        } else if(z==13){   match.categ= list(spatY$CountryGroupID) 
        merge.categ= c("CountryGroupID") }
        
        match.categ.names= c(merge.categ,"TotalCatch") #This gets done after merge.categ if statements
        matchid= z+1 #Add 1 because the first match (outside the loop) is ID 1
        
        #Run the data.match function using the current spatial category data and years. If spatial data or nominal data do not exist for whatever reason 
        #(no data in those years, nominal data is already matched), do nothing and keep going
        if( dim(spatY)[1]>0 & dim(nom.dataY)[1]>0 ){  db.match= data.match(nom.dataY,spatY, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.match }
        NM= nom.datai
        #After each loop, match original non-matched data to matched data and only keep what hasn't been matched yet
        NM= subset( merge(NM, cbind("NID"=sort(unique(db.match$NID)), "Match"=1), by="NID",all.x=T), is.na(Match) )[,-c(10)]
        
        nni= dim(NM)[1] #Rows in remaining non-matched data for country
        print("% Matched"); print(round((1-nni/rni) * 100, 1))
        
      } #Close year range loop

    } #Close years loop

  } #Close category loop
  
  #Aggregate potential duplicate category values to reduce size of matched database
  db.match= aggregate(db.match$Catch, by= list(db.match$NID,db.match$Year,db.match$FishingEntityID,db.match$Layer3GearID,db.match$TaxonKey,db.match$BigCellID,db.match$MatchID), sum)
  colnames(db.match)= c("NID","Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","MatchID","Catch")
  
}  #Close country loop 

sum(db.match$Catch)/sum(nom$Catch)

#=============================================================================================================
#==SECTION IV: Re-Run with Larger Year Ranges
#=============================================================================================================
#db.nomatch <- read.csv("OUTPUT IATTC No Match Catch 1 of 1.csv",header=T)

db.nomatch= nom
#Make db.nomatch equal to all nom catch except those already matched as indicated by NIDs:
db.nomatch = subset( merge(db.nomatch, cbind("NID"=sort(unique(db.match$NID)), "Match"=1), by.y="NID",all.x=T), is.na(Match) )[,-c(10)]
#Re-run matching sequence above with expanded year range on only unmatched nom catch
db.nomatch= db.nomatch[,colnames(nom)]	#Reset columns in non-matched database
  
fids= sort(unique(db.nomatch$FishingEntityID)) #Vector of unique FishingEntityIDs in the nominal data
YRs= c(10,20,35) #Year ranges for subsetting (i.e. +/- years to match over) TC Removed 1, 10, 15 as these are saying that categories are more important than time. This will bias results towards future tuna fisheries rather than spatial expansion. 

for(i in 1:length(fids)) #Loop over countries
  
{
  ci= fids[i] #Test by changing index to desired FishingEntityID number
  nom.datai= subset(db.nomatch, FishingEntityID==ci) #Nominal data to match for a country
  
  gears= subset(gearres, FishingEntityID==ci)[,c("FishingEntityID","GearGroupID")] #Fishing gears used by that country
  if(dim(gears)[1] == 0) {gears= cbind("FishingEntityID"=ci, "GearGroupID"=unique(spat$GearGroupID))} #If no restriction, use all gears
  y= merge(spat, gears, by="GearGroupID",all.x=T) #Match country gears to spatial data
  
  cells= subset(areares, FishingEntityID==ci)[,c("FishingEntityID","BigCellID")] #Cells used by that country
  if(dim(cells)[1] == 0) {cells= cbind("FishingEntityID"=ci, "BigCellID"=unique(spat$BigCellID))} #If no restriction, use all cells
  y2= merge(y, cells, by="BigCellID",all.x=T)#Match country cells to spatial data
  
  spati= subset(y2, FishingEntityID.y==ci & FishingEntityID==ci) #Only use spatial data that matched to both the allowed country gears and cells
  spati= spati[,-c(12,13)]; colnames(spati)[7]= "FishingEntityID"#Keep necessary data and update column name UPDATE check this for errors
  
  rm(list=c("y","y2","cells", "gears")) #Remove interim datasets
  
  yrs= sort(unique(nom.datai$Year)) #Unique years in the country nominal data
  NM= nom.datai #Non-matched data that will be used for the country. This should be the only time this nom.datai gets used each loop
  rni= dim(nom.datai)[1] #Rows in original country non-matched data
  
  for(z in 14:26) #Ten is the number of category combos in the ifelse statment below
  {
    
    for(j in 1:length(yrs)) #Loop over years
    {
      yrj=yrs[j] #Year j is pulled from the vector of year for that country
      
      for(w in 1:length(YRs)) #Loop over year ranges
      {
        YR=YRs[w] #Year range is pulled from the year ranges (+/- 0 to 20 year range)
        
        print("FishingEntityID"); print(i)
        
        print("Category"); print(z) #Current category for troubleshooting
        
        print("Year"); print(yrj) #Current year for troubleshooting
        
        print("Year range"); print(YR) #Current year range for troubleshooting
        
        nom.dataY= subset(NM, Year==yrj) #Only match data for a given year
        spatY= subset(spat, Year<=yrj+YR & Year>=yrj-YR ) #Only use data for a given year range
        #print("Years spatial");print(sort(unique(spatY$Year)))  #Current years in range for troubleshooting
        
        #Depending on the step z, match by decreasing number of categories (this must be updated manually)
        if(z==14){   match.categ= list(spatY$FishingEntityID, spatY$GearGroupID, spatY$TaxonKey) 
        merge.categ= c("FishingEntityID","GearGroupID","TaxonKey")
        
        } else if(z==15){   match.categ= list(spatY$FishingEntityID, spatY$GearGroupID, spatY$SpeciesGroupID) 
        merge.categ= c("FishingEntityID", "GearGroupID","SpeciesGroupID")
        
        } else if(z==16){   match.categ= list(spatY$FishingEntityID, spatY$TaxonKey) 
        merge.categ= c("FishingEntityID", "TaxonKey")
        
        } else if(z==17){   match.categ= list(spatY$FishingEntityID, spatY$SpeciesGroupID) 
        merge.categ= c("FishingEntityID", "SpeciesGroupID")
        
        } else if(z==18){   match.categ= list(spatY$FishingEntityID) 
        merge.categ= c("FishingEntityID")
        
        } else if(z==19){   match.categ= list(spatY$CountryGroupID, spatY$GearGroupID, spatY$TaxonKey) 
        merge.categ= c("CountryGroupID","GearGroupID","TaxonKey")
        
        } else if(z==20){   match.categ= list(spatY$CountryGroupID, spatY$GearGroupID, spatY$SpeciesGroupID) 
        merge.categ= c("CountryGroupID", "GearGroupID","SpeciesGroupID")
        
        } else if(z==21){   match.categ= list(spatY$CountryGroupID, spatY$TaxonKey) 
        merge.categ= c("CountryGroupID", "TaxonKey")
        
        } else if(z==22){   match.categ= list(spatY$CountryGroupID, spatY$SpeciesGroupID) 
        merge.categ= c("CountryGroupID", "SpeciesGroupID")
        
        } else if(z==23){   match.categ= list(spatY$TaxonKey) 
        merge.categ= c("TaxonKey")
        
        } else if(z==24){   match.categ= list(spatY$SpeciesGroupID) 
        merge.categ= c("SpeciesGroupID")
        
        } else if(z==25){   match.categ= list(spatY$GearGroupID) 
        merge.categ= c("GearGroupID") 
        
        } else if(z==26){   match.categ= list(spatY$CountryGroupID) 
        merge.categ= c("CountryGroupID") }
        
        match.categ.names= c(merge.categ,"TotalCatch") #This gets done after merge.categ if statements
        matchid= z+1 #Add 1 because the first match (outside the loop) is ID 1
        
        #Run the data.match function using the current spatial category data and years. If spatial data or nominal data do not exist for whatever reason 
        #(no data in those years, nominal data is already matched), do nothing and keep going
        if( dim(spatY)[1]>0 & dim(nom.dataY)[1]>0 ){  db.match= data.match(nom.dataY,spatY, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.match }
        NM= nom.datai
        #After each loop, match original non-matched data to matched data and only keep what hasn't been matched yet
        NM= subset( merge(NM, cbind("NID"=sort(unique(db.match$NID)), "Match"=1), by="NID",all.x=T), is.na(Match) )[,-c(10)]
        
        nni= dim(NM)[1] #Rows in remaining non-matched data for country
        print("% Matched"); print(round((1-nni/rni) * 100, 1))
        
      } #Close year range loop
      
    } #Close years loop
    
  } #Close category loop
  
  #Aggregate potential duplicate category values to reduce size of matched database
  db.match= aggregate(db.match$Catch, by= list(db.match$NID,db.match$Year,db.match$FishingEntityID,db.match$Layer3GearID,db.match$TaxonKey,db.match$BigCellID,db.match$MatchID), sum)
  colnames(db.match)= c("NID","Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","MatchID","Catch")
  
}  #Close country loop 

#Aggregate potential duplicate category values to reduce size of matched database, drop NID column
db.match= aggregate(db.match$Catch, by= list(db.match$Year,db.match$FishingEntityID,db.match$Layer3GearID,db.match$TaxonKey,db.match$BigCellID,db.match$MatchID), sum)
colnames(db.match)= c("Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","MatchID","Catch")

###STOP###
#stop("SHOULD BE DONE...") #


#====================================Writing======================================#
db.match= db.match[order(db.match$Year),]

db.match= aggregate(db.match$Catch, by= list(db.match$Year,db.match$FishingEntityID,db.match$Layer3GearID,db.match$TaxonKey,db.match$BigCellID,db.match$MatchID), sum)
colnames(db.match)= c("Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","MatchID","Catch")

db.match$RFMOID <- 5  #RFMO i.d. for IATTC = 5
db.match<- db.match[ order(db.match$Year), c("RFMOID","Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","MatchID","Catch") ]

chunk_size <- 1000000
n <- nrow(db.match)
num_chunks <- ceiling(n / chunk_size)

for (i in 1:num_chunks) {
  start_row <- ((i - 1) * chunk_size) + 1
  end_row <- min(i * chunk_size, n)
  chunk <- db.match[start_row:end_row, ]
  filename <- paste0("Final IATTC Spatialized Catch with MatchID ",i, " of ", num_chunks,".csv")
  write.table(chunk, filename, sep = ",", row.names = FALSE, quote = FALSE)
}

#=========================Testing===========================================#

#Country catch check, change index to desired FishingEntityID
#After you read in the function, just type ccatch(#)
ccatch=function(country=x)
{
  cnom= sum( subset(nom, FishingEntityID==country)$Catch )
  cspat= sum( subset(db.match, FishingEntityID==country)$Catch )
  return(list(cnom=cnom, cspat=cspat))
}

#To see the quality of matches by category run test below: 
#Aggregate catch by MatchID and Year
agc= aggregate(db.match$Catch, by= list(db.match$Year, db.match$MatchID),sum)
colnames(agc)= c("Year","MatchID","Catch")
write.table(agc, "OUTPUT IATTC Catch by Year and MatchID.csv", sep=",",row.names=F, quote=F)




