#Last updated: September 6th, 2018

#Please note, this script erases the global environment several times to increase the speed
#Please save any in-progress work of your own before running this script. 

#Formatting information for input datasets is found in the README

#Check for and install dependent packages 
if (!require("dplyr")){
  install.packages("dplyr")
  library("dplyr")
  }
if (!require("data.table")){
  install.packages("data.table")
  library("data.table")
}
if (!require("reshape2")){
  install.packages("reshape2")
  library("reshape2")
}

#You will have to download the data and replace the '..' with your local directory.
#After this, the program will run within the folders within the 'Tuna_Harmonization' folder
#All results will be stored in the 'Outputs' folder when the script is finished running 
setwd("../Tuna_Harmonization")

#CCSBT--------
setwd("./CCSBT")
#Raw data formatting
#Read-in data
ccsbt.n= read.csv("INPUT CCSBT Nominal Catch For Formatting.csv", sep=",")
ccsbt.s= read.csv("INPUT CCSBT Spatial Catch For Formatting.csv", sep=",")


#Download these reference files from the Sea Around Us GitHub Repository
ccsbt.cou= read.table("INPUT CCSBT Nominal Country Codes.txt", header=T, sep="\t")
ccsbt.oce= read.table("INPUT CCSBT Nominal Ocean Codes.txt", header=T, sep="\t")
ccsbt.soce= read.table("INPUT CCSBT Spatial Ocean Codes.txt", header=T, sep="\t")
ccsbt.cel= read.table("INPUT CCSBT Spatial Cell Codes.txt", header=T, sep="\t")
ccsbt.gea= read.table("INPUT CCSBT Gear Codes.txt", header=T, sep="\t")
cellid= read.csv("INPUT CellTypeID.csv", sep=",")

#Melt dataframe of ccsbt.n as header column has countries
ccsbt.n= melt(ccsbt.n, id.vars= c("OceanName", "Year"), variable.name="CountryName", value.name="Catch")

#Exclude entries with no catch or, for spatial data, month or location data
ccsbt.n= ccsbt.n[ccsbt.n$Catch!=0,]
#ccsbt.s= ccsbt.s[is.na(ccsbt.s$Month)==F,]
ccsbt.s= ccsbt.s[ccsbt.s$Catch!=0,]
ccsbt.s= ccsbt.s[is.na(ccsbt.s$Lat)==F,]

#Match codes in nominal data
ccsbt.N= left_join(ccsbt.n, ccsbt.cou, by= c("CountryName"))
#ccsbt.N= left_join(ccsbt.N, ccsbt.gea, by= c("GearName"))
ccsbt.N= left_join(ccsbt.N, ccsbt.oce, by= c("OceanName"))

ccsbt.N= ccsbt.N[ , c("Year","FishingEntityID","CountryGroupID","OceanID","Catch") ]

#Exclude 'research' catch (fishing entity id=2000) and split 'all countries' catch (fishing entity ID=1000)
ccsbt.N= ccsbt.N[ ccsbt.N$FishingEntityID!=2000, ]

#Allocate catch out from "All countries" fishing entity (FishingEntityID= 1000). 
x= ccsbt.N[ccsbt.N$FishingEntityID==1000,]
y= ccsbt.N[ccsbt.N$FishingEntityID!=1000,]

y2= aggregate(y$Catch, by= list(y$Year, y$OceanID), sum)
colnames(y2)= c("Year","OceanID","TotalCatch")

y3= left_join(y, y2, by= c("Year","OceanID"))
y3$Proportion= y3$Catch / y3$TotalCatch
y3= y3[,c("Year","FishingEntityID","CountryGroupID","OceanID","Proportion")]

z= left_join(x, y3, by= c("Year","OceanID"))
z$PropCatch= z$Catch * z$Proportion
z= z[,c("Year","FishingEntityID.y","CountryGroupID.y","OceanID","PropCatch")]
colnames(z)= c("Year","FishingEntityID","CountryGroupID","OceanID","Catch")

z2= rbind(y,z)

z3= aggregate(z2$Catch, by= list(z2$Year, z2$FishingEntityID, z2$CountryGroupID, z2$OceanID), sum)
colnames(z3)= colnames(y)

ccsbt.N= z3
ccsbt.N$TaxonKey= 600145	#Add the southern bluefin tuna TaxonKey code
ccsbt.N= ccsbt.N[, c("Year","FishingEntityID","CountryGroupID","OceanID","TaxonKey","Catch") ]

write.table(ccsbt.N, "INPUT CCSBT Nominal Catch For Spatial Matching.csv", sep=",", row.names=F)

#Spatial data
#Match codes
ccsbt.S= left_join(ccsbt.s, ccsbt.soce, by= c("OceanName"))
ccsbt.S= left_join(ccsbt.S, ccsbt.gea, by= c("GearName"))
ccsbt.S= left_join(ccsbt.S, ccsbt.cel, by= c("Lat","Lon"))
ccsbt.S= left_join(ccsbt.S, cellid, by= c("x","y","BigCellTypeID"))

ccsbt.S= ccsbt.S[ , c("Year","Layer3GearID","GearGroupID","OceanID","BigCellID","Catch") ]
#Monthly data into yearly spatial data

ccsbt.S$Catch <- as.numeric(as.character(ccsbt.S$Catch))
ccsbt.S= aggregate(ccsbt.S$Catch, by=list(ccsbt.S$Year, ccsbt.S$Layer3GearID,ccsbt.S$GearGroupID,ccsbt.S$OceanID,ccsbt.S$BigCellID) , sum)
colnames(ccsbt.S)= c("Year","Layer3GearID","GearGroupID","OceanID","BigCellID","Catch")

ccsbt.S$TaxonKey= 600145	#Add southern bluefin tuna TaxonKey
ccsbt.S= ccsbt.S[, c("Year","Layer3GearID","GearGroupID","OceanID","BigCellID","TaxonKey","Catch") ]

write.table(ccsbt.S, "INPUT CCSBT Spatial Catch For Spatial Matching.csv", sep=",", row.names=F)



####Section I-CCSBT: INITIAL DATA AND TRANSFORMATION ####
rm(list=ls())

#1.Read in formatted databases of nominal and spatialized catch by ocean

nom=read.csv("INPUT CCSBT Nominal Catch For Spatial Matching.csv",sep=",",header=T)	#Yearly nominal catch
spat=read.csv("INPUT CCSBT Spatial Catch For Spatial Matching.csv",sep=",",header=T)	#Yearly available spatial catch

nom= nom[nom$Catch!=0,]

spat= spat[spat$Catch!=0,]
spat= spat[is.na(spat$BigCellID)==F,]

#Download these reference files from the Sea Around Us GitHub Repository
gearres= read.csv("INPUT CCSBT GearRestrictionTable.csv",sep=",") #Read in gear restrictions
areares= read.csv("INPUT CCSBT AreaRestrictionTable.csv",sep=",") #Read in area restrictions

tot.nom.ct=sum(nom$Catch)	#Total catch in nominal database; this is used to compare catches after spatializing.

tot.spat.ct= sum(spat$Catch)	#Total catch in spatial database; this is not used in the routine but is available to look at.


final.categ= c("Year","FishingEntityID","TaxonKey","BigCellID","SpatCatch")
final.categ.names= c("Year","FishingEntityID","TaxonKey","BigCellID","Catch")

#Add record IDs
nom$NID= 1:dim(nom)[1]

#Add ID categories everywhere

final.categ= c("NID","Year","FishingEntityID","TaxonKey","BigCellID","SpatCatch","MatchID")
final.categ.names= c("NID","Year","FishingEntityID","TaxonKey","BigCellID","Catch","MatchID")


####Section II-CCSBT: PERFECT MATCH ####

#First match all the best-case scenarios (i.e. perfect data match)
#This doesn't need any spatial data subsetting

nom.data= nom
match.categ= list(spat$Year, spat$OceanID, spat$TaxonKey)
merge.categ= c("Year","OceanID","TaxonKey")
match.categ.names= c(merge.categ,"TotalCatch")
matchid= 1


#1.Sum up the total catch, by all categories, in all reported spatial cells.

ct.all= aggregate(spat$Catch,by= match.categ,sum)  #Total catch in all cells, all categories

colnames(ct.all)= match.categ.names #Add matching column names

#2.Transform reported catch per cell into proportions of the total for all cells, by all categories.

spat.all= merge(spat,ct.all,by= merge.categ, all.x=T)	#Spatialized catch total in all cells by Year/GearGroupID/TaxonKey

spat.all$Proportion= spat.all$Catch / spat.all$TotalCatch	#Proportion of catch per cell = catch / total catch in cells reported for that category combo
merge.categ2 = c("Year","OceanID","TaxonKey", "Layer3GearID", "GearGroupID")
spat.all= spat.all[,c(merge.categ2,"BigCellID","Proportion")]	#Remove the catch per cell in this dataframe (but leave the proportions)


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

#6.Reset columns so that they match the original nominal and spatial databases (to avoid potential indexing mistakes) 
final.categ= c("NID","Year","FishingEntityID","TaxonKey","Layer3GearID", "GearGroupID", "BigCellID","SpatCatch","MatchID")
final.categ.names= c("NID","Year","FishingEntityID","TaxonKey","Layer3GearID", "GearGroupID","BigCellID","Catch","MatchID")
db.match= db.match[,final.categ]	#Re-order columns for matched database
colnames(db.match)= final.categ.names

db.nomatch= db.nomatch[,colnames(nom)]	#Reset columns in non-matched database

#Clean up your memory for faster computations
rm(list=c("ct.all","spat.all","new.db","nom.data"))	

####Section IIIa-CCSBT: Matching Algorithm ####

#This is the key function. Same as above but does not return the non-matched data as this is unnecessary
#given the functions below
#This function takes the categories for the level of the match and spatializes 
#the catch where matches are found for those match categories
#Match categories come from the loop process below where the function is called repeatedly for different levels of match categories and year ranges within a country


data.match= function(nom.dataY, spat, db.match, match.categ, merge.categ, match.categ.names, matchid )
{
  ct.all= aggregate(spatY$Catch,by= match.categ,sum)
  colnames(ct.all)= match.categ.names 
  spat.all= left_join(spatY,ct.all,by= merge.categ)  
  spat.all$Proportion= spat.all$Catch / spat.all$TotalCatch  
  spat.all= spat.all[,c(merge.categ,"BigCellID","Proportion")]  
  new.db= left_join(nom.dataY,spat.all,by= merge.categ)
  new.db$SpatCatch= new.db$Catch * new.db$Proportion	
  new.db$MatchID= matchid
  db.match.new= filter(new.db,is.na(BigCellID)==F)	
  db.match.new= db.match.new[,final.categ]	
  colnames(db.match.new)= final.categ.names
  db.match= rbind(db.match, db.match.new)
  rm(list=c("ct.all","spat.all","new.db","db.match.new")) 
  
  return(list(db.match=db.match))
}


fids= sort(unique(db.nomatch$FishingEntityID)) #Vector of unique FishingEntityIDs in the nominal data

YRs= c(0,2,5) #Year ranges for subsetting (i.e. +/- years to match over) TC Removed 1, 10, 15 as these are saying that categories are more important than time. This will bias results towards future tuna fisheries rather than spatial expansion. 

for(i in 1:length(fids)) #Loop over countries
  
{
  ci= fids[i] #Test by changing index to desired FishingEntityID number
  
  nom.datai= subset(db.nomatch, FishingEntityID==ci) #Nominal data to match for a country
  
  gears= subset(gearres, FishingEntityID==ci)[,c("FishingEntityID","Layer3GearID")] #Fishing gears used by that country
  if(dim(gears)[1] == 0) {gears= cbind("FishingEntityID"=ci, "Layer3GearID"=unique(spat$Layer3GearID))} #If no restriction, use all gears
  y= merge(spat, gears, by="Layer3GearID",all.x=T) #Match country gears to spatial data
  
  cells= subset(areares, FishingEntityID==ci)[,c("FishingEntityID","BigCellID")] #Cells used by that country
  if(dim(cells)[1] == 0) {cells= cbind("FishingEntityID"=ci, "BigCellID"=unique(spat$BigCellID))} #If no restriction, use all cells
  y2= merge(y, cells, by="BigCellID",all.x=T)#Match country cells to spatial data
  #Commented out this section and removed as there are no restrictions for CCSBT
  spati= subset(y2, FishingEntityID.y==ci & FishingEntityID.x==ci) #Only use spatial data that matched to both the allowed country gears and cells
  spati= spati[,-c(9)]; colnames(spati)[8]= "FishingEntityID"#Keep necessary data and update column name UPDATE check this for errors
  
  rm(list=c("y","y2","gears","cells")) #Remove interim datasets
  
  yrs= sort(unique(nom.datai$Year)) #Unique years in the country nominal data
  
  NM= nom.datai #Non-matched data that will be used for the country. This should be the only time this nom.datai gets used each loop
  
  rni= dim(nom.datai)[1] #Rows in original country non-matched data
  
  for(z in 1:1) #One is the number of category combos in the ifelse statment below
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
        spatY= subset(spati, Year<=yrj+YR & Year>=yrj-YR ) #Only use data for a given year range
        
        #print("Years spatial");print(sort(unique(spatY$Year)))  #Current years in range for troubleshooting
        
        #Depending on the step z, match by decreasing number of categories (this must be updated manually)
        if(z==1){   match.categ= list(spatY$OceanID, spatY$TaxonKey) 
        merge.categ= c("OceanID","TaxonKey")
        }
        
        match.categ.names= c(merge.categ,"TotalCatch") #This gets done after merge.categ if statements
        matchid= z+1 #Add 1 because the first match (outside the loop) is ID 1
        
        #Run the data.match function using the current spatial category data and years. If spatial data or nominal data do not exist for whatever reason 
        #(no data in those years, nominal data is already matched), do nothing and keep going
        if( dim(spatY)[1]>0 & dim(nom.dataY)[1]>0 ){  db.match= data.match(nom.dataY,spatY, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.match }
        
        #After each loop, match original non-matched data to matched data and only keep what hasn't been matched yet
        NM= nom.datai #Non-matched data that will be used for the country. This should be the only time this nom.datai gets used each loop
        NM= subset( merge(NM, cbind("NID"=sort(unique(db.match$NID)), "Match"=1), by="NID",all.x=T), is.na(Match) )[,-c(10)] 
        
        nni= dim(NM)[1] #Rows in remaining non-matched data for country
        print("% Matched"); print(round((1-nni/rni) * 100, 1))
        
        
      } #Close year range loop
      
      
    } #Close years loop
    
    
  } #Close category loop
  
  #Aggregate potential duplicate category values to reduce size of matched database
  db.match= aggregate(db.match$Catch, by= list(db.match$NID,db.match$Year,db.match$FishingEntityID,db.match$TaxonKey,db.match$BigCellID,db.match$MatchID), sum)
  colnames(db.match)= c("NID","Year","FishingEntityID","TaxonKey","BigCellID","MatchID","Catch")
  
}  #Close country loop 
#First Round Done

####Section IIIb-CCSBT: Re-Run with Larger Year Ranges####

db.nomatch = nom #Re-set db.nomatch
#Make db.nomatch equal to all nom catch except those already matched as indicated by NIDs:
db.nomatch = subset( merge(db.nomatch, cbind("NID"=sort(unique(db.match$NID)), "Match"=1), by="NID",all.x=T), is.na(Match) )[,-c(11)]
#Re-run matching sequence above with expanded year range on only unmatched nom catch

db.nomatch= db.nomatch[,colnames(nom)]	#Reset columns in non-matched database


sum(db.nomatch$Catch)


sum(db.match$Catch)/sum(nom$Catch)

fids= sort(unique(db.nomatch$FishingEntityID)) #Vector of unique FishingEntityIDs in the nominal data

YRs= c(10,20,35) #Year ranges for subsetting (i.e. +/- years to match over) Removed 1, 10, 15 as these are saying that categories are more important than time. This will bias results towards future tuna fisheries rather than spatial expansion. 


for(i in 1:length(fids)) #Loop over countries
  
{
  ci= fids[i] #Test by changing index to desired FishingEntityID number
  
  nom.datai= subset(db.nomatch, FishingEntityID==ci) #Nominal data to match for a country
  
  gears= subset(gearres, FishingEntityID==ci)[,c("FishingEntityID","Layer3GearID")] #Fishing gears used by that country
  if(dim(gears)[1] == 0) {gears= cbind("FishingEntityID"=ci, "Layer3GearID"=unique(spat$Layer3GearID))} #If no restriction, use all gears
  y= merge(spat, gears, by="Layer3GearID",all.x=T) #Match country gears to spatial data
  
  cells= subset(areares, FishingEntityID==ci)[,c("FishingEntityID","BigCellID")] #Cells used by that country
  if(dim(cells)[1] == 0) {cells= cbind("FishingEntityID"=ci, "BigCellID"=unique(spat$BigCellID))} #If no restriction, use all cells
  y2= merge(y, cells, by="BigCellID",all.x=T)#Match country cells to spatial data
  #Commented out this section and removed as there are no restrictions for CCSBT
  spati= subset(y2, FishingEntityID.y==ci & FishingEntityID.x==ci) #Only use spatial data that matched to both the allowed country gears and cells
  spati= spati[,-c(9)]; colnames(spati)[8]= "FishingEntityID"#Keep necessary data and update column name UPDATE check this for errors
  
  rm(list=c("y","y2","gears","cells")) #Remove interim datasets
  
  yrs= sort(unique(nom.datai$Year)) #Unique years in the country nominal data
  
  NM= nom.datai #Non-matched data that will be used for the country. This should be the only time this nom.datai gets used each loop
  
  rni= dim(nom.datai)[1] #Rows in original country non-matched data
  
  for(z in 2:2) #One is the number of category combos in the ifelse statment below
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
        spatY= subset(spati, Year<=yrj+YR & Year>=yrj-YR ) #Only use data for a given year range
        
        #print("Years spatial");print(sort(unique(spatY$Year)))  #Current years in range for troubleshooting
        
        #Depending on the step z, match by decreasing number of categories (this must be updated manually)
        if(z==2){   match.categ= list(spatY$OceanID, spatY$TaxonKey) 
        merge.categ= c("OceanID","TaxonKey")
        
        }
        
        match.categ.names= c(merge.categ,"TotalCatch") #This gets done after merge.categ if statements
        matchid= z+1 #Add 1 because the first match (outside the loop) is ID 1
        
        #Run the data.match function using the current spatial category data and years. If spatial data or nominal data do not exist for whatever reason 
        #(no data in those years, nominal data is already matched), do nothing and keep going
        if( dim(spatY)[1]>0 & dim(nom.dataY)[1]>0 ){  db.match= data.match(nom.dataY,spatY, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.match }
        
        #Re-run this line so line below will properly subset NM. 
        #After each loop, match original non-matched data to matched data and only keep what hasn't been matched yet
        NM= nom.datai #Non-matched data that will be used for the country. This should be the only time this nom.datai gets used each loop
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


####Section IV-CCSBT: Testing #####
db.nomatch = subset( merge(db.nomatch, cbind("NID"=sort(unique(db.match$NID)), "Match"=1), by="NID",all.x=T), is.na(Match) )[,-c(11)]
#Refine db.nomatch data 

db.nomatch= db.nomatch[,colnames(nom)]	#Reset columns in non-matched database


sum(db.nomatch$Catch) #Nomatch is equal to 0?


sum(db.match$Catch)/sum(nom$Catch) #Matched % is equal to 1?


#Aggregate potential duplicate category values to reduce size of matched database, drop NID column
db.match= aggregate(db.match$Catch, by= list(db.match$Year,db.match$FishingEntityID,db.match$Layer3GearID,db.match$TaxonKey,db.match$BigCellID,db.match$MatchID), sum)
colnames(db.match)= c("Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","MatchID","Catch")

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
write.table(agc, "OUTPUT CCSBT Catch by Year and MatchID.csv", sep=",",row.names=F, quote=F)


####Section V-CCSBT: Writing #####
db.match= db.match[order(db.match$Year),]

db.match= aggregate(db.match$Catch, by= list(db.match$Year,db.match$FishingEntityID,db.match$Layer3GearID,db.match$TaxonKey,db.match$BigCellID, db.match$MatchID), sum)
colnames(db.match)= c("Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID", "MatchID", "Catch")


#EXPORT DATA

db.match$RFMOID= 3  #RFMO i.d. for CCSBT = 3

db.match= db.match[ , c("RFMOID","Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","MatchID", "Catch") ]

write.table(db.match[order(db.match$Year),], "../Outputs/OUTPUT CCSBT Spatialized Catch 1 of 1.csv", sep=",", row.names=F, quote=F)




#IOTC--------
####IOTC nominal data formatting####

rm(list=ls())
setwd("../IOTC")
#read in nominal data set
iotc.n= read.csv("INPUT IOTC Nominal Catch for Formatting.csv", sep=",") 

#Read in numeric codes for country, gear, taxon and area
iotc.gea= read.table("INPUT IOTC Nominal Gear Codes.txt", header=T)
iotc.spp= read.table("INPUT IOTC Species Codes.txt", header=T)
iotc.cou= read.csv("INPUT IOTC Nominal Country Codes.csv", sep=",")
iotc.are= read.csv("INPUT IOTC Area Codes.csv", sep=",")

#Merge nominal data with numeric codes 
iotc.N= left_join(iotc.n, iotc.cou, by= c("Country"))
iotc.N= left_join(iotc.N, iotc.gea, by= c("Gear"))
iotc.N= left_join(iotc.N, iotc.are, by= c("Area"))
iotc.N= left_join(iotc.N, iotc.spp, by= c("SpeciesCode"))

#Remove columns with alphabetical names
#iotc.N= iotc.N[,-c(1:4)]

#Remove entries with 0 or NA values
iotc.N= iotc.N[ is.na(iotc.N$Catch)==F & iotc.N$Catch!=0, ]

iotc.art = iotc.N[iotc.N$Sector=="ART", ]
iotc.ind = iotc.N[iotc.N$Sector=="IND", ]
iotc.art_countries = iotc.art[iotc.art$FishingEntityID==58, ] #France has other 'artisanal' gears than below but is reclassified as industrial because its a DWF
iotc.art_subset_ind_gears = iotc.art[iotc.art$Layer3GearID %in% c(3, 41, 43, 55, 68),] #Separate mechanized baitboats and trolls, and various purse seines 
iotc.art_subset_ind_gears = iotc.art_subset_ind_gears[iotc.art_subset_ind_gears$GearGroupID %in% c(31, 10, 41), ] #Eliminate extra 'purse seine' like gear that for now we'll keep as artisanal

iotc.art_subset_ind_gears = iotc.art_subset_ind_gears[iotc.art_subset_ind_gears$FishingEntityID !=9, ] #Eliminate Bahrain as catch is assigned in Sea Around Us artisanal work
iotc.art_subset_ind_gears = iotc.art_subset_ind_gears[iotc.art_subset_ind_gears$FishingEntityID !=156, ] #Eliminate Saudi Arabia as catch is assigned in Sea Around Us artisanal work

iotc.N = rbind(iotc.ind, iotc.art_subset_ind_gears, iotc.art_countries) #Combine reclassified artisanal to industrial 
iotc.N$Sector="IND"  #Relabel all as industrial sector. (Unnecessary but explicit of what we're doing)
iotc.N = iotc.N %>% select(-Sector)  #Delete sector column 

iotc.N= iotc.N[ , c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","AreaID","TaxonKey","SpeciesGroupID","Catch") ] 


iotc.art_countries2 = iotc.art[iotc.art$FishingEntityID!=58, ] #France has other 'artisanal' gears than below but is reclassified as industrial because its a DWF
iotc.art_subset_ind_gears2 = iotc.art[iotc.art$Layer3GearID %in% c(3, 41, 43, 55, 68),] #Separate mechanized baitboats and trolls, and various purse seines 
iotc.art_subset_ind_gears2 = iotc.art_subset_ind_gears[iotc.art_subset_ind_gears$GearGroupID %in% c(31, 10, 41), ] #Eliminate extra 'purse seine' like gear that for now we'll keep as artisanal

#USSR split
#USSR: Ukraine-80%, Russia 20%:  http://www.siodfa.org/index.php/the-sio/fishing-in-the-sio
#FishingEntityID: Ukraine= 181; Russia= 148
#CountryGroupID: Ukraine= 4; Russia= 4

x= iotc.N[ iotc.N$FishingEntityID!=2000, ]
y= iotc.N[ iotc.N$FishingEntityID==2000, ]

uk= ru= y
uk$Catch= uk$Catch * 0.8; uk$FishingEntityID= 181; uk$CountryGroupID= 4
ru$Catch= ru$Catch * 0.2; ru$FishingEntityID= 148; ru$CountryGroupID= 4

iotc.N= rbind(x, uk, ru)

#All countries split
#Allocate catch out from "All countries" fishing entity (FishingEntityID= 1000). 
x= iotc.N[iotc.N$FishingEntityID==1000,]
y= iotc.N[iotc.N$FishingEntityID!=1000,]

#Matching by year,gear, area, and species
y2= aggregate(y$Catch, by= list(y$Year, y$TaxonKey, y$Layer3GearID, y$AreaID), sum)
colnames(y2)= c("Year","TaxonKey","Layer3GearID","AreaID","TotalCatch")

y3= left_join(y, y2, by= c("Year","TaxonKey","Layer3GearID","AreaID"))
y3$Proportion= y3$Catch / y3$TotalCatch
y3= y3[,c("Year","Layer3GearID","GearGroupID","FishingEntityID","CountryGroupID","AreaID","TaxonKey","SpeciesGroupID","Proportion")]

z= left_join(x, y3, by= c("Year","Layer3GearID","GearGroupID","AreaID","TaxonKey","SpeciesGroupID"))
z$PropCatch= z$Catch * z$Proportion

zm= z[is.na(z$Proportion)==F,]
znm1= z[is.na(z$Proportion),]

zm1= zm[,c("Year","FishingEntityID.y","CountryGroupID.y","Layer3GearID","GearGroupID","AreaID","TaxonKey","SpeciesGroupID","PropCatch")]
colnames(zm1)= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","AreaID","TaxonKey","SpeciesGroupID","Catch")

znm1= znm1[,c("Year","FishingEntityID.x","CountryGroupID.x","Layer3GearID","GearGroupID","AreaID","TaxonKey","SpeciesGroupID","Catch")]
colnames(znm1)= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","AreaID","TaxonKey","SpeciesGroupID","Catch")

#Matching by year, area, and gear only
y4= aggregate(y$Catch, by= list(y$Year, y$Layer3GearID, y$AreaID), sum)
colnames(y4)= c("Year","Layer3GearID","AreaID","TotalCatch")

y5= left_join(y, y4, by= c("Year","Layer3GearID","AreaID"))
y5$Proportion= y5$Catch / y5$TotalCatch
y5= y5[,c("Year","Layer3GearID","GearGroupID","FishingEntityID","CountryGroupID","AreaID","TaxonKey","SpeciesGroupID","Proportion")]

z2= left_join(znm1, y5, by= c("Year","Layer3GearID","AreaID"))
z2$PropCatch= z2$Catch * z2$Proportion

zm2= z2[is.na(z2$Proportion)==F,]
znm2= z2[is.na(z2$Proportion),]

zm2= zm2[,c("Year","FishingEntityID.y","CountryGroupID.y","Layer3GearID","GearGroupID.x","AreaID","TaxonKey.x","SpeciesGroupID.x","PropCatch")]
colnames(zm2)= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","AreaID","TaxonKey","SpeciesGroupID","Catch")

znm2= znm2[,c("Year","FishingEntityID.x","CountryGroupID.x","Layer3GearID","GearGroupID.x","AreaID","TaxonKey.x","SpeciesGroupID.x","Catch")]
colnames(znm2)= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","AreaID","TaxonKey","SpeciesGroupID","Catch")

#Matching by year and area only
y4= aggregate(y$Catch, by= list(y$Year, y$AreaID), sum)
colnames(y4)= c("Year","AreaID","TotalCatch")

y5= left_join(y, y4, by= c("Year","AreaID"))
y5$Proportion= y5$Catch / y5$TotalCatch
y5= y5[,c("Year","Layer3GearID","GearGroupID","FishingEntityID","CountryGroupID","AreaID","TaxonKey","SpeciesGroupID","Proportion")]

z3= left_join(znm2, y5, by= c("Year","AreaID"))
z3$PropCatch= z3$Catch * z3$Proportion

zm3= z3[is.na(z3$Proportion)==F,]
znm3= z3[is.na(z3$Proportion),]

zm3= zm3[,c("Year","FishingEntityID.y","CountryGroupID.y","Layer3GearID.x","GearGroupID.x","AreaID","TaxonKey.x","SpeciesGroupID.x","PropCatch")]
colnames(zm3)= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","AreaID","TaxonKey","SpeciesGroupID","Catch")

x= rbind(y, zm1, zm2, zm3)

iotc.N= aggregate(x$Catch, by= list(x$Year,x$FishingEntityID,x$CountryGroupID,x$Layer3GearID,x$GearGroupID,x$AreaID,x$TaxonKey,x$SpeciesGroupID), sum)
colnames(iotc.N)= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","AreaID","TaxonKey","SpeciesGroupID","Catch")

iotc.N= iotc.N[order(iotc.N$Year), ]

write.table(iotc.N, "INPUT IOTC Nominal Catch For Spatial Matching.csv", sep=",", row.names=F)	#Export the new table to a csv file


####IOTC spatial data formatting####

rm(list=ls())


#Read spatial catch data
iotc.ss= read.csv("INPUT IOTC Spatial Surface Catch for Formatting.csv", sep=",")
iotc.sl= read.csv("INPUT IOTC Spatial Longline Catch for Formatting.csv", sep=",")
iotc.sc= read.csv("INPUT IOTC Spatial Coastal Catch for Formatting.csv", sep=",")

#Read spatial data codes
iotc.spps= read.table("INPUT IOTC Spatial Surface Species Codes.txt", header=T)
iotc.sppl= read.table("INPUT IOTC Spatial Longline Species Codes.txt", header=T)
iotc.sppc= read.table("INPUT IOTC Spatial Coastal Species Codes.txt", header=T)
iotc.cel= read.csv("INPUT IOTC Spatial Cell Codes.csv", sep=",", header=T)
iotc.are= read.csv("INPUT IOTC Area Codes.csv", sep=",")
iotc.gea= read.table("INPUT IOTC Spatial Gear Codes.txt", header=T)
iotc.cou= read.csv("INPUT IOTC Spatial Country Codes.csv", sep=",")
cellid= read.csv("INPUT CellTypeID.csv",sep=",",header=T)

#Surface data
#iotc.ss= iotc.ss[iotc.ss$CatchUnits=="MT",]
iotc.Ss= left_join(iotc.ss, iotc.cel, by=c("Grid"))
iotc.Ss= left_join(iotc.Ss, cellid, by=c("x","y","BigCellTypeID"))
write.table(iotc.Ss[is.na(iotc.Ss$BigCellID),], "OUTPUT IOTC No Spatial Surface Match Entries.csv", sep=",", row.names=F)
iotc.Ss= iotc.Ss[is.na(iotc.Ss$BigCellID)==F , ] #Only keep cells with a BigCellID match, the remainer are erroniously reported on land

iotc.Ss= left_join(iotc.Ss, iotc.cou, by=c("Fleet"))
iotc.Ss= left_join(iotc.Ss, iotc.gea, by=c("Gear"))
iotc.Ss= iotc.Ss %>% select(-c(Fleet, Gear, Grid)) #Get rid of columns with outdated codes

#Re-shape data, discard empty catch entries, and aggregate catch by year
iotc.Ss= melt(iotc.Ss, id= c("Year","MonthStart","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","AreaID","BigCellID"))
colnames(iotc.Ss)[9:10]= c("SpeciesCode","Catch")
iotc.Ss= left_join(iotc.Ss, iotc.spps, by= c("SpeciesCode"))
iotc.Ss= iotc.Ss[is.na(iotc.Ss$Catch)==F,]

iotc.Ss= aggregate(iotc.Ss$Catch, by= list(iotc.Ss$Year,iotc.Ss$FishingEntityID,iotc.Ss$CountryGroupID,iotc.Ss$Layer3GearID,iotc.Ss$GearGroupID,iotc.Ss$AreaID,iotc.Ss$BigCellID,iotc.Ss$TaxonKey,iotc.Ss$SpeciesGroupID), sum)


colnames(iotc.Ss)= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","AreaID","BigCellID","TaxonKey","SpeciesGroupID","Catch")


#Split "all countries" and USSR into individual fishing entities.

#USSR split
#USSR: Ukraine-80%, Russia 20%:  http://www.siodfa.org/index.php/the-sio/fishing-in-the-sio
#FishingEntityID: Ukraine= 181; Russia= 148
#CountryGroupID: Ukraine= 4; Russia= 4

x= iotc.Ss[ iotc.Ss$FishingEntityID!=2000, ]
y= iotc.Ss[ iotc.Ss$FishingEntityID==2000, ]

uk= ru= y
uk$Catch= uk$Catch * 0.8; uk$FishingEntityID= 181; uk$CountryGroupID= 4
ru$Catch= ru$Catch * 0.2; ru$FishingEntityID= 148; ru$CountryGroupID= 4

iotc.Ss= rbind(x, uk, ru)

#Allocate catch out from "All countries" fishing entity (FishingEntityID= 1000). 
x= iotc.Ss[iotc.Ss$FishingEntityID==1000,]
y= iotc.Ss[iotc.Ss$FishingEntityID!=1000,]

#Matching by year, gear, area, and species
y2= aggregate(y$Catch, by= list(y$Year, y$TaxonKey, y$Layer3GearID, y$AreaID), sum)
colnames(y2)= c("Year","TaxonKey","Layer3GearID","AreaID","TotalCatch")

y3= left_join(y, y2, by= c("Year","TaxonKey","Layer3GearID","AreaID"))
y3$Proportion= y3$Catch / y3$TotalCatch
y3= y3[,c("Year","Layer3GearID","GearGroupID","FishingEntityID","CountryGroupID","AreaID","TaxonKey","SpeciesGroupID","Proportion")]

y3= aggregate(y3$Proportion, by= list(y3$Year, y3$TaxonKey, y3$FishingEntityID, y3$CountryGroupID, y3$Layer3GearID, y3$AreaID), sum)
colnames(y3)= c("Year","TaxonKey","FishingEntityID", "CountryGroupID","Layer3GearID","AreaID","Proportion")

z= left_join(x, y3, by= c("Year","Layer3GearID","AreaID","TaxonKey"))
z$PropCatch= z$Catch * z$Proportion

zm1= z[is.na(z$Proportion)==F,]
znm1= z[is.na(z$Proportion),]

zm1= zm1[,c("Year","FishingEntityID.y","CountryGroupID.y","Layer3GearID","GearGroupID","AreaID","BigCellID","TaxonKey","SpeciesGroupID","PropCatch")]
colnames(zm1)= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","AreaID","BigCellID","TaxonKey","SpeciesGroupID","Catch")

znm1= znm1[,c("Year","FishingEntityID.x","CountryGroupID.x","Layer3GearID","GearGroupID","AreaID","BigCellID","TaxonKey","SpeciesGroupID","Catch")]
colnames(znm1)= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","AreaID","BigCellID","TaxonKey","SpeciesGroupID","Catch")


#Matching by year, area, and species
y2= aggregate(y$Catch, by= list(y$Year, y$TaxonKey, y$AreaID), sum)
colnames(y2)= c("Year","TaxonKey","AreaID","TotalCatch")

y3= left_join(y, y2, by= c("Year","TaxonKey","AreaID"))
y3$Proportion= y3$Catch / y3$TotalCatch
y3= y3[,c("Year","Layer3GearID","GearGroupID","FishingEntityID","CountryGroupID","AreaID","TaxonKey","SpeciesGroupID","Proportion")]

y3= aggregate(y3$Proportion, by= list(y3$Year, y3$TaxonKey, y3$FishingEntityID, y3$CountryGroupID, y3$AreaID), sum)
colnames(y3)= c("Year","TaxonKey","FishingEntityID", "CountryGroupID","AreaID","Proportion")

z= left_join(znm1, y3, by= c("Year","AreaID","TaxonKey"))
z$PropCatch= z$Catch * z$Proportion

zm2= z[is.na(z$Proportion)==F,]
znm2= z[is.na(z$Proportion),]

zm2= zm2[,c("Year","FishingEntityID.y","CountryGroupID.y","Layer3GearID","GearGroupID","AreaID","BigCellID","TaxonKey","SpeciesGroupID","PropCatch")]
colnames(zm2)= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","AreaID","BigCellID","TaxonKey","SpeciesGroupID","Catch")

znm2= znm2[,c("Year","FishingEntityID.x","CountryGroupID.x","Layer3GearID","GearGroupID","AreaID","BigCellID","TaxonKey","SpeciesGroupID","Catch")]
colnames(znm2)= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","AreaID","BigCellID","TaxonKey","SpeciesGroupID","Catch")


#Matching by year and area
y2= aggregate(y$Catch, by= list(y$Year, y$AreaID), sum)
colnames(y2)= c("Year","AreaID","TotalCatch")

y3= left_join(y, y2, by= c("Year","AreaID"))
y3$Proportion= y3$Catch / y3$TotalCatch
y3= y3[,c("Year","Layer3GearID","GearGroupID","FishingEntityID","CountryGroupID","AreaID","TaxonKey","SpeciesGroupID","Proportion")]

y3= aggregate(y3$Proportion, by= list(y3$Year, y3$TaxonKey, y3$FishingEntityID, y3$CountryGroupID, y3$AreaID), sum)
colnames(y3)= c("Year","TaxonKey","FishingEntityID", "CountryGroupID","AreaID","Proportion")

z= left_join(znm1, y3, by= c("Year","AreaID"))
z$PropCatch= z$Catch * z$Proportion

zm3= z[is.na(z$Proportion)==F,]
znm3= z[is.na(z$Proportion),]

zm3= zm3[,c("Year","FishingEntityID.y","CountryGroupID.y","Layer3GearID","GearGroupID","AreaID","BigCellID","TaxonKey.x","SpeciesGroupID","PropCatch")]
colnames(zm3)= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","AreaID","BigCellID","TaxonKey","SpeciesGroupID","Catch")

znm3= znm3[,c("Year","FishingEntityID.x","CountryGroupID.x","Layer3GearID","GearGroupID","AreaID","BigCellID","TaxonKey.x","SpeciesGroupID","Catch")]
colnames(znm3)= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","AreaID","BigCellID","TaxonKey","SpeciesGroupID","Catch")


iotc.Ss= rbind(y, zm1, zm2, zm3) 

write.table(iotc.Ss, "INPUT IOTC Spatial Surface Catch For Spatial Matching.csv", sep=",", row.names=F)  #Export the new table to a csv file


#Coastal data
iotc.Sc= left_join(iotc.sc, iotc.cel, by=c("Grid"))
iotc.Sc= left_join(iotc.Sc, cellid, by=c("x","y","BigCellTypeID"))
write.table(iotc.Sc[is.na(iotc.Sc$BigCellID),], "OUTPUT IOTC No Spatial Coastal Match Entries.csv", sep=",", row.names=F)
iotc.Sc= iotc.Sc[is.na(iotc.Sc$BigCellID)==F , ] #Only keep cells with a BigCellID match

iotc.Sc= left_join(iotc.Sc, iotc.cou, by=c("Fleet"))
iotc.Sc= left_join(iotc.Sc, iotc.gea, by=c("Gear"))
iotc.Sc = iotc.Sc %>% select(-c(MonthStart, Fleet, Gear, Grid))
#iotc.Sc= iotc.Sc[ , -c(1:6) ] #Get rid of columns with outdated codes

#Re-shape data, discard empty catch entries, and aggregate catch by year
iotc.Sc= melt(iotc.Sc, id= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","AreaID","BigCellID"))
colnames(iotc.Sc)[8:9]= c("SpeciesCode","Catch")
iotc.Sc= left_join(iotc.Sc, iotc.sppc, by= c("SpeciesCode"))
iotc.Sc= iotc.Sc[is.na(iotc.Sc$Catch)==F, ]  #Discard entries without catch, and outdated species code

iotc.Sc= aggregate(iotc.Sc$Catch, by= list(iotc.Sc$Year,iotc.Sc$FishingEntityID,iotc.Sc$CountryGroupID,iotc.Sc$Layer3GearID,iotc.Sc$GearGroupID,iotc.Sc$AreaID,iotc.Sc$BigCellID,iotc.Sc$TaxonKey,iotc.Sc$SpeciesGroupID), sum)
colnames(iotc.Sc)= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","AreaID","BigCellID","TaxonKey","SpeciesGroupID","Catch")

write.table(iotc.Sc, "INPUT IOTC Spatial Coastal Catch For Spatial Matching.csv", sep=",", row.names=F)  #Export the new table to a csv file


#Longline data
iotc.Sl= left_join(iotc.sl, iotc.cel, by=c("Grid"))
iotc.Sl= left_join(iotc.Sl, cellid, by=c("x","y","BigCellTypeID"))
write.table(iotc.Sl[is.na(iotc.Sl$BigCellID),], "OUTPUT IOTC No Spatial Longline Match Entries.csv", sep=",", row.names=F)
iotc.Sl= iotc.Sl[is.na(iotc.Sl$BigCellID)==F , ] #Only keep cells with a BigCellID match

iotc.Sl= left_join(iotc.Sl, iotc.cou, by=c("Fleet"))
iotc.Sl= left_join(iotc.Sl, iotc.gea, by=c("Gear"))
iotc.Sl = iotc.Sl %>% select(-c(MonthStart, Fleet, Gear, Grid))#Get rid of columns with outdated codes

#Re-shape data, discard empty catch entries, and aggregate catch by year
iotc.Sl= melt(iotc.Sl, id= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","AreaID","BigCellID"))
colnames(iotc.Sl)[8:9]= c("SpeciesCode","Catch")
iotc.Sl= left_join(iotc.Sl, iotc.sppl, by= c("SpeciesCode"))
iotc.Sl= iotc.Sl[is.na(iotc.Sl$Catch)==F, ]  #Discard entries without catch, and outdated species code

iotc.Sl= aggregate(iotc.Sl$Catch, by= list(iotc.Sl$Year,iotc.Sl$FishingEntityID,iotc.Sl$CountryGroupID,iotc.Sl$Layer3GearID,iotc.Sl$GearGroupID,iotc.Sl$AreaID,iotc.Sl$BigCellID,iotc.Sl$TaxonKey,iotc.Sl$SpeciesGroupID), sum)
colnames(iotc.Sl)= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","AreaID","BigCellID","TaxonKey","SpeciesGroupID","Catch")

write.table(iotc.Sl, "INPUT IOTC Spatial Longline Catch For Spatial Matching.csv", sep=",", row.names=F)  #Export the new table to a csv file

#Datasets are now ready for spatialization

####Section I-IOTC: INITIAL DATA AND TRANSFORMATION ####
rm(list=ls())


#1.Read in databases of nominal and spatialized catch by ocean

nom=read.csv("INPUT IOTC Nominal Catch For Spatial Matching.csv",sep=",",header=T)	#Yearly nominal catch


spats=read.csv("INPUT IOTC Spatial Surface Catch For Spatial Matching.csv",sep=",",header=T)  #Yearly available spatial catch
spatl=read.csv("INPUT IOTC Spatial Longline Catch For Spatial Matching.csv",sep=",",header=T)  #Yearly available spatial catch
spatc=read.csv("INPUT IOTC Spatial Coastal Catch For Spatial Matching.csv",sep=",",header=T)	#Yearly available spatial catch

spat= rbind(spats, spatl, spatc)

nom= nom[nom$Catch!=0,] #Exclude nominal records with zero catch

spat= spat[spat$Catch!=0,]
spat= spat[is.na(spat$BigCellID)==F,]

gearres= read.csv("INPUT IOTC GearRestrictionTable.csv",sep=",")
areares= read.csv("INPUT IOTC AreaRestrictionTable.csv",sep=",")

tot.nom.ct=sum(nom$Catch)	#Total catch in nominal database; this is used to compare catches after spatializing.
tot.spat.ct= sum(spat$Catch)	#Total catch in spatial database; this is not used in the routine but is available to look at.

rm(list=c("spats","spatl","spatc"))


#Add record IDs
nom$NID= 1:dim(nom)[1]


#Add ID categories everywhere

final.categ= c("NID","Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","SpatCatch","MatchID")
final.categ.names= c("NID","Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","Catch","MatchID")



####Section II-IOTC: PERFECT MATCH ####

#First match all the best-case scenarios (i.e. perfect data match)
#This doesn't need any spatial data subsetting

nom.data= nom
match.categ= list(spat$FishingEntityID, spat$CountryGroupID,spat$Year, spat$AreaID, spat$Layer3GearID, spat$GearGroupID, spat$TaxonKey, spat$SpeciesGroupID)
merge.categ= c("FishingEntityID", "CountryGroupID","Year","AreaID","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID")
match.categ.names= c(merge.categ,"TotalCatch")
matchid= 1


#1.Sum up the total catch, by all categories, in all reported spatial cells.

ct.all= aggregate(spat$Catch,by= match.categ,sum)  #Total catch in all cells, all categories

colnames(ct.all)= match.categ.names #Add matching column names

#2.Transform reported catch per cell into proportions of the total for all cells, by all categories.

spat.all= merge(spat,ct.all,by= merge.categ)	#Spatialized catch total in all cells by Year/GearGroupID/TaxonKey

spat.all$Proportion= spat.all$Catch / spat.all$TotalCatch	#Proportion of catch per cell = catch / total catch in cells reported for that category combo

spat.all= spat.all[,c(merge.categ,"BigCellID","Proportion")]	#Remove the catch per cell in this dataframe (but leave the proportions)

#3.Match nominal and cell proportion catch by all categories.
new.db= left_join(nom.data,spat.all,by= merge.categ)#Match and merge the spatialized (with proportions) and 
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




####Section IIIa-IOTC: Matching Algorithm ####

#This is the key function. Same as above but does not return the non-matched data as this is unnecessary
#given the functions below
#This function takes the categories for the level of the match and spatializes 
#the catch where matches are found for those match categories
#Match categories come from the loop process below where the function is called repeatedly for different levels of match categories and year ranges within a country



data.match= function(nom.dataY, spat, db.match, match.categ, merge.categ, match.categ.names, matchid )
{
  ct.all= aggregate(spatY$Catch,by= match.categ,sum)
  colnames(ct.all)= match.categ.names
  spat.all= left_join(spatY,ct.all,by= merge.categ)
  spat.all$Proportion= spat.all$Catch / spat.all$TotalCatch
  spat.all= spat.all[,c(merge.categ,"BigCellID","Proportion")]
  new.db= left_join(nom.dataY,spat.all,by= merge.categ)
  new.db$SpatCatch= new.db$Catch * new.db$Proportion
  new.db$MatchID= matchid
  db.match.new= filter(new.db,is.na(BigCellID)==F)
  db.match.new= db.match.new[,final.categ]
  colnames(db.match.new)= final.categ.names
  db.match= rbind(db.match, db.match.new)
  rm(list=c("ct.all","spat.all","new.db","db.match.new"))

  return(list(db.match=db.match))
}


fids= sort(unique(db.nomatch$FishingEntityID)) #Vector of unique FishingEntityIDs in the nominal data

YRs= c(0,2,5) #Year ranges for subsetting (i.e. +/- years to match over) TC Removed 1, 10, 15 as these are saying that categories are more important than time. This will bias results towards future tuna fisheries rather than spatial expansion. 

for(i in 1:length(fids)) #Loop over countries
{  
  ci= fids[i] #Test by changing index to desired FishingEntityID number
  
  nom.datai= subset(db.nomatch, FishingEntityID==ci) #Nominal data to match for a country
  
  gears= subset(gearres, FishingEntityID==ci)[,c("FishingEntityID","Layer3GearID")] #Fishing gears used by that country
  if(dim(gears)[1] == 0) {gears= cbind("FishingEntityID"=ci, "Layer3GearID"=unique(spat$Layer3GearID))} #If no restriction, use all gears
  y= merge(spat, gears, by="Layer3GearID",all.x=T) #Match country gears to spatial data
  
  cells= subset(areares, FishingEntityID==ci)[,c("FishingEntityID","BigCellID")] #Cells used by that country
  if(dim(cells)[1] == 0) {cells= cbind("FishingEntityID"=ci, "BigCellID"=unique(spat$BigCellID))} #If no restriction, use all cells
  y2= merge(y, cells, by="BigCellID",all.x=T)#Match country cells to spatial data
  
  spati= subset(y2, FishingEntityID.y==ci & FishingEntityID==ci) #Only use spatial data that matched to both the allowed country gears and cells
  spati= spati[,-c(11,12)]; colnames(spati)[4]= "FishingEntityID"#Keep necessary data and update column name UPDATE check this for errors
  
  rm(list=c("y","y2","gears","cells")) #Remove interim datasets
  
  yrs= sort(unique(nom.datai$Year)) #Unique years in the country nominal data
  
  NM= nom.datai #Non-matched data that will be used for the country. This should be the only time this nom.datai gets used each loop
  
  rni= dim(nom.datai)[1] #Rows in original country non-matched data
  
  for(z in 1:17) #Seventeen is the number of category combos in the ifelse statment below
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
        spatY= subset(spati, Year<=yrj+YR & Year>=yrj-YR ) #Only use data for a given year range
        
        #print("Years spatial");print(sort(unique(spatY$Year)))  #Current years in range for troubleshooting
        
        #Depending on the step z, match by decreasing number of categories (this must be updated manually)
        if(z==1){   match.categ= list(spatY$FishingEntityID,spatY$AreaID, spatY$Layer3GearID, spatY$TaxonKey) 
        merge.categ= c("FishingEntityID", "AreaID","Layer3GearID","TaxonKey")
        
        } else if(z==2){   match.categ= list(spatY$FishingEntityID, spatY$AreaID, spatY$GearGroupID, spatY$TaxonKey) 
        merge.categ= c("FishingEntityID", "AreaID","GearGroupID","TaxonKey")
        
        } else if(z==3){   match.categ= list(spatY$FishingEntityID, spatY$AreaID, spatY$Layer3GearID, spatY$SpeciesGroupID) 
        merge.categ= c("FishingEntityID", "AreaID","Layer3GearID", "SpeciesGroupID")
        
        } else if(z==4){   match.categ= list(spatY$FishingEntityID, spatY$AreaID, spatY$GearGroupID, spatY$SpeciesGroupID) 
        merge.categ= c("FishingEntityID", "AreaID","GearGroupID", "SpeciesGroupID")
        
        } else if(z==5){   match.categ= list(spatY$FishingEntityID, spatY$AreaID) 
        merge.categ= c("FishingEntityID", "AreaID")
        
        } else if(z==6){   match.categ= list(spatY$CountryGroupID,spatY$AreaID, spatY$Layer3GearID, spatY$TaxonKey) 
        merge.categ= c("CountryGroupID", "AreaID","Layer3GearID","TaxonKey")
        
        } else if(z==7){   match.categ= list(spatY$CountryGroupID, spatY$AreaID, spatY$GearGroupID, spatY$TaxonKey) 
        merge.categ= c("CountryGroupID", "AreaID","GearGroupID","TaxonKey")
        
        } else if(z==8){   match.categ= list(spatY$CountryGroupID, spatY$AreaID, spatY$Layer3GearID, spatY$SpeciesGroupID) 
        merge.categ= c("CountryGroupID", "AreaID","Layer3GearID", "SpeciesGroupID")
        
        } else if(z==9){   match.categ= list(spatY$CountryGroupID, spatY$AreaID, spatY$GearGroupID, spatY$SpeciesGroupID) 
        merge.categ= c("CountryGroupID", "AreaID","GearGroupID", "SpeciesGroupID")
        
        } else if(z==10){   match.categ= list(spatY$AreaID, spatY$Layer3GearID, spatY$TaxonKey) 
        merge.categ= c("AreaID","Layer3GearID","TaxonKey")
        
        } else if(z==11){   match.categ= list(spatY$AreaID, spatY$GearGroupID, spatY$SpeciesGroupID) 
        merge.categ= c("AreaID","GearGroupID","SpeciesGroupID")
        
        } else if(z==12){   match.categ= list(spatY$AreaID, spatY$TaxonKey) 
        merge.categ= c("AreaID","TaxonKey")
        
        } else if(z==13){   match.categ= list(spatY$AreaID, spatY$SpeciesGroupID) 
        merge.categ= c("AreaID","SpeciesGroupID")
        
        } else if(z==14){   match.categ= list(spatY$AreaID, spatY$Layer3GearID) 
        merge.categ= c("AreaID","Layer3GearID")
        
        } else if(z==15){   match.categ= list(spatY$AreaID, spatY$GearGroupID) 
        merge.categ= c("AreaID","GearGroupID")
        
        } else if(z==16){   match.categ= list(spatY$AreaID, spatY$CountryGroupID) 
        merge.categ= c("AreaID", "CountryGroupID") 
        
        } else if(z==17){   match.categ= list(spatY$AreaID) 
        merge.categ= c("AreaID") }        
        
        match.categ.names= c(merge.categ,"TotalCatch") #This gets done after merge.categ if statements
        matchid= z+1 #Add 1 because the first match (outside the loop) is ID 1
        
        #Run the data.match function using the current spatial category data and years. If spatial data or nominal data do not exist for whatever reason 
        #(no data in those years, nominal data is already matched), do nothing and keep going
        if( dim(spatY)[1]>0 & dim(nom.dataY)[1]>0 ){  db.match= data.match(nom.dataY,spatY, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.match }
        
        #After each loop, match original non-matched data to matched data and only keep what hasn't been matched yet
        NM= nom.datai #Non-matched data that will be used for the country. This should be the only time this nom.datai gets used each loop
        NM= subset( merge(NM, cbind("NID"=sort(unique(db.match$NID)), "Match"=1), by="NID",all.x=T), is.na(Match) )[,-c(11)] 
        
        nni= dim(NM)[1] #Rows in remaining non-matched data for country
        print("% Matched"); print(round((1-nni/rni) * 100, 1))
        
      } #Close year range loop
      
      
    } #Close years loop
    
    
  } #Close category loop
  
  #Aggregate potential duplicate category values to reduce size of matched database
  db.match= aggregate(db.match$Catch, by= list(db.match$NID,db.match$Year,db.match$FishingEntityID,db.match$Layer3GearID,db.match$TaxonKey,db.match$BigCellID,db.match$MatchID), sum)
  colnames(db.match)= c("NID","Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","MatchID","Catch")
  
}  #Close country loop 



####Section IIIb-IOTC: Matching Algorithm - Re-Run with Larger Year Ranges ####

db.nomatch = nom #Re-set db.nomatch
#Make db.nomatch equal to all nom catch except those already matched as indicated by NIDs:
db.nomatch = subset( merge(db.nomatch, cbind("NID"=sort(unique(db.match$NID)), "Match"=1), by="NID",all.x=T), is.na(Match) )[,-c(11)]
#Re-run matching sequence above with expanded year range on only unmatched nom catch

sum(db.match$Catch)/sum(nom$Catch) #If equal to 1, skip to end and export data 


fids= sort(unique(db.nomatch$FishingEntityID)) #Vector of unique FishingEntityIDs in the nominal data

YRs= c(10,20,40) #Year ranges for subsetting (i.e. +/- years to match over) 
#Note, IOTC needs a larger subset range due to poor data quality in the earlier years

for(i in 1:length(fids)) #Loop over countries
  
{
  ci= fids[i] #Test by changing index to desired FishingEntityID number
  
  nom.datai= subset(db.nomatch, FishingEntityID==ci) #Nominal data to match for a country
  
  gears= subset(gearres, FishingEntityID==ci)[,c("FishingEntityID","Layer3GearID")] #Fishing gears used by that country
  if(dim(gears)[1] == 0) {gears= cbind("FishingEntityID"=ci, "Layer3GearID"=unique(spat$Layer3GearID))} #If no restriction, use all gears
  y= merge(spat, gears, by="Layer3GearID",all.x=T) #Match country gears to spatial data
  
  cells= subset(areares, FishingEntityID==ci)[,c("FishingEntityID","BigCellID")] #Cells used by that country
  if(dim(cells)[1] == 0) {cells= cbind("FishingEntityID"=ci, "BigCellID"=unique(spat$BigCellID))} #If no restriction, use all cells
  y2= merge(y, cells, by="BigCellID",all.x=T)#Match country cells to spatial data
  
  spati= subset(y2, FishingEntityID.y==ci & FishingEntityID==ci) #Only use spatial data that matched to both the allowed country gears and cells
  spati= spati[,-c(11,12)]; colnames(spati)[4]= "FishingEntityID"#Keep necessary data and update column name UPDATE check this for errors
  
  rm(list=c("y","y2","gears","cells")) #Remove interim datasets
  
  yrs= sort(unique(nom.datai$Year)) #Unique years in the country nominal data
  
  NM= nom.datai #Non-matched data that will be used for the country. This should be the only time this nom.datai gets used each loop
  
  rni= dim(nom.datai)[1] #Rows in original country non-matched data
  
  for(z in 18:34) #17 is the number of category combos in the ifelse statment below
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
        spatY= subset(spati, Year<=yrj+YR & Year>=yrj-YR ) #Only use data for a given year range
        
        #print("Years spatial");print(sort(unique(spatY$Year)))  #Current years in range for troubleshooting
        
        #Depending on the step z, match by decreasing number of categories (this must be updated manually)
        if(z==18){   match.categ= list(spatY$FishingEntityID,spatY$AreaID, spatY$Layer3GearID, spatY$TaxonKey) 
        merge.categ= c("FishingEntityID", "AreaID","Layer3GearID","TaxonKey")
        
        } else if(z==19){   match.categ= list(spatY$FishingEntityID, spatY$AreaID, spatY$GearGroupID, spatY$TaxonKey) 
        merge.categ= c("FishingEntityID", "AreaID","GearGroupID","TaxonKey")
        
        } else if(z==20){   match.categ= list(spatY$FishingEntityID, spatY$AreaID, spatY$Layer3GearID, spatY$SpeciesGroupID) 
        merge.categ= c("FishingEntityID", "AreaID","Layer3GearID", "SpeciesGroupID")
        
        } else if(z==21){   match.categ= list(spatY$FishingEntityID, spatY$AreaID, spatY$GearGroupID, spatY$SpeciesGroupID) 
        merge.categ= c("FishingEntityID", "AreaID","GearGroupID", "SpeciesGroupID")
        
        } else if(z==22){   match.categ= list(spatY$FishingEntityID, spatY$AreaID) 
        merge.categ= c("FishingEntityID", "AreaID")
        
        } else if(z==23){   match.categ= list(spatY$CountryGroupID,spatY$AreaID, spatY$Layer3GearID, spatY$TaxonKey) 
        merge.categ= c("CountryGroupID", "AreaID","Layer3GearID","TaxonKey")
        
        } else if(z==24){   match.categ= list(spatY$CountryGroupID, spatY$AreaID, spatY$GearGroupID, spatY$TaxonKey) 
        merge.categ= c("CountryGroupID", "AreaID","GearGroupID","TaxonKey")
        
        } else if(z==25){   match.categ= list(spatY$CountryGroupID, spatY$AreaID, spatY$Layer3GearID, spatY$SpeciesGroupID) 
        merge.categ= c("CountryGroupID", "AreaID","Layer3GearID", "SpeciesGroupID")
        
        } else if(z==26){   match.categ= list(spatY$CountryGroupID, spatY$AreaID, spatY$GearGroupID, spatY$SpeciesGroupID) 
        merge.categ= c("CountryGroupID", "AreaID","GearGroupID", "SpeciesGroupID")
        
        } else if(z==27){   match.categ= list(spatY$AreaID, spatY$Layer3GearID, spatY$TaxonKey) 
        merge.categ= c("AreaID","Layer3GearID","TaxonKey")
        
        } else if(z==28){   match.categ= list(spatY$AreaID, spatY$GearGroupID, spatY$SpeciesGroupID) 
        merge.categ= c("AreaID","GearGroupID","SpeciesGroupID")
        
        } else if(z==29){   match.categ= list(spatY$AreaID, spatY$TaxonKey) 
        merge.categ= c("AreaID","TaxonKey")
        
        } else if(z==30){   match.categ= list(spatY$AreaID, spatY$SpeciesGroupID) 
        merge.categ= c("AreaID","SpeciesGroupID")
        
        } else if(z==31){   match.categ= list(spatY$AreaID, spatY$Layer3GearID) 
        merge.categ= c("AreaID","Layer3GearID")
        
        } else if(z==32){   match.categ= list(spatY$AreaID, spatY$GearGroupID) 
        merge.categ= c("AreaID","GearGroupID")
        
        } else if(z==33){   match.categ= list(spatY$AreaID, spatY$CountryGroupID) 
        merge.categ= c("AreaID", "CountryGroupID")
        
        } else if(z==34){   match.categ= list(spatY$AreaID) 
        merge.categ= c("AreaID") }       
        
        match.categ.names= c(merge.categ,"TotalCatch") #This gets done after merge.categ if statements
        matchid= z+1 #Add 1 because the first match (outside the loop) is ID 1
        
        #Run the data.match function using the current spatial category data and years. If spatial data or nominal data do not exist for whatever reason 
        #(no data in those years, nominal data is already matched), do nothing and keep going
        if( dim(spatY)[1]>0 & dim(nom.dataY)[1]>0 ){  db.match= data.match(nom.dataY,spatY, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.match }
        
        #After each loop, match original non-matched data to matched data and only keep what hasn't been matched yet
        NM= nom.datai #Non-matched data that will be used for the country. This should be the only time this nom.datai gets used each loop
        NM= subset( merge(NM, cbind("NID"=sort(unique(db.match$NID)), "Match"=1), by="NID",all.x=T), is.na(Match) )[,-c(11)] 
        
        nni= dim(NM)[1] #Rows in remaining non-matched data for country
        print("% Matched"); print(round((1-nni/rni) * 100, 1))
        
      } #Close year range loop
      
      
    } #Close years loop
    
    
  } #Close category loop
  
  #Aggregate potential duplicate category values to reduce size of matched database
  db.match= aggregate(db.match$Catch, by= list(db.match$NID,db.match$Year,db.match$FishingEntityID,db.match$Layer3GearID,db.match$TaxonKey,db.match$BigCellID,db.match$MatchID), sum)
  colnames(db.match)= c("NID","Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","MatchID","Catch")
  
}  #Close country loop 

db.nomatch = nom #Re-set db.nomatch
#Make db.nomatch equal to all nom catch except those already matched as indicated by NIDs:
db.nomatch = subset( merge(db.nomatch, cbind("NID"=sort(unique(db.match$NID)), "Match"=1), by="NID",all.x=T), is.na(Match) )[,-c(11)]
#Re-run matching sequence above with expanded year range on only unmatched nom catch

sum(db.match$Catch)/sum(nom$Catch) #If equal to 1, follow through and export data 




#Aggregate potential duplicate category values to reduce size of matched database, drop NID column
db.match= aggregate(db.match$Catch, by= list(db.match$Year,db.match$FishingEntityID,db.match$Layer3GearID,db.match$TaxonKey,db.match$BigCellID,db.match$MatchID), sum)
colnames(db.match)= c("Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","MatchID","Catch")




#### Section IV-IOTC: Testing ####

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
write.table(agc, "OUTPUT IOTC Catch by Year and MatchID.csv", sep=",",row.names=F, quote=F)



#### Section V-IOTC: Writing ####

db.match= db.match[order(db.match$Year),]

db.match= aggregate(db.match$Catch, by= list(db.match$Year,db.match$FishingEntityID,db.match$Layer3GearID,db.match$TaxonKey,db.match$BigCellID,db.match$MatchID), sum)
colnames(db.match)= c("Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","MatchID","Catch")


db.match$RFMOID= 7  #RFMO i.d. for IOTC = 7
db.match= db.match[ order(db.match$Year), c("RFMOID","Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","Catch","MatchID") ]


write.table(db.match[1:dim(db.match)[1], ], "../Outputs/OUTPUT IOTC Spatialized Catch 1 of 1.csv", sep=",", row.names=F, quote=F)













#ICCAT-----
####ICCAT nominal data formatting####

#Clear any stored data for faster computations
rm(list=ls())
setwd("../ICCAT")

#Read nominal data
iccat.n= read.csv("INPUT ICCAT Nominal Catch for Formatting.csv", sep=",") 

iccat.n= iccat.n[iccat.n$Catch!=0 & is.na(iccat.n$Catch)==F & iccat.n$Year>=1950, ]

#Read nominal data codes
#Download these reference files from the Sea Around Us GitHub Repository
iccat.gea= read.table("INPUT ICCAT Nominal Gear Codes.txt", header=T)
iccat.spp= read.table("INPUT ICCAT Nominal Species Codes.txt", header=T)
iccat.cou= read.csv("INPUT ICCAT Nominal Country Codes.csv", sep=",")

iccat.N= merge(iccat.n, iccat.cou, by= c("Flag"), all.x=T)
iccat.N= merge(iccat.N, iccat.gea, by= c("GearCode"), all.x=T)
iccat.N= merge(iccat.N, iccat.spp, by= c("SpeciesCode"), all.x=T)

iccat.N= iccat.N[ ,-c(1:3) ]


#Disaggregate former countries and joint ventures into individual fishing entities

#France and Spain country split
x <- iccat.N[iccat.N$FishingEntityID!=3000,]
fr <- es <- iccat.N[iccat.N$FishingEntityID==3000,]#France and Spain "Mixed Flag" catch

fr$FishingEntityID= 58  #Update FishingEntityID to France/Spain
es$FishingEntityID= 165

fr$Catch= fr$Catch/2  #Split catch in half
es$Catch= es$Catch/2

iccat.N= rbind(x, fr, es)

#USSR splits
x= iccat.N[iccat.N$FishingEntityID!=2000,]
est= geo= lat= rus= ukr= iccat.N[iccat.N$FishingEntityID==2000,]

#53 Estonia, 63 Georgia, 98 Latvia, 148 Russian Fed, 181 Ukraine

soviet= rbind(x[x$FishingEntity==53,],
              x[x$FishingEntity==63,],
              x[x$FishingEntity==98,],
              x[x$FishingEntity==148,],
              x[x$FishingEntity==181,])

sov1991= soviet[soviet$Year==1991,] #Catch by former Soviet countries in 1991

soviet.prop= as.data.frame(cbind("Estonia"=sum(sov1991[sov1991$FishingEntityID==53,"Catch"])/sum(sov1991$Catch),
                                 "Georgia"=sum(sov1991[sov1991$FishingEntityID==63,"Catch"])/sum(sov1991$Catch),
                                 "Latvia"=sum(sov1991[sov1991$FishingEntityID==98,"Catch"])/sum(sov1991$Catch),
                                 "RussianFed"=sum(sov1991[sov1991$FishingEntityID==148,"Catch"])/sum(sov1991$Catch),
                                 "Ukraine"=sum(sov1991[sov1991$FishingEntityID==181,"Catch"])/sum(sov1991$Catch)))

#Export this calculation for reference
write.table(soviet.prop, "INPUT Soviet Catch Proportions 1991.txt",sep="\t", row.names = F)

est$FishingEntityID= 53; est$Catch= est$Catch * soviet.prop$Estonia
geo$FishingEntityID= 63; geo$Catch= geo$Catch * soviet.prop$Georgia
lat$FishingEntityID= 98; lat$Catch= lat$Catch * soviet.prop$Latvia
rus$FishingEntityID= 148; rus$Catch= rus$Catch * soviet.prop$RussianFed
ukr$FishingEntityID= 181; ukr$Catch= ukr$Catch * soviet.prop$Ukraine

soviets= rbind(est, geo, lat, rus, ukr)

iccat.N= rbind(x, soviets)

#Yugoslavia split
x= iccat.N[iccat.N$FishingEntityID!=4000,]
#Croatia and Montenegro are countries fishing in this data set
cro = mon = iccat.N[iccat.N$FishingEntityID==4000,]

#42 Croatia, #194 Montenegro 

yugo= rbind(x[x$FishingEntity==42,],x[x$FishingEntity==194,])

yugo1991= yugo[yugo$Year==1991,] #Catch by former Yugoslavia countries in 1991

yugo.prop= as.data.frame(cbind("Croatia"=sum(yugo1991[yugo1991$FishingEntityID==42,"Catch"])/sum(yugo1991$Catch),
                               "Montenegro"=sum(yugo1991[yugo1991$FishingEntityID==194,"Catch"])/sum(yugo1991$Catch)))

cro$FishingEntityID= 42; cro$Catch= cro$Catch * yugo.prop$Croatia
mon$FishingEntityID= 194; mon$Catch= mon$Catch * yugo.prop$Montenegro

yugoslavia= rbind(cro,mon)

iccat.N= rbind(x, yugoslavia)

#Export this calculation for reference
write.table(yugo.prop, "INPUT Yugoslavia Proportions 1991.txt", row.names = F, sep="\t")

#Make split tables for joint ventures
#FEID 3000 = France,Cote D'Ivoire and Senegal
#France = FEID 58
#Cote D'Ivoire = FEID 89
#Senegal = FEID 157

fis <- rbind(iccat.N[iccat.N$FishingEntity==58,],
             iccat.N[iccat.N$FishingEntity==89,],
             iccat.N[iccat.N$FishingEntity==157,])
#Catch by countries in 1969-1990
fis6990 <- subset(fis, Year > 1968)
fis6990 <- subset(fis6990, Year < 1991)
#Aggregate catch by year for total catch by year
tot <- aggregate(fis6990$Catch, by=list(fis6990$Year), sum)
colnames(tot) <- c("Year","TotalCatch")
#Aggregate catch by year and FEID for FEID totals
fetot <- aggregate(fis6990$Catch, by=list(fis6990$Year, fis6990$FishingEntityID), sum)
colnames(fetot) <- c("Year","FishingEntityID","Catch")
fr <- subset(fetot, FishingEntityID==58)
ci <- subset(fetot, FishingEntityID==89)
se <- subset(fetot, FishingEntityID==157)

#Join country total catch together
fis <- rbind(fr,ci,se)
fis <- merge(fis,tot, by="Year")
#Calculate proportion by year
fis$Prop <- fis$Catch / fis$TotalCatch
fisprop <- fis[,-c(3:4)]
#Export this calculation for reference
write.csv(fisprop, "INPUT France, Cote D'Ivoire and Senegal Proportions.csv", row.names = F)

#FEID 5000 = Korea and Panama
#Korea = FEID 95
#Panama = FEID 135

kp <- rbind(iccat.N[iccat.N$FishingEntity==95,],
            iccat.N[iccat.N$FishingEntity==135,])
#Catch by countries in 1974-1990
kp7490 <- subset(kp, Year > 1973)
kp7490 <- subset(kp7490, Year < 1990)
#Aggregate catch by year for total catch by year
tot <- aggregate(kp7490$Catch, by=list(kp7490$Year), sum)
colnames(tot) <- c("Year","TotalCatch")
#Aggregate catch by year and FEID for FEID totals
fetot <- aggregate(kp7490$Catch, by=list(kp7490$Year, kp7490$FishingEntityID), sum)
colnames(fetot) <- c("Year","FishingEntityID","Catch")
kr <- subset(fetot, FishingEntityID==95)
pa <- subset(fetot, FishingEntityID==135)
#Join country total catch together
fis <- rbind(pa,kr)
fis <- merge(fis,tot, by="Year")
#Calculate proportion by year
fis$Prop <- fis$Catch / fis$TotalCatch
fisprop <- fis[,-c(3:4)]
#Export this calculation for reference
write.csv(fisprop, "INPUT Korea Panama Proportions.csv", row.names = F)


#All countries disaggregated
#Allocate catch out from "All countries" fishing entity (FishingEntityID= 1000). 
x= iccat.N[iccat.N$FishingEntityID==1000,]
y= iccat.N[iccat.N$FishingEntityID!=1000,]

#Matching by year, gear, and species
y2= aggregate(y$Catch, by= list(y$Year, y$TaxonKey, y$Layer3GearID), sum)
colnames(y2)= c("Year","TaxonKey","Layer3GearID","TotalCatch")

y3= merge(y, y2, by= c("Year","TaxonKey","Layer3GearID"), all.x=T)
y3$Proportion= y3$Catch / y3$TotalCatch
y3= y3[,c("Year","Layer3GearID","GearGroupID","FishingEntityID","CountryGroupID","TaxonKey","SpeciesGroupID","Proportion")]

z= merge(x, y3, by= c("Year","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID"), all.x=T)
z$PropCatch= z$Catch * z$Proportion

zm1= z[is.na(z$Proportion)==F,]
znm1= z[is.na(z$Proportion),]

zm1= zm1[,c("Year","FishingEntityID.y","CountryGroupID.y","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","PropCatch")]
colnames(zm1)= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","Catch")

x= rbind(y, zm1)

iccat.N= aggregate(x$Catch, by= list(x$Year,x$FishingEntityID,x$CountryGroupID,x$Layer3GearID,x$GearGroupID,x$TaxonKey,x$SpeciesGroupID), sum)
colnames(iccat.N)= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","Catch")


iccat.N= iccat.N[order(iccat.N$Year), ]

#Remove NA values
iccat.N <- iccat.N[is.na(iccat.N$Catch)==F,]

#Export formatted nominal catch data file for spatialization
write.table(iccat.N, "INPUT ICCAT Nominal Catch For Spatial Matching.csv", sep=",", row.names=F)


####ICCAT spatial data formatting####

#Clear any stored data for faster computations
rm(list=ls())

#Read spatial catch data
iccat.s= read.csv("INPUT ICCAT Spatial Catch for Formatting.csv", sep=",")

iccat.s= iccat.s[iccat.s$CatchUnit=="kg", ]

#Read spatial data codes
#Download these reference files from the Sea Around Us GitHub Repository
iccat.spp= read.table("INPUT ICCAT Spatial Species Codes.txt", header=T)
iccat.cel= read.csv("INPUT ICCAT Spatial Cell Codes.csv", sep=",")
iccat.gea= read.table("INPUT ICCAT Spatial Gear Codes.txt", header=T)
iccat.cou= read.csv("INPUT ICCAT Spatial Country Codes.csv", sep=",")
cellid= read.csv("INPUT CellTypeID.csv",sep=",",header=T)

#Reshape data into long form
iccat.S <- melt(iccat.s, id= c("FleetID","GearCode","Year","SquareTypeCode","QuadID","Lat","Lon","CatchUnit"))
colnames(iccat.S)[9:10] <- c("SpeciesCode","Catch")

#Remove rows with no spatial data and where catch=0
iccat.S <- iccat.S[iccat.S$SquareTypeCode != "none",]
iccat.S <- iccat.S[iccat.S$Catch != 0,]

#Merge data with fleet and gear codes
iccat.S= left_join(iccat.S, iccat.cou, by= c("FleetID"))
iccat.S= left_join(iccat.S, iccat.gea, by= c("GearCode"))

#Some Spain longline data for 2010-2016 is erroniously reported in 1x1 when we believe it should be 5x5
#This creates a grid of "hotspot" cells when mapped, concentrating a 5x5 cells worth of catch into a 1x1 cell in 5 degree intervals
#subset Spain's 1x1 catch
Spain <- iccat.S[iccat.S$FishingEntityID==165,]
World <- iccat.S[iccat.S$FishingEntityID!=165,]

Spain1x1 <- Spain[Spain$SquareTypeCode=="1x1",]
SpainOther <- Spain[Spain$SquareTypeCode!="1x1",]

#identify 1x1s reported as longline
longline <- Spain1x1[Spain1x1$GearGroupID==302,]
othergear <- Spain1x1[Spain1x1$GearGroupID!=302,]

longline2010 <- longline[longline$Year >= 2009,]
longlinepre2010 <- longline[longline$Year < 2009,]

#Write a csv to look at this catch in GIS to verify it is creating the grid pattern
write.csv(longline2010, "OUTPUT ICCAT Spain 1x1 Longline.csv",row.names = F)

#reassign the 1x1 BigCellTypeID to 5x5
longline2010$SquareTypeCode <- "5x5"

#bind everything together for fixed spatial dataset, check if it is the same size as original iccat.S
iccat.SNEW <- rbind(World,SpainOther,othergear,longlinepre2010,longline2010)
#make it the new iccat.S
iccat.S <- iccat.SNEW

#join with species and cell codes
iccat.S <- left_join(iccat.S, iccat.cel, by= c("SquareTypeCode","QuadID", "Lon","Lat"))

iccat.S <- left_join(iccat.S, iccat.spp, by= c("SpeciesCode"))
iccat.S <- left_join(iccat.S, cellid, by= c("BigCellTypeID", "x","y"))

#Remove newly redundant columns, keep QuadID for all countries matching later
iccat.S <- iccat.S %>% select(-c(FleetID, GearCode,SquareTypeCode, Lat, Lon, CatchUnit, SpeciesCode))

#Identify the rows that don't match to a big cell and write a table to look at them
iccat.NoMatch <- iccat.S[(is.na(iccat.S$BigCellID)==T),]
write.csv(iccat.NoMatch, "OUTPUT ICCAT BigCellID No Match.csv", row.names = F)

#Remove no matches, they are erroniously reported on land
iccat.S <- iccat.S[(is.na(iccat.S$BigCellID)==F),]

#Convert catch from kilograms to tonnes
iccat.S$Catch <- iccat.S$Catch / 1000 

#Aggregate catch
iccat.S <- aggregate(iccat.S$Catch, by=list(iccat.S$Year,iccat.S$FishingEntityID,iccat.S$CountryGroupID,iccat.S$Layer3GearID,iccat.S$GearGroupID,iccat.S$BigCellID,iccat.S$BigCellTypeID,iccat.S$x,iccat.S$y,iccat.S$QuadID, iccat.S$TaxonKey,iccat.S$SpeciesGroupID), sum)
#Rename columns 
colnames(iccat.S) <- c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","BigCellID","BigCellTypeID","x","y","QuadID","TaxonKey","SpeciesGroupID","Catch")

#Remove stored data
rm(cellid,iccat.cel,iccat.cou,iccat.gea,iccat.NoMatch,iccat.s,iccat.spp,iccat.SNEW,longline,longline2010,longlinepre2010,othergear,Spain,Spain1x1,SpainOther,World)


#Fishing entity splits
#sum of catch to make sure none is lost
test <- sum(iccat.S$Catch)

#All countries split
#Allocate catch out from "All countries" fishing entity (FishingEntityID= 1000). 
x <- iccat.S[iccat.S$FishingEntityID==1000,]
y <- iccat.S[iccat.S$FishingEntityID!=1000,]

#Remove iccat.S for RAM space
rm(iccat.S)

#Matching by year, gear, bigcellid, and species
y2 <- aggregate(y$Catch, by= list(y$Year, y$TaxonKey, y$Layer3GearID, y$BigCellID), sum)
colnames(y2) <- c("Year","TaxonKey","Layer3GearID","BigCellID","TotalCatch")

y3 <- merge(y, y2, by= c("Year","TaxonKey","Layer3GearID","BigCellID"), all.x=T)
y3$Proportion <- y3$Catch / y3$TotalCatch
y3 <- y3[,c("Year","Layer3GearID","GearGroupID","FishingEntityID","CountryGroupID","TaxonKey","SpeciesGroupID","QuadID","BigCellID","Proportion")]

z <- left_join(x, y3, by= c("Year","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","QuadID","BigCellID"))
z$PropCatch <- z$Catch * z$Proportion

zm1 <- z[is.na(z$Proportion)==F,]
znm1 <- z[is.na(z$Proportion),]

zm1 <- zm1[,c("Year","FishingEntityID.y","CountryGroupID.y","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","QuadID","BigCellID","PropCatch")]
colnames(zm1) <- c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","QuadID","BigCellID","Catch")

znm1 <- znm1[,c("Year","FishingEntityID.x","CountryGroupID.x","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","QuadID","BigCellID","Catch")]
colnames(znm1) <- c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","QuadID","BigCellID","Catch")


#Matching by year, gear, quadid, and species
y2 <- aggregate(y$Catch, by= list(y$Year, y$TaxonKey, y$Layer3GearID, y$QuadID), sum)
colnames(y2) <- c("Year","TaxonKey","Layer3GearID","QuadID","TotalCatch")

y3 <- left_join(y, y2, by= c("Year","TaxonKey","Layer3GearID","QuadID"))
y3$Proportion <- y3$Catch / y3$TotalCatch
y3 <- y3[,c("Year","Layer3GearID","GearGroupID","FishingEntityID","CountryGroupID","TaxonKey","SpeciesGroupID","QuadID","BigCellID","Proportion")]

z <- left_join(znm1, y3, by= c("Year","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","QuadID"))
z$PropCatch <- z$Catch * z$Proportion

zm2 <- z[is.na(z$Proportion)==F,]
znm2 <- z[is.na(z$Proportion),]

zm2 <- zm2[,c("Year","FishingEntityID.y","CountryGroupID.y","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","QuadID","BigCellID.x","PropCatch")]
colnames(zm2) <- c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","QuadID","BigCellID","Catch")

znm2 <- znm2[,c("Year","FishingEntityID.x","CountryGroupID.x","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","QuadID","BigCellID.x","Catch")]
colnames(znm2) <- c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","QuadID","BigCellID","Catch")

#Matching by year, geargroupid, quadid, and species
y2 <- aggregate(y$Catch, by= list(y$Year, y$TaxonKey, y$GearGroupID, y$QuadID), sum)
colnames(y2) <- c("Year","TaxonKey","GearGroupID","QuadID","TotalCatch")

y3 <- left_join(y, y2, by= c("Year","TaxonKey","GearGroupID","QuadID"))
y3$Proportion <- y3$Catch / y3$TotalCatch
y3 <- y3[,c("Year","Layer3GearID","GearGroupID","FishingEntityID","CountryGroupID","TaxonKey","SpeciesGroupID","QuadID","BigCellID","Proportion")]

z <-left_join(znm2, y3, by= c("Year","GearGroupID","TaxonKey","QuadID"))
z$PropCatch <- z$Catch * z$Proportion

zm3 <- z[is.na(z$Proportion)==F,]
znm3 <- z[is.na(z$Proportion),]

zm3 <- zm3[,c("Year","FishingEntityID.y","CountryGroupID.y","Layer3GearID.x","GearGroupID","TaxonKey","SpeciesGroupID.x","QuadID","BigCellID.x","PropCatch")]
colnames(zm3) <- c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","QuadID","BigCellID","Catch")

znm3 <- znm3[,c("Year","FishingEntityID.x","CountryGroupID.x","Layer3GearID.x","GearGroupID","TaxonKey","SpeciesGroupID.x","QuadID","BigCellID.x","Catch")]
colnames(znm3) <- c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","QuadID","BigCellID","Catch")

#Matching by year, quadid, and species
y2 <- aggregate(y$Catch, by= list(y$Year, y$TaxonKey, y$QuadID), sum)
colnames(y2) <- c("Year","TaxonKey","QuadID","TotalCatch")

y3 <- left_join(y, y2, by= c("Year","TaxonKey","QuadID"))
y3$Proportion <- y3$Catch / y3$TotalCatch
y3 <- y3[,c("Year","Layer3GearID","GearGroupID","FishingEntityID","CountryGroupID","TaxonKey","SpeciesGroupID","QuadID","BigCellID","Proportion")]

z <- left_join(znm3, y3, by= c("Year","TaxonKey","QuadID"))
z$PropCatch <- z$Catch * z$Proportion

zm4 <- z[is.na(z$Proportion)==F,]
znm4 <- z[is.na(z$Proportion),]

zm4 <- zm4[,c("Year","FishingEntityID.y","CountryGroupID.y","Layer3GearID.x","GearGroupID.x","TaxonKey","SpeciesGroupID.x","QuadID","BigCellID.x","PropCatch")]
colnames(zm4) <- c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","QuadID","BigCellID","Catch")

znm4 <- znm4[,c("Year","FishingEntityID.x","CountryGroupID.x","Layer3GearID.x","GearGroupID.x","TaxonKey","SpeciesGroupID.x","QuadID","BigCellID.x","Catch")]
colnames(znm4) <- c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","QuadID","BigCellID","Catch")

#Matching by year and quadid
y2 <- aggregate(y$Catch, by= list(y$Year, y$QuadID), sum)
colnames(y2) <- c("Year","QuadID","TotalCatch")

y3 <- left_join(y, y2, by= c("Year","QuadID"))
y3$Proportion <- y3$Catch / y3$TotalCatch
y3 <- y3[,c("Year","Layer3GearID","GearGroupID","FishingEntityID","CountryGroupID","TaxonKey","SpeciesGroupID","QuadID","BigCellID","Proportion")]

z <- left_join(znm4, y3, by= c("Year","QuadID"))
z$PropCatch <- z$Catch * z$Proportion

zm5 <- z[is.na(z$Proportion)==F,]
znm5 <- z[is.na(z$Proportion),]

zm5 <- zm5[,c("Year","FishingEntityID.y","CountryGroupID.y","Layer3GearID.x","GearGroupID.x","TaxonKey.x","SpeciesGroupID.x","QuadID","BigCellID.x","PropCatch")]
colnames(zm5) <- c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","QuadID","BigCellID","Catch")

#Join all the matched data, remove QuadID
z <- rbind(zm1, zm2, zm3, zm4, zm5)
z <- z %>% select(-QuadID)

#Remove QuadID, reorder for rbind
y <- y[,c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","QuadID","BigCellID","Catch")]
y <- y %>% select(-QuadID)
#Bind together
x <- rbind(y, z) #Join the split 'All countries' data with the other data


#Aggregate any repeated entries (due to the split-up)
iccat.S <- aggregate(x$Catch, by= list(x$Year, x$FishingEntityID, x$CountryGroupID, x$Layer3GearID, x$GearGroupID, x$TaxonKey, x$SpeciesGroupID, x$BigCellID), sum) 
colnames(iccat.S) <- c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","BigCellID","Catch")

#Memory clean up
rm(list = c("y","y2","y3","z","znm1","zm2","zm3","zm4","zm5","zm1","znm2","znm3","znm4","znm5"))

test2 <- sum(iccat.S$Catch)

#USSR splits
x <- iccat.S[iccat.S$FishingEntityID!=2000,]
est= geo= lat= rus= ukr= iccat.S[iccat.S$FishingEntityID==2000,]

#53 Estonia, 63 Georgia, 98 Latvia, 148 Russian Fed, 181 Ukraine
soviet.prop <- read.table("INPUT Soviet Catch Proportions 1991.txt", header=T)

est$FishingEntityID <- 53; est$Catch= est$Catch * soviet.prop$Estonia
geo$FishingEntityID <- 63; geo$Catch= geo$Catch * soviet.prop$Georgia
lat$FishingEntityID <- 98; lat$Catch= lat$Catch * soviet.prop$Latvia
rus$FishingEntityID <- 148; rus$Catch= rus$Catch * soviet.prop$RussianFed
ukr$FishingEntityID <- 181; ukr$Catch= ukr$Catch * soviet.prop$Ukraine

soviets <- rbind(est, geo, lat, rus, ukr)

iccat.S <- rbind(x, soviets)

test3 <- sum(iccat.S$Catch)

#France + C. Ivoire + Senegal = Fleet800-00 = FEID3000
#France = FEID 58
#Cote D'Ivoire = FEID 89
#Senegal = FEID 157
x <- iccat.S[iccat.S$FishingEntityID!=3000,]
fr= ci= se= iccat.S[iccat.S$FishingEntityID==3000,]
fis.prop <- read.csv("INPUT France, Cote D'Ivoire and Senegal Proportions.csv", header=T)

fr$FishingEntityID <- 58
fr <- merge (fr,fis.prop, by=c("FishingEntityID","Year"))
fr$SplitCatch <- fr$Catch * fr$Prop

ci$FishingEntityID <- 89
ci <- merge (ci,fis.prop, by=c("FishingEntityID","Year"))
ci$SplitCatch <- ci$Catch * ci$Prop

se$FishingEntityID <- 157
se <- merge (se,fis.prop, by=c("FishingEntityID","Year"))
se$SplitCatch <- se$Catch * se$Prop

fis <- rbind(fr,ci,se)
fis <- fis[-c(9:10)]
colnames(fis)[colnames(fis) == "SplitCatch"] <- "Catch"
iccat.S <- rbind(x, fis)

test4 <- sum(iccat.S$Catch)

#Korea + Panama = Fleet801-00 = FEID5000
#Korea = FEID 95
#Panama = FEID 135
x <- iccat.S[iccat.S$FishingEntityID!=5000,]
kr= pa= iccat.S[iccat.S$FishingEntityID==5000,]
kp.prop <- read.csv("INPUT Korea Panama Proportions.csv", header=T)

kr$FishingEntityID <- 95
kr <- merge (kr,kp.prop, by=c("FishingEntityID","Year"))
kr$SplitCatch <- kr$Catch * kr$Prop

pa$FishingEntityID <- 135
pa <- merge (pa,kp.prop, by=c("FishingEntityID","Year"))
pa$SplitCatch <- pa$Catch * pa$Prop

kp <- rbind(kr,pa)
kp <- kp[-c(9:10)]
colnames(kp)[colnames(kp) == "SplitCatch"] <- "Catch"

sum(kp$SplitCatch)
sum(x$Catch)
iccat.S <- rbind(x, kp)

test5 <- sum(iccat.S$Catch)

#Yugoslavia split
x <- iccat.S[iccat.S$FishingEntityID!=4000,]
#Croatia and Montenegro are countries fishing in this data set
cro = mon = iccat.S[iccat.S$FishingEntityID==4000,]
yugo.prop <- read.table("INPUT Yugoslavia Proportions 1991.txt", header = T)
#42 Croatia
#194 Montenegro 
cro$FishingEntityID <- 42 
cro$Catch <- cro$Catch * yugo.prop$Croatia
mon$FishingEntityID <- 194
mon$Catch <- mon$Catch * yugo.prop$Montenegro
yugo <- rbind(cro,mon)
iccat.S <- rbind(x, yugo)

#Sum up catch from splits
iccat.S <- aggregate(iccat.S$Catch, by= list(iccat.S$Year, iccat.S$FishingEntityID, iccat.S$CountryGroupID, iccat.S$Layer3GearID, iccat.S$GearGroupID, iccat.S$TaxonKey, iccat.S$SpeciesGroupID, iccat.S$BigCellID), sum) 
colnames(iccat.S) <- c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","BigCellID","Catch")

test6 <- sum(iccat.S$Catch)

#Export formatted spatial catch data file for spatialization
write.table(iccat.S, "INPUT ICCAT Spatial Catch For Spatial Matching.csv", sep=",", row.names=F)  


####Section I-ICCAT: INITIAL DATA AND TRANSFORMATION####
rm(list=ls())

#1.Read in databases of nominal and spatialized catch by ocean

nom=read.csv("INPUT ICCAT Nominal Catch For Spatial Matching.csv",sep=",",header=T)	#Yearly nominal catch

spat=read.csv("INPUT ICCAT Spatial Catch For Spatial Matching.csv",sep=",",header=T)  #Yearly available spatial catch

nom= nom[nom$Catch!=0,]

spat= spat[spat$Catch!=0,]
spat= spat[is.na(spat$BigCellID)==F,]

gearres= read.csv("INPUT ICCAT GearRestrictionTable.csv",sep=",") #Read in gear restrictions
areares= read.csv("INPUT ICCAT AreaRestrictionTable.csv",sep=",") #Read in area restrictions

tot.nom.ct=sum(nom$Catch)	#Total catch in nominal database; this is used to compare catches after spatializing.
tot.spat.ct= sum(spat$Catch)	#Total catch in spatial database; this is not used in the routine but is available to look at.


final.categ= c("Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","SpatCatch")
final.categ.names= c("Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","Catch")

#Add record IDs
nom$NID= 1:dim(nom)[1]

#Add ID categories everywhere

final.categ= c("NID","Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","SpatCatch","MatchID")
final.categ.names= c("NID","Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","Catch","MatchID")

####Section II-ICCAT: PERFECT MATCH ####

#First match all the best-case scenarios (i.e. perfect data match)
#This doesn't need any spatial data subsetting

nom.data= nom
match.categ= list(spat$FishingEntityID, spat$CountryGroupID,spat$Year, spat$Layer3GearID, spat$GearGroupID, spat$TaxonKey, spat$SpeciesGroupID)
merge.categ= c("FishingEntityID", "CountryGroupID","Year","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID")
match.categ.names= c(merge.categ,"TotalCatch")
matchid= 1


#1.Sum up the total catch, by all categories, in all reported spatial cells.

ct.all= aggregate(spat$Catch,by= match.categ,sum)  #Total catch in all cells, all categories

colnames(ct.all)= match.categ.names #Add matching column names

#2.Transform reported catch per cell into proportions of the total for all cells, by all categories.

spat.all= left_join(spat,ct.all,by= merge.categ)	#Spatialized catch total in all cells by Year/GearGroupID/TaxonKey

spat.all$Proportion= spat.all$Catch / spat.all$TotalCatch	#Proportion of catch per cell = catch / total catch in cells reported for that category combo

spat.all= spat.all[,c(merge.categ,"BigCellID","Proportion")]	#Remove the catch per cell in this dataframe (but leave the proportions)

#3.Match nominal and cell proportion catch by all categories.
new.db= left_join(nom.data ,spat.all,by= merge.categ)#Match and merge the spatialized (with proportions) and 
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


####Section IIIa-ICCAT: Matching Algorithm ####

#This is the key function. Same as above but does not return the non-matched data as this is unnecessary
#given the functions below
#This function takes the categories for the level of the match and spatializes 
#the catch where matches are found for those match categories
#Match categories come from the loop process below where the function is called repeatedly for different levels of match categories and year ranges within a country


data.match= function(nom.dataY, spat, db.match, match.categ, merge.categ, match.categ.names, matchid )
{
  ct.all= aggregate(spatY$Catch,by= match.categ,sum)
  colnames(ct.all)= match.categ.names
  spat.all= left_join(spatY,ct.all,by= merge.categ)
  spat.all$Proportion= spat.all$Catch / spat.all$TotalCatch
  spat.all= spat.all[,c(merge.categ,"BigCellID","Proportion")]
  new.db= left_join(nom.dataY,spat.all,by= merge.categ)
  new.db$SpatCatch= new.db$Catch * new.db$Proportion
  new.db$MatchID= matchid
  db.match.new= filter(new.db,is.na(BigCellID)==F)
  db.match.new= db.match.new[,final.categ]
  colnames(db.match.new)= final.categ.names
  db.match= rbind(db.match, db.match.new)
  rm(list=c("ct.all","spat.all","new.db","db.match.new"))
  
  return(list(db.match=db.match))
}



fids= sort(unique(db.nomatch$FishingEntityID)) #Vector of unique FishingEntityIDs in the nominal data

YRs= c(0,2,5) #Year ranges for subsetting (i.e. +/- years to match over) TC Removed 1, 10, 15 as these are saying that categories are more important than time. This will bias results towards future tuna fisheries rather than spatial expansion. 


for(i in 1:length(fids)) #Loop over countries
  
{
  ci= fids[i] #Test by changing index to desired FishingEntityID number
  
  nom.datai= subset(db.nomatch, FishingEntityID==ci) #Nominal data to match for a country
  
  gears= subset(gearres, FishingEntityID==ci)[,c("FishingEntityID","Layer3GearID")] #Fishing gears used by that country
  if(dim(gears)[1] == 0) {gears= cbind("FishingEntityID"=ci, "Layer3GearID"=unique(spat$Layer3GearID))} #If no restriction, use all gears
  y= merge(spat, gears, by="Layer3GearID",all.x=T) #Match country gears to spatial data
  
  cells= subset(areares, FishingEntityID==ci)[,c("FishingEntityID","BigCellID")] #Cells used by that country
  if(dim(cells)[1] == 0) {cells= cbind("FishingEntityID"=ci, "BigCellID"=unique(spat$BigCellID))} #If no restriction, use all cells
  y2= merge(y, cells, by="BigCellID",all.x=T)#Match country cells to spatial data
  
  spati= subset(y2, FishingEntityID.y==ci & FishingEntityID==ci) #Only use spatial data that matched to both the allowed country gears and cells
  spati= spati[,-c(11,12)]; colnames(spati)[4]= "FishingEntityID"#Keep necessary data and update column name UPDATE check this for errors
  
  rm(list=c("y","y2","gears","cells")) #Remove interim datasets
  
  yrs= sort(unique(nom.datai$Year)) #Unique years in the country nominal data
  
  NM= nom.datai #Non-matched data that will be used for the country. This should be the only time this nom.datai gets used each loop
  
  rni= dim(nom.datai)[1] #Rows in original country non-matched data
  
  for(z in 1:14) #Fourteen is the number of category combos in the ifelse statment below
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
        spatY= subset(spati, Year<=yrj+YR & Year>=yrj-YR ) #Only use data for a given year range
        
        #print("Years spatial");print(sort(unique(spatY$Year)))  #Current years in range for troubleshooting
        
        #Depending on the step z, match by decreasing number of categories (this must be updated manually)
        if(z==1){   match.categ= list(spatY$FishingEntityID, spatY$Layer3GearID, spatY$TaxonKey) 
        merge.categ= c("FishingEntityID","Layer3GearID","TaxonKey")
        
        } else if(z==2){   match.categ= list(spatY$FishingEntityID, spatY$Layer3GearID, spatY$SpeciesGroupID) 
        merge.categ= c("FishingEntityID", "Layer3GearID","SpeciesGroupID")
        
        } else if(z==3){   match.categ= list(spatY$FishingEntityID, spatY$Layer3GearID) 
        merge.categ= c("FishingEntityID", "Layer3GearID")
        
        } else if(z==4){   match.categ= list(spatY$FishingEntityID, spatY$GearGroupID) 
        merge.categ= c("FishingEntityID", "GearGroupID")
        
        } else if(z==5){   match.categ= list(spatY$FishingEntityID) 
        merge.categ= c("FishingEntityID")
        
        } else if(z==6){   match.categ= list(spatY$CountryGroupID, spatY$Layer3GearID, spatY$TaxonKey) 
        merge.categ= c("CountryGroupID","Layer3GearID","TaxonKey")
        
        } else if(z==7){   match.categ= list(spatY$CountryGroupID, spatY$Layer3GearID, spatY$SpeciesGroupID) 
        merge.categ= c("CountryGroupID", "Layer3GearID","SpeciesGroupID")
        
        } else if(z==8){   match.categ= list(spatY$CountryGroupID, spatY$Layer3GearID) 
        merge.categ= c("CountryGroupID", "Layer3GearID")
        
        } else if(z==9){   match.categ= list(spatY$CountryGroupID, spatY$GearGroupID) 
        merge.categ= c("CountryGroupID", "GearGroupID")
        
        } else if(z==10){   match.categ= list(spatY$TaxonKey, spatY$Layer3GearID) 
        merge.categ= c("TaxonKey", "Layer3GearID")
        
        } else if(z==11){   match.categ= list(spatY$SpeciesGroupID, spatY$Layer3GearID) 
        merge.categ= c("SpeciesGroupID", "Layer3GearID")
        
        } else if(z==12){   match.categ= list(spatY$Layer3GearID) 
        merge.categ= c("Layer3GearID")
        
        } else if(z==13){   match.categ= list(spatY$GearGroupID) 
        merge.categ= c("GearGroupID") 
        
        } else if(z==14){   match.categ= list(spatY$CountryGroupID) 
        merge.categ= c("CountryGroupID") }
        
        
        match.categ.names= c(merge.categ,"TotalCatch") #This gets done after merge.categ if statements
        matchid= z+1 #Add 1 because the first match (outside the loop) is ID 1
        
        #Run the data.match function using the current spatial category data and years. If spatial data or nominal data do not exist for whatever reason 
        #(no data in those years, nominal data is already matched), do nothing and keep going
        if( dim(spatY)[1]>0 & dim(nom.dataY)[1]>0 ){  db.match= data.match(nom.dataY,spatY, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.match }
        
        #After each loop, match original non-matched data to matched data and only keep what hasn't been matched yet
        NM= nom.datai
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

####Section IIIb-ICCAT: Matching Algorithm - Re-Run with Larger Year Ranges ####

db.nomatch = nom #Re-set db.nomatch
#Make db.nomatch equal to all nom catch except those already matched as indicated by NIDs:
db.nomatch = subset( merge(db.nomatch, cbind("NID"=sort(unique(db.match$NID)), "Match"=1), by="NID",all.x=T), is.na(Match) )[,-c(10)]
#Re-run matching sequence above with expanded year range on only unmatched nom catch

sum(db.match$Catch)/sum(nom$Catch)

fids= sort(unique(db.nomatch$FishingEntityID)) #Vector of unique FishingEntityIDs in the nominal data

YRs= c(10,20,35) #Year ranges for subsetting (i.e. +/- years to match over) TC Removed 1, 10, 15 as these are saying that categories are more important than time. This will bias results towards future tuna fisheries rather than spatial expansion. 


for(i in 1:length(fids)) #Loop over countries
  
{
  ci= fids[i] #Test by changing index to desired FishingEntityID number
  
  nom.datai= subset(db.nomatch, FishingEntityID==ci) #Nominal data to match for a country
  
  gears= subset(gearres, FishingEntityID==ci)[,c("FishingEntityID","Layer3GearID")] #Fishing gears used by that country
  if(dim(gears)[1] == 0) {gears= cbind("FishingEntityID"=ci, "Layer3GearID"=unique(spat$Layer3GearID))} #If no restriction, use all gears
  y= merge(spat, gears, by="Layer3GearID",all.x=T) #Match country gears to spatial data
  
  cells= subset(areares, FishingEntityID==ci)[,c("FishingEntityID","BigCellID")] #Cells used by that country
  if(dim(cells)[1] == 0) {cells= cbind("FishingEntityID"=ci, "BigCellID"=unique(spat$BigCellID))} #If no restriction, use all cells
  y2= merge(y, cells, by="BigCellID",all.x=T)#Match country cells to spatial data
  
  spati= subset(y2, FishingEntityID.y==ci & FishingEntityID==ci) #Only use spatial data that matched to both the allowed country gears and cells
  spati= spati[,-c(11,12)]; colnames(spati)[4]= "FishingEntityID"#Keep necessary data and update column name UPDATE check this for errors
  
  rm(list=c("y","y2","gears","cells")) #Remove interim datasets
  
  yrs= sort(unique(nom.datai$Year)) #Unique years in the country nominal data
  
  NM= nom.datai #Non-matched data that will be used for the country. This should be the only time this nom.datai gets used each loop
  
  rni= dim(nom.datai)[1] #Rows in original country non-matched data
  
  for(z in 11:20) #Ten is the number of category combos in the ifelse statment below
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
        spatY= subset(spati, Year<=yrj+YR & Year>=yrj-YR ) #Only use data for a given year range
        
        #print("Years spatial");print(sort(unique(spatY$Year)))  #Current years in range for troubleshooting
        
        #Depending on the step z, match by decreasing number of categories (this must be updated manually)
        if(z==11){   match.categ= list(spatY$FishingEntityID, spatY$CountryGroupID, spatY$Layer3GearID, spatY$GearGroupID, spatY$TaxonKey, spatY$SpeciesGroupID) 
        merge.categ= c("FishingEntityID", "CountryGroupID","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID")
        
        } else if(z==12){   match.categ= list(spatY$FishingEntityID, spatY$CountryGroupID, spatY$Layer3GearID, spatY$GearGroupID, spatY$SpeciesGroupID) 
        merge.categ= c("FishingEntityID", "CountryGroupID","Layer3GearID","GearGroupID","SpeciesGroupID")
        
        } else if(z==13){   match.categ= list(spatY$FishingEntityID, spatY$CountryGroupID, spatY$Layer3GearID, spatY$GearGroupID) 
        merge.categ= c("FishingEntityID", "CountryGroupID","Layer3GearID","GearGroupID")
        
        } else if(z==14){   match.categ= list(spatY$FishingEntityID, spatY$CountryGroupID, spatY$GearGroupID) 
        merge.categ= c("FishingEntityID", "CountryGroupID","GearGroupID")
        
        } else if(z==15){   match.categ= list(spatY$FishingEntityID, spatY$CountryGroupID) 
        merge.categ= c("FishingEntityID", "CountryGroupID")
        
        } else if(z==16){   match.categ= list(spatY$CountryGroupID) 
        merge.categ= c("CountryGroupID")
        
        } else if(z==17){   match.categ= list(spatY$TaxonKey) 
        merge.categ= c("TaxonKey")
        
        } else if(z==18){   match.categ= list(spatY$SpeciesGroupID) 
        merge.categ= c("SpeciesGroupID")
        
        } else if(z==19){   match.categ= list(spatY$Layer3GearID) 
        merge.categ= c("Layer3GearID")
        
        } else if(z==20){   match.categ= list(spatY$GearGroupID) 
        merge.categ= c("GearGroupID") }
        
        
        match.categ.names= c(merge.categ,"TotalCatch") #This gets done after merge.categ if statements
        matchid= z+1 #Add 1 because the first match (outside the loop) is ID 1
        
        #Run the data.match function using the current spatial category data and years. If spatial data or nominal data do not exist for whatever reason 
        #(no data in those years, nominal data is already matched), do nothing and keep going
        if( dim(spatY)[1]>0 & dim(nom.dataY)[1]>0 ){  db.match= data.match(nom.dataY,spatY, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.match }
        
        #After each loop, match original non-matched data to matched data and only keep what hasn't been matched yet
        NM= nom.datai
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



#Re-set db.nomatch for testing
db.nomatch = nom 
#Make db.nomatch equal to all nom catch except those already matched as indicated by NIDs:
db.nomatch = subset( merge(db.nomatch, cbind("NID"=sort(unique(db.match$NID)), "Match"=1), by="NID",all.x=T), is.na(Match) )[,-c(10)]


#Aggregate potential duplicate category values to reduce size of matched database, drop NID column
db.match= aggregate(db.match$Catch, by= list(db.match$Year,db.match$FishingEntityID,db.match$Layer3GearID,db.match$TaxonKey,db.match$BigCellID,db.match$MatchID), sum)
colnames(db.match)= c("Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","MatchID","Catch")


####Section IV-ICCAT: Testing ####

sum(db.nomatch$Catch) #Nomatch is equal to 0?

sum(db.match$Catch)/sum(nom$Catch) #Matched % is equal to 1?



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
write.table(agc, "OUTPUT ICCAT Catch by Year and MatchID.csv", sep=",",row.names=F, quote=F)

####Section V-ICCAT: Writing ####

#Order by year
db.match= db.match[order(db.match$Year),]
#Assign an RFMO ID, RFMO ID for ICCAT = 6
db.match$RFMOID= 6
db.match= db.match[ order(db.match$Year), c("RFMOID","Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","Catch", "MatchID") ]

#Write table with match IDs
write.table(db.match[1:1000000, ], "../Outputs/OUTPUT ICCAT Spatialized Catch 1 of 2.csv", sep=",", row.names=F, quote=F)
write.table(db.match[1000001:dim(db.match)[1], ], "../Outputs/OUTPUT ICCAT Spatialized Catch 2 of 2.csv", sep=",", row.names=F, quote=F)



































































#Updates to 2014 work
#1) Integrate discard rate formatting into the spatialization code
#2) Transform Laurenne's discard reconstruction catch amounts into discard rates that can be applied to the RFMO catch



#IATTC----
####IATTC nominal data formatting####
rm(list=ls())
setwd("../IATTC")

#Read-in data
iattc.nom <- read.csv("INPUT IATTC Nominal Catch For Formatting.csv", sep=",")

#Download these reference files from the Sea Around Us GitHub Repository
iattc.spp <- read.table("INPUT IATTC Nominal Species Codes.txt", header=T)
iattc.gea <- read.table("INPUT IATTC Gear Codes.txt", header=T, sep="\t")
iattc.cou <- read.table("INPUT IATTC Nominal Country Codes.txt", header=T)

#Exclude entries with no catch, or for years prior to 1950
iattc.nom <- iattc.nom[iattc.nom$Year>=1950 & iattc.nom$Catch!=0,]

#Match country and gear codes
iattc.N <- left_join(iattc.nom, iattc.gea, by= c("Gear"))
iattc.N <- left_join(iattc.N, iattc.cou, by= c("Flag"))
iattc.N <- left_join(iattc.N, iattc.spp, by= c("Species"))
iattc.N <- iattc.N[ , c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","Catch") ]


#Allocate catch out from "All countries" fishing entity (FishingEntityID= 1000). 
x= iattc.N[iattc.N$FishingEntityID==1000,]
y= iattc.N[iattc.N$FishingEntityID!=1000,]

#Matching by year, gear, and species
y2= aggregate(y$Catch, by= list(y$Year, y$TaxonKey, y$Layer3GearID), sum)
colnames(y2)= c("Year","TaxonKey","Layer3GearID","TotalCatch")

y3= left_join(y, y2, by= c("Year","TaxonKey","Layer3GearID"))
y3$Proportion= y3$Catch / y3$TotalCatch
y3= y3[,c("Year","Layer3GearID","GearGroupID","FishingEntityID","CountryGroupID","TaxonKey","SpeciesGroupID","Proportion")]

z= left_join(x, y3, by= c("Year","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID"))
z$PropCatch= z$Catch * z$Proportion

zm1= z[is.na(z$Proportion)==F,]
znm1= z[is.na(z$Proportion),]

zm1= zm1[,c("Year","FishingEntityID.y","CountryGroupID.y","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","PropCatch")]
colnames(zm1)= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","Catch")

#year, gear and species group
y2= aggregate(y$Catch, by= list(y$Year, y$SpeciesGroupID, y$Layer3GearID), sum)
colnames(y2)= c("Year","SpeciesGroupID","Layer3GearID","TotalCatch")

y3= left_join(y, y2, by= c("Year","SpeciesGroupID","Layer3GearID"))
y3$Proportion= y3$Catch / y3$TotalCatch
y3= y3[,c("Year","SpeciesGroupID","Layer3GearID","GearGroupID","FishingEntityID","CountryGroupID","Proportion")]

z= left_join(x, y3, by= c("Year","Layer3GearID","GearGroupID","SpeciesGroupID"))
z$PropCatch= z$Catch * z$Proportion

zm2= z[is.na(z$Proportion)==F,]
znm2= z[is.na(z$Proportion),]

zm2= zm2[,c("Year","FishingEntityID.y","CountryGroupID.y","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","PropCatch")]
colnames(zm2)= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","Catch")
znm2= znm2[,c("Year","FishingEntityID.x","CountryGroupID.x","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","Catch")]
colnames(znm2)= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","Catch")

#year and gear
y2= aggregate(y$Catch, by= list(y$Year, y$Layer3GearID), sum)
colnames(y2)= c("Year","Layer3GearID","TotalCatch")

y3= left_join(y, y2, by= c("Year","Layer3GearID"))
y3$Proportion= y3$Catch / y3$TotalCatch
y3= y3[,c("Year","SpeciesGroupID","Layer3GearID","GearGroupID","FishingEntityID","CountryGroupID","Proportion")]

z= left_join(x, y3, by= c("Year","Layer3GearID","GearGroupID"))
z$PropCatch= z$Catch * z$Proportion

zm3= z[is.na(z$Proportion)==F,]
znm3= z[is.na(z$Proportion),]

zm3= zm3[,c("Year","FishingEntityID.y","CountryGroupID.y","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID.x","PropCatch")]
colnames(zm3)= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","Catch")
znm3= znm3[,c("Year","FishingEntityID.x","CountryGroupID.x","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID.x","Catch")]
colnames(znm3)= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","Catch")

#year
y2= aggregate(y$Catch, by= list(y$Year), sum)
colnames(y2)= c("Year","TotalCatch")

y3= left_join(y, y2, by= c("Year"))
y3$Proportion= y3$Catch / y3$TotalCatch
y3= y3[,c("Year","SpeciesGroupID","Layer3GearID","GearGroupID","FishingEntityID","CountryGroupID","Proportion")]

z= left_join(x, y3, by= c("Year"))
z$PropCatch= z$Catch * z$Proportion

zm4= z[is.na(z$Proportion)==F,]
znm4= z[is.na(z$Proportion),]

zm4= zm4[,c("Year","FishingEntityID.y","CountryGroupID.y","Layer3GearID.x","GearGroupID.x","TaxonKey","SpeciesGroupID.x","PropCatch")]
colnames(zm4)= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","Catch")

x= rbind(y, zm1, zm2, zm3, zm4)

iattc.N= aggregate(x$Catch, by= list(x$Year,x$FishingEntityID,x$CountryGroupID,x$Layer3GearID,x$GearGroupID,x$TaxonKey,x$SpeciesGroupID), sum)
colnames(iattc.N)= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","Catch")

#Order formatting and disaggregated nominal data by year
iattc.N= iattc.N[order(iattc.N$Year), ]

#Export formatted nominal catch data file for spatialization
write.table(iattc.N[order(iattc.N$Year),], "INPUT IATTC Nominal Catch For Spatial Matching.csv", sep=",", row.names=F)

####IATTC spatial data formatting####

#Clear any stored data for faster computations
rm(list=ls())



#Read spatial catch data for formatting

#Purse Seine
iattc.psb <- read.csv("INPUT IATTC Spatial Catch For Billfish Purse Seine Formatting.csv", sep=",")
iattc.pss <- read.csv("INPUT IATTC Spatial Catch For Shark Purse Seine Formatting.csv", sep=",")
iattc.pst <- read.csv("INPUT IATTC Spatial Catch For Tuna Purse Seine Formatting.csv", sep=",")

#Longline
iattc.llt <- read.csv("INPUT IATTC Spatial Catch For Tuna and Billfish Longline Formatting.csv", sep=",")
iattc.lls <- read.csv("INPUT IATTC Spatial Catch For Shark Longline Formatting.csv", sep=",")

#Pole and Line 
iattc.pl <- read.csv("INPUT IATTC Spatial Catch For Tuna Pole and Line Formatting.csv", sep=",")

#Download these reference files from the Sea Around Us GitHub Repository
#Species Codes
iattc.psspp <- read.table("INPUT IATTC Spatial Purse Seine Species Codes.txt", header=T)
iattc.llspp <- read.table("INPUT IATTC Spatial Longline Species Codes.txt", header=T)
iattc.plspp <- read.table("INPUT IATTC Spatial Pole and Line Species Codes.txt", header=T)

#Purse Seine Formatting
#Function to re-shape database, aggregate by year, flag, and match species codes

spp.match= function(x= iattc.psb, y= iattc.psspp)
{
  
  z= melt(x, id= c("Year","Flag","Lat","Lon") )
  
  names(z)[names(z)=="variable"] = "SpeciesName"
  names(z)[names(z)=="value"] = "Catch"
  
  z= z[ z$Catch!=0, ]	#Only keep entries where there is catch (i.e. catch does not equal zero)
  
  z= left_join(z, y, by= c("SpeciesName"))	#Match species codes
  z <- z %>% select(-SpeciesName)	#Delete "SpeciesName" column (obsolete)
  
  z= aggregate(z$Catch, by= list(z$Year,z$Flag,z$Lat,z$Lon,z$TaxonKey,z$SpeciesGroupID), sum)
  
  colnames(z)= c("Year","Flag","Lat","Lon","TaxonKey","SpeciesGroupID","Catch")
  
  return(z=z)
  
}

iattc.psb2 <- spp.match(x=iattc.psb, y= iattc.psspp)
iattc.pss2 <- spp.match(x=iattc.pss, y= iattc.psspp)
iattc.pst2 <- spp.match(x=iattc.pst, y= iattc.psspp)

#Combine the purse seine data sets
iattc.ps <- rbind(iattc.psb2,iattc.pss2,iattc.pst2)

#Add GearGroupID, specify this is purse seine catch
iattc.ps$GearGroupID <- 401
#Add a BigCellTypeID, specify this is a 1x1 spatial scale
iattc.ps$BigCellTypeID <- 1
#Order the data
iattc.ps <- iattc.ps[ , c("Year","GearGroupID","Flag","Lat","Lon","BigCellTypeID","TaxonKey","SpeciesGroupID","Catch")]


#Longline Formatting
iattc.llt2 <- spp.match(x=iattc.llt, y=iattc.llspp)
iattc.lls2 <- spp.match(x=iattc.lls, y=iattc.llspp)
#Combine the purse seine data sets
iattc.ll <- rbind(iattc.llt2,iattc.lls2)
#Add GearGroupID, specify this is longline catch
iattc.ll$GearGroupID <- 302
#Add a BigCellTypeID, specify this is a 5x5 spatial scale
iattc.ll$BigCellTypeID <- 2
#Final order
iattc.ll <- iattc.ll[ , c("Year","GearGroupID","Flag","Lat","Lon","BigCellTypeID","TaxonKey","SpeciesGroupID","Catch")]


#Pole and Line Formatting
iattc.pl <- spp.match(x=iattc.pl, y=iattc.plspp)
#Add GearGroupID, specify this is pole and line catch, gear group i.d. = 5, "Small-scale gears"
iattc.pl$GearGroupID <- 301
#Add a BigCellTypeID, specify this is a 1x1 spatial scale
iattc.pl$BigCellTypeID <- 1
#Final order
iattc.pl <- iattc.pl[ , c("Year","GearGroupID","Flag","Lat","Lon","BigCellTypeID","TaxonKey","SpeciesGroupID","Catch")]

#Combine gear data sets together
iattc.spat <- rbind(iattc.pl,iattc.ll,iattc.ps)


#Turn flags into CountryGroupID and	FishingEntityID 
#Country Codes
iattc.cou <- read.table("INPUT IATTC Spatial Country Codes.txt", header=T)
#Merge the codes
iattc.spat <- left_join(iattc.spat,iattc.cou, by= c("Flag"))
iattc.spat <- iattc.spat %>% select(-Flag) #Delete "Flag" column (now obsolete)
#Make it prettier, change the order of the columns
iattc.spat <- iattc.spat[ , c("Year","FishingEntityID","CountryGroupID","GearGroupID","Lat","Lon","BigCellTypeID","TaxonKey","SpeciesGroupID","Catch")]


#Rename Lat=Y, Lon=X
names(iattc.spat)[names(iattc.spat)=="Lat"] <- "y"
names(iattc.spat)[names(iattc.spat)=="Lon"] <- "x"
#Reorder
iattc.spat <- iattc.spat[ , c("Year","FishingEntityID","CountryGroupID","GearGroupID","x","y","BigCellTypeID","TaxonKey","SpeciesGroupID","Catch")]


#Convert Lat-Lon to BigCellID
#Read cell code reference data
cellid <- read.csv("INPUT CellTypeID.csv", sep=",")
#Match BigCellID to x,y,BigCellIDType, all.x=T shows NA values
iattc.spat <- left_join(iattc.spat,cellid, by=c("x","y","BigCellTypeID"))

#Some data don't match, this is because it is erroniously reported on land
#Write a table so we can look at the data after
iattc.nomatch <- subset(iattc.spat, is.na(iattc.spat$BigCellID))
write.table(iattc.nomatch,"OUTPUT IATTC Spatial No Match.csv", sep=",", row.names=F)

#Remove the no match cells from our data
iattc.spat <- subset(iattc.spat, iattc.spat$BigCellID > 0)

#Fishing entity 1000 split
#Allocate catch out from "All countries" fishing entity (FishingEntityID= 1000). 
x= iattc.spat[iattc.spat$FishingEntityID==1000,]
y= iattc.spat[iattc.spat$FishingEntityID!=1000,]

#Matching by year, gear group, bigcellid, and species
y2= aggregate(y$Catch, by= list(y$Year, y$TaxonKey, y$GearGroupID, y$BigCellID), sum)
colnames(y2)= c("Year","TaxonKey","GearGroupID","BigCellID","TotalCatch")

y3= left_join(y, y2, by= c("Year","TaxonKey","GearGroupID","BigCellID"))
y3$Proportion= y3$Catch / y3$TotalCatch
y3= y3[,c("Year","GearGroupID","FishingEntityID","CountryGroupID","TaxonKey","SpeciesGroupID","BigCellID","Proportion")]

z= left_join(x, y3, by= c("Year","GearGroupID","TaxonKey","SpeciesGroupID","BigCellID"))
z$PropCatch= z$Catch * z$Proportion

zm1= z[is.na(z$Proportion)==F,]
znm1= z[is.na(z$Proportion),]

zm1= zm1[,c("Year","FishingEntityID.y","CountryGroupID.y","GearGroupID","TaxonKey","SpeciesGroupID","BigCellID","PropCatch")]
colnames(zm1)= c("Year","FishingEntityID","CountryGroupID","GearGroupID","TaxonKey","SpeciesGroupID","BigCellID","Catch")

znm1= znm1[,c("Year","FishingEntityID.x","CountryGroupID.x","GearGroupID","TaxonKey","SpeciesGroupID","BigCellID","Catch")]
colnames(znm1)= c("Year","FishingEntityID","CountryGroupID","GearGroupID","TaxonKey","SpeciesGroupID","BigCellID","Catch")


#Matching by year, gear group, and species
y2= aggregate(y$Catch, by= list(y$Year, y$TaxonKey, y$GearGroupID), sum)
colnames(y2)= c("Year","TaxonKey","GearGroupID","TotalCatch")

y3= left_join(y, y2, by= c("Year","TaxonKey","GearGroupID"))
y3$Proportion= y3$Catch / y3$TotalCatch
y3= y3[,c("Year","GearGroupID","FishingEntityID","CountryGroupID","TaxonKey","SpeciesGroupID","BigCellID","Proportion")]

z= left_join(znm1, y3, by= c("Year","GearGroupID","TaxonKey","SpeciesGroupID"))
z$PropCatch= z$Catch * z$Proportion

zm2= z[is.na(z$Proportion)==F,]
znm2= z[is.na(z$Proportion),]

zm2= zm2[,c("Year","FishingEntityID.y","CountryGroupID.y","GearGroupID","TaxonKey","SpeciesGroupID","BigCellID.x","PropCatch")]
colnames(zm2)= c("Year","FishingEntityID","CountryGroupID","GearGroupID","TaxonKey","SpeciesGroupID","BigCellID","Catch")

znm2= znm2[,c("Year","FishingEntityID.x","CountryGroupID.x","GearGroupID","TaxonKey","SpeciesGroupID","BigCellID.x","Catch")]
colnames(znm2)= c("Year","FishingEntityID","CountryGroupID","GearGroupID","TaxonKey","SpeciesGroupID","BigCellID","Catch")



#Matching by year, gear group, species group
y2= aggregate(y$Catch, by= list(y$Year, y$GearGroupID, y$SpeciesGroupID), sum)
colnames(y2)= c("Year","GearGroupID","SpeciesGroupID","TotalCatch")

y3= left_join(y, y2, by= c("Year","GearGroupID","SpeciesGroupID"))
y3$Proportion= y3$Catch / y3$TotalCatch
y3= y3[,c("Year","GearGroupID","FishingEntityID","CountryGroupID","TaxonKey","SpeciesGroupID","BigCellID","Proportion")]

z= left_join(znm2, y3, by= c("Year","GearGroupID","SpeciesGroupID"))
z$PropCatch= z$Catch * z$Proportion

zm3= z[is.na(z$Proportion)==F,]
znm3= z[is.na(z$Proportion),]

zm3= zm3[,c("Year","FishingEntityID.y","CountryGroupID.y","GearGroupID","TaxonKey.y","SpeciesGroupID","BigCellID.x","PropCatch")]
colnames(zm3)= c("Year","FishingEntityID","CountryGroupID","GearGroupID","TaxonKey","SpeciesGroupID","BigCellID","Catch")

znm3= znm3[,c("Year","FishingEntityID.x","CountryGroupID.x","GearGroupID","TaxonKey.x","SpeciesGroupID","BigCellID.x","Catch")]
colnames(znm3)= c("Year","FishingEntityID","CountryGroupID","GearGroupID","TaxonKey","SpeciesGroupID","BigCellID","Catch")


#Matching by year and species group
#drop year, no match for 1958, so move to only consider 1959
y2= aggregate(y$Catch, by= list(y$TaxonKey, y$GearGroupID), sum)
colnames(y2)= c("TaxonKey","GearGroupID","TotalCatch")
y4 <- y[y$Year==1959,]

#then merge on y4, where year =1959
y3= left_join(y4, y2, by= c("TaxonKey","GearGroupID"))

y3$Proportion= y3$Catch / y3$TotalCatch
y3= y3[,c("Year","GearGroupID","FishingEntityID","CountryGroupID","TaxonKey","SpeciesGroupID","BigCellID","Proportion")]

z= left_join(znm3, y3, by= c("TaxonKey","GearGroupID"))
z$PropCatch= z$Catch * z$Proportion

zm4= z[is.na(z$Proportion)==F,]
znm4= z[is.na(z$Proportion),]

zm4= zm4[,c("Year.x","FishingEntityID.y","CountryGroupID.y","GearGroupID","TaxonKey","SpeciesGroupID.y","BigCellID.x","PropCatch")]
colnames(zm4)= c("Year","FishingEntityID","CountryGroupID","GearGroupID","TaxonKey","SpeciesGroupID","BigCellID","Catch")

z= rbind(zm1, zm2, zm3, zm4)  #Join all the matched data
y= iattc.spat[iattc.spat$FishingEntityID!=1000,c("Year","FishingEntityID","CountryGroupID","GearGroupID","TaxonKey","SpeciesGroupID","BigCellID","Catch")]

x= rbind(y, z) #Join the split 'All countries' data with the other data

iattc.spat= aggregate(x$Catch, by= list(x$Year, x$FishingEntityID, x$CountryGroupID, x$GearGroupID, x$TaxonKey, x$SpeciesGroupID, x$BigCellID), sum) #Sum up any repeated entries (due to the split-up)
colnames(iattc.spat)= c("Year","FishingEntityID","CountryGroupID","GearGroupID","TaxonKey","SpeciesGroupID","BigCellID","Catch")

#Export formatted spatial catch data file for spatialization
write.table(iattc.spat[order(iattc.spat$Year),], "INPUT IATTC Spatial Catch For Spatial Matching.csv", sep=",", row.names=F)

#Datasets are noe ready for spatialization
end <- Sys.time()
end - start

####SECTION I-IATTC: INITIAL DATA AND TRANSFORMATION ####
rm(list=ls())

#1.Read in databases of nominal and spatialized catch by ocean

nom=read.csv("INPUT IATTC Nominal Catch For Spatial Matching.csv",sep=",",header=T)	#Yearly nominal catch
spat= read.csv("INPUT IATTC Spatial Catch For Spatial Matching.csv", sep=",", header=T) #Yearly spatial catch

nom= nom[nom$Catch!=0,] #Exclude nominal records with zero catch

spat= spat[spat$Catch!=0,] #Exclude spatial records with zero catch
spat= spat[is.na(spat$BigCellID)==F,] #Exclude records without a spatial cell assignment

gearres= read.csv("INPUT IATTC GearRestrictionTable.csv",sep=",")
areares= read.csv("INPUT IATTC AreaRestrictionTable.csv",sep=",")

tot.nom.ct=sum(nom$Catch)	#Total catch in nominal database; this is used to compare catches after spatializing.
tot.spat.ct= sum(spat$Catch)	#Total catch in spatial database; this is not used in the routine but is available to look at.

#Add record IDs
nom$NID= 1:dim(nom)[1]


#Add ID categories everywhere

final.categ= c("NID","Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","SpatCatch","MatchID")
final.categ.names= c("NID","Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","Catch","MatchID")

####SECTION II-IATTC: PERFECT MATCH ####

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

spat.all= left_join(spat,ct.all,by= merge.categ)	#Spatialized catch total in all cells by Year/GearGroupID/TaxonKey

spat.all$Proportion= spat.all$Catch / spat.all$TotalCatch	#Proportion of catch per cell = catch / total catch in cells reported for that category combo

spat.all= spat.all[,c(merge.categ,"BigCellID","Proportion")]	#Remove the catch per cell in this dataframe (but leave the proportions)

#3.Match nominal and cell proportion catch by all categories.
new.db= left_join(nom.data,spat.all,by= merge.categ)#Match and merge the spatialized (with proportions) and 
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


####SECTION IIIa-IATTC: Matching Algorithm ####

#This is the key function. Same as above but does not return the non-matched data as this is unnecessary
#given the functions below
#This function takes the categories for the level of the match and spatializes 
#the catch where matches are found for those match categories
#Match categories come from the loop process below where the function is called repeatedly for different levels of match categories and year ranges within a country


data.match= function(nom.dataY, spatY, db.match, match.categ, merge.categ, match.categ.names, matchid )
{
  ct.all= aggregate(spatY$Catch,by= match.categ,sum)
  colnames(ct.all)= match.categ.names
  spat.all= left_join(spatY,ct.all,by= merge.categ)
  spat.all$Proportion= spat.all$Catch / spat.all$TotalCatch
  spat.all= spat.all[,c(merge.categ,"BigCellID","Proportion")]
  new.db= left_join(nom.dataY,spat.all,by= merge.categ)
  new.db$SpatCatch= new.db$Catch * new.db$Proportion
  new.db$MatchID= matchid
  db.match.new= filter(new.db,is.na(BigCellID)==F)
  db.match.new= db.match.new[,final.categ]
  colnames(db.match.new)= final.categ.names
  db.match= rbind(db.match, db.match.new)
  rm(list=c("ct.all","spat.all","new.db","db.match.new"))
  
  return(list(db.match=db.match))
}



fids= sort(unique(nom$FishingEntityID)) #Vector of unique FishingEntityIDs in the nominal data

YRs= c(0,2,5) #Year ranges for subsetting (i.e. +/- years to match over) TC Removed 1, 10, 15 as these are saying that categories are more important than time. This will bias results towards future tuna fisheries rather than spatial expansion. 


for(i in 1:length(fids)) #Loop over countries
  
{
  ci= fids[i] #Test by changing index to desired FishingEntityID number
  
  nom.datai= filter(db.nomatch, FishingEntityID==ci) #Nominal data to match for a country
  
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
  
  for(z in 1:13) #Twelve is the number of category combos in the ifelse statment below
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
        
        #After each loop, match original non-matched data to matched data and only keep what hasn't been matched yet
        NM= nom.datai
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

####SECTION IIIb-IATTC: Matching Algorithm - Re-Run with Larger Year Ranges ####

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
        
        #After each loop, match original non-matched data to matched data and only keep what hasn't been matched yet
        NM= nom.datai
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



#### SECTION IV-IATTC: Testing####

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


db.nomatch = subset( merge(db.nomatch, cbind("NID"=sort(unique(db.match$NID)), "Match"=1), by="NID",all.x=T), is.na(Match) )[,-c(11)]
#Refine db.nomatch data 

db.nomatch= db.nomatch[,colnames(nom)]	#Reset columns in non-matched database


sum(db.nomatch$Catch) #Nomatch is equal to 0?


sum(db.match$Catch)/sum(nom$Catch) #Matched % is equal to 1?


#### SECTION V-IATTC: Writing####
db.match= db.match[order(db.match$Year),]

#Aggregate potential duplicate category values to reduce size of matched database, drop NID column
db.match <- aggregate(db.match$Catch, by= list(db.match$Year,db.match$FishingEntityID,db.match$Layer3GearID,db.match$TaxonKey,db.match$BigCellID,db.match$MatchID), sum)
colnames(db.match) <- c("Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","MatchID","Catch")
db.match$RFMOID <- 5  #RFMO i.d. for IATTC = 5
db.match<- db.match[ order(db.match$Year), c("RFMOID","Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","MatchID","Catch") ]

write.table(db.match[1:1000000, ], "../Outputs/OUTPUT IATTC Spatialized Catch 1 of 4.csv", sep=",", row.names=F, quote=F)
write.table(db.match[1000001:2000000,], "../Outputs/OUTPUT IATTC Spatialized Catch 2 of 4.csv", sep=",", row.names=F, quote=F)
write.table(db.match[2000001:3000000, ], "../Outputs/OUTPUT IATTC Spatialized Catch 3 of 4.csv", sep=",", row.names=F, quote=F)
write.table(db.match[3000000:nrow(db.match)[1], ], "../Outputs/OUTPUT IATTC Spatialized Catch 4 of 4.csv", sep=",", row.names=F, quote=F)


write.table(db.nomatch[1:1000000, ], "OUTPUT IATTC Spatialized Catch No Matches 1 of 1.csv", sep=",", row.names=F, quote=F)





#WCPFC----
####WCPFC Nominal data Formatting####

rm(list=ls())
setwd("../WCPFC")
#Read data

wcpfc.nom= read.csv("INPUT WCPFC Nominal Catch For Formatting.csv", sep=",")
wcpfc.spp= read.table("INPUT WCPFC Nominal Species Codes.txt", header=T)
wcpfc.gea= read.table("INPUT WCPFC Nominal Gear Codes.txt", header=T)
wcpfc.cou= read.table("INPUT WCPFC Nominal Country Codes.txt", header=T)

#Match country and gear codes

wcpfc.N= left_join(wcpfc.nom, wcpfc.gea, by= c("Gear"))

wcpfc.N= left_join(wcpfc.N, wcpfc.cou, by= c("Flag"))

#Some american territories are recorded as US, need to change their fishing entities
#American Samoa FEID=3, CountryGroupID = 6
wcpfc.AS <- subset(wcpfc.N, Fleet == "AS")
wcpfc.x <- subset(wcpfc.N, Fleet != "AS")
wcpfc.AS$FishingEntityID <- 3
wcpfc.AS$CountryGroupID <- 6
wcpfc.N <- rbind(wcpfc.x,wcpfc.AS)

#Guam FEID=74, CountryGroupID = 6
wcpfc.GU <- subset(wcpfc.N, Fleet == "GU")
wcpfc.x <- subset(wcpfc.N, Fleet != "GU")
wcpfc.GU$FishingEntityID <- 74
wcpfc.GU$CountryGroupID <- 6
wcpfc.N <- rbind(wcpfc.x,wcpfc.GU)

#North Marianas FEID=129, CountryGroupID = 7
wcpfc.MP <- subset(wcpfc.N, Fleet == "MP")
wcpfc.x <- subset(wcpfc.N, Fleet != "MP")
wcpfc.MP$FishingEntityID <- 129
wcpfc.MP$CountryGroupID <- 7
wcpfc.N <- rbind(wcpfc.x,wcpfc.MP)
wcpfc.N <- wcpfc.N %>% select(-c(Gear, Fleet, Flag))
#wcpfc.N= wcpfc.N[ , -c(1:2,4) ]	#Delete first two columns with "Gear" and "Flag" (obsolete)


#Re-shape database into a dataframe

wcpfc.N= melt( wcpfc.N, id= c("Year","Layer3GearID","GearGroupID","FishingEntityID","CountryGroupID") )
colnames(wcpfc.N)[6:7]= c("SpeciesName","Catch")	#Re-name last two columns


#Match species codes

wcpfc.N= left_join(wcpfc.N, wcpfc.spp, by= c("SpeciesName"))

wcpfc.N= wcpfc.N[ , c("Year","Layer3GearID","GearGroupID","FishingEntityID","CountryGroupID","TaxonKey","SpeciesGroupID","Catch") ]

#remove lines where catch = 0
wcpfc.N <- wcpfc.N[wcpfc.N$Catch > 0,]

#Export formatted nominal catch data file for matching

write.table(wcpfc.N[order(wcpfc.N$Year),], "INPUT WCPFC Nominal Catch For Spatial Matching.csv", sep=",", row.names=F)



####WCPFC Spatial data Formatting####

#rm(list=ls())

#Read cell code data
wcpfc.cel= read.table("INPUT WCPFC Spatial Cell Codes.txt", header=T)
cellid= read.csv("INPUT CellTypeID2017.csv", sep=",")


#Read spatial catch data

wcpfc.ll60= read.csv("INPUT WCPFC Spatial Longline 60 For Formatting.csv", sep=",")
wcpfc.ll70= read.csv("INPUT WCPFC Spatial Longline 70 For Formatting.csv", sep=",")
wcpfc.ll80= read.csv("INPUT WCPFC Spatial Longline 80 For Formatting.csv", sep=",")
wcpfc.ll90= read.csv("INPUT WCPFC Spatial Longline 90 For Formatting.csv", sep=",")
wcpfc.ll00= read.csv("INPUT WCPFC Spatial Longline 00 For Formatting.csv", sep=",")

wcpfc.ps= read.csv("INPUT WCPFC Spatial Purse Seine For Formatting.csv", sep=",")
wcpfc.pl= read.csv("INPUT WCPFC Spatial Pole And Line For Formatting.csv", sep=",")
wcpfc.dn= read.csv("INPUT WCPFC Spatial Driftnet For Formatting.csv", sep=",")

wcpfc.llspp= read.table("INPUT WCPFC Spatial Longline Species Codes.txt", header=T)
wcpfc.psspp= read.table("INPUT WCPFC Spatial Purse Seine Species Codes.txt", header=T)
wcpfc.plspp= read.table("INPUT WCPFC Spatial Pole And Line Species Codes.txt", header=T)
wcpfc.dnspp= read.table("INPUT WCPFC Spatial Driftnet Species Codes.txt", header=T)


#Longline data
#Re-shape database, aggregate by year, and match species codes
spp.match= function(x= wcpfc.ll00, y= wcpfc.llspp)
{
  
  z= melt(x, id= c("Year","Month","Lat","Lon") )
  
  names(z)[names(z)=="variable"] = "SpeciesName"
  names(z)[names(z)=="value"] = "Catch"
  
  z= z[ z$Catch!=0, ]	#Only keep entries where there is catch (i.e. catch does not equal zero)
  
  z= left_join(z, y, by= c("SpeciesName"))	#Match species codes
  z <- z %>% select(-SpeciesName)	#Delete "SpeciesName" column (obsolete)
  
  z= aggregate(z$Catch, by= list(z$Year,z$Lat,z$Lon,z$TaxonKey,z$SpeciesGroupID), sum)
  
  colnames(z)= c("Year","Lat","Lon","TaxonKey","SpeciesGroupID","Catch")
  
  return(z=z)
  
}

wcpfc.ll00= spp.match(x= wcpfc.ll00, y= wcpfc.llspp)
wcpfc.ll90= spp.match(x= wcpfc.ll90, y= wcpfc.llspp)
wcpfc.ll80= spp.match(x= wcpfc.ll80, y= wcpfc.llspp)
wcpfc.ll70= spp.match(x= wcpfc.ll70, y= wcpfc.llspp)
wcpfc.ll60= spp.match(x= wcpfc.ll60, y= wcpfc.llspp)

wcpfc.ll= rbind(wcpfc.ll60, wcpfc.ll70, wcpfc.ll80, wcpfc.ll90, wcpfc.ll00)


#Add GearGroupID
wcpfc.ll$GearGroupID= 302	#Specify that this is Longline catch (i.e. gear group i.d. = 32)

wcpfc.ll= wcpfc.ll[ , c("Year","GearGroupID","TaxonKey","SpeciesGroupID","Lat","Lon","Catch") ]



#Purse seine data

wcpfc.ps= spp.match(x= wcpfc.ps, y= wcpfc.psspp)

wcpfc.ps$GearGroupID= 401	#Specify that this is Purse seine catch (i.e. gear group i.d. = 41)

wcpfc.ps= wcpfc.ps[ , c("Year","GearGroupID","TaxonKey","SpeciesGroupID","Lat","Lon","Catch") ]


#Pole and line data

wcpfc.pl= spp.match(x= wcpfc.pl, y= wcpfc.plspp)

wcpfc.pl$GearGroupID= 301	#Specify that this is Pole and line catch (i.e. gear group i.d. = 31)

wcpfc.pl= wcpfc.pl[ , c("Year","GearGroupID","TaxonKey","SpeciesGroupID","Lat","Lon","Catch") ]

#Driftnet Data, assume DN is a type of GN, use Layer3GearID 10, GearGroupID 50

wcpfc.dn= spp.match(x= wcpfc.dn, y= wcpfc.dnspp)

wcpfc.dn$GearGroupID= 500	#Specify that this is driftnet catch (i.e. gear group i.d. = 50, "Gillnet")

#Join WCPFC spatial datasets

wcpfc.S= rbind(wcpfc.ll, wcpfc.ps, wcpfc.pl, wcpfc.dn )


#Replace Lat-Lon with BigCellID

wcpfc.S= left_join(wcpfc.S, wcpfc.cel, by= c("Lat","Lon"))
wcpfc.S= left_join(wcpfc.S, cellid, by= c("x","y","BigCellTypeID"))


wcpfc.S= wcpfc.S[ , c("Year","GearGroupID","TaxonKey","SpeciesGroupID","BigCellID","Catch") ]
wcpfc.S <- wcpfc.S[is.na(wcpfc.S$BigCellID) == F,]


#Export formatted spatial catch data file for matching

write.table(wcpfc.S[order(wcpfc.S$Year),], "INPUT WCPFC Spatial Catch For Spatial Matching.csv", sep=",", row.names=F)

####Section I-WCPFC: INITIAL DATA AND TRANSFORMATION####
rm(list=ls())


#1.Read in databases of nominal and spatialized catch by ocean

nom=read.csv("INPUT WCPFC Nominal Catch For Spatial Matching.csv",sep=",",header=T)	#Yearly nominal catch
spat=read.csv("INPUT WCPFC Spatial Catch For Spatial Matching.csv", sep=",",header=T) #Yearly spatial data


nom= nom[nom$Catch!=0,] #Exclude nominal records with zero catch




spat= spat[spat$Catch!=0,]
spat= spat[is.na(spat$BigCellID)==F,]

gearres= read.csv("INPUT WCPFC GearRestrictionTable.csv",sep=",")
areares= read.csv("INPUT WCPFC AreaRestrictionTable.csv",sep=",")

tot.nom.ct=sum(nom$Catch)	#Total catch in nominal database; this is used to compare catches after spatializing.
tot.spat.ct= sum(spat$Catch)	#Total catch in spatial database; this is not used in the routine but is available to look at.


#Add record IDs
nom$NID= 1:dim(nom)[1]


#Add ID categories everywhere

final.categ= c("NID","Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","SpatCatch","MatchID")
final.categ.names= c("NID","Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","Catch","MatchID")

####Section II-WCPFC: PERFECT MATCH ####

#First match all the best-case scenarios (i.e. perfect data match)
#This doesn't need any spatial data subsetting

nom.data= nom
match.categ= list(spat$Year, spat$GearGroupID, spat$TaxonKey, spat$SpeciesGroupID)
merge.categ= c("Year","GearGroupID","TaxonKey","SpeciesGroupID")
match.categ.names= c(merge.categ,"TotalCatch")
matchid= 1


#1.Sum up the total catch, by all categories, in all reported spatial cells.

ct.all= aggregate(spat$Catch,by= match.categ,sum)  #Total catch in all cells, all categories

colnames(ct.all)= match.categ.names #Add matching column names

#2.Transform reported catch per cell into proportions of the total for all cells, by all categories.

spat.all= left_join(spat,ct.all,by= merge.categ)	#Spatialized catch total in all cells by Year/GearGroupID/TaxonKey

spat.all$Proportion= spat.all$Catch / spat.all$TotalCatch	#Proportion of catch per cell = catch / total catch in cells reported for that category combo

spat.all= spat.all[,c(merge.categ,"BigCellID","Proportion")]	#Remove the catch per cell in this dataframe (but leave the proportions)

#3.Match nominal and cell proportion catch by all categories.
new.db= left_join(nom.data,spat.all,by= merge.categ)#Match and merge the spatialized (with proportions) and 
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




####Section III-WCPFC: Matching Algorithm ####


#This is the key function. Same as above but does not return the non-matched data as this is unnecessary
#given the functions below
#This function takes the categories for the level of the match and spatializes 
#the catch where matches are found for those match categories
#Match categories come from the loop process below where the function is called repeatedly for different levels of match categories and year ranges within a country


data.match= function(nom.dataY, spat, db.match, match.categ, merge.categ, match.categ.names, matchid )
{
  ct.all= aggregate(spatY$Catch,by= match.categ,sum)
  colnames(ct.all)= match.categ.names
  spat.all= left_join(spatY,ct.all,by= merge.categ)
  spat.all$Proportion= spat.all$Catch / spat.all$TotalCatch
  spat.all= spat.all[,c(merge.categ,"BigCellID","Proportion")]
  new.db= left_join(nom.dataY,spat.all,by= merge.categ)
  new.db$SpatCatch= new.db$Catch * new.db$Proportion
  new.db$MatchID= matchid
  db.match.new= filter(new.db,is.na(BigCellID)==F)
  db.match.new= db.match.new[,final.categ]
  colnames(db.match.new)= final.categ.names
  db.match= rbind(db.match, db.match.new)
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
  
  
  spati= subset(y2, FishingEntityID.y==ci & FishingEntityID.x==ci) #Only use spatial data that matched to both the allowed country gears and cells
  spati= spati[,-c(8)]; colnames(spati)[7]= "FishingEntityID"#Keep necessary data and update column name UPDATE check this for errors
  #Only use spatial data that matched to both the allowed country gears and cells
  
  rm(list=c("y","y2","gears","cells")) #Remove interim datasets
  
  yrs= sort(unique(nom.datai$Year)) #Unique years in the country nominal data
  
  NM= nom.datai #Non-matched data that will be used for the country. This should be the only time this nom.datai gets used each loop
  
  rni= dim(nom.datai)[1] #Rows in original country non-matched data
  
  for(z in 1:5) #Five is the number of category combos in the ifelse statment below
  {
    
    for(j in 1:length(yrs)) #Loop over all years of nominal data
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
        spatY= subset(spati, Year<=yrj+YR & Year>=yrj-YR ) #Only use data for a given year range
        
        #print("Years spatial");print(sort(unique(spatY$Year)))  #Current years in range for troubleshooting
        
        #Depending on the step z, match by decreasing number of categories (this must be updated manually)
        if(z==1){   match.categ= list(spatY$GearGroupID, spatY$TaxonKey) 
        merge.categ= c("GearGroupID","TaxonKey")
        
        } else if(z==2){   match.categ= list(spatY$GearGroupID, spatY$SpeciesGroupID) 
        merge.categ= c("GearGroupID","SpeciesGroupID")
        
        } else if(z==3){   match.categ= list(spatY$GearGroupID) 
        merge.categ= c("GearGroupID")
        
        } else if(z==4){   match.categ= list(spatY$TaxonKey) 
        merge.categ= c("TaxonKey")
        
        } else if(z==5){   match.categ= list(spatY$SpeciesGroupID) 
        merge.categ= c("SpeciesGroupID") }
        
        
        match.categ.names= c(merge.categ,"TotalCatch") #This gets done after merge.categ if statements
        matchid= z+1 #Add 1 because the first match (outside the loop) is ID 1
        
        #Run the data.match function using the current spatial category data and years. If spatial data or nominal data do not exist for whatever reason 
        #(no data in those years, nominal data is already matched), do nothing and keep going
        if( dim(spatY)[1]>0 & dim(nom.dataY)[1]>0 ){  db.match= data.match(nom.dataY,spatY, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.match }
        
        #After each loop, match original non-matched data to matched data and only keep what hasn't been matched yet
        NM= nom.datai
        NM= subset( merge(NM, cbind("NID"=sort(unique(db.match$NID)), "Match"=1), by="NID",all.x=T), is.na(Match) )[,-c(11)] 
        
        nni= dim(NM)[1] #Rows in remaining non-matched data for country
        print("% Matched"); print(round((1-nni/rni) * 100, 1))
        
      } #Close year range loop
      
      
    } #Close years loop
    
    
  } #Close category loop
  
  #Aggregate potential duplicate category values to reduce size of matched database
  db.match= aggregate(db.match$Catch, by= list(db.match$NID,db.match$Year,db.match$FishingEntityID,db.match$Layer3GearID,db.match$TaxonKey,db.match$BigCellID,db.match$MatchID), sum)
  colnames(db.match)= c("NID","Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","MatchID","Catch")
  
}  #Close country loop 




#db.nomatch = nom #Re-set db.nomatch
#Make db.nomatch equal to all nom catch except those already matched as indicated by NIDs:
db.nomatch = subset( merge(db.nomatch, cbind("NID"=sort(unique(db.match$NID)), "Match"=1), by="NID",all.x=T), is.na(Match) )[,-c(11)]
#Re-run matching sequence above with expanded year range on only unmatched nom catch

db.nomatch= db.nomatch[,colnames(nom)]	#Reset columns in non-matched database


sum(db.nomatch$Catch)


sum(db.match$Catch)/sum(nom$Catch)










####Section IV-WCPFC: Testing ####


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
write.table(agc, "OUTPUT WCPFC Catch by Year and MatchID.csv", sep=",",row.names=F, quote=F)


####Section V-WCPFC: Writing ####

db.match= db.match[order(db.match$Year),]

db.match= aggregate(db.match$Catch, by= list(db.match$Year,db.match$FishingEntityID,db.match$Layer3GearID,db.match$TaxonKey,db.match$BigCellID,db.match$MatchID), sum)
colnames(db.match)= c("Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","MatchID","Catch")


db.match$RFMOID= 18  #RFMO i.d. for WCPFC = 18
db.match= db.match[ order(db.match$Year), c("RFMOID","Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","MatchID","Catch") ]


write.table(db.match[1:1000000,], "../Outputs/OUTPUT WCPFC Spatialized Catch 1 of 3.csv", sep=",", row.names=F, quote=F)	
write.table(db.match[1000001:2000000,], "../Outputs/OUTPUT WCPFC Spatialized Catch 2 of 3.csv", sep=",", row.names=F, quote=F)	 
write.table(db.match[2000001:dim(db.match)[1],], "../Outputs/OUTPUT WCPFC Spatialized Catch 3 of 3.csv", sep=",", row.names=F, quote=F)	 


#Discards----
#### Section I-Discards: Formatting Rates ####

rm(list=ls())
setwd("../Discards")
#Indian and Atlantic Discards

disc <- read.csv("INPUT Indian and Atlantic Discard Rates.csv", sep=",", header=T) #Discard rate data from Fred LeManach, carried forward to 2015
colnames(disc)[5:71] <- 1950:2016  #Rename columns into years

gear <- read.table("Indian Atlantic GearID.txt", header=T, sep="\t")

newdisc <- melt(disc, id=c("Ocean","Gear","GearGroup","Source")) #Turn data into dataframe
colnames(newdisc)[5:6] <- c("Year","Discards")  #Rename columns

#Extract only non-NA data
newdisc <- newdisc[is.na(newdisc$Discards)==F,]

#Pull out data with a reference (i.e. not interpolated)
discr <- newdisc[newdisc$Source=="Reference",]
#Triplicate data with references (3x weighting for average)
newdisc <- rbind(newdisc, discr, discr)

#Count data points for each gear/ocean combination
disccnt <- aggregate(newdisc$Discards, by= list(newdisc$Ocean,newdisc$Gear,newdisc$Year),length)
#Sum reported/interpolated discard rates for each gear/ocean combination
discsum <- aggregate(newdisc$Discards, by= list(newdisc$Ocean,newdisc$Gear,newdisc$Year),sum)

#Combine counts and sums into new dataframe
discfin <- cbind(discsum,disccnt$x) 
colnames(discfin) <- c("Ocean","Gear","Year","SumDiscards","DataPoints") #Rename columns
discfin$DiscardRate <- discfin$SumDiscards / discfin$DataPoints  #Calculate average discard rate

disc.atl.ind <- merge(discfin,gear, by= c("Gear"))  #Merge data with Gear IDs
disc.atl.ind <- disc.atl.ind[,c("Ocean","Gear","Layer3GearID","GearTypeID", "Year","DiscardRate")]
#Aggregate for gears with the same Layer3GearIDs
disc.atl.ind <- aggregate(disc.atl.ind$DiscardRate, by= list(disc.atl.ind$Year,disc.atl.ind$Ocean,disc.atl.ind$Layer3GearID,disc.atl.ind$GearTypeID),mean)
colnames(disc.atl.ind) <- c("Year","TunaDiscardRegion","Layer3GearID","GearTypeID","DiscardRate")
#Add species and taxon group IDs, everything is marine fishes NEI
disc.atl.ind$TaxonKey <- 100039
disc.atl.ind$SpeciesGroupID <- 6

#reorder
disc.atl.ind <- disc.atl.ind[,c("Year","TunaDiscardRegion","TaxonKey", "SpeciesGroupID","Layer3GearID","GearTypeID","DiscardRate")]
write.table(disc.atl.ind, "Discard Rates Atlantic and Indian Oceans.csv", sep=",", row.names=F)

#Pacific Discards#
rm(list=ls())

#Discard rates calculated from Laurenne Schiller's Masters thesis data to 2010, carried forward to 2015
disc <- read.csv("INPUT Pacific Discard Rates.csv", sep=",", header=T) 
colnames(disc)[4:70] <- 1950:2016  #Rename columns into years
#Add an ocean
disc$TunaDiscardRegion <- "Pacific"
gear <- read.table("Pacific GearID.txt", header=T, sep="\t")
country <- read.table("Pacific FishingEntityID.txt", header=T, sep="\t")
taxon <- read.table("Pacific TaxonID.txt", header=T, sep="\t")

newdisc <- melt(disc, id=c("TunaDiscardRegion","GearName","Species","FishingEntity")) #Turn data into dataframe
colnames(newdisc)[5:6] <- c("Year","DiscardRate")  #Rename columns

#Replace country with FEID
disc.pac <- merge(newdisc,country, by="FishingEntity")
#Add gear codes
disc.pac <- merge(disc.pac,gear, by="GearName")
#Add species codes
disc.pac <- merge(disc.pac,taxon, by="Species")
#Remove text codes
disc.pac <- disc.pac[,-c(1:3)]

#remove rows with 0 values
disc.pac <- disc.pac[disc.pac$DiscardRate > 0,]
#reorder
disc.pac <- disc.pac[c("Year","TunaDiscardRegion","TaxonKey", "SpeciesGroupID","FishingEntityID","Layer3GearID","GearTypeID","DiscardRate")]
write.table(disc.pac, "Discard Rates Pacific Ocean.csv", sep=",", row.names=F)


#### Section II-Discards: Spatialization #### 
rm(list=ls())

#Compile spatialized catch

ccsbt <- read.csv("../Outputs/OUTPUT CCSBT Spatialized Catch 1 of 1.csv", sep=",")
#remove match ID
ccsbt <- ccsbt %>% select(-MatchID)

iattc1 <- read.csv("../Outputs/OUTPUT IATTC Spatialized Catch 1 of 4.csv", sep=",")
iattc2 <- read.csv("../Outputs/OUTPUT IATTC Spatialized Catch 2 of 4.csv", sep=",")
iattc3 <- read.csv("../Outputs/OUTPUT IATTC Spatialized Catch 3 of 4.csv", sep=",")
iattc4 <- read.csv("../Outputs/OUTPUT IATTC Spatialized Catch 4 of 4.csv", sep=",")
iattc <- rbind(iattc1,iattc2,iattc3,iattc4)
#remove match ID
iattc <- iattc %>% select(-MatchID)

iccat1 <- read.csv("../Outputs/OUTPUT ICCAT Spatialized Catch 1 of 2.csv", sep=",")
iccat2 <- read.csv("../Outputs/OUTPUT ICCAT Spatialized Catch 2 of 2.csv", sep=",")
iccat <- rbind(iccat1,iccat2)
#remove match ID
iccat <- iccat %>% select(-MatchID)

iotc <- read.csv("../Outputs/OUTPUT IOTC Spatialized Catch 1 of 1.csv", sep=",")
#remove match ID
iotc <- iotc %>% select(-MatchID)

wcpfc1 <- read.csv("../Outputs/OUTPUT WCPFC Spatialized Catch 1 of 3.csv", sep=",")
wcpfc2 <- read.csv("../Outputs/OUTPUT WCPFC Spatialized Catch 2 of 3.csv", sep=",")
wcpfc3 <- read.csv("../Outputs/OUTPUT WCPFC Spatialized Catch 3 of 3.csv", sep=",")
wcpfc <- rbind(wcpfc1,wcpfc2,wcpfc3)
#remove match ID
wcpfc <- wcpfc %>% select(-MatchID)

#Compile a single RFMO spatialized catch dataframe 
spatc= rbind(ccsbt,iattc,iccat,iotc,wcpfc)

#Clear individual RFMO data
rm(list=c("ccsbt","iattc","iccat","iotc","wcpfc",
          "iccat1","iccat2","wcpfc1","wcpfc2","wcpfc3","iattc1","iattc2","iattc3","iattc4"))

#Cells where RFMOs overlap
overlapcells <- read.csv("RFMOOverlap.csv",header=T)

#join the overlapping cell key to the spatial data
spatoverlaps <- left_join(spatc,overlapcells, by="BigCellID")
#find rows with overlaps
overlap <- spatoverlaps[complete.cases(spatoverlaps),]
#separate rows with no overlaps
nooverlap <- spatoverlaps[!complete.cases(spatoverlaps),]

#find where WCPFC overlaps IATTC
iattc <- overlap[overlap$RFMO1 == "IATTC",]
#keep entries for IATTC, RFMOID = 5
iattc <- iattc[iattc$RFMOID == 5,]

#find where CCSBT overlaps the other RFMOs
ccsbt <- overlap[overlap$RFMO1 == "CCSBT",]
#remove southern bluefin tuna from ICCAT, IOTC and WCPFC
bluefin <- ccsbt[ccsbt$TaxonKey == 600145,]
#only keep bluefin catch from CCSBT
bluefin <- bluefin[bluefin$RFMOID == 3,]
#keep catch entries for other taxa where other RFMOs overlap CCSBT
other <- ccsbt[ccsbt$TaxonKey != 600145,]

#bind no overlaps, IATTC, CCSBT bluefin and other CCSBT overlap together for new spatc
spatc2 <- rbind(nooverlap,iattc,bluefin,other)
#remove RFMO overlap columns
spatc2 <- spatc2 %>% select(-c(RFMO1,RFMO2,RFMO3))

#remove the duplicated rows from the join
spatc2 <- spatc2[!duplicated(spatc2),]

#reset spatc
spatc <- spatc2

#clear working data
rm(list=c("bluefin","ccsbt","iattc","nooverlap","other",
          "overlap","overlapcells","spatoverlaps","spatc2"))

#Discard BCIDs
dbcid <- read.csv("DiscardRegionBigCellID2017.csv", header = T)

#Match Ocean Region to BCIDs
spatc <-left_join(spatc, dbcid, by= "BigCellID")
#Remove ocean name
spatc <- spatc %>% select(-TunaDiscardRegion)

#Indian and Atlantic Oceans
#Spatialized catch
iaspatc <- subset(spatc, TunaDiscardRegionID != 5) #Not TDRID=5, ie Pacific
#Remove the catch taxon ID, Indian and Atlantic discards are not associated
iaspatc <- iaspatc %>% select(-TaxonKey)
#Discard Rates
riadisc <- read.csv("Discard Rates Atlantic and Indian Oceans.csv", header = T)

#TunaDiscardRegionIDs
tdrid <- read.table("TunaDiscardRegionID.txt", header = T, sep="")

#Match Ocean Regions to discard rates
riadisc <- left_join(riadisc, tdrid, by="TunaDiscardRegion")

#Remove the text region
riadisc <- riadisc %>% select(-TunaDiscardRegion)

#Merge discard rates with catch
ciadisc <- merge(iaspatc, riadisc, by=c("Year","Layer3GearID","TunaDiscardRegionID"))

#Remove any that did not have a match
ciadisc <- ciadisc[is.na(ciadisc$DiscardRate)==F,]

#Calculate discard weight
ciadisc$DiscardCatch <- ciadisc$Catch*ciadisc$DiscardRate

#Remove catch and discard rate columns
ciadisc <- ciadisc %>% select(-c(Catch, DiscardRate))

#Save the final Indian and Atlantic discarded catch for later
iadiscfin <- ciadisc

#Remove unneeded stored data
rm(list=c("ciadisc","iaspatc","riadisc"))


#Pacific Ocean
#Spatialized catch
pspatc <- subset(spatc, TunaDiscardRegionID == 5) #TDRID=5, Pacific
#Remove the catch taxon ID, Pacific discards are not associated
pspatc <- pspatc %>% select(-TaxonKey)
#Discard Rates
rpdisc <- read.csv("Discard Rates Pacific Ocean.csv", header = T)

#Assign ocean region ID to discard rates, delete ocen region text column
rpdisc$TunaDiscardRegionID <- 5

#Remove the text region
rpdisc <- rpdisc %>% select(-TunaDiscardRegion)

#Merge discard rates with catch
cpdisc <- left_join(pspatc, rpdisc, by=c("Year","Layer3GearID","TunaDiscardRegionID", "FishingEntityID"))

#Remove any that did not have a match
cpdisc <- cpdisc[is.na(cpdisc$DiscardRate)==F,]

#Calculate discard weight
cpdisc$DiscardCatch <- cpdisc$Catch*cpdisc$DiscardRate

#Remove catch and discard rate columns
cpdisc <- cpdisc %>% select(-c(Catch, DiscardRate))

#Save the final Pacific discarded catch for later
pdiscfin <- cpdisc

#Remove unneeded stored data
rm(list=c("cpdisc","pspatc","rpdisc"))

#order the final data sets
iadiscfin <- iadiscfin[c("Year","RFMOID","FishingEntityID","TaxonKey","SpeciesGroupID","Layer3GearID","GearTypeID","TunaDiscardRegionID","BigCellID","DiscardCatch")]
pdiscfin <- pdiscfin[c("Year","RFMOID","FishingEntityID","TaxonKey","SpeciesGroupID","Layer3GearID","GearTypeID","TunaDiscardRegionID","BigCellID","DiscardCatch")]

#Bind data together
discfin <- rbind(iadiscfin,pdiscfin)
#Aggregate discfin
discfin <- aggregate(discfin$DiscardCatch, by=list(discfin$Year,discfin$RFMOID,discfin$FishingEntityID,discfin$TaxonKey,discfin$SpeciesGroupID,discfin$Layer3GearID,discfin$GearTypeID,discfin$TunaDiscardRegionID,discfin$BigCellID),FUN=sum)
colnames(discfin) <- c("Year","RFMOID","FishingEntityID","TaxonKey","SpeciesGroupID","Layer3GearID","GearTypeID","TunaDiscardRegionID","BigCellID","DiscardCatch")

#remove working data
rm(list=c("dbcid","iadiscfin","pdiscfin","tdrid"))

#### Section III-Discards: Writing #### 

#Export data
write.table(discfin[1:1000000, ], "../Outputs/OUTPUT Spatialized Discards 1 of 5.csv", sep=",", row.names=F, quote=F)
write.table(discfin[1000001:2000000, ], "../Outputs/OUTPUT Spatialized Discards 2 of 5.csv", sep=",", row.names=F, quote=F)
write.table(discfin[2000001:3000000, ], "../Outputs/OUTPUT Spatialized Discards 3 of 5.csv", sep=",", row.names=F, quote=F)
write.table(discfin[3000001:4000000, ], "../Outputs/OUTPUT Spatialized Discards 4 of 5.csv", sep=",", row.names=F, quote=F)
write.table(discfin[4000001:nrow(discfin), ], "../Outputs/OUTPUT Spatialized Discards 5 of 5.csv", sep=",", row.names=F, quote=F)


#Final Results----
#Prepare for writing

#Add catch types to landings and discards
#Landing are CatchTypeID = 1 
spatc$CatchTypeID <- 1
#Discards are CatchTypeID = 2 
discfin$CatchTypeID <- 2

#rename discard catch
colnames(discfin)[colnames(discfin) == "DiscardCatch"] <- "Catch"

#select final columns for landings and discards
spatc <- spatc %>% select(RFMOID,Year,FishingEntityID,Layer3GearID,TaxonKey,BigCellID,Catch,CatchTypeID)
discfin <- discfin %>% select(RFMOID,Year,FishingEntityID,Layer3GearID,TaxonKey,BigCellID,Catch,CatchTypeID)

#bind landings and discards together for final tuna database
tuna.tot <- rbind(spatc,discfin)

#aggregate final database by all columns to reduce size as much as possible
tuna.tot <- aggregate(tuna.tot$Catch, by=list(tuna.tot$Year,tuna.tot$RFMOID,tuna.tot$FishingEntityID,tuna.tot$TaxonKey,tuna.tot$Layer3GearID,tuna.tot$BigCellID,tuna.tot$CatchTypeID),FUN=sum)
colnames(tuna.tot) <- c("Year","RFMOID","FishingEntityID","TaxonKey","Layer3GearID","BigCellID","CatchTypeID","Catch")
tuna.tot <- tuna.tot[order(tuna.tot$Year),]

#Write Results
write.table(tuna.tot[1:1000000, ], "../Results/OUTPUT Tuna 1 of 13 06-20-2018.csv", sep=",", row.names=F, quote=F)
write.table(tuna.tot[1000001:2000000, ], "../Results/OUTPUT Tuna 2 of 13 06-20-2018.csv", sep=",", row.names=F, quote=F)
write.table(tuna.tot[2000001:3000000, ], "../Results/OUTPUT Tuna 3 of 13 06-20-2018.csv", sep=",", row.names=F, quote=F)
write.table(tuna.tot[3000001:4000000, ], "../Results/OUTPUT Tuna 4 of 13 06-20-2018.csv", sep=",", row.names=F, quote=F)
write.table(tuna.tot[4000001:5000000, ], "../Results/OUTPUT Tuna 5 of 13 06-20-2018.csv", sep=",", row.names=F, quote=F)
write.table(tuna.tot[5000001:6000000, ], "../Results/OUTPUT Tuna 6 of 13 06-20-2018.csv", sep=",", row.names=F, quote=F)
write.table(tuna.tot[6000001:7000000, ], "../Results/OUTPUT Tuna 7 of 13 06-20-2018.csv", sep=",", row.names=F, quote=F)
write.table(tuna.tot[7000001:8000000, ], "../Results/OUTPUT Tuna 8 of 13 06-20-2018.csv", sep=",", row.names=F, quote=F)
write.table(tuna.tot[8000001:9000000, ], "../Results/OUTPUT Tuna 9 of 13 06-20-2018.csv", sep=",", row.names=F, quote=F)
write.table(tuna.tot[9000001:10000000, ], "../Results/OUTPUT Tuna 10 of 13 06-20-2018.csv", sep=",", row.names=F, quote=F)
write.table(tuna.tot[10000001:11000000, ], "../Results/OUTPUT Tuna 11 of 13 06-20-2018.csv", sep=",", row.names=F, quote=F)
write.table(tuna.tot[11000001:12000000, ], "../Results/OUTPUT Tuna 12 of 13 06-20-2018.csv", sep=",", row.names=F, quote=F)
write.table(tuna.tot[12000001:nrow(tuna.tot), ], "../Results/OUTPUT Tuna 13 of 13 06-20-2018.csv", sep=",", row.names=F, quote=F)

#Aggregate catch to year and BigCellID for mapping
maps <- aggregate(tuna.tot$Catch, by=list(tuna.tot$Year,tuna.tot$BigCellID),FUN=sum)
names(maps) <- c("Year","BigCellID","Catch")
write.csv(maps, "../Results/Aggregated Catch for Mapping 06-20-2018.csv",row.names=F)
