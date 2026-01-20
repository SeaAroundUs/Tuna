#
#====IOTC
#
#--------------------------------------------------Nominal data--------------------------------------------------
rm(list=ls())
#library(tidyverse)
library(reshape)
library(dplyr)

#read in nominal data set
iotc.n= read.csv("INPUT IOTC Nominal Catch for Formatting.csv", sep=",")

#read in numeric codes for country, gear, taxon and area
iotc.gea= read.table("input_codes/INPUT IOTC Nominal Gear Codes.txt", header=T)
iotc.cou= read.csv("input_codes/INPUT IOTC Nominal Country Codes.csv", sep=",")
iotc.spp= read.table("input_codes/INPUT IOTC Species Codes.txt", header=T)
iotc.are= read.csv("input_codes/INPUT IOTC Area Codes.csv", sep=",")

#merge nominal data with numeric codes 
iotc.N= merge(iotc.n, iotc.cou, by= c("Country"), all.x=T)
iotc.N= merge(iotc.N, iotc.gea, by= c("Gear"), all.x=T)
iotc.N= merge(iotc.N, iotc.are, by= c("Area"), all.x=T)
iotc.N= merge(iotc.N, iotc.spp, by= c("SpeciesCode"), all.x=T)

#remove columns with alphabetical names
iotc.N= iotc.N[,-c(1:4)]

#Remove entries with 0 or NA values
iotc.N= iotc.N[ is.na(iotc.N$Catch)==F & iotc.N$Catch!=0, ]

#EC: force catch column to be numeric
#iotc.Ntest <- iotc.N %>% mutate(Catch = as.numeric(Catch))
##
#remove everything except for merged data table
rm(list = c("iotc.are", "iotc.cou", "iotc.gea", "iotc.spp", "iotc.n"))

iotc.art = iotc.N[iotc.N$Sector=="ART", ]
iotc.ind = iotc.N[iotc.N$Sector=="IND", ]
iotc.art_countries = iotc.art[iotc.art$FishingEntityID==58, ] #France has other 'artisanal' gears than below but is reclassified as industrial because its a DWF  #EC: France does not have artisanal catch
iotc.art_subset_ind_gears = iotc.art[iotc.art$Layer3GearID %in% c(3, 41, 43, 55, 68),] #Separate mechanized baitboats and trolls, and various purse seines 
iotc.art_subset_ind_gears = iotc.art_subset_ind_gears[iotc.art_subset_ind_gears$GearGroupID %in% c(31, 10, 41), ] #Eliminate extra 'purse seine' like gear that for now we'll keep as artisanal

iotc.art_subset_ind_gears = iotc.art_subset_ind_gears[iotc.art_subset_ind_gears$FishingEntityID !=9, ] #Eliminate Bahrain as catch is assigned in layers 1 and 2
iotc.art_subset_ind_gears = iotc.art_subset_ind_gears[iotc.art_subset_ind_gears$FishingEntityID !=156, ] #Eliminate Saudi Arabia as catch is assigned in layers 1 and 2

iotc.N = rbind(iotc.ind, iotc.art_subset_ind_gears, iotc.art_countries) #Combine reclassified artisanal to industrial 
iotc.N$Sector="IND"  #Relabel all as industrial sector. (Unnecessary but explicit of what we're doing)
iotc.N = iotc.N[ ,-c(2)]  #Delete sector column 

iotc.N= iotc.N[ , c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","AreaID","TaxonKey","SpeciesGroupID","Catch") ] #EC: this is to reorganize the columns

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

#Matching by year, gear, area, and species
y2= aggregate(y$Catch, by= list(y$Year, y$TaxonKey, y$Layer3GearID, y$AreaID), sum)
colnames(y2)= c("Year","TaxonKey","Layer3GearID","AreaID","TotalCatch")

y3= merge(y, y2, by= c("Year","TaxonKey","Layer3GearID","AreaID"), all.x=T)
y3$Proportion= y3$Catch / y3$TotalCatch
y3= y3[,c("Year","Layer3GearID","GearGroupID","FishingEntityID","CountryGroupID","AreaID","TaxonKey","SpeciesGroupID","Proportion")]

z= merge(x, y3, by= c("Year","Layer3GearID","GearGroupID","AreaID","TaxonKey","SpeciesGroupID"), all.x=T)
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

y5= merge(y, y4, by= c("Year","Layer3GearID","AreaID"), all.x=T)
y5$Proportion= y5$Catch / y5$TotalCatch
y5= y5[,c("Year","Layer3GearID","GearGroupID","FishingEntityID","CountryGroupID","AreaID","TaxonKey","SpeciesGroupID","Proportion")]

#EC original code using merge took too much memory
z2 <- left_join(znm1,y5,by = c("Year", "Layer3GearID", "AreaID"),relationship = "many-to-many")
#z2= merge(znm1, y5, by= c("Year","Layer3GearID","AreaID"), all.x=T)
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

y5= merge(y, y4, by= c("Year","AreaID"), all.x=T)
y5$Proportion= y5$Catch / y5$TotalCatch
y5= y5[,c("Year","Layer3GearID","GearGroupID","FishingEntityID","CountryGroupID","AreaID","TaxonKey","SpeciesGroupID","Proportion")]

z3= merge(znm2, y5, by= c("Year","AreaID"), all.x=T)
z3$PropCatch= z3$Catch * z3$Proportion

zm3= z3[is.na(z3$Proportion)==F,]
znm3= z3[is.na(z3$Proportion),]

zm3= zm3[,c("Year","FishingEntityID.y","CountryGroupID.y","Layer3GearID.x","GearGroupID.x","AreaID","TaxonKey.x","SpeciesGroupID.x","PropCatch")]
colnames(zm3)= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","AreaID","TaxonKey","SpeciesGroupID","Catch")

x= rbind(y, zm1, zm2, zm3)

iotc.N= aggregate(x$Catch, by= list(x$Year,x$FishingEntityID,x$CountryGroupID,x$Layer3GearID,x$GearGroupID,x$AreaID,x$TaxonKey,x$SpeciesGroupID), sum)
colnames(iotc.N)= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","AreaID","TaxonKey","SpeciesGroupID","Catch")

iotc.N= iotc.N[order(iotc.N$Year), ]

write.table(iotc.N, "Formatted IOTC Nominal Catch For Spatial Matching.csv", sep=",", row.names=F)	#Export the new table to a csv file

#--------------------------------------------------Spatial data--------------------------------------------------

rm(list=ls())

library(reshape)

#Read spatial catch data
iotc.ss= read.csv("INPUT IOTC Spatial Surface Catch for Formatting.csv", sep=",")
iotc.sl= read.csv("INPUT IOTC Spatial Longline Catch for Formatting.csv", sep=",")
iotc.sc= read.csv("INPUT IOTC Spatial Coastal Catch for Formatting.csv", sep=",")

#Read spatial data codes
iotc.spps= read.table("input_codes/INPUT IOTC Spatial Surface Species Codes.txt", header=T)
iotc.sppl= read.table("input_codes/INPUT IOTC Spatial Longline Species Codes.txt", header=T)
iotc.sppc= read.table("input_codes/INPUT IOTC Spatial Coastal Species Codes.txt", header=T)
iotc.cel= read.csv("input_codes/INPUT IOTC Spatial Cell Codes.csv", sep=",", header=T)
iotc.are= read.csv("input_codes/INPUT IOTC Area Codes.csv", sep=",")
iotc.gea= read.table("input_codes/INPUT IOTC Spatial Gear Codes.txt", header=T)
iotc.cou= read.csv("input_codes/INPUT IOTC Spatial Country Codes.csv", sep=",")
cellid= read.csv("input_codes/INPUT CellTypeID.csv",sep=",",header=T)
#SO Aligned BigCellTypeID columns
names(cellid)[names(cellid) == "big_cell_type_id"] <- "BigCellTypeID"
names(cellid)[names(cellid) == "big_cell_id"] <- "BigCellID"

#Surface data
iotc.ss= iotc.ss[iotc.ss$CatchUnits=="MT",]
iotc.Ss= merge(iotc.ss, iotc.cel, by=c("Grid"), all.x=T)
iotc.Ss= merge(iotc.Ss, cellid, by=c("x","y","BigCellTypeID"), all.x=T)
write.table(iotc.Ss[is.na(iotc.Ss$BigCellID),], "OUTPUT IOTC No Spatial Surface Match Entries.csv", sep=",", row.names=F)
iotc.Ss= iotc.Ss[is.na(iotc.Ss$BigCellID)==F , ] #Only keep cells with a BigCellID match, the remainder are erroneously reported on land

iotc.Ss= merge(iotc.Ss, iotc.cou, by=c("Fleet"), all.x=T)
iotc.Ss= merge(iotc.Ss, iotc.gea, by=c("Gear"), all.x=T)
iotc.Ss= iotc.Ss[ , -c(1:6) ] #Get rid of columns with outdated codes

#Re-shape data, discard empty catch entries, and aggregate catch by year
iotc.Ss= melt(iotc.Ss, id= c("Year","MonthStart","CatchUnits","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","AreaID","BigCellID"))
colnames(iotc.Ss)[10:11]= c("SpeciesCode","Catch")
iotc.Ss= merge(iotc.Ss, iotc.spps, by= c("SpeciesCode"),all.x=T)
iotc.Ss= iotc.Ss[is.na(iotc.Ss$Catch)==F, -c(1)]

iotc.Ss= aggregate(iotc.Ss$Catch, by= list(iotc.Ss$Year,iotc.Ss$CatchUnits,iotc.Ss$FishingEntityID,iotc.Ss$CountryGroupID,iotc.Ss$Layer3GearID,iotc.Ss$GearGroupID,iotc.Ss$AreaID,iotc.Ss$BigCellID,iotc.Ss$TaxonKey,iotc.Ss$SpeciesGroupID), sum)
iotc.Ss= iotc.Ss[ ,-c(2)]

colnames(iotc.Ss)= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","AreaID","BigCellID","TaxonKey","SpeciesGroupID","Catch")


#All countries.......... USSR.........

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

#All countries split
#Allocate catch out from "All countries" fishing entity (FishingEntityID= 1000). 
x= iotc.Ss[iotc.Ss$FishingEntityID==1000,]
y= iotc.Ss[iotc.Ss$FishingEntityID!=1000,]

#Matching by year, gear, area, and species
y2= aggregate(y$Catch, by= list(y$Year, y$TaxonKey, y$Layer3GearID, y$AreaID), sum)
colnames(y2)= c("Year","TaxonKey","Layer3GearID","AreaID","TotalCatch")

y3= merge(y, y2, by= c("Year","TaxonKey","Layer3GearID","AreaID"), all.x=T)
y3$Proportion= y3$Catch / y3$TotalCatch
y3= y3[,c("Year","Layer3GearID","GearGroupID","FishingEntityID","CountryGroupID","AreaID","TaxonKey","SpeciesGroupID","Proportion")]

y3= aggregate(y3$Proportion, by= list(y3$Year, y3$TaxonKey, y3$FishingEntityID, y3$CountryGroupID, y3$Layer3GearID, y3$AreaID), sum)
colnames(y3)= c("Year","TaxonKey","FishingEntityID", "CountryGroupID","Layer3GearID","AreaID","Proportion")

z= merge(x, y3, by= c("Year","Layer3GearID","AreaID","TaxonKey"), all.x=T)
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

y3= merge(y, y2, by= c("Year","TaxonKey","AreaID"), all.x=T)
y3$Proportion= y3$Catch / y3$TotalCatch
y3= y3[,c("Year","Layer3GearID","GearGroupID","FishingEntityID","CountryGroupID","AreaID","TaxonKey","SpeciesGroupID","Proportion")]

y3= aggregate(y3$Proportion, by= list(y3$Year, y3$TaxonKey, y3$FishingEntityID, y3$CountryGroupID, y3$AreaID), sum)
colnames(y3)= c("Year","TaxonKey","FishingEntityID", "CountryGroupID","AreaID","Proportion")

z= merge(znm1, y3, by= c("Year","AreaID","TaxonKey"), all.x=T)
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

y3= merge(y, y2, by= c("Year","AreaID"), all.x=T)
y3$Proportion= y3$Catch / y3$TotalCatch
y3= y3[,c("Year","Layer3GearID","GearGroupID","FishingEntityID","CountryGroupID","AreaID","TaxonKey","SpeciesGroupID","Proportion")]

y3= aggregate(y3$Proportion, by= list(y3$Year, y3$TaxonKey, y3$FishingEntityID, y3$CountryGroupID, y3$AreaID), sum)
colnames(y3)= c("Year","TaxonKey","FishingEntityID", "CountryGroupID","AreaID","Proportion")

z= merge(znm1, y3, by= c("Year","AreaID"), all.x=T)
z$PropCatch= z$Catch * z$Proportion

zm3= z[is.na(z$Proportion)==F,]
znm3= z[is.na(z$Proportion),]

zm3= zm3[,c("Year","FishingEntityID.y","CountryGroupID.y","Layer3GearID","GearGroupID","AreaID","BigCellID","TaxonKey.x","SpeciesGroupID","PropCatch")]
colnames(zm3)= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","AreaID","BigCellID","TaxonKey","SpeciesGroupID","Catch")

znm3= znm3[,c("Year","FishingEntityID.x","CountryGroupID.x","Layer3GearID","GearGroupID","AreaID","BigCellID","TaxonKey.x","SpeciesGroupID","Catch")]
colnames(znm3)= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","AreaID","BigCellID","TaxonKey","SpeciesGroupID","Catch")

iotc.Ss= rbind(y, zm1, zm2, zm3) 

write.table(iotc.Ss, "Formatted IOTC Spatial Surface Catch For Spatial Matching.csv", sep=",", row.names=F)  #Export the new table to a csv file


#Coastal data
iotc.Sc= merge(iotc.sc, iotc.cel, by=c("Grid"), all.x=T)
iotc.Sc= merge(iotc.Sc, cellid, by=c("x","y","BigCellTypeID"), all.x=T)
write.table(iotc.Sc[is.na(iotc.Sc$BigCellID),], "OUTPUT IOTC No Spatial Coastal Match Entries.csv", sep=",", row.names=F)
iotc.Sc= iotc.Sc[is.na(iotc.Sc$BigCellID)==F , ] #Only keep cells with a BigCellID match

iotc.Sc= merge(iotc.Sc, iotc.cou, by=c("Fleet"), all.x=T)
iotc.Sc= merge(iotc.Sc, iotc.gea, by=c("Gear"), all.x=T)
iotc.Sc= iotc.Sc[ , -c(1:6) ] #Get rid of columns with outdated codes

#Re-shape data, discard empty catch entries, and aggregate catch by year
iotc.Sc= melt(iotc.Sc, id= c("Year","MonthStart","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","AreaID","BigCellID"))
colnames(iotc.Sc)[9:10]= c("SpeciesCode","Catch")
iotc.Sc= merge(iotc.Sc, iotc.sppc, by= c("SpeciesCode"),all.x=T)
iotc.Sc= iotc.Sc[is.na(iotc.Sc$Catch)==F, -c(1)]  #Discard entries without catch, and outdated species code

iotc.Sc= aggregate(iotc.Sc$Catch, by= list(iotc.Sc$Year,iotc.Sc$FishingEntityID,iotc.Sc$CountryGroupID,iotc.Sc$Layer3GearID,iotc.Sc$GearGroupID,iotc.Sc$AreaID,iotc.Sc$BigCellID,iotc.Sc$TaxonKey,iotc.Sc$SpeciesGroupID), sum)
colnames(iotc.Sc)= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","AreaID","BigCellID","TaxonKey","SpeciesGroupID","Catch")
write.table(iotc.Sc, "Formatted IOTC Spatial Coastal Catch For Spatial Matching.csv", sep=",", row.names=F)  #Export the new table to a csv file


#Longline data
iotc.Sl= merge(iotc.sl, iotc.cel, by=c("Grid"), all.x=T)
iotc.Sl= merge(iotc.Sl, cellid, by=c("x","y","BigCellTypeID"), all.x=T)
write.table(iotc.Sl[is.na(iotc.Sl$BigCellID),], "OUTPUT IOTC No Spatial Longline Match Entries.csv", sep=",", row.names=F)
iotc.Sl= iotc.Sl[is.na(iotc.Sl$BigCellID)==F , ] #Only keep cells with a BigCellID match

iotc.Sl= merge(iotc.Sl, iotc.cou, by=c("Fleet"), all.x=T)
iotc.Sl= merge(iotc.Sl, iotc.gea, by=c("Gear"), all.x=T)
iotc.Sl= iotc.Sl[ , -c(1:6) ] #Get rid of columns with outdated codes

#Re-shape data, discard empty catch entries, and aggregate catch by year
iotc.Sl= melt(iotc.Sl, id= c("Year","MonthStart","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","AreaID","BigCellID"))
colnames(iotc.Sl)[9:10]= c("SpeciesCode","Catch")
iotc.Sl= merge(iotc.Sl, iotc.sppl, by= c("SpeciesCode"),all.x=T)
iotc.Sl= iotc.Sl[is.na(iotc.Sl$Catch)==F, -c(1)]  #Discard entries without catch, and outdated species code

iotc.Sl= aggregate(iotc.Sl$Catch, by= list(iotc.Sl$Year,iotc.Sl$FishingEntityID,iotc.Sl$CountryGroupID,iotc.Sl$Layer3GearID,iotc.Sl$GearGroupID,iotc.Sl$AreaID,iotc.Sl$BigCellID,iotc.Sl$TaxonKey,iotc.Sl$SpeciesGroupID), sum)
colnames(iotc.Sl)= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","AreaID","BigCellID","TaxonKey","SpeciesGroupID","Catch")
write.table(iotc.Sl, "Formatted IOTC Spatial Longline Catch For Spatial Matching.csv", sep=",", row.names=F)  #Export the new table to a csv file


#=============================================================================================================
#==SECTION I: INITIAL DATA AND TRANSFORMATION
#=============================================================================================================
rm(list=ls())

library(reshape)

#1.Read in databases of nominal and spatialized catch by ocean

nom=read.csv("Formatted IOTC Nominal Catch For Spatial Matching.csv",sep=",",header=T)	#Yearly nominal catch

spats=read.csv("Formatted IOTC Spatial Surface Catch For Spatial Matching.csv",sep=",",header=T)  #Yearly available spatial catch
spatl=read.csv("Formatted IOTC Spatial Longline Catch For Spatial Matching.csv",sep=",",header=T)  #Yearly available spatial catch
spatc=read.csv("Formatted IOTC Spatial Coastal Catch For Spatial Matching.csv",sep=",",header=T)	#Yearly available spatial catch
#spatr= read.csv("INPUT IOTC Reassigned Spatial Catch For Spatial Matching.csv",sep=",",header=T)  #Yearly available spatial catch
#SPATR was removed as we no longer spatially re-assign catches

spat= rbind(spats, spatl, spatc)

nom= nom[nom$Catch!=0,] #Exclude nominal records with zero catch
#nom= nom[nom$Catch>1,] #Exclude nominal catch records with less than 1 tonne of catch 

spat= spat[spat$Catch!=0,]
spat= spat[is.na(spat$BigCellID)==F,]

gearres= read.csv("input_codes/INPUT IOTC GearRestrictionTable.csv",sep=",")
areares= read.csv("input_codes/INPUT IOTC AreaRestrictionTable.csv",sep=",")

tot.nom.ct=sum(nom$Catch)	#Total catch in nominal database; this is used to compare catches after spatializing.
tot.spat.ct= sum(spat$Catch)	#Total catch in spatial database; this is not used in the routine but is available to look at.

rm(list=c("spats","spatl","spatc"))  #MEMORY CLEAN-UP

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
match.categ= list(spat$FishingEntityID, spat$CountryGroupID,spat$Year, spat$AreaID, spat$Layer3GearID, spat$GearGroupID, spat$TaxonKey, spat$SpeciesGroupID)
merge.categ= c("FishingEntityID", "CountryGroupID","Year","AreaID","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID")
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


#=============================================================================================================
#==SECTION III: 
#=============================================================================================================
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

#=============================================================================================================
#==SECTION IV: Re-Run with Larger Year Ranges
#=============================================================================================================

db.nomatch = nom #Re-set db.nomatch
#Make db.nomatch equal to all nom catch except those already matched as indicated by NIDs:
db.nomatch = subset( merge(db.nomatch, cbind("NID"=sort(unique(db.match$NID)), "Match"=1), by="NID",all.x=T), is.na(Match) )[,-c(11)]
#Re-run matching sequence above with expanded year range on only unmatched nom catch

sum(db.match$Catch)/sum(nom$Catch) #If equal to 1, skip to end and export data 

fids= sort(unique(db.nomatch$FishingEntityID)) #Vector of unique FishingEntityIDs in the nominal data

YRs= c(10,20,40) #Year ranges for subsetting (i.e. +/- years to match over) TC Removed 1, 10, 15 as these are saying that categories are more important than time. This will bias results towards future tuna fisheries rather than spatial expansion. 

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
  
  for(z in 18:33) #Ten is the number of category combos in the ifelse statment below
  {
    
    for(j in 1:length(yrs)) #Loop over year
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

###STOP###

#stop("SHOULD BE DONE...") #

#====================================Writing======================================#

db.match= db.match[order(db.match$Year),]

db.match= aggregate(db.match$Catch, by= list(db.match$Year,db.match$FishingEntityID,db.match$Layer3GearID,db.match$TaxonKey,db.match$BigCellID,db.match$MatchID), sum)
colnames(db.match)= c("Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","MatchID","Catch")

db.match$RFMOID= 7  #RFMO i.d. for IOTC = 7
db.match= db.match[ order(db.match$Year), c("RFMOID","Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","Catch","MatchID") ]

#SO Added check to row count and only create the second csv output if above 1000000
#write.table(db.match[1:dim(db.match)[1], ], "FINAL IOTC Spatialized Catch 1 of 1.csv", sep=",", row.names=F, quote=F)
#if (nrow(db.match) > 1000000) {
#  write.table(db.match[1000000:dim(db.match)[1], ], "FINAL IOTC Spatialized Catch 2 of 2.csv", sep=",", row.names=F, quote=F)
#}

chunk_size <- 1000000
n <- nrow(db.match)
num_chunks <- ceiling(n / chunk_size)

for (i in 1:num_chunks) {
  start_row <- ((i - 1) * chunk_size) + 1
  end_row <- min(i * chunk_size, n)
  chunk <- db.match[start_row:end_row, ]
  filename <- paste0("Final IOTC Spatialized Catch with MatchID ",i, " of ", num_chunks,".csv")
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
write.table(agc, "OUTPUT IOTC Catch by Year and MatchID.csv", sep=",",row.names=F, quote=F)

##================================================Old Code is below.==========================================
#
#db.nomatch= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.nomatch
#db.match= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.match
#
##=============================================================================================================
##==SECTION IV: MATCHING BY Year,AreaID,Layer3GearID,GearGroupID,SpeciesGroupID
##=============================================================================================================
#
#nom.data= db.nomatch
#match.categ= list(spat$Year, spat$AreaID, spat$Layer3GearID, spat$GearGroupID, spat$SpeciesGroupID)
#merge.categ= c("Year","AreaID","Layer3GearID","GearGroupID","SpeciesGroupID")
#match.categ.names= c(merge.categ,"TotalCatch")
#matchid= 3
#
#db.nomatch= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.nomatch
#db.match= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.match
#
##=============================================================================================================
##==SECTION V: MATCHING BY Year,AreaID, GearGroupID,SpeciesGroupID
##=============================================================================================================
#
#nom.data= db.nomatch
#match.categ= list(spat$Year, spat$AreaID, spat$GearGroupID, spat$SpeciesGroupID)
#merge.categ= c("Year","AreaID","GearGroupID","SpeciesGroupID")
#match.categ.names= c(merge.categ,"TotalCatch")
#matchid= 4
#
#db.nomatch= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.nomatch
#db.match= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.match
#
##=============================================================================================================
##==SECTION VI: MATCHING BY Year,AreaID,TaxonKey,SpeciesGroupID
##=============================================================================================================
#
#nom.data= db.nomatch
#match.categ= list(spat$Year, spat$AreaID, spat$TaxonKey, spat$SpeciesGroupID)
#merge.categ= c("Year","AreaID","TaxonKey", "SpeciesGroupID")
#match.categ.names= c(merge.categ,"TotalCatch")
#matchid= 5
#
#db.nomatch= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.nomatch
#db.match= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.match
#
##=============================================================================================================
##==SECTION VII: MATCHING BY Year,AreaID,Layer3GearID,GearGroupID
##=============================================================================================================
#
#nom.data= db.nomatch
#match.categ= list(spat$Year, spat$AreaID,spat$Layer3GearID, spat$GearGroupID)
#merge.categ= c("Year","AreaID","Layer3GearID", "GearGroupID")
#match.categ.names= c(merge.categ,"TotalCatch")
#matchid= 6
#
#db.nomatch= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.nomatch
#db.match= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.match
#
##=============================================================================================================
##==SECTION VIII: MATCHING BY AreaID,Layer3GearID,GearGroupID,TaxonKey,SpeciesGroupID
##=============================================================================================================
#
#nom.data= db.nomatch
#match.categ= list(spat$AreaID,spat$Layer3GearID, spat$GearGroupID, spat$TaxonKey, spat$SpeciesGroupID)
#merge.categ= c("AreaID","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID")
#match.categ.names= c(merge.categ,"TotalCatch")
#matchid= 7
#
#db.nomatch= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.nomatch
#db.match= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.match
#
##=============================================================================================================
##==SECTION IX: MATCHING BY AreaID,GearGroupID,TaxonKey,SpeciesGroupID
##=============================================================================================================
#
#nom.data= db.nomatch
#match.categ= list(spat$AreaID,spat$GearGroupID, spat$TaxonKey, spat$SpeciesGroupID)
#merge.categ= c("AreaID","GearGroupID","TaxonKey","SpeciesGroupID")
#match.categ.names= c(merge.categ,"TotalCatch")
#matchid= 8
#
#db.nomatch= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.nomatch
#db.match= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.match
#
#
##=============================================================================================================
##==SECTION X: MATCHING BY AreaID,Layer3GearID,GearGroupID,SpeciesGroupID
##=============================================================================================================
#
#nom.data= db.nomatch
#match.categ= list(spat$AreaID,spat$Layer3GearID, spat$GearGroupID, spat$SpeciesGroupID)
#merge.categ= c("AreaID","Layer3GearID", "GearGroupID","SpeciesGroupID")
#match.categ.names= c(merge.categ,"TotalCatch")
#matchid= 9
#
#db.nomatch= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.nomatch
#db.match= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.match
#
##=============================================================================================================
##==SECTION XI: MATCHING BY AreaID,GearGroupID,SpeciesGroupID
##=============================================================================================================
#
#nom.data= db.nomatch
#match.categ= list(spat$AreaID,spat$GearGroupID, spat$SpeciesGroupID)
#merge.categ= c("AreaID","GearGroupID","SpeciesGroupID")
#match.categ.names= c(merge.categ,"TotalCatch")
#matchid= 10
#
#db.nomatch= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.nomatch
#db.match= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.match
#
##=============================================================================================================
##==SECTION XII: MATCHING BY AreaID,TaxonKey,SpeciesGroupID
##=============================================================================================================
#
#nom.data= db.nomatch
#match.categ= list(spat$AreaID, spat$TaxonKey, spat$SpeciesGroupID)
#merge.categ= c("AreaID","TaxonKey","SpeciesGroupID")
#match.categ.names= c(merge.categ,"TotalCatch")
#matchid= 11
#
#db.nomatch= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.nomatch
#db.match= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.match
#
##=============================================================================================================
##==SECTION XIII: MATCHING BY AreaID, Layer3GearID,GearGroupID
##=============================================================================================================
#
#nom.data= db.nomatch
#match.categ= list(spat$AreaID,spat$Layer3GearID, spat$GearGroupID)
#merge.categ= c("AreaID","Layer3GearID","GearGroupID")
#match.categ.names= c(merge.categ,"TotalCatch")
#matchid= 12
#
#db.nomatch= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.nomatch
#db.match= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.match
#
##=============================================================================================================
##==SECTION XIV: MATCHING BY Year, AreaID
##=============================================================================================================
#
#nom.data= db.nomatch
#match.categ= list(spat$Year,spat$AreaID)
#merge.categ= c("Year", "AreaID")
#match.categ.names= c(merge.categ,"TotalCatch")
#matchid= 13
#
#db.nomatch= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.nomatch
#db.match= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.match
#
##================EXPORT DATA
#
#db.match= db.match[order(db.match$Year),]
#
#db.match= aggregate(db.match$Catch, by= list(db.match$Year,db.match$FishingEntityID,db.match$Layer3GearID,db.match$TaxonKey,db.match$BigCellID,db.match$MatchID), sum)
#colnames(db.match)= c("Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","MatchID","Catch")
#
#db.match$RFMOID= 7  #RFMO i.d. for IOTC = 7
#db.match= db.match[ order(db.match$Year), c("RFMOID","Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","Catch","MatchID") ]
#
#write.table(db.match[1:1000000, ], "OUTPUT IOTC Spatialized Catch 1 of 2.csv", sep=",", row.names=F, quote=F)
#write.table(db.match[1000001:dim(db.match)[1], ], "OUTPUT IOTC Spatialized Catch 2 of 2.csv", sep=",", row.names=F, quote=F)
#
##Aggregate catch by MatchID and Year
#agc= aggregate(db.match$Catch, by= list(db.match$Year, db.match$MatchID),sum)
#colnames(agc)= c("Year","MatchID","Catch")
#write.table(agc, "OUTPUT IOTC Catch by Year and MatchID.csv", sep=",",row.names=F, quote=F)#