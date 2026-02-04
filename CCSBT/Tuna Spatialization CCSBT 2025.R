#
#====CCSBT
#
#--------------------------------------------------Nominal data--------------------------------------------------
rm(list=ls())

#Read-in data
ccsbt.n= read.csv("INPUT CCSBT Nominal Catch For Formatting.csv", sep=",")
ccsbt.s= read.csv("INPUT CCSBT Spatial Catch For Formatting.csv", sep=",")

ccsbt.cou= read.table("input_codes/INPUT CCSBT Nominal Country Codes.txt", header=T, sep="\t")
ccsbt.oce= read.table("input_codes/INPUT CCSBT Nominal Ocean Codes.txt", header=T, sep="\t")
ccsbt.soce= read.table("input_codes/INPUT CCSBT Spatial Ocean Codes.txt", header=T, sep="\t")
ccsbt.cel= read.table("input_codes/INPUT CCSBT Spatial Cell Codes.txt", header=T, sep="\t")
ccsbt.gea= read.table("input_codes/INPUT CCSBT Gear Codes.txt", header=T, sep="\t")
cellid= read.csv("input_codes/INPUT CellTypeID.csv", sep=",")

#Exclude entries with no catch or, for spatial data, month or location data
ccsbt.n= ccsbt.n[ccsbt.n$Catch!=0,]
#ccsbt.s= ccsbt.s[is.na(ccsbt.s$Month)==F,] #Month was already removed
ccsbt.s= ccsbt.s[ccsbt.s$Catch!=0,]
ccsbt.s= ccsbt.s[is.na(ccsbt.s$Lat)==F,]

#Match codes in nominal data
ccsbt.N= merge(ccsbt.n, ccsbt.cou, by= c("CountryName"), all.x=T)
ccsbt.N= merge(ccsbt.N, ccsbt.gea, by= c("GearName"), all.x=T)
ccsbt.N= merge(ccsbt.N, ccsbt.oce, by= c("OceanName"), all.x=T)

ccsbt.N= ccsbt.N[ , c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","OceanID","Catch") ]

#Exclude 'research' catch (fishing entity id=2000) and split 'all countries' catch (fishing entity ID=1000)
ccsbt.N= ccsbt.N[ ccsbt.N$FishingEntityID!=2000, ]

#Allocate catch out from "All countries" fishing entity (FishingEntityID= 1000). 
x= ccsbt.N[ccsbt.N$FishingEntityID==1000,]
y= ccsbt.N[ccsbt.N$FishingEntityID!=1000,]

y2= aggregate(y$Catch, by= list(y$Year, y$Layer3GearID, y$OceanID), sum)
colnames(y2)= c("Year","Layer3GearID","OceanID","TotalCatch")

y3= merge(y, y2, by= c("Year","Layer3GearID","OceanID"), all.x=T)
y3$Proportion= y3$Catch / y3$TotalCatch
y3= y3[,c("Year","Layer3GearID","GearGroupID","FishingEntityID","CountryGroupID","OceanID","Proportion")]

z= merge(x, y3, by= c("Year","Layer3GearID","GearGroupID","OceanID"), all.x=T)
z$PropCatch= z$Catch * z$Proportion
z= z[,c("Year","FishingEntityID.y","CountryGroupID.y","Layer3GearID","GearGroupID","OceanID","PropCatch")]
colnames(z)= c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","OceanID","Catch")

z2= rbind(y,z)

z3= aggregate(z2$Catch, by= list(z2$Year, z2$FishingEntityID, z2$CountryGroupID, z2$Layer3GearID, z2$GearGroupID, z2$OceanID), sum)
colnames(z3)= colnames(y)

ccsbt.N= z3
ccsbt.N$TaxonKey= 600145	#Add southern bluefin tuna TaxonKey
ccsbt.N= ccsbt.N[, c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","OceanID","TaxonKey","Catch") ]

write.table(ccsbt.N, "Formatted CCSBT Nominal Catch For Spatial Matching.csv", sep=",", row.names=F)

#Spatial data

#Match codes
ccsbt.S= merge(ccsbt.s, ccsbt.soce, by= c("OceanName"), all.x=T)
ccsbt.S= merge(ccsbt.S, ccsbt.gea, by= c("GearName"), all.x=T)
ccsbt.S= merge(ccsbt.S, ccsbt.cel, by= c("Lat","Lon"), all.x=T)
ccsbt.S= merge(ccsbt.S, cellid, by= c("x","y","BigCellTypeID"), all.x=T)

ccsbt.S= ccsbt.S[ , c("Year","Layer3GearID","GearGroupID","OceanID","BigCellID","Catch") ]

#Monthly data into yearly spatial data
ccsbt.S= aggregate(ccsbt.S$Catch, by=list(ccsbt.S$Year, ccsbt.S$Layer3GearID,ccsbt.S$GearGroupID,ccsbt.S$OceanID,ccsbt.S$BigCellID) , sum)
colnames(ccsbt.S)= c("Year","Layer3GearID","GearGroupID","OceanID","BigCellID","Catch")

ccsbt.S$TaxonKey= 600145	#Add southern bluefin tuna TaxonKey
ccsbt.S= ccsbt.S[, c("Year","Layer3GearID","GearGroupID","OceanID","BigCellID","TaxonKey","Catch") ]

write.table(ccsbt.S, "Formatted CCSBT Spatial Catch For Spatial Matching.csv", sep=",", row.names=F)

#=============================================================================================================
#==SECTION I: INITIAL DATA AND TRANSFORMATION
#=============================================================================================================
rm(list=ls())

#1.Read in databases of nominal and spatialized catch by ocean
nom=read.csv("Formatted CCSBT Nominal Catch For Spatial Matching.csv",sep=",",header=T)	#Yearly nominal catch
spat=read.csv("Formatted CCSBT Spatial Catch For Spatial Matching.csv",sep=",",header=T)	#Yearly available spatial catch

nom= nom[nom$Catch!=0,]
#nom= nom[nom$Catch>1,] #Remove rows less than 1 tonne?

spat= spat[spat$Catch!=0,]
spat= spat[is.na(spat$BigCellID)==F,]

gearres= read.csv("input_codes/INPUT CCSBT GearRestrictionTable.csv",sep=",") #Read in gear restrictions
areares= read.csv("input_codes/INPUT CCSBT AreaRestrictionTable.csv",sep=",") #Read in area restrictions

tot.nom.ct=sum(nom$Catch)	#Total catch in nominal database; this is used to compare catches after spatializing.
tot.spat.ct= sum(spat$Catch)	#Total catch in spatial database; this is not used in the routine but is available to look at.

final.categ= c("Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","SpatCatch")
final.categ.names= c("Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","Catch")

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
match.categ= list(spat$Year, spat$OceanID, spat$Layer3GearID, spat$GearGroupID, spat$TaxonKey)
merge.categ= c("Year","OceanID","Layer3GearID","GearGroupID","TaxonKey")
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

data.match= function(nom.dataY, spat, db.match, match.categ, merge.categ, match.categ.names, matchid )
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

#db.nomatch = nom #Re-set db.nomatch
#Make db.nomatch equal to all nom catch except those already matched as indicated by NIDs:
#db.nomatch = subset( merge(db.nomatch, cbind("NID"=sort(unique(db.match$NID)), "Match"=1), by="NID",all.x=T), is.na(Match) )[,-c(11)]
#Re-run matching sequence above with expanded year range on only unmatched nom catch
#db.nomatch$Match=1

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
  
  for(z in 1:3) #Three is the number of category combos in the ifelse statment below
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
        if(z==1){   match.categ= list(spatY$OceanID, spatY$Layer3GearID, spatY$TaxonKey) 
        merge.categ= c("OceanID","Layer3GearID","TaxonKey")
        
        } else if(z==2){   match.categ= list(spatY$OceanID, spatY$GearGroupID, spatY$TaxonKey) 
        merge.categ= c("OceanID","GearGroupID","TaxonKey")
        
        } else if(z==3){   match.categ= list(spatY$OceanID, spatY$TaxonKey) 
        merge.categ= c("OceanID","TaxonKey")}
        
        match.categ.names= c(merge.categ,"TotalCatch") #This gets done after merge.categ if statements
        matchid= z+1 #Add 1 because the first match (outside the loop) is ID 1
        
        #Run the data.match function using the current spatial category data and years. If spatial data or nominal data do not exist for whatever reason 
        #(no data in those years, nominal data is already matched), do nothing and keep going
        if( dim(spatY)[1]>0 & dim(nom.dataY)[1]>0 ){  db.match= data.match(nom.dataY,spatY, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.match }
        
        NM= nom.datai #Non-matched data that will be used for the country. This should be the only time this nom.datai gets used each loop
        #Re-run this line so line below will properly subset NM. 
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
#First Round Done
#=============================================================================================================
#==SECTION IV: Re-Run with Larger Year Ranges
#=============================================================================================================

db.nomatch = nom #Re-set db.nomatch
#Make db.nomatch equal to all nom catch except those already matched as indicated by NIDs:
db.nomatch = subset( merge(db.nomatch, cbind("NID"=sort(unique(db.match$NID)), "Match"=1), by="NID",all.x=T), is.na(Match) )[,-c(11)]
#Re-run matching sequence above with expanded year range on only unmatched nom catch

db.nomatch= db.nomatch[,colnames(nom)]	#Reset columns in non-matched database

sum(db.nomatch$Catch)

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
  #Commented out this section and removed as there are no restrictions for CCSBT
  spati= subset(y2, FishingEntityID.y==ci & FishingEntityID.x==ci) #Only use spatial data that matched to both the allowed country gears and cells
  spati= spati[,-c(9)]; colnames(spati)[8]= "FishingEntityID"#Keep necessary data and update column name UPDATE check this for errors
  
  rm(list=c("y","y2","gears","cells")) #Remove interim datasets
  
  yrs= sort(unique(nom.datai$Year)) #Unique years in the country nominal data
  
  NM= nom.datai #Non-matched data that will be used for the country. This should be the only time this nom.datai gets used each loop
  
  rni= dim(nom.datai)[1] #Rows in original country non-matched data
  
  for(z in 4:6) #Three is the number of category combos in the ifelse statment below
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
        if(z==4){   match.categ= list(spatY$OceanID, spatY$Layer3GearID, spatY$TaxonKey) 
        merge.categ= c("OceanID","Layer3GearID","TaxonKey")
        
        } else if(z==5){   match.categ= list(spatY$OceanID, spatY$GearGroupID, spatY$TaxonKey) 
        merge.categ= c("OceanID","GearGroupID","TaxonKey")
        
        } else if(z==6){   match.categ= list(spatY$OceanID, spatY$TaxonKey) 
        merge.categ= c("OceanID","TaxonKey")}
        
        match.categ.names= c(merge.categ,"TotalCatch") #This gets done after merge.categ if statements
        matchid= z+1 #Add 1 because the first match (outside the loop) is ID 1
        
        #Run the data.match function using the current spatial category data and years. If spatial data or nominal data do not exist for whatever reason 
        #(no data in those years, nominal data is already matched), do nothing and keep going
        if( dim(spatY)[1]>0 & dim(nom.dataY)[1]>0 ){  db.match= data.match(nom.dataY,spatY, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.match }
        
        #NM= nom.datai #Non-matched data that will be used for the country. This should be the only time this nom.datai gets used each loop
        #Re-run this line so line below will properly subset NM. 
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
#=========================Testing===========================================#
db.nomatch = subset( merge(db.nomatch, cbind("NID"=sort(unique(db.match$NID)), "Match"=1), by="NID",all.x=T), is.na(Match) )[,-c(11)]
#Refine db.nomatch data 

db.nomatch= db.nomatch[,colnames(nom)]	#Reset columns in non-matched database
sum(db.nomatch$Catch) #Nomatch is equal to 0?
sum(db.match$Catch)/sum(nom$Catch) #Matched % is equal to 1?

###STOP###
#Aggregate potential duplicate category values to reduce size of matched database, drop NID column
db.match= aggregate(db.match$Catch, by= list(db.match$Year,db.match$FishingEntityID,db.match$Layer3GearID,db.match$TaxonKey,db.match$BigCellID,db.match$MatchID), sum)
colnames(db.match)= c("Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","MatchID","Catch")

#stop("SHOULD BE DONE...") #


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

#=======================================Writing===========================================

db.match= db.match[order(db.match$Year),]
db.match= aggregate(db.match$Catch, by= list(db.match$Year,db.match$FishingEntityID,db.match$Layer3GearID,db.match$TaxonKey,db.match$BigCellID, db.match$MatchID), sum)
colnames(db.match)= c("Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID", "MatchID", "Catch")

#EXPORT DATA
db.match$RFMOID= 3  #RFMO i.d. for CCSBT = 3
db.match= db.match[ , c("RFMOID","Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","MatchID", "Catch") ]
write.table(db.match[order(db.match$Year),], "Final CCSBT Spatialized Catch with MatchID 1 of 1.csv", sep=",", row.names=F, quote=F)


#================================Old code is below============================================================
#=============================================================================================================
#==SECTION II: MATCHING BY Year/Layer3GearID/GearGroupID/OceanID/TaxonKey
#=============================================================================================================
#nom.data= nom
#match.categ= list(spat$Year, spat$Layer3GearID, spat$GearGroupID, spat$TaxonKey, spat$OceanID)
#merge.categ= c("Year","Layer3GearID","GearGroupID","TaxonKey","OceanID")
#match.categ.names= c(merge.categ,"TotalCatch")
#
#
##1.Sum up the total catch, by all categories, in all reported spatial cells.
#
#ct.all= aggregate(spat$Catch,by= match.categ,sum)  #Total catch in all cells, all categories
#
#colnames(ct.all)= match.categ.names #Add matching column names
#
##2.Transform reported catch per cell into proportions of the total for all cells, by all categories.
#
#spat.all= merge(spat,ct.all,by= merge.categ, all.x=T)	#Spatialized catch total in all cells by Year/GearGroupID/TaxonKey
#
#spat.all$Proportion= spat.all$Catch / spat.all$TotalCatch	#Proportion of catch per cell = catch / total catch in cells reported for that category combo
#
#spat.all= spat.all[,c(merge.categ,"BigCellID","Proportion")]	#Remove the catch per cell in this dataframe (but leave the proportions)
#
##3.Match nominal and cell proportion catch by all categories.
#new.db= merge(nom.data,spat.all,by= merge.categ,all.x=T)#Match and merge the spatialized (with proportions) and 
##nominal catch databases into a new database (db)
#new.db$SpatCatch= new.db$Catch * new.db$Proportion	#New spatialized catch = nominal catch * spatial proportions
#
##4.Split the new database into matched and non-matched databases (makes computation faster)
#db.match= subset(new.db,is.na(BigCellID)==F)	#Separate the new database into matched (d for "data")
#
#db.nomatch= subset(new.db,is.na(BigCellID))	#and non-matched (nd for "no data") records
#
##5.Check that all catch is accounted for (whether matched or not) and return proportion of catch that was not matched at this stage
#print(c("Current refinement",sum(c(db.match$SpatCatch,db.nomatch$Catch)),"Nominal",tot.nom.ct))
#print(c("Proportion Matched Tonnes", 1-( sum(db.nomatch$Catch)/tot.nom.ct) ) )
#
##6.Reset columns so that they match the original nominal and spatial databases (to avoid potential indexing screw-ups) 
#db.match= db.match[,final.categ]	#Re-order columns for matched database
#colnames(db.match)= final.categ.names
#
#db.nomatch= db.nomatch[,colnames(nom)]	#Reset columns in non-matched database
#
#rm(list=c("ct.all","spat.all","new.db","nom.data"))	#MEMORY CLEAN-UP
#
##=============================================================================================================
##==SECTION III: MATCHING BY Year/GearGroupID/OceanID/TaxonKey
##=============================================================================================================
#
#nom.data= db.nomatch
#match.categ= list(spat$Year, spat$GearGroupID, spat$OceanID, spat$TaxonKey)
#merge.categ= c("Year","GearGroupID","OceanID","TaxonKey")
#match.categ.names= c(merge.categ,"TotalCatch")
#
#
#data.match= function( nom.data, db.match, match.categ, merge.categ, match.categ.names )
#{
#  
#  ct.all= aggregate(spat$Catch,by= match.categ,sum)
#  colnames(ct.all)= match.categ.names 
#  spat.all= merge(spat,ct.all,by= merge.categ,all.x=T)	
#  spat.all$Proportion= spat.all$Catch / spat.all$TotalCatch	
#  spat.all= spat.all[,c(merge.categ,"BigCellID","Proportion")]	
#  new.db= merge(nom.data,spat.all,by= merge.categ,all.x=T)
#  new.db$SpatCatch= new.db$Catch * new.db$Proportion	
#  db.match.new= subset(new.db,is.na(BigCellID)==F)	
#  db.nomatch= subset(new.db,is.na(BigCellID))	
#  print(c("Current refinement",sum(c(db.match.new$SpatCatch,db.nomatch$Catch)),"Nominal",sum(nom.data$Catch)))
#  print(c("Proportion Matched Tonnes",1-( sum(db.nomatch$Catch)/tot.nom.ct) ))
#  db.match.new= db.match.new[,final.categ]	
#  colnames(db.match.new)= final.categ.names
#  db.match= rbind(db.match, db.match.new)
#  db.nomatch= db.nomatch[,colnames(nom)]	
#  rm(list=c("ct.all","spat.all","new.db","nom.data","db.match.new")) 
#  
#  return(list(db.nomatch=db.nomatch, db.match=db.match))
#  
#}
#
#db.nomatch= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names)$db.nomatch
#db.match= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names)$db.match
#
#
##=============================================================================================================
##==SECTION IV: MATCHING BY Year/OceanID/TaxonKey
##=============================================================================================================
#
#nom.data= db.nomatch
#match.categ= list(spat$Year, spat$OceanID, spat$TaxonKey)
#merge.categ= c("Year","OceanID","TaxonKey")
#match.categ.names= c(merge.categ,"TotalCatch")
#
#db.nomatch= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names)$db.nomatch
#db.match= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names)$db.match
#
#
##=============================================================================================================
##==SECTION V: MATCHING BY Year/GearGroupID/TaxonKey
##=============================================================================================================
#
#nom.data= db.nomatch
#match.categ= list(spat$Year, spat$GearGroupID, spat$TaxonKey)
#merge.categ= c("Year", "GearGroupID","TaxonKey")
#match.categ.names= c(merge.categ,"TotalCatch")
#
#db.nomatch= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names)$db.nomatch
#db.match= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names)$db.match
#
#
##=============================================================================================================
##==SECTION VI: MATCHING BY Layer3GearID/GearGroupID/OceanID/TaxonKey
##=============================================================================================================
#
#nom.data= db.nomatch
#match.categ= list(spat$Layer3GearID, spat$GearGroupID, spat$TaxonKey, spat$OceanID)
#merge.categ= c("Layer3GearID","GearGroupID","TaxonKey","OceanID")
#match.categ.names= c(merge.categ,"TotalCatch")
#
#db.nomatch= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names)$db.nomatch
#db.match= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names)$db.match
#
##=============================================================================================================
##==SECTION VII: MATCHING BY GearGroupID/OceanID/TaxonKey
##=============================================================================================================
#
#nom.data= db.nomatch
#match.categ= list(spat$GearGroupID, spat$TaxonKey, spat$OceanID)
#merge.categ= c("GearGroupID","TaxonKey","OceanID")
#match.categ.names= c(merge.categ,"TotalCatch")
#
#db.nomatch= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names)$db.nomatch
#db.match= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names)$db.match
#
#
##=============================================================================================================
##==SECTION VIII: MATCHING BY TaxonKey/OceanID
##=============================================================================================================
#
#nom.data= db.nomatch
#match.categ= list(spat$TaxonKey, spat$OceanID)
#merge.categ= c("TaxonKey","OceanID")
#match.categ.names= c(merge.categ,"TotalCatch")
#
#db.nomatch= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names)$db.nomatch
#db.match= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names)$db.match
#
#
#
#db.match= db.match[order(db.match$Year),]
#
#db.match= aggregate(db.match$Catch, by= list(db.match$Year,db.match$FishingEntityID,db.match$Layer3GearID,db.match$TaxonKey,db.match$BigCellID), sum)
#colnames(db.match)= c("Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","Catch")
#
#
#
##EXPORT DATA
#
#db.match$RFMOID= 3  #RFMO i.d. for CCSBT = 3
#
#db.match= db.match[ , c("RFMOID","Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","Catch") ]
#
#write.table(db.match[order(db.match$Year),], "OUTPUT CCSBT Spatialized Catch 1 of 1.csv", sep=",", row.names=F, quote=F)
