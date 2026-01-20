#
#====WCPFC
#
#--------------------------------------------------Nominal data--------------------------------------------------
rm(list=ls())
library(reshape)

#Read data
wcpfc.nom= read.csv("INPUT WCPFC Nominal Catch For Formatting.csv", sep=",")
wcpfc.spp= read.table("input_codes/INPUT WCPFC Nominal Species Codes.txt", header=T)
wcpfc.gea= read.table("input_codes/INPUT WCPFC Nominal Gear Codes.txt", header=T)
wcpfc.cou= read.table("input_codes/INPUT WCPFC Nominal Country Codes.txt", header=T)

#Match country and gear codes
wcpfc.N= merge(wcpfc.nom, wcpfc.gea, by= c("Gear"), all.x=T)
wcpfc.N= merge(wcpfc.N, wcpfc.cou, by= c("Flag"), all.x=T)

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

wcpfc.N= wcpfc.N[ , -c(1:2,4) ]	#Delete first two columns with "Gear" and "Flag" (obsolete)

#Re-shape database into a dataframe
#wcpfc.N= melt( wcpfc.N, id= c("Year","Layer3GearID","GearGroupID","FishingEntityID","CountryGroupID") )
#colnames(wcpfc.N)[6:7]= c("SpeciesName","Catch")	#Re-name last two columns

#Match species codes
wcpfc.N= merge(wcpfc.N, wcpfc.spp, by= c("SpeciesName"), all.x=T)
wcpfc.N= wcpfc.N[ , c("Year","Layer3GearID","GearGroupID","FishingEntityID","CountryGroupID","TaxonKey","SpeciesGroupID","Catch") ]

#remove lines where catch = 0
wcpfc.N <- wcpfc.N[wcpfc.N$Catch > 0,]

#Export formatted nominal catch data file for matching
write.table(wcpfc.N[order(wcpfc.N$Year),], "Formatted WCPFC Nominal Catch For Spatial Matching.csv", sep=",", row.names=F)

#--------------------------------------------------Spatial data--------------------------------------------------

rm(list=ls())
library(reshape)

#Read cell code data
wcpfc.cel= read.table("input_codes/INPUT WCPFC Spatial Cell Codes.txt", header=T)
cellid= read.csv("input_codes/INPUT CellTypeID.csv", sep=",")

#Read spatial catch data
#wcpfc.ll60= read.csv("INPUT WCPFC Spatial Longline 60 For Formatting.csv", sep=",")
#wcpfc.ll70= read.csv("INPUT WCPFC Spatial Longline 70 For Formatting.csv", sep=",")
#wcpfc.ll80= read.csv("INPUT WCPFC Spatial Longline 80 For Formatting.csv", sep=",")
#wcpfc.ll90= read.csv("INPUT WCPFC Spatial Longline 90 For Formatting.csv", sep=",")
#wcpfc.ll00= read.csv("INPUT WCPFC Spatial Longline 00 For Formatting.csv", sep=",")
wcpfc.ll= read.csv("INPUT WCPFC Spatial Longline For Formatting.csv", sep=",")

wcpfc.ps= read.csv("INPUT WCPFC Spatial Purse Seine For Formatting.csv", sep=",")
wcpfc.pl= read.csv("INPUT WCPFC Spatial Pole And Line For Formatting.csv", sep=",")
wcpfc.dn= read.csv("INPUT WCPFC Spatial Driftnet For Formatting.csv", sep=",")

wcpfc.llspp= read.table("input_codes/INPUT WCPFC Spatial Longline Species Codes.txt", header=T)
wcpfc.psspp= read.table("input_codes/INPUT WCPFC Spatial Purse Seine Species Codes.txt", header=T)
wcpfc.plspp= read.table("input_codes/INPUT WCPFC Spatial Pole And Line Species Codes.txt", header=T)
wcpfc.dnspp= read.table("input_codes/INPUT WCPFC Spatial Driftnet Species Codes.txt", header=T)

#Longline data
#Re-shape database, aggregate by year, and match species codes
spp.match= function(x= wcpfc.ll00, y= wcpfc.llspp)
{
  z= melt(x, id= c("Year","Month","Lat","Lon") )

  names(z)[names(z)=="variable"] = "SpeciesName"
  names(z)[names(z)=="value"] = "Catch"
  
  z= z[ z$Catch!=0, ]	#Only keep entries where there is catch (i.e. catch does not equal zero)
  z= merge(z, y, by= c("SpeciesName"), all.x=T)	#Match species codes
  z= z[ , -c(1) ]	#Delete "SpeciesName" column (obsolete)
  z= aggregate(z$Catch, by= list(z$Year,z$Lat,z$Lon,z$TaxonKey,z$SpeciesGroupID), sum)
  colnames(z)= c("Year","Lat","Lon","TaxonKey","SpeciesGroupID","Catch")
  
  return(z=z)
}

#wcpfc.ll00= spp.match(x= wcpfc.ll00, y= wcpfc.llspp)
#wcpfc.ll90= spp.match(x= wcpfc.ll90, y= wcpfc.llspp)
#wcpfc.ll80= spp.match(x= wcpfc.ll80, y= wcpfc.llspp)
#wcpfc.ll70= spp.match(x= wcpfc.ll70, y= wcpfc.llspp)
#wcpfc.ll60= spp.match(x= wcpfc.ll60, y= wcpfc.llspp)
#wcpfc.ll= rbind(wcpfc.ll60, wcpfc.ll70, wcpfc.ll80, wcpfc.ll90, wcpfc.ll00)

#Add GearGroupID
wcpfc.ll= spp.match(x= wcpfc.ll, y= wcpfc.llspp)
#wcpfc.ll$GearGroupID= 32	#Specify that this is Longline catch (i.e. gear group i.d. = 32)
wcpfc.ll$GearGroupID= 302
wcpfc.ll= wcpfc.ll[ , c("Year","GearGroupID","TaxonKey","SpeciesGroupID","Lat","Lon","Catch") ]

#Purse seine data
wcpfc.ps= spp.match(x= wcpfc.ps, y= wcpfc.psspp)
#wcpfc.ps$GearGroupID= 41	#Specify that this is Purse seine catch (i.e. gear group i.d. = 41)
wcpfc.ps$GearGroupID= 401
wcpfc.ps= wcpfc.ps[ , c("Year","GearGroupID","TaxonKey","SpeciesGroupID","Lat","Lon","Catch") ]

#Pole and line data
wcpfc.pl= spp.match(x= wcpfc.pl, y= wcpfc.plspp)
#wcpfc.pl$GearGroupID= 31	#Specify that this is Pole and line catch (i.e. gear group i.d. = 31)
wcpfc.pl$GearGroupID= 301
wcpfc.pl= wcpfc.pl[ , c("Year","GearGroupID","TaxonKey","SpeciesGroupID","Lat","Lon","Catch") ]

#Driftnet Data, assume DN is a type of GN, use Layer3GearID 10, GearGroupID 50
wcpfc.dn= spp.match(x= wcpfc.dn, y= wcpfc.dnspp)
#wcpfc.dn$GearGroupID= 50	#Specify that this is driftnet catch (i.e. gear group i.d. = 50, "Gillnet")
wcpfc.dn$GearGroupID= 500

#Join WCPFC spatial datasets
wcpfc.S= rbind(wcpfc.ll, wcpfc.ps, wcpfc.pl, wcpfc.dn )

#Replace Lat-Lon with BigCellID
wcpfc.S= merge(wcpfc.S, wcpfc.cel, by= c("Lat","Lon"), all.x=T)
wcpfc.S= merge(wcpfc.S, cellid, by= c("x","y","BigCellTypeID"), all.x=T)

#write table for nomatch BCIDS
wcpfc.NM <- wcpfc.S[is.na(wcpfc.S$BigCellID ==T),]
write.csv(wcpfc.NM, "OUTPUT WCPFC No BigCellID Match.csv", row.names = F)

wcpfc.S= wcpfc.S[ , c("Year","GearGroupID","TaxonKey","SpeciesGroupID","BigCellID","Catch") ]
wcpfc.S <- wcpfc.S[is.na(wcpfc.S$BigCellID) == F,]

#some no matches are misreported, WCPFC reports in intervals of 5degrees and these are not 
#I have rounded to the nearest 5 to correct
#wcpfc.NM <- read.csv("OUTPUT WCPFC No BigCellID Match Corrected.csv", header=T)
#wcpfc.NM <- wcpfc.NM[,-c(1:3,11)]
##rematch
#wcpfc.NM= merge(wcpfc.NM, wcpfc.cel, by= c("Lat","Lon"), all.x=T)
#wcpfc.NM= merge(wcpfc.NM, cellid, by= c("x","y","BigCellTypeID"), all.x=T)
##remaining no match is reported on land, so exclude it
#wcpfc.NM <- wcpfc.NM[is.na(wcpfc.NM$BigCellID) == F,]
#wcpfc.NM <- wcpfc.NM[,-c(1:5)]

#join the newly matched
#wcpfc.S <- rbind(wcpfc.S,wcpfc.NM)

#Export formatted spatial catch data file for matching
write.table(wcpfc.S[order(wcpfc.S$Year),], "Formatted WCPFC Spatial Catch For Spatial Matching.csv", sep=",", row.names=F)

#=============================================================================================================
#==SECTION I: INITIAL DATA AND TRANSFORMATION
#=============================================================================================================
rm(list=ls())
library(reshape)

#1.Read in databases of nominal and spatialized catch by ocean

nom=read.csv("Formatted WCPFC Nominal Catch For Spatial Matching.csv",sep=",",header=T)	#Yearly nominal catch
spat=read.csv("Formatted WCPFC Spatial Catch For Spatial Matching.csv", sep=",",header=T) #Yearly spatial data

nom= nom[nom$Catch!=0,] #Exclude nominal records with zero catch
#nom= nom[nom$Catch>1,] #Exclude nominal catch records with less than 1 tonne of catch 

spat= spat[spat$Catch!=0,]
spat= spat[is.na(spat$BigCellID)==F,]

gearres= read.csv("input_codes/INPUT WCPFC GearRestrictionTable.csv",sep=",")
areares= read.csv("input_codes/INPUT WCPFC AreaRestrictionTable.csv",sep=",")

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
match.categ= list(spat$Year, spat$GearGroupID, spat$TaxonKey, spat$SpeciesGroupID)
merge.categ= c("Year","GearGroupID","TaxonKey","SpeciesGroupID")
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

#fids= sort(unique(nom$FishingEntityID)) #Vector of unique FishingEntityIDs in the nominal data
fids= sort(unique(db.nomatch$FishingEntityID)) #Vector of unique FishingEntityIDs in the nominal data
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
        print(names(NM))
        print(names(db.match))
        print(dim(NM))
        print(dim(db.match))
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

#=========================Writing===========================================#

db.match= db.match[order(db.match$Year),]

db.match= aggregate(db.match$Catch, by= list(db.match$Year,db.match$FishingEntityID,db.match$Layer3GearID,db.match$TaxonKey,db.match$BigCellID,db.match$MatchID), sum)
colnames(db.match)= c("Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","MatchID","Catch")

db.match$RFMOID= 18  #RFMO i.d. for WCPFC = 18
db.match= db.match[ order(db.match$Year), c("RFMOID","Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","Catch","MatchID") ]

#write.table(db.match[1:1000000,], "OUTPUT WCPFC Spatialized Catch 1 of 2.csv", sep=",", row.names=F, quote=F)	
#write.table(db.match[1000001:dim(db.match),], "OUTPUT WCPFC Spatialized Catch 2 of 2.csv", sep=",", row.names=F, quote=F)

chunk_size <- 1000000
n <- nrow(db.match)
num_chunks <- ceiling(n / chunk_size)

for (i in 1:num_chunks) {
  start_row <- ((i - 1) * chunk_size) + 1
  end_row <- min(i * chunk_size, n)
  chunk <- db.match[start_row:end_row, ]
  filename <- paste0("Final WCPFC Spatialized Catch with MatchID ",i, " of ", num_chunks,".csv")
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
write.table(agc, "OUTPUT WCPFC Catch by Year and MatchID.csv", sep=",",row.names=F, quote=F)

#===============================No Longer necessary code is below========================
#fids= sort(unique(nom$FishingEntityID)) #Vector of unique FishingEntityIDs in the nominal data
#YRs= c(10,20,35) #Year ranges for subsetting (i.e. +/- years to match over) TC Removed 1, 10, 15 as these are saying that categories are more important than time. This will bias results towards future tuna fisheries rather than spatial expansion. 
#
#for(i in 1:length(fids)) #Loop over countries
#  
#{
#  ci= fids[i] #Test by changing index to desired FishingEntityID number
#  
#  nom.datai= subset(db.nomatch, FishingEntityID==ci) #Nominal data to match for a country
#  
#  gears= subset(gearres, FishingEntityID==ci)[,c("FishingEntityID","GearGroupID")] #Fishing gears used by that country
#  if(dim(gears)[1] == 0) {gears= cbind("FishingEntityID"=ci, "GearGroupID"=unique(spat$Layer3GearID))} #If no restriction, use all gears
#  y= merge(spat, gears, by="GearGroupID",all.x=T) #Match country gears to spatial data
#  y=spat
#  #Commented out above as no fishing entities in spatial data and thus gear restrictions can't be used. 
#  
#  cells= subset(areares, FishingEntityID==ci)[,c("FishingEntityID","BigCellID")] #Cells used by that country
#  if(dim(cells)[1] == 0) {cells= cbind("FishingEntityID"=ci, "BigCellID"=unique(spat$BigCellID))} #If no restriction, use all cells
#  y2= merge(y, cells, by="BigCellID",all.x=T)#Match country cells to spatial data
#  
#  spati= spat #Only use spatial data that matched to both the allowed country gears and cells
#  #spati= spati[,-c(11,12)]; colnames(spati)[4]= "FishingEntityID"#Keep necessary data and update column name UPDATE check this for errors
#  
#  #rm(list=c("y","y2","gears","cells")) #Remove interim datasets
#  
#  yrs= sort(unique(nom.datai$Year)) #Unique years in the country nominal data
#  
#  NM= nom.datai #Non-matched data that will be used for the country. This should be the only time this nom.datai gets used each loop
#  
#  rni= dim(nom.datai)[1] #Rows in original country non-matched data
#  
#  for(z in 6:10) #Five is the number of category combos in the ifelse statment below
#  {
#    
#    for(j in 1:length(yrs)) #Loop over years
#    {
#      yrj=yrs[j] #Year j is pulled from the vector of year for that country
#      
#      for(w in 1:length(YRs)) #Loop over year ranges
#      {
#        YR=YRs[w] #Year range is pulled from the year ranges (+/- 0 to 20 year range)
#        
#        print("FishingEntityID"); print(i)
#        
#        print("Category"); print(z) #Current category for troubleshooting
#        
#        print("Year"); print(yrj) #Current year for troubleshooting
#        
#        print("Year range"); print(YR) #Current year range for troubleshooting
#        
#        nom.dataY= subset(NM, Year==yrj) #Only match data for a given year
#        spatY= subset(spati, Year<=yrj+YR & Year>=yrj-YR ) #Only use data for a given year range
#        
#        #print("Years spatial");print(sort(unique(spatY$Year)))  #Current years in range for troubleshooting
#        
#        #Depending on the step z, match by decreasing number of categories (this must be updated manually)
#        if(z==6){   match.categ= list(spatY$GearGroupID, spatY$TaxonKey, spatY$SpeciesGroupID) 
#        merge.categ= c("GearGroupID","TaxonKey","SpeciesGroupID")
#        
#        } else if(z==7){   match.categ= list(spatY$GearGroupID, spatY$SpeciesGroupID) 
#        merge.categ= c("GearGroupID","SpeciesGroupID")
#        
#        } else if(z==8){   match.categ= list(spatY$GearGroupID) 
#        merge.categ= c("GearGroupID")
#        
#        } else if(z==9){   match.categ= list(spatY$TaxonKey, spatY$SpeciesGroupID) 
#        merge.categ= c("TaxonKey","SpeciesGroupID")
#        
#        } else if(z==10){   match.categ= list(spatY$SpeciesGroupID) 
#        merge.categ= c("SpeciesGroupID") }
#        
#        
#        match.categ.names= c(merge.categ,"TotalCatch") #This gets done after merge.categ if statements
#        matchid= z+1 #Add 1 because the first match (outside the loop) is ID 1
#        
#        #Run the data.match function using the current spatial category data and years. If spatial data or nominal data do not exist for whatever reason 
#        #(no data in those years, nominal data is already matched), do nothing and keep going
#        if( dim(spatY)[1]>0 & dim(nom.dataY)[1]>0 ){  db.match= data.match(nom.dataY,spatY, db.match, match.categ, merge.categ, match.categ.names, matchid)$db.match }
#        
#        #After each loop, match original non-matched data to matched data and only keep what hasn't been matched yet
#        NM= nom.datai
#        NM= subset( merge(NM, cbind("NID"=sort(unique(db.match$NID)), "Match"=1), by="NID",all.x=T), is.na(Match) )[,-c(11)] 
#        
#        nni= dim(NM)[1] #Rows in remaining non-matched data for country
#        print("% Matched"); print(round((1-nni/rni) * 100, 1))
#        
#      } #Close year range loop
#      
#    } #Close years loop
#    
#  } #Close category loop
#  
#  #Aggregate potential duplicate category values to reduce size of matched database
#  db.match= aggregate(db.match$Catch, by= list(db.match$NID,db.match$Year,db.match$FishingEntityID,db.match$Layer3GearID,db.match$TaxonKey,db.match$BigCellID,db.match$MatchID), sum)
#  colnames(db.match)= c("NID","Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","MatchID","Catch")
#  
#}  #Close country loop 
#
##=============================================================================================================
##==SECTION I: INITIAL DATA AND TRANSFORMATION
##=============================================================================================================
#
#rm(list=ls())
#
##1.Read in databases of nominal and spatialized catch by ocean
#
##Nominal data represents: The "INPUT ... " files do not include years prior to 1950 or entries where catch is zero. 
#nom=read.csv("INPUT WCPFC Nominal Catch For Spatial Matching.csv",sep=",",header=T)	#Yearly nominal catch
#nom= nom[nom$Catch!=0,]
#
#spat= read.csv("INPUT WCPFC Spatial Catch For Spatial Matching.csv",sep=",",header=T)
#spat= spat[spat$Catch!=0,]
#spat= spat[is.na(spat$BigCellID)==F,]
#
##Allocate catch out from "All countries" fishing entity (FishingEntityID= 1000). 
##x= nom[nom$FishingEntityID==1000,]
##y= nom[nom$FishingEntityID!=1000,]
##y2= aggregate(y$Catch, by= list(y$Year, y$Layer3GearID, y$TaxonKey), sum)
##colnames(y2)= c("Year","Layer3GearID","TaxonKey","TotalCatch")
##y3= merge(y, y2, by= c("Year","Layer3GearID","TaxonKey"), all.x=T)
##y3$Proportion= y3$Catch / y3$TotalCatch
##y3= y3[,c("Year","Layer3GearID","GearGroupID","FishingEntityID","TaxonKey","SpeciesGroupID","Proportion")]
##z= merge(x, y3, by= c("Year","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID"), all.x=T)
##z$PropCatch= z$Catch * z$Proportion
##z= z[,c("Year","Layer3GearID","GearGroupID","FishingEntityID.y","TaxonKey","SpeciesGroupID","PropCatch")]
##colnames(z)= c("Year","Layer3GearID","GearGroupID","FishingEntityID","TaxonKey","SpeciesGroupID","Catch")
##z2= rbind(y,z)
##z3= aggregate(z2$Catch, by= list(z2$Year, z2$Layer3GearID, z2$GearGroupID, z2$FishingEntityID, z2$TaxonKey, z2$SpeciesGroupID), sum)
##colnames(z3)= colnames(nom)
##nom= z3
#
#tot.nom.ct=sum(nom$Catch)	#Total catch in nominal database; this is used to compare catches after spatializing.
#tot.spat.ct= sum(spat$Catch)	#Total catch in spatial database; this is not used in the routine but is available to look at.
#
#final.categ= c("Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","SpatCatch")
#final.categ.names= c("Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","Catch")
#
##=============================================================================================================
##==SECTION II: MATCHING BY: Year/TaxonKey/GearGroupID/SpeciesGroupID
##=============================================================================================================
#
#nom.data= nom
#match.categ= list(spat$Year,spat$TaxonKey,spat$GearGroupID,spat$SpeciesGroupID)
#merge.categ= c("Year","TaxonKey","GearGroupID","SpeciesGroupID")
#match.categ.names= c(merge.categ,"TotalCatch")
#
#
##1.Sum up the total catch, by all categories, in all reported spatial cells.
#ct.all= aggregate(spat$Catch,by= match.categ,sum)	#Total catch in all cells, all categories
#colnames(ct.all)= match.categ.names #Add matching column names
#
##2.Transform reported catch per cell into proportions of the total for all cells, by all categories.
#spat.all= merge(spat,ct.all,by= merge.categ,all.x=T)	#Spatialized catch total in all cells by Year/GearGroupID/TaxonKey
#spat.all$Proportion= spat.all$Catch / spat.all$TotalCatch	#Proportion of catch per cell = catch / total catch in cells reported for that category combo
#spat.all= spat.all[,c(merge.categ,"BigCellID","Proportion")]	#Remove the catch per cell in this dataframe (but leave the proportions)
#
##3.Match nominal and cell proportion catch by all categories.
#new.db= merge(nom.data,spat.all,by= merge.categ,all.x=T)#Match and merge the spatialized (with proportions) and 
#nominal catch databases into a new database (db)
#new.db$SpatCatch= new.db$Catch * new.db$Proportion	#New spatialized catch = nominal catch * spatial proportions
#
##4.Split the new database into matched and non-matched databases (makes computation faster)
#db.match= subset(new.db,is.na(BigCellID)==F)	#Separate the new database into matched (d for "data")
#db.nomatch= subset(new.db,is.na(BigCellID))	#and non-matched (nd for "no data") records
#
##5.Check that all catch is accounted for (whether matched or not) and return proportion of catch that was not matched at this stage
#print(c("Current refinement",sum(c(db.match$SpatCatch,db.nomatch$Catch)),"Nominal",tot.nom.ct))
#print(c("Proportion Matched Tonnes", 1-( sum(db.nomatch$Catch)/tot.nom.ct) ) )
#
##6.Reset columns so that they match the original nominal and spatial databases (to avoid potential indexing screw-ups) 
#db.match= db.match[,final.categ]	#Re-order columns for matched database
#colnames(db.match)= final.categ.names
#db.nomatch= db.nomatch[,colnames(nom)]	#Reset columns in non-matched database
#rm(list=c("ct.all","spat.all","new.db","nom.data"))	#MEMORY CLEAN-UP
#
##=============================================================================================================
##==SECTION III: MATCHING BY: Year/GearGroupID/SpeciesGroupID
##=============================================================================================================
#
#nom.data= db.nomatch
#match.categ= list(spat$Year,spat$GearGroupID,spat$SpeciesGroupID)
#merge.categ= c("Year","GearGroupID","SpeciesGroupID")
#match.categ.names= c(merge.categ,"TotalCatch")
#
#data.match= function( nom.data, db.match, match.categ, merge.categ, match.categ.names )
#{
#	ct.all= aggregate(spat$Catch,by= match.categ,sum)
#	colnames(ct.all)= match.categ.names 
#	spat.all= merge(spat,ct.all,by= merge.categ,all.x=T)	
#	spat.all$Proportion= spat.all$Catch / spat.all$TotalCatch	
#	spat.all= spat.all[,c(merge.categ,"BigCellID","Proportion")]	
#	new.db= merge(nom.data,spat.all,by= merge.categ,all.x=T)
#	new.db$SpatCatch= new.db$Catch * new.db$Proportion	
#	db.match.new= subset(new.db,is.na(BigCellID)==F)	
#	db.nomatch= subset(new.db,is.na(BigCellID))	
#	print(c("Current refinement",sum(c(db.match.new$SpatCatch,db.nomatch$Catch)),"Nominal",sum(nom.data$Catch)))
#	print(c("Proportion Matched Tonnes",1-( sum(db.nomatch$Catch)/tot.nom.ct) ))
#	db.match.new= db.match.new[,final.categ]	
#	colnames(db.match.new)= final.categ.names
#	db.match= rbind(db.match, db.match.new)
#	db.nomatch= db.nomatch[,colnames(nom)]	
#	rm(list=c("ct.all","spat.all","new.db","nom.data","db.match.new")) 
#
#	return(list(db.nomatch=db.nomatch, db.match=db.match))
#}
#
#db.nomatch= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names)$db.nomatch
#db.match= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names)$db.match
#
##=============================================================================================================
##==SECTION IV: MATCHING BY: Year/GearGroupID
##=============================================================================================================
#nom.data= db.nomatch
#match.categ= list(spat$Year,spat$GearGroupID)
#merge.categ= c("Year","GearGroupID")
#match.categ.names= c(merge.categ,"TotalCatch")
#
#db.nomatch= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names)$db.nomatch
#db.match= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names)$db.match
#
##=============================================================================================================
##==SECTION V: MATCHING BY: Year/TaxonKey/SpeciesGroupID
##=============================================================================================================
#
#nom.data= db.nomatch
#match.categ= list(spat$Year,spat$TaxonKey,spat$SpeciesGroupID)
#merge.categ= c("Year","TaxonKey","SpeciesGroupID")
#match.categ.names= c(merge.categ,"TotalCatch")
#
#db.nomatch= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names)$db.nomatch
#db.match= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names)$db.match
#
##=============================================================================================================
##==SECTION VI: MATCHING BY: TaxonKey/SpeciesGroupID/GearGroupID
##=============================================================================================================
#nom.data= db.nomatch
#match.categ= list(spat$TaxonKey, spat$SpeciesGroupID,spat$GearGroupID)
#merge.categ= c("TaxonKey","SpeciesGroupID","GearGroupID")
#match.categ.names= c(merge.categ,"TotalCatch")
#
#db.nomatch= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names)$db.nomatch
#db.match= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names)$db.match
#
##=============================================================================================================
##==SECTION VII: MATCHING BY: TaxonKey/SpeciesGroupID
##=============================================================================================================
#nom.data= db.nomatch
#match.categ= list(spat$TaxonKey, spat$SpeciesGroupID)
#merge.categ= c("TaxonKey","SpeciesGroupID")
#match.categ.names= c(merge.categ,"TotalCatch")
#
#db.nomatch= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names)$db.nomatch
#db.match= data.match(nom.data, db.match, match.categ, merge.categ, match.categ.names)$db.match
#
##=============================================================================================================
##==SECTION VIII: EXPORTING DATA
##=============================================================================================================
#
#stop("Matching routine finalized")
#
#db.match= db.match[order(db.match$Year),]
#db.match= aggregate(db.match$Catch, by= list(db.match$Year,db.match$FishingEntityID,db.match$Layer3GearID,db.match$TaxonKey,db.match$BigCellID), sum)
#colnames(db.match)= c("Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","Catch")
#
#db.match$RFMOID= 18  #RFMO i.d. for WCPFC = 18
#db.match= db.match[ order(db.match$Year), c("RFMOID","Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","Catch") ]
#
#write.table(db.match[1:1000000,], "OUTPUT WCPFC Spatialized Catch 1 of 2.csv", sep=",", row.names=F, quote=F)	
#write.table(db.match[1000001:1472321,], "OUTPUT WCPFC Spatialized Catch 2 of 2.csv", sep=",", row.names=F, quote=F)	 
#
##Check the dimensions of the final database and change up the divisions below (numbers in square brackets indicate row numbers)
#
##write.table(db.match[1:1000000,], "OUTPUT WCPFC Spatialized Catch 1 of 3.csv", sep=",", row.names=F, quote=F)	
##write.table(db.match[1000001:2000000,], "OUTPUT WCPFC Spatialized Catch 2 of 3.csv", sep=",", row.names=F, quote=F)	 
##write.table(db.match[2000001:2882433,], "OUTPUT WCPFC Spatialized Catch 3 of 3.csv", sep=",", row.names=F, quote=F)
