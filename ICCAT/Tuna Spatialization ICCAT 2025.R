#
#====ICCAT
#
#--------------------------------------------------Nominal data--------------------------------------------------
rm(list=ls())
library(reshape)

#Read nominal data
iccat.n= read.csv("INPUT ICCAT Nominal Catch for Formatting.csv", sep=",")

iccat.n= iccat.n[iccat.n$Catch!=0 & is.na(iccat.n$Catch)==F & iccat.n$Year>=1950, ]

#Read nominal data codes
iccat.gea= read.table("input_codes/INPUT ICCAT Nominal Gear Codes.txt", header=T)
iccat.spp= read.table("input_codes/INPUT ICCAT Nominal Species Codes.txt", header=T)
iccat.cou= read.csv("input_codes/INPUT ICCAT Nominal Country Codes.csv", sep=",")

iccat.N= merge(iccat.n, iccat.cou, by= c("Flag"), all.x=T)
iccat.N= merge(iccat.N, iccat.gea, by= c("GearCode"), all.x=T)
iccat.N= merge(iccat.N, iccat.spp, by= c("SpeciesCode"), all.x=T)

iccat.N= iccat.N[ ,-c(1:3) ]

#Fishing entity splits

#France and Spain country split
x= iccat.N[iccat.N$FishingEntityID!=3000,]
fr= es= iccat.N[iccat.N$FishingEntityID==3000,]#France and Spain "Mixed Flag" catch

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

write.table(soviet.prop, "Formatted INPUT Soviet Catch Proportions 1991.txt",sep="\t")

est$FishingEntityID= 53; est$Catch= est$Catch * soviet.prop$Estonia
geo$FishingEntityID= 63; geo$Catch= geo$Catch * soviet.prop$Georgia
lat$FishingEntityID= 98; lat$Catch= lat$Catch * soviet.prop$Latvia
rus$FishingEntityID= 148; rus$Catch= rus$Catch * soviet.prop$RussianFed
ukr$FishingEntityID= 181; ukr$Catch= ukr$Catch * soviet.prop$Ukraine

soviets= rbind(est, geo, lat, rus, ukr)

iccat.N= rbind(x, soviets)

#Yugoslavia split, separate out
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

#Write table for spatial split
write.table(yugo.prop, "Formatted INPUT Yugoslavia Proportions 1991.txt", row.names = F, sep="\t")

#Make split tables for joint ventures
#FEID 3000 = France,Cote D'Ivoire and Senegal
#France = FEID 58
#Cote D'Ivoire = FEID 89
#Senegal = FEID 157

fis <- rbind(iccat.N[iccat.N$FishingEntity==58,],
             iccat.N[iccat.N$FishingEntity==89,],
             iccat.N[iccat.N$FishingEntity==157,])
# catch by countries in 1969-1990
fis6990 <- subset(fis, Year > 1968)
fis6990 <- subset(fis6990, Year < 1991)
#aggregate catch by year for total catch by year
tot <- aggregate(fis6990$Catch, by=list(fis6990$Year), sum)
colnames(tot) <- c("Year","TotalCatch")
#aggregate catch by year and FEID for FEID totals
fetot <- aggregate(fis6990$Catch, by=list(fis6990$Year, fis6990$FishingEntityID), sum)
colnames(fetot) <- c("Year","FishingEntityID","Catch")
fr <- subset(fetot, FishingEntityID==58)
ci <- subset(fetot, FishingEntityID==89)
se <- subset(fetot, FishingEntityID==157)

#Join country total catch together
fis <- rbind(fr,ci,se)
fis <- merge(fis,tot, by="Year")
#calculate proportion by year
fis$Prop <- fis$Catch / fis$TotalCatch
fisprop <- fis[,-c(3:4)]
write.csv(fisprop, "Formatted INPUT France, Cote D'Ivoire and Senegal Proportions.csv", row.names = F)

#FEID 5000 = Korea and Panama
#Korea = FEID 95
#Panama = FEID 135

kp <- rbind(iccat.N[iccat.N$FishingEntity==95,],
            iccat.N[iccat.N$FishingEntity==135,])
# catch by countries in 1974-1983
kp7483 <- subset(kp, Year > 1973)
kp7483 <- subset(kp7483, Year < 1984)
#aggregate catch by year for total catch by year
tot <- aggregate(kp7483$Catch, by=list(kp7483$Year), sum)
colnames(tot) <- c("Year","TotalCatch")
#aggregate catch by year and FEID for FEID totals
fetot <- aggregate(kp7483$Catch, by=list(kp7483$Year, kp7483$FishingEntityID), sum)
colnames(fetot) <- c("Year","FishingEntityID","Catch")
kr <- subset(fetot, FishingEntityID==95)
pa <- subset(fetot, FishingEntityID==135)
#Join country total catch together
fis <- rbind(pa,kr)
fis <- merge(fis,tot, by="Year")
#calculate proportion by year
fis$Prop <- fis$Catch / fis$TotalCatch
fisprop <- fis[,-c(3:4)]
write.csv(fisprop, "Formatted INPUT Korea Panama Proportions.csv", row.names = F)

#All countries split
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

#remove NA values
iccat.N <- iccat.N[is.na(iccat.N$Catch)==F,]

write.table(iccat.N, "Formatted ICCAT Nominal Catch For Spatial Matching.csv", sep=",", row.names=F)	#Export the new table to a csv file

#--------------------------------------------------Spatial data--------------------------------------------------

rm(list=ls())

library(reshape)
library(tidyverse)

#Read spatial catch data
iccat.s= read.csv("INPUT ICCAT Spatial Catch for Formatting.csv", sep=",")

iccat.s= iccat.s[iccat.s$CatchUnit=="kg", ]

#Read spatial data codes
iccat.spp= read.table("input_codes/INPUT ICCAT Spatial Species Codes.txt", header=T)
iccat.cel= read.csv("input_codes/INPUT ICCAT Spatial Cell Codes.csv", sep=",")
iccat.gea= read.table("input_codes/INPUT ICCAT Spatial Gear Codes.txt", header=T)
iccat.cou= read.csv("input_codes/INPUT ICCAT Spatial Country Codes.csv", sep=",")
cellid= read.csv("input_codes/INPUT CellTypeID.csv",sep=",",header=T)

#transform data into long form
iccat.S <- melt(iccat.s, id= c("FleetID","GearCode","Year","SquareTypeCode","QuadID","Lat","Lon","CatchUnit"))

# rename column names and remove rows with no spatial catch or have catch = 0
iccat.S <- iccat.S |> 
  rename(SpeciesCode = variable,
         Catch = value) |>
  filter(SquareTypeCode!= "none", Catch !=0)

# Sanity Check
og_catch <- sum(iccat.S$Catch)

#merge data with codes
iccat.S= merge(iccat.S, iccat.cou, by= c("FleetID"), all.x=T)
iccat.S= merge(iccat.S, iccat.gea, by= c("GearCode"), all.x=T)
iccat.S= merge(iccat.S, iccat.cel, by= c("SquareTypeCode","QuadID", "Lon","Lat"), all.x=T)
iccat.S <- merge(iccat.S, iccat.spp, by= c("SpeciesCode"), all.x=T)
iccat.S= merge(iccat.S, cellid, by= c("BigCellTypeID", "x","y"), all.x=T)

# Sanity check
post_catch <- sum(iccat.S$Catch)

#remove newly redundant columns, keep QuadID for all countries matching later
iccat.S <- iccat.S |> select(QuadID, Year, CatchUnit, Catch, CountryGroupID, 
                               FishingEntityID, Layer3GearID, GearGroupID,
                               TaxonKey, SpeciesGroupID, BigCellID)

#Identify the rows that don't match to a big cell and write a table to look at them
iccat.NoMatch <- iccat.S[(is.na(iccat.S$BigCellID)==T),]
write.csv(iccat.NoMatch, "OUTPUT ICCAT BigCellID No Match.csv", row.names = F)

#remove no matches, they are erroneously reported on land
iccat.S <- iccat.S[(is.na(iccat.S$BigCellID)==F),]

#Convert catch from kilograms to tonnes
iccat.S$Catch <- iccat.S$Catch / 1000 
#remove the catch unit column
iccat.S <- iccat.S[,-3]

#Aggregate catch
iccat.S <- aggregate(iccat.S$Catch, by=list(iccat.S$Year,iccat.S$FishingEntityID,iccat.S$CountryGroupID,iccat.S$Layer3GearID,iccat.S$GearGroupID,iccat.S$BigCellID,iccat.S$QuadID, iccat.S$TaxonKey,iccat.S$SpeciesGroupID), sum)
#rename columns 
colnames(iccat.S) <- c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","BigCellID","QuadID","TaxonKey","SpeciesGroupID","Catch")

#remove stored data
rm(cellid,iccat.cel,iccat.cou,iccat.gea,iccat.NoMatch,iccat.s,iccat.spp)

#Fishing entity splits

#All countries split
#Allocate catch out from "All countries" fishing entity (FishingEntityID= 1000). 
x <- iccat.S[iccat.S$FishingEntityID==1000,]
y <- iccat.S[iccat.S$FishingEntityID!=1000,]

#remove iccat.S for RAM space
rm(iccat.S)

#Matching by year, gear, bigcellid, and species
y2 <- aggregate(y$Catch, by= list(y$Year, y$TaxonKey, y$Layer3GearID, y$BigCellID), sum)
colnames(y2) <- c("Year","TaxonKey","Layer3GearID","BigCellID","TotalCatch")

y3 <- merge(y, y2, by= c("Year","TaxonKey","Layer3GearID","BigCellID"), all.x=T)
y3$Proportion <- y3$Catch / y3$TotalCatch
y3 <- y3[,c("Year","Layer3GearID","GearGroupID","FishingEntityID","CountryGroupID","TaxonKey","SpeciesGroupID","QuadID","BigCellID","Proportion")]

z <- merge(x, y3, by= c("Year","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","QuadID","BigCellID"), all.x=T)
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

y3 <- merge(y, y2, by= c("Year","TaxonKey","Layer3GearID","QuadID"), all.x=T)
y3$Proportion <- y3$Catch / y3$TotalCatch
y3 <- y3[,c("Year","Layer3GearID","GearGroupID","FishingEntityID","CountryGroupID","TaxonKey","SpeciesGroupID","QuadID","BigCellID","Proportion")]

z <- merge(znm1, y3, by= c("Year","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","QuadID"), all.x=T)
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

y3 <- merge(y, y2, by= c("Year","TaxonKey","GearGroupID","QuadID"), all.x=T)
y3$Proportion <- y3$Catch / y3$TotalCatch
y3 <- y3[,c("Year","Layer3GearID","GearGroupID","FishingEntityID","CountryGroupID","TaxonKey","SpeciesGroupID","QuadID","BigCellID","Proportion")]

z <- merge(znm2, y3, by= c("Year","GearGroupID","TaxonKey","QuadID"), all.x=T)
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

y3 <- merge(y, y2, by= c("Year","TaxonKey","QuadID"), all.x=T)
y3$Proportion <- y3$Catch / y3$TotalCatch
y3 <- y3[,c("Year","Layer3GearID","GearGroupID","FishingEntityID","CountryGroupID","TaxonKey","SpeciesGroupID","QuadID","BigCellID","Proportion")]

z <- merge(znm3, y3, by= c("Year","TaxonKey","QuadID"), all.x=T)
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

y3 <- merge(y, y2, by= c("Year","QuadID"), all.x=T)
y3$Proportion <- y3$Catch / y3$TotalCatch
y3 <- y3[,c("Year","Layer3GearID","GearGroupID","FishingEntityID","CountryGroupID","TaxonKey","SpeciesGroupID","QuadID","BigCellID","Proportion")]

z <- merge(znm4, y3, by= c("Year","QuadID"), all.x=T)
z$PropCatch <- z$Catch * z$Proportion

zm5 <- z[is.na(z$Proportion)==F,]
znm5 <- z[is.na(z$Proportion),]

zm5 <- zm5[,c("Year","FishingEntityID.y","CountryGroupID.y","Layer3GearID.x","GearGroupID.x","TaxonKey.x","SpeciesGroupID.x","QuadID","BigCellID.x","PropCatch")]
colnames(zm5) <- c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","QuadID","BigCellID","Catch")

#Join all the matched data, remove QuadID
z <- rbind(zm1, zm2, zm3, zm4, zm5)[,-8]
#remove QuadID, reorder for rbind
y <- y[,c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","QuadID","BigCellID","Catch")]
y <- y[,-8]
#Bind together
x <- rbind(y, z) #Join the split 'All countries' data with the other data

#Sum up any repeated entries (due to the split-up)
iccat.S <- aggregate(x$Catch, by= list(x$Year, x$FishingEntityID, x$CountryGroupID, x$Layer3GearID, x$GearGroupID, x$TaxonKey, x$SpeciesGroupID, x$BigCellID), sum) 
colnames(iccat.S) <- c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","BigCellID","Catch")

#clear up memory
rm(list = c("y","y2","y3","z","znm1","zm2","zm3","zm4","zm5","zm1","znm2","znm3","znm4","znm5"))

#USSR splits
x <- iccat.S[iccat.S$FishingEntityID!=2000,]
est= geo= lat= rus= ukr= iccat.S[iccat.S$FishingEntityID==2000,]

#53 Estonia, 63 Georgia, 98 Latvia, 148 Russian Fed, 181 Ukraine
soviet.prop <- read.table("Formatted INPUT Soviet Catch Proportions 1991.txt", header=T)

est$FishingEntityID <- 53; est$Catch= est$Catch * soviet.prop$Estonia
geo$FishingEntityID <- 63; geo$Catch= geo$Catch * soviet.prop$Georgia
lat$FishingEntityID <- 98; lat$Catch= lat$Catch * soviet.prop$Latvia
rus$FishingEntityID <- 148; rus$Catch= rus$Catch * soviet.prop$RussianFed
ukr$FishingEntityID <- 181; ukr$Catch= ukr$Catch * soviet.prop$Ukraine

soviets <- rbind(est, geo, lat, rus, ukr)

iccat.S <- rbind(x, soviets)

#France + C. Ivoire + Senegal = Fleet800-00 = FEID3000
#France = FEID 58
#Cote D'Ivoire = FEID 89
#Senegal = FEID 157
x <- iccat.S[iccat.S$FishingEntityID!=3000,]
fr= ci= se= iccat.S[iccat.S$FishingEntityID==3000,]
fis.prop <- read.csv("Formatted INPUT France, Cote D'Ivoire and Senegal Proportions.csv", header=T)

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

#Korea + Panama = Fleet801-00 = FEID5000
#Korea = FEID 95
#Panama = FEID 135
x <- iccat.S[iccat.S$FishingEntityID!=5000,]
kr= pa= iccat.S[iccat.S$FishingEntityID==5000,]
kp.prop <- read.csv("Formatted INPUT Korea Panama Proportions.csv", header=T)

kr$FishingEntityID <- 95
kr <- merge (kr,kp.prop, by=c("FishingEntityID","Year"))
kr$SplitCatch <- kr$Catch * kr$Prop

pa$FishingEntityID <- 135
pa <- merge (pa,kp.prop, by=c("FishingEntityID","Year"))
pa$SplitCatch <- pa$Catch * pa$Prop

kp <- rbind(kr,pa)
kp <- kp[-c(9:10)]
colnames(kp)[colnames(kp) == "SplitCatch"] <- "Catch"
iccat.S <- rbind(x, fis)

#Yugoslavia split
x <- iccat.S[iccat.S$FishingEntityID!=4000,]
#Croatia and Montenegro are countries fishing in this data set
cro = mon = iccat.S[iccat.S$FishingEntityID==4000,]
yugo.prop <- read.table("Formatted INPUT Yugoslavia Proportions 1991.txt", header = T)
#42 Croatia
#194 Montenegro 
cro$FishingEntityID <- 42 
cro$Catch <- cro$Catch * yugo.prop$Croatia
mon$FishingEntityID <- 194
mon$Catch <- mon$Catch * yugo.prop$Montenegro
yugo <- rbind(cro,mon)
iccat.S <- rbind(x, yugo)

#Sum up catch from splits
iccat.S <- aggregate(x$Catch, by= list(x$Year, x$FishingEntityID, x$CountryGroupID, x$Layer3GearID, x$GearGroupID, x$TaxonKey, x$SpeciesGroupID, x$BigCellID), sum) 
colnames(iccat.S) <- c("Year","FishingEntityID","CountryGroupID","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID","BigCellID","Catch")

#Export the new table to a csv file
write.table(iccat.S, "Formatted ICCAT Spatial Catch For Spatial Matching.csv", sep=",", row.names=F)  

#=============================================================================================================
#==SECTION I: INITIAL DATA AND TRANSFORMATION
#=============================================================================================================
rm(list=ls())

#1.Read in databases of nominal and spatialized catch by ocean

nom=read.csv("Formatted ICCAT Nominal Catch For Spatial Matching.csv",sep=",",header=T)	#Yearly nominal catch

spat=read.csv("Formatted ICCAT Spatial Catch For Spatial Matching.csv",sep=",",header=T)  #Yearly available spatial catch

nom= nom[nom$Catch!=0,]
#nom= nom[nom$Catch>1,] #Remove all catches less than 1 t

spat= spat[spat$Catch!=0,]
spat= spat[is.na(spat$BigCellID)==F,]

gearres= read.csv("input_codes/INPUT ICCAT GearRestrictionTable.csv",sep=",") #Read in gear restrictions
areares= read.csv("input_codes/INPUT ICCAT AreaRestrictionTable.csv",sep=",") #Read in area restrictions

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
match.categ= list(spat$FishingEntityID, spat$CountryGroupID,spat$Year, spat$Layer3GearID, spat$GearGroupID, spat$TaxonKey, spat$SpeciesGroupID)
merge.categ= c("FishingEntityID", "CountryGroupID","Year","Layer3GearID","GearGroupID","TaxonKey","SpeciesGroupID")
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

#=============================================================================================================
#==SECTION IV: Re-Run with Larger Year Ranges
#=============================================================================================================

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

###STOP###

#stop("SHOULD BE DONE...") #

#====================================Writing======================================#

#order by year
db.match= db.match[order(db.match$Year),]
#RFMO i.d. for ICCAT = 6
db.match$RFMOID= 6
db.match= db.match[ order(db.match$Year), c("RFMOID","Year","FishingEntityID","Layer3GearID","TaxonKey","BigCellID","Catch","MatchID") ]

#SO Added the write function to loop instead of manually counting the rows and creating separate files
#write table with match IDs
#write.table(db.match[1:1000000, ], "OUTPUT ICCAT Spatialized Catch 1 of 2 with MatchID.csv", sep=",", row.names=F, quote=F)
#write.table(db.match[1000001:dim(db.match)[1], ], "OUTPUT ICCAT Spatialized Catch 2 of 2 with MatchID.csv", sep=",", row.names=F, quote=F)
chunk_size <- 1000000
n <- nrow(db.match)
num_chunks <- ceiling(n / chunk_size)

for (i in 1:num_chunks) {
  start_row <- ((i - 1) * chunk_size) + 1
  end_row <- min(i * chunk_size, n)
  chunk <- db.match[start_row:end_row, ]
  filename <- paste0("Final ICCAT Spatialized Catch with MatchID ",i, " of ", num_chunks,".csv")
  write.table(chunk, filename, sep = ",", row.names = FALSE, quote = FALSE)
}

#aggregate catch without match IDs
#agc= aggregate(db.match$Catch, by= list(db.match$Year,db.match$FishingEntityID,db.match$Layer3GearID,db.match$TaxonKey,db.match$BigCellID), sum)
#write.table(agc[1:1000000, ], "OUTPUT ICCAT Spatialized Catch 1 of 2 no MatchID.csv", sep=",", row.names=F, quote=F)
#write.table(agc[1000001:dim(db.match)[1], ], "OUTPUT ICCAT Spatialized Catch 2 of 2 no MatchID.csv", sep=",", row.names=F, quote=F)
if (sum(db.nomatch$Catch) != 0) {
  agc= aggregate(db.nomatch$Catch, by= list(db.match$Year,db.match$FishingEntityID,db.match$Layer3GearID,db.match$TaxonKey,db.match$BigCellID), sum)
  n <- nrow(agc)
  num_chunks <- ceiling(n / chunk_size)
  
  for (i in 1:num_chunks) {
    start_row <- ((i - 1) * chunk_size) + 1
    end_row <- min(i * chunk_size, n)
    chunk <- agc[start_row:end_row, ]
    filename <- paste0(
      "Final ICCAT Spatialized Catch no MatchID ",
      i, " of ", num_chunks,
      ".csv"
    )
    write.table(chunk, filename, sep = ",", row.names = FALSE, quote = FALSE)
  }
}

#=========================Testing===========================================#

sum(db.nomatch$Catch) #Nomatch is equal to 0?

sum(db.match$Catch)/sum(nom$Catch) #Matched % is equal to 1?

#Country catch check, change index to desired FishingEnt
ccatch=function(country=x)
#After you read in the function, just type ccatch(#)
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
