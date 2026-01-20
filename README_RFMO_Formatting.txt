#The following steps were taken to format the raw RFMO data for spatialization as of September 2025:

#======================IOTC======================
#Dowload the IOTC nominal and spatial datasets here: http://www.iotc.org/data/datasets
#Nominal = "Nominal catch by species and gear, by vessel flag reporting country"
#Spatial = "All CE files", this contained three datasets for surface, longline and coastal fleets

#To prepare the nominal data for formatting, we did the following:
#Open IOTC nominal catch data. This file is named "IOTC-DATASETS-2025-03-12-NC-ALL_1950-2023". In the "Catches_Captures" sheet:
#Delete "FlSort" column. 
#Delete "FlCode" column. 
#Delete "Flotte" column. 
#Delete "ArCode" column. 
#Delete "ZoneCTOI" column. 
#Delete "TypeFishery" column.
#Delete "TypeP?cherie" column.
#Delete "GrSort" column. 
#Delete "Gear" column.
#Delete "Engin" column.
#Delete "GrGroup" column.
#Delete "GrGroupe" column.
#Delete "GrMult" column.
#Delete "SpSort" column.
#Delete "Species" column.
#Delete "Esp?ce" column.
#Delete "SpLat" column.
#Delete "SpGroup" column.
#Delete "SpGroupe" column.
#Delete "SpWP/SpGT" column.
#Delete "SpIOTC" column.
#Delete "SpMult" column.
#Delete "Fgrounds" column.
#Delete "CatalogGroup" column.
#Change column name "Fleet" to "Country".
#Change column name "AreaIOTC" to "Area".
#Change column name "Year/An" to "Year".
#Change column name "TFCde" to "Sector".
#Change column name "GrCde" to "Gear".
#Change column name "SpCde" to "SpeciesCode".
#Change column name "Catch/Capture(t)" to "Catch".
#Save file as a *csv, named "INPUT IOTC Nominal Catch For Formatting.csv".

#To prepare the three spatial datasets, we did the following:
#Open IOTC spatial catch data. These file are named "IOTC-DATASETS-2025-03-13-CEOthers","IOTC-DATASETS-2025-03-13-CELongline" and "IOTC-DATASETS-2025-03-13-CESurface"
#Delete "MonthEnd" column.
#Delete "iGrid" column.
#Delete "Effort" column.
#Delete "EffortUnits" column.
#Delete "QualityCode" column.
#Delete "Source" column.
#Delete all species columns with "-NO". These are catch in numbers, rather than metric tonnes. 
#Make sure the catch column does not contain commas (thousands separators). 
#Save file as a *csv, named "INPUT IOTC Spatial Coastal Catch For Formatting.csv","INPUT IOTC Spatial Longline Catch For Formatting.csv","INPUT IOTC Spatial Surface Catch For Formatting.csv"

#======================CCSBT======================
#Dowload the CCSBT nominal and spatial datasets here: https://www.ccsbt.org/en/content/sbt-data
#Nominal: "Annual catch by flag or gear from 1952 to 2016 inclusive"
#Spatial: "Catch by year, month, gear, ocean and 5 degree grid from 1965 to 2016 inclusive"

#Prior to the 2013, CCSBT reported their data by fishing country by gear
#They now report by flag OR by  gear
#We used an old dataset for 1952-2012 as the totals matched the new data, previously called "SBT_Catch" when downloaded off of the CCSBT website
#Then we used the gear proportions of each fishing country by ocean in 2012 to disaggregate the 2013-2016 data, from the file called "CatchByOYF" when downloaded off of the CCSBT website

#To prepare the disaggregated 1952-2015 nominal data for formatting, we did the following:
#Change column name "YEAR" to "Year". 
#Change column name "Country_CODE" to "CountryName".
#Change column name "GEAR_CODE" to "GearName".
#Change column name "OCEAN_CODE" to "OceanName".
#Change column name "WEIGHT" to "Catch".
#Save file as a *csv, named "INPUT CCSBT Nominal Catch For Formatting.csv"

#Open CCSBT raw spatial data, called "CatchByYMGOLoLa" when downloaded off of the CCSBT website
#Delete rows 1 through 6 (spreadsheet should start with column headings). 
#Change column name "Calendar Year" to "Year".
#Change column name "Gear" to "GearName".
#Change column name "Ocean" to "OceanName".
#Change column name "Latitude" to "Lat".
#Change column name "Longitude to "Lon"
#Change column name "Tonnes" to "Catch".
#Make sure the catch column does not contain commas (thousands separators). 
#Save file as a *csv, named "INPUT CCSBT Spatial Catch For Formatting.csv". 


#======================IATTC======================
#Dowload the IATTC nominal and spatial datasets here: https://www.iattc.org/Catchbygear/IATTC-Catch-by-species1.htm
#Nominal = "EPO total estimated catch by year, flag, gear, species"
#Spatial = "Tuna EPO purse seine catch and effort aggregated by year, month, flag or set type, 1?x1?", "Billfish EPO purse seine catch and effort aggregated by year, month, flag or set type, 1?x1?","Shark EPO purse seine catch and effort aggregated by year, month, flag or set type, 1?x1?","Tuna and billfish EPO longline catch and effort aggregated by year, month, flag, 5?x5?","Shark EPO longline catch and effort aggregated by year, month, flag, 5?x5?","Tuna EPO pole and line catch and effort aggregated by year, month, flag, 1?x1?"

#To prepare the disaggregated 1918-2016 nominal data for formatting, we did the following:
#Open IATTC raw nominal data, called "CatchFlagGear1918-2016" when downloaded off of the IATTC website
#Change column name "AnoYear" to "Year". 
#Change column name "BanderaFlag" to "Flag".
#Change column name "ArteGear" to "Gear".
#Change column name "EspeciesSpecies" to "Species".
#Change column name "t" to "Catch".
#Save file as a *csv, named "INPUT IATTC Nominal Catch For Formatting.csv".

#IATTC spatial data is split by target taxon group and gear, 6 separate files
#To prepare the files we did the following, raw data file names from the website downloads in brackets

#Purse Seine

#Billfish (PublicPSBillfishFlag) /Tuna (PublicPSTunaFlag)
#Change column name "LatC1" to "Lat". 
#Change column name "LonC1" to "Lon". 
#Delete the column "Month".
#Delete the column "NumSets".
#Save file as a csv named "INPUT IATTC Spatial Catch For Billfish Purse Seine Formatting.csv" and  "INPUT IATTC Spatial Catch For Tuna Purse Seine Formatting.csv"

#Shark (PublicPSSharkFlag)
#Change column name "LatC1" to "Lat". 
#Change column name "LonC1" to "Lon". 
#Delete the column "Month".
#Delete the column "NumSets".
#Delete all columns ending with "n".
#Delete the "mt" from the end of every taxon column.
#Save file as a csv named "INPUT IATTC Spatial Catch For Shark Purse Seine Formatting.csv"

#Longline

#Tuna and Billfish (PublicLLTunaBillfishMt)/Shark (LLSharkMt)
#Change column name "LatC5" to "Lat". 
#Change column name "LonC5" to "Lon". 
#Delete the column "Month".
#Delete the column "Hooks".
#Delete all columns ending with "n".
#Save file as a csv named "INPUT IATTC Spatial Catch For Shark Longline Formatting.csv" and "INPUT IATTC Spatial Catch For Tuna and Billfish Longline Formatting.csv"

#Pole and Line 
#Tuna (PublicLPTunaFlag)
#Change column name "LatC1" to "Lat". 
#Change column name "LonC1" to "Lon". 
#Delete the column "Month".
#Delete the column "NumSets".
#Save file as a csv named "INPUT IATTC Spatial Catch For Tuna Pole and Line Formatting.csv"


#======================ICCAT======================
#Dowload the IATTC nominal and spatial datasets here: https://www.iccat.int/en/accesingdb.htm
#Nominal: "Task I" under "Nominal Catch Information"
#Spatial: "Task II catch/effort" under "Sample fishing statistics and fish sizes"

#To prepare the ICCAT nominal data for formatting, we did the following:
#Open the raw ICCAT nominal data, called "t1nc_all_20161114" when downloaded off of the ICCAT website
#Delete first three rows of data.
#Delete columns A:Y (The filtered data and the QalInfoCode key table)
#Change column name "YearC" to "Year". 
#Change column name "Qty_t" to "Catch".
#Change column name "Species" to "SpeciesCode".
#Delete "RecID" column.
#Delete "ScieName" column. 
#Delete "SpeciesGrp" column.
#Delete "Decade" column.
#Delete "Status" column.
#Delete "PartyName" column.
#Delete "Fleet" column.
#Delete "Stock" column.
#Delete "SampAreaCode" column.
#Delete "Area" column.
#Delete "SpcGearGrp" column.
#Delete "GearGrp" column.
#Delete "QalInfoCode" column.
#Delete the rows containing DD and DM from "CatchTypeCode" column, these are discards and are estimated later in the process. Then delete "CatchTypeCode" column.
#Make sure that the catch column does not contain commas (thousands separators). 
#Save file as a *csv, named "INPUT ICCAT Nominal Catch For Formatting.csv".

#To prepare the ICCAT spatial data for formatting, we did the following:
#Open the raw ICCAT spatial data, called "t2ce" when downloaded off of the ICCAT website
#Change column name "YearC" to "Year". 
#Delete "StrataID" column. 
#Delete "DSetID" column.
#Delete "GearGrpCode" column.
#Delete "FileTypeCode" column.
#Delete "TimePeriodID" column.
#Delete "SchoolTypeCode" column.
#Delete "Eff1" column.
#Delete "Eff1Type" column.
#Delete "Eff2" column.
#Delete "Eff2Type" column.
#Delete "DSetType" column.
#Delete the rows containing LLD from "GearCode", these are discards and are estimated later in the process.
#Make sure the catch columns do not contain commas (thousands separators). 
#Save file as a *csv, named "INPUT ICCAT Spatial Catch For Formatting.csv".

#======================WCPFC======================
#Dowload the WCPFC nominal dataset here: https://www.wcpfc.int/doc/annual-catch-estimates-2022-data-files
#Dowload the WCPFC spatial datasets here: https://www.wcpfc.int/wcpfc-public-domain-aggregated-catcheffort-data-download-page

#To prepare the nominal data for formatting, we did the following:
#Open WCPFC raw nominal data, called "XLS_WCPFC" when downloaded off of the WCPFC website
#Change column name “YY” to “Year”. 
#Change column name “GEAR_CODE” to “Gear”.
#Change column name “FLAG_CODE” to “Flag”.
#Change column name “FLEET_CODE” to “Fleet”.
#Change column name “SP_CODE” to “SpeciesName”.
#Change column name “SP_MT” to “Catch”.
#Delete column name “SP_NAME”.
#This is catch in metric tonnes, as opposed to catch in numbers (present in some databases). 
#Save file as a *csv, named “INPUT WCPFC Nominal Catch For Formatting.csv”.

#The WCPFC spatial datasets are separated into gears in Excel format

#To prepare the longline spatial datasets, we did the following:
#Open WCPFC extracted raw spatial data, called WCPFC_L_PUBLIC_BY_YR_MON
#Change column name “yy” to “Year”.
#Change column name “mm” to “Month”.
#Change column name “lat5” to “Lat”.
#Change column name “lon5” to “Lon”
#Delete column “cwp_grid” and "hhooks".
#Delete all species name columns with “_n”. This will only leave catch in metric tonnes, as opposed to catch in numbers. 
#Save file as a *csv, named “INPUT WCPFC Spatial Longline For Formatting.csv”.

#To prepare the purse seine spatial datasets, we did the following:
#Open WCPFC extracted raw spatial data, called WCPFC_S_PUBLIC_BY_YR_MON
#Change column name “yy” to “Year”.
#Change column name “mm” to “Month”.
#Change column name “lat5” to “Lat”.
#Change column name “lon5” to “Lon”
#Delete column “cwp_grid”, "days", "sets_una", "sets_log", "sets_dfad", "sets_afad", and "sets_oth".
#Save file as a *csv, named “INPUT WCPFC Spatial Purse Seine For Formatting.csv”.

#To prepare the pole and line spatial datasets, we did the following:
#Open WCPFC extracted raw spatial data, called WCPFC_P_PUBLIC_BY_YR_MON
#Change column name “yy” to “Year”.
#Change column name “mm” to “Month”.
#Change column name “lat5” to “Lat”.
#Change column name “lon5” to “Lon”
#Delete column “cwp_grid” and "days".
#Save file as a *csv, named “INPUT WCPFC Spatial Pole and Line For Formatting.csv”.

#To prepare the driftnet spatial datasets, we did the following:
#Open WCPFC extracted raw spatial data, called WCPFC_G_PUBLIC_BY_YR_MON
#Change column name “yy” to “Year”.
#Change column name “mm” to “Month”.
#Change column name “lat5” to “Lat”.
#Change column name “lon5” to “Lon”
#Delete column "days" and "alb_n".
#Save file as a *csv, named “INPUT WCPFC Spatial Driftnet For Formatting.csv”.