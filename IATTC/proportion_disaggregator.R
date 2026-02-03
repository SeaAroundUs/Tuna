library(tidyverse)
#' Allocates Catch with unknown Fishing Entity Based on Proportions of Catch with Known Fishing Entity
#'
#' This function calculates the catch proportions of specific fishing entities 
#' from catch data with known fishing entity (`known_catch`) within specified groups.
#' It then applies these proportions to disaggregate catch data with unknown fishing
#' entities (`unknown_catch`).
#'
#' @param known_catch A dataframe containing reference data with known FishingEntityIDs. 
#'   Must contain the columns: "Catch", "FishingEntityID", "CountryGroupID", and 
#'   all columns specified in `grouping_cols`.
#' @param unknown_catch A dataframe containing the catch data to be disaggregated. 
#'   Must contain "Catch" and all columns specified in `grouping_cols`.
#' @param grouping_cols A character vector of column names to group by 
#'   (e.g., `c("Year", "TaxonKey", "Layer3GearID")`). These columns define the 
#'   strata within which proportions are calculated and applied.
#'   @param is_spat A boolean that specifies if the input dataframe is from spatial catch or not. Default is FALSE (i.e. "BigCellID" will be considered )
#'
#' @return A list containing two dataframes:
#'   \item{matched_catch}{A dataframe of the disaggregated catch calculated as 
#'      Original Catch * Proportion.}
#'   \item{unmatched_catch}{A dataframe of the rows from `unknown_catch` that could 
#'     not be matched to any group in `known_catch`. These rows retain their original structure.}

proportional_catch <- function(known_catch, unknown_catch, grouping_cols, is_spat=FALSE){

  # Input error checks before proceeding to calculations
  if(!("FishingEntityID" %in% colnames(known_catch))){
    stop("FishingEntityID must be in the columns of known_catch dataframe")
  }
  
  if(!("CountryGroupID" %in% colnames(known_catch))){
    stop("CountryGroupID must be in the columns of known_catch dataframe")
  }
  
  if(is_spat && !("BigCellID" %in% colnames(known_catch))){
    stop("BigCellID must be in the columns of known_catch dataframe")
  }
  
  if(is_spat && !("BigCellID" %in% colnames(unknown_catch))){
    stop("BigCellID must be in the columns of unknown_catch dataframe")
  }
  
  if(!all(grouping_cols %in% colnames(known_catch))){
    stop("Given grouping columns are not all in known_catch dataframe")
  }
  
  if(!all(grouping_cols %in% colnames(unknown_catch))){
    stop("Given grouping columns are not all in unknown_catch dataframe")
  }
  
  
  # Calculate summed catch based on grouping columns in catch with known fishing entities
  known_sum <- known_catch |> 
    group_by(across(all_of(grouping_cols))) |> 
    summarise(TotalCatch = sum(Catch), .groups = "keep")
  
  # Calculate proportion of catch based on grouping columns for catches with known fishing entities
  known_prop <- known_catch |>
    left_join(known_sum, by = grouping_cols) |>
    mutate(Proportion = Catch/TotalCatch) |> 
    select(all_of(grouping_cols), "FishingEntityID",
           "CountryGroupID", 
           if(is_spat)"BigCellID",
           "Proportion" 
    )
  
  # Match proportions to target catches and disaggregate
  matched_unk <- unknown_catch |> 
    inner_join(known_prop, by = grouping_cols, relationship = "many-to-many") |> 
    mutate(PropCatch = Catch * Proportion) |> 
    select(-c("FishingEntityID.x", "CountryGroupID.x","Catch", "Proportion",
              if(is_spat && !("BigCellID" %in% grouping_cols))"BigCellID.y")) |> 
    dplyr::rename(FishingEntityID = FishingEntityID.y,
                  CountryGroupID = CountryGroupID.y,
                  Catch = PropCatch)
  
  
  if(is_spat && !("BigCellID" %in% grouping_cols)){
    matched_unk <- matched_unk |> 
      dplyr::rename(BigCellID = BigCellID.x)
  }
  
  # Filter unknown catch for ones that could not be matched with proportions with given grouping criteria
  unmatched_unk <- unknown_catch |> 
    anti_join(known_prop, by = grouping_cols) 
  
  # Print matching summary
  percent_matched <- (1-nrow(unmatched_unk)/nrow(unknown_catch))*100
  rows_matched <- nrow(unknown_catch)-nrow(unmatched_unk)
  cat(sprintf("%.2f%% records matched (%d records out of %d records), %d records remaining.", 
              percent_matched, rows_matched, nrow(unknown_catch), nrow(unmatched_unk)))
  
  return(list("matched_catch" = matched_unk, "unmatched_catch" = unmatched_unk))
}


