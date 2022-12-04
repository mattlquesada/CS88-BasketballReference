# CS88: Basketball Reference Project

import pandas as pd

# Read all the csv files into a joint pandas DF

# Drop lines that are "TOT"

# Drop lines that have very few game count

# Old Years: Zero out value were it makes sense: 3PM, 3P% (not stls, blocks)

# Add a column for the year of that specific NBA season

# Iterate through the dataframe; create dict/json object, upload to mongo


def read_in_data():
    """
    read_in_data: Iterates through input file directory for all raw csv file inputs. Consolidates into single dataframe.
    Also adds additional column with the year. The year will refer to the year the season started.
    :return:
    :rtype:
    """
    data_directory = "/Users/mquesada/PycharmProjects/CS88-BasketballReference/test_data/"
    # data_directory = "/Users/mquesada/PycharmProjects/CS88-BasketballReference/data/"
    season_yr_pre_min = 2019
    season_yr_pre_max = 2021

    first_season = True
    df = None

    # Iterate through all files in the specified data file directory
    for season_yr_pre in range(season_yr_pre_min, season_yr_pre_max + 1):
        season_yr_post = season_yr_pre + 1
        current_filename = str(season_yr_pre) + '-' + str(season_yr_post) + '.csv'
        filepath = data_directory + current_filename

        # Build up a single data frame with raw data + year
        try:
            new_df = pd.read_csv(filepath)
            new_df.insert(31, "Year", season_yr_pre)
            if first_season:    # If first season, load the df
                df = new_df
                first_season = False
            else:    # If not first season, add the dataframe to the growing cumulative dataframe
                df = pd.concat([df, new_df], axis=0)
        except FileNotFoundError:
            print("\n\nERROR: {} was not found\n\n".format(filepath))

    return df


def check_if_same_columns():

    # data_directory = "/Users/mquesada/PycharmProjects/CS88-BasketballReference/test_data/"
    data_directory = "/Users/mquesada/PycharmProjects/CS88-BasketballReference/data/"
    season_yr_pre_min = 1949
    season_yr_pre_max = 2021

    prev_columns = None
    current_columns = None

    # Iterate through all files in the specified data file directory
    for season_yr_pre in range(season_yr_pre_min, season_yr_pre_max + 1):
        season_yr_post = season_yr_pre + 1
        current_filename = str(season_yr_pre) + '-' + str(season_yr_post) + '.csv'

        filepath = data_directory + current_filename
        # print("filepath: {}".format(filepath))

        try:
            df = pd.read_csv(filepath)
            # print(df)
            current_columns = list(df.columns)
            if prev_columns is not None:    # First input file being inspected

                if current_columns == prev_columns:
                    print("Match Found")
                else:
                    print("Error: Match Not Found")

            prev_columns = current_columns

        except FileNotFoundError:
            print("\n\nERROR: {} was not found\n\n".format(filepath))


def main():
    full_df = read_in_data()
    print(full_df)
    #check_if_same_columns()


if __name__ == "__main__":
    main()



