# CS88: Basketball Reference Project

import pandas as pd
from pymongo import MongoClient
import certifi

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


def clean_and_format_data(dataframe):

    # Set all NaN 3-pointer related statistics to 0
    dataframe['3P'] = dataframe['3P'].fillna(0)
    dataframe['3PA'] = dataframe['3PA'].fillna(0)
    dataframe['3P%'] = dataframe['3P%'].fillna(0)

    # Remove all rows where Games played is less than 10
    dataframe = dataframe[dataframe.G >= 10]

    # Remove all rows where Team = TOT (only targeting the team specific rows)
    dataframe = dataframe[dataframe.Tm != 'TOT']

    return dataframe


def upload_to_mongoDB(df):


    CONNECTION_STRING = "mongodb+srv://mattcs88:ballislife1221ref@cluster0.f5msv.mongodb.net/?retryWrites=true&w=majority"
    client = MongoClient(CONNECTION_STRING, tlsCAFile=certifi.where())

    # Database
    mongo_database = client['testBasketballReference']

    # Collection
    per_game_statistics = mongo_database["testplayer_stats"]

    # Iterate through the data frame, create an "item" and based on the row value
    for index, row in df.iterrows():
        document = {
            "Rk": row["Rk"],
            "Player": row["Player"],
            "Pos": row["Pos"],
            "Age": row["Age"],
            "Tm": row["Tm"],
            "G": row["G"],
            "GS": row["GS"],
            "MP": row["MP"],
            "FG": row["FG"],
            "FGA": row["FGA"],
            "FG%": row["FG%"],
            "3P": row["3P"],
            "3PA": row["3PA"],
            "3P%": row["3P%"],
            "2P": row["2P"],
            "2PA": row["2PA"],
            "2P%": row["2P%"],
            "eFG%": row["eFG%"],
            "FT": row["FT"],
            "FTA": row["FTA"],
            "FT%": row["FT%"],
            "ORB": row["ORB"],
            "DRB": row["DRB"],
            "TRB": row["TRB"],
            "AST": row["AST"],
            "STL": row["STL"],
            "BLK": row["BLK"],
            "TOV": row["TOV"],
            "PF": row["PF"],
            "PTS": row["PTS"],
            "Player-additional": row["Player-additional"],
            "Year": row["Year"]
        }
        # Insert the row as a document into MongoDO
        per_game_statistics.insert_one(document)



def _check_if_same_columns():

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
            if prev_columns is not None:  # First input file being inspected

                if current_columns == prev_columns:
                    print("Match Found")
                else:
                    print("Error: Match Not Found")

            prev_columns = current_columns

        except FileNotFoundError:
            print("\n\nERROR: {} was not found\n\n".format(filepath))


def main():
    full_df = read_in_data()
    print("FULL DF: {}".format(full_df))

    cleaned_df = clean_and_format_data(full_df)
    print("CLEANED DF: {}".format(cleaned_df))
    #check_if_same_columns()

    upload_to_mongoDB(cleaned_df)


if __name__ == "__main__":
    main()



