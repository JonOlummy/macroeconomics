import pandas as pd
from configparser import ConfigParser
import logging
import warnings
warnings.simplefilter("ignore")

# Configure logging
logging.basicConfig(filename='data_cleaning.log', filemode='w', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def find_columns_with_missing_data(df: pd.DataFrame) -> list:
    """
    Identify columns in a dataframe that has missing data.

    :param <pd.DataFrame> df: dataframe to check for missing data.
    return <list>: column names that have missing data.
    """
    missing_data_columns = []
    for column in df.columns:
        if df[column].isnull().any() or (df[column] == '').any():
            missing_data_columns.append(column)
    return missing_data_columns

def read_config(config_file: str) -> ConfigParser:
    """
    Read a configuration file and return a ConfigParser object.

    :parameter config_file (str): The path to the configuration file.
    :return <ConfigParser>: ConfigParser object containing the configuration.
    """
    try:
        config = ConfigParser()
        config.read(config_file)
        logging.info(f'Read config file {config_file} successfully.')
        return config
    except Exception as e:
        logging.error(f'Error reading config file {config_file}: {e}')
        raise

def input_missing_data(df: pd.DataFrame, config: ConfigParser) -> pd.DataFrame:
    """
    Input missing data in a dataframe based on strategy defined for each column in a configuration file.
    
    :param df: The dataframe with missing data.
    :param config: The ConfigParser object with input strategies.
    :return: The DataFrame with missing data inputted.
    """
    try:
        for column in df.columns:
            if column.strip() in config.sections():
                strategy = config.get(column.strip(), 'strategy').lower()
                    # Apply strategy defined in the parameter configuration file
                if strategy == 'mean':
                    df.loc[:, column] = df[column].fillna(df[column].mean())
                elif strategy == 'median':
                    df.loc[:, column] = df[column].fillna(df[column].median())
                elif strategy == 'mode':
                    mode_value = df[column].mode()[0] 
                    df.loc[:, column] = df[column].fillna(mode_value)
                elif strategy == 'zero':
                    df.loc[:, column] = df[column].fillna(0)
                elif strategy == 'remove':
                    df = df.dropna(subset=[column])
                elif strategy == 'ffill':
                    df.loc[:, column] = df[column].ffill()
                elif strategy == 'bfill':
                    df.loc[:, column] = df[column].bfill()
                elif strategy in ('nearest','linear','slinear','quadratic','cubic'):
                    df.loc[:, column] = df[column].interpolate(method=strategy)
                else:
                    raise AttributeError(f"invalid strategy ({strategy})")

                logging.info(f'{column}: cleaned using strategy {strategy}')
        return df
    except Exception as e:
        logging.error(f'inputting missing data for {column}: {e}')

if __name__ == '__main__':
    # Read raw downloaded data in CSV file
    data = pd.read_csv('data/data.csv', parse_dates=['Date'])

    # print(find_columns_with_missing_data(data))

    # Remove any whitespace from the column names
    data.columns = data.columns.str.strip()

    config_file = 'parameter_config.ini'
    config = read_config(config_file)

    df_cleaned = input_missing_data(data, config)

    try:
        df_cleaned.to_csv('data/cleaned_data.csv', index=False)
        logging.info('Data cleaning process completed successfully and output saved as cleaned_data.csv')
        print("Data cleaning process completed successfully!")
    except Exception as error:
        logging.error('Data Cleaning process failed')
        print('etl task failed!!!')