import pandas as pd
from pathlib import Path


def read_data(file_path, file_type='csv', sheet_name=None):
    """Reads data from a specified file into a DataFrame with error handling.

    Parameters:
    file_path (str or Path): The path to the file to read.
    file_type (str): The type of file to read ('csv' or 'excel'). Defaults to 'csv'.
    sheet_name (str): The name of the sheet to read if the file is an Excel file. Defualts to None.

    Returns:
    DataFrame or None: Returns the loaded DataFrame or None if file is not found.
    """
    if not Path(file_path).exists():
        print(f"File not found. Please check the file path. Error: {file_path}")
        return None

    try:
        if file_type == 'csv':
            return pd.read_csv(file_path)
        elif file_type == 'excel':
            return pd.read_excel(file_path, sheet_name=sheet_name)
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except pd.errors.EmptyDataError:
        print(f"No data found in the file: {file_path}")
    except Exception as e:
        print(f"An error occured while reading the file: {e}")
    return None


def describe_dataframe(df):
    """Prints information that describes the data in the DataFrame.

    Parameters:
    df (DataFrame): The DataFrame to describe.

    Returns:
    None: This function does not return any value. It prints the shape,
          column names, data types, info, and descriptive statistics of the DataFrame.
    """
    if df is not None:
        print("Shape of DataFrame:", df.shape)
        print("\nFirst 5 rows:\n", df.head())
        print("\nLast 5 rows:\n", df.tail())
        print("\nColumn Labels:\n", df.columns.tolist())
        print("\nData Types:\n", df.dtypes)
        print("\nDataFrame Info:")
        df.info()
        print("\nDescriptive Statistics:\n", df.describe(include='all'))
    else:
        print("DataFrame is None; skipping description.")


def prepare_dataframe(df, npc_codes_df=None):
    """
    Prepare the DataFrame by cleaning column names, converting data fields,
    filling NaNs in specified columns, and merging with NPC codes if provided.

    Parameters:
        df (pd.DataFrame): The DataFrame to prepare.
        npc_codes_df (pd.DataFrame, opitonal): The DataFrame with NPC codes to merge with df.

    Returns:
        pd.DataFrame: The cleaned and prepared DataFrame
    """

    # Strip whitespace, convert to lowercase, and replace spaces in column names
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

    # Convert 'start' and 'end' columns to date time
    if 'start' in df.columns:
        df['start'] = pd.to_datetime(df['start'], format='%d/%m/%Y')
    if 'end' in df.columns:
        df['end'] = pd.to_datetime(df['end'], format='%d/%m/%Y')

    # Define columns to (fill NaN with 0) and convert float64 columns to int
    columns_to_change = ['countries', 'events', 'participants_m', 'participants_f', 'participants']

    for column in columns_to_change:
        if column in df.columns:
            df[column] = df[column].fillna(0).astype(int)  # *Can also fill with others or drop rows

    # Merge with NPC codes DataFrame if provided
    if npc_codes_df is not None:
        npc_codes_df.columns = npc_codes_df.columns.str.strip().str.lower()
        df = df.merge(npc_codes_df, how='left', left_on='country', right_on='Name')

    return df


def main():
    # Set display option to show all columns
    pd.set_option("display.max_columns", None)

    # Define file paths
    paralympics_datafile_csv = Path(__file__).parent.parent.joinpath("src", "tutorialpkg", "data", "paralympics_events_raw.csv")
    paralympics_datafile_excel = Path(__file__).parent.parent.joinpath("src", "tutorialpkg", "data", "paralympics_all_raw.xlsx")

    # Load and describe the CSV data file
    events_csv_df = read_data(paralympics_datafile_csv, file_type='csv')
    describe_dataframe(events_csv_df)

    # Load NPC codes data, specifying columns and handling encoding issues
    npc_codes_df = pd.read_csv(
        'src/tutorialpkg/data/npc_codes.csv',
        usecols=['Code', 'Name'],
        encoding='utf-8',
        encoding_errors='ignore'
    )

    # Prepare the data and merge with NPC codes
    prepared_events_df = prepare_dataframe(events_csv_df, npc_codes_df=npc_codes_df)

    # Print the 'country', 'Code', 'Name' columns to check the merged result
    print(prepared_events_df[['country', 'Code', 'Name']])

    # Prepare the events CSV DataFrame
    prepared_events_csv_df = prepare_dataframe(events_csv_df)

    # Load and describe the first sheet of the Excel data file
    events_excel_df = read_data(paralympics_datafile_excel, file_type='excel', sheet_name=0)
    describe_dataframe(events_excel_df)

    # Prepare the events Excel DataFrame
    prepared_events_excel_df = prepare_dataframe(events_excel_df)

    # Load and describe the second sheet ('medal_standings) of the Excel data file
    medal_standings_df = read_data(paralympics_datafile_excel, file_type='excel', sheet_name='medal_standings')
    describe_dataframe(medal_standings_df)

    # Prepare the medal standings DataFrame
    prepared_medal_standings_df = prepare_dataframe(medal_standings_df)


if __name__ == '__main__':
    main()
