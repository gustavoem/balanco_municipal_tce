import wget
import zipfile
import pandas as pd
import os

def get_expenses(city, year):
    """ Fetches expenses data from tce for a given city and year. 
    
        Parameters
        city: a string in lower case with the name of the city.
        year: an integer representing the year

        Returns a pandas.DataFrame object
    """
    url_prefix = "https://transparencia.tce.sp.gov.br/sites/default/" \
            + "files/csv/despesas"
    expenses_url = url_prefix + "-" + city + "-" + str(year) + ".zip"
    try:
        os.mkdir('data')
    except OSError as error:
        pass
    expenses_zipfile = 'data/expense_{}_{}.zip'.format(city, year)
    wget.download(expenses_url, expenses_zipfile)

    with zipfile.ZipFile(expenses_zipfile, 'r') as zip_ref:
        expenses_csv = 'data/' + zip_ref.namelist()[-1]
        extracted_files = zip_ref.extractall('data/')

    # Create df with csv
    data = pd.read_csv(expenses_csv, delimiter=';', 
            encoding='Windows 1252')
    data['vl_despesa'] = data['vl_despesa'].apply(
            lambda desp: float(desp.replace(',', '.')))
    return data


def only_paid(df):
    """ Filters on only paid expenses. """
    return df[df["tp_despesa"] == "Valor Pago"]


def get_all_themes(df):
    """ Get all themes of expenses.

        Parameters
        df: a pandas.DataFrame object with the expenses of a city.
    """
    return df["ds_funcao_governo"].unique()


def get_all_subthemes_of(df, theme):
    """ Get all subthemes of expenses of a theme.
    
        Parameters
        df: a pandas.DataFrame object with the expenses of a city.
        theme: a string with a theme that is present on the expenses
            of df. You can use get_all_themes to get a list of possible
            values.
    """

    return df[df["ds_funcao_governo"] == theme]\
            ["ds_subfuncao_governo"].unique()


def group_by_theme(df):
    """ Groups all expenses according to their theme. 
    
        Parameters
        df: a pandas.DataFrame object with the expenses of a city.
    """
    theme_df = df.groupby('ds_funcao_governo').agg(
            {'vl_despesa': 'sum', 'ds_subfuncao_governo': 'unique'})
    return theme_df


def group_by_theme_and_subtheme(df):
    """ Groups all expenses accoring to their theme and subtheme 
    
        Parameters
        df: a pandas.DataFrame object with expenses of a city.
    """
    subtheme_df = df.groupby(['ds_funcao_governo', 
        'ds_subfuncao_governo']).agg({'vl_despesa': 'sum'})
    return subtheme_df


def group_by_subtheme(df, theme):
    """ Groups expenses according to an specific theme. 
        
        Parameters
        df: a pandas.DataFrame object with the expenses of a city.
        theme: a string with a theme that is present on the expenses
            of df. You can use get_all_themes to get a list of possible
            values.
    """
    theme_n_subtheme_df = group_by_theme_and_subtheme(df)
    return theme_n_subtheme_df.loc[theme]


def get_themed_historical(city, years, themes):
    """ Aggregates expenses for each year in the determined themes. 
    
        Parameters
        city: a string in lower case with the name of the city.
        years: a list of integers representing the year
        theme: a list of themes of expenses. Check get_all_themes for 
            possible themes.
    """
    full_df = pd.DataFrame()
    for year in years:
        df = get_expenses(city, year)
        paid_df = only_paid(df)
        year_df = pd.DataFrame()
        for theme in themes:
            theme_df = group_by_theme(paid_df).loc[theme]
            year_df["vl_despesa_" + theme] = [theme_df["vl_despesa"]]
        full_df = pd.DataFrame.append(full_df, year_df)
    full_df.index = [y for y in years]
    return full_df


def get_subthemed_historical(city, years, subthemes):
    """ Aggregates expenses for each year in the determined subthemes. 
    
        Parameters
        city: a string in lower case with the name of the city.
        years: a list of integers representing the year
        subthemes: a list of pairs (theme, subtheme) of expenses. Check 
            get_all_subthemes_of for possible themes.
    """
    full_df = pd.DataFrame()
    for year in years:
        df = get_expenses(city, year)
        paid_df = only_paid(df)
        year_df = pd.DataFrame()
        for theme, subtheme in subthemes:
            theme_df = group_by_subtheme(paid_df, theme).loc[subtheme]
            column_name = "vl_despesa_{}_{}".format(theme, subtheme)
            year_df[column_name] = [theme_df["vl_despesa"]]
        full_df = pd.DataFrame.append(full_df, year_df)
    full_df.index = [y for y in years]
    return full_df


def get_all_subthemes_historical(city, years, themes):
    """ Aggregates expenses for each year in all subthemes of the 
        determined themes. 
        
        Parameters
        city: a string in lower case with the name of the city.
        years: a list of integers representing the year
        themes: a list of pairs themes of expenses. Check get_all_themes 
        for possible themes.
    """
    full_df = pd.DataFrame()
    for year in years:
        df = get_expenses(city, year)
        paid_df = only_paid(df)
        year_df = pd.DataFrame()
        for theme in themes:
            subthemes = get_all_subthemes_of(paid_df, theme)
            subthemes_df = group_by_subtheme(paid_df, theme)
            for subtheme in subthemes:
                subtheme_df = subthemes_df.loc[subtheme]
                column_name = "vl_despesa_{}_{}".format(theme, subtheme)
                year_df[column_name] = [subtheme_df["vl_despesa"]]
        full_df = pd.DataFrame.append(full_df, year_df)
    full_df.index = [y for y in years]
    return full_df
