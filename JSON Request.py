import requests
import pandas as pd
class EIAQuerying:
    '''
    Initiates an instance of the EIAQuerying class 
    taking in the users api key as an argument
    '''
    def __init__(self, api_key):
        self.api_key = api_key

    def _get_json_query(self, url):
        request = requests.get(url = url)
        return request.json()

    def get_series_data(self, series_id, start_date = None, start_time = None, end_date = None, end_time - None):
        '''
        Accepts a single series_id or a list of series ids of the form

        "series_1;series_2;series_3" with a maximum of 100 series that
        can be queried with one request
        '''

        if start_date or end_date == None:
            url = 'https://api.eia.gov/series/?series_id={0}&api_key={1}'.format(series_id,self.api_key)
        else:
            url = 'https://api.eia.gov/series/?series_id={0}&search_value=[{}T{}Z TO {T}{}Z]&api_key={1}'.format(
                series_id, 
                start_date,
                start_time,
                end_date,
                end_time
                self.api_key
                )
        json_response = self._get_json_query(url = url)['series']
        series_dict = {}
        i = 0
        while i < len(json_response):
            dataframe = pd.DataFrame()
            dictionary = json_response[i]
            dictionary_key = dictionary['name']
            dates = [_ for _ in range(0,len(dictionary['data']))]
            values = [_ for _ in range(0,len(dictionary['data']))]
            for index, value in enumerate(dictionary['data']):
                dates[index] = value[0]
                values[index] = value[1]
            dataframe['Dates'] = dates
            dataframe['Values'] = values
            series_dict[dictionary_key] = dataframe
            i += 1
        
        return series_dict

    def get_children_series_ids(self, category_id, frequency):
        '''
        A method that returns the children series of a specified category_id
        that is passed in the arguments

        The frequency argument accepts the following arguments;

        "A" : Annual
        "M" : Monthly
        "H" : Hourly
        "Q" : Quarterly
        '''
        if frequency in ['A', 'M', 'H']:
            url = 'https://api.eia.gov/category/?api_key={0}&category_id={1}'.format(self.api_key, category_id)
            json_response = self._get_json_query(url = url)['category']

            childeries_dict = {
                v['name'] : v['series_id'] for k, v in json_response['childseries'] if v['f'] == frequency 
            }
            return childeries_dict

    def prepare_series_string(self, series_dict = None, series_list = None):
        '''
        Accepts a dictionary or list of series ids as input and formats
        them into the required string format for query.

        Dictionary inputs are of the form;

        {
            "Series Name" : "Series ID",
            ...
        }
        '''
        series_string = ''
        if series_dict is not None:
            for key, value in series_dict:
                if len(series_string) == 0:
                    series_string += value
                else:
                    series_string += ';{}'.format(value)
                    return series_string
        if series_list is not None:
            for id in series_list:
                if len(series_string) == 0:
                    series_string += value
                else:
                    series_string += ';{}'.format(id)
                    return series_string    
    
