
import solara
import ipyleaflet
from ipywidgets import HTML
import altair as alt
import pandas as pd

# solara run sol.py --host=0.0.0.0

# Import the TAHMO module
import TAHMO

# The demo credentials listed below give you access to three pre-defined stations. 
api = TAHMO.apiWrapper()

# set the credentials
api.setCredentials('demo', 'DemoPassword1!')

stations = list(api.getStations())

zoom = solara.reactive(7)
center = solara.reactive([5.53, -0.20])
station = solara.reactive('TA00134')

def request_precip_data(station):
    startDate = '2023-01-01'
    endDate = '2023-11-22'
    variables = ['pr']
    
    df = api.getMeasurements(station.value, startDate=startDate, endDate=endDate, variables=variables)
    try:
        df.index.name = 'Timestamp'
        df.to_csv('data/timeseries.csv', na_rep='', date_format='%Y-%m-%d %H:%M')
    except:
        df = pd.DataFrame(columns=['Timestamp', 'pr'])
        df.to_csv('data/timeseries.csv', na_rep='', date_format='%Y-%m-%d %H:%M')

def load_precip_data():
    try:
        df = pd.read_csv("data/timeseries.csv", parse_dates=True, index_col=0).reset_index().rename(columns={"Timestamp" : "date", "pr": "precipitation"})
        df.loc[:,'date'] = df['date'].dt.date
        df = df.groupby('date').max().reset_index().dropna()
    except:
        df = pd.DataFrame(columns=['date', 'precipitation'])
    return df




def Timeseries(station):
    request_precip_data(station)
    df = load_precip_data()
    with solara.Card("Timeseries"):
        chart =  alt.Chart(df).mark_bar().encode(x="date", y="precipitation", tooltip=['precipitation', 'date']).properties(width=800, height=350).interactive()
        solara.display(chart)

def get_station_data(station):
    station_data = api.getStations()[station.value]
    name = station_data['location']['name']
    latitude = station_data['location']['latitude']
    longitude = station_data['location']['longitude']
    return name, latitude, longitude

@solara.component
def Controls(stations):
    solara.Select(label='station', values=stations, value=station)


@solara.component
def View(station, center, zoom):
    name, latitude, longitude = get_station_data(station)
    center.value = [latitude, longitude]
    m = ipyleaflet.Map(center=center.value, zoom=zoom.value)
    marker = ipyleaflet.Marker(location=(latitude, longitude), draggable=True)
    marker.popup = HTML(str(name))
    m.add_layer(marker)
    solara.display(m)

@solara.component
def Page():
    with solara.AppBarTitle():
        solara.Text("Precipitation forecast and observations")

    # with solara.Sidebar():
    #     solara.Markdown("## I am in the sidebar")
        
    solara.Info("I'm in the main content area, put your main content here")

    with solara.Card("", margin=1):
        solara.Markdown("## Choose a station and see the timeseries and the map")
        Controls(stations)
        
    with solara.Columns([1, 1]):
        View(station, center, zoom)
        Timeseries(station)

                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 