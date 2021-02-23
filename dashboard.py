#!/usr/bin/env python
# coding: utf-8

# In[5]:


import pandas as pd
import glob
import copy
import luigi
from datetime import datetime
import plotly.express as px
from jupyter_dash import JupyterDash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import numpy as np
from simpleplotly import FigureBuilder, FigureHolder
from simpleplotly import Line, Scatter, Bar, Box, Histogram, Heatmap, Scatter3D,Surface
from simpleplotly import Shape, Annotation, XAxis, YAxis, ZAxis
import plotly.figure_factory as plotly_tool
import dash
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots


# In[3]:


path =r'C:/Users/amine/OneDrive/Bureau/Dw Project/DW'
filenames = glob.glob(path + "/*.csv")

dfs = []
for filename in filenames:
    dfs.append(pd.read_csv(filename))
import re
def idr(data):
    mylist = list(data.columns)
    r = re.compile("ID.*")
    newlist = list(filter(r.match, mylist)) 
    if len(newlist)!=0:
        return newlist[0]
    else:
        pass
ids=[]
for i in dfs:
    ids.append(str(idr(i)))
res = dict(zip(ids, dfs)) 
def clearning(datadict):
    temps=0
    framesofid=pd.DataFrame()
    data=[]
    for k,v in datadict.items():
        v.drop_duplicates(inplace=True)
        v.dropna(subset=[str(k)],inplace=True)
        v.sort_values(by=[k], inplace=True)
        if 'Year' in list(v.columns):
            v.drop(v[v.Year < 1980].index, inplace=True)
            v.drop(v[v.Year > 2016 ].index, inplace=True)
        temps+=1
        framesofid[str(k)]=v[str(k)].dropna()
    data.append(framesofid)
    data.append(list(datadict.values()))
    return data
datasets=clearning(res)[1]
idtable=clearning(res)[0] 
data=pd.read_csv('C:/Users/amine/OneDrive/Bureau/Dw Project/Data/final/facttable.csv')
data[['V of Pollution Country %','ValPol','IDCode']].dropna()
death=datasets[7]
death['total']=death[['Deaths - No access to handwashing facility ','Deaths - Smoking','Deaths - Secondhand smoke','Deaths - Unsafe water source ','Deaths - Household air pollution from solid fuels ','Deaths - Air pollution ','Deaths - Air pollution ','Deaths Outdoor air pollution']].sum(axis=1)
death=death.groupby(['Code']).mean()
a=death.loc[death['Deaths - No access to handwashing facility '] == death['Deaths - No access to handwashing facility '].max()]
a.index.tolist()[0]
dataco = datasets[2].merge(datasets[4], on=['Year','Code'],how='outer')
dataco = datasets[7][['Deaths - Air pollution ','Year','Code']].merge(dataco, on=['Year','Code'],how='outer')
dataco = datasets[0].merge(dataco, on=['Year','Code'],how='outer')
dataco = dataco.dropna()


# In[ ]:





# In[45]:


df=datasets[14]
fig1 = px.scatter_geo(df,locations="IDCode_SO",color="IDCode_SO",
                     hover_name="IDCode_SO",
                     size='GDP per capita',
                     
                     projection = "orthographic")

df = datasets[14]

fig2 = go.Figure(data=go.Choropleth(
    locations = df['IDCode_SO'],
    z = df['Life expectancy'],
    colorscale = 'Blues',
    autocolorscale=False,
    reversescale=True,
    marker_line_color='darkgray',
    marker_line_width=0.5,
    colorbar_tickprefix = 'age',
    colorbar_title = 'Life expectancy',
))

fig2.update_layout(
    title_text='Life',
    geo=dict(
        showframe=False,
        showcoastlines=False,
       
    ),

)
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import numpy as np

y_saving = [data['Deaths - No access to handwashing facility '].sum(),
            data['Deaths - Smoking'].sum(),
            data['Deaths - Secondhand smoke'].sum(),
            data['Deaths - Unsafe water source '].sum(),
            data['Deaths - Household air pollution from solid fuels '].sum(),
            data['Deaths - Air pollution '].sum(),
            data['Deaths Outdoor air pollution'].sum(),
            ]
y_net_worth = [data['Deaths - No access to handwashing facility '].sum(),
            data['Deaths - Smoking'].sum(),
            data['Deaths - Secondhand smoke'].sum(),
            data['Deaths - Unsafe water source '].sum(),
            data['Deaths - Household air pollution from solid fuels '].sum(),
            data['Deaths - Air pollution '].sum(),
            data['Deaths Outdoor air pollution'].sum(),
            ]
x = ['Deaths - No access to handwashing facility ',
     'Deaths - Smoking',
     'Deaths - Secondhand smoke',
     'Deaths - Unsafe water source ',
     'Deaths - Household air pollution from solid fuels ',
     'Deaths - Air pollution ',
     'Deaths Outdoor air pollution']


# Creating two subplots
fig3 = make_subplots(rows=1, cols=2, specs=[[{}, {}]], shared_xaxes=True,
                    shared_yaxes=False, vertical_spacing=0.001)

fig3.append_trace(go.Bar(
    x=y_saving,
    y=x,
    marker=dict(
        color='rgba(50, 171, 96, 0.6)',
        line=dict(
            color='rgba(50, 171, 96, 1.0)',
            width=1),
    ),
    name='principaux facteurs de risque de décès au monde',
    orientation='h',
), 1, 1)

fig3.append_trace(go.Scatter(
    x=y_net_worth, y=x,
    mode='lines+markers',
    line_color='rgb(128, 0, 128)',
    name='',
), 1, 2)

fig3.update_layout(
    title='Nombre de décès au monde par risque',
    yaxis=dict(
        showgrid=False,
        showline=False,
        showticklabels=True,
        domain=[0, 0.85],
    ),
    yaxis2=dict(
        showgrid=False,
        showline=True,
        showticklabels=False,
        linecolor='rgba(102, 102, 102, 0.8)',
        linewidth=2,
        domain=[0, 0.85],
    ),
    xaxis=dict(
        zeroline=False,
        showline=False,
        showticklabels=True,
        showgrid=True,
        domain=[0, 0.42],
    ),
    xaxis2=dict(
        zeroline=False,
        showline=False,
        showticklabels=True,
        showgrid=True,
        domain=[0.47, 1],
        side='top',
        dtick=25000,
    ),
    legend=dict(x=0.029, y=1.038, font_size=10),
    margin=dict(l=100, r=20, t=70, b=70),
    paper_bgcolor='rgb(248, 248, 255)',
    plot_bgcolor='rgb(248, 248, 255)',
)

annotations = []

y_s = np.round(y_saving, decimals=2)
y_nw = np.rint(y_net_worth)

# Adding labels
for ydn, yd, xd in zip(y_nw, y_s, x):

    # labeling the bar net worth
    annotations.append(dict(xref='x1', yref='y1',
                            y=xd, x=yd + 3,
                            text=str(yd) ,
                            font=dict(family='Arial', size=12,
                                      color='rgb(50, 171, 96)'),
                            showarrow=False))
# Source

fig3.update_layout(annotations=annotations)
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px

builder = FigureBuilder()
#builder.add(Line(x=dataco.Year,y=dataco['Gas co2'],name='Gas co2'),1,1)
builder.add(Line(x=dataco.Year,y=dataco['Coal co2'],name='Coal co2',line=dict(dash='dot')),1,2)
builder.add(Line(x=dataco.Year,y=dataco['Oil co2'],name='Oil co2'),2,1)
builder.add(Line(x=dataco.Year,y=dataco['Flaring co2'],name='Flaring co2',line=dict(dash='dot')),2,2)
import plotly.express as px
R=pd.read_csv('C:/Users/amine/OneDrive/Bureau/Dw Project/DW/sub-energy-fossil-renewables-nuclear.csv')
R['Code']=R['IDCode']
R=R.merge(datasets[13], on=['Year','Code'],how='outer')
R=R.dropna()
df = R

fig5 = px.scatter(df, x="Year", y="Solar (% sub energy)",
	         size="Renewables (% sub energy)", color="IDCode",
                 hover_name="Year", log_x=True, size_max=60)
fig6 = px.scatter(df, x="Year", y="Hydro (% sub energy)",
	         size="Renewables (% sub energy)", color="IDCode",
                 hover_name="Year", log_x=True, size_max=60)
fig7 = px.scatter(df, x="Year", y="Wind (% sub energy)",
	         size="Renewables (% sub energy)", color="IDCode",
                 hover_name="Year", log_x=True, size_max=60)

import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.express as px

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = JupyterDash(__name__, external_stylesheets=external_stylesheets)

df = pd.read_csv('C:/Users/amine/OneDrive/Bureau/Dw Project/DW//country_indicators .csv')

available_indicators = df['Indicator Name'].unique()

app.layout = html.Div([
    html.Div([

        html.Div([
            dcc.Dropdown(
                id='crossfilter-xaxis-column',
                options=[{'label': i, 'value': i} for i in available_indicators],
                value='Fertility rate, total (births per woman)'
            ),
            dcc.RadioItems(
                id='crossfilter-xaxis-type',
                options=[{'label': i, 'value': i} for i in ['Linear', 'Log']],
                value='Linear',
                labelStyle={'display': 'inline-block'}
            )
        ],
        style={'width': '49%', 'display': 'inline-block'}),

        html.Div([
            dcc.Dropdown(
                id='crossfilter-yaxis-column',
                options=[{'label': i, 'value': i} for i in available_indicators],
                value='Life expectancy at birth, total (years)'
            ),
            dcc.RadioItems(
                id='crossfilter-yaxis-type',
                options=[{'label': i, 'value': i} for i in ['Linear', 'Log']],
                value='Linear',
                labelStyle={'display': 'inline-block'}
            )
        ], style={'width': '49%', 'float': 'right', 'display': 'inline-block'})
    ], style={
        'borderBottom': 'thin lightgrey solid',
        'backgroundColor': 'rgb(250, 250, 250)',
        'padding': '10px 5px'
    }),

    html.Div([
        dcc.Graph(
            id='crossfilter-indicator-scatter',
            hoverData={'points': [{'customdata': 'World'}]}
        )
    ], style={'width': '49%', 'display': 'inline-block', 'padding': '0 20'}),
    html.Div([
        dcc.Graph(id='x-time-series'),
        dcc.Graph(id='y-time-series'),
    ], style={'display': 'inline-block', 'width': '49%'}),

    html.Div(dcc.Slider(
        id='crossfilter-year--slider',
        min=df['Year'].min(),
        max=df['Year'].max(),
        value=df['Year'].max(),
        marks={str(year): str(year) for year in df['Year'].unique()},
        step=None
    ), style={'width': '49%', 'padding': '0px 20px 20px 20px'})
])


@app.callback(
    dash.dependencies.Output('crossfilter-indicator-scatter', 'figure'),
    [dash.dependencies.Input('crossfilter-xaxis-column', 'value'),
     dash.dependencies.Input('crossfilter-yaxis-column', 'value'),
     dash.dependencies.Input('crossfilter-xaxis-type', 'value'),
     dash.dependencies.Input('crossfilter-yaxis-type', 'value'),
     dash.dependencies.Input('crossfilter-year--slider', 'value')])
def update_graph(xaxis_column_name, yaxis_column_name,
                 xaxis_type, yaxis_type,
                 year_value):
    dff = df[df['Year'] == year_value]

    fig = px.scatter(x=dff[dff['Indicator Name'] == xaxis_column_name]['Value'],
            y=dff[dff['Indicator Name'] == yaxis_column_name]['Value'],
            hover_name=dff[dff['Indicator Name'] == yaxis_column_name]['Country Name']
            )

    fig.update_traces(customdata=dff[dff['Indicator Name'] == yaxis_column_name]['Country Name'])

    fig.update_xaxes(title=xaxis_column_name, type='linear' if xaxis_type == 'Linear' else 'log')

    fig.update_yaxes(title=yaxis_column_name, type='linear' if yaxis_type == 'Linear' else 'log')

    fig.update_layout(margin={'l': 40, 'b': 40, 't': 10, 'r': 0}, hovermode='closest')

    return fig


def create_time_series(dff, axis_type, title):

    fig = px.scatter(dff, x='Year', y='Value')

    fig.update_traces(mode='lines+markers')

    fig.update_xaxes(showgrid=False)

    fig.update_yaxes(type='linear' if axis_type == 'Linear' else 'log')

    fig.add_annotation(x=0, y=0.85, xanchor='left', yanchor='bottom',
                       xref='paper', yref='paper', showarrow=False, align='left',
                       bgcolor='rgba(255, 255, 255, 0.5)', text=title)

    fig.update_layout(height=225, margin={'l': 20, 'b': 30, 'r': 10, 't': 10})

    return fig


@app.callback(
    dash.dependencies.Output('x-time-series', 'figure'),
    [dash.dependencies.Input('crossfilter-indicator-scatter', 'hoverData'),
     dash.dependencies.Input('crossfilter-xaxis-column', 'value'),
     dash.dependencies.Input('crossfilter-xaxis-type', 'value')])
def update_y_timeseries(hoverData, xaxis_column_name, axis_type):
    country_name = hoverData['points'][0]['customdata']
    dff = df[df['Country Name'] == country_name]
    dff = dff[dff['Indicator Name'] == xaxis_column_name]
    title = '<b>{}</b><br>{}'.format(country_name, xaxis_column_name)
    return create_time_series(dff, axis_type, title)


@app.callback(
    dash.dependencies.Output('y-time-series', 'figure'),
    [dash.dependencies.Input('crossfilter-indicator-scatter', 'hoverData'),
     dash.dependencies.Input('crossfilter-yaxis-column', 'value'),
     dash.dependencies.Input('crossfilter-yaxis-type', 'value')])
def update_x_timeseries(hoverData, yaxis_column_name, axis_type):
    dff = df[df['Country Name'] == hoverData['points'][0]['customdata']]
    dff = dff[dff['Indicator Name'] == yaxis_column_name]
    return create_time_series(dff, axis_type, yaxis_column_name)






#builder.update_layout(title='Co2 By sources').subplot()


# In[19]:


fig1.show()


# In[21]:


fig2.show()


# In[22]:


fig3.show()


# In[24]:


fig5.show()


# In[25]:


fig6.show()


# In[31]:


fig7


# In[32]:


builder.update_layout(title='Co2 By sources').subplot()


# In[47]:


if __name__ == '__main__':
    app.run_server(mode='inline',port=8051)


# In[36]:


dataco = datasets[2].merge(datasets[4], on=['Year','Code'],how='outer')
dataco = datasets[7][['Deaths - Air pollution ','Year','Code']].merge(dataco, on=['Year','Code'],how='outer')
dataco = datasets[0].merge(dataco, on=['Year','Code'],how='outer')


# In[40]:


import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px

df = dataco

app1 = JupyterDash(__name__)

app1.layout = html.Div([
    dcc.Graph(id="scatter-plot"),
    html.P("cooking with clean fuels:"),
    dcc.RangeSlider(
        id='range-slider',
        min=0, max=2.5, step=0.1,
        marks={0: '0 %', 2.5: '100 %'},
        value=[0.5, 2]
    ),
])

@app1.callback(
    Output("scatter-plot", "figure"), 
    [Input("range-slider", "value")])
def update_bar_chart(slider_range):
    low, high = slider_range
    mask = (df['Access to clean fuels cooking %'] > low) & (df['Access to clean fuels cooking %'] < high)
    fig = px.scatter(
        df[mask], x="Year", y="Annual CO2 emissions", 
        color="Code", size='Deaths - Air pollution ', 
        hover_data=['Access to clean fuels cooking %'])
    return fig

app1.run_server(mode='inline',port=8053)


# In[41]:


from IPython.display import HTML

HTML('''<script>
code_show=true; 
function code_toggle() {
 if (code_show){
 $('div.input').hide();
 } else {
 $('div.input').show();
 }
 code_show = !code_show
} 
$( document ).ready(code_toggle);
</script>
<form action="javascript:code_toggle()"><input type="submit" value="Click here to toggle on/off the raw code."></form>''')


# In[ ]:




