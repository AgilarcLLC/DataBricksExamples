# Databricks notebook source
import plotly.graph_objects as go
import plotly.io as pio

fig = go.Figure()

# Set axes properties
fig.update_xaxes(range=[0, 10], zeroline=False)
fig.update_yaxes(range=[0, 10])

# Add circles

fig.add_shape(type="rect",
    xref="x", yref="y",
    x0=0, y0=0,
    x1=10, y1=10,
    line=dict(
        color="Blue",
        width=3,
    ),
    fillcolor="Blue",
    layer="below"
),

fig.add_shape(type="circle",
    xref="x", yref="y",
    fillcolor="DarkBlue",
    x0=7, y0=7, x1=13, y1=13,
    line_color="DarkBlue",
    layer="below"
),

#Complicated
fig.add_shape(type="circle",
    xref="x", yref="y",
    fillcolor="LightBlue",
    x0=0, y0=3, x1=6, y1=-3,
    line_color="Black",
    layer="below"
),

fig.add_shape(type="circle",
    xref="x", yref="y",
    fillcolor="LightBlue",
    x0=-3, y0=6, x1=3, y1=-0,
    line_color="Black",
    layer="below"
),

fig.add_shape(type="rect",
    xref="x", yref="y",
    x0=0, y0=0,
    x1=3, y1=3,
    line=dict(
        color="Black",
        width=2,
    ),
    fillcolor="LightBlue",
    layer="below"
),

#Simple
fig.add_shape(type="circle",
    xref="x", yref="y",
    fillcolor="White",
    x0=-3, y0=3, x1=3, y1=-3,
    line_color="Black",
    layer="below"
),


# Create scatter trace of text labels
fig.add_trace(go.Scatter(
    x=[1,1.5,4.5],
    y=[1,4,1],
    text=["<b>Simple</b>", "<b>Complicated</b>", "<b>Complicated</b>"],
    mode="text",
    textfont=dict(
        family="Verdana",
        size=14,
        color="Black"
    )
)),

fig.add_trace(go.Scatter(
    x=[5,9],
    y=[5,9],
    text=["<b>Complex</b>", "<b>Chaos</b>"],
    mode="text",
    textfont=dict(
        family="Verdana",
        size=14,
        color="White"
    )
))

# Set figure size
fig.update_layout(width=600, height=600)
fig.update_xaxes(showgrid=False, showticklabels=False)
fig.update_yaxes(visible=False, showticklabels=False)
fig.update_layout(showlegend=False)
fig.show()

#Creates HTML page for chart
#pio.write_html(fig, file='index.html', auto_open=True)
