import dash
from dash import dcc, html, Input, Output
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, text
from dotenv import dotenv_values

# Cargar variables de entorno desde el archivo .env
config = dotenv_values('/home/chris/.env')

# Función para conectarse a la base de datos
def conectar_bd():
    db_host = config.get('DB_HOST_P')
    db_name = config.get('DB_NAME_P')
    db_user = config.get('DB_USER_P')
    db_password = config.get('DB_PASSWORD_P')

    if not all([db_host, db_name, db_user, db_password]):
        print("Falta una o más variables de entorno necesarias para la conexión a la base de datos.")
        return None

    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}/{db_name}"
    return create_engine(connection_string)

# Crear la conexión a la base de datos
engine = conectar_bd()

# Función para consultar datos de las geometrías
def obtener_geometrias(engine):
    query = """
    SELECT 
        p.cod_estacion AS "codigo de estacion", p.nombre AS "nombre de estacion", r.region AS "region", c.comuna AS "comuna", cd.nombre AS "cuenca DGA", s.nombre AS "sscuenca DGA", p.zona AS "zona", p.altitud AS "altitud (m.s.n.m)", ST_Transform(p.geometria, 4326) AS geometria,
        ST_Y(p.geometria) AS latitud, ST_X(p.geometria) AS longitud
    FROM datos_maestros.estacion_punto_monitoreo p
    INNER JOIN datos_maestros.dpa_comuna_subdere c ON p.id_comuna = c.id_dpa_comuna
    INNER JOIN datos_maestros.dga_subsub_cuenca s ON p.id_sscuenca = s.objectid
    INNER JOIN datos_maestros.dga_sub_cuenca sc ON s.id_scuenca = sc.objectid
    INNER JOIN datos_maestros.dga_cuenca cd ON sc.id_cuenca = cd.objectid
    INNER JOIN datos_maestros.dpa_provincia_subdere ps ON ps.objectid = c.id_provincia
    INNER JOIN datos_maestros.dpa_region_subdere r ON r.objectid = ps.id_region
    WHERE p.institucion = 'DMC' AND c.comuna = 'Antofagasta';
    """
    with engine.connect() as conn:
        geometrias_df = pd.read_sql_query(text(query), conn)
    return geometrias_df

# Función para consultar datos históricos
def obtener_datos_historicos(engine):
    query = """
    SELECT re.cod_estacion AS "codigo de estacion",
        to_char(re.fecha, 'YYYY-MM') AS "mes",
        to_char(re.fecha, 'TMMonth "de" YYYY') AS "mes_formateado",
        ROUND(re.temperatura::numeric, 2) AS "temperatura (°C)",
        ROUND(re.humedad_relativa::numeric, 1) AS "humedad relativa (%)",
        ROUND(re.presion::numeric, 2) AS "presión atmosferica (hPAS)",
        ROUND(re.direccion_viento::numeric, 2) AS "direccion viento (°)",
        ROUND(re.fuerza_viento::numeric, 2) AS "fuerza viento (kt)",
        ROUND(re.precipitacion::numeric, 2) AS "precipitación (mm)",
        ROUND(NULLIF(re.radiacion, 'NaN')::numeric, 2) AS "radiacion (W/m²)"
    FROM 
        medio_fisico.registro_monitoreo re
    INNER JOIN 
        datos_maestros.estacion_punto_monitoreo p ON p.cod_estacion = re.cod_estacion AND p.objectid = re.objectid
    INNER JOIN 
        datos_maestros.dpa_comuna_subdere c ON p.id_comuna = c.id_dpa_comuna
    WHERE 
        p.institucion = 'DMC' 
        AND c.comuna = 'Antofagasta' 
        AND to_char(re.fecha, 'YYYY') = '2023';
    """
    with engine.connect() as conn:
        datos_historicos_df = pd.read_sql_query(text(query), conn)
    return datos_historicos_df

# Obtener los datos
geometrias_df = obtener_geometrias(engine)
datos_historicos_df = obtener_datos_historicos(engine)

# Crear diccionario para mapear nombres de estación a códigos
estacion_a_codigo = geometrias_df.set_index('nombre de estacion')['codigo de estacion'].to_dict()

# Verificar columnas de los DataFrames
print("Columnas de geometrias_df:", geometrias_df.columns)
print("Columnas de datos_historicos_df:", datos_historicos_df.columns)

# Calcular el bounding box del mapa
min_lat = geometrias_df['latitud'].min()
max_lat = geometrias_df['latitud'].max()
min_lon = geometrias_df['longitud'].min()
max_lon = geometrias_df['longitud'].max()
bounds = [[min_lat, min_lon], [max_lat, max_lon]]

# Inicializar la aplicación Dash
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Diseño de la aplicación
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.Label("Selecciona una variable"),
            dcc.Dropdown(
                id='variable-dropdown',
                options=[
                    {'label': 'Temperatura (°C)', 'value': 'temperatura (°C)'},
                    {'label': 'Humedad Relativa (%)', 'value': 'humedad relativa (%)'},
                    {'label': 'Presión Atmosférica (hPAS)', 'value': 'presión atmosferica (hPAS)'},
                    {'label': 'Dirección del Viento (°)', 'value': 'direccion viento (°)'},
                    {'label': 'Fuerza del Viento (kt)', 'value': 'fuerza viento (kt)'},
                    {'label': 'Precipitación (mm)', 'value': 'precipitación (mm)'},
                    {'label': 'Radiación (W/m²)', 'value': 'radiacion (W/m²)'}
                ],
                value='temperatura (°C)'
            )
        ], width=3),
        dbc.Col([
            dcc.Graph(id='mapa-interactivo')
        ], width=9)
    ]),
    dbc.Row([
        dbc.Col([
            dcc.Graph(id='grafico-historico')
        ])
    ])
], fluid=True)

# Callback para actualizar el gráfico del mapa
@app.callback(
    Output('mapa-interactivo', 'figure'),
    Input('variable-dropdown', 'value')
)
def actualizar_mapa(variable):
    fig = px.scatter_mapbox(
        geometrias_df,
        lat='latitud',
        lon='longitud',
        hover_name='nombre de estacion',
        hover_data={'region': True, 'comuna': True, 'cuenca DGA': True, 'altitud (m.s.n.m)': True},
        zoom=6
    )
    fig.update_layout(mapbox_style="open-street-map")
    return fig

# Callback para actualizar el gráfico histórico
@app.callback(
    Output('grafico-historico', 'figure'),
    Input('variable-dropdown', 'value'),
    Input('mapa-interactivo', 'clickData')
)
def actualizar_grafico(variable, clickData):
    if clickData is not None:
        estacion_nombre = clickData['points'][0]['hovertext']
        estacion_codigo = estacion_a_codigo.get(estacion_nombre, None)
        print(f"Estación seleccionada: {estacion_nombre}, código: {estacion_codigo}")  # Verificar estación seleccionada
        if estacion_codigo is not None:
            df_filtrado = datos_historicos_df[datos_historicos_df['codigo de estacion'] == estacion_codigo]
            print(df_filtrado.head())  # Verificar datos filtrados
            fig = px.line(
                df_filtrado,
                x='mes',
                y=variable,
                title=f'{variable} en {estacion_nombre}'
            )
        else:
            fig = px.line(
                pd.DataFrame(columns=['mes', variable]),
                x='mes',
                y=variable,
                title='No se encontró la estación seleccionada'
            )
    else:
        fig = px.line(
            pd.DataFrame(columns=['mes', variable]),
            x='mes',
            y=variable,
            title='Selecciona una estación en el mapa'
        )
    return fig

if __name__ == "__main__":
    app.run_server(debug=True)