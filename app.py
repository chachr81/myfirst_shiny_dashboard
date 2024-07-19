from shiny import App, render, ui, reactive
import pandas as pd
import matplotlib.pyplot as plt
from ipyleaflet import Map, Marker, MarkerCluster, Popup
from sqlalchemy import create_engine
from dotenv import dotenv_values
from ipywidgets.embed import embed_minimal_html
import ipywidgets as widgets  # Importar ipywidgets

# Cargar variables de entorno desde el archivo .env
config = dotenv_values('/home/chris/.env')

# Función para conectarse a la base de datos
def conectar_bd():
    db_host = config.get('DB_HOST')
    db_name = config.get('DB_NAME')
    db_user = config.get('DB_USER')
    db_password = config.get('DB_PASSWORD')

    if not all([db_host, db_name, db_user, db_password]):
        print("Falta una o más variables de entorno necesarias para la conexión a la base de datos.")
        print(f"DB_HOST: {db_host}")
        print(f"DB_NAME: {db_name}")
        print(f"DB_USER: {db_user}")
        print(f"DB_PASSWORD: {db_password}")
        return None

    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}/{db_name}"
    return create_engine(connection_string)

# Crear la conexión a la base de datos
engine = conectar_bd()

# Función para consultar datos desde la base de datos
def fetch_data(query):
    if engine is None:
        return pd.DataFrame()
    with engine.connect() as connection:
        result = pd.read_sql(query, connection)
    return result

# Consulta para obtener los datos del mapa
map_query = """
SELECT p.objectid,
    p.cod_estacion,
    p.nombre,
    p.altitud,
    r.region,
    c.comuna,
    s.nombre AS sscuenca_dga,
    p.zona,
    st_transform(p.geometria, 4326) AS geometria,
    st_y(p.geometria) AS latitud,
    st_x(p.geometria) AS longitud,
    st_srid(p.geometria) AS cod_epsg
   FROM datos_maestros.estacion_punto_monitoreo p
     JOIN datos_maestros.dpa_comuna_subdere c ON p.id_comuna = c.id_dpa_comuna
     JOIN datos_maestros.dga_subsub_cuenca s ON p.id_sscuenca = s.objectid
     JOIN datos_maestros.dpa_provincia_subdere ps ON ps.objectid = c.id_provincia
     JOIN datos_maestros.dpa_region_subdere r ON r.objectid = ps.id_region
  WHERE p.institucion::text = 'DMC'::text;
"""

# Consulta para obtener los datos del gráfico
def fetch_graph_data(cod_estacion):
    graph_query = f"""
    SELECT r.region, c.comuna, s.nombre AS sscuenca_dga, p.cod_estacion, p.nombre AS nombre_estacion, p.zona, p.altitud as "altitud (m.s.n.m)", 
    ROUND(re.temperatura::numeric, 2) as "temperatura (°C)", re.humedad_relativa as "humedad_relativa (%)", re.presion as "presion_atmosferica (mbar)", 
    re.direccion_viento as "direccion_viento (°)", re.velocidad_viento as "fuerza_viento (kt)", re.precipitacion as "precipitación (mm)", 
    NULLIF(re.radiacion, 'NaN') AS "radiacion (W/m²)", re.fecha
    FROM datos_maestros.estacion_punto_monitoreo p
    INNER JOIN medio_fisico.registro_monitoreo re ON p.objectid = re.objectid
    INNER JOIN datos_maestros.dpa_comuna_subdere c ON p.id_comuna = c.id_dpa_comuna
    INNER JOIN datos_maestros.dga_subsub_cuenca s ON p.id_sscuenca = s.objectid
    INNER JOIN datos_maestros.dpa_provincia_subdere ps ON ps.objectid = c.id_provincia
    INNER JOIN datos_maestros.dpa_region_subdere r ON r.objectid = ps.id_region
    WHERE p.institucion = 'DMC' AND EXTRACT(YEAR FROM re.fecha) = 2023 AND r.objectid = 16 AND p.cod_estacion = '{cod_estacion}'
    ORDER BY p.cod_estacion ASC;
    """
    return fetch_data(graph_query)

# Obtener los datos para el mapa
map_data = fetch_data(map_query)

# Calcular el bounding box del mapa
min_lat = map_data['latitud'].min()
max_lat = map_data['latitud'].max()
min_lon = map_data['longitud'].min()
max_lon = map_data['longitud'].max()
bounds = [[min_lat, min_lon], [max_lat, max_lon]]

# Definir la interfaz de usuario (UI)
app_ui = ui.page_fluid(
    ui.layout_sidebar(
        ui.panel_sidebar(
            ui.input_slider("n", "Número de datos a mostrar", 1, 100, 10)
        ),
        ui.panel_main(
            ui.output_ui("map"),
            ui.output_plot("plot"),
            ui.output_table("table")
        )
    )
)

# Definir la función del servidor
def server(input, output, session):
    selected_station = reactive.Value(None)

    @output
    @render.ui
    def map():
        m = Map(zoom=6)  # Inicializar el mapa con un zoom arbitrario
        markers = []
        for idx, row in map_data.iterrows():
            marker = Marker(location=(row['latitud'], row['longitud']), draggable=False)
            marker.on_click(lambda event=None, idx=idx: selected_station.set(map_data.iloc[idx]['cod_estacion']))
            popup_content = widgets.HTML(value=f"""
                <b>Estación:</b> {row['nombre']}<br>
                <b>Región:</b> {row['region']}<br>
                <b>Comuna:</b> {row['comuna']}<br>
                <b>Cuenca:</b> {row['sscuenca_dga']}<br>
                <b>Altitud:</b> {row['altitud']} m.s.n.m.<br>
                <b>Zona:</b> {row['zona']}
            """)
            marker.popup = Popup(location=marker.location, child=popup_content, close_button=False)
            markers.append(marker)
        marker_cluster = MarkerCluster(markers=markers)
        m.add_layer(marker_cluster)
        m.fit_bounds(bounds)  # Ajustar el mapa al bounding box calculado
        html_file = '/tmp/map.html'
        embed_minimal_html(html_file, views=[m], title='Mapa')
        with open(html_file, 'r') as f:
            return ui.HTML(f.read())

    @output
    @render.plot
    def plot():
        plt.figure()
        if selected_station.get():
            graph_data = fetch_graph_data(selected_station.get())
            plt.plot(graph_data['fecha'], graph_data['temperatura (°C)'], 'o')
            plt.xlabel('Fecha')
            plt.ylabel('Temperatura (°C)')
            plt.title(f'Temperatura en {selected_station.get()}')
        else:
            plt.text(0.5, 0.5, 'Selecciona una estación en el mapa', horizontalalignment='center', verticalalignment='center')
        return plt.gcf()

    @output
    @render.table
    def table():
        if selected_station.get():
            return fetch_graph_data(selected_station.get())
        else:
            return pd.DataFrame()

# Crear el objeto de la aplicación
app = App(app_ui, server)

# Ejecutar la aplicación
if __name__ == "__main__":
    app.run()