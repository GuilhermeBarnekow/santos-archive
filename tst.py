import os
import glob
import pandas as pd
import json
import dash
import dash_bootstrap_components as dbc
from dash import dcc, html, Input, Output, dash_table
import plotly.express as px
import logging
import base64

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)

def load_data(json_folder='extracao_farmacia', pattern='farmacias_*.json'):
    """
    Carrega dados de arquivos JSON na pasta especificada.

    Args:
        json_folder (str): Nome da pasta onde os arquivos JSON estão localizados.
        pattern (str): Padrão de nomenclatura dos arquivos JSON.

    Returns:
        pd.DataFrame: DataFrame combinado com os dados carregados.
    """
    # Construir o caminho absoluto baseado no diretório do script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    json_folder_path = os.path.join(script_dir, json_folder)

    logging.info(f"Caminho absoluto da pasta JSON: {json_folder_path}")

    if not os.path.isdir(json_folder_path):
        logging.error(f"A pasta '{json_folder}' não existe no caminho '{script_dir}'.")
        return pd.DataFrame()

    all_files = glob.glob(os.path.join(json_folder_path, pattern))
    logging.info(f"Arquivos encontrados para carregar: {all_files}")

    if not all_files:
        logging.warning("Nenhum arquivo JSON encontrado com o padrão especificado.")

    df_list = []
    for file in all_files:
        try:
            with open(file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if not isinstance(data, list):
                    logging.error(f"O arquivo {file} não contém uma lista de objetos.")
                    continue
                if not data:
                    logging.warning(f"O arquivo {file} está vazio.")
                    continue
                df = pd.json_normalize(data)

                filename = os.path.splitext(os.path.basename(file))[0]
                parts = filename.split('_')
                if len(parts) >= 3:
                    # Exemplo: 'farmacias_sao_paulo_sp' -> 'sao paulo'
                    city = ' '.join(parts[1:-1])
                elif len(parts) == 2:
                    # Exemplo: 'farmacias_nova_era' -> 'nova era'
                    city = parts[1]
                else:
                    city = 'Unknown'
                df['City'] = city.lower()
                df_list.append(df)
                logging.info(f"Arquivo {file} carregado com sucesso. Registros: {len(df)}")
        except json.JSONDecodeError as e:
            logging.error(f"Erro ao decodificar JSON no arquivo {file}: {e}")
        except Exception as e:
            logging.error(f"Erro ao carregar o arquivo {file}: {e}")

    if df_list:
        combined_df = pd.concat(df_list, ignore_index=True)
        logging.info(f"Total de registros carregados: {len(combined_df)}")
        return combined_df
    else:
        logging.warning("Nenhum arquivo JSON foi carregado com sucesso.")
        return pd.DataFrame()

# Lista das 3 cidades de interesse
desired_cities = ['campinas', 'sao paulo', 'ribeirao preto']

# Carregar os dados
df = load_data()

# Verificar se o DataFrame está vazio
if df.empty:
    logging.error("Nenhum dado foi carregado. Verifique os arquivos JSON e suas estruturas.")
    raise ValueError("Nenhum dado foi carregado. Verifique os arquivos JSON e suas estruturas.")

# Converter 'Latitude' e 'Longitude' para numérico
df['Latitude'] = pd.to_numeric(df['Latitude'], errors='coerce')
df['Longitude'] = pd.to_numeric(df['Longitude'], errors='coerce')

# Remover linhas com valores NaN em 'Latitude' ou 'Longitude'
df = df.dropna(subset=['Latitude', 'Longitude'])

# Padronizar os nomes das cidades para letras minúsculas
df['City'] = df['City'].str.lower()

# Filtrar para manter apenas as cidades de interesse
df = df[df['City'].isin(desired_cities)]

# Verificar se o DataFrame não está vazio após a filtragem
if df.empty:
    logging.error("Nenhum dado foi encontrado para as cidades especificadas.")
    raise ValueError("Nenhum dado foi encontrado para as cidades especificadas.")

# Processar 'SocialLinks' se existir
if 'SocialLinks' in df.columns:
    df['SocialLinks'] = df['SocialLinks'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)

# Classificar o tamanho da empresa
def classify_company_size(user_ratings_total):
    if pd.isnull(user_ratings_total) or user_ratings_total == 0:
        return 'Pequena'
    elif user_ratings_total >= 100:
        return 'Grande'
    elif user_ratings_total >= 20:
        return 'Média'
    else:
        return 'Pequena'

if 'UserRatingsTotal' in df.columns:
    df['UserRatingsTotal'] = pd.to_numeric(df['UserRatingsTotal'], errors='coerce')
    df['CompanySize'] = df['UserRatingsTotal'].apply(classify_company_size)
else:
    df['CompanySize'] = 'Unknown'

# Logar as colunas do DataFrame
logging.info(f"Colunas do DataFrame: {df.columns.tolist()}")

# Inicializar o aplicativo Dash com um tema Bootstrap
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "Rede de Farmácias São João"

server = app.server

# Layout do Dashboard
app.layout = dbc.Container([
    # Header
    dbc.Row(
        dbc.Col(
            html.H1("Rede de Farmácias São João", className='text-center text-primary mb-4'),
            width=12
        )
    ),
    # Filtros e Mapa
    dbc.Row([
        # Sidebar de Filtros
        dbc.Col([
            html.H5("Filtros", className='text-primary'),
            html.Hr(),
            # Filtro de Cidade
            dbc.Form([
                dbc.Label("Cidade"),
                dcc.Dropdown(
                    id='filter-city',
                    options=[{'label': city.title(), 'value': city} for city in sorted(df['City'].unique())],
                    value=[],  # Iniciar sem seleção
                    multi=True,
                    placeholder="Selecione as cidades"
                )
            ], className='mb-4'),
            # Filtro de Tamanho da Empresa
            dbc.Form([
                dbc.Label("Tamanho da Empresa"),
                dcc.Dropdown(
                    id='filter-size',
                    options=[
                        {'label': 'Pequena', 'value': 'Pequena'},
                        {'label': 'Média', 'value': 'Média'},
                        {'label': 'Grande', 'value': 'Grande'}
                    ],
                    value=[],  # Iniciar sem seleção
                    multi=True,
                    placeholder="Selecione os tamanhos"
                )
            ], className='mb-4'),
            # Filtro de Bairro
            dbc.Form([
                dbc.Label("Bairro"),
                dcc.Dropdown(
                    id='filter-neighborhood',
                    options=[{'label': neighborhood, 'value': neighborhood} for neighborhood in sorted(df['Neighborhood'].unique())],
                    value=[],  # Iniciar sem seleção
                    multi=True,
                    placeholder="Selecione os bairros"
                )
            ], className='mb-4'),
            # Botão de Reset
            dbc.Button("Resetar Filtros", id='reset-filters', color='secondary', className='mt-2'),
        ], width=3, style={'backgroundColor': '#f8f9fa', 'padding': '20px'}),
        
        # Área Principal com os Gráficos
        dbc.Col([
            # Gráfico de Mapa
            dcc.Graph(id='map'),
            html.Hr(),
            # Gráfico de Barras por Cidade
            dcc.Graph(id='bar-chart'),
            html.Hr(),
            # Gráfico de Barras por Bairro
            dcc.Graph(id='bar-chart-neighborhood'),
        ], width=9)
    ]),
    # Tabela e Download
    dbc.Row([
        dbc.Col([
            html.H5("Detalhes das Farmácias", className='text-primary'),
            html.Hr(),
            dash_table.DataTable(
                id='table',
                columns=[{"name": i, "id": i} for i in df.columns],
                data=[],  # Iniciar sem dados; serão preenchidos pelo callback
                page_size=10,
                filter_action="native",
                sort_action="native",
                style_table={'overflowX': 'auto'},
                style_cell={
                    'minWidth': '150px', 'width': '150px', 'maxWidth': '300px',
                    'whiteSpace': 'normal',
                    'overflow': 'hidden',
                    'textOverflow': 'ellipsis',
                    'maxHeight': '100px',
                    'height': 'auto',
                    'backgroundColor': '#e9ecef',
                    'padding': '5px',
                },
                style_cell_conditional=[
                    {
                        'if': {'column_id': 'Address'},
                        'width': '300px',
                        'textAlign': 'left',
                    },
                    {
                        'if': {'column_id': 'SocialLinks'},
                        'width': '250px',
                        'textAlign': 'left',
                    },
                ],
                style_header={
                    'backgroundColor': '#343a40',
                    'color': 'white',
                    'fontWeight': 'bold'
                },
                style_data={
                    'backgroundColor': '#f8f9fa',
                    'color': 'black',
                    'lineHeight': '15px',
                },
                tooltip_data=[
                    {column: {'value': str(row[column]), 'type': 'markdown'} for column in df.columns}
                    for row in df.to_dict('records')
                ],
                tooltip_duration=None,
            ),
            # Botão de Download
            html.A(
                dbc.Button("Baixar Dados", color='success', className='mt-3'),
                id='download-button',
                download="farmacias_sao_joao.csv",
                href="",
                target="_blank",
            )
        ], width=12)
    ], className='mt-4')
], fluid=True)

# Callback para atualizar os gráficos e a tabela
@app.callback(
    [Output('map', 'figure'),
     Output('bar-chart', 'figure'),
     Output('bar-chart-neighborhood', 'figure'),
     Output('table', 'data')],
    [Input('filter-city', 'value'),
     Input('filter-size', 'value'),
     Input('filter-neighborhood', 'value')]
)
def update_dashboard(selected_cities, selected_sizes, selected_neighborhoods):
    """
    Atualiza os gráficos e a tabela com base nos filtros selecionados.

    Args:
        selected_cities (list): Lista de cidades selecionadas.
        selected_sizes (list): Lista de tamanhos de empresa selecionados.
        selected_neighborhoods (list): Lista de bairros selecionados.

    Returns:
        tuple: Figuras atualizadas e dados para a tabela.
    """
    # Iniciar com todos os dados
    filtered_df = df.copy()
    
    # Aplicar filtros
    if selected_cities:
        filtered_df = filtered_df[filtered_df['City'].isin(selected_cities)]
    
    if selected_sizes:
        filtered_df = filtered_df[filtered_df['CompanySize'].isin(selected_sizes)]
    
    if selected_neighborhoods:
        filtered_df = filtered_df[filtered_df['Neighborhood'].isin(selected_neighborhoods)]
    
    # Logar informações para depuração
    logging.info(f"Número de registros após filtragem: {len(filtered_df)}")
    logging.info(f"Colunas disponíveis: {filtered_df.columns.tolist()}")
    
    # Verificar se o DataFrame filtrado está vazio
    if filtered_df.empty:
        logging.warning("DataFrame filtrado está vazio. Retornando figuras vazias.")
        return {}, {}, {}, []
    
    # Converter 'Latitude' e 'Longitude' para numérico no DataFrame filtrado
    filtered_df['Latitude'] = pd.to_numeric(filtered_df['Latitude'], errors='coerce')
    filtered_df['Longitude'] = pd.to_numeric(filtered_df['Longitude'], errors='coerce')
    filtered_df = filtered_df.dropna(subset=['Latitude', 'Longitude'])
    
    # Mapear o centro do mapa com base nos dados filtrados
    center_lat = filtered_df['Latitude'].mean()
    center_lon = filtered_df['Longitude'].mean()
    
    # Determinar se nenhum filtro está aplicado
    no_filters_applied = not selected_cities and not selected_sizes and not selected_neighborhoods
    
    # Mapa de Pontos
    map_fig = px.scatter_mapbox(
        filtered_df,
        lat='Latitude',
        lon='Longitude',
        hover_name='Name',
        hover_data={'Address': True, 'Neighborhood': True, 'Rating': True},
        color_discrete_sequence=['blue'],
        height=500,
        title='Localização das Farmácias',
        zoom=10 if not no_filters_applied else 3,  # Ajuste de zoom padrão
        center={'lat': center_lat, 'lon': center_lon} if not no_filters_applied else {'lat': -15.7942, 'lon': -47.8822}  # Centro padrão do Brasil
    )
    
    map_fig.update_layout(
        mapbox_style="open-street-map",
        margin={"r":0,"t":50,"l":0,"b":0},
        title_font_size=20,
        title_x=0.5,
        title_y=0.95,
        title_pad=dict(t=10, b=10),
    )
    
    # Gráfico de Barras por Cidade
    city_counts = filtered_df['City'].value_counts().reset_index()
    city_counts.columns = ['City', 'count']
    
    bar_chart = px.bar(
        city_counts,
        x='City',
        y='count',
        labels={'City': 'Cidade', 'count': 'Número de Farmácias'},
        title='Número de Farmácias por Cidade',
        color='City',
        color_discrete_sequence=px.colors.sequential.Blues
    )
    bar_chart.update_layout(
        xaxis_title="Cidade",
        yaxis_title="Número de Farmácias",
        title_font_size=20,
        title_x=0.5,
        title_y=0.95,
        title_pad=dict(t=10, b=10),
    )
    
    # Gráfico de Barras por Bairro
    neighborhood_counts = filtered_df['Neighborhood'].value_counts().reset_index()
    neighborhood_counts.columns = ['Neighborhood', 'count']
    
    bar_chart_neighborhood = px.bar(
        neighborhood_counts,
        x='Neighborhood',
        y='count',
        labels={'Neighborhood': 'Bairro', 'count': 'Número de Farmácias'},
        title='Número de Farmácias por Bairro',
        color='Neighborhood',
        color_discrete_sequence=px.colors.sequential.Oranges
    )
    bar_chart_neighborhood.update_layout(
        xaxis_title="Bairro",
        yaxis_title="Número de Farmácias",
        title_font_size=20,
        title_x=0.5,
        title_y=0.95,
        title_pad=dict(t=10, b=10),
    )
    
    # Dados para a Tabela
    table_data = filtered_df.to_dict('records')
    
    return map_fig, bar_chart, bar_chart_neighborhood, table_data

# Callback para resetar filtros
@app.callback(
    [Output('filter-city', 'value'),
     Output('filter-size', 'value'),
     Output('filter-neighborhood', 'value')],
    [Input('reset-filters', 'n_clicks')]
)
def reset_filters(n_clicks):
    """
    Reseta os filtros quando o botão é clicado.

    Args:
        n_clicks (int): Número de vezes que o botão foi clicado.

    Returns:
        tuple: Valores vazios para todos os filtros.
    """
    if n_clicks:
        return [], [], []
    return dash.no_update

# Callback para download dos dados filtrados
@app.callback(
    Output('download-button', 'href'),
    [Input('table', 'data')],
    prevent_initial_call=True
)
def generate_csv_link(table_data):
    """
    Gera um link para download dos dados filtrados em formato CSV.

    Args:
        table_data (list): Dados da tabela filtrada.

    Returns:
        str: Link para download do arquivo CSV.
    """
    if not table_data:
        return ""
    dff = pd.DataFrame(table_data)
    csv_string = dff.to_csv(index=False, encoding='utf-8')
    csv_bytes = csv_string.encode()
    b64 = base64.b64encode(csv_bytes).decode()
    href = f'data:text/csv;base64,{b64}'
    return href
