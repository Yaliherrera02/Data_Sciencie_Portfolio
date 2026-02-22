#%%
import pandas as pd
import duckdb
#%%

#ENTREGABLE 4 a: cargar csv
ruta = 'enunciado_tablas/enunciado_'
equipos_crudo = pd.read_csv(ruta + 'equipos.csv')
jugador_crudo = pd.read_csv(ruta + 'jugadores.csv')
jugador_atributos_crudo = pd.read_csv(ruta + 'jugadores_atributos.csv')
liga_crudo = pd.read_csv(ruta + 'liga.csv')
paises_crudo = pd.read_csv(ruta + 'paises.csv')
partido_crudo = pd.read_csv (ruta + 'partidos.csv')


#%%%
#ENTREGABLE 4 b: cargar tablas para SQL
equipo_A=duckdb.sql('''
    SELECT DISTINCT a.team_api_id as ID_EQUIPO,
    a.team_long_name NOMBRE,
    c.name as PAIS
    FROM equipos_crudo a
    LEFT JOIN partido_crudo b
    on a.team_api_id = b.home_team_api_id
    INNER JOIN paises_crudo c
    on c.id=b.country_id
''')
equipo_A_df = equipo_A.df()
equipo_A_df.to_csv('equipo_A.csv',index=False)

equipo_B = duckdb.sql('''
    SELECT DISTINCT team_long_name as NOMBRE,
    team_short_name as ABREVIACION
    FROM equipos_crudo
''')    
equipo_B_df = equipo_B.df()
equipo_B_df.to_csv('equipo_B.csv',index=False)

equipo_C = duckdb.sql('''
    SELECT a.name as PAIS,
    b.name as LIGA
    FROM paises_crudo a
    JOIN liga_crudo b
    ON b.country_id = a.id
''')
equipo_C_df = equipo_C.df()
equipo_C_df.to_csv('equipo_C.csv',index=False)


jugador = duckdb.sql('''
    SELECT player_api_id as ID_JUGADOR,
    player_name as NOMBRE,
    birthday as FECHA_DE_NACIMIENTO,
    weight as PESO,
    height as ALTURA
    FROM jugador_crudo
''')
jugador_df = jugador.df()
jugador_df.to_csv('jugador.csv',index=False)

jugador_atributos = duckdb.sql('''
    SELECT player_api_id as ID_JUGADOR,
    date as FECHA_CALENDARIO,
    potential as POTENCIA,
    dribbling as DRIBBLING,
    FROM jugador_atributos_crudo
''')
jugador_atributos_df = jugador_atributos.df()
jugador_atributos_df.to_csv('jugador_atributos.csv',index=False)

temporada = duckdb.sql('''
    SELECT DISTINCT season as ID_TEMPORADA, season as TEMPORADA
    FROM partido_crudo
''')
temporada_df = temporada.df()
temporada_df.to_csv('temporada.csv',index=False)

partido = duckdb.sql('''
    SELECT  match_api_id as ID_PARTIDO,
    season as ID_TEMPORADA,
    stage as FECHA,
    home_team_api_id as ID_EQUIPO_LOCAL,
    away_team_api_id as ID_EQUIPO_VISITANTE,
    home_team_goal as GOLES_LOCAL,
    away_team_goal as GOLES_VISITANTE
    FROM partido_crudo
''')
partido_df = partido.df()
partido_df.to_csv('partido.csv',index=False)

disputan = duckdb.sql('''
    SELECT season ID_TEMPORADA, match_api_id as ID_PARTIDO
    FROM partido_crudo
''')
disputan_df = disputan.df()
disputan_df.to_csv('disputan.csv',index=False)

es_local_en = duckdb.sql('''
    SELECT home_team_api_id ID_EQUIPO, match_api_id ID_PARTIDO
    FROM partido_crudo
''')
es_local_en_df = es_local_en.df()
es_local_en_df.to_csv('es_local_en.csv',index=False)

es_visitante_en = duckdb.sql('''
    SELECT away_team_api_id ID_EQUIPO, match_api_id ID_PARTIDO
    FROM partido_crudo
''')
es_visitante_en_df = es_visitante_en.df()
es_visitante_en_df.to_csv('es_visitante_en.csv',index=False)

componen_plantel = duckdb.sql('''
    SELECT DISTINCT ID_JUGADOR, planteles.ID_TEMPORADA, ID_EQUIPO,
    case when FECHA < (fechas_liga/2)+1 then 1 else 2 end as SEMESTRE
    FROM
    (   --De partidos crudo levanto todos los jugadores que jugaron de local
        SELECT DISTINCT jugadores as ID_JUGADOR,
        season as ID_TEMPORADA,
        home_team_api_id ID_EQUIPO,
        stage as FECHA,
        league_id as LIGA
        FROM partido_crudo
        UNPIVOT
            (jugadores for jugador in
                  (home_player_1, home_player_2, home_player_3,
                home_player_4, home_player_5, home_player_6, home_player_7,
                home_player_8, home_player_9, home_player_10, home_player_11,
                  )
            )
        --group by all

        UNION
        --De partidos_crudo levanto todos los jugadores que jugaron de visitante
        SELECT DISTINCT jugadores as ID_JUGADOR,
        season as ID_TEMPORADA,
        away_team_api_id ID_EQUIPO,
        stage as FECHA,
        league_id as LIGA
        FROM partido_crudo
        UNPIVOT
              (jugadores for jugador in
                  (away_player_1, away_player_2, away_player_3, away_player_4,
                  away_player_5, away_player_6, away_player_7, away_player_8,
                  away_player_9, away_player_10, away_player_11
                  )
              )
        --GROUP BY all
    ) AS planteles
    LEFT JOIN
    (
        --Cantidad de fechas de las ligas por cada temporada
        select season as ID_TEMPORADA,
        league_id as LIGA,
        MAX(stage) as fechas_liga
        from partido_crudo
        group by all
    ) duracion_liga
    on planteles.ID_TEMPORADA=duracion_liga.ID_TEMPORADA
    and planteles.LIGA = duracion_liga.LIGA
    ORDER BY ALL
''')
componen_plantel_df = componen_plantel.df()
componen_plantel_df.to_csv('componen_plantel.csv',index=False)
# %%
