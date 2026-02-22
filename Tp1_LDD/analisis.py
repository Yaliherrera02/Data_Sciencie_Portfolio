#Proyecto original: TP Grupal - Laboratorio de Datos (UBA)
# Refactorización individual: Alison Herrera
# Objetivo: Optimización de consultas SQL y automatización de visualizaciones.
#%%%
import pandas as pd
import duckdb
import matplotlib.pyplot as plt
from   matplotlib import ticker
import seaborn as sns
#%%
ruta = ''
equipo_A = pd.read_csv(ruta + 'equipo_A.csv')
equipo_B = pd.read_csv(ruta + 'equipo_B.csv')
equipo_C = pd.read_csv(ruta + 'equipo_C.csv')
es_local_en = pd.read_csv(ruta + 'es_local_en.csv')
es_visitante_en = pd.read_csv(ruta + 'es_visitante_en.csv')
jugador = pd.read_csv(ruta + 'jugador.csv')
jugador_atributos = pd.read_csv(ruta + 'jugador_atributos.csv')
partido = pd.read_csv(ruta + 'partido.csv')
temporada = pd.read_csv(ruta + 'temporada.csv')
componen_plantel = pd.read_csv(ruta + 'componen_plantel.csv')
disputan = pd.read_csv(ruta + 'disputan.csv')
#%%

# 1) ¿Cuál es el equipo con mayor cantidad de partidos ganados?
# Paso 1: Calculamos el total de victorias por equipo en Bélgica
victorias_belgica = duckdb.sql('''
    SELECT b.NOMBRE, count(*) as PARTIDOS_GANADOS 
    FROM (
        SELECT CASE 
            WHEN goles_local > goles_visitante THEN ID_EQUIPO_LOCAL 
            ELSE ID_EQUIPO_VISITANTE 
        END AS GANADOR
        FROM partido
        WHERE ID_TEMPORADA IN ('2009/2010','2010/2011','2011/2012','2012/2013')
        AND GOLES_LOCAL <> GOLES_VISITANTE
    ) AS Ganadores
    JOIN equipo_A b ON Ganadores.GANADOR = b.ID_EQUIPO
    WHERE PAIS = 'Belgium'
    GROUP BY b.NOMBRE
''')
# Buscamos a los que tengan el número más alto
ganador_final = duckdb.sql('''
    SELECT NOMBRE, PARTIDOS_GANADOS
    FROM victorias_belgica
    WHERE PARTIDOS_GANADOS = (SELECT MAX(PARTIDOS_GANADOS) FROM victorias_belgica)
''')

print("Resultado final")
print((ganador_final))

###############################################################################
#%%
#2 ¿Cuál es el equipo con mayor cantidad de partidos perdidos de cada año?

derrotas_belgica = '''SELECT 
        b.NOMBRE, 
        ID_TEMPORADA, 
        COUNT(*) AS PARTIDOS_PERDIDOS
    FROM (
        SELECT 
            ID_PARTIDO,
            ID_TEMPORADA,
            CASE 
                WHEN goles_local > goles_visitante THEN ID_EQUIPO_VISITANTE 
                ELSE ID_EQUIPO_LOCAL 
            END AS PERDEDOR
        FROM partido a
        WHERE ID_TEMPORADA IN ('2009/2010','2010/2011','2011/2012','2012/2013')
        AND GOLES_LOCAL <> GOLES_VISITANTE
    ) a
    JOIN equipo_A b ON a.PERDEDOR = b.ID_EQUIPO
    WHERE PAIS = 'Belgium'
    GROUP BY b.NOMBRE, ID_TEMPORADA
'''
perdedores_por_temporada = f'''SELECT 
    NOMBRE, 
    ID_TEMPORADA, 
    PARTIDOS_PERDIDOS
FROM (
    SELECT 
        NOMBRE, 
        ID_TEMPORADA, 
        PARTIDOS_PERDIDOS,
        ROW_NUMBER() OVER (PARTITION BY ID_TEMPORADA ORDER BY PARTIDOS_PERDIDOS DESC) AS RANK
    FROM ({derrotas_belgica})
) ranked_teams
WHERE RANK = 1
ORDER BY ID_TEMPORADA DESC
'''

print("los equipos que mas partidos perdieron en cada año son: ")
print(duckdb.sql(perdedores_por_temporada))

###############################################################################
#%%
#3 ¿Cuál es el equipo con mayor cantidad de partidos empatados en el último año?
# Anotamos los equipos que empataron
conteo_empates = duckdb.sql('''
    SELECT b.NOMBRE, COUNT(*) as TOTAL_EMPATES
    FROM (
        SELECT ID_EQUIPO_LOCAL as ID_EQUIPO FROM partido
        WHERE GOLES_LOCAL = GOLES_VISITANTE AND ID_TEMPORADA = '2012/2013'
        UNION ALL
        SELECT ID_EQUIPO_VISITANTE as ID_EQUIPO FROM partido
        WHERE GOLES_LOCAL = GOLES_VISITANTE AND ID_TEMPORADA = '2012/2013'
    ) AS tabla_empates
    JOIN equipo_A b ON tabla_empates.ID_EQUIPO = b.ID_EQUIPO
    WHERE b.PAIS = 'Belgium'
    GROUP BY b.NOMBRE
''')
#Nos quedamos con el que mas partidos empato
resultado = duckdb.sql('''
    SELECT NOMBRE, TOTAL_EMPATES
    FROM conteo_empates
    WHERE TOTAL_EMPATES = (SELECT MAX(TOTAL_EMPATES) FROM conteo_empates)
''')
print("el equipo con mayor cantidad de partidos empatados en el último año es:  ")
print((resultado))
###############################################################################
#%%
#4 ¿Cuál es el equipo con mayor cantidad de goles a favor?
goles_por_equipo = duckdb.sql('''
    SELECT e.NOMBRE, SUM(Goles) as TOTAL_GOLES
    FROM (
        -- Goles cuando el equipo fue local
        SELECT ID_EQUIPO_LOCAL as ID_EQUIPO, GOLES_LOCAL as Goles
        FROM partido
        WHERE ID_TEMPORADA IN ('2009/2010', '2010/2011', '2011/2012', '2012/2013')
        
        UNION ALL
        
        -- Goles cuando el equipo fue visitante
        SELECT ID_EQUIPO_VISITANTE as ID_EQUIPO, GOLES_VISITANTE as Goles
        FROM partido
        WHERE ID_TEMPORADA IN ('2009/2010', '2010/2011', '2011/2012', '2012/2013')
    ) AS tabla_goles
    JOIN equipo_A e ON tabla_goles.ID_EQUIPO = e.ID_EQUIPO
    WHERE e.PAIS = 'Belgium'
    GROUP BY e.NOMBRE
''')
# Buscamos al equipo con el número más alto de goles
resultado_final = duckdb.sql('''
    SELECT NOMBRE, TOTAL_GOLES
    FROM goles_por_equipo
    WHERE TOTAL_GOLES = (SELECT MAX(TOTAL_GOLES) FROM goles_por_equipo)
''')

print("El equipo con mayor cantidad de goles a favor es:")
print(resultado_final)
###############################################################################
#%%%

#5 ¿Cuál es el equipo con mayor diferencia de goles?
tabla_diferencias = duckdb.sql('''
    SELECT 
        e.NOMBRE, 
        SUM(GF) as TOTAL_FAVOR, 
        SUM(GC) as TOTAL_CONTRA,
        (SUM(GF) - SUM(GC)) as DIFERENCIA
    FROM (
        -- Bloque Local: 
        SELECT 
            ID_EQUIPO_LOCAL as ID_EQUIPO, 
            GOLES_LOCAL as GF, 
            GOLES_VISITANTE as GC
        FROM partido
        WHERE ID_TEMPORADA IN ('2009/2010','2010/2011','2011/2012','2012/2013')
        UNION ALL

        -- Bloque Visitante: 
        SELECT 
            ID_EQUIPO_VISITANTE as ID_EQUIPO, 
            GOLES_VISITANTE as GF, 
            GOLES_LOCAL as GC
        FROM partido
        WHERE ID_TEMPORADA IN ('2009/2010','2010/2011','2011/2012','2012/2013')                       
    ) AS tabla
    INNER JOIN equipo_A e ON tabla.ID_EQUIPO = e.ID_EQUIPO
    WHERE e.PAIS = 'Belgium'
    GROUP BY e.NOMBRE
''')
#  Buscamos al o los ganadores de la estadística
resultado_final = duckdb.sql('''
    SELECT NOMBRE, TOTAL_FAVOR, TOTAL_CONTRA, DIFERENCIA
    FROM tabla_diferencias
    WHERE DIFERENCIA = (SELECT MAX(DIFERENCIA) FROM tabla_diferencias)
''')

print("El equipo con mayor diferencia de goles es:")
print(resultado_final)

###############################################################################
#%%
#6 ¿Cuántos jugadores tuvo durante el período de tiempo seleccionado cada equipo en su plantel?

# Obtenemos la lista única de jugadores por temporada
jugadores_unicos = duckdb.sql('''
    SELECT DISTINCT ID_EQUIPO, ID_TEMPORADA, ID_JUGADOR
    FROM componen_plantel
    WHERE ID_TEMPORADA IN ('2009/2010', '2010/2011', '2011/2012', '2012/2013')
''')
# Ahora contamos sobre la tabla ya limpia
consulta_final = duckdb.sql('''
    SELECT 
        e.Nombre, 
        ju.ID_TEMPORADA AS PERIODO, 
        COUNT(ju.ID_JUGADOR) AS Cantidad_de_jugadores
    FROM jugadores_unicos AS ju
    INNER JOIN equipo_A AS e ON ju.ID_EQUIPO = e.ID_EQUIPO
    WHERE e.PAIS = 'Belgium'
    GROUP BY e.Nombre, ju.ID_TEMPORADA
    ORDER BY e.Nombre, ju.ID_TEMPORADA
''')
print("la cantidad de jugadores que tuvieron los equipos en el periodo de tiempo seleccionado es:")
print(consulta_final)

#%%
# ¿Cuáles son los jugadores que más partido ganó su equipo?
# i) parte: obtener la suma de partidos ganado por equipo en cada semestre y temporada
query = '''
    --Cantidad de partidos ganados por equipo en cada temporada y semestre:

    SELECT a.ID_TEMPORADA,
    b.ID_EQUIPO AS ID_EQUIPO_GANADOR,
    b.NOMBRE as NOMBRE_EQUIPO_GANADOR,

    --el siguiente case lo puedo hacer porque todas las temporadas se juegan anualmente (filtre Belgica 2013/2014)
    --semestre me indica en que equipo jugó en cada mitad de temporada, lo cual es importante para contabilizar partidos ganados
    case when a.FECHA < (DURACION_LIGA/2)+1 then 1 else 2 end as SEMESTRE,
    count(*) as PARTIDOS_GANADOS
    FROM
    (SELECT ID_TEMPORADA,
    FECHA,
    case when GOLES_LOCAL>GOLES_VISITANTE then ID_EQUIPO_LOCAL
            when GOLES_LOCAL<GOLES_VISITANTE then ID_EQUIPO_VISITANTE
            end as EQUIPO_GANADOR,
    FROM partido
    WHERE GOLES_LOCAL <> GOLES_VISITANTE) a

    --A continuacion los siguientes joins son solo para filtrar por la liga asignada
    JOIN
    equipo_A b on b.ID_EQUIPO = a.EQUIPO_GANADOR
    JOIN
    equipo_C c on c.PAIS = b.PAIS
    and c.LIGA = 'Belgium Jupiler League'

    --A continuacion el siguiente join es para obtener la duracion de la liga en cada temporada
    JOIN
    (
    SELECT ID_TEMPORADA,
    MAX(FECHA) AS DURACION_LIGA
    FROM partido as a

    --A continuacion los siguientes joins son solo para filtrar por la liga asignada
    JOIN equipo_A b
    on b.ID_EQUIPO = a.ID_EQUIPO_LOCAL
    JOIN
    equipo_C c on c.PAIS = b.PAIS
    and c.LIGA = 'Belgium Jupiler League'
    Where ID_TEMPORADA in ('2009/2010','2010/2011','2011/2012','2012/2013')
    group by a.ID_TEMPORADA
    ) d
    on a.ID_TEMPORADA = d.ID_TEMPORADA
    GROUP BY ALL
    ORDER BY ALL
'''
# ii): obtener la suma historica de los partidos ganado por jugador
query = f'''
--Cantidad de partidos ganados por jugador en todas las temporadas y semestres:
SELECT NOMBRE, sum(PARTIDOS_GANADOS) as PARTIDOS_GANADOS
FROM (SELECT a.*,b.ID_JUGADOR, c.NOMBRE FROM ({query}) as a
JOIN componen_plantel as bs
on a.ID_TEMPORADA = b.ID_TEMPORADA
and a.ID_EQUIPO_GANADOR = b.ID_EQUIPO
and a.SEMESTRE=b.SEMESTRE
JOIN jugador c
on c.ID_JUGADOR = b.ID_JUGADOR
)
group by 1
order by 2 desc limit 3
'''
print("los jugadores que más partidos ganó su equipo son: ")

############################################################################################

# ¿Cuál es el jugador que estuvo en más equipos?
query = '''SELECT a.ID_JUGADOR,b.NOMBRE,COUNT(*) as numero_equipos
FROM (select distinct id_jugador,id_equipo from componen_plantel Where ID_TEMPORADA in ('2009/2010','2010/2011','2011/2012','2012/2013')) a
JOIN jugador b
on b.ID_JUGADOR = a.ID_JUGADOR
group by 1,2
order by 3 desc
limit 1
'''
print("el jugador que estuvo en mas equipos es: ")
print(duckdb.sql(query)) #RESPUESTA

############################################################################################

# ¿Cuál es el jugador que menor variación de potencia ha tenido a lo largo de los años?

# i) como sabemos que un jugador puede haber estado en mas de una liga hay que segmentar jugadores_atributo
#por semestre y id_temporada para filtrar solo por los que juegan en BELGICA

query=''' SELECT e.NOMBRE,
    max(potencia_maxima) as MAXIMO_HISTORICO,
    MIN(potencia_minima) as MINIMO_HISTORICO
    FROM
    (--De jugador_atributos me genero temporada y semestre para joinear con componen_plantel y filtrar por liga
        SELECT
        ID_JUGADOR,
        CASE
            WHEN CAST(SPLIT(fecha_calendario, '-')[2] AS INTEGER) >= 7 THEN 1
            ELSE 2
        END AS semestre_temporada,
        CASE
            WHEN CAST(SPLIT(fecha_calendario, '-')[2] AS INTEGER) >= 7
                THEN CONCAT(SPLIT(fecha_calendario, '-')[1], '/', CAST(CAST(SPLIT(fecha_calendario, '-')[1] AS INTEGER) + 1 AS VARCHAR))
            ELSE CONCAT(CAST(CAST(SPLIT(fecha_calendario, '-')[1] AS INTEGER) - 1 AS VARCHAR), '/', SPLIT(fecha_calendario, '-')[1])
        END AS ID_TEMPORADA,
        --maxima y minima potencia por temporada y semestre
        MAX(POTENCIA) as potencia_maxima,
        MIN(POTENCIA) as potencia_minima
        FROM jugador_atributos
        group by 1,2,3
        order by all
     ) a
    JOIN componen_plantel b
    on b.ID_TEMPORADA = A.ID_TEMPORADA
    and b.SEMESTRE = a.SEMESTRE_TEMPORADA
    and b.ID_JUGADOR = a.ID_JUGADOR
    JOIN jugador e
    on e.ID_JUGADOR = a.ID_JUGADOR

    --A continuacion los siguientes joins son solo para filtrar por la liga asignada
    JOIN equipo_A c
    on c.ID_EQUIPO = b.ID_EQUIPO
    JOIN equipo_C d
    on d.PAIS = c.PAIS
    and d.LIGA = 'Belgium Jupiler League'
    Where b.ID_TEMPORADA in ('2009/2010','2010/2011','2011/2012','2012/2013')
    group by all
    order by all
'''
# ii)
query = f'''select NOMBRE, MAXIMO_HISTORICO-MINIMO_HISTORICO AS DIFERENCIA_POTENCIA_HISTORICA
from ({query})
ORDER BY 2
limit 1 
'''
print("el jugador que menor variación de potencia ha tenido a lo largo de los años es: ")
print(duckdb.sql(query)) #RESPUESTA

##############################################################################

# ¿Hay algún equipo que haya sido a la vez el más goleador y el que tenga mayor valor de alguno
# de los atributos (considerando la suma de todos los jugadores)?

# i): veo el equipo mas goleador
query_0 = '''
			  SELECT ID_PARTIDO, equipos,
              case when equipo = 'ID_EQUIPO_LOCAL' then GOLES_LOCAL else GOLES_VISITANTE end as goles
			  FROM partido
			  UNPIVOT
			  (equipos for equipo in
						(ID_EQUIPO_LOCAL,ID_EQUIPO_VISITANTE)
			) 
          where ID_TEMPORADA in ('2009/2010','2010/2011','2011/2012','2012/2013')
      order by 1
'''
query_1 = f'''
SELECT EQUIPOS as ID_EQUIPO,c.NOMBRE, sum(goles) as goles_a_favor,
ROW_NUMBER() OVER (ORDER BY GOLES_A_FAVOR DESC) AS RANKING
        FROM
		({query_0}) b

        --A continuacion los siguientes joins son solo para filtrar por la liga asignada
        JOIN equipo_A c
        on c.ID_EQUIPO = b.EQUIPOS
        JOIN equipo_C d
        on d.PAIS = c.PAIS
        and d.LIGA = 'Belgium Jupiler League'
        group by 1,2
        order by 3

'''
# ii): veo el equipo con mayor suma de atributos (potencia y dribbling)
query_2 = '''
SELECT ID_EQUIPO,NOMBRE, sum(potencia_total_equipo+dribbling_total_equipo) as puntaje_historico,
ROW_NUMBER() OVER (ORDER BY puntaje_historico DESC) AS RANKING
FROM

(SELECT b.ID_EQUIPO,
c.NOMBRE,
    --Ahora con los promedios de los jugadores por cada temporada de la liga Belga,
    --hago la suma de ellos por temporada
    sum(potencia_promedio) as potencia_total_equipo,
    sum(dribbling_promedio) as dribbling_total_equipo
    FROM
    (--De jugador_atributos me genero temporada y semestre para joinear con componen_plantel y filtrar por liga
        SELECT
        ID_JUGADOR,
        
        CASE
            WHEN CAST(SPLIT(fecha_calendario, '-')[2] AS INTEGER) >= 7 THEN 1
            ELSE 2
        END AS semestre_temporada,
        CASE
            WHEN CAST(SPLIT(fecha_calendario, '-')[2] AS INTEGER) >= 7
                THEN CONCAT(SPLIT(fecha_calendario, '-')[1], '/', CAST(CAST(SPLIT(fecha_calendario, '-')[1] AS INTEGER) + 1 AS VARCHAR))
            ELSE CONCAT(CAST(CAST(SPLIT(fecha_calendario, '-')[1] AS INTEGER) - 1 AS VARCHAR), '/', SPLIT(fecha_calendario, '-')[1])
        END AS ID_TEMPORADA,
        --promedio de las mediciones de potencia y dribbling por temporada
        mean(POTENCIA) as potencia_promedio,
        mean(dribbling) as dribbling_promedio
        FROM jugador_atributos
        group by 1,2,3
     ) a
    JOIN componen_plantel b
    on b.ID_TEMPORADA = A.ID_TEMPORADA
    and b.SEMESTRE = a.SEMESTRE_TEMPORADA
    and b.ID_JUGADOR = a.ID_JUGADOR
    
    --A continuacion los siguientes joins son solo para filtrar por la liga asignada
    JOIN equipo_A c
    on c.ID_EQUIPO = b.ID_EQUIPO
    JOIN equipo_C d
    on d.PAIS = c.PAIS
    and d.LIGA = 'Belgium Jupiler League'
    where a.ID_TEMPORADA in ('2009/2010','2010/2011','2011/2012','2012/2013')
    group by all)
    GROUP BY ALL
'''
query = f''' select a.NOMBRE,a.goles_a_favor,b.puntaje_historico,b.RANKING 
from ({query_1}) a
join ({query_2}) b
on b.ranking = a.ranking
and b.ID_EQUIPO = a.ID_EQUIPO

'''
print("el equipo mas goleador y con plantel mas talentoso es: ")
print(duckdb.sql(query))
print("RTA: no hay equipo historicamente que sea el mas goleador y el mas talentoso al mismo tiempo ")

##############################################################################

#%%
# 1) Graficar la cantidad de goles a favor y en contra de cada equipo a lo largo de los años que elijan
    #ARMO TABLA PARA LUEGO HACER LA VISUALIZACION.
    #OBTENGO LOS GOLES HECHOS POR TEMPORADAS DE CADA EQUIPO.

goles_para_grafico = duckdb.sql('''
    SELECT 
        e.NOMBRE, 
        t.ID_TEMPORADA, 
        SUM(t.Goles_H) as Goles_totales_hechos, 
        SUM(t.Goles_R) as Goles_totales_recibidos
    FROM (
        -- Cuando es Local: Goles_LOCAL son hechos, Goles_VISITANTE son recibidos
        SELECT ID_TEMPORADA, ID_EQUIPO_LOCAL as ID_EQUIPO, GOLES_LOCAL as Goles_H, GOLES_VISITANTE as Goles_R
        FROM partido
        
        UNION ALL
        
        -- Cuando es Visitante: Goles_VISITANTE son hechos, Goles_LOCAL son recibidos
        SELECT ID_TEMPORADA, ID_EQUIPO_VISITANTE as ID_EQUIPO, GOLES_VISITANTE as Goles_H, GOLES_LOCAL as Goles_R
        FROM partido
    ) AS t
    JOIN equipo_A e ON t.ID_EQUIPO = e.ID_EQUIPO
    WHERE e.PAIS = 'Belgium' 
      AND t.ID_TEMPORADA IN ('2009/2010', '2010/2011', '2011/2012', '2012/2013')
    GROUP BY e.NOMBRE, t.ID_TEMPORADA
''')

df_goles = goles_para_grafico.df()

# Ordenamos por temporada para que el eje X no se mezcle
df_goles = df_goles.sort_values(['ID_TEMPORADA', 'NOMBRE'])

# 2. Separación de equipos
conteos = df_goles.groupby('NOMBRE')['ID_TEMPORADA'].nunique()
equipos_completos = conteos[conteos == 4].index

df_completos = df_goles[df_goles['NOMBRE'].isin(equipos_completos)]
df_incompletos = df_goles[~df_goles['NOMBRE'].isin(equipos_completos)]

def dibujar(df, columna, titulo):
    plt.figure(figsize=(9, 6))
    
    # Usamos el mismo orden de colores de Matplotlib
    for equipo in df['NOMBRE'].unique():
        data = df[df['NOMBRE'] == equipo]
        # Agregamos 'zorder' y 'markersize' para que los puntos sueltos se vean bien
        plt.plot(data['ID_TEMPORADA'], data[columna], 
                 marker='o', linestyle='-', linewidth=1, label=equipo)
    
    plt.title(titulo, fontsize=10)
    plt.xlabel('Temporada')
    plt.ylabel('Goles hechos' if 'hechos' in columna.lower() else 'Goles recibidos')
    
    # La leyenda afuera a la derecha como la de tu compañero
    plt.legend(title='Equipos', loc='upper left', bbox_to_anchor=(1, 1))
    plt.grid(True, linestyle=':', alpha=0.6)
    plt.tight_layout()
    plt.show()

# 4. GENERAMOS LOS 4 GRÁFICOS
dibujar(df_completos, 'Goles_totales_hechos', 'GOLES HECHOS POR EQUIPOS QUE JUGARON TODAS LAS TEMPORADAS')
dibujar(df_completos, 'Goles_totales_recibidos', 'GOLES RECIBIDOS POR EQUIPOS QUE JUGARON TODAS LAS TEMPORADAS')
dibujar(df_incompletos, 'Goles_totales_hechos', 'GOLES HECHOS POR EQUIPOS QUE NO JUGARON TODAS LAS TEMPORADAS')
dibujar(df_incompletos, 'Goles_totales_recibidos', 'GOLES RECIBIDOS POR EQUIPOS QUE NO JUGARON TODAS LAS TEMPORADAS')
# 2) Graficar el promedio de gol de los equipos a lo largo de los años que elijan 
#%%
###############################################################################
#ARMO MI TABLA PARA LUEGO HACER LA VISUALIZACION.


# Consulta para unificar y calcular promedios
df_promedios = duckdb.sql('''
    SELECT 
        e.NOMBRE,
        COUNT(DISTINCT t.ID_TEMPORADA) as Temporadas_Jugadas,
        CAST(SUM(t.Goles) AS FLOAT) / COUNT(t.Goles) as Promedio_de_gol
    FROM (
        SELECT ID_TEMPORADA, ID_EQUIPO_LOCAL AS Equipo, GOLES_LOCAL AS Goles FROM partido
        UNION ALL
        SELECT ID_TEMPORADA, ID_EQUIPO_VISITANTE AS Equipo, GOLES_VISITANTE AS Goles FROM partido
    ) AS t
    JOIN equipo_A e ON t.Equipo = e.ID_EQUIPO
    WHERE e.PAIS = 'Belgium' 
      AND t.ID_TEMPORADA IN ('2009/2010', '2010/2011', '2011/2012', '2012/2013')
    GROUP BY e.NOMBRE
    ORDER BY Temporadas_Jugadas DESC, Promedio_de_gol DESC
''').df()

# Creamos una lista de colores: si Temporadas == 4 va aqua, si no deeppink
colores = []
for cant in df_promedios['Temporadas_Jugadas']:
    if cant == 4:
        colores.append('aqua')
    else:
        colores.append('deeppink')

# Graficamos
fig, ax = plt.subplots(figsize=(10, 6))

ax.bar(df_promedios['NOMBRE'], df_promedios['Promedio_de_gol'], color=colores)

ax.set_title('PROMEDIO DE GOL DE LA LIGA (2009-2013)', fontweight='bold')
ax.set_ylabel('Promedio de gol')
plt.xticks(rotation=90)

# Agregamos las leyendas para que se entienda el gráfico
plt.figtext(0.15, -0.05, '■ Equipos que participaron en todas las temporadas en Aqua', color='darkcyan', fontsize=10)
plt.figtext(0.15, -0.10, '■ Equipos que participaron en alguna de ellas en deepoink', color='deeppink', fontsize=10)

plt.tight_layout()
plt.show()
#############################################################
#%%
# 3) Graficar la diferencia de goles convertidos jugando de local vs visitante a lo largo del tiempo
#Goles como local
goles_local = duckdb.sql('''
    SELECT
        b.ID_TEMPORADA AS TEMPORADA,
        b.ID_EQUIPO_LOCAL AS ID_EQUIPO,
        a.NOMBRE,
        SUM(b.GOLES_LOCAL) AS GOLES_A_FAVOR_LOCAL,
        count(ID_PARTIDO) as cantidad_partidos
    FROM partido b
    INNER JOIN equipo_A a ON a.ID_EQUIPO = b.ID_EQUIPO_LOCAL
    WHERE a.PAIS = 'Belgium'
    AND b.ID_TEMPORADA IN ('2009/2010', '2010/2011', '2011/2012', '2012/2013')
    GROUP BY b.ID_TEMPORADA, b.ID_EQUIPO_LOCAL, a.NOMBRE
''')
print(goles_local)

#-- Goles como visitante
goles_visitante = duckdb.sql('''
    SELECT
        b.ID_TEMPORADA AS TEMPORADA,
        b.ID_EQUIPO_VISITANTE AS ID_EQUIPO,
        a.NOMBRE,
        SUM(b.GOLES_VISITANTE) AS GOLES_A_FAVOR_VISITANTE,
        count(ID_PARTIDO) as cantidad_partidos
    FROM partido b
    INNER JOIN equipo_A a ON a.ID_EQUIPO = b.ID_EQUIPO_VISITANTE
    WHERE a.PAIS = 'Belgium'
    AND b.ID_TEMPORADA IN ('2009/2010', '2010/2011', '2011/2012', '2012/2013')
    GROUP BY b.ID_TEMPORADA, b.ID_EQUIPO_VISITANTE, a.NOMBRE
''')
goles_totales_por_equipo = duckdb.sql('''
    SELECT
        a.TEMPORADA AS TEMPORADA,
        a.ID_EQUIPO AS ID_EQUIPO,
        a.NOMBRE AS NOMBRE,
        a.GOLES_A_FAVOR_LOCAL,
        a.cantidad_partidos as CANTIDAD_PARTIDOS_LOCAL,
        b.GOLES_A_FAVOR_VISITANTE,
        b.cantidad_partidos as CANTIDAD_PARTIDOS_VISITANTE
    FROM goles_local a
    INNER JOIN goles_visitante b ON a.ID_EQUIPO = b.ID_EQUIPO AND a.TEMPORADA = b.TEMPORADA
    ORDER BY TEMPORADA, ID_EQUIPO
''')
print("los goles totales por equipo son ",goles_totales_por_equipo)

# %%
import duckdb
import pandas as pd
import matplotlib.pyplot as plt

# Cargar datos
goles_totales_por_equipo_df = goles_totales_por_equipo.df()
# Crear la figura y los ejes del gráfico para los goles como local
fig, ax = plt.subplots(figsize=(12, 8))  # Tamaño del gráfico

# Obtener los equipos únicos para graficar una línea por equipo
equipos = goles_totales_por_equipo_df['NOMBRE'].unique()

# Configuración de fuente
plt.rcParams['font.family'] = 'sans-serif'

# Graficar una línea por cada equipo (goles como local)
for equipo in equipos:
    # Filtrar los datos del equipo
    data_equipo = goles_totales_por_equipo_df[goles_totales_por_equipo_df['NOMBRE'] == equipo]

    # Graficar los goles como local por temporada
    ax.plot(data_equipo['TEMPORADA'],
            data_equipo['GOLES_A_FAVOR_LOCAL'],
            marker='.',
            linestyle='-',
            linewidth=1.2,
            label=equipo)

# Agregar título y etiquetas
ax.set_title('Goles como Local por equipo en Bélgica')
ax.set_xlabel('Temporada')
ax.set_ylabel('Goles como Local')

# Configurar los ticks y etiquetas del eje X
ax.set_xticks(goles_totales_por_equipo_df['TEMPORADA'].unique())
ax.set_xticklabels(goles_totales_por_equipo_df['TEMPORADA'].unique(), rotation=45)

# Mostrar leyenda fuera del gráfico
ax.legend(loc='upper left', bbox_to_anchor=(1, 1))

# Mostrar el gráfico
plt.tight_layout()
plt.show()

# Crear la figura y los ejes del gráfico para los goles como visitante
fig, ax = plt.subplots(figsize=(12, 8))  # Tamaño del gráfico

# Graficar una línea por cada equipo (goles como visitante)
for equipo in equipos:
    # Filtrar los datos del equipo
    data_equipo = goles_totales_por_equipo_df[goles_totales_por_equipo_df['NOMBRE'] == equipo]

    # Graficar los goles como visitante por temporada
    ax.plot(data_equipo['TEMPORADA'],
            data_equipo['GOLES_A_FAVOR_VISITANTE'],
            marker='.',
            linestyle='-',
            linewidth=1.2,
            label=equipo)

# Agregar título y etiquetas
ax.set_title('Goles como Visitante por equipo en Bélgica')
ax.set_xlabel('Temporada')
ax.set_ylabel('Goles como Visitante')

# Configurar los ticks y etiquetas del eje X
ax.set_xticks(goles_totales_por_equipo_df['TEMPORADA'].unique())
ax.set_xticklabels(goles_totales_por_equipo_df['TEMPORADA'].unique(), rotation=45)

# Mostrar leyenda fuera del gráfico
ax.legend(loc='upper left', bbox_to_anchor=(1, 1))

# Mostrar el gráfico
plt.tight_layout()
plt.show()
# 4) Graficar el número de goles convertidos por cada equipo en función de la suma de todos sus atributos.
# Cargar los datos desde el archivo CSV
#%%

equipos_de_belgica = duckdb.sql('''
    SELECT DISTINCT a.ID_EQUIPO, a.NOMBRE
    FROM equipo_A a
    INNER JOIN equipo_C b ON a.PAIS = b.PAIS
    WHERE b.PAIS='Belgium'
''')

jugadores_liga_belga = duckdb.sql (''' Select distinct a.ID_JUGADOR, a.ID_TEMPORADA, a.ID_EQUIPO, b.NOMBRE
                        FROM componen_plantel a
                        INNER JOIN equipos_de_belgica b
                        ON a.ID_EQUIPO=b.ID_EQUIPO
                        where a.ID_TEMPORADA IN ('2009/2010', '2010/2011', '2011/2012', '2012/2013')
                     ''')

atributos_jugadores_belgica=duckdb.sql('''SELECT a.ID_EQUIPO,b.* from (SELECT DISTINCT ID_JUGADOR,
                                CASE
                                 WHEN FECHA_CALENDARIO LIKE '2009%' THEN '2009/2010'
                                 WHEN FECHA_CALENDARIO LIKE '2010%' THEN '2010/2011'
                                 WHEN FECHA_CALENDARIO LIKE '2011%' THEN '2011/2012'
                                 WHEN FECHA_CALENDARIO LIKE '2012%' THEN '2012/2013'
                                 END AS ID_TEMPORADA,
                                 mean(POTENCIA) as PROMEDIO_POTENCIA,
                                 mean(DRIBBLING) as PROMEDIO_DRIBBLING
                                FROM jugador_atributos
                                WHERE FECHA_CALENDARIO LIKE '2009%'
                                OR FECHA_CALENDARIO LIKE '2010%'
                                OR FECHA_CALENDARIO LIKE '2011%'
                                OR FECHA_CALENDARIO LIKE '2012%'
                                group by all) b
                                INNER JOIN jugadores_liga_belga a
                                ON b.ID_JUGADOR = a.ID_JUGADOR
                                and b.ID_TEMPORADA = a.ID_TEMPORADA


                              ''')
print("la tabla atributos_jugadores_belgica es : ",atributos_jugadores_belgica)
#%%
partidos_de_belgica = duckdb.sql('''
    SELECT ID_PARTIDO, ID_TEMPORADA, ID_EQUIPO_LOCAL as ID_EQUIPO
    FROM partido b
    JOIN equipos_de_belgica a ON (b.ID_EQUIPO_LOCAL = a.ID_EQUIPO)
    WHERE ID_TEMPORADA IN ('2009/2010', '2010/2011', '2011/2012', '2012/2013')
    UNION ALL
    SELECT ID_PARTIDO, ID_TEMPORADA, ID_EQUIPO_VISITANTE as ID_EQUIPO
    FROM partido b
    JOIN equipos_de_belgica ON (b.ID_EQUIPO_VISITANTE = ID_EQUIPO)
    WHERE ID_TEMPORADA IN ('2009/2010', '2010/2011', '2011/2012', '2012/2013')
    ''')
print("los partidos_de_belgica son : ",partidos_de_belgica)

#%%%
partidos_por_equipo = duckdb.sql('''
    SELECT ID_EQUIPO,count(ID_PARTIDO) as CANTIDAD_PARTIDOS
    FROM partidos_de_belgica
    group BY ID_EQUIPO
    ORDER BY ID_EQUIPO
    ''')
print("LOS PARTIDOS POR EQUIPO SON, ",partidos_por_equipo)

#%%%
metricas_por_equipo_belga = duckdb.sql('''
    SELECT
        a.ID_EQUIPO,
        SUM(a.PROMEDIO_POTENCIA+a.PROMEDIO_DRIBBLING) as prom_suma_atributos
    FROM
        atributos_jugadores_belgica a

    GROUP BY
       a.ID_EQUIPO
    ORDER BY
        a.ID_EQUIPO;
''')

print("la tabla metricas_por_equipo_belga es : ",metricas_por_equipo_belga)
#%%
promedio_metricas_por_equipo_belga=duckdb.sql('''
    SELECT a.ID_EQUIPO, a.prom_suma_atributos as PROMEDIO_ATRIBUTOS, count(b.ID_PARTIDO) as CANTIDAD_PARTIDOS
    FROM metricas_por_equipo_belga a
    join partidos_de_belgica b
    on a.ID_EQUIPO = b.ID_EQUIPO
    group by all
    order by a.ID_EQUIPO

''')
print("el promedio_metricas_por_equipo_belga es : ",promedio_metricas_por_equipo_belga)
# %%
goles_en_total_con_promedio = duckdb.sql('''
    SELECT
        l.ID_EQUIPO,
        l.NOMBRE,
        SUM(l.GOLES_A_FAVOR_LOCAL + l.GOLES_A_FAVOR_VISITANTE) AS GOLES_TOTALES_A_FAVOR
        ,sum(cantidad_partidos_local+cantidad_partidos_visitante) as PARTIDOS_JUGADOS
        ,(goles_totales_a_favor/partidos_jugados) as PROMEDIO_DE_GOL
    FROM goles_totales_por_equipo l
    GROUP BY l.ID_EQUIPO, l.NOMBRE
    ORDER BY l.ID_EQUIPO
''')


print("los goles en total son ",goles_en_total_con_promedio)
#%%=
promedio_metricas_goles_por_equipo = duckdb.sql('''
    SELECT a.ID_EQUIPO,
           a.PROMEDIO_ATRIBUTOS,
            ROUND (b.PROMEDIO_DE_GOL,2) as PROMEDIO_DE_GOL
    FROM promedio_metricas_por_equipo_belga a
    INNER JOIN goles_en_total_con_promedio b ON a.ID_EQUIPO = b.ID_EQUIPO
    GROUP BY a.ID_EQUIPO, a.PROMEDIO_ATRIBUTOS, b.PROMEDIO_DE_GOL
    ORDER BY a.ID_EQUIPO
''')

print(promedio_metricas_goles_por_equipo)
#%%=

import pandas as pd
import matplotlib.pyplot as plt

# Carga de los datos
promedio_metrica_goles_por_equipo_df = promedio_metricas_goles_por_equipo.df()
# Configuración de la figura y ejes
fig, ax = plt.subplots()

# Personalización de la fuente
plt.rcParams['font.family'] = 'sans-serif'

# Gráfico de dispersión
ax.scatter(data=promedio_metrica_goles_por_equipo_df,
           x='PROMEDIO_DE_GOL',       # Eje X
           y='PROMEDIO_ATRIBUTOS',     # Eje Y
           s=8,                        # Tamaño de los puntos
           color='magenta')            # Color de los puntos

# Configuración del gráfico
ax.set_title('Promedio de goles Totales a Favor vs. Promedio de Métricas')  # Título del gráfico
ax.set_xlabel('Promedio de goles Totales a Favor', fontsize='medium')       # Etiqueta eje X
ax.set_ylabel('Promedio de suma de Métricas', fontsize='medium')            # Etiqueta eje Y

# Ajuste del límite del eje X para incluir todos los puntos
max_x_value = max(promedio_metrica_goles_por_equipo_df['PROMEDIO_DE_GOL'])
ax.set_xlim(0, max(max_x_value + 0.5, 2))  # Ajusta el límite a 2.5 o al valor máximo + 0.5


# Muestra el gráfico
plt.show()

# %%