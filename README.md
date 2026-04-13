# Match REFES / Georef INDEC

Aplicación web local con dos módulos independientes:

1. **Match REFES** — cruza cualquier padrón de establecimientos de salud contra REFES, el dato maestro nacional, usando scoring múltiple por campo.
2. **Georef INDEC** — normaliza domicilios de cualquier archivo consultando la API oficial Georef del gobierno argentino, y agrega códigos INDEC de provincia/departamento más coordenadas geográficas.

Backend en Python/Flask · Frontend HTML/JS vanilla · Sin dependencias de Node.

---

## Objetivo

En el sistema de salud argentino conviven múltiples actores que mantienen sus propios registros de establecimientos: obras sociales, prepagas, colegios profesionales, ministerios provinciales, financiadores y prestadores de distinta naturaleza. Cada uno carga los datos con criterios propios — nombres abreviados, domicilios sin normalizar, sin código de establecimiento oficial.

REFES (Registro Federal de Establecimientos de Salud) es el dato maestro: el padrón oficial del Ministerio de Salud de la Nación que identifica de forma unívoca cada establecimiento sanitario del país con un ID propio y datos georreferenciados.

El objetivo de esta herramienta es **vincular cualquiera de esos padrones sectoriales contra REFES** para determinar a qué establecimiento REFES corresponde cada registro, y así obtener el ID oficial. Ese ID permite después consolidar información de fuentes distintas sobre el mismo establecimiento.

Ejemplos de padrones que se pueden cruzar:
- Padrón de farmacias de un colegio farmacéutico provincial (ej. COFA)
- Cartilla de prestadores de una obra social o prepaga
- Registro de efectores de un ministerio provincial de salud
- Cualquier otro listado de establecimientos con nombre, domicilio y/o CUIT

## ¿Qué son los códigos INDEC?

El INDEC (Instituto Nacional de Estadística y Censos) define una codificación estándar para la división político-territorial argentina:

- **Provincia**: código de 2 dígitos (ej. `06` = Buenos Aires, `02` = CABA, `14` = Córdoba)
- **Departamento / Partido**: código de 3 dígitos dentro de la provincia (ej. `028` = Bahía Blanca)
- El código completo de un departamento es de 5 dígitos (provincia + departamento)

Usar estos códigos en lugar de los nombres de texto mejora significativamente la precisión del matching porque evita variaciones ortográficas.

---

## Estructura del proyecto

```
/
├── app.py                  ← servidor Flask (matching + endpoints Georef)
├── georef_normalizer.py    ← módulo de normalización vía API Georef
├── requirements.txt
├── static/
│   └── index.html          ← interfaz web completa
├── uploads/                ← archivos subidos (se crea automáticamente)
└── outputs/                ← excels generados (se crea automáticamente)
```

---

## Instalación y uso

### Opción A — con entorno virtual (recomendado)

```bash
# Crear y activar venv (solo la primera vez)
python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # Mac/Linux

# Instalar dependencias
pip install -r requirements.txt

# Correr el servidor
python app.py
```

### Opción B — sin venv

```bash
# Las dependencias se instalan automáticamente al arrancar
python app.py
```

Abrir en el browser: http://localhost:5001

---

## Módulo 1: Match REFES

### ¿Qué hace?

Recibe dos archivos (el padrón REFES y un padrón externo) y asigna a cada establecimiento del padrón externo el ID REFES más probable, calculando un score de similitud por múltiples campos.

### Flujo

**Paso 1 — Archivos**
Se sube el archivo REFES y el archivo externo (.xlsx, .xls o .csv).
Se indica si el domicilio viene en un campo unificado o en dos campos separados (calle / número).

**Paso 2 — Mapeo de columnas**
Para cada campo relevante (ID, provincia, departamento, localidad, nombre, domicilio, CUIT)
se selecciona qué columna del archivo le corresponde. La app autodetecta nombres comunes.

**Paso 3 — Parámetros**
- Umbrales de calidad (score bueno / revisar / dudoso)
- Si usar CUIT para agrupar primero (camino A)
- Prefijos a eliminar de nombres y domicilios
- Ponderaciones de cada campo (deben sumar 100%)

**Paso 4 — Resultados**
Tablero con métricas, distribución por provincia, histograma de scores,
tabla filtrable y descarga del Excel de resultados.

### Lógica de matching

**Camino A — por CUIT** (si está activado y hay CUIT compartido):
Agrupa por CUIT, calcula scores dentro del grupo, asigna greedy 1-a-1.

**Camino B — por departamento INDEC:**
Agrupa por par (código provincia, código departamento), calcula scores,
asigna greedy 1-a-1. Si el archivo externo no tiene códigos INDEC,
usa comparación de texto para provincia y departamento.

**Score por campo** (0–100 cada uno, suma ponderada):
- Provincia: código INDEC exacto si disponible, texto fuzzy si no
- Departamento: ídem
- Localidad: fuzzy token set ratio
- Nombre establecimiento: fuzzy (sin prefijo FARMACIA/FARM; números romanos normalizados a arábigos)
- Domicilio: fuzzy (sin prefijo AV/AVENIDA/CALLE; separadores espacio y punto)

**Bonuses sobre el score final:**
- +15 pts si coinciden los códigos INDEC de provincia y departamento exactos
- +10 pts si el domicilio normalizado coincide exactamente

El total se capa en 100.

### Campos esperados

**Fundamental** (sin esto no funciona)
- `id`: identificador único por fila en cada archivo

**Importante** (mejora significativamente el resultado)
- Códigos INDEC de provincia (2 dígitos) y departamento (3 dígitos)
- Nombre de provincia y departamento (se usan como fallback si no hay código)
- Nombre de localidad
- Nombre del establecimiento
- CUIT (activa el camino A)

**Opcional**
- Domicilio / Calle y Número
- Latitud (`lat`) y Longitud (`lon`) — se propagan al Excel de salida sin afectar el scoring

---

## Módulo 2: Georef INDEC

### ¿Qué hace?

Toma cualquier archivo con columnas de domicilio y consulta la [API Georef](https://georef-ar-api.readthedocs.io/es/latest/) del gobierno argentino para normalizar las direcciones. El resultado es el mismo archivo con columnas nuevas que contienen los códigos INDEC oficiales y, cuando es posible, las coordenadas geográficas (latitud/longitud).

Este módulo es independiente del matching: sirve para enriquecer cualquier padrón antes de usarlo, o simplemente para normalizar domicilios.

### Flujo

**Paso 1 — Subir archivo**
Se sube el archivo a enriquecer (.xlsx, .xls o .csv).

**Paso 2 — Mapear columnas**
Se indica qué columna corresponde a cada campo. Todos son opcionales, pero conviene mapear al menos provincia y domicilio para obtener buenos resultados:

| Campo | Descripción |
|---|---|
| Provincia | Nombre de la provincia |
| Departamento / Partido | Nombre del departamento o partido |
| Localidad | Nombre de la localidad |
| Domicilio (combinado) | Calle + número en un solo campo |
| Calle | Solo el nombre de la calle (si vienen separados) |
| Número | Solo el número de puerta (si vienen separados) |

**Paso 3 — Procesar**
La app consulta la API Georef en paralelo (múltiples workers configurables).
Se muestra una barra de progreso mientras se procesan las filas.

**Paso 4 — Descargar**
Se descarga un Excel con las columnas originales más las columnas nuevas.

### Columnas que agrega al archivo

| Columna | Descripción |
|---|---|
| `provincia_normalizada` | Nombre oficial de la provincia según INDEC |
| `id_provincia_indec` | Código INDEC de provincia (2 dígitos) |
| `departamento_normalizado` | Nombre oficial del departamento según INDEC |
| `id_departamento_indec` | Código INDEC de departamento (5 dígitos) |
| `domicilio_normalizado` | Dirección tal como la devuelve Georef |
| `latitud` | Latitud decimal de la dirección geocodificada |
| `longitud` | Longitud decimal de la dirección geocodificada |
| `provincia_error` | Descripción del error si no se pudo normalizar la provincia |
| `departamento_error` | Ídem para departamento |
| `domicilio_error` | Ídem para domicilio / geocodificación |

### Notas

- El departamento se busca primero por nombre directo en el índice de departamentos INDEC. Si no hay resultado (por ejemplo, porque el archivo usa el nombre de la ciudad capital en lugar del nombre del departamento, como "San Miguel de Tucumán" en lugar de "Capital"), se hace un segundo intento buscando el valor como localidad y extrayendo el departamento al que pertenece.
- No todas las provincias tienen datos de altura (número de puerta) en Georef. En esos casos se recuperan provincia y departamento, pero no coordenadas.
- La API Georef es pública y gratuita. El módulo incluye reintentos automáticos con backoff exponencial ante errores de red o límites de tasa.
