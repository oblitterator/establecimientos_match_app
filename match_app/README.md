# Match REFES — vinculador de padrones

App web local para cruzar cualquier padrón contra REFES por scoring múltiple.  
Backend en Python/Flask · Frontend HTML/JS vanilla · Sin dependencias de Node.

## Estructura

```
match_app/
├── app.py              ← servidor Flask (toda la lógica de matching)
├── requirements.txt
├── static/
│   └── index.html      ← interfaz web completa
├── uploads/            ← archivos subidos (se crea automáticamente)
└── outputs/            ← excels generados (se crea automáticamente)
```

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

Abrir en el browser: http://localhost:5000

## Flujo de uso

**Paso 1 — Archivos**  
Subís el archivo REFES y el archivo externo (.xlsx, .xls o .csv).  
Indicás si el domicilio viene en un campo o en dos campos separados.

**Paso 2 — Mapeo de columnas**  
Para cada campo relevante (ID, provincia, departamento, localidad, nombre, domicilio, CUIT)
elegís qué columna del archivo le corresponde. La app autodetecta nombres comunes.

**Paso 3 — Parámetros**  
- Umbrales de calidad (score bueno / revisar / dudoso)
- Si usar CUIT para agrupar primero (camino A)
- Prefijos a eliminar de nombres y domicilios
- Ponderaciones de cada campo (deben sumar 100%)

**Paso 4 — Resultados**  
Tablero con métricas, distribución por provincia, histograma de scores,
tabla filtrable y descarga del Excel de resultados.

## Lógica de matching

El matching corre en Python (app.py → `run_matching`).

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
- Nombre establecimiento: fuzzy (sin prefijo FARMACIA/FARM)
- Domicilio: fuzzy (sin prefijo AV/AVENIDA/CALLE)

## Campos esperados

### Fundamental (sin esto no funciona)
- `id`: identificador único por fila en cada archivo

### Importante (mejora significativamente el resultado)
- Códigos INDEC de provincia (2 dígitos) y departamento (3 dígitos)
- Nombre provincia y departamento (fallback si no hay código)
- Nombre localidad
- Nombre del establecimiento
- CUIT (activa el camino A)

### Opcional
- Domicilio / Calle y Número
- Latitud (`lat`) y Longitud (`lon`) — para ambos archivos; se propagan al resultado y al Excel de salida sin afectar el scoring
