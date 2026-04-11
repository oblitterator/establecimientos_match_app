"""
match_app — backend Flask para el vinculador REFES
Correr con: python app.py
Requiere: flask pandas openpyxl
"""
import subprocess, sys

def _ensure(*pkgs):
    for pkg in pkgs:
        try:
            __import__(pkg.split('[')[0].replace('-', '_'))
        except ImportError:
            print(f'Instalando {pkg}...')
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', pkg, '--quiet'])

_ensure('flask', 'pandas', 'openpyxl')

import os, re, uuid, unicodedata
from pathlib import Path
import pandas as pd
from flask import Flask, request, jsonify, send_file, send_from_directory

app = Flask(__name__, static_folder='static', static_url_path='')

UPLOAD_DIR = Path('uploads')
OUTPUT_DIR = Path('outputs')
UPLOAD_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

ALLOWED = {'.xlsx', '.xls', '.csv'}

# ─────────────────────────────────────────────────────────────────────
# UTILIDADES
# ─────────────────────────────────────────────────────────────────────

def read_file(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext == '.csv':
        for enc in ['utf-8', 'latin-1', 'iso-8859-1']:
            try:
                return pd.read_csv(path, dtype=str, encoding=enc).fillna('')
            except UnicodeDecodeError:
                continue
    return pd.read_excel(path, dtype=str).fillna('')


def norm(s: str) -> str:
    if not s or str(s).strip() in ('nan', 'None', ''):
        return ''
    s = str(s).upper().strip()
    s = unicodedata.normalize('NFD', s)
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    s = re.sub(r'[^\w\s]', ' ', s)
    return re.sub(r'\s+', ' ', s).strip()


def strip_prefix(s: str, words: list) -> str:
    for w in words:
        s = re.sub(r'^' + re.escape(w) + r'\s+', '', s)
    return s.strip()


def to_indec(v, n: int):
    try:
        return str(int(float(v))).zfill(n)
    except (ValueError, TypeError):
        return None


def clean_cuit(s) -> str | None:
    if not s:
        return None
    c = re.sub(r'[^0-9]', '', str(s))
    return c if len(c) >= 10 else None


def token_set_ratio(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    sa, sb = set(a.split()), set(b.split())
    inter = len(sa & sb)
    union = len(sa | sb)
    return round(inter / union * 100, 1) if union else 0.0


def exact_code(a, b) -> float:
    if not a or not b:
        return 0.0
    return 100.0 if str(a) == str(b) else 0.0


# ─────────────────────────────────────────────────────────────────────
# SCORING
# ─────────────────────────────────────────────────────────────────────

def score_pair(ext_row: dict, ref_row: dict, cfg: dict) -> dict:
    cm_e = cfg['col_ext']
    cm_r = cfg['col_refes']
    name_pfx = cfg['name_prefixes']
    addr_pfx = cfg['addr_prefixes']
    W = cfg['weights']

    def gf(row, col):
        return row.get(col, '') if col else ''

    # Provincia
    ep = to_indec(gf(ext_row, cm_e.get('prov_indec')), 2)
    rp = to_indec(gf(ref_row, cm_r.get('prov_indec')), 2)
    if ep and rp:
        s_prov, pm = exact_code(ep, rp), 'codigo'
    else:
        s_prov, pm = token_set_ratio(
            norm(gf(ext_row, cm_e.get('prov_text'))),
            norm(gf(ref_row, cm_r.get('prov_text')))
        ), 'texto'

    # Departamento
    ed = to_indec(gf(ext_row, cm_e.get('dept_indec')), 3)
    rd = to_indec(gf(ref_row, cm_r.get('dept_indec')), 3)
    if ed and rd:
        s_dept, dm = exact_code(ed, rd), 'codigo'
    else:
        s_dept, dm = token_set_ratio(
            norm(gf(ext_row, cm_e.get('dept_text'))),
            norm(gf(ref_row, cm_r.get('dept_text')))
        ), 'texto'

    # Localidad
    s_loc = token_set_ratio(
        norm(gf(ext_row, cm_e.get('loc'))),
        norm(gf(ref_row, cm_r.get('loc')))
    )

    # Nombre
    nom_e = strip_prefix(norm(gf(ext_row, cm_e.get('name'))), name_pfx)
    nom_r = strip_prefix(norm(gf(ref_row, cm_r.get('name'))), name_pfx)
    s_nom = token_set_ratio(nom_e, nom_r)

    # Domicilio
    if cfg.get('ext_addr_type') == 'split':
        dom_e = norm(gf(ext_row, cm_e.get('addr'))) + ' ' + norm(gf(ext_row, cm_e.get('num')))
    else:
        dom_e = norm(gf(ext_row, cm_e.get('addr')))

    if cfg.get('refes_addr_type') == 'split':
        dom_r = norm(gf(ref_row, cm_r.get('addr'))) + ' ' + norm(gf(ref_row, cm_r.get('num')))
    else:
        dom_r = norm(gf(ref_row, cm_r.get('addr')))

    s_dom = token_set_ratio(
        strip_prefix(dom_e.strip(), addr_pfx),
        strip_prefix(dom_r.strip(), addr_pfx)
    )

    total = (
        s_prov * W['PROV'] +
        s_dept * W['DEPT'] +
        s_loc  * W['LOC']  +
        s_nom  * W['NOM']  +
        s_dom  * W['DOM']
    ) / 100

    return {
        'score_provincia':    round(s_prov, 1),
        'score_departamento': round(s_dept, 1),
        'score_localidad':    round(s_loc,  1),
        'score_nombre':       round(s_nom,  1),
        'score_domicilio':    round(s_dom,  1),
        'score_total':        round(total,  1),
        'prov_method': pm,
        'dept_method': dm,
    }


def greedy_match(ext_list: list, ref_list: list, cfg: dict, id_col_ext: str, id_col_ref: str) -> dict:
    """
    Retorna dict { ext_id: {'ref': ref_row, 'sc': score_dict} }
    Asignación 1-a-1 greedy. Si hay más ext que ref, comparten la mejor ref disponible.
    """
    pairs = []
    for e in ext_list:
        for r in ref_list:
            sc = score_pair(e, r, cfg)
            pairs.append((e[id_col_ext], r[id_col_ref], sc, e, r))

    pairs.sort(key=lambda x: -x[2]['score_total'])

    used_e, used_r, matched = set(), set(), {}
    for eid, rid, sc, e, r in pairs:
        if eid not in used_e and rid not in used_r:
            matched[eid] = {'ref': r, 'sc': sc}
            used_e.add(eid)
            used_r.add(rid)

    for eid, rid, sc, e, r in pairs:
        if eid not in matched:
            matched[eid] = {'ref': r, 'sc': sc}

    return matched


def run_matching(ext_df: pd.DataFrame, ref_df: pd.DataFrame, cfg: dict) -> list:
    cm_e = cfg['col_ext']
    cm_r = cfg['col_refes']
    thr_good = cfg['thr_good']
    thr_warn = cfg['thr_warn']
    use_cuit = cfg.get('use_cuit', True)

    id_col_e = cm_e['id']
    id_col_r = cm_r['id']

    # Convertir a lista de dicts para velocidad
    ext_rows  = ext_df.to_dict('records')
    ref_rows  = ref_df.to_dict('records')

    # Indexar REFES por CUIT
    ref_by_cuit: dict[str, list] = {}
    if use_cuit and cm_r.get('cuit'):
        for r in ref_rows:
            c = clean_cuit(r.get(cm_r['cuit'], ''))
            if c:
                ref_by_cuit.setdefault(c, []).append(r)

    # Indexar REFES por (prov_indec, dept_indec)
    ref_by_dept: dict[str, list] = {}
    for r in ref_rows:
        pv = to_indec(r.get(cm_r.get('prov_indec', ''), ''), 2)
        dv = to_indec(r.get(cm_r.get('dept_indec', ''), ''), 3)
        if pv and dv:
            k = f'{pv}-{dv}'
            ref_by_dept.setdefault(k, []).append(r)

    results = []

    def categorize(sc):
        if sc is None:
            return 'SIN_MATCH'
        if sc >= thr_good:
            return 'MATCH_BUENO'
        if sc >= thr_warn:
            return 'MATCH_REVISAR'
        return 'MATCH_DUDOSO'

    def build_row(e, ref, sc_dict, camino):
        sc_total = sc_dict['score_total'] if sc_dict else None
        return {
            'ext_id':              e.get(id_col_e, ''),
            'ext_nombre':          e.get(cm_e.get('name', ''), ''),
            'ext_provincia':       e.get(cm_e.get('prov_text', ''), '') or e.get(cm_e.get('prov_indec', ''), ''),
            'ext_departamento':    e.get(cm_e.get('dept_text', ''), '') or e.get(cm_e.get('dept_indec', ''), ''),
            'ext_localidad':       e.get(cm_e.get('loc', ''), ''),
            'ext_domicilio':       e.get(cm_e.get('addr', ''), ''),
            'ext_lat':             e.get(cm_e.get('lat', ''), '') if cm_e.get('lat') else None,
            'ext_lon':             e.get(cm_e.get('lon', ''), '') if cm_e.get('lon') else None,
            'refes_id':            ref.get(id_col_r, '') if ref else None,
            'refes_nombre':        ref.get(cm_r.get('name', ''), '') if ref else None,
            'refes_provincia':     ref.get(cm_r.get('prov_text', ''), '') if ref else None,
            'refes_domicilio':     ref.get(cm_r.get('addr', ''), '') if ref else None,
            'refes_lat':           ref.get(cm_r.get('lat', ''), '') if ref and cm_r.get('lat') else None,
            'refes_lon':           ref.get(cm_r.get('lon', ''), '') if ref and cm_r.get('lon') else None,
            'score_provincia':     sc_dict['score_provincia']    if sc_dict else None,
            'score_departamento':  sc_dict['score_departamento'] if sc_dict else None,
            'score_localidad':     sc_dict['score_localidad']    if sc_dict else None,
            'score_nombre':        sc_dict['score_nombre']       if sc_dict else None,
            'score_domicilio':     sc_dict['score_domicilio']    if sc_dict else None,
            'score_total':         sc_total,
            'prov_method':         sc_dict['prov_method'] if sc_dict else None,
            'dept_method':         sc_dict['dept_method'] if sc_dict else None,
            'camino':              camino,
            'categoria':           categorize(sc_total),
        }

    # ── CAMINO A: agrupar por CUIT ────────────────────────────────────
    ext_a: dict[str, list] = {}
    ext_b: list = []

    for e in ext_rows:
        c = clean_cuit(e.get(cm_e.get('cuit', ''), '')) if use_cuit and cm_e.get('cuit') else None
        if c and c in ref_by_cuit:
            ext_a.setdefault(c, []).append(e)
        else:
            ext_b.append(e)

    for cuit, e_list in ext_a.items():
        r_list = ref_by_cuit[cuit]
        matched = greedy_match(e_list, r_list, cfg, id_col_e, id_col_r)
        for e in e_list:
            eid = e[id_col_e]
            m = matched.get(eid)
            results.append(build_row(e, m['ref'] if m else None, m['sc'] if m else None, 'A'))

    # ── CAMINO B: agrupar por departamento INDEC ─────────────────────
    # Agrupar ext_b por depto
    ext_b_by_dept: dict[str, list] = {}
    ext_b_no_indec: list = []

    for e in ext_b:
        pv = to_indec(e.get(cm_e.get('prov_indec', ''), ''), 2)
        dv = to_indec(e.get(cm_e.get('dept_indec', ''), ''), 3)
        if pv and dv:
            ext_b_by_dept.setdefault(f'{pv}-{dv}', []).append(e)
        else:
            ext_b_no_indec.append(e)

    for k, e_list in ext_b_by_dept.items():
        r_list = ref_by_dept.get(k, [])
        if not r_list:
            for e in e_list:
                results.append(build_row(e, None, None, 'B'))
        else:
            matched = greedy_match(e_list, r_list, cfg, id_col_e, id_col_r)
            for e in e_list:
                eid = e[id_col_e]
                m = matched.get(eid)
                results.append(build_row(e, m['ref'] if m else None, m['sc'] if m else None, 'B'))

    for e in ext_b_no_indec:
        results.append(build_row(e, None, None, 'B'))

    return results


# ─────────────────────────────────────────────────────────────────────
# RUTAS API
# ─────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return send_from_directory('static', 'index.html')


@app.route('/api/upload', methods=['POST'])
def upload():
    role = request.form.get('role')  # 'refes' o 'ext'
    if 'file' not in request.files:
        return jsonify({'error': 'No se recibió archivo'}), 400

    f = request.files['file']
    ext = Path(f.filename).suffix.lower()
    if ext not in ALLOWED:
        return jsonify({'error': f'Formato no soportado: {ext}'}), 400

    fid = str(uuid.uuid4())
    dest = UPLOAD_DIR / f'{fid}{ext}'
    f.save(dest)

    try:
        df = read_file(dest)
    except Exception as e:
        return jsonify({'error': f'Error al leer el archivo: {e}'}), 400

    return jsonify({
        'file_id': fid,
        'filename': f.filename,
        'rows': len(df),
        'columns': list(df.columns),
    })


@app.route('/api/match', methods=['POST'])
def match():
    body = request.json

    # Cargar archivos
    try:
        ref_ext = next(p.suffix for p in UPLOAD_DIR.iterdir() if body['refes_id'] in p.name)
        ext_ext = next(p.suffix for p in UPLOAD_DIR.iterdir() if body['ext_id'] in p.name)
        ref_df  = read_file(UPLOAD_DIR / f"{body['refes_id']}{ref_ext}")
        ext_df  = read_file(UPLOAD_DIR / f"{body['ext_id']}{ext_ext}")
    except Exception as e:
        return jsonify({'error': f'Error cargando archivos: {e}'}), 400

    cfg = {
        'col_refes':       body['col_refes'],
        'col_ext':         body['col_ext'],
        'refes_addr_type': body.get('refes_addr_type', 'combined'),
        'ext_addr_type':   body.get('ext_addr_type', 'combined'),
        'weights': {
            'PROV': body['weights']['PROV'],
            'DEPT': body['weights']['DEPT'],
            'LOC':  body['weights']['LOC'],
            'NOM':  body['weights']['NOM'],
            'DOM':  body['weights']['DOM'],
        },
        'thr_good':       body.get('thr_good', 70),
        'thr_warn':       body.get('thr_warn', 50),
        'use_cuit':       body.get('use_cuit', True),
        'name_prefixes':  [norm(p) for p in body.get('name_prefixes', ['FARMACIA', 'FARM'])],
        'addr_prefixes':  [norm(p) for p in body.get('addr_prefixes', ['AVENIDA', 'AV', 'CALLE'])],
    }

    try:
        results = run_matching(ext_df, ref_df, cfg)
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'trace': traceback.format_exc()}), 500

    # Guardar para descarga
    out_id = str(uuid.uuid4())
    _save_xlsx(results, OUTPUT_DIR / f'{out_id}.xlsx')

    # Resumen para el frontend
    def cnt(cat): return sum(1 for r in results if r['categoria'] == cat)

    scored = [r['score_total'] for r in results if r['score_total'] is not None]
    avg    = round(sum(scored)/len(scored), 1) if scored else None

    return jsonify({
        'out_id':  out_id,
        'total':   len(results),
        'bueno':   cnt('MATCH_BUENO'),
        'revisar': cnt('MATCH_REVISAR'),
        'dudoso':  cnt('MATCH_DUDOSO'),
        'sin_match': cnt('SIN_MATCH'),
        'score_avg': avg,
        'results': results,
    })


@app.route('/api/download/<out_id>')
def download(out_id):
    path = OUTPUT_DIR / f'{out_id}.xlsx'
    if not path.exists():
        return jsonify({'error': 'Archivo no encontrado'}), 404
    return send_file(path, as_attachment=True, download_name='match_refes_resultados.xlsx')


def _save_xlsx(results: list, path: Path):
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment
    from openpyxl.utils import get_column_letter

    fill_hdr   = PatternFill('solid', start_color='1F4E79')
    fill_green = PatternFill('solid', start_color='C6EFCE')
    fill_yellow= PatternFill('solid', start_color='FFEB9C')
    fill_red   = PatternFill('solid', start_color='FFC7CE')
    fill_gray  = PatternFill('solid', start_color='D9D9D9')

    cat_fill = {
        'MATCH_BUENO':   fill_green,
        'MATCH_REVISAR': fill_yellow,
        'MATCH_DUDOSO':  fill_red,
        'SIN_MATCH':     fill_gray,
    }

    cols = ['ext_id','ext_nombre','ext_provincia','ext_departamento','ext_localidad',
            'ext_lat','ext_lon',
            'refes_id','refes_nombre','refes_provincia',
            'refes_lat','refes_lon',
            'score_provincia','score_departamento','score_localidad',
            'score_nombre','score_domicilio','score_total',
            'prov_method','dept_method','camino','categoria']

    wb = Workbook()
    ws = wb.active
    ws.title = 'Resultados'

    ws.append(cols)
    for i, c in enumerate(cols, 1):
        cell = ws.cell(1, i)
        cell.font = Font(name='Arial', bold=True, color='FFFFFF', size=9)
        cell.fill = fill_hdr
        cell.alignment = Alignment(horizontal='center', wrap_text=True)

    cat_idx   = cols.index('categoria') + 1
    score_idx = cols.index('score_total') + 1

    orden = {'MATCH_BUENO':0,'MATCH_REVISAR':1,'MATCH_DUDOSO':2,'SIN_MATCH':3}
    sorted_results = sorted(results, key=lambda r: (orden.get(r['categoria'],4), -(r['score_total'] or 0)))

    for r in sorted_results:
        ws.append([r.get(c) for c in cols])
        row_n = ws.max_row
        f = cat_fill.get(r['categoria'])
        if f:
            ws.cell(row_n, cat_idx).fill   = f
            ws.cell(row_n, score_idx).fill = f

    widths = {
        'ext_id':12,'ext_nombre':25,'ext_provincia':16,'ext_departamento':18,
        'ext_localidad':18,'ext_lat':12,'ext_lon':12,
        'refes_id':18,'refes_nombre':25,'refes_provincia':16,
        'refes_lat':12,'refes_lon':12,
        'score_provincia':13,'score_departamento':14,'score_localidad':13,
        'score_nombre':12,'score_domicilio':13,'score_total':11,
        'prov_method':10,'dept_method':10,'camino':8,'categoria':18,
    }
    for i, c in enumerate(cols, 1):
        ws.column_dimensions[get_column_letter(i)].width = widths.get(c, 13)
    ws.freeze_panes = 'A2'

    # Hoja resumen
    ws2 = wb.create_sheet('Resumen')
    for cat, label in [
        ('MATCH_BUENO','Match bueno'),('MATCH_REVISAR','Revisar'),
        ('MATCH_DUDOSO','Dudoso'),('SIN_MATCH','Sin match')
    ]:
        n = sum(1 for r in results if r['categoria']==cat)
        ws2.append([label, n])

    wb.save(path)


if __name__ == '__main__':
    print('Iniciando servidor en http://localhost:5000')
    app.run(debug=True, port=5000)
