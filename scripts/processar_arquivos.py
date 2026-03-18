import pandas as pd
import glob
import os
import re
import time
from datetime import timedelta

print("--- INICIANDO PROCESSAMENTO (V16.1 - CORREÇÃO DE SINTAXE) ---")

# --- 1. FUNÇÕES ---

def timecode_to_seconds(tc):
    if pd.isna(tc): return 0
    tc_str = str(tc).strip().replace(':', ';') 
    parts = tc_str.split(';')
    try:
        if len(parts) == 4:
            hh, mm, ss, ff = float(parts[0]), float(parts[1]), float(parts[2]), float(parts[3])
            return timedelta(hours=hh, minutes=mm, seconds=ss + ff/100).total_seconds()
        elif len(parts) == 3:
            hh, mm = float(parts[0]), float(parts[1])
            if '.' in parts[2]:
                s_split = parts[2].split('.')
                ss = float(s_split[0]) + float(s_split[1]) / 100
            else:
                ss = float(parts[2])
            return timedelta(hours=hh, minutes=mm, seconds=ss).total_seconds()
    except: pass
    return 0

def format_duration_human(seconds):
    """Converte segundos para texto legível (ex: 14 horas, 30 minutos)"""
    if pd.isna(seconds) or seconds == 0:
        return "0 seg"
    
    seconds = int(round(seconds))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    
    if h > 0:
        label = "hora" if h == 1 else "horas"
        if m > 0:
            return f"{h} {label} e {m} min"
        return f"{h} {label}"
    elif m > 0:
        return f"{m} min"
    else:
        return f"{s} seg"

def format_size_string(bytes_val):
    """Converte Bytes para string com GB (ex: 75,20 GB)"""
    if pd.isna(bytes_val) or bytes_val == 0:
        return "0 GB"
    try:
        gb = float(bytes_val) / (1024 ** 3)
        return f"{gb:.2f} GB".replace('.', ',')
    except:
        return "0 GB"

def clean_filename(fname):
    if pd.isna(fname): return ""
    name = str(fname).lower()
    # A linha abaixo foi a que deu erro, aqui está ela completa:
    for ext in ['.mp4', '.mxf', '.mov', '.avi', '.riptmp']:
        name = name.replace(ext, '')
    return name.strip()

def extract_guid(text):
    if pd.isna(text): return None
    match = re.search(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', str(text).lower())
    return match.group(0) if match else None

def load_unl(pattern):
    files = glob.glob(f"{pattern}*.unl")
    if not files: files = glob.glob(f"{pattern.lower()}*.unl")
    dfs = []
    for f in files:
        try:
            df = pd.read_csv(f, sep='|', header=None, dtype=str, encoding='utf-8-sig', on_bad_lines='warn')
            if not df.empty and df.shape[1] > 0:
                 if pd.isna(df.iloc[0, -1]) or df.iloc[0, -1] == '':
                     df = df.iloc[:, :-1]
            dfs.append(df)
        except: pass
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

# --- 2. CARREGAMENTO ---

print("> Carregando tabelas...")
df_files = load_unl('JobResultFiles')
df_locs = load_unl('JobResultFileLocations')
df_jobs = load_unl('Jobs')
df_planned = load_unl('PlannedTasks')

if df_files.empty or df_locs.empty:
    print("ERRO CRÍTICO: Tabelas de Arquivos ou Locais não encontradas.")
    exit()

# --- 3. PREPARAÇÃO ---

df_files.rename(columns={0: 'Id', 1: 'FileName'}, inplace=True)
if 'Id' not in df_files.columns: df_files['Id'] = df_files.iloc[:, 0]
if 'FileName' not in df_files.columns: df_files['FileName'] = df_files.iloc[:, 1]

df_files['Id_lower'] = df_files['Id'].str.lower().str.strip()
df_files['CleanName'] = df_files['FileName'].apply(clean_filename)
df_files['FileGUID'] = df_files['FileName'].apply(extract_guid)

df_locs.rename(columns={1: 'FileSize', 2: 'JobResultFileId', 4: 'Location'}, inplace=True)
df_locs['JobResultFileId_lower'] = df_locs['JobResultFileId'].str.lower().str.strip()

job_lookup = {}
if not df_jobs.empty:
    df_jobs.rename(columns={0: 'Id', 8: 'JobStatus', 13: 'StartTimecode', 15: 'StopTimecode'}, inplace=True)
    df_jobs['Id_lower'] = df_jobs['Id'].str.lower().str.strip()
    job_lookup = df_jobs.set_index('Id_lower')['JobStatus'].to_dict()

planned_lookup_title = {}
planned_lookup_id = {}    

if not df_planned.empty:
    cols = {0: 'Id', 3: 'DurationTC', 5: 'JobId', 11: 'Title'}
    df_planned.rename(columns=cols, inplace=True)
    
    for idx, row in df_planned.iterrows():
        pid = str(row.get('Id', '')).lower().strip()
        jobid = str(row.get('JobId', '')).lower().strip()
        title = clean_filename(row.get('Title', ''))
        dur = row.get('DurationTC', 0)
        
        task_info = {'JobId': jobid, 'Duration': dur}
        if pid: planned_lookup_id[pid] = task_info
        if title: planned_lookup_title[title] = task_info

# --- 4. MATCHING ---

print("> Realizando conexões...")
match_results = [] 

for idx, row in df_files.iterrows():
    f_id = row['Id_lower']
    f_name = row['CleanName']
    f_guid = row['FileGUID']
    
    found_info = None
    if f_guid and f_guid in planned_lookup_id:
        found_info = planned_lookup_id[f_guid]
    elif f_name and f_name in planned_lookup_title:
        found_info = planned_lookup_title[f_name]
    
    status = 'DESCONHECIDO'
    duracao_segundos = 0
    
    if found_info:
        raw_dur = found_info['Duration']
        duracao_segundos = timecode_to_seconds(raw_dur)
        
        job_id = found_info['JobId']
        if job_id in job_lookup:
            raw_status = job_lookup[job_id]
            status = 'FINALIZADO' if str(raw_status) == 'Completed' else 'PENDENTE'
        else:
            status = 'PENDENTE' 
            
    match_results.append({'Id_lower': f_id, 'STATUS': status, 'DURAÇÃO_SEG': duracao_segundos})

df_matches = pd.DataFrame(match_results)

# --- 5. MERGE E FORMATAÇÃO FINAL ---

df_final = df_files.merge(df_locs, left_on='Id_lower', right_on='JobResultFileId_lower', suffixes=('', '_loc'), how='left')
df_final = df_final.merge(df_matches, on='Id_lower', how='left')

def classify_loc(path):
    if pd.isna(path): return 'OUTRA'
    p = str(path).lower()
    if any(x in p for x in ['mam', 'proxy', 'baixa', 'low']): return 'BAIXA'
    if any(x in p for x in ['alta', 'clean', 'master', 'mxf']): return 'ALTA'
    if 'mp4' in p and 'live' in p: return 'ALTA'
    return 'OUTRA'

df_final['Tipo_Local'] = df_final['Location'].apply(classify_loc)

df_pivot = df_final.pivot_table(index='Id_lower', columns='Tipo_Local', values='Location', aggfunc='first').reset_index()
for c in ['ALTA', 'BAIXA']: 
    if c not in df_pivot.columns: df_pivot[c] = None

df_final['FileSize'] = pd.to_numeric(df_final['FileSize'], errors='coerce').fillna(0)
df_size = df_final.groupby('Id_lower')['FileSize'].max().reset_index()

cols_out = ['Id_lower', 'FileName', 'DURAÇÃO_SEG', 'STATUS']
df_export = df_final[cols_out].drop_duplicates(subset=['Id_lower'])

df_export = df_export.merge(df_pivot[['Id_lower', 'ALTA', 'BAIXA']], on='Id_lower', how='left')
df_export = df_export.merge(df_size, on='Id_lower', how='left')

# Formatações Visuais
df_export['DURAÇÃO'] = df_export['DURAÇÃO_SEG'].apply(format_duration_human)
df_export['TAMANHO'] = df_export['FileSize'].apply(format_size_string)

df_export.rename(columns={
    'Id_lower': 'ID',
    'FileName': 'NOME DO ARQUIVO',
    'ALTA': 'LOCAL ALTA QUALIDADE',
    'BAIXA': 'LOCAL BAIXA QUALIDADE'
}, inplace=True)

final_cols = ['ID', 'NOME DO ARQUIVO', 'LOCAL ALTA QUALIDADE', 'LOCAL BAIXA QUALIDADE', 'TAMANHO', 'DURAÇÃO', 'STATUS']
df_export = df_export[[c for c in final_cols if c in df_export.columns]]

# --- 6. SALVAMENTO SEGURO ---

nome_base = 'planilha_organizada.xlsx'
try:
    df_export.to_excel(nome_base, index=False)
    print(f"\nSUCESSO! '{nome_base}' atualizado.")
except PermissionError:
    print(f"\n[AVISO] O arquivo '{nome_base}' está aberto no Excel.")
    timestamp = int(time.time())
    novo_nome = f'planilha_organizada_{timestamp}.xlsx'
    print(f"Salvando como '{novo_nome}' para não perder os dados...")
    df_export.to_excel(novo_nome, index=False)
    print(f"SUCESSO! '{novo_nome}' gerado.")