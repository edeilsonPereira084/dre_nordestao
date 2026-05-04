"""
DRE Interativo - Supermercado Nordestão
Análise de Demonstração do Resultado do Exercício
Suporte a Varejo (Nordestão) e Atacado (Superfácil)
Múltiplos períodos: Consolidado e Parcial
"""
import pandas as pd
from flask import Flask, render_template, jsonify, request
import os
import re
import logging
import traceback
from datetime import datetime
from glob import glob
from functools import lru_cache

logging.basicConfig(level=logging.DEBUG)

app = Flask(__name__)

BASE_DIR        = os.path.dirname(os.path.abspath(__file__))
CONSOLIDADO_DIR = os.path.join(BASE_DIR, 'consolidado')
PARCIAL_DIR     = os.path.join(BASE_DIR, 'parcial')
SHEET_NAME      = 'DRE'

MESES = {
    'JAN': '01', 'FEV': '02', 'MAR': '03', 'ABR': '04',
    'MAI': '05', 'JUN': '06', 'JUL': '07', 'AGO': '08',
    'SET': '09', 'OUT': '10', 'NOV': '11', 'DEZ': '12'
}
MESES_NOME = {
    '01': 'Janeiro', '02': 'Fevereiro', '03': 'Março', '04': 'Abril',
    '05': 'Maio',    '06': 'Junho',     '07': 'Julho', '08': 'Agosto',
    '09': 'Setembro','10': 'Outubro',   '11': 'Novembro','12': 'Dezembro'
}

CONTAS_PRINCIPAIS = [
    'Receita bruta de vendas',
    'Desc. / Canc. e Devoluções S/ Vendas',
    'Receita de vendas',
    'Receita bruta de serviços',
    'Impostos e devoluções sobre vendas',
    'Receita líquida total',
    'Custo das mercadorias vendidas',
    'Lucro bruto',
    'Recomposição de margem',
    'Novo lucro bruto',
    'Quebras',
    'Lucro bruto após quebras',
    'GENTE E GESTÃO',
    'EFICIÊNCIA',
    'MANUTENÇÃO',
    'COMERCIAL & MARKETING',
    'Provisões e Depreciações',
    'Prestação de Serviços',
    'Aluguel de Prédios',
    'Outras Desp Financeiras',
    'TI',
    'Gastos ADM & Taxas',
    'Instalações & Utilidades',
    'Mobilidade',
    'Desp. Gerais e Administrativas',
    'Lucro Operacional',
    'OUTROS MARKETING',
    'DESP. CORPORATIVAS',
    'DESP. LOGISTICA',
    'DESP. PRODUÇÕES & INDUSTRIAS',
    'LUCRO ANTES IMPOSTOS',
]

ESTRUTURA_DRE = {
    'Receita bruta de vendas':              {'nivel': 1, 'tipo': 'receita',  'expansivel': True},
    'Desc. / Canc. e Devoluções S/ Vendas': {'nivel': 1, 'tipo': 'deducao',  'expansivel': True},
    'Receita de vendas':                    {'nivel': 1, 'tipo': 'subtotal', 'expansivel': False},
    'Receita bruta de serviços':            {'nivel': 1, 'tipo': 'receita',  'expansivel': True},
    'Impostos e devoluções sobre vendas':   {'nivel': 1, 'tipo': 'deducao',  'expansivel': True},
    'Impostos e devoluções sobre serviços': {'nivel': 1, 'tipo': 'deducao',  'expansivel': True},
    'Receita líquida total':                {'nivel': 0, 'tipo': 'total',    'expansivel': False},
    'Custo das mercadorias vendidas':       {'nivel': 1, 'tipo': 'custo',    'expansivel': True},
    'Lucro bruto':                          {'nivel': 0, 'tipo': 'total',    'expansivel': False},
    'Recomposição de margem':               {'nivel': 1, 'tipo': 'receita',  'expansivel': True},
    'Impostos sobre recomposição de margem':{'nivel': 1, 'tipo': 'deducao',  'expansivel': True},
    'Novo lucro bruto':                     {'nivel': 0, 'tipo': 'total',    'expansivel': False},
    'Quebras':                              {'nivel': 1, 'tipo': 'despesa',  'expansivel': True},
    'Lucro bruto após quebras':             {'nivel': 0, 'tipo': 'total',    'expansivel': False},
    'GENTE E GESTÃO':                       {'nivel': 1, 'tipo': 'despesa',  'expansivel': True},
    'EFICIÊNCIA':                           {'nivel': 1, 'tipo': 'despesa',  'expansivel': True},
    'MANUTENÇÃO':                           {'nivel': 1, 'tipo': 'despesa',  'expansivel': True},
    'COMERCIAL & MARKETING':                {'nivel': 1, 'tipo': 'despesa',  'expansivel': True},
    'Provisões e Depreciações':             {'nivel': 1, 'tipo': 'despesa',  'expansivel': True},
    'Prestação de Serviços':                {'nivel': 1, 'tipo': 'despesa',  'expansivel': True},
    'Aluguel de Prédios':                   {'nivel': 1, 'tipo': 'despesa',  'expansivel': True},
    'Outras Desp Financeiras':              {'nivel': 1, 'tipo': 'despesa',  'expansivel': True},
    'TI':                                   {'nivel': 1, 'tipo': 'despesa',  'expansivel': True},
    'Gastos ADM & Taxas':                   {'nivel': 1, 'tipo': 'despesa',  'expansivel': True},
    'Instalações & Utilidades':             {'nivel': 1, 'tipo': 'despesa',  'expansivel': True},
    'Mobilidade':                           {'nivel': 1, 'tipo': 'despesa',  'expansivel': True},
    'Desp. Gerais e Administrativas':       {'nivel': 0, 'tipo': 'subtotal', 'expansivel': False},
    'Lucro Operacional':                    {'nivel': 0, 'tipo': 'total',    'expansivel': False},
    'OUTROS MARKETING':                     {'nivel': 1, 'tipo': 'despesa',  'expansivel': True},
    'DESP. CORPORATIVAS':                   {'nivel': 1, 'tipo': 'despesa',  'expansivel': False},
    'DESP. LOGISTICA':                      {'nivel': 1, 'tipo': 'despesa',  'expansivel': False},
    'DESP. PRODUÇÕES & INDUSTRIAS':         {'nivel': 1, 'tipo': 'despesa',  'expansivel': False},
    'LUCRO ANTES IMPOSTOS':                 {'nivel': 0, 'tipo': 'resultado','expansivel': False},
}

# ── Ordem e grupos das lojas ──
ORDEM_VAREJO = [
    'LJ. ALECRIM', 'LJ. PETRÓPOLIS', 'LJ. IGAPÓ', 'LJ. LAGOA NOVA',
    'LJ. TIROL', 'LJ. PONTA NEGRA', 'LJ. CIDADE JARDIM',
    'LJ. NOVA PARNAMIRIM', 'LJ. SANTA CATARINA',
]
ORDEM_ATACADO = [
    'SUPERFÁCIL EMAÚS', 'SUPERFÁCIL JOÃO PESSOA',
    'SUPERFÁCIL RODOVIÁRIA', 'SUPERFÁCIL SGA',
]
LOJAS_MOSSORO = [
    'LJ. ABOLIÇÃO IV', 'LJ. DOZE ANOS',
    'SUPERFÁCIL ALTO SÃO MANOEL', 'SUPERFÁCIL NOVA BETANIA',
]

GRUPOS = {
    'varejo':  {'label': 'DRE VAREJO',  'logo': 'static/logo.png'},
    'atacado': {'label': 'DRE ATACADO', 'logo': 'static/facil.png'},
    'mossoro': {'label': 'DRE MOSSORÓ',              'logo': 'static/mosso.png'},
}


# ─────────────────────────────────────────────
# CACHE
# ─────────────────────────────────────────────
@lru_cache(maxsize=16)
def ler_excel_cached(excel_path):
    return pd.read_excel(excel_path, sheet_name=SHEET_NAME, header=None)


# ── Helpers ──
def normalize_conta(conta):
    if pd.isna(conta):
        return ''
    return str(conta).strip().lower()

def is_conta_principal(conta):
    conta_norm = normalize_conta(conta)
    return any(normalize_conta(cp) == conta_norm for cp in CONTAS_PRINCIPAIS)

def get_conta_config(conta):
    conta_str = str(conta).strip() if pd.notna(conta) else ''
    for key, config in ESTRUTURA_DRE.items():
        if normalize_conta(key) == normalize_conta(conta_str):
            return config
    if any(x in conta_str.upper() for x in ['LUCRO', 'RESULTADO']):
        if 'ANTES' in conta_str.upper() or 'LÍQUIDO' in conta_str.upper():
            return {'nivel': 0, 'tipo': 'resultado', 'expansivel': False}
        return {'nivel': 0, 'tipo': 'total', 'expansivel': False}
    if conta_str.startswith('(-)'):
        return {'nivel': 2, 'tipo': 'deducao', 'expansivel': False}
    if conta_str.isupper() and len(conta_str) > 3:
        return {'nivel': 2, 'tipo': 'despesa', 'expansivel': False}
    return {'nivel': 2, 'tipo': 'normal', 'expansivel': False}

def find_conta(dados, nome):
    nome_lower = nome.lower()
    for item in dados:
        if item['conta'].lower() == nome_lower:
            return item
    return {'real_2024': 0, 'real_2025': 0, 'var_25_24': 0, 'var_orc_real': 0}

def classificar_loja(nome):
    nome_upper = nome.upper()
    if 'SUPERFÁCIL' in nome_upper or 'SUPERFACIL' in nome_upper:
        return 'atacado'
    return 'varejo'

def extrair_periodo_arquivo(nome_arquivo):
    match = re.search(r'_([A-Z]{3})(\d{2})\.xlsx$', nome_arquivo, re.IGNORECASE)
    if match:
        mes = match.group(1).upper()
        ano = match.group(2)
        return {
            'mes_abrev': mes, 'ano': ano,
            'mes_num':   MESES.get(mes, '01'),
            'mes_nome':  MESES_NOME.get(MESES.get(mes, '01'), mes),
            'label':     f"{mes}/{ano}",
            'sort_key':  f"20{ano}{MESES.get(mes, '01')}"
        }
    return None

def _sort_lojas(lojas, ordem):
    ordem_norm = [n.upper() for n in ordem]
    def key(l):
        try:
            return ordem_norm.index(l['nome'].upper())
        except ValueError:
            return len(ordem_norm)
    return sorted(lojas, key=key)

def _anos_do_arquivo(excel_path):
    periodo_info = extrair_periodo_arquivo(os.path.basename(excel_path))
    if periodo_info:
        ano_num = int('20' + periodo_info['ano'])
        return str(ano_num), str(ano_num - 1), periodo_info['mes_nome'], periodo_info['mes_abrev']
    return '2025', '2024', '', ''

def _build_kpis_e_aliases(dados, indicadores):
    """Calcula KPIs, margens e adiciona aliases usados pelo template."""
    receita     = find_conta(dados, 'Receita líquida total')
    lucro_bruto = find_conta(dados, 'Lucro bruto')
    lucro_op    = find_conta(dados, 'Lucro Operacional')
    lucro_final = find_conta(dados, 'LUCRO ANTES IMPOSTOS')

    rec_atual = receita.get('real_2025', 0) or 1
    margem_bruta   = (lucro_bruto.get('real_2025', 0) / rec_atual) * 100
    margem_op      = (lucro_op.get('real_2025', 0)    / rec_atual) * 100
    margem_liquida = (lucro_final.get('real_2025', 0) / rec_atual) * 100

    kpis = {
        'receita_2025':     receita.get('real_2025', 0),
        'receita_2024':     receita.get('real_2024', 0),
        'receita_var':      receita.get('var_25_24', 0),
        'lucro_bruto_2025': lucro_bruto.get('real_2025', 0),
        'lucro_bruto_var':  lucro_bruto.get('var_25_24', 0),
        'lucro_op_2025':    lucro_op.get('real_2025', 0),
        'lucro_op_var':     lucro_op.get('var_25_24', 0),
        'lucro_final_2025': lucro_final.get('real_2025', 0),
        'lucro_final_var':  lucro_final.get('var_orc_real', 0),
        'margem_bruta':     margem_bruta,
        'margem_op':        margem_op,
        'margem_liquida':   margem_liquida,
        # Aliases do template
        'receita_atual':     receita.get('real_2025', 0),
        'receita_anterior':  receita.get('real_2024', 0),
        'lucro_bruto_atual': lucro_bruto.get('real_2025', 0),
        'lucro_op_atual':    lucro_op.get('real_2025', 0),
        'lucro_final_atual': lucro_final.get('real_2025', 0),
    }

    # Aliases de indicadores
    indicadores['func_atual']      = indicadores.get('func_2025', 0)
    indicadores['func_anterior']   = indicadores.get('func_2024', 0)
    indicadores['cupons_atual']    = indicadores.get('cupons_2025', 0)
    indicadores['cupons_anterior'] = indicadores.get('cupons_2024', 0)
    indicadores['ticket_atual']    = indicadores.get('ticket_2025', 0)
    indicadores['ticket_anterior'] = indicadores.get('ticket_2024', 0)

    return kpis


# ─────────────────────────────────────────────
# Períodos
# ─────────────────────────────────────────────
@lru_cache(maxsize=1)
def listar_periodos():
    periodos = {'consolidado': [], 'parcial': [], 'todos': []}

    for diretorio, tipo in [(CONSOLIDADO_DIR, 'consolidado'), (PARCIAL_DIR, 'parcial')]:
        if os.path.exists(diretorio):
            for arquivo in glob(os.path.join(diretorio, 'DRE_LOJAS_*.xlsx')):
                nome = os.path.basename(arquivo)
                periodo = extrair_periodo_arquivo(nome)
                if periodo:
                    periodo['arquivo'] = arquivo
                    periodo['tipo'] = tipo
                    periodos[tipo].append(periodo)

    periodos['consolidado'].sort(key=lambda x: x['sort_key'], reverse=True)
    periodos['parcial'].sort(key=lambda x: x['sort_key'], reverse=True)

    todos_periodos = {}
    for p in periodos['consolidado'] + periodos['parcial']:
        if p['label'] not in todos_periodos:
            todos_periodos[p['label']] = {
                'label': p['label'], 'mes_nome': p['mes_nome'],
                'ano': p['ano'], 'sort_key': p['sort_key'],
                'tem_consolidado': False, 'tem_parcial': False
            }
        todos_periodos[p['label']]['tem_' + p['tipo']] = True

    periodos['todos'] = sorted(todos_periodos.values(), key=lambda x: x['sort_key'], reverse=True)
    return periodos

def obter_arquivo(periodo_label, tipo='consolidado'):
    diretorio = CONSOLIDADO_DIR if tipo == 'consolidado' else PARCIAL_DIR
    if not os.path.exists(diretorio):
        return None
    for arquivo in glob(os.path.join(diretorio, 'DRE_LOJAS_*.xlsx')):
        periodo = extrair_periodo_arquivo(os.path.basename(arquivo))
        if periodo and periodo['label'] == periodo_label:
            return arquivo
    return None


# ─────────────────────────────────────────────
# Lojas
# ─────────────────────────────────────────────
def listar_lojas(excel_path):
    df = ler_excel_cached(excel_path).copy()

    data_ref_raw = df.iloc[3, 3]
    if isinstance(data_ref_raw, (pd.Timestamp, datetime)):
        data_ref = data_ref_raw.strftime('%d/%m/%Y')
    elif pd.notna(data_ref_raw):
        data_ref = str(data_ref_raw)[:10]
    else:
        data_ref = ''

    row_lojas = df.iloc[3]
    lojas_varejo = []
    lojas_atacado = []
    lojas_mossoro = []
    mossoro_norm = [n.upper() for n in LOJAS_MOSSORO]

    for col_idx, val in enumerate(row_lojas):
        if pd.notna(val) and isinstance(val, str):
            val_clean = val.strip()
            if 'LJ.' in val_clean or 'SUPERFÁCIL' in val_clean or 'SUPERFACIL' in val_clean:
                tipo = classificar_loja(val_clean)
                loja = {'id': str(col_idx), 'nome': val_clean,
                        'col_inicio': col_idx, 'data_ref': data_ref, 'tipo': tipo}
                if val_clean.upper() in mossoro_norm:
                    lojas_mossoro.append(loja)
                elif tipo == 'varejo':
                    lojas_varejo.append(loja)
                else:
                    lojas_atacado.append(loja)

    varejo_ord  = _sort_lojas(lojas_varejo,  ORDEM_VAREJO)
    atacado_ord = _sort_lojas(lojas_atacado, ORDEM_ATACADO)
    mossoro_ord = _sort_lojas(lojas_mossoro, LOJAS_MOSSORO)

    return {
        'varejo':  varejo_ord,
        'atacado': atacado_ord,
        'mossoro': mossoro_ord,
        'todas':   varejo_ord + atacado_ord + mossoro_ord,
        'data_ref': data_ref
    }


# ─────────────────────────────────────────────
# Carrega DRE de UMA loja
# ─────────────────────────────────────────────
def _extrair_dados_loja(excel_path, col_inicio):
    """Extrai os dados brutos de uma loja específica do Excel."""
    df  = ler_excel_cached(excel_path).copy()
    col_inicio = int(col_inicio)

    col_real_2024 = col_inicio
    col_perc_2024 = col_inicio + 1
    col_orcamento = col_inicio + 2
    col_perc_orc  = col_inicio + 3
    col_real_2025 = col_inicio + 4
    col_perc_2025 = col_inicio + 5
    col_var_orc   = col_inicio + 6
    col_var_24    = col_inicio + 7
    col_contas    = 3

    def safe_float(val):
        try:
            return 0.0 if pd.isna(val) else float(val)
        except:
            return 0.0

    dados = []
    indicadores = {}
    conta_pai = None

    for idx in range(5, len(df)):
        row = df.iloc[idx]
        conta = row[col_contas]

        if pd.isna(conta) or str(conta).strip() == '':
            continue
        conta_str = str(conta).strip()
        if conta_str in ['Nº de Cupons', 'Ticket Médio', 'Nº de Func.',
                         'Crescimento da Receita (%)', 'Crescimento Ticket Médio (%)',
                         'Crescimento Clientes (%)', 'COD', '2025', '2024']:
            # captura indicadores operacionais
            if conta_str == 'Nº de Cupons':
                indicadores['cupons_2024'] = indicadores.get('cupons_2024', 0) + safe_float(row[col_real_2024])
                indicadores['cupons_2025'] = indicadores.get('cupons_2025', 0) + safe_float(row[col_real_2025])
            elif conta_str == 'Ticket Médio':
                # ticket médio: não somamos, usamos média simples depois
                indicadores.setdefault('_ticket_2024_list', []).append(safe_float(row[col_real_2024]))
                indicadores.setdefault('_ticket_2025_list', []).append(safe_float(row[col_real_2025]))
            elif conta_str == 'Nº de Func.':
                indicadores['func_2024'] = indicadores.get('func_2024', 0) + safe_float(row[col_real_2024])
                indicadores['func_2025'] = indicadores.get('func_2025', 0) + safe_float(row[col_real_2025])
            continue

        if isinstance(conta, pd.Timestamp):
            continue

        is_principal = is_conta_principal(conta_str)
        config = get_conta_config(conta_str)

        r2024 = safe_float(row[col_real_2024])
        r2025 = safe_float(row[col_real_2025])
        orc   = safe_float(row[col_orcamento])
        item = {
            'conta':        conta_str,
            'real_2024':    r2024,
            'orcamento':    orc,
            'real_2025':    r2025,
            'perc_2024':    0.0,
            'perc_orc':     0.0,
            'perc_2025':    0.0,
            'var_orc_real': 0.0,
            'var_25_24':    0.0,
            'is_principal': is_principal,
            'nivel':        config['nivel'],
            'tipo':         config['tipo'],
            'expansivel':   config['expansivel'] and is_principal,
            'conta_pai':    conta_pai if not is_principal else None,
            # aliases sempre presentes
            'real_atual':    r2025,
            'real_anterior': r2024,
            'perc_atual':    0.0,
            'perc_anterior': 0.0,
            'var_ano':       0.0,
        }
        if is_principal:
            conta_pai = conta_str
        dados.append(item)

    return dados, indicadores


def _montar_dre_final(dados_somados, indicadores, excel_path, nome_exibicao, tipo_loja, data_ref, lojas):
    """Recalcula percentuais e variações e monta o dict padrão de retorno."""
    # receita líquida atual para % sobre faturamento
    rec_item = next((i for i in dados_somados if i['conta'].lower() == 'receita líquida total'), None)
    rec_2025 = rec_item['real_2025'] if rec_item and rec_item['real_2025'] else 1
    rec_2024 = rec_item['real_2024'] if rec_item and rec_item['real_2024'] else 1

    for item in dados_somados:
        item['perc_2025']    = item['real_2025'] / rec_2025 if rec_2025 else 0
        item['perc_2024']    = item['real_2024'] / rec_2024 if rec_2024 else 0
        item['perc_orc']     = item['orcamento'] / rec_2025 if rec_2025 else 0
        # variação orç x real
        if item['orcamento'] != 0:
            item['var_orc_real'] = (item['real_2025'] - item['orcamento']) / abs(item['orcamento'])
        else:
            item['var_orc_real'] = 0
        # variação ano x ano
        if item['real_2024'] != 0:
            item['var_25_24'] = (item['real_2025'] - item['real_2024']) / abs(item['real_2024'])
        else:
            item['var_25_24'] = 0
        # aliases do template
        item['real_atual']    = item['real_2025']
        item['real_anterior'] = item['real_2024']
        item['perc_atual']    = item['perc_2025']
        item['perc_anterior'] = item['perc_2024']
        item['var_ano']       = item['var_25_24']

    # ticket médio: média das lojas
    t24 = indicadores.pop('_ticket_2024_list', [])
    t25 = indicadores.pop('_ticket_2025_list', [])
    indicadores['ticket_2024'] = sum(t24) / len(t24) if t24 else 0
    indicadores['ticket_2025'] = sum(t25) / len(t25) if t25 else 0

    kpis = _build_kpis_e_aliases(dados_somados, indicadores)
    ano_atual, ano_anterior, mes_nome, mes_abrev = _anos_do_arquivo(excel_path)

    return {
        'data_ref':        data_ref,
        'loja':            nome_exibicao,
        'tipo_loja':       tipo_loja,
        'col_inicio':      None,
        'dados':           dados_somados,
        'indicadores':     indicadores,
        'kpis':            kpis,
        'lojas':           lojas,
        'ano_atual':       ano_atual,
        'ano_anterior':    ano_anterior,
        'mes_nome_atual':  mes_nome,
        'mes_abrev_atual': mes_abrev,
    }


def carregar_dre(excel_path, col_inicio=None):
    """Carrega o DRE de uma loja individual."""
    df    = ler_excel_cached(excel_path).copy()
    lojas = listar_lojas(excel_path)

    if col_inicio is None:
        if lojas['todas']:
            col_inicio = lojas['todas'][0]['col_inicio']
        else:
            return None

    col_inicio = int(col_inicio)
    loja_nome  = df.iloc[3, col_inicio]
    tipo_loja  = classificar_loja(str(loja_nome))

    data_ref_raw = df.iloc[3, 3]
    if isinstance(data_ref_raw, (pd.Timestamp, datetime)):
        data_ref = data_ref_raw.strftime('%d/%m/%Y')
    elif pd.notna(data_ref_raw):
        data_ref = str(data_ref_raw)[:10]
    else:
        data_ref = ''

    dados, indicadores = _extrair_dados_loja(excel_path, col_inicio)

    # Adiciona campos de percentual e variação da planilha original (mais preciso)
    col_perc_2024 = col_inicio + 1
    col_perc_orc  = col_inicio + 3
    col_perc_2025 = col_inicio + 5
    col_var_orc   = col_inicio + 6
    col_var_24    = col_inicio + 7
    col_contas    = 3

    def safe_float(val):
        try:
            return 0.0 if pd.isna(val) else float(val)
        except:
            return 0.0

    # Mapa para enriquecer com dados originais da planilha
    dados_map = {i['conta']: i for i in dados}
    for idx in range(5, len(df)):
        row      = df.iloc[idx]
        conta    = row[col_contas]
        if pd.isna(conta) or str(conta).strip() == '':
            continue
        conta_str = str(conta).strip()
        if conta_str in dados_map:
            dados_map[conta_str]['perc_2024']    = safe_float(row[col_perc_2024])
            dados_map[conta_str]['perc_orc']     = safe_float(row[col_perc_orc])
            dados_map[conta_str]['perc_2025']    = safe_float(row[col_perc_2025])
            dados_map[conta_str]['var_orc_real'] = safe_float(row[col_var_orc])
            dados_map[conta_str]['var_25_24']    = safe_float(row[col_var_24])
            dados_map[conta_str]['real_atual']    = dados_map[conta_str]['real_2025']
            dados_map[conta_str]['real_anterior'] = dados_map[conta_str]['real_2024']
            dados_map[conta_str]['perc_atual']    = dados_map[conta_str]['perc_2025']
            dados_map[conta_str]['perc_anterior'] = dados_map[conta_str]['perc_2024']
            dados_map[conta_str]['var_ano']       = dados_map[conta_str]['var_25_24']

    t24 = indicadores.pop('_ticket_2024_list', [])
    t25 = indicadores.pop('_ticket_2025_list', [])
    indicadores['ticket_2024'] = sum(t24) / len(t24) if t24 else 0
    indicadores['ticket_2025'] = sum(t25) / len(t25) if t25 else 0

    kpis = _build_kpis_e_aliases(dados, indicadores)
    ano_atual, ano_anterior, mes_nome, mes_abrev = _anos_do_arquivo(excel_path)

    return {
        'data_ref':        data_ref,
        'loja':            loja_nome if pd.notna(loja_nome) else '',
        'tipo_loja':       tipo_loja,
        'col_inicio':      col_inicio,
        'dados':           dados,
        'indicadores':     indicadores,
        'kpis':            kpis,
        'lojas':           lojas,
        'ano_atual':       ano_atual,
        'ano_anterior':    ano_anterior,
        'mes_nome_atual':  mes_nome,
        'mes_abrev_atual': mes_abrev,
    }


# ─────────────────────────────────────────────
# Carrega DRE CONSOLIDADO DE UM GRUPO
# ─────────────────────────────────────────────
def carregar_dre_grupo(excel_path, grupo):
    """
    Soma os DREs de todas as lojas do grupo e retorna um único DRE consolidado.
    grupo: 'varejo' | 'atacado' | 'mossoro'
    """
    lojas = listar_lojas(excel_path)
    lojas_grupo = lojas.get(grupo, [])

    if not lojas_grupo:
        return None

    df = ler_excel_cached(excel_path).copy()
    data_ref_raw = df.iloc[3, 3]
    if isinstance(data_ref_raw, (pd.Timestamp, datetime)):
        data_ref = data_ref_raw.strftime('%d/%m/%Y')
    elif pd.notna(data_ref_raw):
        data_ref = str(data_ref_raw)[:10]
    else:
        data_ref = ''

    # Soma os dados de todas as lojas do grupo
    soma_por_conta = {}   # conta -> {real_2024, orcamento, real_2025, ...metadados}
    soma_indicadores = {}

    for loja in lojas_grupo:
        dados_loja, ind_loja = _extrair_dados_loja(excel_path, loja['col_inicio'])

        for item in dados_loja:
            conta = item['conta']
            if conta not in soma_por_conta:
                soma_por_conta[conta] = {
                    'conta':        conta,
                    'real_2024':    0.0,
                    'orcamento':    0.0,
                    'real_2025':    0.0,
                    'perc_2024':    0.0,
                    'perc_orc':     0.0,
                    'perc_2025':    0.0,
                    'var_orc_real': 0.0,
                    'var_25_24':    0.0,
                    'is_principal': item['is_principal'],
                    'nivel':        item['nivel'],
                    'tipo':         item['tipo'],
                    'expansivel':   item['expansivel'],
                    'conta_pai':    item['conta_pai'],
                    'real_atual':    0.0,
                    'real_anterior': 0.0,
                    'perc_atual':    0.0,
                    'perc_anterior': 0.0,
                    'var_ano':       0.0,
                }
            soma_por_conta[conta]['real_2024'] += item['real_2024']
            soma_por_conta[conta]['orcamento'] += item['orcamento']
            soma_por_conta[conta]['real_2025'] += item['real_2025']

        # Soma indicadores operacionais
        for k in ['cupons_2024', 'cupons_2025', 'func_2024', 'func_2025']:
            soma_indicadores[k] = soma_indicadores.get(k, 0) + ind_loja.get(k, 0)
        # Ticket médio: acumula listas
        soma_indicadores.setdefault('_ticket_2024_list', []).extend(ind_loja.get('_ticket_2024_list', []))
        soma_indicadores.setdefault('_ticket_2025_list', []).extend(ind_loja.get('_ticket_2025_list', []))

    # Mantém a ordem original das contas (primeira loja como referência)
    ordem_contas = [i['conta'] for i in _extrair_dados_loja(excel_path, lojas_grupo[0]['col_inicio'])[0]]
    dados_somados = [soma_por_conta[c] for c in ordem_contas if c in soma_por_conta]

    info_grupo = GRUPOS.get(grupo, {})
    tipo_loja  = 'atacado' if grupo == 'atacado' else 'varejo'
    nome_grupo = info_grupo.get('label', grupo.title())

    return _montar_dre_final(dados_somados, soma_indicadores, excel_path,
                             nome_grupo, tipo_loja, data_ref, lojas)


# ─────────────────────────────────────────────
# Rotas
# ─────────────────────────────────────────────
@app.route('/')
def index():
    try:
        periodo  = request.args.get('periodo')
        tipo     = request.args.get('tipo', 'consolidado')
        loja_id  = request.args.get('loja')
        grupo    = request.args.get('grupo')   # 'varejo' | 'atacado' | 'mossoro'

        periodos = listar_periodos()

        if not periodo and periodos['todos']:
            periodo = periodos['todos'][0]['label']

        if periodo:
            periodo_info = next((p for p in periodos['todos'] if p['label'] == periodo), None)
            if periodo_info:
                if tipo == 'parcial' and not periodo_info['tem_parcial']:
                    tipo = 'consolidado'
                elif tipo == 'consolidado' and not periodo_info['tem_consolidado']:
                    tipo = 'parcial'

        excel_path = obter_arquivo(periodo, tipo) if periodo else None

        if not excel_path:
            return render_template('index.html',
                                   dre=None,
                                   lojas={'varejo': [], 'atacado': [], 'mossoro': [], 'todas': []},
                                   periodos=periodos,
                                   periodo_selecionado=periodo,
                                   tipo_selecionado=tipo,
                                   loja_selecionada=None,
                                   grupo_selecionado=None,
                                   erro="Nenhum arquivo DRE encontrado.")

        # Decide se carrega grupo ou loja individual
        if grupo and grupo in GRUPOS:
            dre_data = carregar_dre_grupo(excel_path, grupo)
            loja_id  = None
        else:
            grupo    = None
            col_inicio = int(loja_id) if loja_id else None
            dre_data   = carregar_dre(excel_path, col_inicio)
            # Descobre o grupo da loja selecionada
            if dre_data:
                lojas_info = dre_data['lojas']
                loja_selecionada_id = loja_id or (str(lojas_info['todas'][0]['col_inicio']) if lojas_info['todas'] else None)
                for g, lst_key in [('varejo','varejo'),('atacado','atacado'),('mossoro','mossoro')]:
                    if any(str(l['id']) == str(loja_selecionada_id) for l in lojas_info.get(lst_key, [])):
                        grupo = g
                        break

        if not dre_data:
            return render_template('index.html',
                                   dre=None,
                                   lojas={'varejo': [], 'atacado': [], 'mossoro': [], 'todas': []},
                                   periodos=periodos,
                                   periodo_selecionado=periodo,
                                   tipo_selecionado=tipo,
                                   loja_selecionada=None,
                                   grupo_selecionado=None,
                                   erro="Erro ao carregar dados do DRE.")

        dre_data['periodo']      = periodo
        dre_data['tipo_arquivo'] = tipo

        return render_template('index.html',
                               dre=dre_data,
                               lojas=dre_data['lojas'],
                               periodos=periodos,
                               periodo_selecionado=periodo,
                               tipo_selecionado=tipo,
                               loja_selecionada=loja_id,
                               grupo_selecionado=grupo,
                               erro=None)

    except Exception as e:
        logging.error("ERRO NA ROTA /: %s", traceback.format_exc())
        return f"<h2>Erro interno</h2><pre>{traceback.format_exc()}</pre>", 500


@app.route('/api/periodos')
def api_periodos():
    return jsonify(listar_periodos())


@app.route('/api/dre')
def api_dre():
    periodo = request.args.get('periodo')
    tipo    = request.args.get('tipo', 'consolidado')
    loja_id = request.args.get('loja')
    grupo   = request.args.get('grupo')

    excel_path = obter_arquivo(periodo, tipo)
    if not excel_path:
        return jsonify({'erro': 'Arquivo não encontrado'}), 404

    if grupo and grupo in GRUPOS:
        dre_data = carregar_dre_grupo(excel_path, grupo)
    else:
        col_inicio = int(loja_id) if loja_id else None
        dre_data   = carregar_dre(excel_path, col_inicio)

    if dre_data:
        dre_data['periodo']      = periodo
        dre_data['tipo_arquivo'] = tipo

    return jsonify(dre_data)


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)