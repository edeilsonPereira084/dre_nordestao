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

# Diretórios de dados
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONSOLIDADO_DIR = os.path.join(BASE_DIR, 'consolidado')
PARCIAL_DIR = os.path.join(BASE_DIR, 'parcial')
SHEET_NAME = 'DRE'

# Mapeamento de meses
MESES = {
    'JAN': '01', 'FEV': '02', 'MAR': '03', 'ABR': '04',
    'MAI': '05', 'JUN': '06', 'JUL': '07', 'AGO': '08',
    'SET': '09', 'OUT': '10', 'NOV': '11', 'DEZ': '12'
}

MESES_NOME = {
    '01': 'Janeiro', '02': 'Fevereiro', '03': 'Março', '04': 'Abril',
    '05': 'Maio', '06': 'Junho', '07': 'Julho', '08': 'Agosto',
    '09': 'Setembro', '10': 'Outubro', '11': 'Novembro', '12': 'Dezembro'
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
    'Receita bruta de vendas': {'nivel': 1, 'tipo': 'receita', 'expansivel': True},
    'Desc. / Canc. e Devoluções S/ Vendas': {'nivel': 1, 'tipo': 'deducao', 'expansivel': True},
    'Receita de vendas': {'nivel': 1, 'tipo': 'subtotal', 'expansivel': False},
    'Receita bruta de serviços': {'nivel': 1, 'tipo': 'receita', 'expansivel': True},
    'Impostos e devoluções sobre vendas': {'nivel': 1, 'tipo': 'deducao', 'expansivel': True},
    'Impostos e devoluções sobre serviços': {'nivel': 1, 'tipo': 'deducao', 'expansivel': True},
    'Receita líquida total': {'nivel': 0, 'tipo': 'total', 'expansivel': False},
    'Custo das mercadorias vendidas': {'nivel': 1, 'tipo': 'custo', 'expansivel': True},
    'Lucro bruto': {'nivel': 0, 'tipo': 'total', 'expansivel': False},
    'Recomposição de margem': {'nivel': 1, 'tipo': 'receita', 'expansivel': True},
    'Impostos sobre recomposição de margem': {'nivel': 1, 'tipo': 'deducao', 'expansivel': True},
    'Novo lucro bruto': {'nivel': 0, 'tipo': 'total', 'expansivel': False},
    'Quebras': {'nivel': 1, 'tipo': 'despesa', 'expansivel': True},
    'Lucro bruto após quebras': {'nivel': 0, 'tipo': 'total', 'expansivel': False},
    'GENTE E GESTÃO': {'nivel': 1, 'tipo': 'despesa', 'expansivel': True},
    'EFICIÊNCIA': {'nivel': 1, 'tipo': 'despesa', 'expansivel': True},
    'MANUTENÇÃO': {'nivel': 1, 'tipo': 'despesa', 'expansivel': True},
    'COMERCIAL & MARKETING': {'nivel': 1, 'tipo': 'despesa', 'expansivel': True},
    'Provisões e Depreciações': {'nivel': 1, 'tipo': 'despesa', 'expansivel': True},
    'Prestação de Serviços': {'nivel': 1, 'tipo': 'despesa', 'expansivel': True},
    'Aluguel de Prédios': {'nivel': 1, 'tipo': 'despesa', 'expansivel': True},
    'Outras Desp Financeiras': {'nivel': 1, 'tipo': 'despesa', 'expansivel': True},
    'TI': {'nivel': 1, 'tipo': 'despesa', 'expansivel': True},
    'Gastos ADM & Taxas': {'nivel': 1, 'tipo': 'despesa', 'expansivel': True},
    'Instalações & Utilidades': {'nivel': 1, 'tipo': 'despesa', 'expansivel': True},
    'Mobilidade': {'nivel': 1, 'tipo': 'despesa', 'expansivel': True},
    'Desp. Gerais e Administrativas': {'nivel': 0, 'tipo': 'subtotal', 'expansivel': False},
    'Lucro Operacional': {'nivel': 0, 'tipo': 'total', 'expansivel': False},
    'OUTROS MARKETING': {'nivel': 1, 'tipo': 'despesa', 'expansivel': True},
    'DESP. CORPORATIVAS': {'nivel': 1, 'tipo': 'despesa', 'expansivel': False},
    'DESP. LOGISTICA': {'nivel': 1, 'tipo': 'despesa', 'expansivel': False},
    'DESP. PRODUÇÕES & INDUSTRIAS': {'nivel': 1, 'tipo': 'despesa', 'expansivel': False},
    'LUCRO ANTES IMPOSTOS': {'nivel': 0, 'tipo': 'resultado', 'expansivel': False},
}


# ─────────────────────────────────────────────
# CACHE: lê cada arquivo Excel apenas uma vez
# ─────────────────────────────────────────────
@lru_cache(maxsize=16)
def ler_excel_cached(excel_path):
    """Lê o arquivo Excel e armazena em memória. Reutilizado em todas as requisições."""
    return pd.read_excel(excel_path, sheet_name=SHEET_NAME, header=None)


def normalize_conta(conta):
    if pd.isna(conta):
        return ''
    return str(conta).strip().lower()

def is_conta_principal(conta):
    conta_norm = normalize_conta(conta)
    for cp in CONTAS_PRINCIPAIS:
        if normalize_conta(cp) == conta_norm:
            return True
    return False

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
            'mes_abrev': mes,
            'ano': ano,
            'mes_num': MESES.get(mes, '01'),
            'mes_nome': MESES_NOME.get(MESES.get(mes, '01'), mes),
            'label': f"{mes}/{ano}",
            'sort_key': f"20{ano}{MESES.get(mes, '01')}"
        }
    return None


# ─────────────────────────────────────────────
# CACHE: lista de períodos (glob no disco)
# ─────────────────────────────────────────────
@lru_cache(maxsize=1)
def listar_periodos():
    """Lista todos os períodos disponíveis. Resultado em cache após primeira chamada."""
    periodos = {
        'consolidado': [],
        'parcial': [],
        'todos': []
    }

    if os.path.exists(CONSOLIDADO_DIR):
        for arquivo in glob(os.path.join(CONSOLIDADO_DIR, 'DRE_LOJAS_*.xlsx')):
            nome = os.path.basename(arquivo)
            periodo = extrair_periodo_arquivo(nome)
            if periodo:
                periodo['arquivo'] = arquivo
                periodo['tipo'] = 'consolidado'
                periodos['consolidado'].append(periodo)

    if os.path.exists(PARCIAL_DIR):
        for arquivo in glob(os.path.join(PARCIAL_DIR, 'DRE_LOJAS_*.xlsx')):
            nome = os.path.basename(arquivo)
            periodo = extrair_periodo_arquivo(nome)
            if periodo:
                periodo['arquivo'] = arquivo
                periodo['tipo'] = 'parcial'
                periodos['parcial'].append(periodo)

    periodos['consolidado'].sort(key=lambda x: x['sort_key'], reverse=True)
    periodos['parcial'].sort(key=lambda x: x['sort_key'], reverse=True)

    todos_periodos = {}
    for p in periodos['consolidado'] + periodos['parcial']:
        if p['label'] not in todos_periodos:
            todos_periodos[p['label']] = {
                'label': p['label'],
                'mes_nome': p['mes_nome'],
                'ano': p['ano'],
                'sort_key': p['sort_key'],
                'tem_consolidado': False,
                'tem_parcial': False
            }
        if p['tipo'] == 'consolidado':
            todos_periodos[p['label']]['tem_consolidado'] = True
        else:
            todos_periodos[p['label']]['tem_parcial'] = True

    periodos['todos'] = sorted(todos_periodos.values(), key=lambda x: x['sort_key'], reverse=True)
    return periodos


def obter_arquivo(periodo_label, tipo='consolidado'):
    diretorio = CONSOLIDADO_DIR if tipo == 'consolidado' else PARCIAL_DIR

    if not os.path.exists(diretorio):
        return None

    for arquivo in glob(os.path.join(diretorio, 'DRE_LOJAS_*.xlsx')):
        nome = os.path.basename(arquivo)
        periodo = extrair_periodo_arquivo(nome)
        if periodo and periodo['label'] == periodo_label:
            return arquivo

    return None


def listar_lojas(excel_path):
    """Usa o Excel em cache para listar lojas."""
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

    for col_idx, val in enumerate(row_lojas):
        if pd.notna(val) and isinstance(val, str):
            val_clean = val.strip()
            if 'LJ.' in val_clean or 'SUPERFÁCIL' in val_clean or 'SUPERFACIL' in val_clean:
                tipo = classificar_loja(val_clean)
                loja = {
                    'id': str(col_idx),
                    'nome': val_clean,
                    'col_inicio': col_idx,
                    'data_ref': data_ref,
                    'tipo': tipo
                }
                if tipo == 'varejo':
                    lojas_varejo.append(loja)
                else:
                    lojas_atacado.append(loja)

    return {
        'varejo': sorted(lojas_varejo, key=lambda x: x['nome']),
        'atacado': sorted(lojas_atacado, key=lambda x: x['nome']),
        'todas': sorted(lojas_varejo + lojas_atacado, key=lambda x: x['nome']),
        'data_ref': data_ref
    }


def carregar_dre(excel_path, col_inicio=None):
    """Usa o Excel em cache para carregar o DRE. Não relê o arquivo do disco."""
    df = ler_excel_cached(excel_path).copy()

    lojas = listar_lojas(excel_path)

    if col_inicio is None:
        if lojas['todas']:
            col_inicio = lojas['todas'][0]['col_inicio']
        else:
            return None

    col_inicio = int(col_inicio)

    loja_nome = df.iloc[3, col_inicio]
    tipo_loja = classificar_loja(str(loja_nome))

    data_ref_raw = df.iloc[3, 3]
    if isinstance(data_ref_raw, (pd.Timestamp, datetime)):
        data_ref = data_ref_raw.strftime('%d/%m/%Y')
    elif pd.notna(data_ref_raw):
        data_ref = str(data_ref_raw)[:10]
    else:
        data_ref = ''

    col_real_2024 = col_inicio
    col_perc_2024 = col_inicio + 1
    col_orcamento = col_inicio + 2
    col_perc_orc  = col_inicio + 3
    col_real_2025 = col_inicio + 4
    col_perc_2025 = col_inicio + 5
    col_var_orc   = col_inicio + 6
    col_var_24    = col_inicio + 7
    col_contas    = 3

    dados = []
    conta_pai = None

    def safe_float(val):
        try:
            if pd.isna(val):
                return 0.0
            return float(val)
        except:
            return 0.0

    for idx in range(5, len(df)):
        row = df.iloc[idx]
        conta = row[col_contas]

        if pd.isna(conta) or str(conta).strip() == '':
            continue

        conta_str = str(conta).strip()

        if conta_str in ['Nº de Cupons', 'Ticket Médio', 'Nº de Func.',
                         'Crescimento da Receita (%)', 'Crescimento Ticket Médio (%)',
                         'Crescimento Clientes (%)', 'COD', '2025', '2024']:
            continue

        if isinstance(conta, pd.Timestamp):
            continue

        is_principal = is_conta_principal(conta_str)
        config = get_conta_config(conta_str)

        item = {
            'conta': conta_str,
            # Valores originais
            'real_2024':   safe_float(row[col_real_2024]),
            'perc_2024':   safe_float(row[col_perc_2024]),
            'orcamento':   safe_float(row[col_orcamento]),
            'perc_orc':    safe_float(row[col_perc_orc]),
            'real_2025':   safe_float(row[col_real_2025]),
            'perc_2025':   safe_float(row[col_perc_2025]),
            'var_orc_real':safe_float(row[col_var_orc]),
            'var_25_24':   safe_float(row[col_var_24]),
            'is_principal': is_principal,
            'nivel':       config['nivel'],
            'tipo':        config['tipo'],
            'expansivel':  config['expansivel'] and is_principal,
            'conta_pai':   conta_pai if not is_principal else None,
            # Aliases usados pelo template
            'real_atual':     safe_float(row[col_real_2025]),
            'real_anterior':  safe_float(row[col_real_2024]),
            'perc_atual':     safe_float(row[col_perc_2025]),
            'perc_anterior':  safe_float(row[col_perc_2024]),
            'var_ano':        safe_float(row[col_var_24]),
        }

        if is_principal:
            conta_pai = conta_str

        dados.append(item)

    indicadores = {}
    for idx in range(len(df)-15, len(df)):
        if idx < 0:
            continue
        row = df.iloc[idx]
        conta = str(row[col_contas]).strip() if pd.notna(row[col_contas]) else ''

        if conta == 'Nº de Cupons':
            indicadores['cupons_2024'] = safe_float(row[col_real_2024])
            indicadores['cupons_2025'] = safe_float(row[col_real_2025])
        elif conta == 'Ticket Médio':
            indicadores['ticket_2024'] = safe_float(row[col_real_2024])
            indicadores['ticket_2025'] = safe_float(row[col_real_2025])
        elif conta == 'Nº de Func.':
            indicadores['func_2024'] = safe_float(row[col_real_2024])
            indicadores['func_2025'] = safe_float(row[col_real_2025])

    # Aliases de indicadores usados pelo template
    indicadores['func_atual']      = indicadores.get('func_2025', 0)
    indicadores['func_anterior']   = indicadores.get('func_2024', 0)
    indicadores['cupons_atual']    = indicadores.get('cupons_2025', 0)
    indicadores['cupons_anterior'] = indicadores.get('cupons_2024', 0)
    indicadores['ticket_atual']    = indicadores.get('ticket_2025', 0)
    indicadores['ticket_anterior'] = indicadores.get('ticket_2024', 0)

    receita     = find_conta(dados, 'Receita líquida total')
    lucro_bruto = find_conta(dados, 'Lucro bruto')
    lucro_op    = find_conta(dados, 'Lucro Operacional')
    lucro_final = find_conta(dados, 'LUCRO ANTES IMPOSTOS')

    margem_bruta   = (lucro_bruto.get('real_2025', 0) / receita.get('real_2025', 1)) * 100 if receita.get('real_2025', 0) != 0 else 0
    margem_op      = (lucro_op.get('real_2025', 0)    / receita.get('real_2025', 1)) * 100 if receita.get('real_2025', 0) != 0 else 0
    margem_liquida = (lucro_final.get('real_2025', 0) / receita.get('real_2025', 1)) * 100 if receita.get('real_2025', 0) != 0 else 0

    kpis = {
        # Chaves originais
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
        # Aliases usados pelo template index.html
        'receita_atual':     receita.get('real_2025', 0),
        'receita_anterior':  receita.get('real_2024', 0),
        'lucro_bruto_atual': lucro_bruto.get('real_2025', 0),
        'lucro_op_atual':    lucro_op.get('real_2025', 0),
        'lucro_final_atual': lucro_final.get('real_2025', 0),
    }

    # ── Anos dinâmicos: extraídos do nome do arquivo ──
    # Ex: DRE_LOJAS_FEV26.xlsx → ano_atual=2026, ano_anterior=2025
    # Ex: DRE_LOJAS_NOV25.xlsx → ano_atual=2025, ano_anterior=2024
    periodo_info = extrair_periodo_arquivo(os.path.basename(excel_path))
    if periodo_info:
        ano_num          = int('20' + periodo_info['ano'])
        ano_atual_str    = str(ano_num)
        ano_anterior_str = str(ano_num - 1)
        mes_nome_atual   = periodo_info['mes_nome']
        mes_abrev_atual  = periodo_info['mes_abrev']
    else:
        ano_atual_str    = '2025'
        ano_anterior_str = '2024'
        mes_nome_atual   = ''
        mes_abrev_atual  = ''

    return {
        'data_ref':       data_ref,
        'loja':           loja_nome if pd.notna(loja_nome) else '',
        'tipo_loja':      tipo_loja,
        'col_inicio':     col_inicio,
        'dados':          dados,
        'indicadores':    indicadores,
        'kpis':           kpis,
        'lojas':          lojas,
        'ano_atual':      ano_atual_str,
        'ano_anterior':   ano_anterior_str,
        'mes_nome_atual': mes_nome_atual,
        'mes_abrev_atual': mes_abrev_atual,
    }


@app.route('/')
def index():
    try:
        periodo = request.args.get('periodo')
        tipo    = request.args.get('tipo', 'consolidado')
        loja_id = request.args.get('loja')

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
                                   lojas={'varejo': [], 'atacado': [], 'todas': []},
                                   periodos=periodos,
                                   periodo_selecionado=periodo,
                                   tipo_selecionado=tipo,
                                   loja_selecionada=None,
                                   erro="Nenhum arquivo DRE encontrado.")

        col_inicio = int(loja_id) if loja_id else None
        dre_data   = carregar_dre(excel_path, col_inicio)

        if not dre_data:
            return render_template('index.html',
                                   dre=None,
                                   lojas={'varejo': [], 'atacado': [], 'todas': []},
                                   periodos=periodos,
                                   periodo_selecionado=periodo,
                                   tipo_selecionado=tipo,
                                   loja_selecionada=None,
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
                               erro=None)

    except Exception as e:
        logging.error("ERRO NA ROTA /: %s", traceback.format_exc())
        return f"<h2>Erro interno</h2><pre>{traceback.format_exc()}</pre>", 500


@app.route('/api/periodos')
def api_periodos():
    periodos = listar_periodos()
    return jsonify(periodos)


@app.route('/api/dre')
def api_dre():
    periodo = request.args.get('periodo')
    tipo    = request.args.get('tipo', 'consolidado')
    loja_id = request.args.get('loja')

    excel_path = obter_arquivo(periodo, tipo)
    if not excel_path:
        return jsonify({'erro': 'Arquivo não encontrado'}), 404

    col_inicio = int(loja_id) if loja_id else None
    dre_data   = carregar_dre(excel_path, col_inicio)

    if dre_data:
        dre_data['periodo']      = periodo
        dre_data['tipo_arquivo'] = tipo

    return jsonify(dre_data)


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)