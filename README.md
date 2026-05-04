# DRE Interativo - Grupo Nordestão

Sistema web para visualização e análise de Demonstração do Resultado do Exercício (DRE).

## 🚀 Funcionalidades

- **Múltiplos Períodos**: Selecione o mês que deseja analisar
- **Consolidado vs Parcial**: Visualize dados fechados ou em evolução semanal
- **Comparativos Dinâmicos**: Anos calculados automaticamente com base no período
- **13 Unidades**: 9 lojas Varejo (Nordestão) + 4 lojas Atacado (Superfácil)
- **KPIs Visuais**: Receita, Lucro Bruto, Lucro Operacional, Margens

## 📁 Estrutura de Pastas

```
dre_nordestao/
├── app.py
├── consolidado/           ← Arquivos DRE mensais fechados
│   ├── DRE_LOJAS_JAN25.xlsx
│   └── DRE_LOJAS_FEV26.xlsx
├── parcial/               ← Arquivos DRE em atualização semanal
│   └── DRE_LOJAS_MAR26.xlsx
├── static/
│   └── logo.png
└── templates/
    └── index.html
```

## 📋 Padrão de Nomenclatura

```
DRE_LOJAS_MÊSANO.xlsx
```

Exemplos:
- `DRE_LOJAS_JAN25.xlsx` → Janeiro/2025
- `DRE_LOJAS_FEV26.xlsx` → Fevereiro/2026

## 🔄 Comparativos Dinâmicos

O sistema calcula automaticamente os anos de comparação:

| Arquivo | Ano Atual | Ano Anterior | Comparativo |
|---------|-----------|--------------|-------------|
| FEV26 | 2026 | 2025 | Fev/26 vs Fev/25 |
| DEZ25 | 2025 | 2024 | Dez/25 vs Dez/24 |
| MAI25 | 2025 | 2024 | Mai/25 vs Mai/24 |

**Cabeçalhos da tabela:**
- Real {ano_anterior}
- Orçamento
- Real {ano_atual}
- {ano_atual} x {ano_anterior}

## 🔧 Instalação

```bash
pip install flask pandas openpyxl
python app.py
```

Acesse: `http://localhost:5000`

---

Grupo Nordestão — Sistema de Gestão Financeira

git add "informar o nome do arquivo"
git commit -m "assunto do commit"
git push