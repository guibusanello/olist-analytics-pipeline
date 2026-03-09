# Contributing

## Pré-requisitos

- Python 3.12+
- pyenv (recomendado)
- Git

## Configuração do ambiente

1. Clone o repositório:
```bash
   git clone https://github.com/seu-usuario/olist-analytics-pipeline.git
   cd olist-analytics-pipeline
```

2. Crie e ative o ambiente virtual:
```bash
   python -m venv .venv
   source .venv/bin/activate
```

3. Instale as dependências:
```bash
   pip install -r requirements.txt
```

4. Configure as variáveis de ambiente criando um arquivo `.env` na raiz:
```
   RAW_DATA_PATH=data/raw
   DUCKDB_PATH=data/olist.duckdb
```

5. Baixe o dataset do Olist no [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) e coloque os CSVs em `data/raw/`.

## Rodando o pipeline

1. Ingestão dos dados:
```bash
   python ingestion/load_to_duckdb.py
```

2. Entre na pasta do projeto dbt:
```bash
   cd olist_analytics
```

3. Instale os pacotes dbt:
```bash
   dbt deps
```

4. Rode os modelos:
```bash
   dbt run
```

5. Rode os testes:
```bash
   dbt test
```

6. Gere a documentação:
```bash
   dbt docs generate
   dbt docs serve
```

## Estrutura do projeto
```
olist-analytics-pipeline/
├── ingestion/          # Script de ingestão dos CSVs para DuckDB
├── olist_analytics/    # Projeto dbt
│   ├── models/
│   │   ├── staging/        # Limpeza e tipagem das fontes
│   │   ├── intermediate/   # Lógica de negócio intermediária
│   │   └── marts/          # Tabelas analíticas finais
│   ├── macros/         # Macros reutilizáveis
│   └── tests/          # Testes customizados
└── data/
    └── raw/            # CSVs do Olist (não versionados)
```

## Convenções

- Modelos staging: prefixo `stg_`
- Modelos intermediate: prefixo `int_`
- Tabelas fato: prefixo `fct_`
- Tabelas dimensão: prefixo `dim_`
- Marts analíticos: prefixo `mart_`