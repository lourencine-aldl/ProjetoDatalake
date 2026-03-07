# 🔍 Diagnóstico: Tabela Criada Mas Sem Dados

## Problema
Tabela `public.silver_tab_faturamento_full` foi criada no banco, mas nenhum dado foi inserido.

---

## 🚀 Passos para Diagnóstico

### 1️⃣ Execute o script de debug
```bash
cd /home/Projetos/ProjetosDocker/Orquestradores/airflow/dags/Sost/scripts/Datalake/Silver/
python debug_faturamento.py
```

Isso vai analisar:
- ✅ Se dados existem no Bronze (MinIO)
- ✅ Se a tabela existe no banco
- ✅ Quantas linhas estão na tabela
- ✅ Se houver PK nula nos dados

---

## 🎯 Possíveis Causas e Soluções

### Causa 1: **Dados sendo filtrados por PK nula**
Se o debug mostrar "Linhas descartadas por PK nula: XXXX":
- Verifique se as colunas PK têm nomes diferentes no Bronze
- Ajuste o mapeamento de aliases em `ALIASES` dict

**Colunas PK obrigatórias:**
- `codfilial`
- `data_faturamento`
- `numnota`
- `codigo`
- `codcli`

**Solução:** Adicione ao `ALIASES` dict:
```python
ALIASES: Dict[str, str] = {
    # ... existentes ...
    "seu_nome_coluna_pk": "codfilial",  # por exemplo
}
```

---

### Causa 2: **Todos os dados sendo marcados como "vazios"**
Se o debug mostrar "Linhas descartadas por estar vazias: XXXX":
- A função `drop_invalid_rows()` marca linhas como vazias incorretamente
- Comente temporariamente a lógica de "linhas vazias"

**Solução temporária** em `drop_invalid_rows()`:
```python
# Comentar esta seção se achar que tá matando dados válidos:
# meta_cols = {"_ingestion_ts_utc", "_ingestion_date", "_silver_ts_utc", "created_at"}
# data_cols = [c for c in out.columns if c not in meta_cols]
# if data_cols:
#     all_null_data = out[data_cols].isna().all(axis=1)
#     out = out.loc[~all_null_data].copy()
```

---

### Causa 3: **Bronze vazio (dados não foram carregados)**
Se `debug_faturamento.py` mostrar 0 linhas no Bronze:
- Execute o Bronze primeiro:
```bash
python /home/Projetos/ProjetosDocker/Orquestradores/airflow/dags/Sost/scripts/Datalake/Bronze/005_bronze_tab_faturamento_full.py
```
- Verify no MinIO ou Dremio que os dados chegram lá

---

### Causa 4: **Conversão de tipos falhando**
Se PK existem mas dados ainda não inserem:
- Aumenta verbosidade dos logs na função `_to_int_series()` e `_to_decimal_maybe()`
- Adicione:
```python
logger.debug(f"Convertendo {col}: {out[col].head(3).tolist()}")
```

---

## 🔧 Executando Silver novamente (após correção)

```bash
cd /home/Projetos/ProjetosDocker/Orquestradores/airflow/dags/Sost/scripts/Datalake/Silver/

# Ver logs enquanto executa:
python 005_silver_tab_faturamento_full.py 2>&1 | tee silver_run.log

# Depois verificar quantas linhas foram inseridas:
grep "total linhas inseridas" silver_run.log
```

---

## 📋 Checklist de Diagnóstico

- [ ] Executar `debug_faturamento.py` e salvar output
- [ ] Verificar se Bronze tem dados
- [ ] Verificar estrutura de colunas no Bronze vs esperado
- [ ] Conferir nomes de colunas PK no Bronze (podem estar diferentes)
- [ ] Executar Silver com logs e verificar quantas linhas são filtradas
- [ ] Se dados forem filtrados, ver o motivo (PK neta ou linhas vazias)
- [ ] Ajustar `ALIASES` ou comentar filtros conforme necessário
- [ ] Re-executar Silver
- [ ] Conferir contagem final com: `SELECT COUNT(*) FROM public.silver_tab_faturamento_full;`

---

## 📞 Informações Úteis

**Arquivo Silver:** 
`/home/Projetos/ProjetosDocker/Orquestradores/airflow/dags/Sost/scripts/Datalake/Silver/005_silver_tab_faturamento_full.py`

**Arquivo Bronze:**
`/home/Projetos/ProjetosDocker/Orquestradores/airflow/dags/Sost/scripts/Datalake/Bronze/005_bronze_tab_faturamento_full.py`

**Config:**
`/home/Projetos/ProjetosDocker/Orquestradores/airflow/.env`

**Logs recentes:**
`/home/Projetos/ProjetosDocker/Orquestradores/airflow/logs/dag_id=dag_silver_sost/`
