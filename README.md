# 🎥 ETL e Análise de Logs - Cinegy Capture Engine

## 📌 Sobre o Projeto
Este projeto documenta um pipeline de dados (ETL) construído para investigar e auditar falhas de gravação em servidores de captura de vídeo (Cinegy). O objetivo foi extrair registros de quatro bancos de dados isolados, cruzar as informações e gerar um relatório final consolidado para identificar a causa raiz de arquivos perdidos (ex: evento Copa Davis).

A análise dos dados extraídos revelou que não havia falha sistêmica, mas sim um **erro operacional de roteamento**: arquivos MXF e MP4 estavam sendo salvos em diretórios divergentes das regras de workflow (ex: `\live\MP4_ALTA\` vs `\edicao\MXF\`).

## 🛠️ Tecnologias e Ferramentas
* **PowerShell:** Automação da extração (Dump) dos bancos de dados locais.
* **Python (Pandas):** Processamento dos arquivos desestruturados (UNL/Tables), limpeza, cruzamento de dados e exportação.
* **CompactView / SQL Server Compact:** Visualização e acesso aos bancos `.sdf`.

## ⚙️ Arquitetura da Solução (Pipeline)

1. **Extração (Extract):** Os bancos de dados originais ficam ocultos no diretório do Cinegy no formato `.sdf` (SQL Server Compact). O script `dumpcinegy.ps1` foi criado para isolar o banco (garantindo que o serviço não esteja em uso) e gerar um dump compactado `.zip` contendo os dados brutos separados por tabelas e arquivos `.unl`.
   
2. **Transformação (Transform):** O script `processar_arquivos.py` lê os dumps dos 4 servidores (Cinegy 1 a 4), padroniza os campos necessários de status (ex: "FINALIZADO") e mapeia os caminhos dinâmicos dos arquivos gerados, lidando com as variações de Workflow (Ingest, Clip, Live) e Codec (MP4 vs MXF).

3. **Carga e Análise (Load):** O Python compila as informações processadas e gera uma planilha consolidada (`CinegyLGrade.xlsx`). Foi através desta planilha que a trilha dos arquivos pôde ser auditada com clareza.

## 🚀 Como Executar

> **Nota:** Por questões de confidencialidade de dados (LGPD e políticas internas), os arquivos de banco de dados (`.sdf`), os dumps gerados (`.zip`) e a planilha de saída final não estão versionados neste repositório.

1. Instale as dependências de ambiente (DLLs do `SSCERuntime` caso deseje ler os `.sdf` manualmente).
2. Execute o dump via PowerShell no servidor alvo:
   ```powershell
   .\scripts\dumpcinegy.ps1
