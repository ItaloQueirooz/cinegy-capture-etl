<#
    SCRIPT DE DUMP CINEGY (SQL COMPACT) - VERSÃO FINAL
    -----------------------------------------------------
    Correções aplicadas: 
    1. Erro de sintaxe (variável com dois pontos).
    2. Erro de comparação de campos vazios.
    3. Bloqueio de arquivos.
#>

# --- 1. CONFIGURAÇÃO INICIAL ---
Clear-Host
Write-Host "--- GERADOR DE DUMP CINEGY (SDF) ---" -ForegroundColor Cyan
Write-Host "Versão Final Corrigida" -ForegroundColor Gray
Write-Host ""

# Inputs do usuário
$cliente = Read-Host "Digite o nome do cliente"
$dbNameInput = Read-Host "Digite o nome do arquivo SDF (ex: 201901101419360)"

if ($dbNameInput -notlike "*.sdf") { $sdfFileName = "$dbNameInput.sdf" }
else { $sdfFileName = $dbNameInput }

if (-not (Test-Path $sdfFileName)) {
    Write-Error "ERRO: O arquivo '$sdfFileName' não foi encontrado nesta pasta."
    Pause; Exit
}

$dataStr = Get-Date -Format "yyyy-MM-dd"
$dumpName = "${cliente}-${dataStr}"

# --- 2. CARREGAR DLL ---
try {
    # Tenta sistema
    [Reflection.Assembly]::Load("System.Data.SqlServerCe, Version=4.0.0.0, Culture=neutral, PublicKeyToken=89845dcd8080cc91") | Out-Null
    Write-Host "Driver carregado do sistema." -ForegroundColor DarkGray
}
catch {
    # Tenta local
    $localDll = Join-Path (Get-Location) "System.Data.SqlServerCe.dll"
    if (Test-Path $localDll) { [Reflection.Assembly]::LoadFile($localDll) | Out-Null }
    else {
        Write-Error "ERRO: Driver SQL Server Compact não encontrado (Sistema ou Local)."
        Pause; Exit
    }
}

# --- 3. PREPARAR PASTAS ---
Write-Host "Criando pastas..." -ForegroundColor Yellow
$folders = @("tables", "unl") 
foreach ($f in $folders) {
    if (Test-Path $f) { Remove-Item $f -Recurse -Force }
    New-Item -ItemType Directory -Path $f | Out-Null
}

# --- 4. PROCESSAMENTO ---
$connString = "Data Source='$sdfFileName';Max Database Size=4091"
$conn = New-Object System.Data.SqlServerCe.SqlCeConnection($connString)

try {
    $conn.Open()
    Write-Host "Conectado ao banco. Iniciando extração..." -ForegroundColor Green

    $cmdTables = $conn.CreateCommand()
    $cmdTables.CommandText = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'TABLE'"
    $readerTables = $cmdTables.ExecuteReader()
    $listaTabelas = @()
    while ($readerTables.Read()) { $listaTabelas += $readerTables["TABLE_NAME"] }
    $readerTables.Close()

    foreach ($tabela in $listaTabelas) {
        Write-Host "Processando: $tabela..."

        # === A. GERAR SCHEMA ===
        $arquivoSchema = "tables\$tabela.txt"
        $swSchema = $null
        
        try {
            $swSchema = New-Object System.IO.StreamWriter($arquivoSchema, $false, [System.Text.Encoding]::UTF8)
            
            $cmdSchema = $conn.CreateCommand()
            $cmdSchema.CommandText = "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '$tabela' ORDER BY ORDINAL_POSITION"
            $rSchema = $cmdSchema.ExecuteReader()

            $swSchema.WriteLine("{ SCHEMA AUTOMATICO PARA: $tabela }")
            $swSchema.WriteLine("create table $tabela (")
            while ($rSchema.Read()) {
                $col = $rSchema["COLUMN_NAME"]
                $type = $rSchema["DATA_TYPE"]
                $len = $rSchema["CHARACTER_MAXIMUM_LENGTH"]
                
                # Validação de tamanho para evitar erro de comparação
                if ($len -ne [DBNull]::Value -and $len -ne $null) {
                     $lenInt = 0
                     if ([int]::TryParse($len.ToString(), [ref]$lenInt)) {
                        if ($lenInt -gt 0) { $type = "$type($lenInt)" }
                     }
                }

                $swSchema.WriteLine("  $col $type,")
            }
            $swSchema.WriteLine(");")
        }
        catch {
            # CORREÇÃO AQUI: ${tabela}
            Write-Warning "Erro ao gerar schema da tabela ${tabela}: $_"
        }
        finally {
            if ($rSchema -ne $null) { $rSchema.Close() }
            if ($swSchema -ne $null) { $swSchema.Close(); $swSchema.Dispose() }
        }

        # === B. GERAR DADOS ===
        $arquivoDados = "unl\$tabela.unl"
        $swData = $null
        
        try {
            $swData = New-Object System.IO.StreamWriter($arquivoDados, $false, [System.Text.Encoding]::UTF8)

            $cmdData = $conn.CreateCommand()
            $cmdData.CommandText = "SELECT * FROM [$tabela]"
            $rData = $cmdData.ExecuteReader()
            $fieldCount = $rData.FieldCount

            while ($rData.Read()) {
                $linhaArray = @()
                for ($i = 0; $i -lt $fieldCount; $i++) {
                    $valor = $rData.GetValue($i)
                    if ($valor -eq [DBNull]::Value) { $valor = "" }
                    else { 
                        $strVal = $valor.ToString()
                        $strVal = $strVal -replace "`r", "" -replace "`n", " " 
                        $valor = $strVal
                    }
                    $linhaArray += $valor
                }
                $linhaFinal = ($linhaArray -join "|") + "|" 
                $swData.WriteLine($linhaFinal)
            }
        }
        catch {
             # CORREÇÃO AQUI: ${tabela}
             Write-Warning "Erro ao exportar dados da tabela ${tabela}: $_"
        }
        finally {
             if ($rData -ne $null) { $rData.Close() }
             if ($swData -ne $null) { $swData.Close(); $swData.Dispose() }
        }
    }
}
catch {
    Write-Error "Erro Geral: $($_.Exception.Message)"
}
finally {
    if ($conn.State -eq 'Open') { $conn.Close() }
    
    # Força limpeza da memória
    [System.GC]::Collect()
    [System.GC]::WaitForPendingFinalizers()
}

# --- 5. COMPACTAÇÃO ---
Write-Host "Compactando arquivos (ZIP)..." -ForegroundColor Yellow
$zipFile = "$dumpName.zip"
if (Test-Path $zipFile) { Remove-Item $zipFile }

Start-Sleep -Seconds 2 

try {
    Compress-Archive -Path "tables", "unl" -DestinationPath $zipFile -ErrorAction Stop
}
catch {
    Write-Error "Erro ao criar ZIP. Verifique se os arquivos foram gerados nas pastas tables/unl."
    Write-Error $_
}

# --- 6. LIMPEZA ---
Write-Host "Limpando pastas temporárias..."
try {
    Remove-Item "tables" -Recurse -Force -ErrorAction SilentlyContinue
    Remove-Item "unl" -Recurse -Force -ErrorAction SilentlyContinue
} catch {}

Write-Host "--- SUCESSO! ---" -ForegroundColor Green
Write-Host "Arquivo gerado: $zipFile"
Pause