$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$EnvFile = Join-Path $RepoRoot ".env.local"

Set-Location $RepoRoot

if (Test-Path $EnvFile) {
    Get-Content $EnvFile | ForEach-Object {
        $Line = $_.Trim()
        if (-not $Line -or $Line.StartsWith("#") -or -not $Line.Contains("=")) {
            return
        }

        $Name, $Value = $Line -split "=", 2
        $Name = $Name.Trim()
        $Value = $Value.Trim().Trim('"').Trim("'")

        if ($Name) {
            Set-Item -Path "Env:$Name" -Value $Value
        }
    }
}

if (-not $env:NEXT_TEST_WASM_DIR) {
    $env:NEXT_TEST_WASM_DIR = "./node_modules/@next/swc-wasm-nodejs"
}

pnpm exec next dev
