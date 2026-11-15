<#
.SYNOPSIS
  Drive icefleet's Linux dev container from Windows (PLAN decision 28,
  docs/PLAN_linux_dev_environment.md). Every build, test and tool run happens in it.

.DESCRIPTION
  .\scripts\dev.ps1 up                     build the image if needed, start the container
  .\scripts\dev.ps1 setup [auto|online|wheelhouse]
                                           create / refresh the Linux venv. auto (default)
                                           tries `uv sync` in the container and falls back to
                                           the wheelhouse if container downloads fail
  .\scripts\dev.ps1 shell                  interactive bash in the container
  .\scripts\dev.ps1 down                   stop and remove the container (volumes are kept)
  .\scripts\dev.ps1 <command ...>          run it in the container, from /work:
                                             .\scripts\dev.ps1 uv run tox
                                             .\scripts\dev.ps1 uv run pytest -q -k splits

  Windows PowerShell 5.1 strips double quotes inside arguments to native programs, so a
  command that needs quoting goes into a script file run in the container, or into `shell`.
#>

$all = @($args)
$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$compose = @('compose', '-f', (Join-Path $root 'deploy\dev\compose.yml'))
$network = 'iceberg-nyc_iceberg_net'
if ($env:ICEFLEET_DEV_CATALOG_NETWORK) { $network = $env:ICEFLEET_DEV_CATALOG_NETWORK }

function Show-Usage {
    Write-Host 'usage: .\scripts\dev.ps1 up | setup [auto|online|wheelhouse] | shell | down | <command ...>'
}

if ($all.Count -eq 0) { Show-Usage; exit 2 }
$verb = $all[0]
$rest = @($all | Select-Object -Skip 1)

switch ($verb) {
    'up' {
        # The wheelhouse is bind-mounted; create it so Docker does not.
        New-Item -ItemType Directory -Force (Join-Path $root '.wheelhouse') | Out-Null
        $null = docker network inspect $network 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Host "dev: Docker network '$network' not found -- start the dev catalog stack (iceberg-nyc) first," -ForegroundColor Red
            Write-Host '     or set ICEFLEET_DEV_CATALOG_NETWORK to its network name.' -ForegroundColor Red
            exit 1
        }
        & docker @compose up -d --build
        exit $LASTEXITCODE
    }
    'down' {
        & docker @compose down
        exit $LASTEXITCODE
    }
    'setup' {
        $mode = 'auto'
        if ($rest.Count -gt 0) { $mode = $rest[0] }
        if ($mode -notin @('auto', 'online', 'wheelhouse')) { Show-Usage; exit 2 }
        if ($mode -ne 'wheelhouse') {
            & docker @compose exec -T dev devsetup online
            if ($LASTEXITCODE -eq 0) { exit 0 }
            if ($mode -eq 'online') { exit $LASTEXITCODE }
            Write-Host 'dev: online setup failed (container downloads?) -- falling back to the wheelhouse' -ForegroundColor Yellow
        }
        & (Join-Path $PSScriptRoot 'wheelhouse.ps1')
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        & docker @compose exec -T dev devsetup wheelhouse
        exit $LASTEXITCODE
    }
    'shell' {
        & docker @compose exec dev devrun bash
        exit $LASTEXITCODE
    }
    default {
        & docker @compose exec -T dev devrun @all
        exit $LASTEXITCODE
    }
}
