<#
.SYNOPSIS
  Build icefleet's runtime image offline, check it with networking off, and optionally load
  it into the kind cluster's node (PLAN §3.5, Phase 6 kickoff answers 5 and 6).

.DESCRIPTION
  1. Stage into .scratch\image\ (in the dev container): icefleet's wheel (`uv build`) and
     requirements-runtime.txt (`uv export --frozen --no-dev`: the runtime closure only).
  2. Stage httpfs.duckdb_extension.gz for the locked DuckDB version, downloaded on Windows
     into .wheelhouse\duckdb-extensions\ the first time (container downloads are flaky).
  3. docker build deploy/Dockerfile with the wheelhouse and the stage as build contexts;
     tags icefleet:<version>-<git sha>[-dirty] and icefleet:dev.
  4. docker run --network none ... check_image.py: imports, httpfs offline, non-root.
  5. -Load: docker save -> docker cp -> ctr import on the kind node (no registry, no kind
     CLI); imagePullPolicy: Never in the manifests.

  .\scripts\image.ps1                build + check
  .\scripts\image.ps1 -Load          build + check + load into the kind node
  .\scripts\image.ps1 -LoadOnly      load the current icefleet:dev without rebuilding
  .\scripts\image.ps1 -Test -Load    also build icefleet-test (runtime + pytest + py_test)
                                     for the integration tier on the cluster

  Prints the exact tag on the last line (scripts\k8s.ps1 run ... image=<tag>).
#>
param(
    [switch]$Load,
    [switch]$Test,
    [switch]$LoadOnly,
    [string]$Node = $(if ($env:ICEFLEET_KIND_NODE) { $env:ICEFLEET_KIND_NODE } else { 'temporal-poc-control-plane' })
)

$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$stage = Join-Path $root '.scratch\image'
$wheelhouse = Join-Path $root '.wheelhouse'
$dev = Join-Path $PSScriptRoot 'dev.ps1'

function Invoke-Native([string]$what, [scriptblock]$block) {
    & $block
    if ($LASTEXITCODE -ne 0) { throw "image: $what failed (exit $LASTEXITCODE)" }
}

function Import-ToNode([string[]]$tags) {
    # docker save | ctr import through files: Windows PowerShell 5.1 pipes native output as
    # text, which would corrupt the tar.
    $tar = Join-Path $stage 'icefleet-image.tar'
    Invoke-Native 'docker save' { docker save -o $tar @tags }
    Invoke-Native 'docker cp' { docker cp $tar "${Node}:/var/tmp/icefleet-image.tar" }
    try {
        Invoke-Native 'ctr import' { docker exec $Node ctr -n k8s.io images import /var/tmp/icefleet-image.tar }
    } finally {
        docker exec $Node rm -f /var/tmp/icefleet-image.tar | Out-Null
        Remove-Item -Force $tar -ErrorAction SilentlyContinue
    }
    Write-Host "image: loaded into ${Node}: $($tags -join ', ')"
}

if ($LoadOnly) {
    # As JSON: Windows PowerShell 5.1 strips the double quotes an `index` template needs.
    $labels = (docker image inspect icefleet:dev --format '{{json .Config.Labels}}') | ConvertFrom-Json
    $tag = $labels.'org.opencontainers.image.version'
    $tags = @('icefleet:dev')
    if ($tag -and $tag -ne 'unknown') { $tags += "icefleet:$tag" }
    New-Item -ItemType Directory -Force $stage | Out-Null
    Import-ToNode $tags
    Write-Output $tags[-1]
    exit 0
}

# --- 1. stage icefleet's wheel and the runtime requirements ------------------------------
if (Test-Path $stage) { Remove-Item -Recurse -Force $stage }
New-Item -ItemType Directory -Force $stage | Out-Null
if (-not (Test-Path (Join-Path $wheelhouse 'requirements-linux.txt'))) {
    Invoke-Native 'wheelhouse' { & (Join-Path $PSScriptRoot 'wheelhouse.ps1') }
}
Invoke-Native 'uv build' { & $dev uv build --wheel --out-dir .scratch/image }
Invoke-Native 'uv export' { & $dev uv export --frozen --no-dev --no-emit-project --no-hashes --no-header -q -o .scratch/image/requirements-runtime.txt }

$requirements = Get-Content (Join-Path $stage 'requirements-runtime.txt')
$duckdbLine = $requirements | Where-Object { $_ -match '^duckdb==([0-9.]+)' } | Select-Object -First 1
if (-not $duckdbLine) { throw 'image: duckdb is not pinned in requirements-runtime.txt' }
$null = $duckdbLine -match '^duckdb==([0-9.]+)'
$duckdbVersion = $matches[1]
$version = (Select-String -Path (Join-Path $root 'pyproject.toml') -Pattern '^version = "([^"]+)"').Matches[0].Groups[1].Value

# --- 2. httpfs for the locked DuckDB, downloaded once on Windows --------------------------
$extDir = Join-Path $wheelhouse "duckdb-extensions\v$duckdbVersion\linux_amd64"
$ext = Join-Path $extDir 'httpfs.duckdb_extension.gz'
if (-not (Test-Path $ext)) {
    New-Item -ItemType Directory -Force $extDir | Out-Null
    $url = "https://extensions.duckdb.org/v$duckdbVersion/linux_amd64/httpfs.duckdb_extension.gz"
    Write-Host "image: downloading $url"
    $ProgressPreference = 'SilentlyContinue'
    Invoke-WebRequest -UseBasicParsing -Uri $url -OutFile "$ext.part"
    Move-Item -Force "$ext.part" $ext
}
Copy-Item $ext (Join-Path $stage 'httpfs.duckdb_extension.gz')

# --- 3. build ----------------------------------------------------------------------------
Push-Location $root
try {
    $sha = (git rev-parse --short HEAD).Trim()
    if (git status --porcelain) { $sha = "$sha-dirty" }
} finally { Pop-Location }
$tag = "icefleet:$version-$sha"

function Build-Target([string]$target, [string[]]$tags) {
    $tagArgs = foreach ($t in $tags) { '-t'; $t }
    Invoke-Native "docker build ($target)" {
        docker build -f (Join-Path $root 'deploy\Dockerfile') --target $target `
            --build-context "wheels=$wheelhouse" --build-context "stage=$stage" `
            --build-arg "VERSION=$version-$sha" --build-arg "REVISION=$sha" `
            --provenance=false --sbom=false `
            @tagArgs (Join-Path $root 'deploy\image')
    }
}

Build-Target 'runtime' @($tag, 'icefleet:dev')
$loaded = @($tag, 'icefleet:dev')
if ($Test) {
    # The tests image: py_test, the scripts the tier imports, pyproject.toml (pytest's config).
    $tests = Join-Path $stage 'tests'
    New-Item -ItemType Directory -Force (Join-Path $tests 'scripts') | Out-Null
    Copy-Item -Recurse (Join-Path $root 'py_test') $tests
    Get-ChildItem -Recurse -Directory -Filter __pycache__ $tests | Remove-Item -Recurse -Force
    Copy-Item (Join-Path $root 'scripts\*.py') (Join-Path $tests 'scripts')
    Copy-Item (Join-Path $root 'pyproject.toml') $tests
    $testTag = "icefleet-test:$version-$sha"
    Build-Target 'test' @($testTag, 'icefleet-test:dev')
    $loaded += @($testTag, 'icefleet-test:dev')
}

# --- 4. check, offline ---------------------------------------------------------------------
Invoke-Native 'image check' { docker run --rm --network none --entrypoint python $tag /opt/icefleet-image/check_image.py }
$size = [int]((docker image inspect $tag --format '{{.Size}}') / 1MB)
Write-Host "image: $tag built and checked ($size MB)"

# --- 5. into the cluster -------------------------------------------------------------------
if ($Load) { Import-ToNode $loaded }
Write-Output $tag
