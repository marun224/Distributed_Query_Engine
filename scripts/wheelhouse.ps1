<#
.SYNOPSIS
  Download the locked Linux wheels into .wheelhouse\ on Windows, for when the dev container
  cannot download them itself (docs/PLAN_linux_dev_environment.md §5).

.DESCRIPTION
  uv.lock -> requirements for every extra and the dev group -> narrowed to Linux x86_64 /
  CPython 3.12 -> `pip download` of exactly those wheels, plus hatchling and editables for
  the editable build. Fails if any Linux pin differs from the lock. Uses the Windows .venv's uv and pip
  (both run under Smart App Control). Normally called by `.\scripts\dev.ps1 setup`.

  .\scripts\wheelhouse.ps1           add what is missing (already-downloaded wheels are kept)
  .\scripts\wheelhouse.ps1 -Clean    empty .wheelhouse\ first
#>
param([switch]$Clean)

$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$wh = Join-Path $root '.wheelhouse'
$uv = Join-Path $root '.venv\Scripts\uv.exe'
$py = Join-Path $root '.venv\Scripts\python.exe'
$all = Join-Path $wh 'requirements-all.txt'
$linux = Join-Path $wh 'requirements-linux.txt'

if ($Clean -and (Test-Path $wh)) { Get-ChildItem $wh -File | Remove-Item -Force }
New-Item -ItemType Directory -Force $wh | Out-Null

Push-Location $root
try {
    & $uv export --frozen --all-extras --no-emit-project --no-header --no-hashes -q -o $all
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    & $uv pip compile $all --python-platform x86_64-manylinux_2_28 --python-version 3.12 --no-header --no-annotate -q -o $linux
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
} finally { Pop-Location }

# Every Linux pin must be the locked version: the narrowing step may drop packages, never move one.
$locked = @{}
foreach ($line in Get-Content $all) {
    if ($line -match '^([A-Za-z0-9_.\-]+)==([^ ;\\]+)') { $locked[$matches[1].ToLower()] = $matches[2] }
}
$drift = @(Get-Content $linux | Where-Object { $_ -match '^([A-Za-z0-9_.\-]+)==(\S+)' -and $locked[$matches[1].ToLower()] -ne $matches[2] })
if ($drift.Count -gt 0) {
    Write-Host "wheelhouse: Linux pins differ from uv.lock: $($drift -join ', ')" -ForegroundColor Red
    exit 1
}

$platform = @('--only-binary=:all:', '--python-version', '3.12', '--implementation', 'cp',
    '--abi', 'cp312', '--abi', 'abi3', '--abi', 'none',
    '--platform', 'manylinux_2_28_x86_64', '--platform', 'manylinux_2_17_x86_64',
    '--platform', 'manylinux2014_x86_64', '--platform', 'manylinux_2_5_x86_64',
    '--platform', 'linux_x86_64', '--platform', 'any')
& $py -m pip download -q --no-deps -r $linux -d $wh @platform
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
# The build backend for `pip install -e`: hatchling, plus `editables` for the editable build.
& $py -m pip download -q hatchling editables -d $wh --only-binary=:all: --python-version 3.12 --implementation py --platform any
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$wheels = @(Get-ChildItem $wh -Filter *.whl)
$mb = [int](($wheels | Measure-Object Length -Sum).Sum / 1MB)
Write-Host "wheelhouse: $($wheels.Count) wheels, $mb MB in .wheelhouse\"
exit 0
