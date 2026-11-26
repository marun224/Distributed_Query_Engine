<#
.SYNOPSIS
  Drive icefleet on the dev Kubernetes cluster (kind `temporal-poc`, decision 29) from
  Windows, through the node's own kubectl (deploy\k8s\node.sh) -- no host route to the API
  server needed, no kind / argo CLI.

.DESCRIPTION
  .\scripts\k8s.ps1 setup                 namespace, RBAC, fleet template, Secrets, job ConfigMap
  .\scripts\k8s.ps1 secrets               (re)create the Secrets from .env, hosts rewritten
  .\scripts\k8s.ps1 jobs                  (re)publish scripts as the ConfigMap icefleet-jobs
  .\scripts\k8s.ps1 run <script> [k=v...] one fleet-per-workflow run (model B): submit the
                                          Workflow, wait, print the coordinator's log, check
                                          that no pod or Service of it is left. k=v are the
                                          template's parameters: image=, workers=, worker-memory=,
                                          script-args=, ... (deploy\argo\fleet.yaml)
  .\scripts\k8s.ps1 workers up|down       model A's standing fleet
  .\scripts\k8s.ps1 status
  .\scripts\k8s.ps1 kubectl <args...>     the node's kubectl

  The image must be in the node first: .\scripts\image.ps1 -Load.
  Secrets go from .env to the node over stdin, held in RAM there (/dev/shm) and removed;
  only localhost / 127.0.0.1 are rewritten to host.docker.internal (PLAN §3.5).
#>

$ErrorActionPreference = 'Stop'
$all = @($args)
$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$node = 'temporal-poc-control-plane'
if ($env:ICEFLEET_KIND_NODE) { $node = $env:ICEFLEET_KIND_NODE }
$helper = '/var/tmp/icefleet/deploy/k8s/node.sh'
$jobs = @('fleet_check.py', 'smoke_cluster.py', 'benchmark.py', 'integration.py')

function Invoke-Node([string[]]$arguments) {
    # Out-Host: otherwise the command's output becomes part of the return value.
    & docker exec $node sh $helper @arguments | Out-Host
    return $LASTEXITCODE
}

function Sync-Files {
    docker exec $node rm -rf /var/tmp/icefleet | Out-Null
    docker exec $node mkdir -p /var/tmp/icefleet/jobs | Out-Null
    docker cp (Join-Path $root 'deploy') "${node}:/var/tmp/icefleet/deploy" | Out-Null
    foreach ($job in $jobs) {
        $path = Join-Path $root "scripts\$job"
        if (Test-Path $path) { docker cp $path "${node}:/var/tmp/icefleet/jobs/$job" | Out-Null }
    }
    # Windows checkouts may carry CRLF; the node's sh and kubectl want LF.
    docker exec $node find /var/tmp/icefleet -type f -exec sed -i 's/\r$//' '{}' '+'
    if ($LASTEXITCODE -ne 0) { throw 'k8s: copying files into the node failed' }
}

function Publish-Secrets {
    $lines = foreach ($line in Get-Content (Join-Path $root '.env')) {
        if ($line -match '^\s*(ICETL_[A-Z0-9_]+)\s*=\s*(.*?)\s*$') {
            $value = $matches[2].Trim('"').Trim("'")
            $value = $value -replace '//localhost:', '//host.docker.internal:' -replace '//127\.0\.0\.1:', '//host.docker.internal:'
            "$($matches[1])=$value"
        }
    }
    # No byte-order mark: PowerShell would otherwise prefix one to the first key's name.
    $OutputEncoding = New-Object System.Text.UTF8Encoding $false
    ($lines -join "`n") | docker exec -i $node sh $helper secrets
    if ($LASTEXITCODE -ne 0) { throw 'k8s: creating the Secrets failed' }
}

if ($all.Count -eq 0) { Get-Help $MyInvocation.MyCommand.Path -Detailed; exit 2 }
$verb = $all[0]
$rest = @($all | Select-Object -Skip 1)

switch ($verb) {
    'setup' {
        Sync-Files
        if ((Invoke-Node @('apply')) -ne 0) { exit 1 }
        Publish-Secrets
        exit (Invoke-Node @('jobs'))
    }
    'secrets' { Sync-Files; Publish-Secrets; exit 0 }
    'jobs' { Sync-Files; exit (Invoke-Node @('jobs')) }
    'workers' { Sync-Files; exit (Invoke-Node (@('workers') + $rest)) }
    'status' { exit (Invoke-Node @('status')) }
    'kubectl' {
        & docker exec -i $node env KUBECONFIG=/etc/kubernetes/admin.conf kubectl @rest
        exit $LASTEXITCODE
    }
    'run' {
        if ($rest.Count -lt 1) { Write-Host 'usage: .\scripts\k8s.ps1 run <script> [key=value ...]'; exit 2 }
        $script = $rest[0]
        $params = @("script=$script") + @($rest | Select-Object -Skip 1)
        if (-not ($params | Where-Object { $_ -like 'image=*' })) { $params += 'image=icefleet:dev' }
        Sync-Files
        if ((Invoke-Node @('jobs')) -ne 0) { exit 1 }
        $name = (& docker exec $node sh $helper submit @params | Select-Object -Last 1).Trim()
        if ($LASTEXITCODE -ne 0 -or -not $name) { throw 'k8s: submitting the workflow failed' }
        Write-Host "k8s: submitted $name ($($params -join ' '))"
        $started = Get-Date
        $null = Invoke-Node @('wait', $name, '5400')
        $succeeded = ($LASTEXITCODE -eq 0)
        Write-Host ("k8s: {0} ran {1:N0}s" -f $name, ((Get-Date) - $started).TotalSeconds)
        Write-Host '----- coordinator log -----'
        $null = Invoke-Node @('logs', $name)
        Write-Host '---------------------------'
        $null = Invoke-Node @('leftovers', $name, '240')
        $clean = ($LASTEXITCODE -eq 0)
        if ($succeeded -and $clean) { Write-Host "k8s: $name succeeded and left nothing behind"; exit 0 }
        exit 1
    }
    default { Write-Host "k8s: unknown verb '$verb'"; exit 2 }
}
