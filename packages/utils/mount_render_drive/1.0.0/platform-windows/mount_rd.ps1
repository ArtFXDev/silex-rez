$USER = $env:SERVER_USER
$PWD = $env:SERVER_PASS

$path = ("\\{0}" -f $args[0])
$path += "\PIPELINE"

Write-Output "DELETE *"
net use * /delete /yes
Write-Output "---------------------------------"

If ((Get-PSDrive).Name -eq 'PIPELINE' -or (Get-PSDrive).Name -eq 'P') {
	Write-Output "TODO"
}

try {
	Write-Output "NET USE P"
	net use P: $path /user:$USER $PWD /persistent:no
	Write-Output "---------------------------------"
	
	if ($LASTEXITCODE -eq 0) {
        write-host "net use successful"
    } else {
        throw $error[0].Exception
    }
} catch {
	Write-Output "ERROR MOUNTING P:"
	Write-Output $error[0].Exception
	exit 1
}

Write-Output "AFTER MOUNT P"
net use
Write-Output "---------------------------------"

Write-Output "LS P"
ls P:\
Write-Output "---------------------------------"


# New-SmbMapping -LocalPath 'P:' -RemotePath (Convert-Path $path) -UserName $user -Password $PWD