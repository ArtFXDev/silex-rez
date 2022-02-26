$letter = "P"
$user = $env:SERVER_USER
$PWD = $env:SERVER_PASS

$path = ("\\{0}" -f $args[0])
$path += "\PIPELINE"

Write-Output "BEFORE DELETE"
net use
Write-Output "---------------------------------"

Write-Output "DELETE *"
net use * /delete /yes
Write-Output "---------------------------------"

If ((Get-PSDrive).Name -eq 'PIPELINE' -or (Get-PSDrive).Name -eq 'P') {
	Write-Output "TODO"
}

Write-Output "USE P AND MARVIN"
net use P: $path /user:$user $PWD
net use Z: \\marvin\installers /USER:etudiant artfx2020
Write-Output "---------------------------------"

Write-Output "AFTER MOUNT P"
net use
Write-Output "---------------------------------"

Write-Output "LS P"
ls P:\
Write-Output "---------------------------------"

# New-SmbMapping -LocalPath 'P:' -RemotePath (Convert-Path $path) -UserName $user -Password $PWD