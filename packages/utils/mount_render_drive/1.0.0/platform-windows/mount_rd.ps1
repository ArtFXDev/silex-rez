$letter = "P"
$user = $env:SERVER_USER
$PWD = $env:SERVER_PASS

$path = ("\\{0}" -f $args[0])
$path += "\PIPELINE"

net use 
net use * /delete /yes

If ((Get-PSDrive).Name -eq 'PIPELINE' -or (Get-PSDrive).Name -eq 'P') {
	Write-Output "TODO"
}

net use P: $path /user:$user $PWD
net use Z: \\marvin\installers /USER:etudiant artfx2020
net use 
New-SmbMapping -LocalPath 'P:' -RemotePath (Convert-Path $path) -UserName $user -Password $PWD