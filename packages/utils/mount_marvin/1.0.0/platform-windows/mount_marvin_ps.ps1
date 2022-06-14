if (($null -eq $env:MARVIN_USER) -or ($null -eq $env:MARVIN_PASS)) {
	Write-Output "[ERROR] mount_marvin | Please provide MARVIN_USER and MARVIN_PASS env variables."
	exit 1
}

net use * /delete /yes
net use \\marvin /user:$env:MARVIN_USER $env:MARVIN_PASS