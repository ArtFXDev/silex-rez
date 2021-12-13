# silex-rez

Silex rez is the repository containing all packages of the pipeline

## Introduction
Because silex-rez, contains silex packages we used git submodules to keep theses packages in their git environment
```
📦silex
 ┣ 📂aiogazu
 ┃ ┗ 📂1.0.0  => git submodule to aiogazu on branch main
 ┣ 📂silex_client
 ┃ ┣ 📂beta.0.1.0 => git submodule to silex_client on branch beta
 ┃ ┗ 📂prod.0.1.0 => git submodule to silex_client on branch prod
 ┣ 📂silex_houdini
 ┃ ┣ 📂beta.0.1.0 => git submodule to silex_houdini on branch beta
 ┃ ┗ 📂prod.0.1.0 => git submodule to silex_houdini on branch prod
 ┣ 📂silex_maya
 ┃ ┣ 📂beta.0.1.0 => git submodule to silex_maya on branch beta
 ┃ ┗ 📂prod.0.1.0 => git submodule to silex_maya on branch prod
 ┣ 📂silex_nuke
 ┃ ┣ 📂beta.0.1.0 => git submodule to silex_nuke on branch beta
 ┃ ┗ 📂prod.0.1.0 => git submodule to silex_nuke on branch prod
 ┗ 📜.rez
 ```
 
## Update
 ```
git pull
git submodule update --remote
```

## Config

This repository use .rez to step down recusrsively in folders tree. Algorithm to find '.rez' files are defined in [this](https://github.com/ArtFXDev/silex_fog_snapin/blob/main/rez/config/rezconfig.py) rezconfig.py 
