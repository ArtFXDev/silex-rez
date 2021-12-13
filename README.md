# silex-rez

<p align="center">
 <a href="https://github.com/nerdvegas/rez" target="_blank" style="text-decoration: none">
   <img src="https://img.shields.io/badge/rez-Rez_Packages-orange?style=for-the-badge">
 </a>
 <a href="https://github.com/nerdvegas/rez" target="_blank" style="text-decoration: none">
   <img src="https://img.shields.io/badge/aiogazu-Git_Submodule-success?style=for-the-badge">
 </a>
 <a href="https://github.com/ArtFXDev/silex_client" target="_blank" style="text-decoration: none">
   <img src="https://img.shields.io/badge/silex_client-Git_Submodule-success?style=for-the-badge">
 </a>
 <a href="https://github.com/ArtFXDev/silex_houdini" target="_blank" style="text-decoration: none">
   <img src="https://img.shields.io/badge/silex_houdini-Git_Submodule-success?style=for-the-badge">
 </a>
 <a href="https://github.com/ArtFXDev/silex_maya" target="_blank" style="text-decoration: none">
   <img src="https://img.shields.io/badge/silex_maya-Git_Submodule-success?style=for-the-badge">
 </a>
 <a href="https://github.com/ArtFXDev/silex_nuke" target="_blank" style="text-decoration: none">
   <img src="https://img.shields.io/badge/silex_nuke-Git_Submodule-success?style=for-the-badge">
 </a>
  <a href="https://github.com/ArtFXDev/tractor_lib" target="_blank" style="text-decoration: none">
   <img src="https://img.shields.io/badge/tractor_lib-Git_Submodule-success?style=for-the-badge">
 </a>
</p>

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
 
 📦softwares
 ┣ 📂tractor
 ┃ ┗ 📂0.1.0 => git submodule to tractor_lib on branch main
 ┗ 📜.rez
 ```

## Update
```
git pull
git submodule update --remote
```

## Config

This repository use .rez to step down recursively in folders tree. Algorithm to find '.rez' files are defined in [this](https://github.com/ArtFXDev/silex_fog_snapin/blob/main/rez/config/rezconfig.py) rezconfig.py 
