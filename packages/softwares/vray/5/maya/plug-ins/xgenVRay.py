from __future__ import absolute_import, division, print_function
from vray.vray_2to3 import *

import sys
import os.path
import maya.api.OpenMaya as om
import maya.OpenMayaAnim as oma
import maya.cmds as cmds
from xgenm import XgExternalAPI as xgapi
import xgenm as xg

from vray import *
from vray.xgen import *

# using the Maya Python API 2.0.
def maya_useNewAPI():
	pass

# Initialize the plug-in
def initializePlugin(plugin):
	xgenPlug = om.MFnPlugin(plugin, "Chaos Group", cmds.vray("versionName"))
	# Install XGen UI callbacks to extend the Render UI.
	xg.registerCallback("RenderAPIRendererTabUIInit", "vray.xgenVRayUI.xgVRayUI")
	xg.registerCallback("RenderAPIRendererTabUIRefresh", "vray.xgenVRayUI.xgVRayRefresh")
	xg.registerCallback("PostDescriptionCreate", "vray.xgenVRayUI.xgVRayOnCreateDescription")

# Uninitialize the plug-in
def uninitializePlugin(plugin):
	xg.deregisterCallback("RenderAPIRendererTabUIInit", "vray.xgenVRayUI.xgVRayUI")
	xg.deregisterCallback("RenderAPIRendererTabUIRefresh", "vray.xgenVRayUI.xgVRayRefresh")
	xg.deregisterCallback("PostDescriptionCreate", "vray.xgenVRayUI.xgVRayOnCreateDescription")
