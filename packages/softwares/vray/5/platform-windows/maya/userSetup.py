import maya.cmds as cmds
new_renderer = "V-Ray"

print(f"Set current Renderer to: {new_renderer}")
cmds.setAttr("defaultRenderGlobals.currentRenderer", new_renderer, type="string")