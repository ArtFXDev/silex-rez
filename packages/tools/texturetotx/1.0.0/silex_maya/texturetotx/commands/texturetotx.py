from __future__ import annotations

import typing
from typing import Any, Dict, List


# Forward references
if typing.TYPE_CHECKING:
    from silex_client.action.action_query import ActionQuery

from silex_maya.utils.thread import execute_in_main_thread
from silex_client.utils import thread as thread_client
from silex_client.utils.parameter_types import TextParameterMeta
from silex_client.action.parameter_buffer import ParameterBuffer
from silex_client.action.command_base import CommandBase
from silex_client.utils import command_builder
from silex_client.utils import files

import subprocess
import logging
import os
from pathlib import Path

from maya import cmds


class TextureToTx(CommandBase):
    """
    Convert texture to the scene to tx
    """

    parameters = {
        "keep_existing_tx": {
            "label": "Keep existing tx",
            "type": bool,
            "value": False,
        }
    }

    async def prompt_filepath(self, file_paths: List[str], action_query: ActionQuery):
        """
        Info box on action
        """
        info_parameter = ParameterBuffer(
            type=TextParameterMeta("info"),
            name="info",
            label="Info",
            value="Theses path will be convert to tx :\n\n {}".format(
                "\n".join(file_paths)
            ),
        )

        await self.prompt_user(action_query, {"info": info_parameter})

    def get_textures_file_path(self) -> Dict[str, Dict[str,str]]:
        """
        Return texturepath for each maya node file
        """
        return {
            node: {
                "file": cmds.getAttr(f"{node}.fileTextureName"),
                "aces": cmds.getAttr(f"{node}.colorSpace"),
            }
            for node in cmds.ls(type="file")
        }

    def get_expand_path(self, file_path: Path) -> Dict[str, str]:
        """
        return expand_path from utils
        """
        return files.expand_path(file_path)

    def get_version_path(self, file_path: str):
        """
        Search and return the version folder.
        Like this: P:/test_pipe/shots/s01/p010/storyboard_main/publish/v000
        """

        file_path = Path(file_path)
        ext = file_path.suffix
        ext = ext.replace(".", "")
        expand_path = self.get_expand_path(file_path)
        ext = expand_path["OutputType"]
        version_path = file_path.parent

        while len(version_path.parents) > 1 and ext not in version_path.stem:
            version_path = version_path.parent

        return version_path.parent

    def set_texture_attribute(self, attribute: str, node: str, value: str):
        """
        Set path to node file maya.
        """
        cmds.setAttr(f"{node}.{attribute}", value, type="string")

    async def exec_make_tx(self, input_file: str, out_file: str):
        """
        Launch the maketx process to convert tx in background
        """
        batch_cmd = (
            command_builder.CommandBuilder("maketx", delimiter=None)
            .param("o", out_file)
            .value(input_file)
        )
        await thread_client.execute_in_thread(
            subprocess.call, batch_cmd.as_argv(), shell=True
        )

    @CommandBase.conform_command()
    async def __call__(
        self,
        parameters: Dict[str, Any],
        action_query: ActionQuery,
        logger: logging.Logger,
    ):

        # get paramters
        keep_existing_tx = parameters["keep_existing_tx"]

        # Export obj to fbx
        file_nodes_paths = await execute_in_main_thread(self.get_textures_file_path)
        
        file_nodes_paths = {
            n: p for n, p in file_nodes_paths.items() if Path(p["file"]).suffix != ".tx"
        }

        file_paths = [p["file"] for p in file_nodes_paths.values()]
        if len(file_paths) > 0:
            await self.prompt_filepath(file_paths, action_query)

        # prompt path files
        for node, temp_object in file_nodes_paths.items():
            file = temp_object["file"]
            aces = temp_object["aces"]
            version_path = self.get_version_path(file)
            expand_path = self.get_expand_path(Path(file))
            out_file_name = Path(file).with_suffix(".tx")
            out_file_name = out_file_name.name
            final_path = version_path / "tx" / expand_path["Name"] / out_file_name
            logger.error(final_path)
            logger.error(os.path.isfile(final_path))

             # create out dir if not exist
            if not os.path.exists(final_path.parent):
                os.makedirs(final_path.parent)

            # If keep existing and tx already exist
            if not os.path.isfile(final_path) or not keep_existing_tx:
                # exec maektx
                await self.exec_make_tx(file, final_path)

            # test if export completed and set texture data
            if os.path.isfile(final_path):
                await execute_in_main_thread(
                    self.set_texture_attribute, "fileTextureName", node, final_path
                )
                await execute_in_main_thread(
                    self.set_texture_attribute, "colorSpace", node, aces
                )
