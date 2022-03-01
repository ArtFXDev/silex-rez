from __future__ import annotations
from importlib.resources import path

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
        },
        "file_paths": {
            "label": "Input file paths",
            "type": dict,
            "value": {},
        },
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

    async def get_aces_file_path(self, file_nodes_paths) -> Dict[str, Dict[str,str]]:
        """
        Return texturepath for each maya node file
        """
        return [
            { 
                "paths": obj["paths"],
                "attr": obj["attr"],
                "aces": cmds.getAttr(f"{obj['attr']}.colorSpace")
            }
            for obj in file_nodes_paths
        ]

    def get_expand_path(self, file_path: Path) -> Dict[str, str]:
        """
        return expand_path from utils
        """
        return files.expand_path(file_path)

    def get_version_path(self, file_path: Path, logger):
        """
        Search and return the version folder.
        Like this: P:/test_pipe/shots/s01/p010/storyboard_main/publish/v000
        """
        ext = file_path.suffix
        ext = ext.replace(".", "")
        expand_path = self.get_expand_path(file_path)
        logger.error(file_path)
        logger.error(expand_path)

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
        file_nodes_paths = parameters["file_paths"]

        attr = file_nodes_paths["attributes"]
        path = file_nodes_paths["file_paths"]
        file_nodes_paths = zip(attr,path)

        # exclude tx
        file_nodes_paths = [
            {   
                "paths": [p for p in path if p.suffix != ".tx"],
                "attr": attr.split(".")[0]
            } for attr, path in file_nodes_paths
        ]


        file_paths = [p["paths"] for p in file_nodes_paths]
        file_paths = [str(p) for pl in file_paths for p in pl]
        logger.error(file_paths)

        if len(file_paths) > 0:
            await self.prompt_filepath(file_paths, action_query)

        # fill with aces
        logger.error(file_nodes_paths)
        file_nodes_paths = await self.get_aces_file_path(file_nodes_paths)
        logger.error(file_nodes_paths)

        # prompt path files
        for temp_object in file_nodes_paths:
            paths = temp_object["paths"]
            attr = temp_object["attr"]
            aces = temp_object["aces"]
            logger.error(paths)
            logger.error(attr)
            logger.error(aces)
            for path in paths:
                
                expand_path = self.get_expand_path(path)
                logger.error(expand_path)

                out_file_name = path.with_suffix(".tx")

                if files.is_valid_pipeline_path(path):
                    version_path = self.get_version_path(path, logger)
                    final_path = version_path / "tx" / expand_path["Name"] / out_file_name
                else:
                    final_path = path.parent / out_file_name.name

                logger.error(final_path)
                logger.error(os.path.isfile(final_path))

                # create out dir if not exist
                if not os.path.exists(final_path.parent):
                    os.makedirs(final_path.parent)

                # If keep existing and tx already exist
                if not os.path.isfile(final_path) or not keep_existing_tx:
                    # exec maektx
                    await self.exec_make_tx(str(path), final_path)

                # test if export completed and set texture data
                if os.path.isfile(final_path):
                    await execute_in_main_thread(
                        self.set_texture_attribute, "fileTextureName", attr, final_path
                    )
                    await execute_in_main_thread(
                        self.set_texture_attribute, "colorSpace", attr, aces
                    )
