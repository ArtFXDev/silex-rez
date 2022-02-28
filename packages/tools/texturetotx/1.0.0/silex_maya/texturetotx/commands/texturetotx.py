
from __future__ import annotations
from cmath import exp

import typing
from typing import Any, Dict, List

from importlib_metadata import pathlib

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

    async def prompt_filepath(self, file_paths, action_query):
        # Create a new parameter to prompt label
        info_parameter = ParameterBuffer(
            type=TextParameterMeta("info"),
            name="info",
            label="Info",
            value="Theses path will be convert to tx :\n\n {}".format('\n'.join(file_paths)),
        )

        await self.prompt_user(action_query, { "info": info_parameter})

    def get_textures_file_path(self):
        return { node: cmds.getAttr(f"{node}.fileTextureName") for node in cmds.ls(type="file")  }
    
    def get_expand_path(self, file_path: Path):
        return files.expand_path(file_path)

    def get_version_path(self, file_path: str):
        file_path = Path(file_path)
        ext = file_path.suffix
        ext = ext.replace('.','')
        expand_path = self.get_expand_path(file_path)
        ext = expand_path["OutputType"]
        version_path = file_path.parent

        while len(version_path.parents) > 1 and ext not in version_path.stem:
            version_path = version_path.parent

        return version_path.parent

    def set_file_texture_attribute(self, node, value):
        cmds.setAttr(f"{node}.fileTextureName", value, type="string")

    async def exec_make_tx(self, input_file:str, out_file:str, logger):
        batch_cmd = (
            command_builder.CommandBuilder(
                "maketx", delimiter=None
            )
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

        # Export obj to fbx
        file_nodes_paths = await execute_in_main_thread(self.get_textures_file_path)
        logger.error(file_nodes_paths)
        file_nodes_paths = { n: p for n, p in file_nodes_paths.items() if Path(p).suffix != ".tx" }
        logger.error(file_nodes_paths)

        file_paths = [ p for n, p in file_nodes_paths.items() ]
        await self.prompt_filepath(file_paths, action_query)

        # prompt path files
        for node, file in file_nodes_paths.items():
            version_path = self.get_version_path(file)
            expand_path = self.get_expand_path(Path(file))
            out_file_name = Path(file).with_suffix(".tx")
            out_file_name = out_file_name.name
            final_path = version_path / "tx" / expand_path["Name"] / out_file_name
            logger.error(final_path)

        # exec maektx
        await self.exec_make_tx(file, final_path, logger)

        # set file attribute
        if os.path.isfile(final_path):
            await execute_in_main_thread(self.set_file_texture_attribute, node, file)
