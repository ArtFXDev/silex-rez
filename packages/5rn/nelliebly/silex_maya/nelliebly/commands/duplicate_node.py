from __future__ import annotations

import logging
import typing
from typing import Any, Dict

from silex_client.action.command_base import CommandBase
from silex_client.action.parameter_buffer import ParameterBuffer
from silex_client.utils.parameter_types import SelectParameterMeta, TextParameterMeta
from silex_maya.utils.thread import execute_in_main_thread

# Forward references
if typing.TYPE_CHECKING:
    from silex_client.action.action_query import ActionQuery

from maya import cmds


class DuplicateNode(CommandBase):
    """
    Duplicate the selected node to all the instances of the given node type
    """

    parameters = {
        "node_type": {
            "label": "Node type",
            "type": SelectParameterMeta("locator", "transform", "camera"),
            "value": "locator",
        },
    }

    @CommandBase.conform_command()
    async def __call__(
        self,
        parameters: Dict[str, Any],
        action_query: ActionQuery,
        logger: logging.Logger,
    ):
        node_type: str = parameters["node_type"]

        selected_nodes = await execute_in_main_thread(cmds.ls, sl=True)
        while not selected_nodes:
            await self.prompt_user(
                action_query,
                {
                    "info": ParameterBuffer(
                        name="info",
                        value="The current selection is incorrect: Please select nodes",
                        type=TextParameterMeta(color="info"),
                    )
                },
            )
            selected_nodes = await execute_in_main_thread(cmds.ls, sl=True)

        for node in await execute_in_main_thread(cmds.ls, type=node_type):
            transform = node
            if not await execute_in_main_thread(
                lambda: cmds.nodeType(node) == "transform"
            ):
                transform = await execute_in_main_thread(
                    cmds.listRelatives, node, parent=True
                )

            for selected_node in selected_nodes:
                duplicated = await execute_in_main_thread(cmds.duplicate, selected_node)
                await execute_in_main_thread(cmds.parent, duplicated, transform)
