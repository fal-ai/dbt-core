from typing import Union

from dbt.adapters.base.impl import BaseAdapter
from dbt.adapters.teleport.impl import TeleportAdapter
from dbt.config.runtime import RuntimeConfig
from dbt.teleport.teleport_info import LocalTeleportInfo

def is_teleport_adapter(adapter: Union[BaseAdapter, TeleportAdapter]) -> bool:
    methods = [
        "storage_formats",
        "teleport_from_external_storage",
        "teleport_to_external_storage",
    ]
    return isinstance(adapter, TeleportAdapter) or all(map(lambda m: hasattr(adapter, m), methods))

def find_format(target_adapter: TeleportAdapter, ref_adapter: TeleportAdapter):
    target_formats = target_adapter.storage_formats()
    ref_formats = ref_adapter.storage_formats()
    for format in target_formats:
        if format in ref_formats:
            return format

    raise RuntimeError(
        f"No common format between {target_adapter.type()} and {ref_adapter.type()} "
        f"—  {ref_formats} | {target_formats}"
    )

def build_teleport_info(config: RuntimeConfig, target_adapter: TeleportAdapter, ref_adapter: TeleportAdapter):
    teleport_format = find_format(target_adapter, ref_adapter)

    # TODO: think of how to build other infos
    return LocalTeleportInfo(teleport_format)
