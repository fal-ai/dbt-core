from dataclasses import dataclass
from typing import Union

from dbt.adapters.base.relation import BaseRelation
from dbt.contracts.relation import ComponentName

@dataclass
class TeleportInfo:
    format: str

    @classmethod
    def relation_name(cls, relation: Union[str, BaseRelation]):
        if isinstance(relation, str):
            # TODO: should we check for quoting?
            return relation
        else:
            path = relation.path
            db = path.get_lowered_part(ComponentName.Database)
            sc = path.get_lowered_part(ComponentName.Schema)
            tb = path.get_lowered_part(ComponentName.Identifier)
            return f"{db}.{sc}.{tb}"

    def build_relation_path(self, relation: Union[str, BaseRelation]):
        rel_name = TeleportInfo.relation_name(relation)
        return rel_name + "." + self.format

    def build_relation_url(self, relation: Union[str, BaseRelation]):
        return self.build_url(self.build_relation_path(relation))

    def build_url(self, path: str) -> str:
        raise NotImplemented

@dataclass
class LocalTeleportInfo(TeleportInfo):
    def build_url(self, path: str):
        from pathlib import Path

        dir = Path.cwd() / 'teleport'
        dir.mkdir(exist_ok=True)

        return dir / path

# TODO: How to do Teleport Configuration
# Will each adapter have to implement a specific case for each teleport backend.
# And how will we generalize these configurations?
# Each adapter will have a specific `teleport` property?
#       my_db:
#         type: duckdb
#         path: ./local_file.db
#         teleport:
#             type: s3
#             bucket: my_bucket
#             access_key_id: ...
#             secret_access_key: ...
# Or a central configuration will be shared accross all adapters?
#       targets:
#         - name: quack
#           type: duckdb
#           path: ./local_file.db
#         - name: pyfal
#           type: fal
#           mode: local
#       envs:
#         - name: dev
#           targets:
#             - quack
#             - pyfal
#           default: quack
#           teleport: # sharing these to `quack` and `pyfal` targets
#             type: s3
#             bucket: my_bucket
#             access_key_id: ...
#             secret_access_key: ...
#         - name: prod
#           targets:
#             ...
# Hard to achieve for all different access methods Teleport backends can have.
@dataclass
class S3TeleportInfo(TeleportInfo):
    bucket: str
    inner_path: str = 'teleport/'
    # access_key ?

    def build_url(self, path: str):
        return f's3://{self.bucket}/{self.inner_path}{path}'
