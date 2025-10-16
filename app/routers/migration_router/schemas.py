from pydantic import BaseModel


class MigrateObjectTypeAsExportRequest(BaseModel):
    object_type_id: int
    parents: bool = False
    children: bool = False


class ParsedRequestedFileAsDict(BaseModel):
    object_type_instances_by_name: dict[str, dict] = {}
    parameter_type_instances_by_object_type_name: dict[str, list[dict]] = {}
