from pydantic import BaseModel, Field


class PermissionResponse(BaseModel):
    permission_id: int = Field(..., alias="id")
    parent_id: int = Field(..., ge=1, alias="itemId")
    root_item_id: int | None = Field(None, alias="rootItemId")
    root_permission_id: int | None = Field(None, alias="rootPermissionId")
    permission: str = Field(..., min_length=1)
    permission_name: str = Field(..., alias="permissionName", min_length=1)
    create: bool = Field(...)
    read: bool = Field(...)
    update: bool = Field(...)
    delete: bool = Field(...)
    admin: bool = Field(...)

    class Config:
        populate_by_name = True
